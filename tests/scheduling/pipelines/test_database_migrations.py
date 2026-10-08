"""Unit tests for the startup expand migration.

Startup installs the ORM-required columns, the two indexes the write tiers gate
on, and the saved-pipeline foreign key -- the last under a size gate and two
read-only preflights, because it is the one statement here that copies the table
and blocks DML. It runs no census query over table data, and never adds the
source CHECK to an existing table -- that is verified and reported so PR3 gates
on an explicit signal rather than on a clean boot.

Two things no test here can establish. The first is how long the index build
takes, because `ADD INDEX` scans and sorts the table and only a rehearsal
against production-shaped data measures that. The second is server lock
behaviour: these tests assert the emitted SQL and its order, not that MySQL
honours LOCK=NONE while `last_run_at` is being written.
"""

import ast
import contextlib
import dataclasses
import inspect
import logging
import pathlib
import threading
import time
import types
import typing
from unittest import mock

import pytest
import sqlalchemy
from sqlalchemy.dialects import mysql, sqlite
from sqlalchemy.schema import CreateTable

from cloud_pipelines_backend.scheduling.pipelines import (
    database_migrations as migrations,
)
from cloud_pipelines_backend.scheduling.pipelines import db_models
from tests.scheduling.pipelines.schema_test_support import (
    _LEGACY_SCHEDULE_TABLE_DDL,
    _code_without_docstrings,
    _empty_engine,
    _execute,
    _insert_legacy_row,
    _legacy_engine,
    _RecordingConnection,
    _snapshot,
    _target_engine,
)


class TestMigrationConnectionDoesNotPoisonThePool:
    """ProxySQL disables multiplexing after GET_LOCK and does not restore it.

    ProxySQL can stop multiplexing a frontend connection once it sees GET_LOCK
    without re-enabling it on RELEASE_LOCK. The migration borrows a connection
    from the *application's own* engine, so returning it to the pool would leave
    the app one permanently de-multiplexed connection for the life of the
    process. Nothing downstream could detect or undo that.

    These tests observe the real DBAPI connection object across pool checkouts.
    An earlier version only asserted that a monkeypatched `invalidate` method was
    called (pi-29), which proves a call happened and not that anything was
    recycled -- the same vacuity as asserting a mock was invoked.
    """

    @staticmethod
    def _pooled_engine(tmp_path: pathlib.Path) -> sqlalchemy.Engine:
        """File-backed SQLite with a real reusable pool of exactly one.

        A pool of one makes reuse observable: without a discard, the next
        checkout returns the *same* DBAPI object. In-memory SQLite cannot show
        this because each connection would be a different database.
        """
        engine = sqlalchemy.create_engine(
            f"sqlite:///{tmp_path / 'pool.sqlite'}",
            poolclass=sqlalchemy.pool.QueuePool,
            pool_size=1,
            max_overflow=0,
        )
        _execute(engine, _LEGACY_SCHEDULE_TABLE_DDL)
        return engine

    @staticmethod
    def _driver_connection(engine: sqlalchemy.Engine) -> object:
        with engine.connect() as conn:
            return conn.connection.driver_connection

    def test_the_pool_reuses_the_same_dbapi_connection_without_a_discard(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Establishes the control: reuse is real, so the next test means something."""
        engine = self._pooled_engine(tmp_path)

        assert self._driver_connection(engine) is self._driver_connection(engine)

    def test_the_migration_connection_is_physically_replaced(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """The real DBAPI object must not survive into the next checkout."""
        engine = self._pooled_engine(tmp_path)
        before = self._driver_connection(engine)
        # Real invalidate(); only the dialect predicate and the MySQL-only lock
        # statements are faked, because SQLite has no GET_LOCK.
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda dialect: True)
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_: True)
        monkeypatch.setattr(migrations, "_release_lock", lambda **_: None)

        report = migrations.expand_schema(db_engine=engine)

        assert report.columns_ready
        assert self._driver_connection(engine) is not before
        # The expansion's own work survived the discard.
        assert (
            "pipeline_task_spec_from_user_pipeline_id" in _snapshot(engine)["columns"]
        )

    def test_the_lock_loser_discards_too(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """A failed GET_LOCK is still a GET_LOCK to ProxySQL.

        This is the path that runs when two pods boot together, so leaking here
        would leak on exactly the occasion the lock exists for.
        """
        engine = self._pooled_engine(tmp_path)
        before = self._driver_connection(engine)
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda dialect: True)
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_: False)
        monkeypatch.setattr(migrations, "_release_lock", lambda **_: None)

        migrations.expand_schema(db_engine=engine)

        assert self._driver_connection(engine) is not before

    def test_a_failure_inside_the_expansion_still_discards(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """The exception must propagate and the connection must not be pooled."""
        engine = self._pooled_engine(tmp_path)
        before = self._driver_connection(engine)
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda dialect: True)
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_: True)
        monkeypatch.setattr(migrations, "_release_lock", lambda **_: None)

        def _boom(**_: object) -> None:
            raise RuntimeError("expansion failed")

        monkeypatch.setattr(migrations, "_expand", _boom)

        with pytest.raises(RuntimeError, match="expansion failed"):
            migrations.expand_schema(db_engine=engine)

        assert self._driver_connection(engine) is not before

    def test_no_sql_is_issued_after_the_discard(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """Anything executed after invalidate would run on a fresh connection.

        That would silently escape both the advisory lock and the bounded
        lock_wait_timeout, so the discard has to be genuinely last.
        """
        engine = self._pooled_engine(tmp_path)
        events: list[str] = []
        sqlalchemy.event.listen(
            engine,
            "before_cursor_execute",
            lambda conn, cursor, statement, *a: events.append(
                statement.split()[0].upper()
            ),
        )
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda dialect: True)
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_: True)
        monkeypatch.setattr(migrations, "_release_lock", lambda **_: None)
        real_discard = migrations._discard_connection

        def _marked(*, conn: sqlalchemy.Connection) -> None:
            events.append("<discard>")
            real_discard(conn=conn)

        monkeypatch.setattr(migrations, "_discard_connection", _marked)

        migrations.expand_schema(db_engine=engine)

        assert "<discard>" in events, events
        assert (
            events[-1] == "<discard>"
        ), f"SQL issued after the discard: {events[events.index('<discard>') + 1 :]}"

    def test_the_discard_runs_after_the_lock_wait_restore(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """Ordering: the restore needs a live connection, so it must come first."""
        engine = self._pooled_engine(tmp_path)
        order: list[str] = []
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda dialect: True)
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_: True)
        monkeypatch.setattr(
            migrations, "_release_lock", lambda **_: order.append("release")
        )
        real_discard = migrations._discard_connection

        def _marked(*, conn: sqlalchemy.Connection) -> None:
            order.append("discard")
            real_discard(conn=conn)

        monkeypatch.setattr(migrations, "_discard_connection", _marked)

        @contextlib.contextmanager
        def _fake_wait(*, conn: object, dialect: str) -> object:
            order.append("set_timeout")
            yield
            order.append("restore_timeout")

        monkeypatch.setattr(migrations, "_bounded_metadata_lock_wait", _fake_wait)

        migrations.expand_schema(db_engine=engine)

        assert order == ["set_timeout", "release", "restore_timeout", "discard"]

    def test_a_fresh_database_never_reaches_the_lock_or_the_discard(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """create_all owns an absent table, so no lock statement is issued."""
        engine = self._pooled_engine(tmp_path)
        _execute(engine, f"DROP TABLE {migrations._SCHEDULE_TABLE}")
        before = self._driver_connection(engine)
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda dialect: True)

        report = migrations.expand_schema(db_engine=engine)

        assert report.status_of("table") is migrations.StepStatus.SKIPPED
        assert self._driver_connection(engine) is before

    def test_sqlite_does_not_discard_because_it_takes_no_advisory_lock(
        self, tmp_path: pathlib.Path
    ) -> None:
        """The discard is justified by GET_LOCK; without one it must not fire."""
        engine = self._pooled_engine(tmp_path)
        before = self._driver_connection(engine)

        migrations.expand_schema(db_engine=engine)

        assert self._driver_connection(engine) is before

    def test_an_unrecyclable_connection_fails_startup_closed(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """Never knowingly return a pinned connection to the pool.

        Reversed after pi-29's review: this previously logged and continued,
        leaving the pool in exactly the state the function exists to prevent.
        Raising keeps the old singleton pod Available and the next boot retries.
        """
        engine = self._pooled_engine(tmp_path)
        conn = engine.connect()
        monkeypatch.setattr(
            conn,
            "invalidate",
            lambda: _raise(RuntimeError("no")),
            raising=False,
        )
        monkeypatch.setattr(
            conn,
            "detach",
            lambda: _raise(RuntimeError("nor this")),
            raising=False,
        )

        with pytest.raises(
            migrations.SchedulerSchemaError, match="cannot be proven unpinned"
        ):
            migrations._discard_connection(conn=conn)

    def test_the_detach_fallback_physically_recycles_too(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """The fallback must recycle, not merely be called in the right order.

        pi-29's point: mocking detach/close proved call order and nothing about
        the pool. Only `invalidate` is forced to fail here; `detach()`/`close()`
        are real, and the assertion is the same identity check used for the
        primary path.
        """
        engine = self._pooled_engine(tmp_path)
        before = self._driver_connection(engine)
        conn = engine.connect()
        assert conn.connection.driver_connection is before
        monkeypatch.setattr(
            conn,
            "invalidate",
            lambda: _raise(RuntimeError("no")),
            raising=False,
        )

        migrations._discard_connection(conn=conn)

        assert self._driver_connection(engine) is not before

    def test_a_double_failure_names_both_causes(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """Which path failed first tells an operator what is unhealthy."""
        engine = self._pooled_engine(tmp_path)
        conn = engine.connect()
        monkeypatch.setattr(
            conn,
            "invalidate",
            lambda: _raise(TimeoutError("hung")),
            raising=False,
        )
        monkeypatch.setattr(
            conn,
            "detach",
            lambda: _raise(ValueError("nor this")),
            raising=False,
        )

        with pytest.raises(migrations.SchedulerSchemaError) as raised:
            migrations._discard_connection(conn=conn)

        assert "invalidate raised TimeoutError" in str(raised.value)
        assert "detach/close raised ValueError" in str(raised.value)
        # Chained from the proximate failure so its traceback survives.
        assert isinstance(raised.value.__cause__, ValueError)

    def test_one_dialect_predicate_governs_lock_and_discard(self) -> None:
        """Three independent `== "mysql"` tests is how one stops matching.

        The shape of the `_add_column` fall-through bug found earlier, so the
        predicate is asserted to be shared rather than merely correct today.
        """
        assert migrations._uses_advisory_lock("mysql") is True
        assert migrations._uses_advisory_lock("sqlite") is False
        for function in (migrations._acquire_lock, migrations._release_lock):
            assert "_uses_advisory_lock" in inspect.getsource(function)
        assert "_uses_advisory_lock" in inspect.getsource(migrations.expand_schema)


class TestAbsenceHintsMatchTheObjectKind:
    """A missing column, index and CHECK have three different consequences.

    pi-29 found one shared sentence telling a missing column that a hardening
    step had not installed an index, and telling a missing CHECK that writes were
    disabled when no tier gates on check_ready.
    """

    @staticmethod
    def _hint(step: str) -> str:
        return migrations._absence_hint(step=step, dialect="mysql")

    def test_a_missing_column_reports_startup_expansion_not_an_index(
        self,
    ) -> None:
        hint = self._hint("column:schedule_path")
        assert "cannot serve" in hint
        assert "index" not in hint
        assert "hardening" not in hint

    def test_a_missing_index_reports_disabled_writes_before_the_retry(
        self,
    ) -> None:
        """Startup builds the indexes now, so the hint may name the retry.

        It must still lead with the consequence. An operator reading this is
        deciding whether a closed write tier is expected, and "the next boot
        retries" alone does not answer that. The discarded operator command must
        not be named: it no longer exists, and startup owns the build.
        """
        hint = self._hint(f"index:{migrations._UQ_SCHEDULE_PATH}")
        assert "disabled" in hint
        assert "retries" in hint
        assert "hardening" not in hint
        assert hint.index("disabled") < hint.index("retries")

    def test_a_missing_check_must_not_claim_writes_are_disabled(self) -> None:
        """No readiness tier gates on check_ready, so saying so would be false."""
        hint = self._hint(f"check:{migrations._CK_SOURCE}")
        assert "disabled" not in hint
        assert "Defence in depth" in hint and "fresh database" in hint

    def test_all_three_kinds_differ(self) -> None:
        hints = {
            self._hint("column:schedule_path"),
            self._hint(f"index:{migrations._UQ_SCHEDULE_PATH}"),
            self._hint(f"check:{migrations._CK_SOURCE}"),
        }
        assert len(hints) == 3, hints

    def test_an_unknown_kind_raises_rather_than_defaulting(self) -> None:
        """A silent default is how a new object kind gets the wrong consequence."""
        with pytest.raises(migrations.SchedulerSchemaError, match="no absence hint"):
            self._hint("trigger:something_new")

    def test_every_policy_kind_has_a_hint(self) -> None:
        """The two tables must stay in step, or a policy kind hits the raise.

        The index kind is probed with a REAL index name rather than a placeholder:
        its hint names the consequence per index, so an invented name legitimately
        raises. That raise is asserted separately below.
        """
        real_names = {"index:": migrations._UQ_SCHEDULE_PATH}
        for prefix, _policy in migrations._ABSENT_POLICY:
            assert self._hint(f"{prefix}{real_names.get(prefix, 'whatever')}")

    def test_each_index_states_its_own_consequence(self) -> None:
        """A closed feature and a slow query are not the same news.

        Two of the indexes gate a write tier; the owner-page index gates nothing
        and only costs a scan. One shared sentence told an operator that listing
        schedules was disabled when it was merely expensive.
        """
        assert "disabled" in self._hint(f"index:{migrations._UQ_SCHEDULE_PATH}")
        assert "disabled" in self._hint(f"index:{migrations._IX_REFERENCE}")

        owner_page = self._hint(f"index:{migrations._IX_OWNER_PAGE}")
        assert "no feature is disabled" in owner_page
        assert "scans rows belonging to other users" in owner_page

    def test_every_target_index_has_a_consequence(self) -> None:
        """Adding an index to the target set must not inherit someone else's news."""
        for spec in migrations._TARGET_INDEXES:
            assert self._hint(f"index:{spec.name}")

    def test_an_unrecorded_index_raises_rather_than_defaulting(self) -> None:
        with pytest.raises(
            migrations.SchedulerSchemaError, match="no absence consequence"
        ):
            self._hint("index:ix_something_nobody_described")


def _raise(error: Exception) -> None:
    raise error


class TestStartupExpansion:
    """Columns and indexes, by re-inspection. No census, no CHECK install."""

    def test_legacy_table_gains_the_mapped_columns(self) -> None:
        engine = _legacy_engine()
        _insert_legacy_row(engine)

        report = migrations.expand_schema(db_engine=engine)

        assert report.columns_ready
        for column in (
            "pipeline_task_spec_from_user_pipeline_version_key",
            "schedule_path",
        ):
            assert report.status_of(f"column:{column}") is migrations.StepStatus.APPLIED

    def test_startup_installs_both_indexes(self) -> None:
        """`web` runs at count 1 and LOCK=NONE permits concurrent DML.

        So a slow build costs one pod a slower boot, not a scheduling outage,
        and the separate operator command it used to need is gone.
        """
        engine = _legacy_engine()

        report = migrations.expand_schema(db_engine=engine)

        for spec in migrations._TARGET_INDEXES:
            assert (
                report.status_of(f"index:{spec.name}") is migrations.StepStatus.APPLIED
            )
        assert report.indexes_ready
        with engine.connect() as conn:
            live = set(migrations._inspect(conn=conn).indexes)
        assert {spec.name for spec in migrations._TARGET_INDEXES} <= live

    def test_the_write_tiers_open_on_the_same_boot(self) -> None:
        """The whole point: the tiers PR3 gates on are satisfied by startup."""
        engine = _legacy_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert report.path_writes_ready
        assert report.reference_writes_ready
        assert report.schema_ready

    def test_each_index_is_installed_independently(self) -> None:
        """One tier gating on a conflicting index must not withhold the other."""
        engine = _legacy_engine()
        migrations.expand_schema(db_engine=engine)
        _execute(engine, f"DROP INDEX {migrations._IX_REFERENCE}")

        report = migrations.expand_schema(db_engine=engine)

        assert (
            report.status_of(f"index:{migrations._UQ_SCHEDULE_PATH}")
            is migrations.StepStatus.ALREADY_PRESENT
        )
        assert (
            report.status_of(f"index:{migrations._IX_REFERENCE}")
            is migrations.StepStatus.APPLIED
        )

    def test_duplicate_paths_close_only_the_path_tier_and_change_no_rows(
        self,
    ) -> None:
        """The fail-safe if the no-writer assumption is ever wrong.

        There is no writer for `schedule_path` today, so every value is NULL and
        the unique build cannot fail on data -- which is why this module runs no
        census. That reasoning is load-bearing, so this pins the behaviour if it
        stops holding: the unique build fails, its tier stays closed, the
        independent reference index still lands, and not one row is touched.
        """
        engine = _legacy_engine()
        for index in range(2):
            _insert_legacy_row(engine, schedule_id=str(index) * 20)
        _execute(
            engine,
            f"ALTER TABLE {migrations._SCHEDULE_TABLE} ADD COLUMN schedule_path VARCHAR(255)",
        )
        _execute(
            engine,
            f"UPDATE {migrations._SCHEDULE_TABLE} SET schedule_path = 'upi/nightly'",
        )
        with engine.connect() as conn:
            before = conn.execute(
                sqlalchemy.text(
                    f"SELECT id, created_by, schedule_path FROM {migrations._SCHEDULE_TABLE} ORDER BY id"
                )
            ).all()

        report = migrations.migrate_db(db_engine=engine)

        assert (
            report.status_of(f"index:{migrations._UQ_SCHEDULE_PATH}")
            is migrations.StepStatus.BLOCKED
        )
        assert not report.path_writes_ready
        assert (
            report.status_of(f"index:{migrations._IX_REFERENCE}")
            is migrations.StepStatus.APPLIED
        )
        assert report.reference_writes_ready
        # No row is deduplicated, deleted or rewritten to make the build pass.
        with engine.connect() as conn:
            after = conn.execute(
                sqlalchemy.text(
                    f"SELECT id, created_by, schedule_path FROM {migrations._SCHEDULE_TABLE} ORDER BY id"
                )
            ).all()
        assert after == before
        assert len(after) == 2

    def test_an_absent_check_is_not_told_to_wait_for_a_build(self) -> None:
        """Startup never installs the CHECK, so promising a retry misleads.

        The single hint used to tell an absent CHECK that "the next boot retries
        the build". Anyone reading that would go looking for a failing build
        that does not and will not exist.
        """
        engine = _legacy_engine()

        report = migrations.migrate_db(db_engine=engine)

        step = next(
            s for s in report.steps if s.name == f"check:{migrations._CK_SOURCE}"
        )
        detail = step.detail or ""
        assert "retries" not in detail
        assert "fresh database" in detail
        assert "no readiness tier depends on it" in detail

    def test_an_absent_index_is_told_it_will_be_retried(self) -> None:
        """The same hint has to stay right for the kind it was written for."""
        hint = migrations._absence_hint(
            step=f"index:{migrations._UQ_SCHEDULE_PATH}", dialect="mysql"
        )

        assert "next boot retries the build" in hint

    def test_a_conflicting_index_blocks_only_its_own_tier(self) -> None:
        """Reported and left alone; dropping it is an operator decision."""
        engine = _legacy_engine()
        _execute(
            engine,
            f"CREATE INDEX {migrations._IX_REFERENCE} ON {migrations._SCHEDULE_TABLE} (created_by)",
        )

        report = migrations.expand_schema(db_engine=engine)

        assert (
            report.status_of(f"index:{migrations._IX_REFERENCE}")
            is migrations.StepStatus.BLOCKED
        )
        assert not report.reference_writes_ready
        assert report.path_writes_ready
        with engine.connect() as conn:
            live = migrations._inspect(conn=conn).indexes[migrations._IX_REFERENCE]
        assert list(live["column_names"]) == ["created_by"]

    def test_an_index_failure_is_not_fatal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A closed feature gate is not the same as every request failing."""
        engine = _legacy_engine()

        def _fail(**_kwargs: object) -> None:
            raise sqlalchemy.exc.OperationalError(
                "ALTER", {}, Exception("lock wait timeout exceeded")
            )

        monkeypatch.setattr(migrations, "_create_index", _fail)

        report = migrations.migrate_db(db_engine=engine)

        assert report.columns_ready
        assert not report.indexes_ready
        assert not report.path_writes_ready

    def test_a_failed_build_is_retried_on_the_next_boot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        real_create = migrations._create_index

        def _fail(**_kwargs: object) -> None:
            raise sqlalchemy.exc.OperationalError(
                "ALTER", {}, Exception("lock wait timeout exceeded")
            )

        monkeypatch.setattr(migrations, "_create_index", _fail)
        migrations.migrate_db(db_engine=engine)
        monkeypatch.setattr(migrations, "_create_index", real_create)

        report = migrations.migrate_db(db_engine=engine)

        assert report.indexes_ready

    def test_legacy_rows_keep_a_null_path_and_no_column_is_dropped(
        self,
    ) -> None:
        engine = _legacy_engine()
        _insert_legacy_row(engine)
        before = set(_snapshot(engine)["columns"])

        migrations.expand_schema(db_engine=engine)

        assert before <= set(_snapshot(engine)["columns"])
        with engine.connect() as conn:
            assert (
                conn.execute(
                    sqlalchemy.text(
                        "SELECT COUNT(*) FROM scheduled_pipeline_run WHERE schedule_path IS NULL"
                    )
                ).scalar_one()
                == 1
            )

    def test_startup_never_installs_the_hardening_itself(self) -> None:
        """The CHECK is data-dependent, so a separate coded step installs it."""
        engine = _legacy_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert (
            report.status_of("check:ck_scheduled_pipeline_run_source")
            is migrations.StepStatus.SKIPPED
        )
        assert not report.check_ready

    def test_the_startup_module_contains_no_data_scan(self) -> None:
        """Finding 4: a repeated per-replica full scan is the thing to avoid.

        Structural rather than behavioural on purpose — the scans now live in a
        separate operator module, so their absence here is a property of the file
        rather than of one code path a future edit could re-enter.
        """
        code = _code_without_docstrings(migrations.__file__)

        assert "COUNT(" not in code
        assert "GROUP BY" not in code

    def test_second_run_changes_nothing(self) -> None:
        engine = _legacy_engine()
        _insert_legacy_row(engine)
        migrations.expand_schema(db_engine=engine)
        after_first = _snapshot(engine)

        second = migrations.expand_schema(db_engine=engine)

        assert not second.applied
        assert _snapshot(engine) == after_first

    def test_a_fresh_create_all_database_is_already_final(self) -> None:
        engine = _target_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert not report.applied
        assert report.schema_ready
        assert report.check_ready

    def test_partially_applied_table_completes_without_reapplying(self) -> None:
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(255)",
        )

        report = migrations.expand_schema(db_engine=engine)

        assert (
            report.status_of("column:schedule_path")
            is migrations.StepStatus.ALREADY_PRESENT
        )
        assert (
            report.status_of("column:pipeline_task_spec_from_user_pipeline_version_key")
            is migrations.StepStatus.APPLIED
        )

    def test_absent_table_is_left_to_create_all(self) -> None:
        engine = _empty_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert report.status_of("table") is migrations.StepStatus.SKIPPED
        assert not report.applied


class TestExactVerification:
    """Finding 3: a matching name is not a completed step."""

    def test_a_column_of_the_right_name_and_wrong_type_is_fatal(self) -> None:
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path INTEGER",
        )

        with pytest.raises(
            migrations.SchedulerSchemaError, match="expected a VARCHAR column"
        ):
            migrations.expand_schema(db_engine=engine)

    def test_a_column_that_is_merely_too_short_is_widened(self) -> None:
        """Too short is repairable; the verdict afterwards is still exact."""
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(64)",
        )

        report = migrations.expand_schema(db_engine=engine)

        assert report.status_of("column:schedule_path") is migrations.StepStatus.APPLIED
        with engine.connect() as conn:
            live = migrations._inspect(conn=conn).columns["schedule_path"]
        assert live["type"].length == db_models.SCHEDULE_PATH_LENGTH

    def test_a_column_wider_than_the_target_is_fatal(self) -> None:
        """Not repairable: narrowing can truncate already-stored values."""
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(1024)",
        )

        with pytest.raises(
            migrations.SchedulerSchemaError,
            match="expected length 255, found 1024",
        ):
            migrations.expand_schema(db_engine=engine)

    def test_a_not_null_added_column_is_fatal(self) -> None:
        """A NOT NULL column here would break a previous image's INSERTs."""
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(255) NOT NULL DEFAULT ''",
        )

        with pytest.raises(migrations.SchedulerSchemaError):
            migrations.expand_schema(db_engine=engine)

    def test_a_unique_index_on_the_wrong_columns_is_reported_not_fatal(
        self,
    ) -> None:
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(255)",
            "CREATE UNIQUE INDEX uq_scheduled_pipeline_run_created_by_schedule_path ON scheduled_pipeline_run (id)",
        )

        report = migrations.expand_schema(db_engine=engine)

        step = f"index:{migrations._UQ_SCHEDULE_PATH}"
        assert report.status_of(step) is migrations.StepStatus.BLOCKED
        assert "expected columns" in report.detail_of(step)
        assert report.columns_ready and not report.path_writes_ready

    def test_a_same_named_index_that_is_not_unique_is_reported_not_fatal(
        self,
    ) -> None:
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(255)",
            "CREATE INDEX uq_scheduled_pipeline_run_created_by_schedule_path"
            " ON scheduled_pipeline_run (created_by, schedule_path)",
        )

        report = migrations.expand_schema(db_engine=engine)

        step = f"index:{migrations._UQ_SCHEDULE_PATH}"
        assert report.status_of(step) is migrations.StepStatus.BLOCKED
        assert "expected unique=True" in report.detail_of(step)
        assert not report.path_writes_ready

    def test_the_reference_index_must_carry_both_columns(self) -> None:
        """Lookup by pipeline alone must be served without a second index."""
        engine = _legacy_engine()
        _execute(
            engine,
            "CREATE INDEX ix_scheduled_pipeline_run_user_pipeline_id_version_key"
            " ON scheduled_pipeline_run (pipeline_task_spec_from_user_pipeline_id)",
        )

        report = migrations.expand_schema(db_engine=engine)

        assert (
            report.status_of(f"index:{migrations._IX_REFERENCE}")
            is migrations.StepStatus.BLOCKED
        )
        assert not report.indexes_ready

    def test_an_absent_reference_column_is_added_not_refused(self) -> None:
        """Absent is a thing to add; wrong is a thing to refuse.

        `pipeline_task_spec_from_user_pipeline_id` used to be widen-only, so its
        absence was fatal.
        Routing all three columns through one target set makes absence ordinary
        and keeps fatality for definitions that actually conflict.
        """
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run DROP COLUMN pipeline_task_spec_from_user_pipeline_id",
        )

        report = migrations.expand_schema(db_engine=engine)

        assert report.columns_ready
        assert (
            report.status_of("column:pipeline_task_spec_from_user_pipeline_id")
            is migrations.StepStatus.APPLIED
        )

    def test_an_unverifiable_check_body_is_not_reported_as_present(
        self,
    ) -> None:
        shape = migrations.LiveShape(
            exists=True,
            columns={},
            indexes={},
            checks={"ck_scheduled_pipeline_run_source": ""},
            foreign_keys={},
        )

        verdict, detail = migrations._verify_check(shape)

        assert verdict is migrations._Verdict.CONFLICTS
        assert "cannot verify" in detail

    def test_the_reference_foreign_key_is_verified_on_shape_not_on_name(
        self,
    ) -> None:
        """A same-named constraint pointing elsewhere must not read as ready.

        The name is ours, so a mismatch means something installed a constraint
        nobody here wrote, and certifying it would gate reference writes on a
        rule no one has read. Each property is checked separately, because a
        verifier that happens to catch this one case by luck is not a verifier.
        """
        spec = migrations._TARGET_FOREIGN_KEYS[0]

        def shape(**overrides: object) -> migrations.LiveShape:
            live = {
                "constrained_columns": list(spec.columns),
                "referred_table": spec.referred_table,
                "referred_columns": list(spec.referred_columns),
                "options": {},
            }
            live.update(overrides)
            return migrations.LiveShape(
                exists=True,
                columns={},
                indexes={},
                checks={},
                foreign_keys={spec.name: live},
            )

        assert (
            migrations._verify_foreign_key(shape(), spec)[0]
            is migrations._Verdict.MATCHES
        )
        for override, expected in [
            (
                {"constrained_columns": ["pipeline_task_spec_from_pipeline_run_id"]},
                "expected columns",
            ),
            ({"referred_table": "some_other_table"}, "expected to reference"),
            ({"referred_columns": ["user_id"]}, "expected to reference"),
            ({"options": {"ondelete": "CASCADE"}}, "ON DELETE CASCADE"),
            ({"options": {"ondelete": "SET NULL"}}, "ON DELETE SET NULL"),
            ({"options": {"onupdate": "CASCADE"}}, "ON UPDATE CASCADE"),
        ]:
            verdict, detail = migrations._verify_foreign_key(shape(**override), spec)
            assert verdict is migrations._Verdict.CONFLICTS, override
            assert expected in detail, (override, detail)

    def test_an_omitted_restrict_clause_is_not_a_conflict(self) -> None:
        """MySQL's SHOW CREATE TABLE omits the default action, and that is what
        SQLAlchemy reflects.

        Requiring the literal string "RESTRICT" would report our own freshly
        installed constraint as conflicting on the very next boot -- a permanent
        BLOCKED step and a permanently closed write tier, caused by nothing but
        a reflection detail. The property that matters is negative: no action
        that deletes or rewrites a schedule row.
        """
        spec = migrations._TARGET_FOREIGN_KEYS[0]
        for options in [
            {},
            {"ondelete": None},
            {"ondelete": "RESTRICT"},
            {"ondelete": "NO ACTION"},
        ]:
            shape = migrations.LiveShape(
                exists=True,
                columns={},
                indexes={},
                checks={},
                foreign_keys={
                    spec.name: {
                        "constrained_columns": list(spec.columns),
                        "referred_table": spec.referred_table,
                        "referred_columns": list(spec.referred_columns),
                        "options": options,
                    }
                },
            )
            assert (
                migrations._verify_foreign_key(shape, spec)[0]
                is migrations._Verdict.MATCHES
            ), options

    def test_the_version_pair_is_never_given_a_foreign_key(self) -> None:
        """MATCH SIMPLE would skip the common row, which is worse than nothing.

        InnoDB treats a composite foreign key as satisfied whenever any column
        is NULL, and a NULL version key is the ordinary track-current mode. A
        constraint that looks like it covers the pair while skipping most of it
        misleads the next reader.
        """
        constrained = {
            column
            for spec in migrations._TARGET_FOREIGN_KEYS
            for column in spec.columns
        }
        assert constrained == {migrations._PIPELINE_ID_COLUMN}
        assert all(len(spec.columns) == 1 for spec in migrations._TARGET_FOREIGN_KEYS)


class TestOneVerificationPath:
    """Every consumer resolves through the same target set and verifiers."""

    def test_the_lock_timeout_branch_verifies_the_whole_target_set(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The regression that made a loser pod laxer than the winner.

        A legacy table with `pipeline_task_spec_from_user_pipeline_id` still
        VARCHAR(20) and neither
        index present must not read as ready just because the lock was lost.
        """
        engine = _legacy_engine()
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_kwargs: False)

        report = migrations.expand_schema(db_engine=engine)

        assert not report.columns_ready
        assert not report.path_writes_ready
        assert not report.indexes_ready
        # The column the winner would have widened is blocked, not silently ready.
        assert f"column:{migrations._PIPELINE_ID_COLUMN}" in {
            step.name for step in report.blocked
        }
        # Indexes are the operator's job, so absence is reported as not-yet.
        assert (
            report.status_of(f"index:{migrations._UQ_SCHEDULE_PATH}")
            is migrations.StepStatus.SKIPPED
        )

    def test_a_peer_that_finished_the_work_lets_the_loser_proceed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine = _target_engine()
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_kwargs: False)

        report = migrations.expand_schema(db_engine=engine)

        assert report.schema_ready
        assert report.path_writes_ready

    def test_readiness_requires_every_target_object_to_be_present(self) -> None:
        """A report that never reached a step must not read as ready."""
        report = migrations.MigrationReport(dialect="mysql")
        for spec in migrations._TARGET_COLUMNS:
            report.add(f"column:{spec.name}", migrations.StepStatus.APPLIED)

        assert not report.schema_ready

        for spec in migrations._TARGET_INDEXES:
            report.add(f"index:{spec.name}", migrations.StepStatus.APPLIED)

        assert report.schema_ready

    def test_path_writes_need_the_unique_index_specifically(self) -> None:
        """The only thing standing between PR3 and duplicate schedule paths."""
        report = migrations.MigrationReport(dialect="mysql")
        for spec in migrations._TARGET_COLUMNS:
            report.add(f"column:{spec.name}", migrations.StepStatus.APPLIED)
        report.add(
            f"index:{migrations._UQ_SCHEDULE_PATH}",
            migrations.StepStatus.BLOCKED,
            "conflicts",
        )
        report.add(f"index:{migrations._IX_REFERENCE}", migrations.StepStatus.APPLIED)
        report.add(
            f"foreign_key:{migrations._FK_USER_PIPELINE}",
            migrations.StepStatus.APPLIED,
        )

        assert not report.schema_ready
        assert not report.path_writes_ready
        # The reference tier depends on the reference index and the constraint,
        # not on this index.
        assert report.reference_writes_ready

    def test_reference_writes_need_the_reference_index_specifically(
        self,
    ) -> None:
        """The mirror image: the two tiers must not imply each other."""
        report = migrations.MigrationReport(dialect="mysql")
        for spec in migrations._TARGET_COLUMNS:
            report.add(f"column:{spec.name}", migrations.StepStatus.APPLIED)
        for collation_step in migrations._COLLATION_STEPS:
            report.add(collation_step, migrations.StepStatus.APPLIED)
        report.add(
            f"index:{migrations._UQ_SCHEDULE_PATH}",
            migrations.StepStatus.APPLIED,
        )
        report.add(
            f"index:{migrations._IX_REFERENCE}",
            migrations.StepStatus.BLOCKED,
            "conflicts",
        )
        report.add(
            f"foreign_key:{migrations._FK_USER_PIPELINE}",
            migrations.StepStatus.APPLIED,
        )

        assert not report.schema_ready
        assert report.path_writes_ready
        assert not report.reference_writes_ready

    def test_reference_writes_need_the_foreign_key_too(self) -> None:
        """Both indexes present and the constraint missing is not ready.

        The state a size-gated decline or a blocked preflight actually produces,
        and the one a gate reading only the index list would wave through.
        """
        report = migrations.MigrationReport(dialect="mysql")
        for spec in migrations._TARGET_COLUMNS:
            report.add(f"column:{spec.name}", migrations.StepStatus.APPLIED)
        for collation_step in migrations._COLLATION_STEPS:
            report.add(collation_step, migrations.StepStatus.APPLIED)
        report.add(
            f"index:{migrations._UQ_SCHEDULE_PATH}",
            migrations.StepStatus.APPLIED,
        )
        report.add(f"index:{migrations._IX_REFERENCE}", migrations.StepStatus.APPLIED)
        report.add(
            f"foreign_key:{migrations._FK_USER_PIPELINE}",
            migrations.StepStatus.SKIPPED,
            "too big",
        )

        assert report.path_writes_ready
        assert not report.reference_writes_ready
        assert [s.name for s in report.tier_causes("reference_writes")] == [
            f"foreign_key:{migrations._FK_USER_PIPELINE}"
        ]

    def test_not_ready_names_absent_objects_and_blocked_ones(self) -> None:
        """`blocked` alone is empty in the ordinary incomplete state."""
        report = migrations.MigrationReport(dialect="mysql")
        report.add("column:schedule_path", migrations.StepStatus.APPLIED)
        report.add(
            f"index:{migrations._UQ_SCHEDULE_PATH}",
            migrations.StepStatus.SKIPPED,
            "absent",
        )
        report.add(
            f"check:{migrations._CK_SOURCE}",
            migrations.StepStatus.BLOCKED,
            "conflicts",
        )

        assert [s.name for s in report.blocked] == [f"check:{migrations._CK_SOURCE}"]
        assert [s.name for s in report.not_ready] == [
            f"index:{migrations._UQ_SCHEDULE_PATH}",
            f"check:{migrations._CK_SOURCE}",
        ]


class TestStartupWarnsPerTier:
    """A warning that fires for one tier must not silence the other.

    Startup installs the indexes now, so these unready states are produced by
    making the build fail -- which is also the only way they can occur in
    production, and therefore the state the warnings actually have to describe.
    """

    @staticmethod
    def _fail_index_builds(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
        """Make `_create_index` fail, for the named indexes or for all of them."""
        real_create = migrations._create_index

        def _create(*, conn: object, dialect: str, spec: object) -> None:
            if not names or spec.name in names:  # type: ignore[attr-defined]
                raise sqlalchemy.exc.OperationalError(
                    "ALTER", {}, Exception("lock wait timeout exceeded")
                )
            real_create(conn=conn, dialect=dialect, spec=spec)  # type: ignore[arg-type]

        monkeypatch.setattr(migrations, "_create_index", _create)

    @staticmethod
    def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
        return [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]

    @classmethod
    def _warning_text(cls, caplog: pytest.LogCaptureFixture) -> str:
        return "\n".join(cls._warnings(caplog))

    @classmethod
    def _tier_warning(cls, caplog: pytest.LogCaptureFixture) -> str:
        return next(m for m in cls._warnings(caplog) if "not yet safe to enable" in m)

    def test_both_unready_tiers_are_named(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        self._fail_index_builds(monkeypatch)

        with caplog.at_level(logging.WARNING, logger=migrations.__name__):
            report = migrations.migrate_db(db_engine=engine)

        assert not report.path_writes_ready
        assert not report.reference_writes_ready
        text = self._warning_text(caplog)
        assert "path_writes" in text
        assert "reference_writes" in text

    def test_a_ready_reference_tier_does_not_silence_the_path_warning(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The asymmetric state the previous combined condition swallowed.

        Produced by failing one named build rather than by monkeypatching the
        readiness property. That distinction matters: overriding the property
        proves the warning reads it, whereas failing the build proves the state
        is reachable at all -- and this state is the one a real partial install
        lands in.
        """
        engine = _legacy_engine()
        self._fail_index_builds(monkeypatch, migrations._UQ_SCHEDULE_PATH)

        with caplog.at_level(logging.WARNING, logger=migrations.__name__):
            report = migrations.migrate_db(db_engine=engine)

        assert not report.path_writes_ready
        assert report.reference_writes_ready  # genuinely installed, not faked
        text = self._warning_text(caplog)
        assert "path_writes" in text
        assert "reference_writes" not in text

    def test_the_warning_identifies_the_objects_not_an_empty_blocked_list(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The warning has to name the objects, whatever bucket they landed in.

        This test predates startup installing the indexes: back then they were
        SKIPPED and `blocked` was empty, and the warning printed that empty
        list. Now a failed build is BLOCKED, so the list is populated -- but the
        assertion that matters is unchanged, because it was never about the
        bucket. It is that the tier index names appear.

        The owner-page index is blocked here too and deliberately absent from the
        warning text: it belongs to no tier, and a per-tier warning that named it
        would be reporting a cause of nothing.
        """
        engine = _legacy_engine()
        self._fail_index_builds(monkeypatch)

        with caplog.at_level(logging.WARNING, logger=migrations.__name__):
            report = migrations.migrate_db(db_engine=engine)

        assert [step.name for step in report.blocked] == [
            f"index:{migrations._UQ_SCHEDULE_PATH}",
            f"index:{migrations._IX_REFERENCE}",
            f"index:{migrations._IX_OWNER_PAGE}",
        ]
        text = self._warning_text(caplog)
        assert migrations._UQ_SCHEDULE_PATH in text
        assert migrations._IX_REFERENCE in text
        assert migrations._IX_OWNER_PAGE not in text

    def test_the_tier_warning_names_only_causes_of_those_tiers(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The CHECK gates nothing, so listing it invites the wrong conclusion."""
        engine = _legacy_engine()
        self._fail_index_builds(monkeypatch)

        with caplog.at_level(logging.WARNING, logger=migrations.__name__):
            report = migrations.migrate_db(db_engine=engine)

        assert not report.check_ready  # absent, and therefore in `not_ready`
        assert f"check:{migrations._CK_SOURCE}" in {s.name for s in report.not_ready}
        # ...but not offered as a cause of a write tier.
        assert "check:" not in self._tier_warning(caplog)
        # The foreign key IS a cause -- of one tier. A path write touches no
        # pipeline reference, so naming it there would send an operator to fix
        # an object that has nothing to do with the closed feature.
        assert not [
            s
            for s in report.tier_causes("path_writes")
            if s.name.startswith("foreign_key:")
        ]
        assert [
            s
            for s in report.tier_causes("reference_writes")
            if s.name.startswith("foreign_key:")
        ]

    def test_each_tier_is_attributed_to_its_own_index(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        self._fail_index_builds(monkeypatch)

        with caplog.at_level(logging.WARNING, logger=migrations.__name__):
            report = migrations.migrate_db(db_engine=engine)

        assert [s.name for s in report.tier_causes("path_writes")] == [
            f"index:{migrations._UQ_SCHEDULE_PATH}"
        ]
        # The reference tier depends on its index AND the foreign key, and the
        # index failing is why the constraint was never attempted.
        assert [s.name for s in report.tier_causes("reference_writes")] == [
            f"index:{migrations._IX_REFERENCE}",
            f"foreign_key:{migrations._FK_USER_PIPELINE}",
        ]

    def test_a_fully_ready_schema_warns_about_nothing(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        engine = _target_engine()

        with caplog.at_level(logging.WARNING, logger=migrations.__name__):
            report = migrations.migrate_db(db_engine=engine)

        assert report.path_writes_ready
        assert report.reference_writes_ready
        assert self._warning_text(caplog) == ""


class TestAbsencePolicyIsExhaustive:
    """A step kind with no policy is a bug here, not a schema outcome."""

    def test_an_unknown_step_kind_raises_instead_of_defaulting(self) -> None:
        report = migrations.MigrationReport(dialect="mysql")

        with pytest.raises(migrations.SchedulerSchemaError, match="no absence policy"):
            migrations.record_verification(
                report=report,
                results=[("trigger:whatever", migrations._Verdict.ABSENT, "")],
            )

    def test_a_lenient_default_would_have_been_invisible(self) -> None:
        """Why it raises: an unknown prefix belongs to no tier.

        A defaulted status would leave startup running and every write gate
        reading true, so the missing policy could never be noticed.
        """
        report = migrations.MigrationReport(dialect="mysql")
        # The real tier, not a hand-assembled approximation of it: a step the tier
        # gained would otherwise be missing here and the assertion would fail for a
        # reason that has nothing to do with the unknown kind under test.
        for name in migrations._WRITE_TIER_STEPS["path_writes"]():
            report.add(name, migrations.StepStatus.APPLIED)
        report.add("trigger:whatever", migrations.StepStatus.BLOCKED, "unknown kind")

        assert report.path_writes_ready

    def test_every_declared_step_kind_has_a_policy(self) -> None:
        shape = migrations.LiveShape(
            exists=True, columns={}, indexes={}, checks={}, foreign_keys={}
        )
        steps = [
            step
            for step, *_ in migrations.verify_schema(shape)
            + migrations.verify_hardening_objects(shape)
        ]

        assert steps
        for step in steps:
            assert migrations._absence_policy(step) in migrations.StepStatus


class TestUnexpectedReferenceForeignKey:
    """One expected constraint; any other on these columns is unexplained.

    Narrowed rather than deleted when the foreign key became a target object.
    The risk it was written for is unchanged: a constraint this application did
    not install is invisible to every other verifier here, and an
    `ON DELETE CASCADE` one deletes a customer's schedule as a side effect of
    deleting a pipeline.
    """

    @staticmethod
    def _engine_with(constraint: str) -> sqlalchemy.Engine:
        engine = _empty_engine()
        _execute(
            engine,
            "CREATE TABLE pipeline (id VARCHAR(36) NOT NULL, PRIMARY KEY (id))",
            _LEGACY_SCHEDULE_TABLE_DDL.replace(
                "PRIMARY KEY (id)", f"PRIMARY KEY (id), {constraint}"
            ),
        )
        return engine

    @classmethod
    def _engine_with_cascade_fk(cls) -> sqlalchemy.Engine:
        """Our own name, our own column -- and a delete rule we never write."""
        return cls._engine_with(
            "CONSTRAINT fk_scheduled_pipeline_run_user_pipeline_id"
            " FOREIGN KEY (pipeline_task_spec_from_user_pipeline_id)"
            " REFERENCES pipeline (id) ON DELETE CASCADE"
        )

    def test_a_cascade_under_our_own_name_is_a_conflict_not_a_match(
        self,
    ) -> None:
        """The name matching is exactly why this has to be verified on shape.

        A name-only check would report this ready and open reference writes over
        a constraint that deletes schedules.
        """
        report = migrations.expand_schema(db_engine=self._engine_with_cascade_fk())

        step = f"foreign_key:{migrations._FK_USER_PIPELINE}"
        assert report.status_of(step) is migrations.StepStatus.BLOCKED
        assert "CASCADE" in report.detail_of(step)
        assert not report.reference_writes_ready

    def test_it_is_reported_but_never_dropped_and_never_fatal(self) -> None:
        """Dropping a constraint nobody can explain is an operator decision."""
        engine = self._engine_with_cascade_fk()

        report = migrations.migrate_db(db_engine=engine)

        assert report.columns_ready
        after = sqlalchemy.inspect(engine).get_foreign_keys("scheduled_pipeline_run")
        assert [fk["name"] for fk in after] == [
            "fk_scheduled_pipeline_run_user_pipeline_id"
        ]

    def test_a_second_constraint_under_another_name_is_reported_as_unexpected(
        self,
    ) -> None:
        """This application installs exactly one, so a second one is someone's
        hand-written idea and nothing here can vouch for it."""
        engine = self._engine_with(
            "CONSTRAINT fk_someone_elses_idea"
            " FOREIGN KEY (pipeline_task_spec_from_user_pipeline_id)"
            " REFERENCES pipeline (id) ON DELETE CASCADE"
        )

        report = migrations.expand_schema(db_engine=engine)

        step = "foreign_key:unexpected:fk_someone_elses_idea"
        assert report.status_of(step) is migrations.StepStatus.BLOCKED
        assert "CASCADE" in report.detail_of(step)
        assert migrations._PIPELINE_ID_COLUMN in report.detail_of(step)

    def test_an_unnamed_constraint_is_reported_and_not_destroyed_by_the_rebuild(
        self,
    ) -> None:
        """SQLite does not require a symbol; MySQL always assigns one.

        Reflection used to discard unnamed foreign keys, which would have made
        this report claim an exhaustiveness it did not have. Worse, the SQLite
        install path *rebuilds* the table, and reflection cannot round-trip an
        unnamed constraint -- SQLAlchemy parses it from the DDL and then fails
        to match it in PRAGMA, so the recreated table loses it. Adding our
        constraint must not silently delete someone else's.
        """
        engine = self._engine_with(
            "FOREIGN KEY (pipeline_task_spec_from_user_pipeline_id) REFERENCES pipeline (id)"
        )

        report = migrations.expand_schema(db_engine=engine)

        unexpected = [
            s for s in report.steps if s.name.startswith("foreign_key:unexpected:")
        ]
        assert len(unexpected) == 1
        assert "unnamed" in unexpected[0].name
        # Declined rather than attempted, and the constraint is still there.
        expected_step = report.status_of(f"foreign_key:{migrations._FK_USER_PIPELINE}")
        assert expected_step is migrations.StepStatus.SKIPPED
        assert "would\n" not in report.detail_of(
            f"foreign_key:{migrations._FK_USER_PIPELINE}"
        )
        assert "drop it" in report.detail_of(
            f"foreign_key:{migrations._FK_USER_PIPELINE}"
        )
        assert not report.reference_writes_ready
        assert (
            len(sqlalchemy.inspect(engine).get_foreign_keys("scheduled_pipeline_run"))
            == 1
        )

    def test_the_expected_constraint_is_not_reported_as_unexpected(
        self,
    ) -> None:
        """The legacy pipeline_run FK is out of scope, and ours is expected."""
        report = migrations.expand_schema(db_engine=_target_engine())

        assert [
            s.name for s in report.steps if s.name.startswith("foreign_key:unexpected:")
        ] == []
        assert report.status_of(f"foreign_key:{migrations._FK_USER_PIPELINE}") is (
            migrations.StepStatus.ALREADY_PRESENT
        )


class TestSqliteHardeningIsVerifiedNotNamed:
    """The name-only shortcut accepted a constraint that enforced nothing."""

    def test_a_same_named_check_with_a_different_body_is_not_ready(
        self,
    ) -> None:
        engine = _empty_engine()
        _execute(
            engine,
            _LEGACY_SCHEDULE_TABLE_DDL.replace(
                "PRIMARY KEY (id)",
                "PRIMARY KEY (id), CONSTRAINT ck_scheduled_pipeline_run_source CHECK (1 = 1)",
            ),
        )

        report = migrations.expand_schema(db_engine=engine)

        step = f"check:{migrations._CK_SOURCE}"
        assert report.status_of(step) is migrations.StepStatus.BLOCKED
        assert not report.check_ready
        assert "different check body" in report.detail_of(step)

    def test_a_fresh_sqlite_database_passes_on_exact_reflection(self) -> None:
        """Not by dialect exemption: SQLite really does prove both definitions."""
        engine = _target_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert report.check_ready
        assert (
            report.status_of(f"check:{migrations._CK_SOURCE}")
            is migrations.StepStatus.ALREADY_PRESENT
        )

    def test_an_absent_constraint_is_skipped_and_not_ready(self) -> None:
        engine = _legacy_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert (
            report.status_of(f"check:{migrations._CK_SOURCE}")
            is migrations.StepStatus.SKIPPED
        )
        assert not report.check_ready
        # And the tiers are unaffected by it: an existing table may legitimately
        # never acquire the CHECK, so no gate may depend on it. The indexes are
        # installed on this same boot, so both tiers are open regardless.
        assert report.reference_writes_ready
        assert report.path_writes_ready


class TestCheckBodyComparison:
    """Grouping is structural: normalization must not erase it."""

    def test_different_grouping_is_not_treated_as_equal(self) -> None:
        strict = "count = 1 AND (vkey IS NULL OR pid IS NOT NULL)"
        weakened = "(count = 1 AND vkey IS NULL) OR pid IS NOT NULL"

        assert migrations._normalize_sql(strict) != migrations._normalize_sql(weakened)

    def test_whitespace_and_identifier_quoting_are_still_ignored(self) -> None:
        assert migrations._normalize_sql(
            "a  AND\n  (`b` OR c)"
        ) == migrations._normalize_sql('A AND ("b" OR C)')

    def test_a_redundant_outer_wrapper_is_ignored(self) -> None:
        """MySQL reflects the body wrapped in parens; SQLite does not."""
        assert migrations._normalize_sql("(a AND b)") == migrations._normalize_sql(
            "a AND b"
        )

    def test_a_leading_group_is_not_mistaken_for_a_wrapper(self) -> None:
        assert migrations._normalize_sql("(a OR b) AND (c OR d)") == "(aorb)and(cord)"


class TestFailedDdlRecovery:
    def test_a_failed_statement_is_rolled_back_before_reinspection(
        self,
    ) -> None:
        """Re-inspection is the trust mechanism, so it must be able to run.

        Honest limitation: SQLite does not reproduce the failure this guards
        against. I checked — reflection still works on a connection with a failed
        statement behind it there, so the aborted-transaction state that makes
        the reflection queries themselves fail is MySQL/PostgreSQL-shaped and not
        reachable in this suite. What is testable without a server is that the
        rollback is actually issued between the failure and the re-inspection,
        which is what the fix consists of.
        """
        engine = _legacy_engine()
        rollbacks: list[str] = []

        with engine.connect() as conn:
            report = migrations.MigrationReport(dialect="sqlite")
            original_rollback = conn.rollback

            def _record_rollback() -> None:
                rollbacks.append("rollback")
                original_rollback()

            conn.rollback = _record_rollback  # type: ignore[method-assign]

            def _failing_emit() -> None:
                # A real failed statement, not a bare raise: this is the shape
                # that leaves a transaction behind on a server that has them.
                conn.execute(
                    sqlalchemy.text(
                        "INSERT INTO scheduled_pipeline_run (id) VALUES ('x')"
                    )
                )

            migrations._apply(
                conn=conn,
                report=report,
                step="index:probe",
                verify=lambda shape: (
                    (migrations._Verdict.MATCHES, "")
                    if "probe" in shape.indexes
                    else (migrations._Verdict.ABSENT, "")
                ),
                emit=_failing_emit,
                required=False,
            )

        assert rollbacks == ["rollback"]
        # Reached the re-inspection and reported the driver error, rather than
        # dying inside the reflection call.
        assert report.status_of("index:probe") is migrations.StepStatus.BLOCKED
        assert "NOT NULL" in report.detail_of("index:probe")

    def test_a_peer_that_won_the_race_is_reconciled_not_reported(self) -> None:
        engine = _legacy_engine()

        with engine.connect() as conn:
            report = migrations.MigrationReport(dialect="sqlite")

            def _emit_then_fail() -> None:
                conn.execute(
                    sqlalchemy.text(
                        "CREATE INDEX probe ON scheduled_pipeline_run (created_by)"
                    )
                )
                conn.commit()
                raise RuntimeError("duplicate object")

            migrations._apply(
                conn=conn,
                report=report,
                step="index:probe",
                verify=lambda shape: (
                    (migrations._Verdict.MATCHES, "")
                    if "probe" in shape.indexes
                    else (migrations._Verdict.ABSENT, "")
                ),
                emit=_emit_then_fail,
                required=False,
            )

        assert report.status_of("index:probe") is migrations.StepStatus.ALREADY_PRESENT


class TestFatality:
    """Finding 1: booting with unreachable mapped columns only defers failure."""

    def test_migrate_db_raises_when_columns_cannot_be_reached(self) -> None:
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path INTEGER",
        )

        with pytest.raises(migrations.SchedulerSchemaError):
            migrations.migrate_db(db_engine=engine)

    def test_migrate_db_returns_when_only_hardening_is_incomplete(self) -> None:
        """Inline schedules stay safe to read and write, so this is not fatal."""
        engine = _legacy_engine()
        _insert_legacy_row(engine)

        report = migrations.migrate_db(db_engine=engine)

        assert report.columns_ready
        assert report.indexes_ready
        # The CHECK is the only object startup cannot install on an existing
        # table, and it gates nothing. Boot proceeds.
        assert not report.check_ready
        assert report.schema_ready

    def test_readiness_is_reported_per_tier_not_as_one_verdict(self) -> None:
        engine = _legacy_engine()

        summary = migrations.expand_schema(db_engine=engine).summary()

        assert summary["columns_ready"] is True
        assert summary["indexes_ready"] is True
        assert summary["schema_ready"] is True
        assert summary["path_writes_ready"] is True
        assert summary["reference_writes_ready"] is True
        # Reported, never gating: an existing table may never acquire it.
        assert summary["check_ready"] is False
        assert "fk_ready" not in summary


class TestMySqlDdl:
    """MySQL statements asserted by emission/compilation; no server available.

    Precedent: tests/user_pipelines/test_user_pipeline_db_models.py compiles
    CreateTable against the MySQL dialect for the same reason.
    """

    def test_create_table_carries_every_object_the_migration_adds(self) -> None:
        ddl = str(
            CreateTable(db_models.ScheduledPipelineRun.__table__).compile(
                dialect=mysql.dialect()
            )
        )

        assert "uq_scheduled_pipeline_run_created_by_schedule_path" in ddl
        assert "ck_scheduled_pipeline_run_source" in ddl
        # The reference FK, on a fresh database and on a migrated one. A fresh
        # database that enforced a rule production could not would be the same
        # divergence in the other direction.
        assert "fk_scheduled_pipeline_run_user_pipeline_id" in ddl
        assert "REFERENCES pipeline (id)" in ddl
        assert "ON DELETE RESTRICT" in ddl
        assert (
            "ix_scheduled_pipeline_run_user_pipeline_id_version_key" not in ddl
        )  # separate CREATE INDEX
        # Fresh and migrated databases cannot disagree about a mode column,
        # because there is not one.
        assert "resolution_mode" not in ddl

    def test_the_model_and_the_migration_agree_on_every_index_shape(
        self,
    ) -> None:
        """A fresh database and a migrated one must get the SAME index.

        `create_all` builds what the model declares; this module builds what the
        target set declares; and `_verify_index` certifies readiness by comparing
        the live shape to the target set. Let the two drift and a fresh database
        is reported CONFLICTS against its own index, or -- worse in the other
        direction -- production quietly gets a different key order from the one
        the query was designed around, which is a difference no status code shows.

        Column ORDER is compared, not membership: for the owner-page index the
        order is the entire point.
        """
        table = db_models.ScheduledPipelineRun.__table__
        declared = {
            index.name: [column.name for column in index.columns]
            for index in table.indexes
        }
        declared.update(
            {
                constraint.name: [column.name for column in constraint.columns]
                for constraint in table.constraints
                if isinstance(constraint, sqlalchemy.UniqueConstraint)
            }
        )

        for spec in migrations._TARGET_INDEXES:
            assert (
                spec.name in declared
            ), f"{spec.name} is installed by the migration but not declared on the model"
            assert declared[spec.name] == list(spec.columns), spec.name

    def test_ddl_bounds_its_metadata_lock_wait_before_altering(self) -> None:
        """INSTANT describes the rebuild, not the metadata lock.

        A long-running transaction holding a shared MDL makes the ALTER wait,
        and MySQL queues later DML behind the ALTER's pending exclusive
        request — so an "instant" DDL can stall every schedule fire's
        `last_run_at` write. A bounded wait makes that a fast, loud failure.
        """
        conn = _RecordingConnection(scalar_results=[50])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            pass

        assert conn.statements[0] == "SELECT @@SESSION.lock_wait_timeout"
        assert (
            conn.statements[1]
            == f"SET SESSION lock_wait_timeout = {migrations._MYSQL_LOCK_WAIT_TIMEOUT_SECONDS}"
        )

    def test_it_restores_the_previous_value_and_then_recycles_the_connection(
        self,
    ) -> None:
        """This connection comes from the shared engine; it must not go back.

        Two separate leaks. Returning it resets the transaction but not session
        variables, so a leaked 3s bound would be inherited by ordinary API and
        scheduler traffic and make *their* statements fail under unrelated
        metadata-lock contention. And the statements that install the bound name
        a session variable, which ProxySQL may take as a reason to stop
        multiplexing this frontend connection for good — something restoring the
        value cannot undo (pi-41). So the value is put back AND the connection
        is physically discarded.
        """
        conn = _RecordingConnection(scalar_results=[50])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            pass

        assert conn.statements[-1] == "SET SESSION lock_wait_timeout = 50"
        assert (
            conn.invalidated
        ), "a session-variable connection was returned to the pool"

    def test_the_restore_survives_a_failed_migration(self) -> None:
        """A raise inside the block must not leak the bound either."""
        conn = _RecordingConnection(scalar_results=[50])

        with pytest.raises(RuntimeError):
            with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
                raise RuntimeError("DDL blew up")

        assert conn.statements[-1] == "SET SESSION lock_wait_timeout = 50"
        assert conn.invalidated

    def test_a_missing_previous_value_leaves_the_discard_as_the_only_cleanup(
        self,
    ) -> None:
        """No value to read back means no restore, so the bound leaves with the session.

        The bound IS set -- the read still has to be protected -- and the 3s
        value is then unrestorable, which would be a silent leak on any code
        path that pooled this connection (pi-41). It is not one here only
        because the connection is discarded regardless. MySQL does not return
        NULL for this variable; the branch exists so that assumption failing is
        not also a leak.
        """
        conn = _RecordingConnection(scalar_results=[None])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            pass

        assert conn.statements == [
            "SELECT @@SESSION.lock_wait_timeout",
            "SET SESSION lock_wait_timeout = 3",
        ]
        assert conn.invalidated

    def test_a_failed_restore_does_not_stop_the_recycle(self) -> None:
        """The discard is the guarantee; the restore is only depth behind it."""

        class _RestoreFails(_RecordingConnection):
            def execute(
                self, statement: object, parameters: object = None
            ) -> _RecordingConnection:
                if str(statement) == "SET SESSION lock_wait_timeout = 50":
                    raise RuntimeError("connection went away")
                return super().execute(statement, parameters)

        conn = _RestoreFails(scalar_results=[50])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            pass

        assert (
            conn.invalidated
        ), "a connection that kept the 3s bound was returned to the pool"

    def test_the_restore_happens_before_the_discard(self) -> None:
        """Ordering, not just occurrence: the restore needs a live connection.

        Asserting only that the connection ended up invalidated passes with the
        discard moved ahead of the restore, which would issue the restore on a
        connection that no longer exists -- the bound would then be the last
        thing that ever happened to that session (pi-40).
        """
        conn = _RecordingConnection(scalar_results=[50])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            pass

        assert conn.events == [
            "SELECT @@SESSION.lock_wait_timeout",
            f"SET SESSION lock_wait_timeout = {migrations._MYSQL_LOCK_WAIT_TIMEOUT_SECONDS}",
            "SET SESSION lock_wait_timeout = 50",
            "<invalidate>",
        ]

    def test_a_connection_that_dies_reading_the_previous_value_is_still_recycled(
        self,
    ) -> None:
        """Setup lives inside the `try`, so a failed read cannot skip the recycle.

        The read is `SELECT @@SESSION...`, which is itself a session-variable
        statement: by the time it fails, ProxySQL has already seen it. Leaving
        the setup outside the `try` would return exactly that connection to the
        pool (pi-40).
        """

        class _ReadFails(_RecordingConnection):
            def execute(
                self, statement: object, parameters: object = None
            ) -> _RecordingConnection:
                if "@@SESSION" in str(statement):
                    raise RuntimeError("connection died on the read")
                return super().execute(statement, parameters)

        conn = _ReadFails()

        with pytest.raises(RuntimeError, match="connection died on the read"):
            with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
                pytest.fail("the body must not run when the bound was never installed")

        assert (
            conn.invalidated
        ), "a connection that ran a session read went back to the pool"

    def test_a_connection_that_dies_setting_the_bound_is_still_recycled(
        self,
    ) -> None:
        """The half-applied case: the SET may have landed before the failure."""

        class _SetFails(_RecordingConnection):
            def execute(
                self, statement: object, parameters: object = None
            ) -> _RecordingConnection:
                if str(statement).startswith("SET SESSION"):
                    raise RuntimeError("connection died on the set")
                return super().execute(statement, parameters)

        conn = _SetFails(scalar_results=[50])

        with pytest.raises(RuntimeError, match="connection died on the set"):
            with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
                pytest.fail("the body must not run when the bound was never installed")

        assert conn.invalidated

    def test_an_unrecyclable_connection_raises_rather_than_pooling_silently(
        self,
    ) -> None:
        """`_discard_connection` fails closed, and this must not soften that."""

        class _Unrecyclable(_RecordingConnection):
            def invalidate(self) -> None:
                raise RuntimeError("no")

            def detach(self) -> None:
                raise RuntimeError("nor this")

        conn = _Unrecyclable(scalar_results=[50])

        with pytest.raises(
            migrations.SchedulerSchemaError, match="cannot be proven unpinned"
        ):
            with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
                pass

    def test_the_wait_is_bounded_before_any_ddl_is_emitted(self) -> None:
        """Order matters: a bound applied after the ALTER protects nothing."""
        conn = _RecordingConnection(scalar_results=[50])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            migrations._add_column(
                conn=conn, dialect="mysql", spec=migrations._TARGET_COLUMNS[1]
            )

        assert "lock_wait_timeout" in conn.statements[1]
        assert "ADD COLUMN" in conn.statements[2]

    def test_no_lock_wait_statement_on_other_dialects(self) -> None:
        conn = _RecordingConnection()

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="sqlite"):
            pass

        assert conn.statements == []
        # And no recycle either: the discard pays for session statements that
        # were never issued here, so charging it would churn the pool for free.
        assert not conn.invalidated

    def test_no_ddl_at_all_is_emitted_on_an_unreviewed_dialect(self) -> None:
        """The per-statement guards were not enough: `_add_column` fell through.

        `_widen` refused unknown dialects while `_add_column` still emitted a
        generic ALTER TABLE ADD COLUMN. On PostgreSQL even a metadata-only ADD
        takes ACCESS EXCLUSIVE, and the `lock_wait_timeout` bound is a no-op
        there, so nothing bounded a wait that queues the per-fire
        `last_run_at` write behind it. One guard now runs before any emitter.
        """
        conn = _RecordingConnection()
        report = migrations.MigrationReport(dialect="postgresql")

        with pytest.raises(
            migrations.SchedulerSchemaError, match="refusing to emit schema DDL"
        ):
            migrations._expand(conn=conn, report=report)

        assert conn.statements == []

    def test_an_add_column_on_an_unreviewed_dialect_is_refused_too(
        self,
    ) -> None:
        """Belt and braces: the emitter no longer falls through on its own."""
        conn = _RecordingConnection()

        with pytest.raises(migrations.SchedulerSchemaError, match="refusing to add"):
            migrations._add_column(
                conn=conn,
                dialect="postgresql",
                spec=migrations._TARGET_COLUMNS[1],
            )

        assert conn.statements == []

    def test_both_reviewed_dialects_are_named_once(self) -> None:
        assert migrations._DDL_DIALECTS == frozenset({"mysql", "sqlite"})

    def test_index_ddl_states_an_explicit_algorithm_and_lock(self) -> None:
        """LOCK=NONE is what makes this safe on the boot path.

        It is what permits concurrent DML during the build, so the per-fire
        `last_run_at` write keeps working. A silent COPY fallback would not.
        """
        conn = _RecordingConnection()

        for spec in migrations._TARGET_INDEXES:
            migrations._create_index(conn=conn, dialect="mysql", spec=spec)

        assert conn.statements == [
            f"ALTER TABLE {migrations._SCHEDULE_TABLE} ADD UNIQUE INDEX {migrations._UQ_SCHEDULE_PATH}"
            f" (created_by, schedule_path), {migrations._MYSQL_INPLACE}",
            f"ALTER TABLE {migrations._SCHEDULE_TABLE} ADD INDEX {migrations._IX_REFERENCE}"
            f" ({migrations._PIPELINE_ID_COLUMN}, {migrations._VERSION_KEY_COLUMN}), {migrations._MYSQL_INPLACE}",
            f"ALTER TABLE {migrations._SCHEDULE_TABLE} ADD INDEX {migrations._IX_OWNER_PAGE}"
            f" (created_by, updated_at, id), {migrations._MYSQL_INPLACE}",
        ]

    def test_every_copying_statement_is_bounded_by_the_copy_limit(self) -> None:
        """Every statement that copies the table must be size-gated.

        `ALGORITHM=COPY` blocks DML for its duration, so every other statement
        here stays INSTANT or INPLACE. Two EMITTERS cannot: a *validated* foreign
        key add is COPY on MySQL (the alternative is `foreign_key_checks=OFF`,
        which produces a constraint the optimizer trusts over data nobody
        checked), and a collation change rebuilds the column and every index over
        it.

        Two emitters and, today, up to two statements: the collation emitter runs
        once per target column and `schedule_path` is the only target, so a
        legacy table takes the path conversion and the foreign key in a single
        boot. It was briefly three, while `created_by` was also converted. This
        test walks EMITTERS rather than counting statements, so it covered that
        third statement without being rewritten and covers its removal the same
        way -- which is the property worth keeping, since the count is the thing
        that keeps changing.

        The previous version of this test asserted the FK was the ONLY copying
        statement, and stayed green when the collation change was added -- because
        it counted the literal `ALGORITHM=COPY`, which appears once, in the
        constant. Both emitters interpolate `_MYSQL_COPY`, so the literal count
        could never grow. It is parsed now: the emitters are found by walking for
        uses of the constant, and each one's expander is proved to consult
        `_MAX_FK_COPY_ROWS`. Adding a third copying statement without a gate fails
        here instead of discovering itself in production.
        """
        tree = ast.parse(pathlib.Path(migrations.__file__).read_text())
        functions = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        }

        def _names(function: ast.AST) -> set[str]:
            return {
                node.id for node in ast.walk(function) if isinstance(node, ast.Name)
            }

        def _reaches(entry: str, name: str) -> bool:
            """Is the limit consulted anywhere the expander can reach?

            Transitive, because the foreign-key gate lives one hop away in
            `_foreign_key_preflight` and a direct-use check would report it
            ungated. Only module-local functions are followed, so this stays a
            statement about this module rather than the whole import graph.
            """
            seen: set[str] = set()
            pending = [entry]
            while pending:
                current = pending.pop()
                if current in seen or current not in functions:
                    continue
                seen.add(current)
                used = _names(functions[current])
                if name in used:
                    return True
                pending.extend(used & functions.keys())
            return False

        emitters = {
            name for name, node in functions.items() if "_MYSQL_COPY" in _names(node)
        }
        assert emitters == {"_create_foreign_key", "_apply_collation"}

        for expander in ("_expand_foreign_keys", "_expand_collation"):
            assert _reaches(
                expander, "_MAX_FK_COPY_ROWS"
            ), f"{expander} emits a table copy without consulting the copy-size limit"

        assert migrations._MYSQL_COPY == "ALGORITHM=COPY, LOCK=SHARED"
        code = _code_without_docstrings(migrations.__file__)
        assert "foreign_key_checks" not in "\n".join(
            line.split("#")[0] for line in code.splitlines()
        )
        assert migrations._MAX_FK_COPY_ROWS < migrations._MAX_INDEX_BUILD_ROWS

    def test_columns_are_applied_before_indexes(self) -> None:
        """An index over a column that does not exist yet cannot be built."""
        engine = _legacy_engine()

        report = migrations.expand_schema(db_engine=engine)

        kinds = [step.name.split(":")[0] for step in report.steps]
        assert kinds.index("index") > max(
            index for index, kind in enumerate(kinds) if kind == "column"
        )

    def test_an_index_on_an_unreviewed_dialect_is_refused(self) -> None:
        conn = _RecordingConnection()

        with pytest.raises(migrations.SchedulerSchemaError, match="refusing to create"):
            migrations._create_index(
                conn=conn,
                dialect="postgresql",
                spec=migrations._TARGET_INDEXES[0],
            )

        assert conn.statements == []

    def test_the_index_build_inherits_the_bounded_lock_wait(self) -> None:
        """The bound is set once on the connection, so the build is covered.

        Asserted through statement order rather than by re-reading the variable,
        because the bound is a property of the session the DDL runs in.
        """
        conn = _RecordingConnection(scalar_results=[50])

        with migrations._bounded_metadata_lock_wait(conn=conn, dialect="mysql"):
            migrations._create_index(
                conn=conn, dialect="mysql", spec=migrations._TARGET_INDEXES[0]
            )

        assert (
            conn.statements[1]
            == f"SET SESSION lock_wait_timeout = {migrations._MYSQL_LOCK_WAIT_TIMEOUT_SECONDS}"
        )
        assert "ADD UNIQUE INDEX" in conn.statements[2]
        assert conn.statements[3] == "SET SESSION lock_wait_timeout = 50"
        # Nothing is emitted after the restore: a statement issued past the
        # discard would run on a fresh connection, outside the bound entirely.
        assert len(conn.statements) == 4

    def test_a_widen_on_an_unreviewed_dialect_is_refused(self) -> None:
        """No fall-through to whatever Alembic emits by default.

        On PostgreSQL an ALTER TYPE rewrites the table under ACCESS EXCLUSIVE,
        which would block the per-fire `last_run_at` write. `create_db_engine`
        accepts any URI, so this is about what the process might be handed.
        """
        conn = _RecordingConnection()

        with pytest.raises(migrations.SchedulerSchemaError, match="refusing to widen"):
            migrations._widen(
                conn=conn,
                dialect="postgresql",
                spec=migrations._TARGET_COLUMNS[0],
                live={"type": sqlalchemy.String(20)},
            )

        assert conn.statements == []

    def test_the_sqlite_widen_is_a_table_rewrite_and_says_so(self) -> None:
        """The documented SQLite rebuild requires a small, quiescent database."""
        source = pathlib.Path(migrations.__file__).read_text()
        widen = source.split("def _widen(")[1].split("\ndef ")[0]

        assert "RECREATES the table" in widen
        assert "without concurrent scheduler traffic" in widen

    def test_added_columns_state_an_explicit_algorithm(self) -> None:
        conn = _RecordingConnection()

        for spec in migrations._TARGET_COLUMNS[1:]:
            migrations._add_column(conn=conn, dialect="mysql", spec=spec)

        assert len(conn.statements) == len(migrations._TARGET_COLUMNS) - 1
        for statement in conn.statements:
            assert "ADD COLUMN" in statement
            # Stated so the server errors instead of silently falling back to a
            # blocking table copy.
            assert migrations._MYSQL_INSTANT in statement
            assert " NULL" in statement

    def test_the_reference_column_is_widened_in_place_to_hold_a_uuid(
        self,
    ) -> None:
        conn = _RecordingConnection()

        migrations._widen(
            conn=conn,
            dialect="mysql",
            spec=migrations._TARGET_COLUMNS[0],
            live={"type": mysql.VARCHAR(20)},
        )

        statement = conn.statements[0]
        assert "MODIFY COLUMN" in statement
        assert "VARCHAR(36) NULL" in statement
        assert migrations._MYSQL_INPLACE in statement

    def test_the_advisory_lock_is_taken_with_a_timeout(self) -> None:
        conn = _RecordingConnection()

        assert migrations._acquire_lock(conn=conn, dialect="mysql") is True
        assert "GET_LOCK" in conn.statements[0]

    def test_no_lock_statements_on_sqlite(self) -> None:
        conn = _RecordingConnection()

        assert migrations._acquire_lock(conn=conn, dialect="sqlite") is True
        migrations._release_lock(conn=conn, dialect="sqlite")

        assert conn.statements == []

    def test_the_lock_is_released_even_when_a_step_is_fatal(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        released: list[str] = []
        monkeypatch.setattr(
            migrations,
            "_release_lock",
            lambda **kwargs: released.append(kwargs["dialect"]),
        )
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path INTEGER",
        )

        with pytest.raises(migrations.SchedulerSchemaError):
            migrations.expand_schema(db_engine=engine)

        assert released == ["sqlite"]


class _FailingProbeConnection(_RecordingConnection):
    """Records the probe statement, then fails it.

    The failure is raised from `execute` rather than answered as a NULL scalar
    because that is how the row probe actually fails: a denied permission, a
    proxy rejecting the optimizer hint, or MAX_EXECUTION_TIME firing all surface
    as a raised driver error, not as an empty result.
    """

    def __init__(self, *, error: BaseException) -> None:
        super().__init__()
        self._error = error

    def execute(
        self, statement: object, parameters: object = None
    ) -> "_FailingProbeConnection":
        super().execute(statement, parameters)
        raise self._error


def migrations_source_without_docstrings() -> str:
    """`_expand_indexes` body only, so prose cannot satisfy an ordering assertion."""
    source = _code_without_docstrings(migrations.__file__)
    start = source.index("def _expand_indexes")
    return source[start:]


class TestIndexBuildIsBoundedBeforeItStarts:
    """A build's duration cannot be bounded once started, so size is checked first.

    `ALGORITHM=INPLACE, LOCK=NONE` keeps the build from blocking DML; it does not
    make it fast. `lock_wait_timeout` bounds only metadata-lock acquisition, and
    `max_execution_time` applies only to SELECTs. So the only available bound is
    a decision taken before the statement is emitted.
    """

    def test_a_small_table_is_built_without_complaint(self) -> None:
        # The probe skips `_MAX_INDEX_BUILD_ROWS` rows and asks for one more.
        # No row there means the table does not exceed the limit.
        conn = _RecordingConnection(scalar_results=[None])
        assert migrations._refuse_if_table_is_large(conn=conn, dialect="mysql") is None

    def test_a_large_table_declines_the_build_and_names_the_limit(self) -> None:
        """The refusal names the LIMIT, because no size was ever measured.

        The probe deliberately answers one bounded question -- "more than n?" --
        so there is no row count to report, and inventing one would restate the
        stale statistic this replaced.
        """
        conn = _RecordingConnection(scalar_results=[1])

        reason = migrations._refuse_if_table_is_large(conn=conn, dialect="mysql")

        assert reason is not None
        assert str(migrations._MAX_INDEX_BUILD_ROWS) in reason
        assert "more than" in reason

    def test_an_unprobeable_size_is_treated_as_too_large(self) -> None:
        """Fail closed: starting a build of unknown duration is the risk itself.

        Unknown now arrives as a raised error rather than a NULL statistic -- a
        denied permission, a proxy rejecting the hint, or MAX_EXECUTION_TIME
        firing on a table too large to skip through in time. All three mean the
        same thing, and none of them may reach the caller as a fatal boot error.
        """
        conn = _FailingProbeConnection(
            error=sqlalchemy.exc.OperationalError(
                "SELECT 1 FROM ...",
                {},
                Exception("max execution time exceeded"),
            )
        )

        reason = migrations._refuse_if_table_is_large(conn=conn, dialect="mysql")

        assert reason is not None
        assert "could not be established" in reason
        # The failure kind is named, because it tells the operator whether to
        # grant a permission or to go and build the index by hand.
        assert "OperationalError" in reason

    def test_a_process_level_fault_is_not_reinterpreted_as_a_large_table(
        self,
    ) -> None:
        """Only SQLAlchemyError means "size unknown"; anything else propagates.

        Catching broadly here would silently convert a real fault into a routine
        "too big, build it deliberately" that an operator would act on wrongly.
        """
        conn = _FailingProbeConnection(error=MemoryError("not a database problem"))

        with pytest.raises(MemoryError):
            migrations._refuse_if_table_is_large(conn=conn, dialect="mysql")

    def test_the_probe_is_bounded_and_is_never_a_census(self) -> None:
        """It reads the table, but only n+1 index entries of it.

        The stored statistic this replaced could not be trusted -- InnoDB
        documents it as up to 40-50% off, and `innodb_stats_auto_recalc` can be
        switched off entirely -- so the probe reads real data. What keeps that
        affordable is the shape: `LIMIT 1 OFFSET n` plus a server-side execution
        cap, never an aggregate.
        """
        conn = _RecordingConnection(scalar_results=[None])

        migrations._exceeds_row_limit(conn=conn)

        statement = " ".join(conn.statements).upper()
        assert f"FROM {migrations._SCHEDULE_TABLE.upper()}" in statement
        assert f"OFFSET {migrations._MAX_INDEX_BUILD_ROWS}" in statement
        assert "LIMIT 1" in statement
        assert (
            f"MAX_EXECUTION_TIME({migrations._ROW_PROBE_TIMEOUT_MILLISECONDS})"
            in statement
        )
        # A census is what this exists to avoid.
        assert "COUNT(" not in statement
        assert "GROUP BY" not in statement
        # And the statistic it replaced must not creep back in.
        assert "INFORMATION_SCHEMA" not in statement

    def test_the_check_is_skipped_on_dialects_that_cannot_be_large(
        self,
    ) -> None:
        """SQLite only reaches this on a fresh database create_all already indexed."""
        conn = _RecordingConnection()

        assert migrations._refuse_if_table_is_large(conn=conn, dialect="sqlite") is None
        assert conn.statements == []

    def test_a_declined_build_closes_the_tiers_without_being_fatal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The pod still boots; the feature stays gated off."""
        engine = _legacy_engine()
        monkeypatch.setattr(
            migrations,
            "_refuse_if_table_is_large",
            lambda **_kwargs: "skipped: too big",
        )

        report = migrations.migrate_db(db_engine=engine)

        assert report.columns_ready
        assert not report.indexes_ready
        assert not report.path_writes_ready
        assert not report.reference_writes_ready
        for spec in migrations._TARGET_INDEXES:
            step = f"index:{spec.name}"
            assert report.status_of(step) is migrations.StepStatus.SKIPPED
            assert "too big" in report.detail_of(step)

    def test_a_conflicting_index_is_blocked_and_never_size_gated(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only an ABSENT index needs a build, so only an absent one is gated.

        A same-named index with the wrong definition is not waiting on a build:
        it exists, and it is wrong. Reporting it as "too big; build it
        deliberately" would hide the mismatch behind advice that cannot help --
        building an index that already exists does nothing -- while the tier
        stayed closed for a reason the report never stated.

        Both halves are asserted together because the distinction only exists
        when the two indexes disagree: the absent one is legitimately skipped by
        the size gate in the same pass that blocks the conflicting one.
        """
        engine = _legacy_engine()
        _execute(
            engine,
            f"CREATE INDEX {migrations._UQ_SCHEDULE_PATH} ON {migrations._SCHEDULE_TABLE} (created_by)",
        )
        monkeypatch.setattr(
            migrations,
            "_refuse_if_table_is_large",
            lambda **_kwargs: "skipped: too big",
        )

        report = migrations.migrate_db(db_engine=engine)

        conflicting = f"index:{migrations._UQ_SCHEDULE_PATH}"
        assert report.status_of(conflicting) is migrations.StepStatus.BLOCKED
        assert "too big" not in (report.detail_of(conflicting) or "")
        # The absent one is still gated, which is what makes this a real split.
        absent = f"index:{migrations._IX_REFERENCE}"
        assert report.status_of(absent) is migrations.StepStatus.SKIPPED
        assert "too big" in report.detail_of(absent)
        # Either way the tiers stay closed; the difference is what the operator
        # is told to do about it.
        assert not report.path_writes_ready
        assert not report.reference_writes_ready

    def test_the_gate_is_not_consulted_when_nothing_is_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A conflict alone must not trigger the probe.

        The probe costs a bounded read, but it also decides nothing here: no
        build is pending, so consulting it could only produce a verdict about a
        build that is not going to happen.
        """
        engine = _legacy_engine()
        migrations.migrate_db(db_engine=engine)  # installs both indexes
        _execute(engine, f"DROP INDEX {migrations._IX_REFERENCE}")
        _execute(
            engine,
            f"CREATE INDEX {migrations._IX_REFERENCE} ON {migrations._SCHEDULE_TABLE} (created_by)",
        )
        consulted: list[bool] = []
        monkeypatch.setattr(
            migrations,
            "_refuse_if_table_is_large",
            lambda **_kwargs: consulted.append(True) or None,
        )

        report = migrations.migrate_db(db_engine=engine)

        assert consulted == []
        assert (
            report.status_of(f"index:{migrations._IX_REFERENCE}")
            is migrations.StepStatus.BLOCKED
        )

    def test_no_index_ddl_is_emitted_when_the_build_is_declined(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        monkeypatch.setattr(
            migrations,
            "_refuse_if_table_is_large",
            lambda **_kwargs: "skipped: too big",
        )
        created: list[str] = []
        monkeypatch.setattr(
            migrations,
            "_create_index",
            lambda **kwargs: created.append(kwargs["spec"].name),
        )

        migrations.migrate_db(db_engine=engine)

        assert created == []

    def test_the_size_gate_runs_before_any_index_ddl(self) -> None:
        """Ordering is the point: a check after the build would bound nothing."""
        source = ast.unparse(ast.parse(migrations_source_without_docstrings()))
        gate = source.index("_refuse_if_table_is_large(conn=conn")
        emit = source.index("_create_index(conn=conn")
        assert gate < emit

    def test_an_installed_index_stays_ready_even_if_the_gate_would_refuse(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The gate governs builds, not verdicts about work already done.

        Consulting it unconditionally would let an unavailable row estimate close
        write tiers that are genuinely open — a worse failure than the slow build
        it exists to prevent, because it would happen on every healthy boot.
        """
        engine = _legacy_engine()
        migrations.migrate_db(db_engine=engine)  # installs both indexes
        monkeypatch.setattr(
            migrations,
            "_refuse_if_table_is_large",
            lambda **_kwargs: "skipped: would refuse",
        )

        report = migrations.migrate_db(db_engine=engine)

        assert report.indexes_ready
        assert report.path_writes_ready
        assert report.reference_writes_ready
        for spec in migrations._TARGET_INDEXES:
            assert (
                report.status_of(f"index:{spec.name}")
                is migrations.StepStatus.ALREADY_PRESENT
            )

    def test_the_gate_is_not_consulted_when_both_indexes_exist(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        migrations.migrate_db(db_engine=engine)
        consulted: list[bool] = []

        def _record(**_kwargs: object) -> None:
            consulted.append(True)
            return None

        monkeypatch.setattr(migrations, "_refuse_if_table_is_large", _record)

        migrations.migrate_db(db_engine=engine)

        assert consulted == []


class TestTheSavedPipelineForeignKeyIsInstalledUnderGuard:
    """The one copying statement in the module, and everything guarding it.

    A validated `ADD CONSTRAINT ... FOREIGN KEY` cannot be INPLACE on MySQL --
    the server permits that only with `foreign_key_checks` disabled, which
    produces a constraint the optimizer trusts over unvalidated data -- so this
    is `ALGORITHM=COPY, LOCK=SHARED` and it blocks writes while it runs. The
    reason it is on the boot path anyway is that the table is tiny and the gate
    refuses once it is not.
    """

    @staticmethod
    def _spec() -> object:
        return migrations._TARGET_FOREIGN_KEYS[0]

    def test_the_mysql_statement_is_exact_validated_and_non_cascading(
        self,
    ) -> None:
        conn = _RecordingConnection()

        migrations._create_foreign_key(conn=conn, dialect="mysql", spec=self._spec())

        assert conn.statements == [
            "ALTER TABLE scheduled_pipeline_run ADD"
            " CONSTRAINT fk_scheduled_pipeline_run_user_pipeline_id"
            " FOREIGN KEY (pipeline_task_spec_from_user_pipeline_id)"
            " REFERENCES pipeline (id)"
            " ON DELETE RESTRICT ON UPDATE RESTRICT,"
            " ALGORITHM=COPY, LOCK=SHARED"
        ]
        statement = conn.statements[0]
        # The two ways to make this cheap that must never appear: skipping
        # validation, and letting a parent delete reach a customer's schedule.
        assert "foreign_key_checks" not in statement
        assert "CASCADE" not in statement
        assert "SET NULL" not in statement

    def test_an_unreviewed_dialect_is_refused_rather_than_guessed(self) -> None:
        conn = _RecordingConnection()

        with pytest.raises(migrations.SchedulerSchemaError, match="refusing to add"):
            migrations._create_foreign_key(
                conn=conn, dialect="postgresql", spec=self._spec()
            )

        assert conn.statements == []

    def test_the_copy_is_gated_on_a_far_smaller_limit_than_the_index_build(
        self,
    ) -> None:
        """Different statements, different costs, different thresholds.

        `ADD INDEX` is INPLACE and never blocks DML, so being wrong costs a slow
        boot. This blocks every schedule write for the length of a table copy,
        so being wrong costs a write stall -- and the tolerable one is far
        shorter.
        """
        conn = _RecordingConnection(scalar_results=[None])

        migrations._exceeds_row_limit(conn=conn, limit=migrations._MAX_FK_COPY_ROWS)

        assert f"OFFSET {migrations._MAX_FK_COPY_ROWS}" in conn.statements[0]
        assert migrations._MAX_FK_COPY_ROWS < migrations._MAX_INDEX_BUILD_ROWS

    def test_a_large_table_declines_the_constraint_rather_than_stalling_writes(
        self,
    ) -> None:
        """Fail closed, and name the threshold it declined against.

        Driven through the preflight on a MySQL dialect, because that is the
        only dialect that copies. The probe answers "is there a row beyond the
        limit?" -- a row means yes.
        """
        shape = migrations.LiveShape(
            exists=True,
            columns={},
            indexes={
                migrations._IX_REFERENCE: {
                    "name": migrations._IX_REFERENCE,
                    "column_names": [
                        migrations._PIPELINE_ID_COLUMN,
                        migrations._VERSION_KEY_COLUMN,
                    ],
                    "unique": False,
                }
            },
            checks={},
            foreign_keys={},
        )
        # A row past the FK limit. The size gate now runs BEFORE the orphan
        # probe, so no `scalars_results` is consumed on this path.
        conn = _RecordingConnection(scalar_results=[1])

        decline = migrations._foreign_key_preflight(
            conn=conn, dialect="mysql", shape=shape
        )

        assert decline is not None
        status, detail = decline
        assert status is migrations.StepStatus.SKIPPED
        assert str(migrations._MAX_FK_COPY_ROWS) in detail
        assert "foreign key table copy" in detail
        statements = " ".join(conn.statements)
        assert f"OFFSET {migrations._MAX_FK_COPY_ROWS}" in statements
        # And it declined WITHOUT scanning: the orphan anti-join must not be
        # reachable on a table already judged too large to copy.
        assert "LEFT JOIN" not in statements.upper()

    def test_an_orphaned_reference_blocks_the_add_and_changes_no_rows(
        self,
    ) -> None:
        """The row is evidence about production, not something to clean up.

        Nulling it is not even available: the source CHECK requires exactly one
        source column, so a nulled reference becomes a schedule with no source
        at all -- a row that can never run and that nobody asked to break.
        """
        engine = _legacy_engine()
        _insert_legacy_row(engine)
        _execute(
            engine,
            "UPDATE scheduled_pipeline_run"
            " SET pipeline_task_spec = NULL,"
            "     pipeline_task_spec_from_user_pipeline_id = 'ghost-pipeline'",
        )

        def _rows() -> list[tuple[object, ...]]:
            with engine.connect() as conn:
                return list(
                    conn.execute(
                        sqlalchemy.text(
                            "SELECT id, pipeline_task_spec, pipeline_task_spec_from_user_pipeline_id"
                            " FROM scheduled_pipeline_run ORDER BY id"
                        )
                    ).all()
                )

        before = _rows()

        report = migrations.expand_schema(db_engine=engine)

        step = f"foreign_key:{migrations._FK_USER_PIPELINE}"
        assert report.status_of(step) is migrations.StepStatus.BLOCKED
        detail = report.detail_of(step)
        assert "ghost-pipeline" in detail
        assert "Nothing was modified" in detail
        assert not report.reference_writes_ready
        # Not nulled, not deleted, not remapped.
        assert _rows() == before
        assert (
            sqlalchemy.inspect(engine).get_foreign_keys("scheduled_pipeline_run") == []
        )

    def test_the_orphan_probe_is_bounded_and_is_never_a_census(self) -> None:
        """One row, as evidence. The second orphan changes no decision."""
        conn = _RecordingConnection()

        migrations._saved_pipeline_references_without_a_pipeline(conn=conn)

        statement = " ".join(conn.statements).upper()
        assert "LIMIT 1" in statement
        assert (
            f"MAX_EXECUTION_TIME({migrations._ROW_PROBE_TIMEOUT_MILLISECONDS})"
            in statement
        )
        assert "COUNT(" not in statement
        assert "GROUP BY" not in statement
        # Read-only: the preflight must never repair what it finds.
        for forbidden in ("UPDATE ", "DELETE ", "INSERT "):
            assert forbidden not in statement

    def test_an_unprobeable_orphan_check_declines_instead_of_failing_the_boot(
        self,
    ) -> None:
        conn = _FailingProbeConnection(
            error=sqlalchemy.exc.OperationalError("SELECT ...", {}, Exception("denied"))
        )

        decline = migrations._refuse_if_orphans_exist(conn=conn)

        assert decline is not None
        status, detail = decline
        assert status is migrations.StepStatus.SKIPPED
        assert "OperationalError" in detail

    def test_the_constraint_is_not_attempted_before_its_index_exists(
        self,
    ) -> None:
        """InnoDB invents its own index otherwise, under a name nothing verifies."""
        shape = migrations.LiveShape(
            exists=True, columns={}, indexes={}, checks={}, foreign_keys={}
        )

        decline = migrations._foreign_key_preflight(
            conn=_RecordingConnection(), dialect="mysql", shape=shape
        )

        assert decline is not None
        status, detail = decline
        assert status is migrations.StepStatus.SKIPPED
        assert migrations._IX_REFERENCE in detail

    def test_the_install_is_idempotent_and_ordered_after_the_indexes(
        self,
    ) -> None:
        engine = _legacy_engine()

        first = migrations.expand_schema(db_engine=engine)
        second = migrations.expand_schema(db_engine=engine)

        step = f"foreign_key:{migrations._FK_USER_PIPELINE}"
        assert first.status_of(step) is migrations.StepStatus.APPLIED
        assert second.status_of(step) is migrations.StepStatus.ALREADY_PRESENT
        assert second.reference_writes_ready
        kinds = [s.name.split(":")[0] for s in first.steps]
        assert kinds.index("foreign_key") > max(
            i for i, kind in enumerate(kinds) if kind == "index"
        )

    def test_a_named_rollback_exists_and_is_never_executed_here(self) -> None:
        """An image revert does not undo DDL, so the reversal has to be written
        down -- and it has to stay something a person runs deliberately.
        """
        statement = migrations.rollback_foreign_key_statement(self._spec())

        assert statement == (
            "ALTER TABLE scheduled_pipeline_run DROP FOREIGN KEY fk_scheduled_pipeline_run_user_pipeline_id"
        )
        code = _code_without_docstrings(migrations.__file__)
        # Written once, in the helper that returns it, and executed nowhere: the
        # statement text appears exactly once and no call site consumes it.
        assert code.count("DROP FOREIGN KEY") == 1
        assert code.count("rollback_foreign_key_statement") == 1


class TestTheConstraintMustBeTheOneWeMeant:
    """Two ways a schema can look correct and not be, both found by pi-40.

    Both share a shape: something matches on the attribute the check looked at
    and differs on one it did not, and the gate opens over it.
    """

    @staticmethod
    def _shape(**overrides: object) -> migrations.LiveShape:
        """A matching constraint, plus whatever the caller wants to spoil."""
        live: dict[str, object] = {
            "name": migrations._FK_USER_PIPELINE,
            "constrained_columns": [migrations._PIPELINE_ID_COLUMN],
            "referred_table": "pipeline",
            "referred_columns": ["id"],
            "options": {"ondelete": "RESTRICT", "onupdate": "RESTRICT"},
        }
        live.update(overrides)
        return migrations.LiveShape(
            exists=True,
            columns={},
            indexes={},
            checks={},
            foreign_keys={migrations._FK_USER_PIPELINE: live},
            default_schema="test_schema",
        )

    def test_a_same_named_table_in_another_database_is_not_our_parent(
        self,
    ) -> None:
        """`referred_table` is a bare name, so it matches across schemas.

        A constraint pointing at `wrong_db.pipeline(id)` satisfies every other
        check here -- right columns, right target name, right actions -- and
        enforces against rows this application has never seen.
        """
        verdict, detail = migrations._verify_foreign_key(
            self._shape(referred_schema="wrong_db"),
            migrations._TARGET_FOREIGN_KEYS[0],
        )

        assert verdict is migrations._Verdict.CONFLICTS
        assert "wrong_db" in detail

    def test_an_unqualified_reference_is_our_own_database(self) -> None:
        """Reflection omits the schema when the DDL did not name one.

        The normal case has to keep matching, or the constraint we just
        installed reports CONFLICTS on the next boot.
        """
        assert migrations._verify_foreign_key(
            self._shape(), migrations._TARGET_FOREIGN_KEYS[0]
        )[0] is (migrations._Verdict.MATCHES)

    def test_our_own_schema_named_explicitly_still_matches(self) -> None:
        assert (
            migrations._verify_foreign_key(
                self._shape(referred_schema="test_schema"),
                migrations._TARGET_FOREIGN_KEYS[0],
            )[0]
            is migrations._Verdict.MATCHES
        )


class TestAnUnexplainedConstraintClosesReferenceWrites:
    """ "Ours is correct" is not "the schema is safe to write". Found by pi-40."""

    @staticmethod
    def _report_with_an_extra_constraint() -> migrations.MigrationReport:
        report = migrations.MigrationReport(dialect="mysql")
        for name in migrations._WRITE_TIER_STEPS["reference_writes"]():
            report.add(name, migrations.StepStatus.ALREADY_PRESENT)
        for name in migrations._WRITE_TIER_STEPS["path_writes"]():
            report.add(name, migrations.StepStatus.ALREADY_PRESENT)
        report.add(
            "foreign_key:unexpected:fk_extra",
            migrations.StepStatus.BLOCKED,
            "unexpected foreign key ... (ON DELETE CASCADE)",
        )
        return report

    def test_a_second_cascading_constraint_is_not_made_harmless_by_ours(
        self,
    ) -> None:
        report = self._report_with_an_extra_constraint()

        assert not report.reference_writes_ready

    def test_the_refusal_names_the_constraint_that_caused_it(self) -> None:
        """A closed tier with no cause is an outage nobody can act on."""
        causes = [
            s.name
            for s in self._report_with_an_extra_constraint().tier_causes(
                "reference_writes"
            )
        ]

        assert "foreign_key:unexpected:fk_extra" in causes

    def test_it_does_not_close_schedule_path_writes(self) -> None:
        """Scope: it says nothing about `(created_by, schedule_path)` uniqueness."""
        report = self._report_with_an_extra_constraint()

        assert report.path_writes_ready
        assert [s.name for s in report.tier_causes("path_writes")] == []


class TestATargetColumnNeedNotBeAVarchar:
    """`_ColumnSpec` hardcoded VARCHAR in four places, so a JSON target could
    not be declared: the DDL fragment, the Alembic type, the family check and
    the widen test each assumed a length. They now live on the spec, and this
    class pins both halves -- that JSON works, and that VARCHAR did not change.

    MySQL and SQLite reflect a JSON column as something satisfying
    `isinstance(t, sqlalchemy.JSON)`, which is why one family test covers them.
    That is measured here rather than assumed, because the MySQL type is
    native binary rather than TEXT and a string-family test would pass on the
    wrong server version.
    """

    @staticmethod
    def _reflected(name: str, type_: object) -> migrations.LiveShape:
        return migrations.LiveShape(
            exists=True,
            columns={
                name: {
                    "name": name,
                    "type": type_,
                    "nullable": True,
                    "default": None,
                }
            },
            indexes={},
            checks={},
            foreign_keys={},
        )

    @staticmethod
    def _json_spec() -> migrations._JsonColumnSpec:
        return migrations._JsonColumnSpec(name="settings")

    @staticmethod
    def _varchar_spec() -> migrations._ColumnSpec:
        return next(s for s in migrations._TARGET_COLUMNS if s.name == "schedule_path")

    def test_the_mysql_fragment_is_the_bare_word_json(self) -> None:
        """No length, no charset, no collation -- a JSON column takes none."""
        assert self._json_spec().sql_type == "JSON"

    def test_a_json_column_reflected_from_mysql_matches(self) -> None:
        shape = self._reflected("settings", mysql.JSON())

        verdict, detail = migrations._verify_column(shape, self._json_spec())

        assert verdict is migrations._Verdict.MATCHES, detail

    def test_a_json_column_reflected_from_sqlite_matches_too(self) -> None:
        """The suite's own dialect. A JSON target that only verified on MySQL
        would read as CONFLICTS on every developer's local database."""
        shape = self._reflected("settings", sqlite.JSON())

        verdict, detail = migrations._verify_column(shape, self._json_spec())

        assert verdict is migrations._Verdict.MATCHES, detail

    def test_a_varchar_found_where_json_is_wanted_is_a_conflict(self) -> None:
        """The pre-JSON column type, in case an older image created it."""
        verdict, detail = migrations._verify_column(
            self._reflected("settings", mysql.VARCHAR(255)), self._json_spec()
        )

        assert verdict is migrations._Verdict.CONFLICTS
        assert "expected a JSON column" in detail

    def test_a_json_column_is_never_widenable(self) -> None:
        """There is no "too short" JSON, so the repair path must not open.

        `_widen` emits MODIFY COLUMN. Reaching it with a JSON spec would
        rewrite a column rather than widen one, which is the damage
        `_widenable` exists to refuse.
        """
        live = {"type": mysql.VARCHAR(20), "nullable": True, "default": None}

        assert migrations._widenable(live, self._json_spec()) is False

    def test_the_json_add_column_is_still_pinned_to_instant(self) -> None:
        """The generalisation must not lose the ALGORITHM clause."""
        conn = _RecordingConnection()

        migrations._add_column(conn=conn, dialect="mysql", spec=self._json_spec())

        assert "ADD COLUMN settings JSON NULL" in conn.statements[0]
        assert migrations._MYSQL_INSTANT in conn.statements[0]

    def test_a_varchar_add_column_emits_exactly_what_it_did_before(
        self,
    ) -> None:
        """The no-behaviour-change half, asserted as text rather than trusted."""
        conn = _RecordingConnection()

        migrations._add_column(conn=conn, dialect="mysql", spec=self._varchar_spec())

        assert (
            f"ALTER TABLE {migrations._SCHEDULE_TABLE} ADD COLUMN schedule_path"
            f" VARCHAR({db_models.SCHEDULE_PATH_LENGTH}) NULL, {migrations._MYSQL_INSTANT}"
            in conn.statements[0]
        )

    def test_a_varchar_spec_still_widens_on_a_shorter_column(self) -> None:
        """The behaviour `_widenable` moved onto the spec, unchanged."""
        live = {"type": mysql.VARCHAR(20), "nullable": True, "default": None}

        assert migrations._widenable(live, self._varchar_spec()) is True

    def test_a_json_column_added_to_sqlite_round_trips_as_json(self) -> None:
        """End to end on a real engine: emit, reflect, verify.

        The MySQL assertions above are text, because no test here can execute
        MySQL DDL. SQLite can, so the one dialect that can prove the emitter
        and the verifier agree does so.
        """
        engine = sqlalchemy.create_engine("sqlite://")
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"CREATE TABLE {migrations._SCHEDULE_TABLE} (id VARCHAR(36))"
                )
            )
            migrations._add_column(conn=conn, dialect="sqlite", spec=self._json_spec())

        columns = {
            c["name"]: c
            for c in sqlalchemy.inspect(engine).get_columns(migrations._SCHEDULE_TABLE)
        }
        shape = migrations.LiveShape(
            exists=True, columns=columns, indexes={}, checks={}, foreign_keys={}
        )

        verdict, detail = migrations._verify_column(shape, self._json_spec())

        assert verdict is migrations._Verdict.MATCHES, detail


class TestCollationCannotStopThePodFromBooting:
    """The defect a reviewer caught on #568 before it reached a database.

    `_verify_column` compared the rendered type text, and SQLAlchemy renders a
    reflected MySQL column carrying an explicit collation as
    `VARCHAR(36) COLLATE "utf8mb4_bin"`. That is not equal to `VARCHAR(36)`, so
    the verdict was CONFLICTS; the length is already correct so `_widenable`
    declines; and a conflicting mapped column is fatal. Every pod would have
    failed to boot on a column entirely capable of holding the data.
    """

    @staticmethod
    def _reflected(
        type_: object, name: str = "pipeline_task_spec_from_user_pipeline_id"
    ) -> migrations.LiveShape:
        return migrations.LiveShape(
            exists=True,
            columns={
                name: {
                    "name": name,
                    "type": type_,
                    "nullable": True,
                    "default": None,
                }
            },
            indexes={},
            checks={},
            foreign_keys={},
        )

    @staticmethod
    def _spec() -> migrations._ColumnSpec:
        return next(
            s
            for s in migrations._TARGET_COLUMNS
            if s.name == "pipeline_task_spec_from_user_pipeline_id"
        )

    def test_an_explicitly_collated_column_is_not_a_conflict(self) -> None:
        """The regression itself: rendered COLLATE must not read as wrong."""
        shape = self._reflected(mysql.VARCHAR(36, collation="utf8mb4_bin"))

        verdict, detail = migrations._verify_column(shape, self._spec())

        assert verdict is migrations._Verdict.MATCHES, detail

    def test_a_charset_qualified_column_is_not_a_conflict_either(self) -> None:
        shape = self._reflected(mysql.VARCHAR(36, charset="utf8mb4"))

        assert (
            migrations._verify_column(shape, self._spec())[0]
            is migrations._Verdict.MATCHES
        )

    def test_no_pod_fails_to_boot_over_a_rendered_collate_suffix(self) -> None:
        """End to end: the fatal path must not be reachable from collation alone.

        `expand_schema` raises on a conflicting mapped column, so this asserts
        the absence of a raise -- which is the whole of the bug.
        """
        engine = _legacy_engine()
        _execute(
            engine,
            "ALTER TABLE scheduled_pipeline_run ADD COLUMN schedule_path VARCHAR(255) COLLATE BINARY",
        )

        report = migrations.expand_schema(db_engine=engine)

        assert report.columns_ready
        assert report.status_of("column:schedule_path") in {
            migrations.StepStatus.ALREADY_PRESENT,
            migrations.StepStatus.APPLIED,
        }

    def test_a_structural_length_mismatch_is_still_exact(self) -> None:
        """Loosening the comparison must not loosen what it catches."""
        verdict, detail = migrations._verify_column(
            self._reflected(mysql.VARCHAR(20)), self._spec()
        )

        assert verdict is migrations._Verdict.CONFLICTS
        assert "expected length 36, found 20" in detail

    def test_a_text_column_is_still_a_conflict_not_near_enough(self) -> None:
        """TEXT subclasses String but not VARCHAR, and holds a different thing."""
        verdict, detail = migrations._verify_column(
            self._reflected(mysql.LONGTEXT()), self._spec()
        )

        assert verdict is migrations._Verdict.CONFLICTS
        assert "expected a VARCHAR column" in detail

    def test_an_absent_column_is_still_missing_and_not_conflicting(
        self,
    ) -> None:
        """MISSING and CONFLICT drive different repairs; keep them distinct."""
        empty = migrations.LiveShape(
            exists=True, columns={}, indexes={}, checks={}, foreign_keys={}
        )

        assert (
            migrations._verify_column(empty, self._spec())[0]
            is migrations._Verdict.ABSENT
        )


class TestTheForeignKeyChecksCollationAgainstTheParent:
    """Where collation actually matters: InnoDB refuses a mismatched FK.

    Moved off the column verdict and onto the constraint, so a mismatch closes
    one tier instead of the whole service.
    """

    @staticmethod
    def _conn(child: tuple[str, str], parent: tuple[str, str]) -> _RecordingConnection:
        return _RecordingConnection(
            all_results=[
                [
                    (
                        "scheduled_pipeline_run",
                        "pipeline_task_spec_from_user_pipeline_id",
                        child[0],
                        child[1],
                    ),
                    ("pipeline", "id", parent[0], parent[1]),
                ]
            ]
        )

    def test_matching_effective_collations_proceed(self) -> None:
        conn = self._conn(
            ("utf8mb4", "utf8mb4_0900_ai_ci"), ("utf8mb4", "utf8mb4_0900_ai_ci")
        )

        assert migrations._refuse_if_collations_differ(conn=conn) is None

    def test_a_collation_mismatch_blocks_and_names_both_sides(self) -> None:
        conn = self._conn(("utf8mb4", "utf8mb4_bin"), ("utf8mb4", "utf8mb4_0900_ai_ci"))

        decline = migrations._refuse_if_collations_differ(conn=conn)

        assert decline is not None
        status, detail = decline
        # BLOCKED, not SKIPPED: no later boot clears this on its own.
        assert status is migrations.StepStatus.BLOCKED
        assert "utf8mb4_bin" in detail and "utf8mb4_0900_ai_ci" in detail
        assert "Nothing was modified" in detail

    def test_a_charset_mismatch_blocks_too(self) -> None:
        conn = self._conn(
            ("utf8mb3", "utf8mb3_general_ci"), ("utf8mb4", "utf8mb4_0900_ai_ci")
        )

        decline = migrations._refuse_if_collations_differ(conn=conn)

        assert decline is not None and decline[0] is migrations.StepStatus.BLOCKED

    def test_it_reads_resolved_values_and_not_the_ddl(self) -> None:
        """Inherited table defaults are the case `SHOW CREATE TABLE` cannot show.

        A column that overrides nothing prints no `COLLATE`, so two columns
        inheriting DIFFERENT table defaults look identical in the DDL. Only
        information_schema reports what they resolve to.
        """
        conn = self._conn(
            ("utf8mb4", "utf8mb4_0900_ai_ci"), ("utf8mb4", "utf8mb4_general_ci")
        )

        decline = migrations._refuse_if_collations_differ(conn=conn)

        assert decline is not None and decline[0] is migrations.StepStatus.BLOCKED
        statement = " ".join(conn.statements)
        assert "information_schema.COLUMNS" in statement
        assert "COLLATION_NAME" in statement
        # Metadata only: this must never become a scan of the table itself.
        assert "scheduled_pipeline_run AS" not in statement
        assert "COUNT(" not in statement.upper()

    def test_an_unreadable_collation_skips_rather_than_blocks(self) -> None:
        conn = _FailingProbeConnection(
            error=sqlalchemy.exc.OperationalError("SELECT ...", {}, Exception("denied"))
        )

        decline = migrations._refuse_if_collations_differ(conn=conn)

        assert decline is not None and decline[0] is migrations.StepStatus.SKIPPED

    def test_a_column_information_schema_does_not_report_skips(self) -> None:
        conn = _RecordingConnection(
            all_results=[[("pipeline", "id", "utf8mb4", "utf8mb4_0900_ai_ci")]]
        )

        decline = migrations._refuse_if_collations_differ(conn=conn)

        assert decline is not None
        status, detail = decline
        assert status is migrations.StepStatus.SKIPPED
        assert "pipeline_task_spec_from_user_pipeline_id" in detail
        # Names the database it looked in: if DATABASE() ever resolves somewhere
        # unexpected, the skip has a visible cause instead of being permanent
        # and mysterious.
        assert "SELECT DATABASE()" in " ".join(conn.statements)

    def test_the_mismatch_closes_only_the_reference_tier(self) -> None:
        """Columns, path writes and inline schedules must keep working."""
        report = migrations.MigrationReport(dialect="mysql")
        for name in migrations._WRITE_TIER_STEPS["path_writes"]():
            report.add(name, migrations.StepStatus.ALREADY_PRESENT)
        report.add(
            f"foreign_key:{migrations._FK_USER_PIPELINE}",
            migrations.StepStatus.BLOCKED,
            "collation mismatch",
        )

        assert report.path_writes_ready
        assert not report.reference_writes_ready


_MYSQL_STORED_CHECK = (
    "((case when (`pipeline_task_spec` is not null) then 1 else 0 end"
    " + case when (`pipeline_task_spec_from_pipeline_run_id` is not null) then 1 else 0 end"
    " + case when (`pipeline_task_spec_from_user_pipeline_id` is not null) then 1 else 0 end) = 1)"
    " and ((`pipeline_task_spec_from_user_pipeline_version_key` is null)"
    " or (`pipeline_task_spec_from_user_pipeline_id` is not null))"
)


def _shape_with_check(body: str) -> migrations.LiveShape:
    return migrations.LiveShape(
        exists=True,
        columns={},
        indexes={},
        checks={migrations._CK_SOURCE: body},
        foreign_keys={},
    )


class TestTheCheckIsVerifiedByMeaningNotByText:
    """Thread 3897328708: MySQL stores its own rewrite, so text can never match.

    The constraint was installed and enforcing correctly; only the verdict was
    wrong -- permanently, on every boot. No readiness tier gates on
    `check_ready`, so the cost was a standing false conflict in the boot log
    rather than a closed feature. That is still worth fixing: an operator who
    learns to ignore one conflict line ignores the next one too.
    """

    def test_the_mysql_rewrite_of_our_own_check_matches(self) -> None:
        """The regression: this is what MySQL actually stores."""
        verdict, detail = migrations._verify_check(
            _shape_with_check(_MYSQL_STORED_CHECK)
        )

        assert verdict is migrations._Verdict.MATCHES, detail

    def test_the_declared_text_still_matches(self) -> None:
        """SQLite stores it verbatim; that path must not regress."""
        shape = _shape_with_check(db_models.SOURCE_INVARIANT_CHECK_SQL)

        assert migrations._verify_check(shape)[0] is migrations._Verdict.MATCHES

    def test_reordered_terms_still_mean_the_same_thing(self) -> None:
        """Addition commutes, so term order carries no meaning."""
        reordered = (
            "((case when (`pipeline_task_spec_from_user_pipeline_id` is not null) then 1 else 0 end"
            " + case when (`pipeline_task_spec` is not null) then 1 else 0 end"
            " + case when (`pipeline_task_spec_from_pipeline_run_id` is not null) then 1 else 0 end) = 1)"
            " and ((`pipeline_task_spec_from_user_pipeline_version_key` is null)"
            " or (`pipeline_task_spec_from_user_pipeline_id` is not null))"
        )

        assert (
            migrations._verify_check(_shape_with_check(reordered))[0]
            is migrations._Verdict.MATCHES
        )

    def test_the_weakened_grouping_is_still_caught(self) -> None:
        """The whole reason grouping is walked instead of normalized away.

        `(count = 1 AND vkey IS NULL) OR pid IS NOT NULL` permits ANY row that
        carries a pipeline reference. It must never read as the strict form.
        """
        weakened = (
            "((case when (`pipeline_task_spec` is not null) then 1 else 0 end"
            " + case when (`pipeline_task_spec_from_pipeline_run_id` is not null) then 1 else 0 end"
            " + case when (`pipeline_task_spec_from_user_pipeline_id` is not null) then 1 else 0 end) = 1"
            " and (`pipeline_task_spec_from_user_pipeline_version_key` is null))"
            " or (`pipeline_task_spec_from_user_pipeline_id` is not null)"
        )

        assert (
            migrations._verify_check(_shape_with_check(weakened))[0]
            is migrations._Verdict.CONFLICTS
        )

    def test_a_missing_source_column_conflicts(self) -> None:
        """Two counted sources instead of three is a different invariant."""
        two_sources = (
            "((case when (`pipeline_task_spec` is not null) then 1 else 0 end"
            " + case when (`pipeline_task_spec_from_user_pipeline_id` is not null) then 1 else 0 end) = 1)"
            " and ((`pipeline_task_spec_from_user_pipeline_version_key` is null)"
            " or (`pipeline_task_spec_from_user_pipeline_id` is not null))"
        )

        assert (
            migrations._verify_check(_shape_with_check(two_sources))[0]
            is migrations._Verdict.CONFLICTS
        )

    def test_a_duplicated_term_is_not_deduplicated_into_agreement(self) -> None:
        """A set alone would call four terms equal to three; the count stops that."""
        duplicated = _MYSQL_STORED_CHECK.replace(
            "(case when (`pipeline_task_spec` is not null) then 1 else 0 end",
            "(case when (`pipeline_task_spec` is not null) then 1 else 0 end"
            " + case when (`pipeline_task_spec` is not null) then 1 else 0 end",
            1,
        )

        assert (
            migrations._verify_check(_shape_with_check(duplicated))[0]
            is migrations._Verdict.CONFLICTS
        )

    def test_a_different_bound_conflicts(self) -> None:
        """`= 2` would permit exactly the double-source rows the CHECK forbids."""
        two = _MYSQL_STORED_CHECK.replace("end) = 1)", "end) = 2)")

        assert (
            migrations._verify_check(_shape_with_check(two))[0]
            is migrations._Verdict.CONFLICTS
        )

    def test_the_wrong_column_in_the_implication_conflicts(self) -> None:
        swapped = _MYSQL_STORED_CHECK.replace(
            "(`pipeline_task_spec_from_user_pipeline_version_key` is null)",
            "(`pipeline_task_spec` is null)",
        )

        assert (
            migrations._verify_check(_shape_with_check(swapped))[0]
            is migrations._Verdict.CONFLICTS
        )

    def test_an_unrecognized_body_falls_back_and_still_conflicts(self) -> None:
        """Fail closed on a shape the parser cannot read.

        The fallback is the OLD textual comparison, so an unrecognized body is
        judged exactly as it was before this change: different text, conflict.
        What the fallback guarantees is that replacing the comparison cannot
        turn a previously-passing verdict into a new failure -- not that an
        unreadable constraint is waved through.
        """
        opaque = "some_udf_nobody_here_models(a, b)"
        assert migrations._source_invariant_fingerprint(opaque) is None
        assert (
            migrations._verify_check(_shape_with_check(opaque))[0]
            is migrations._Verdict.CONFLICTS
        )

    def test_the_fallback_accepts_exactly_what_the_old_verifier_accepted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A body the parser cannot read, but that is textually identical.

        The fingerprint is disabled outright so this genuinely travels the
        fallback branch. An earlier version passed the declared constant with
        the parser live, which returns through the structural branch and proves
        nothing about the fallback at all.
        """
        monkeypatch.setattr(
            migrations, "_source_invariant_fingerprint", lambda _body: None
        )

        shape = _shape_with_check(db_models.SOURCE_INVARIANT_CHECK_SQL)

        assert migrations._verify_check(shape)[0] is migrations._Verdict.MATCHES

    def test_an_empty_body_is_still_unverifiable_and_not_satisfied(
        self,
    ) -> None:
        verdict, detail = migrations._verify_check(_shape_with_check(""))

        assert verdict is migrations._Verdict.CONFLICTS
        assert "cannot verify" in detail

    def test_an_absent_check_is_still_absent(self) -> None:
        empty = migrations.LiveShape(
            exists=True, columns={}, indexes={}, checks={}, foreign_keys={}
        )

        assert migrations._verify_check(empty)[0] is migrations._Verdict.ABSENT

    def test_grouping_is_walked_not_stripped(self) -> None:
        """Directly: a top-level OR has no top-level AND to split on."""
        assert migrations._source_invariant_fingerprint("(a and b) or c") is None
        assert migrations._split_at_depth_zero(
            migrations._sql_tokens("( a and b ) or c"), "and"
        ) == [migrations._sql_tokens("( a and b ) or c")]


class TestTheLockIsNotTakenWhenThereIsNothingToDo:
    """Thread 3897334020: every boot queued for a lock before knowing of work."""

    @staticmethod
    def _final_engine() -> sqlalchemy.Engine:
        engine = _empty_engine()
        db_models.ScheduledPipelineRun.metadata.create_all(engine)
        return engine

    def test_a_finished_database_reports_the_same_thing_either_way(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The pre-lock path must not produce a different report.

        Both paths are FORCED rather than assumed. An earlier version of this
        test called `expand_schema` twice on a `create_all` database, where both
        calls take the early return -- so it compared the fast path with itself
        and would have passed no matter what the locked path did.
        """
        engine = self._final_engine()

        early_path = migrations.expand_schema(db_engine=engine)

        monkeypatch.setattr(migrations, "_already_final", lambda **_: False)
        locked_path = migrations.expand_schema(db_engine=engine)

        assert early_path.summary() == locked_path.summary()
        assert early_path.schema_ready
        assert early_path.reference_writes_ready

    def test_it_returns_false_when_anything_is_missing(self) -> None:
        """Anything short of unanimous MATCHES must fall through to the lock."""
        engine = _legacy_engine()
        report = migrations.MigrationReport(dialect="sqlite")

        with engine.connect() as conn:
            assert migrations._already_final(conn=conn, report=report) is False

    def test_it_returns_true_and_records_verdicts_when_everything_matches(
        self,
    ) -> None:
        engine = self._final_engine()
        report = migrations.MigrationReport(dialect="sqlite")

        with engine.connect() as conn:
            assert migrations._already_final(conn=conn, report=report) is True

        # Recorded, not merely observed: the report a caller receives on the
        # early path has to be complete.
        assert report.schema_ready
        assert report.check_ready
        assert (
            report.status_of(f"check:{migrations._CK_SOURCE}")
            is migrations.StepStatus.ALREADY_PRESENT
        )

    def test_a_legacy_database_still_migrates(self) -> None:
        """The skip must never swallow real work."""
        engine = _legacy_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert report.status_of("column:schedule_path") is migrations.StepStatus.APPLIED


class TestTheFastPathFiresOnTheDeployedSteadyState:
    """Both reviewers caught the same defect: the fast path never fired.

    `_already_final` originally required the hardening verdicts too. The only
    hardening object is the source CHECK, which startup never installs on an
    existing table -- so on every database that reached its shape by UPGRADE it
    is ABSENT and stays ABSENT. The predicate was therefore false on exactly the
    deployed steady state thread 3897334020 is about, and every boot still took
    the lock. A fast path that cannot fire is worse than none: it reads as fixed.
    """

    @staticmethod
    def _upgraded_engine() -> sqlalchemy.Engine:
        """A legacy table brought fully up to date by this migration.

        This is production's shape, not `create_all`'s: every actionable object
        present, the source CHECK absent because nothing could add it.
        """
        engine = _legacy_engine()
        migrations.expand_schema(db_engine=engine)
        return engine

    def test_an_upgraded_table_with_no_check_takes_no_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = self._upgraded_engine()
        acquisitions: list[str] = []
        monkeypatch.setattr(
            migrations,
            "_acquire_lock",
            lambda **kwargs: acquisitions.append(kwargs["dialect"]) or True,
        )

        report = migrations.expand_schema(db_engine=engine)

        assert acquisitions == [], "the deployed steady state still queued for the lock"
        # And the report still tells the truth about the CHECK it skipped over.
        assert not report.check_ready
        assert report.schema_ready
        assert report.reference_writes_ready

    def test_an_incomplete_table_still_takes_the_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The skip must never swallow real work."""
        engine = _legacy_engine()
        acquisitions: list[str] = []
        monkeypatch.setattr(
            migrations,
            "_acquire_lock",
            lambda **kwargs: acquisitions.append(kwargs["dialect"]) or True,
        )

        migrations.expand_schema(db_engine=engine)

        assert acquisitions, "a table needing migration must serialize"

    def test_an_unexplained_constraint_keeps_the_slow_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unexpected FKs live in verify_schema, so they gate the fast path.

        Deliberate: an unexplained constraint is an operator-actionable state,
        and it must not be able to slip past on a path that skips inspection.
        Driven through `_already_final` rather than through the verifier alone,
        because the claim is about the DECISION, not about detection.
        """
        engine = self._upgraded_engine()
        with engine.connect() as conn:
            final_shape = migrations._inspect(conn=conn)
        assert not migrations._verify_unexpected_foreign_keys(final_shape)

        intruded = dataclasses.replace(
            final_shape,
            foreign_keys=dict(final_shape.foreign_keys)
            | {
                "fk_someone_elses": {
                    "name": "fk_someone_elses",
                    "constrained_columns": [migrations._PIPELINE_ID_COLUMN],
                    "referred_table": "somewhere",
                    "referred_columns": ["id"],
                }
            },
        )
        monkeypatch.setattr(migrations, "_inspect", lambda **_: intruded)
        report = migrations.MigrationReport(dialect="sqlite")

        with engine.connect() as conn:
            assert migrations._already_final(conn=conn, report=report) is False

    def test_a_dialect_with_neither_pin_pays_no_discard_on_the_fast_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The early return recycles only what it actually pinned.

        Two things can pin a frontend connection at ProxySQL: GET_LOCK, and the
        session statements `_bounded_metadata_lock_wait` runs. On MySQL the fast
        path now issues the second of those and IS recycled by the bounded block
        -- one connection per boot, deliberately. SQLite issues neither, so a
        discard here would cost a pool slot on every boot and could fail startup
        inside `_discard_connection` for nothing. Only the advisory-lock
        predicate is forced on, so what is asserted is that GET_LOCK's
        quarantine alone does not fire without a GET_LOCK.
        """
        engine = self._upgraded_engine()
        discards: list[str] = []
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda _dialect: True)
        monkeypatch.setattr(
            migrations,
            "_discard_connection",
            lambda **_: discards.append("discard"),
        )

        migrations.expand_schema(db_engine=engine)

        assert discards == [], "an unlocked connection was needlessly invalidated"

    def test_a_connection_that_did_lock_is_still_discarded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The quarantine must survive the optimisation."""
        engine = _legacy_engine()
        discards: list[str] = []
        monkeypatch.setattr(migrations, "_uses_advisory_lock", lambda _dialect: True)
        # SQLite has no GET_LOCK; only the attempt is under test here.
        monkeypatch.setattr(migrations, "_acquire_lock", lambda **_: True)
        monkeypatch.setattr(migrations, "_release_lock", lambda **_: None)
        monkeypatch.setattr(
            migrations,
            "_discard_connection",
            lambda **_: discards.append("discard"),
        )

        migrations.expand_schema(db_engine=engine)

        assert discards == ["discard"]


class TestAnUnreviewedDialectIsRefusedOnlyWhenSomethingWouldBeEmitted:
    """The scope of the dialect guard, pinned rather than left incidental.

    Raised by pi-41 against the fast path: verification can now serve a report
    on a dialect nobody reviewed, without reaching `_expand`'s refusal. That is
    the module's stated policy rather than a hole in it -- the guard exists so
    nobody emits unreviewed DDL, and read-only verification is explicitly
    allowed on any dialect. The absent-table return has always behaved this way.

    It was untested in either direction, which is the actual defect. If someone
    later decides the policy should be "never run", these tests are where that
    argument has to be made.
    """

    def test_a_final_schema_is_served_on_a_dialect_nobody_reviewed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        migrations.expand_schema(db_engine=engine)
        monkeypatch.setattr(migrations, "_DDL_DIALECTS", frozenset({"mysql"}))

        report = migrations.expand_schema(db_engine=engine)

        assert report.schema_ready

    def test_anything_left_to_emit_still_stops_the_process(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        monkeypatch.setattr(migrations, "_DDL_DIALECTS", frozenset({"mysql"}))

        with pytest.raises(
            migrations.SchedulerSchemaError, match="refusing to emit schema DDL"
        ):
            migrations.expand_schema(db_engine=engine)


class TestAClosedTierCanNoticeThatItOpened:
    """Thread 3899493527: the boot verdict was frozen for the pod's life.

    A pod that started while the index was still building recorded
    `path_writes_ready = False` and served 503 for that feature forever. The
    documented remedy -- every serving pod must have booted after the migration
    succeeded -- is not enforceable by a deployment, and is violated by any
    restart, autoscale event or crash-loop.
    """

    @staticmethod
    def _closed() -> migrations.MigrationReport:
        return migrations.MigrationReport(dialect="sqlite")

    def test_without_an_engine_the_boot_report_is_final(self) -> None:
        """The previous behaviour, preserved for injected-report callers."""
        report = self._closed()
        readiness = migrations.SchemaReadiness(report=report)

        assert readiness.current() is report

    def test_a_tier_that_opened_is_picked_up(self) -> None:
        engine = _empty_engine()
        db_models.ScheduledPipelineRun.metadata.create_all(engine)
        readiness = migrations.SchemaReadiness(report=self._closed(), db_engine=engine)

        assert readiness.current().path_writes_ready

    def test_an_open_tier_is_never_withdrawn(self) -> None:
        """One-way. A verdict already acted on must not be revoked mid-request."""
        # PARTIALLY open: path ready, reference not. A fully open report would
        # short-circuit before the one-way rule ran, and the test would pass with
        # the rule deleted -- which is exactly what an earlier version did.
        engine = _empty_engine()
        db_models.ScheduledPipelineRun.metadata.create_all(engine)
        with engine.connect() as conn:
            full_shape = migrations._inspect(conn=conn)
        partial = migrations.MigrationReport(dialect="sqlite")
        migrations.record_verification(
            report=partial,
            results=[
                r
                for r in migrations.verify_schema(full_shape)
                if "foreign_key" not in r[0]
            ],
        )
        assert partial.path_writes_ready
        assert not partial.reference_writes_ready

        # A LEGACY table, so the re-check genuinely runs and genuinely reports
        # the path tier CLOSED. An empty engine would short-circuit on `exists`.
        legacy = _legacy_engine()
        readiness = migrations.SchemaReadiness(
            report=partial, db_engine=legacy, recheck_seconds=0.0
        )

        assert (
            readiness.current().path_writes_ready
        ), "an acted-on verdict was withdrawn"

    def test_an_unreachable_database_keeps_the_closed_verdict(self) -> None:
        """Fail closed: no answer is not an excuse to guess `ready`."""
        engine = _empty_engine()
        engine.dispose()

        class _Broken:
            def connect(self) -> object:
                raise sqlalchemy.exc.OperationalError("SELECT 1", {}, Exception("gone"))

        readiness = migrations.SchemaReadiness(
            report=self._closed(),
            db_engine=typing.cast(sqlalchemy.Engine, _Broken()),
        )

        assert readiness.current().path_writes_ready is False

    def test_it_does_not_re_verify_on_every_request(self) -> None:
        """A fleet behind a blocked migration must not become a query source."""
        # Legacy, so the tier STAYS closed and `_is_fully_open` cannot be what
        # stops the second look -- the rate limit has to be.
        engine = _legacy_engine()
        connects: list[int] = []
        real_connect = engine.connect

        def _counting_connect(*args: object, **kwargs: object) -> object:
            connects.append(1)
            return real_connect(*args, **kwargs)

        readiness = migrations.SchemaReadiness(
            report=self._closed(),
            db_engine=engine,
            recheck_seconds=30.0,
            clock=lambda: 100.0,
        )
        with mock.patch.object(engine, "connect", _counting_connect):
            for _ in range(5):
                readiness.current()

        assert len(connects) == 1

    def test_the_clock_advancing_allows_another_look(self) -> None:
        engine = _legacy_engine()
        ticks = iter([100.0, 100.0, 131.0])
        readiness = migrations.SchemaReadiness(
            report=self._closed(),
            db_engine=engine,
            recheck_seconds=30.0,
            clock=lambda: next(ticks),
        )
        connects: list[int] = []
        real_connect = engine.connect

        def _counting_connect(*args: object, **kwargs: object) -> object:
            connects.append(1)
            return real_connect(*args, **kwargs)

        with mock.patch.object(engine, "connect", _counting_connect):
            readiness.current()
            readiness.current()
            readiness.current()

        assert len(connects) == 2

    def test_it_never_acquires_the_lock_or_emits_ddl(self) -> None:
        """Read-only, so it cannot race the migration it is observing."""
        engine = _legacy_engine()
        readiness = migrations.SchemaReadiness(report=self._closed(), db_engine=engine)
        acquisitions: list[str] = []

        with mock.patch.object(
            migrations,
            "_acquire_lock",
            lambda **k: acquisitions.append(k["dialect"]) or True,
        ):
            readiness.current()

        assert acquisitions == []
        # And the legacy table is untouched: no column was added behind the gate.
        with engine.connect() as conn:
            assert "schedule_path" not in migrations._inspect(conn=conn).columns


class TestNoStartupMetadataReadRunsOutsideTheBound:
    """The wrapper's claim has to be true of the FIRST statement too.

    `expand_schema` used to answer "does the table exist?" before entering the
    bounded block, so on a deployed database the very first metadata read waited
    at the server default while the comment beside it said everything was
    covered (pi-41). A fresh database now pays one recycled connection for that,
    once, at the only boot where the table is absent.
    """

    def test_the_existence_check_happens_inside_the_bounded_block(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine = _legacy_engine()
        order: list[str] = []
        real_inspect = migrations._inspect

        @contextlib.contextmanager
        def _fake_wait(*, conn: object, dialect: str) -> typing.Iterator[None]:
            order.append("bound")
            yield
            order.append("restored")

        monkeypatch.setattr(migrations, "_bounded_metadata_lock_wait", _fake_wait)
        monkeypatch.setattr(
            migrations,
            "_inspect",
            lambda **kwargs: order.append("read") or real_inspect(**kwargs),
        )

        migrations.expand_schema(db_engine=engine)

        assert order[0] == "bound", f"a metadata read ran before the bound: {order}"
        assert "read" in order[1:-1]
        assert (
            order[-1] == "restored"
        ), f"a metadata read ran after the restore: {order}"

    def test_an_absent_table_is_still_reported_from_inside_the_block(
        self,
    ) -> None:
        """Moving the check must not change what a fresh database reports."""
        engine = _empty_engine()

        report = migrations.expand_schema(db_engine=engine)

        assert report.status_of("table") is migrations.StepStatus.SKIPPED


class _FakeMySqlConnection(_RecordingConnection):
    """A `_RecordingConnection` that also answers `conn.dialect.name`.

    The readiness re-check branches on the dialect of the connection it is
    handed, and SQLite cannot reproduce a metadata lock, a session variable or
    ProxySQL. So the MySQL behaviour is asserted the way the rest of this file
    asserts MySQL behaviour: by the statements emitted, in order.
    """

    class _Dialect:
        name = "mysql"

    def __init__(self, scalar_results: list[int | None] | None = None) -> None:
        super().__init__(scalar_results=scalar_results)
        self.dialect = self._Dialect()


class _FakeMySqlEngine:
    """Hands out one recording connection and counts pool replacements."""

    def __init__(
        self, conn: _FakeMySqlConnection, *, exit_raises: bool = False
    ) -> None:
        self.conn = conn
        self.disposals = 0
        #: `Connection.__exit__` can raise on the way out -- closing a connection
        #: whose driver is already unhappy is exactly when it does -- and that
        #: replaces whatever was propagating.
        self._exit_raises = exit_raises

    @contextlib.contextmanager
    def connect(self) -> typing.Iterator[_FakeMySqlConnection]:
        try:
            yield self.conn
        finally:
            # In a `finally`, because that is where a real exit closes the
            # connection: the failure lands while another exception is already
            # propagating, and replaces it.
            if self._exit_raises:
                raise RuntimeError("close() failed on the way out")

    def dispose(self) -> None:
        self.disposals += 1


class TestTheRequestRecheckCannotStallOnTheMetadataLock:
    """Regression: the re-check read the schema unbounded.

    Startup wraps its metadata reads in `_bounded_metadata_lock_wait`; the
    request-time refresh called `_inspect` on a bare connection. The inspector's
    reflection takes a shared metadata lock on the table, so a refresh arriving
    behind the migration's pending exclusive request waited at the server
    default -- effectively forever -- while holding the readiness latch. The
    latch is a blast radius (other requests return the previous verdict at
    once), not a bound: it never shortened the wait the holder was in.

    None of this is reproducible on SQLite, which has no metadata locks, no
    session variables and no ProxySQL in front of it. These tests therefore
    assert the emitted statements and the connection's fate, which is the same
    standard `TestMySqlDdl` holds startup to.
    """

    @staticmethod
    def _fully_ready_shape() -> migrations.LiveShape:
        engine = _target_engine()
        with engine.connect() as conn:
            return migrations._inspect(conn=conn)

    def _readiness(
        self, engine: _FakeMySqlEngine, *, recheck_seconds: float = 30.0
    ) -> migrations.SchemaReadiness:
        return migrations.SchemaReadiness(
            report=migrations.MigrationReport(dialect="mysql"),
            db_engine=typing.cast(sqlalchemy.Engine, engine),
            recheck_seconds=recheck_seconds,
        )

    def test_the_reads_run_bounded_and_the_connection_never_returns_to_the_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The whole fix, asserted as one exact sequence.

        Order is the claim: the bound has to be installed BEFORE the reflection
        (a bound applied afterwards protects nothing), restored after it, and the
        connection discarded rather than pooled -- the session-variable
        statements may cost ProxySQL multiplexing permanently, which restoring
        the value does not undo.
        """
        shape = self._fully_ready_shape()
        conn = _FakeMySqlConnection(scalar_results=[50])
        engine = _FakeMySqlEngine(conn)
        monkeypatch.setattr(
            migrations,
            "_inspect",
            lambda **_: conn.events.append("<reflect>") or shape,
        )

        current = self._readiness(engine).current()

        assert conn.events == [
            "SELECT @@SESSION.lock_wait_timeout",
            f"SET SESSION lock_wait_timeout = {migrations._MYSQL_LOCK_WAIT_TIMEOUT_SECONDS}",
            "<reflect>",
            "SET SESSION lock_wait_timeout = 50",
            "<invalidate>",
        ], "the bound, the read, the restore and the recycle must happen in that order"
        # Not vacuous: the re-check still does its job through the new wrapper.
        assert current.path_writes_ready and current.reference_writes_ready

    def test_it_emits_no_ddl_and_takes_no_advisory_lock(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Bounding the read must not have turned it into a writer.

        The re-check is safe to run under a live migration only because it
        observes. `_bounded_metadata_lock_wait` is shared with the DDL path, so
        the prohibition is re-asserted on this side of it.
        """
        shape = self._fully_ready_shape()
        conn = _FakeMySqlConnection(scalar_results=[50])
        monkeypatch.setattr(migrations, "_inspect", lambda **_: shape)
        acquisitions: list[str] = []
        monkeypatch.setattr(
            migrations,
            "_acquire_lock",
            lambda **k: acquisitions.append(k["dialect"]) or True,
        )

        self._readiness(_FakeMySqlEngine(conn)).current()

        assert acquisitions == []
        assert all(
            "lock_wait_timeout" in statement for statement in conn.statements
        ), conn.statements

    def test_a_blocked_read_keeps_the_previous_verdict_and_still_cleans_up(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """What the bound buys: the failure a stall becomes.

        `lock_wait_timeout` turns the stall into `Lock wait timeout exceeded`,
        which must read as "no answer" -- previous verdict stands -- and must
        still put the session back and recycle the connection on the way out.
        """
        conn = _FakeMySqlConnection(scalar_results=[50])
        engine = _FakeMySqlEngine(conn)

        def _timed_out(**_: object) -> migrations.LiveShape:
            raise sqlalchemy.exc.OperationalError(
                "SHOW CREATE TABLE", {}, Exception("Lock wait timeout exceeded")
            )

        monkeypatch.setattr(migrations, "_inspect", _timed_out)

        current = self._readiness(engine).current()

        assert current.path_writes_ready is False
        assert current.reference_writes_ready is False
        assert conn.statements[-1] == "SET SESSION lock_wait_timeout = 50"
        assert conn.invalidated
        # An ordinary "the database did not answer" outcome: quiet, not fatal.
        assert engine.disposals == 0

    def test_a_connection_that_cannot_be_recycled_is_not_answered_quietly(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fail-closed covers the verdict, not the pool.

        A connection that could not be proven unpinned is a different class of
        problem from a timeout, and the broad `except` here used to flatten the
        two into the same warning (pi-41). It raises instead, and the pool it
        would have been handed back to is replaced first.
        """
        shape = self._fully_ready_shape()

        class _Unrecyclable(_FakeMySqlConnection):
            def invalidate(self) -> None:
                raise RuntimeError("no")

            def detach(self) -> None:
                raise RuntimeError("nor this")

        conn = _Unrecyclable(scalar_results=[50])
        engine = _FakeMySqlEngine(conn)
        monkeypatch.setattr(migrations, "_inspect", lambda **_: shape)

        with pytest.raises(
            migrations.SchedulerSchemaError, match="cannot be proven unpinned"
        ):
            self._readiness(engine).current()

        assert engine.disposals == 1, "the pinned record could be handed back out again"

    def test_a_raising_exit_cannot_quiet_a_failed_recycle(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The reason has to survive the unwind.

        `Connection.__exit__` closing a connection the driver has already given
        up on raises its own error, which REPLACES the recycle failure. Keyed on
        an exception type, the outer handler would then see an ordinary
        RuntimeError, keep the previous verdict quietly, and never replace the
        pool -- so the fact is tracked as state instead (pi-41).
        """
        shape = self._fully_ready_shape()

        class _Unrecyclable(_FakeMySqlConnection):
            def invalidate(self) -> None:
                raise RuntimeError("no")

            def detach(self) -> None:
                raise RuntimeError("nor this")

        engine = _FakeMySqlEngine(_Unrecyclable(scalar_results=[50]), exit_raises=True)
        monkeypatch.setattr(migrations, "_inspect", lambda **_: shape)

        with pytest.raises(RuntimeError, match="close\\(\\) failed on the way out"):
            self._readiness(engine).current()

        assert (
            engine.disposals == 1
        ), "the pool was kept after a connection could not be recycled"

    def test_sqlite_is_neither_bounded_nor_recycled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cost is paid where the hazard is. SQLite has neither.

        Charging a discarded connection per re-check on the local/dev dialect
        would churn the pool for a metadata lock that does not exist there.
        """
        engine = _legacy_engine()
        discards: list[str] = []
        monkeypatch.setattr(
            migrations,
            "_discard_connection",
            lambda **_: discards.append("discard"),
        )
        inspections: list[int] = []
        real_inspect = migrations._inspect
        monkeypatch.setattr(
            migrations,
            "_inspect",
            lambda **kwargs: inspections.append(1) or real_inspect(**kwargs),
        )

        migrations.SchemaReadiness(
            report=migrations.MigrationReport(dialect="sqlite"),
            db_engine=engine,
        ).current()

        assert inspections, "the re-check never ran, so this proves nothing"
        assert discards == []


class TestTheOneWayRuleSurvivesACrossedRefresh:
    """pi-38 and pi-41 both found this independently, and my own test missed it.

    The first implementation asked only "did SOME tier open?" and then swapped
    the whole report in. A refresh that opened one tier while reading the other
    closed therefore withdrew a tier that was already open and already acted on --
    the exact thing the one-way contract forbids. The original test refreshed an
    open-path report to *both closed*, which fails the "some tier opened" check
    and so never reached the replacement branch at all.
    """

    class _Report:
        """A stand-in: `MigrationReport` derives readiness from recorded steps,
        and the crossed states below are not reachable by recording real ones."""

        def __init__(self, *, path: bool, reference: bool) -> None:
            self.path_writes_ready = path
            self.reference_writes_ready = reference

    def _readiness(
        self, *, old: _Report, fresh: _Report | None
    ) -> migrations.SchemaReadiness:
        readiness = migrations.SchemaReadiness(
            report=typing.cast(migrations.MigrationReport, old),
            db_engine=typing.cast(sqlalchemy.Engine, object()),
            recheck_seconds=0.0,
        )
        readiness._verify_read_only = lambda: typing.cast(  # type: ignore[method-assign]
            migrations.MigrationReport | None, fresh
        )
        return readiness

    def test_opening_reference_must_not_close_path(self) -> None:
        old = self._Report(path=True, reference=False)
        crossed = self._Report(path=False, reference=True)

        current = self._readiness(old=old, fresh=crossed).current()

        assert current.path_writes_ready, "an open tier was withdrawn"

    def test_opening_path_must_not_close_reference(self) -> None:
        """The mirror direction, so a fix for one case cannot miss the other."""
        old = self._Report(path=False, reference=True)
        crossed = self._Report(path=True, reference=False)

        current = self._readiness(old=old, fresh=crossed).current()

        assert current.reference_writes_ready, "an open tier was withdrawn"

    def test_a_strictly_better_report_is_still_adopted(self) -> None:
        """The rule must not be so strict that it never updates anything."""
        old = self._Report(path=True, reference=False)
        better = self._Report(path=True, reference=True)

        current = self._readiness(old=old, fresh=better).current()

        assert current.reference_writes_ready

    def test_a_wholly_worse_report_is_rejected(self) -> None:
        old = self._Report(path=True, reference=False)
        worse = self._Report(path=False, reference=False)

        current = self._readiness(old=old, fresh=worse).current()

        assert current.path_writes_ready

    def test_a_second_thread_does_not_queue_behind_the_metadata_read(
        self,
    ) -> None:
        """One instance is shared by every request; the loser returns, not waits."""
        old = self._Report(path=False, reference=False)
        opened = self._Report(path=True, reference=True)
        readiness = self._readiness(old=old, fresh=opened)

        entered = threading.Event()
        release = threading.Event()
        reads: list[int] = []

        def _slow_read() -> object:
            reads.append(1)
            entered.set()
            release.wait(timeout=5)
            return opened

        readiness._verify_read_only = _slow_read  # type: ignore[method-assign]

        winner = threading.Thread(target=readiness.current)
        winner.start()
        assert entered.wait(timeout=5)

        # While the winner holds the lock inside the read, this must return now.
        started = time.monotonic()
        during = readiness.current()
        elapsed = time.monotonic() - started

        release.set()
        winner.join(timeout=5)

        assert elapsed < 1.0, "a request blocked on another request's metadata read"
        assert during.path_writes_ready is False, "returned a not-yet-published verdict"
        assert len(reads) == 1, "the rate limit was defeated by concurrency"
        assert readiness.current().path_writes_ready is True


class TestTheCollationNameIsParsedNotMatched:
    """Which names promise case-sensitive comparison, and why position decides.

    This rule was wrong twice, in both directions, and the second was dangerous.

    `endswith(("_bin", "_cs"))` refuses `utf8mb4_ja_0900_as_cs_ks` -- accent-,
    case- and kana-sensitive -- so a fail-closed check would have closed path
    writes on a database that was already correct. Widening to token membership
    fixed that and accepted `utf8mb4_cs_0900_ai_ci`, the CZECH collation, where
    `cs` is the LOCALE and the trailing `ai_ci` is the sensitivity. It folds
    case, and membership called it strict, which would have opened path writes
    against a column that still aliases 'Foo' and 'foo'.
    """

    @pytest.mark.parametrize(
        "collation",
        [
            "utf8mb4_0900_bin",
            "utf8mb4_bin",
            "latin1_bin",
            "utf8mb4_0900_as_cs",
            # The `_ks` family the first rule wrongly refused.
            "utf8mb4_ja_0900_as_cs_ks",
            "UTF8MB4_0900_BIN",
            # Czech AND case-sensitive: `cs` appears twice, as locale and as
            # sensitivity. Only the terminal one decides.
            "utf8mb4_cs_0900_as_cs",
        ],
    )
    def test_a_terminal_sensitivity_is_read(self, collation: str) -> None:
        assert migrations._is_case_sensitive_collation(collation)

    @pytest.mark.parametrize(
        "collation",
        [
            "utf8mb4_0900_ai_ci",
            "utf8mb4_general_ci",
            "utf8mb4_unicode_ci",
            # The dangerous one. `cs` here is the Czech locale.
            "utf8mb4_cs_0900_ai_ci",
            # Kana-sensitive but case-INsensitive: the same trap with `_ks`
            # present.
            "utf8mb4_ja_0900_as_ci_ks",
            # Lookalikes a containment rule would accept.
            "utf8mb4_cscz_ci",
            "utf8mb4_binary_ci",
            # An ending nobody here recognises is not evidence of strictness.
            "utf8mb4_0900_something",
        ],
    )
    def test_anything_else_folds(self, collation: str) -> None:
        assert not migrations._is_case_sensitive_collation(collation)

    @pytest.mark.parametrize(
        ("collation", "sensitive"),
        [
            # `bin` is terminal or it is not a sensitivity: a locale or version
            # token spelled the same way must not be read as one.
            ("utf8mb4_bin_0900_ai_ci", False),
            # Only `ks` may follow the case token.
            ("utf8mb4_0900_as_cs_xx", False),
            ("utf8mb4_0900_as_cs_ks", True),
        ],
    )
    def test_only_the_terminal_grammar_counts(
        self, collation: str, sensitive: bool
    ) -> None:
        """Asserted as a rule, not as a claim that each name exists.

        Twice the reasoning that failed was "no real collation looks like that",
        so the parser's contract is tested directly rather than only against the
        names anyone could recall. Enumerating MySQL 8.0's 91 `utf8mb4_*`
        collations yields four terminal shapes -- `_ci`, `_cs`, `_cs_ks`, `_bin`
        -- and exactly one case-insensitive name carrying a nonterminal `cs`.
        """
        assert migrations._is_case_sensitive_collation(collation) is sensitive

    def test_the_empty_name_is_not_mistaken_for_strict(self) -> None:
        assert not migrations._is_case_sensitive_collation("")


def _live_identity_column(
    target: "migrations._CollationTarget", **overrides: object
) -> dict[str, object]:
    """A reflected column matching what the model declares."""
    return {
        "name": target.column,
        "type": sqlalchemy.VARCHAR(target.length),
        "nullable": target.nullable,
        "default": None,
    } | overrides


def _collation_shape(
    collation: str | None,
    *,
    only: str | None = None,
    columns: dict[str, dict[str, object]] | None = None,
) -> "migrations.LiveShape":
    """A shape whose identity columns all carry `collation`.

    `only` narrows it to one column and gives the others a passing value, for the
    tests that need exactly one half to be wrong. `columns` overrides the
    reflected definitions, for the tests about definition drift.
    """
    return migrations.LiveShape(
        exists=True,
        columns=(
            columns
            if columns is not None
            else {
                t.column: _live_identity_column(t)
                for t in migrations._COLLATION_TARGETS
            }
        ),
        indexes={},
        checks={},
        foreign_keys={},
        collations={
            target.column: (
                collation if only in (None, target.column) else "utf8mb4_0900_bin"
            )
            for target in migrations._COLLATION_TARGETS
        },
    )


def _identity_target(column: str) -> "migrations._CollationTarget":
    return next(t for t in migrations._COLLATION_TARGETS if t.column == column)


class TestTheConversionRefusesADefinitionItHasNotVerified:
    """`MODIFY COLUMN` replaces a definition; it does not adjust one.

    So the statement imposes the model's width, nullability and absence of a
    default on whatever is live. Every other object in this module treats the
    live shape as a fail-closed contract, and this statement was the one that
    assumed it (pi-38).
    """

    def test_a_matching_definition_is_allowed_through(self) -> None:
        """The paired positive: refusing everything would pass every test below."""
        for target in migrations._COLLATION_TARGETS:
            assert (
                migrations._refuse_unexpected_definition(
                    _collation_shape("utf8mb4_0900_ai_ci"), target
                )
                is None
            )

    @pytest.mark.parametrize(
        ("override", "expected"),
        [
            # Truncation. The live column is wider than the model, and the
            # rewrite would silently cut the data down.
            ({"type": sqlalchemy.VARCHAR(512)}, "expected length"),
            # Nullability drift in either direction. `MODIFY COLUMN` restates
            # it, so copying the model's value over a live column that disagrees
            # either changes the meaning of existing rows or fails on the first
            # NULL, mid-ALTER. `schedule_path` is nullable, so the drift to
            # refuse here is a live NOT NULL.
            ({"nullable": False}, "expected nullable=True"),
            # A dropped default breaks a previous image inserting without the
            # column.
            ({"default": "'unknown'"}, "unexpected server default"),
            ({"type": sqlalchemy.TEXT()}, "expected a VARCHAR column"),
        ],
    )
    def test_a_drifted_definition_is_refused(
        self, override: dict[str, object], expected: str
    ) -> None:
        target = _identity_target("schedule_path")
        columns = {
            t.column: (
                _live_identity_column(t, **override)
                if t.column == "schedule_path"
                else _live_identity_column(t)
            )
            for t in migrations._COLLATION_TARGETS
        }

        reason = migrations._refuse_unexpected_definition(
            _collation_shape("utf8mb4_0900_ai_ci", columns=columns), target
        )

        assert reason is not None
        assert expected in reason
        assert "MODIFY COLUMN rewrites the whole definition" in reason
        assert "Nothing was modified" in reason

    def test_a_drift_blocks_rather_than_skips_and_emits_no_ddl(self) -> None:
        """SKIPPED would wait for a boot that never clears it."""
        report = migrations.MigrationReport(dialect="mysql")
        conn = _RecordingConnection(scalar_results=[None, None])
        columns = {
            t.column: _live_identity_column(
                t,
                **({"nullable": False} if t.column == "schedule_path" else {}),
            )
            for t in migrations._COLLATION_TARGETS
        }
        drifted = _collation_shape("utf8mb4_0900_ai_ci", columns=columns)

        with mock.patch.object(migrations, "_inspect", return_value=drifted):
            migrations._expand_collation(conn=conn, report=report)

        emitted = " ".join(conn.statements).upper()
        assert "MODIFY COLUMN SCHEDULE_PATH" not in emitted
        blocked = next(
            s
            for s in report.steps
            if s.name == migrations._collation_step("schedule_path")
        )
        assert blocked.status is migrations.StepStatus.BLOCKED
        assert not report.path_writes_ready

    def test_an_already_converted_column_is_not_preflighted(self) -> None:
        """Nothing is being rewritten, so a drift is not this step's business.

        Complaining here would block a tier over a column the step would never
        touch, which is a refusal with no remedy attached to it.
        """
        report = migrations.MigrationReport(dialect="mysql")
        conn = _RecordingConnection(scalar_results=[None, None])
        columns = {
            t.column: _live_identity_column(
                t,
                **(
                    {"type": sqlalchemy.VARCHAR(512)}
                    if t.column == "schedule_path"
                    else {}
                ),
            )
            for t in migrations._COLLATION_TARGETS
        }

        with mock.patch.object(
            migrations,
            "_inspect",
            return_value=_collation_shape("utf8mb4_0900_bin", columns=columns),
        ):
            migrations._expand_collation(conn=conn, report=report)

        step = next(
            s
            for s in report.steps
            if s.name == migrations._collation_step("schedule_path")
        )
        assert step.status is not migrations.StepStatus.BLOCKED


class TestReadingTheCollationsIsAllOrNothing:
    """A partial answer must not read as a partial pass.

    Written when there were two target columns and a read could genuinely come
    back half-answered. There is one target now, so some of this is insurance
    rather than a live failure mode -- kept, because `_read_collations` is
    written per-target and the one-target case is the one where a
    fall-back-to-the-pass-value bug is least visible.
    """

    #: Whatever the module currently converts. Named through the module so
    #: adding a target does not quietly narrow these tests to the old one.
    TARGET_COLUMNS = frozenset(t.column for t in migrations._COLLATION_TARGETS)

    def test_a_failed_read_reports_every_target_unreadable(self) -> None:
        """Unreadable is CONFLICTS, and a column silently passing would hide it."""

        class _Failing:
            dialect = types.SimpleNamespace(name="mysql")

            def execute(self, *_args: object, **_kwargs: object) -> None:
                raise sqlalchemy.exc.OperationalError(
                    "SELECT", {}, Exception("no access")
                )

        collations = migrations._read_collations(
            conn=_Failing(),  # type: ignore[arg-type]
            present={"created_by", "schedule_path"},
        )

        assert collations == dict.fromkeys(self.TARGET_COLUMNS)

    def test_a_column_the_server_did_not_answer_for_is_unreadable_not_passing(
        self,
    ) -> None:
        """Falling back to the pass value here would report ready on a missing row.

        The server answers about `created_by` -- a real column, and one this
        module used to convert -- and says nothing about the column actually
        asked for. An implementation that keyed off "the query succeeded" rather
        than "this column appeared in the result" passes a folding path column.
        """

        class _Partial:
            dialect = types.SimpleNamespace(name="mysql")

            def execute(self, *_args: object, **_kwargs: object) -> object:
                return types.SimpleNamespace(
                    all=lambda: [("created_by", "utf8mb4_0900_bin")]
                )

        collations = migrations._read_collations(
            conn=_Partial(),  # type: ignore[arg-type]
            present={"created_by", "schedule_path"},
        )

        assert collations == {"schedule_path": None}

    def test_an_untargeted_column_is_not_reported_on(self) -> None:
        """`created_by` is present in the table and is not this module's business.

        It was a target one commit ago. Reporting a collation for it would leave
        a verdict lying around for a column nothing gates on, and the tier logic
        keys off the presence of these entries.
        """

        class _Answering:
            dialect = types.SimpleNamespace(name="mysql")

            def execute(self, *_args: object, **_kwargs: object) -> object:
                return types.SimpleNamespace(
                    all=lambda: [
                        ("created_by", "utf8mb4_0900_ai_ci"),
                        ("schedule_path", "utf8mb4_0900_bin"),
                    ]
                )

        collations = migrations._read_collations(
            conn=_Answering(),  # type: ignore[arg-type]
            present={"created_by", "schedule_path"},
        )

        assert "created_by" not in collations
        assert collations["schedule_path"] == "utf8mb4_0900_bin"

    def test_a_column_absent_from_the_table_is_left_out(self) -> None:
        """The column checks own that failure and name it precisely."""

        class _Unused:
            dialect = types.SimpleNamespace(name="mysql")

            def execute(self, *_args: object, **_kwargs: object) -> object:
                return types.SimpleNamespace(
                    all=lambda: [("created_by", "utf8mb4_0900_bin")]
                )

        collations = migrations._read_collations(
            conn=_Unused(),  # type: ignore[arg-type]
            present={"created_by"},
        )

        assert "schedule_path" not in collations

    def test_one_statement_covers_every_target(self) -> None:
        """Not an optimisation: a half-failed read must not look like a half-answer."""
        executed: list[str] = []

        class _Recorder:
            dialect = types.SimpleNamespace(name="mysql")

            def execute(self, statement: object, *_args: object) -> object:
                executed.append(str(statement))
                return types.SimpleNamespace(all=list)

        migrations._read_collations(
            conn=_Recorder(), present={"created_by", "schedule_path"}
        )  # type: ignore[arg-type]

        assert len(executed) == 1
        assert "information_schema.COLUMNS" in executed[0]
        # Not SHOW CREATE TABLE: it prints COLLATE only when a column OVERRIDES
        # its table default, so a column inheriting a folding default -- the
        # state this exists to detect -- looks unremarkable there.
        assert "SHOW CREATE" not in executed[0].upper()


class TestTheCollationIsVerifiedAndGated:
    """The upgraded-database half: a live column that folds case must close path writes.

    `create_all` gives a fresh MySQL database the right collation, so the only
    way to be wrong is by UPGRADE -- a column that already exists and inherits a
    case-folding table default. Reflection does not carry collation, so nothing
    in `LiveShape`'s reflected fields can notice, which is why it is read from
    `information_schema` and carried on the shape explicitly.
    """

    def test_a_table_too_large_to_convert_is_left_alone_and_stays_closed(
        self,
    ) -> None:
        """The rollout-safety half, and the one SQLite can structurally prove.

        A collation change rebuilds the column and every index over it, so on a
        large table it would stall the boot it is running inside. The gate must
        therefore do three things together, and asserting fewer would let the
        dangerous combination through: emit NO `ALTER`, report the step SKIPPED
        rather than applied, and leave path writes CLOSED. Skipping while
        reporting the tier open would be the worst outcome available -- it would
        accept path writes against the very column it just declined to fix.
        """
        report = migrations.MigrationReport(dialect="mysql")
        # One row found past the limit is how the bounded probe says "too large";
        # it never counts the table.
        conn = _RecordingConnection(scalar_results=[1])

        with mock.patch.object(
            migrations,
            "_inspect",
            return_value=_collation_shape("utf8mb4_0900_ai_ci"),
        ):
            migrations._expand_collation(conn=conn, report=report)

        emitted = " ".join(conn.statements).upper()
        assert "ALTER TABLE" not in emitted
        assert "COLLATE" not in emitted
        # Bounded, not a census: it asks whether a row exists past the limit.
        assert f"OFFSET {migrations._MAX_FK_COPY_ROWS}" in " ".join(conn.statements)
        assert "COUNT(" not in emitted

        skipped = [
            step for step in report.steps if step.name in migrations._COLLATION_STEPS
        ]
        assert len(skipped) == len(self.TARGETS)
        for step in skipped:
            assert step.status is migrations.StepStatus.SKIPPED
            assert str(migrations._MAX_FK_COPY_ROWS) in step.detail
        assert not report.path_writes_ready

    def test_a_small_table_does_get_converted(self) -> None:
        """The gate must not be a permanent refusal dressed up as caution.

        Paired with the test above deliberately: one asserting no ALTER is
        emitted passes just as well against a step that never converts anything.

        Asserts the STEP OUTCOMES, not just that SQL went out. An earlier version
        mocked the post-ALTER shape as `utf8mb4_bin` and checked only that a
        `MODIFY COLUMN` was emitted for each target. When the (since reverted)
        owner rule tightened to require an exact collation, that shape stopped
        being ready and the test went on passing, because a statement is emitted
        either way. It was measuring emission as a proxy for conversion (pi-38).
        The post-ALTER shape is the collation the model actually targets, and
        readiness is asserted rather than inferred.
        """
        report = migrations.MigrationReport(dialect="mysql")
        conn = _RecordingConnection(scalar_results=[None, None])
        folding = _collation_shape("utf8mb4_0900_ai_ci")
        converted = _collation_shape(db_models.SCHEDULE_PATH_COLLATION)
        # One inspection up front, then `_apply` re-verifies per column: before
        # the ALTER it still reads as folding, after it as converted. Two shapes
        # per target plus the initial inspection.
        shapes = iter(
            [
                folding,
                *([folding, converted] * len(migrations._COLLATION_TARGETS)),
            ]
        )

        with mock.patch.object(
            migrations, "_inspect", side_effect=lambda **_: next(shapes)
        ):
            migrations._expand_collation(conn=conn, report=report)

        emitted = " ".join(conn.statements)
        for target in self.TARGETS:
            assert f"MODIFY COLUMN {target.column}" in emitted
        assert migrations._MYSQL_COPY in emitted

        by_name = {step.name: step for step in report.steps}
        for target in self.TARGETS:
            step = by_name[migrations._collation_step(target.column)]

            assert (
                step.status is migrations.StepStatus.APPLIED
            ), f"{target.column}: {step.detail}"

    def test_the_post_conversion_shape_this_suite_uses_is_one_the_rules_accept(
        self,
    ) -> None:
        """Guards the fixture itself, which is what silently went stale.

        The conversion test can only prove readiness is reachable if the shape it
        claims to reach is one both targets accept. Asserted against
        `_verify_collation` directly so a future tightening of either rule fails
        here -- loudly and in one place -- instead of quietly downgrading the
        positive path into another emission check.
        """
        for target in self.TARGETS:
            verdict, detail = migrations._verify_collation(
                _collation_shape(db_models.SCHEDULE_PATH_COLLATION), target
            )

            assert verdict is migrations._Verdict.MATCHES, f"{target.column}: {detail}"

    #: The columns this module holds to a collation. `schedule_path` and nothing
    #: else: `created_by` is deliberately left on the table default, because
    #: owner identity is not case-sensitive.
    TARGETS = migrations._COLLATION_TARGETS

    _shape = staticmethod(_collation_shape)
    _live_column = staticmethod(_live_identity_column)

    _target = staticmethod(_identity_target)

    def test_the_owner_column_is_deliberately_not_a_target(self) -> None:
        """The reverted change, pinned as reverted.

        `created_by` was added here on the premise that a unique key folds if
        either column folds, so an exact path beside a folding owner was "no fix
        at all for the pair". The premise assumed 'jose' and 'Jose' are two
        principals contending for one slot. They are one principal, so folding is
        the intended behaviour on that half and the conversion was excluding
        people from their own schedules.

        Asserted as an equality rather than a `not in`, so ADDING any column here
        also has to come past this test and its reasoning.

        The operational consequence, and the reason this is worth a test of its
        own: one target means ONE table-copying `ALTER` at boot, not two.
        """
        assert {t.column for t in self.TARGETS} == {"schedule_path"}

    @pytest.mark.parametrize("column", ["schedule_path"])
    def test_one_folding_column_closes_the_tier_even_if_the_other_is_exact(
        self, column: str
    ) -> None:
        report = migrations.MigrationReport(dialect="mysql")
        for name in migrations._WRITE_TIER_STEPS["path_writes"]():
            if name not in migrations._COLLATION_STEPS:
                report.add(name, migrations.StepStatus.APPLIED)
        migrations.record_verification(
            report=report,
            results=migrations.verify_collation(
                _collation_shape("utf8mb4_0900_ai_ci", only=column)
            ),
        )

        assert not report.path_writes_ready
        assert [s.name for s in report.tier_causes("path_writes")] == [
            migrations._collation_step(column)
        ]

    def test_the_path_consequence_describes_paths(self) -> None:
        """The operator has to be told what actually breaks, in caller terms."""
        verdict, detail = migrations._verify_collation(
            _collation_shape("utf8mb4_0900_ai_ci", only="schedule_path"),
            _identity_target("schedule_path"),
        )

        assert verdict is migrations._Verdict.ABSENT
        assert "Foo/Bar" in detail
        assert "schedule_path" in detail

    @pytest.mark.parametrize(
        "collation",
        ["utf8mb4_0900_bin", "utf8mb4_bin", "utf8mb4_0900_as_cs", "latin1_bin"],
    )
    def test_a_case_sensitive_collation_matches_for_the_ascii_column(
        self, collation: str
    ) -> None:
        """The PROPERTY is accepted for `schedule_path`, not one blessed name.

        A path is ASCII and trimmed, so over that repertoire any case-sensitive
        collation IS byte-exact, and a database already carrying one must not be
        failed -- or copied -- over a spelling.
        """
        verdict, _ = migrations._verify_collation(
            _collation_shape(collation), _identity_target("schedule_path")
        )

        assert verdict is migrations._Verdict.MATCHES

    def test_verification_asks_only_about_the_property(self) -> None:
        """No target may demand a collation by NAME.

        The reverted owner rule did: it required `utf8mb4_0900_bin` exactly,
        because an owner name has no restricted repertoire and "case-sensitive"
        therefore did not imply byte-exact. With that target gone, the module is
        back to one rule -- does this collation compare case-sensitively -- and a
        name-based rule reappearing would mean a new exactness premise arrived
        without one.

        Asserted through behaviour rather than by reading the dataclass, so it
        holds however the rule is expressed.
        """
        for target in self.TARGETS:
            for collation in (
                "utf8mb4_0900_bin",
                "utf8mb4_bin",
                "utf8mb4_0900_as_cs",
                "utf8mb4_ja_0900_as_cs_ks",
            ):
                verdict, detail = migrations._verify_collation(
                    _collation_shape(collation), target
                )

                assert (
                    verdict is migrations._Verdict.MATCHES
                ), f"{target.column}/{collation}: {detail}"

    @pytest.mark.parametrize(
        "collation",
        ["utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci", "utf8mb4_general_ci"],
    )
    def test_a_case_folding_collation_is_absent(self, collation: str) -> None:
        for target in self.TARGETS:
            verdict, detail = migrations._verify_collation(
                _collation_shape(collation), target
            )

            assert verdict is migrations._Verdict.ABSENT, target.column
            assert collation in detail
            assert target.column in detail

    def test_an_unreadable_collation_conflicts_rather_than_inviting_ddl(
        self,
    ) -> None:
        """ABSENT would invite a table-copying ALTER against an unknown column."""
        for target in self.TARGETS:
            verdict, detail = migrations._verify_collation(
                _collation_shape(None), target
            )

            assert verdict is migrations._Verdict.CONFLICTS, target.column
            assert "could not be read" in detail

    def test_a_dialect_without_collations_passes(self) -> None:
        """SQLite already compares bytes; failing it would close the whole suite."""
        for target in self.TARGETS:
            verdict, _ = migrations._verify_collation(
                _collation_shape(migrations._COLLATION_NOT_APPLICABLE), target
            )

            assert verdict is migrations._Verdict.MATCHES, target.column

    def test_the_step_is_part_of_the_single_verification_path(self) -> None:
        """A pod that only VERIFIES must check this too, or it reports ready wrongly."""
        steps = [
            step
            for step, *_ in migrations.verify_schema(
                _collation_shape("utf8mb4_0900_ai_ci")
            )
        ]

        for step in migrations._COLLATION_STEPS:
            assert step in steps

    def test_path_writes_depend_on_it_and_reference_writes_do_not(self) -> None:
        for step in migrations._COLLATION_STEPS:
            assert step in migrations._WRITE_TIER_STEPS["path_writes"]()
            assert step not in migrations._WRITE_TIER_STEPS["reference_writes"]()

    def test_a_folding_column_closes_path_writes_and_says_why(self) -> None:
        """The whole point: no path write over a column that cannot keep them apart."""
        report = migrations.MigrationReport(dialect="mysql")
        for name in migrations._WRITE_TIER_STEPS["path_writes"]():
            if name not in migrations._COLLATION_STEPS:
                report.add(name, migrations.StepStatus.APPLIED)
        migrations.record_verification(
            report=report,
            results=migrations.verify_collation(_collation_shape("utf8mb4_0900_ai_ci")),
        )

        assert not report.path_writes_ready
        assert [s.name for s in report.tier_causes("path_writes")] == list(
            migrations._COLLATION_STEPS
        )

    def test_a_case_sensitive_column_opens_them(self) -> None:
        report = migrations.MigrationReport(dialect="mysql")
        for name in migrations._WRITE_TIER_STEPS["path_writes"]():
            if name not in migrations._COLLATION_STEPS:
                report.add(name, migrations.StepStatus.APPLIED)
        migrations.record_verification(
            report=report,
            results=migrations.verify_collation(_collation_shape("utf8mb4_0900_bin")),
        )

        assert report.path_writes_ready

    def test_the_absence_hint_names_the_disabled_feature(self) -> None:
        for step in migrations._COLLATION_STEPS:
            hint = migrations._absence_hint(step=step, dialect="mysql")

            assert "case-insensitively" in hint
            assert "disabled" in hint

    def test_the_conversion_statement_names_charset_collation_and_algorithm(
        self,
    ) -> None:
        """Read off the emitted SQL, because each clause is there for a reason.

        `CHARACTER SET` because `COLLATE` alone errors when the column's current
        charset differs; the explicit algorithm so the server refuses rather than
        silently choosing something worse than the copy this already accepts.
        """
        statements: list[str] = []

        class _Recorder:
            def execute(self, statement: object, *_args: object) -> None:
                statements.append(str(statement))

            def commit(self) -> None:
                return None

        for target in self.TARGETS:
            migrations._apply_collation(
                conn=_Recorder(), dialect="mysql", target=target
            )  # type: ignore[arg-type]

        assert len(statements) == len(self.TARGETS)
        for target, emitted in zip(self.TARGETS, statements, strict=True):
            assert f"MODIFY COLUMN {target.column}" in emitted
            assert "CHARACTER SET utf8mb4" in emitted
            assert f"COLLATE {db_models.SCHEDULE_PATH_COLLATION}" in emitted
            assert f"VARCHAR({target.length})" in emitted
            assert migrations._MYSQL_COPY in emitted
            # MODIFY COLUMN rewrites the whole definition, so an omitted NOT NULL
            # silently makes the column nullable.
            assert emitted.count("NOT NULL") == (0 if target.nullable else 1)

    def test_the_conversion_refuses_an_unreviewed_dialect(self) -> None:
        for target in self.TARGETS:
            with pytest.raises(
                migrations.SchedulerSchemaError, match="no statement is defined"
            ):
                migrations._apply_collation(
                    conn=object(), dialect="postgresql", target=target
                )  # type: ignore[arg-type]

    def test_a_sqlite_run_records_the_step_as_satisfied(self) -> None:
        """End to end on the dialect the suite runs: the tier must actually open."""
        report = migrations.migrate_db(db_engine=_target_engine())

        assert report.path_writes_ready
        recorded = {s.name: s.status for s in report.steps}
        for step in migrations._COLLATION_STEPS:
            assert recorded[step] is migrations.StepStatus.ALREADY_PRESENT
