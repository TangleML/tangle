"""Unit tests for scheduling.pipelines.db_models."""

import datetime
import uuid

import pytest
import sqlalchemy
import sqlalchemy.dialects.mysql
import sqlalchemy.dialects.sqlite

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.scheduling.pipelines import (
    database_migrations as migrations,
)
from cloud_pipelines_backend.scheduling.pipelines import db_models
from tests.scheduling.pipelines.conftest import SAMPLE_PIPELINE_TASK_SPEC
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import pipeline_templates

_DIGEST = "a" * 64
# SQLite does not enforce foreign keys unless the pragma is set, and this engine
# does not set it, so the legacy pipeline-run source can be exercised without
# building an ExecutionNode graph. What is under test is the CHECK, not the FK.
_LEGACY_PIPELINE_RUN_ID = "legacy-run-id-000001"


def _make_deployment(
    *,
    session: sqlalchemy.orm.Session,
    user_id: str = "test@example.com",
    file_path: str = "pipelines/daily_pulse.yaml",
    digest: str = _DIGEST,
) -> str:
    """A saved pipeline with one immutable version, for a resolvable reference.

    The schedule now carries an FK to `pipeline.id`, so on a fresh database
    these rows are required for a reference to insert at all -- not merely
    useful for making the reference meaningful.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id=user_id,
        file_path=file_path,
        versioning_mode=user_pipeline_db_models.PipelineVersioningMode.FULL,
    )
    session.add(pipeline)
    session.flush()
    session.add(
        user_pipeline_db_models.UserPipelineVersion(
            pipeline_id=pipeline.id,
            version_key=digest,
            content_digest=digest,
            root_pipeline_task=SAMPLE_PIPELINE_TASK_SPEC,
        )
    )
    pipeline.current_version_key = digest
    session.commit()
    return pipeline.id


class TestScheduledPipelineRunModel:
    def test_create_and_read(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Test Schedule",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="test@example.com",
            )
            session.add(schedule)
            session.commit()

            assert len(schedule.id) == 20

            loaded = session.get(db_models.ScheduledPipelineRun, schedule.id)
            assert loaded is not None
            assert loaded.name == "Test Schedule"
            assert loaded.cron_expression == "0 9 * * *"
            assert loaded.timezone == "UTC"
            assert loaded.paused is False
            assert loaded.pipeline_task_spec == SAMPLE_PIPELINE_TASK_SPEC

    def test_defaults(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Defaults",
                cron_expression="* * * * *",
                pipeline_task_spec={},
                created_by="a@b.com",
            )
            session.add(schedule)
            session.commit()
            session.refresh(schedule)

            assert schedule.timezone == "UTC"
            assert schedule.paused is False
            assert schedule.last_run_at is None
            assert schedule.last_run_submission_result is None
            assert schedule.pipeline_task_spec_from_pipeline_run_id is None
            assert schedule.pipeline_task_spec_from_user_pipeline_id is None
            assert schedule.created_at is not None
            assert schedule.updated_at is not None

    def test_new_fk_columns_default_none(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="FK Defaults",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="a@b.com",
            )
            session.add(schedule)
            session.commit()
            session.refresh(schedule)

            assert schedule.pipeline_task_spec_from_pipeline_run_id is None
            assert schedule.pipeline_task_spec_from_user_pipeline_id is None

    def test_reference_columns_and_schedule_path_default_none(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Reference Defaults",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="a@b.com",
            )
            session.add(schedule)
            session.commit()
            session.refresh(schedule)

            # An unmigrated caller creating an inline schedule must land in a
            # valid state without knowing any of these columns exist.
            assert schedule.pipeline_task_spec_from_user_pipeline_version_key is None
            assert schedule.schedule_path is None

    def test_pipeline_reference_column_holds_a_full_uuid(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The placeholder was String(20); a pipeline id is a 36-char UUID."""
        column = db_models.ScheduledPipelineRun.__table__.c[
            "pipeline_task_spec_from_user_pipeline_id"
        ]
        assert column.type.length == 36

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            pipeline_id = _make_deployment(session=session)
            assert len(pipeline_id) == 36
            schedule = db_models.ScheduledPipelineRun(
                name="Current",
                cron_expression="0 9 * * *",
                created_by="a@b.com",
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            )
            session.add(schedule)
            session.commit()
            session.refresh(schedule)

            assert schedule.pipeline_task_spec_from_user_pipeline_id == pipeline_id
            assert schedule.pipeline_task_spec is None

    def test_pinned_row_stores_the_resolved_version_key(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            pipeline_id = _make_deployment(session=session)
            schedule = db_models.ScheduledPipelineRun(
                name="Pinned",
                cron_expression="0 9 * * *",
                created_by="a@b.com",
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
                pipeline_task_spec_from_user_pipeline_version_key=_DIGEST,
            )
            session.add(schedule)
            session.commit()
            session.refresh(schedule)

            assert schedule.pipeline_task_spec_from_user_pipeline_version_key == _DIGEST


class TestPreviousImageCompatibility:
    """An older application image must keep inserting successfully.

    Both added columns are nullable with no server default, so an image that
    knows about neither still writes a valid row: the invariant is satisfied by
    the inline source alone, and `schedule_path` is permanently NULL-able.
    """

    def test_an_insert_naming_only_the_original_columns_succeeds(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with db_engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO scheduled_pipeline_run"
                    " (id, name, cron_expression, timezone, pipeline_task_spec,"
                    "  paused, created_by, created_at, updated_at)"
                    " VALUES ('0123456789abcdef0123', 'Old image', '0 9 * * *',"
                    " 'UTC', '{}', 0, 'a@b.com', '2026-01-01 00:00:00',"
                    " '2026-01-01 00:00:00')"
                )
            )
            conn.commit()
            row = conn.execute(
                sqlalchemy.text(
                    "SELECT schedule_path,"
                    " pipeline_task_spec_from_user_pipeline_version_key"
                    " FROM scheduled_pipeline_run"
                )
            ).one()
        assert row == (None, None)

    def test_added_columns_are_nullable_without_a_server_default(self) -> None:
        """No default to keep in agreement with the data, and no backfill needed."""
        table = db_models.ScheduledPipelineRun.__table__
        for name in (
            "schedule_path",
            "pipeline_task_spec_from_user_pipeline_version_key",
        ):
            column = table.c[name]
            assert column.nullable
            assert column.server_default is None

    def test_schedule_path_is_nullable_and_stays_that_way(self) -> None:
        """Nullable is the end state, not a transitional one.

        There is no NOT NULL contract step: path presence is an API-level
        guarantee, and legacy NULLs coexist indefinitely.
        """
        assert db_models.ScheduledPipelineRun.__table__.c["schedule_path"].nullable

    def test_the_invariant_says_nothing_about_schedule_path(self) -> None:
        """Path adoption and inline reconciliation are independent concerns."""
        assert "schedule_path" not in db_models.SOURCE_INVARIANT_CHECK_SQL

    def test_no_mode_column_exists(self) -> None:
        """current vs pinned is derived from the version key, never persisted.

        A stored mode could disagree with the columns it describes; a derived one
        cannot.
        """
        assert "resolution_mode" not in db_models.ScheduledPipelineRun.__table__.c


class TestClearedInlineSpecIsSqlNull:
    """Clearing the spec must produce SQL NULL, not the JSON literal ``null``.

    SQLAlchemy's JSON type defaults to ``none_as_null=False``, which persists
    Python ``None`` as JSON ``null`` — a value that is NOT SQL NULL. With that
    default, every ``pipeline_task_spec IS NULL`` branch of the source CHECK is
    false for a cleared spec, so the constraint would reject every reference-mode
    row while accepting a body-less inline row. This is the regression test for
    that, because reads look identical either way.
    """

    def test_none_is_stored_as_sql_null(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            pipeline_id = _make_deployment(session=session)
            schedule = db_models.ScheduledPipelineRun(
                name="Cleared",
                cron_expression="0 9 * * *",
                created_by="a@b.com",
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
                pipeline_task_spec=None,
            )
            session.add(schedule)
            session.commit()

        with db_engine.connect() as conn:
            sql_nulls = conn.execute(
                sqlalchemy.text(
                    "SELECT COUNT(*) FROM scheduled_pipeline_run WHERE pipeline_task_spec IS NULL"
                )
            ).scalar_one()
            json_nulls = conn.execute(
                sqlalchemy.text(
                    "SELECT COUNT(*) FROM scheduled_pipeline_run WHERE pipeline_task_spec = 'null'"
                )
            ).scalar_one()
        assert sql_nulls == 1
        assert json_nulls == 0


class TestSourceInvariant:
    """Exactly one body source, and a version key only alongside a reference."""

    def _insert(self, *, db_engine: sqlalchemy.Engine, **kwargs) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            session.add(
                db_models.ScheduledPipelineRun(
                    name="Invariant",
                    cron_expression="0 9 * * *",
                    created_by="a@b.com",
                    **kwargs,
                )
            )
            session.commit()

    def test_no_source_at_all_is_rejected(self, db_engine: sqlalchemy.Engine) -> None:
        with pytest.raises(sqlalchemy.exc.IntegrityError, match="source"):
            self._insert(db_engine=db_engine, pipeline_task_spec=None)

    def test_inline_plus_pipeline_reference_is_rejected(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            pipeline_id = _make_deployment(session=session)
        with pytest.raises(sqlalchemy.exc.IntegrityError, match="source"):
            self._insert(
                db_engine=db_engine,
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            )

    def test_inline_plus_pipeline_run_reference_is_rejected(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        run_id = _LEGACY_PIPELINE_RUN_ID
        with pytest.raises(sqlalchemy.exc.IntegrityError, match="source"):
            self._insert(
                db_engine=db_engine,
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                pipeline_task_spec_from_pipeline_run_id=run_id,
            )

    def test_a_pipeline_run_reference_alone_is_still_legal(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The legacy third source has no writer today, but rows may use it.

        The invariant must keep them legal rather than quietly outlawing a shape
        that already exists in the table.
        """
        run_id = _LEGACY_PIPELINE_RUN_ID
        self._insert(
            db_engine=db_engine,
            pipeline_task_spec=None,
            pipeline_task_spec_from_pipeline_run_id=run_id,
        )

    def test_a_version_key_without_a_pipeline_reference_is_rejected(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with pytest.raises(sqlalchemy.exc.IntegrityError, match="source"):
            self._insert(
                db_engine=db_engine,
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                pipeline_task_spec_from_user_pipeline_version_key=_DIGEST,
            )

    def test_current_and_pinned_are_distinguished_by_the_version_key(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            pipeline_id = _make_deployment(session=session)
        self._insert(
            db_engine=db_engine,
            pipeline_task_spec=None,
            pipeline_task_spec_from_user_pipeline_id=pipeline_id,
        )
        self._insert(
            db_engine=db_engine,
            pipeline_task_spec=None,
            pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            pipeline_task_spec_from_user_pipeline_version_key=_DIGEST,
        )
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            rows = session.query(db_models.ScheduledPipelineRun).all()
            tracking_current = [
                r
                for r in rows
                if r.pipeline_task_spec_from_user_pipeline_version_key is None
            ]
            pinned = [
                r
                for r in rows
                if r.pipeline_task_spec_from_user_pipeline_version_key is not None
            ]
        assert len(tracking_current) == 1
        assert len(pinned) == 1


class TestSchedulePathUniqueness:
    def _add(
        self,
        *,
        session: sqlalchemy.orm.Session,
        created_by: str,
        schedule_path: str | None,
        name: str = "Path",
    ) -> None:
        session.add(
            db_models.ScheduledPipelineRun(
                name=name,
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by=created_by,
                schedule_path=schedule_path,
            )
        )
        session.commit()

    def test_same_path_same_owner_is_rejected(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            self._add(
                session=session,
                created_by="a@b.com",
                schedule_path="upi/nightly",
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                self._add(
                    session=session,
                    created_by="a@b.com",
                    schedule_path="upi/nightly",
                )

    def test_same_path_different_owner_is_allowed(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            self._add(
                session=session,
                created_by="a@b.com",
                schedule_path="upi/nightly",
            )
            self._add(
                session=session,
                created_by="c@d.com",
                schedule_path="upi/nightly",
            )

    def test_many_null_paths_are_allowed(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Legacy rows have no path, and NULLs are distinct in a unique index.

        This is what makes it safe to add the unique constraint before any
        writer is allowed to set `schedule_path`.
        """
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            for index in range(3):
                self._add(
                    session=session,
                    created_by="a@b.com",
                    schedule_path=None,
                    name=f"Legacy {index}",
                )


class TestNamedSchemaObjects:
    """The migration adds these by name, so the model must own the same names."""

    def test_expected_objects_are_declared(self) -> None:
        table = db_models.ScheduledPipelineRun.__table__
        assert {i.name for i in table.indexes} == {
            "ix_scheduled_pipeline_run_updated_at_desc_id_desc",
            "ix_scheduled_pipeline_run_user_pipeline_id_version_key",
            "ix_scheduled_pipeline_run_created_by_updated_at_id",
        }
        constraint_names = {c.name for c in table.constraints}
        assert {
            "uq_scheduled_pipeline_run_created_by_schedule_path",
            "ck_scheduled_pipeline_run_source",
        } <= constraint_names

    def test_the_owner_page_index_leads_with_the_owner_then_the_sort_key(
        self,
    ) -> None:
        """Column ORDER is the entire value of this index, so it is asserted.

        `(created_by, updated_at, id)` makes the owner equality a range and
        leaves the page's sort key ordered inside it. Any other permutation --
        `(updated_at, created_by, id)`, or dropping `id` -- still "contains
        created_by" and still lets the server read rows belonging to other users
        to fill a page, which is the cost the index exists to remove.

        Ascending on purpose under a descending ORDER BY: a backward index scan
        satisfies the exact reverse of a key without a filesort, and a descending
        index would be an object the startup verifier cannot check exactly --
        SQLAlchemy's MySQL Inspector does not expose key direction to it, only
        column names.
        """
        table = db_models.ScheduledPipelineRun.__table__
        index = next(
            i
            for i in table.indexes
            if i.name == "ix_scheduled_pipeline_run_created_by_updated_at_id"
        )

        assert [column.name for column in index.columns] == [
            "created_by",
            "updated_at",
            "id",
        ]
        assert not index.unique
        # Plain columns, not `column.desc()`: a sorted expression would compile to
        # a descending key that the migration's verifier never gets to see.
        assert all(
            isinstance(element, sqlalchemy.Column) for element in index.expressions
        )

    def test_the_saved_pipeline_column_carries_a_non_cascading_foreign_key(
        self,
    ) -> None:
        """Declared on the model, so a fresh database matches a migrated one.

        The migration installs the same constraint on an existing table under a
        size gate. Declaring it here is what keeps the two shapes identical; the
        earlier no-FK rule made a fresh database enforce *less* than production
        will, which is the same divergence in the other direction.
        """
        table = db_models.ScheduledPipelineRun.__table__
        constraint = next(
            c
            for c in table.constraints
            if isinstance(c, sqlalchemy.ForeignKeyConstraint)
            and c.name == "fk_scheduled_pipeline_run_user_pipeline_id"
        )

        assert [column.name for column in constraint.columns] == [
            "pipeline_task_spec_from_user_pipeline_id"
        ]
        assert [element.target_fullname for element in constraint.elements] == [
            "pipeline.id"
        ]
        # Never CASCADE: deleting a pipeline must not delete a customer's schedule.
        assert constraint.ondelete == "RESTRICT"
        assert constraint.onupdate == "RESTRICT"

    def test_the_version_pair_carries_no_foreign_key(self) -> None:
        """InnoDB MATCH SIMPLE would skip it whenever the version key is NULL.

        A NULL version key is the ordinary track-current mode, so a composite
        constraint would not check the common row while looking like it did.
        """
        table = db_models.ScheduledPipelineRun.__table__
        referencing = {
            column.name
            for constraint in table.constraints
            if isinstance(constraint, sqlalchemy.ForeignKeyConstraint)
            for column in constraint.columns
        }

        assert "pipeline_task_spec_from_user_pipeline_version_key" not in referencing

    def test_the_pre_existing_pipeline_run_foreign_key_is_retained(
        self,
    ) -> None:
        """Untouched by this change: it predates the reference work."""
        column = db_models.ScheduledPipelineRun.__table__.c[
            "pipeline_task_spec_from_pipeline_run_id"
        ]

        assert {fk.column.table.name for fk in column.foreign_keys} == {"pipeline_run"}

    def test_unique_index_key_fits_innodb(self) -> None:
        """(255 + 255) x 4 bytes utf8mb4 = 2040, inside the 3072-byte limit."""
        table = db_models.ScheduledPipelineRun.__table__
        widths = [
            table.c["created_by"].type.length,
            table.c["schedule_path"].type.length,
        ]
        assert sum(widths) * 4 <= 3072

    def test_a_pipeline_reference_survives_a_uuid_round_trip(self) -> None:
        assert len(str(uuid.uuid4())) <= db_models.PIPELINE_ID_LENGTH


class TestTimestampUtcRoundTrip:
    """Verify that timestamps are stored as UTC and read back as UTC-aware.

    SQLite does not natively store timezone metadata — it drops tzinfo on write.
    The UtcDateTime custom type re-attaches tzinfo=UTC on read, so the
    application always sees timezone-aware datetimes regardless of database.
    These tests confirm that behavior using the SQLite test engine.
    """

    def test_created_at_is_utc_aware(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="UTC Test",
                cron_expression="0 9 * * *",
                pipeline_task_spec={},
                created_by="a@b.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            loaded = session.get(db_models.ScheduledPipelineRun, sid)
            assert loaded.created_at.tzinfo is not None
            assert loaded.created_at.tzinfo == datetime.timezone.utc

    def test_updated_at_is_utc_aware(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="UTC Updated",
                cron_expression="0 9 * * *",
                pipeline_task_spec={},
                created_by="a@b.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            loaded = session.get(db_models.ScheduledPipelineRun, sid)
            assert loaded.updated_at.tzinfo is not None
            assert loaded.updated_at.tzinfo == datetime.timezone.utc

    def test_last_run_at_round_trips_as_utc(self, db_engine: sqlalchemy.Engine) -> None:
        known_time = datetime.datetime(
            2026, 6, 9, 14, 30, 0, tzinfo=datetime.timezone.utc
        )

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Last Run UTC",
                cron_expression="0 9 * * *",
                pipeline_task_spec={},
                created_by="a@b.com",
                last_run_at=known_time,
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            loaded = session.get(db_models.ScheduledPipelineRun, sid)
            assert loaded.last_run_at is not None
            assert loaded.last_run_at.tzinfo is not None
            assert loaded.last_run_at.tzinfo == datetime.timezone.utc
            assert loaded.last_run_at.year == 2026
            assert loaded.last_run_at.hour == 14
            assert loaded.last_run_at.minute == 30


class TestSchedulePathComparesCaseSensitively:
    """The database half of case-sensitive paths, which SQLite cannot demonstrate.

    The API tests prove `Foo/Bar` and `foo/bar` stay distinct on SQLite, whose
    `=` is byte comparison. On MySQL that distinctness depends entirely on the
    column's collation, and MySQL's default folds case -- so if the collation
    were dropped from the model, every API test would still pass and production
    would silently alias the two. These are the tests that fail instead.
    """

    @staticmethod
    def _compiled(dialect: object, column_name: str = "schedule_path") -> str:
        column = db_models.ScheduledPipelineRun.__table__.c[column_name]
        return column.type.compile(dialect)  # type: ignore[arg-type]

    def test_the_mysql_column_carries_a_case_sensitive_collation(self) -> None:
        rendered = self._compiled(sqlalchemy.dialects.mysql.dialect())

        assert "COLLATE" in rendered
        assert db_models.SCHEDULE_PATH_COLLATION in rendered

    def test_the_column_keeps_its_declared_width(self) -> None:
        """`with_variant` restates the type, so a mistyped length is a narrower column."""
        rendered = self._compiled(sqlalchemy.dialects.mysql.dialect())

        assert f"VARCHAR({bts._STR_MAX_LENGTH})" in rendered

    def test_the_declared_collation_is_actually_case_sensitive(self) -> None:
        """Guards the constant itself: a `_ci` value here would be silently wrong.

        Judged by the same parser the migration verifies live columns with, not
        by a suffix test. `endswith(("_bin", "_cs"))` was the earlier rule here
        and it is wrong in both directions -- it refuses `utf8mb4_ja_0900_as_cs_ks`
        and, once loosened to token membership, accepts the Czech
        `utf8mb4_cs_0900_ai_ci`, which folds case.
        """
        assert migrations._is_case_sensitive_collation(
            db_models.SCHEDULE_PATH_COLLATION
        )

    def test_sqlite_gets_no_collate_clause(self) -> None:
        """A `utf8mb4_*` collation is not a thing SQLite can create a table with."""
        rendered = self._compiled(sqlalchemy.dialects.sqlite.dialect())

        assert "COLLATE" not in rendered
        assert "utf8mb4" not in rendered

    def test_the_length_is_the_same_on_both(self) -> None:
        """The variant must not quietly change the width the index budget assumes."""
        for dialect in (
            sqlalchemy.dialects.mysql.dialect(),
            sqlalchemy.dialects.sqlite.dialect(),
        ):
            assert str(db_models.SCHEDULE_PATH_LENGTH) in self._compiled(dialect)


class TestTheOwnerColumnTakesTheTableDefault:
    """The other half of the mixed key, asserted so it cannot drift back.

    `created_by` was briefly pinned to the same binary collation as the path, on
    the premise that 'jose' and 'Jose' are two principals contending for one
    `(owner, path)` slot. User identity is not case-sensitive, so they are one
    principal, one slot is correct, and the pin excluded people from their own
    schedules on exactly the deployments it targeted.

    These are negative assertions on purpose. The reverted change is a coherent
    thing to re-derive from "a unique key is as strict as its loosest column",
    so the absence has to be pinned rather than left to be noticed.
    """

    @staticmethod
    def _compiled(dialect: object) -> str:
        column = db_models.ScheduledPipelineRun.__table__.c["created_by"]
        return column.type.compile(dialect)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "dialect",
        [
            sqlalchemy.dialects.mysql.dialect(),
            sqlalchemy.dialects.sqlite.dialect(),
        ],
        ids=["mysql", "sqlite"],
    )
    def test_no_collation_is_imposed_on_any_dialect(self, dialect: object) -> None:
        rendered = self._compiled(dialect)

        assert "COLLATE" not in rendered
        assert "utf8mb4" not in rendered

    def test_the_unique_key_still_covers_both_columns(self) -> None:
        """Mixed, not narrowed. Dropping the owner half is a different bug.

        The reverted change altered how the owner column COMPARES; it never
        changed which columns the key spans, and a later cleanup must not
        conflate the two.
        """
        constraint = next(
            c
            for c in db_models.ScheduledPipelineRun.__table__.constraints
            if getattr(c, "name", None)
            == "uq_scheduled_pipeline_run_created_by_schedule_path"
        )

        assert [c.name for c in constraint.columns] == [
            "created_by",
            "schedule_path",
        ]

    def test_the_two_halves_are_held_to_different_rules(self) -> None:
        """The contract in one assertion: mixed key, stated as such.

        Read as a pair because either half alone looks like an oversight.
        """
        mysql_dialect = sqlalchemy.dialects.mysql.dialect()
        owner = db_models.ScheduledPipelineRun.__table__.c["created_by"].type.compile(
            mysql_dialect
        )
        path = db_models.ScheduledPipelineRun.__table__.c["schedule_path"].type.compile(
            mysql_dialect
        )

        assert "COLLATE" not in owner
        assert f"COLLATE {db_models.SCHEDULE_PATH_COLLATION}" in path


class TestTheSettingsColumn:
    """The column exists to hold feature settings, and its migration entry has to ship
    with it."""

    @pytest.mark.parametrize(
        "stored",
        [None, {}, {"other_feature": 1}, {"pipeline_templates": {}}],
        ids=["null", "empty-column", "another-feature-only", "empty-envelope"],
    )
    def test_every_empty_state_reads_as_no_templates(
        self, db_engine: sqlalchemy.Engine, stored: dict[str, object] | None
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="s",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="test-owner",
                settings=stored,
            )
            session.add(schedule)
            session.commit()
            loaded = session.get(db_models.ScheduledPipelineRun, schedule.id)

        assert loaded is not None
        assert pipeline_templates.get_pipeline_templates(original=loaded.settings) == {}

    def test_a_populated_envelope_round_trips(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        envelope = {"arguments": {"as_of": "{{ schedule_time | date }}"}}

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="s",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="test-owner",
                settings=pipeline_templates.set_pipeline_templates(
                    original=None, updates=envelope
                ),
            )
            session.add(schedule)
            session.commit()
            loaded = session.get(db_models.ScheduledPipelineRun, schedule.id)

        assert loaded is not None
        assert (
            pipeline_templates.get_pipeline_templates(original=loaded.settings)
            == envelope
        )

    def test_the_column_defaults_to_null_rather_than_an_empty_dict(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """An added column is NULL for every row the previous image inserted, and a server
        default is what `_verify_column` refuses."""
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="s",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="test-owner",
            )
            session.add(schedule)
            session.commit()

            assert (
                session.get(db_models.ScheduledPipelineRun, schedule.id).settings
                is None
            )

    def test_the_column_has_a_migration_target_in_the_same_commit(self) -> None:
        """The failure this pins is invisible in CI by construction: `create_all` gives a
        fresh database every mapped column, so a column with no `_TARGET_COLUMNS` entry is
        green here and fatal in production, where `migrate_db` is the only thing that
        ALTERs and was never told to add it."""
        mapped = set(db_models.ScheduledPipelineRun.__table__.columns.keys())
        targets = {spec.name for spec in migrations._TARGET_COLUMNS}

        assert "settings" in mapped
        assert "settings" in targets

    def test_the_settings_target_is_a_json_spec_not_a_varchar_one(self) -> None:
        spec = next(s for s in migrations._TARGET_COLUMNS if s.name == "settings")

        assert isinstance(spec, migrations._JsonColumnSpec)
        assert spec.sql_type == "JSON"

    def test_create_all_produces_a_column_the_verifier_accepts(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """What `create_all` builds on a fresh database has to satisfy the same verifier
        that gates startup on a migrated one, or every local boot reports CONFLICTS on a
        column it created itself."""
        columns = {
            c["name"]: c
            for c in sqlalchemy.inspect(db_engine).get_columns(
                db_models.ScheduledPipelineRun.__tablename__
            )
        }
        shape = migrations.LiveShape(
            exists=True, columns=columns, indexes={}, checks={}, foreign_keys={}
        )
        spec = next(s for s in migrations._TARGET_COLUMNS if s.name == "settings")

        verdict, detail = migrations._verify_column(shape, spec)

        assert verdict is migrations._Verdict.MATCHES, detail
