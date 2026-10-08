"""Tests for the emission schema migrations.

The point of the fixture below is that a fresh `create_all` already has the new index, so a
test built on one proves nothing about a live table -- the bug being fixed exists only where
the superseded index is still installed.
"""

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend.emissions import db_migrations, db_models

NEW = db_models.EmissionEvent.IX_NODE_TYPE_STATUS_NEW
OLD = db_models.EmissionEvent.IX_NODE_TYPE_STATUS_OLD
TABLE = db_models.EmissionEvent.__tablename__

# Captured before any test patches it, so a patched create can still reach the real one.
real_create = sqlalchemy.Index.create


@pytest.fixture()
def legacy_db_engine(*, db_engine: sqlalchemy.Engine) -> sqlalchemy.Engine:
    """A DB shaped like production before this change: the 2-column key, and no 3-column one.

    Built with SQL rather than by editing the model's `__table_args__`, which is process-wide
    and would leak into whatever test ran next.
    """
    with db_engine.connect() as conn:
        conn.execute(sqlalchemy.text(f"DROP INDEX {NEW}"))
        conn.execute(
            sqlalchemy.text(
                f"CREATE UNIQUE INDEX {OLD} ON {TABLE}"
                " (execution_node_id, emission_type)"
            )
        )
        conn.commit()
    return db_engine


def _index_names(*, db_engine: sqlalchemy.Engine) -> set[str]:
    return {index["name"] for index in sqlalchemy.inspect(db_engine).get_indexes(TABLE)}


def _emit(
    *,
    db_engine: sqlalchemy.Engine,
    node: str,
    status: str,
    kind: str = "notification",
) -> bool:
    """Write one emission row, returning whether the DB accepted it."""
    with orm.Session(bind=db_engine) as session:
        session.add(
            db_models.EmissionEvent(
                execution_node_id=node,
                container_execution_id="ce-1",
                container_execution_status=status,
                emission_type=kind,
            )
        )
        try:
            session.commit()
        except sqlalchemy.exc.IntegrityError:
            return False
    return True


def test_the_superseded_key_is_what_rejects_a_second_status(
    *, legacy_db_engine: sqlalchemy.Engine
) -> None:
    """The defect the migration exists for, asserted before fixing it."""
    assert _emit(db_engine=legacy_db_engine, node="n1", status="RUNNING")
    assert not _emit(db_engine=legacy_db_engine, node="n1", status="FAILED")


def test_migrating_swaps_the_index(*, legacy_db_engine: sqlalchemy.Engine) -> None:
    assert OLD in _index_names(db_engine=legacy_db_engine)
    assert NEW not in _index_names(db_engine=legacy_db_engine)

    db_migrations.migrate(db_engine=legacy_db_engine)

    names = _index_names(db_engine=legacy_db_engine)
    assert NEW in names
    assert OLD not in names


def test_a_node_reaching_two_statuses_writes_two_rows(
    *, legacy_db_engine: sqlalchemy.Engine
) -> None:
    db_migrations.migrate(db_engine=legacy_db_engine)
    assert _emit(db_engine=legacy_db_engine, node="n2", status="RUNNING")
    assert _emit(db_engine=legacy_db_engine, node="n2", status="FAILED")
    with orm.Session(bind=legacy_db_engine) as session:
        rows = session.scalars(
            sqlalchemy.select(db_models.EmissionEvent).where(
                db_models.EmissionEvent.execution_node_id == "n2"
            )
        ).all()
    assert {row.container_execution_status for row in rows} == {
        "RUNNING",
        "FAILED",
    }


def test_the_same_status_twice_still_writes_one_row(
    *, legacy_db_engine: sqlalchemy.Engine
) -> None:
    """Widening the key must not stop it being a key."""
    db_migrations.migrate(db_engine=legacy_db_engine)
    assert _emit(db_engine=legacy_db_engine, node="n3", status="FAILED")
    assert not _emit(db_engine=legacy_db_engine, node="n3", status="FAILED")


def test_two_kinds_at_the_same_status_are_not_a_duplicate(
    *, legacy_db_engine: sqlalchemy.Engine
) -> None:
    db_migrations.migrate(db_engine=legacy_db_engine)
    assert _emit(
        db_engine=legacy_db_engine, node="n4", status="FAILED", kind="readiness"
    )
    assert _emit(
        db_engine=legacy_db_engine,
        node="n4",
        status="FAILED",
        kind="notification",
    )


def test_running_twice_is_a_no_op(*, legacy_db_engine: sqlalchemy.Engine) -> None:
    """Every pod runs this at startup, so a second pass must not raise or undo the first."""
    db_migrations.migrate(db_engine=legacy_db_engine)
    after_first = _index_names(db_engine=legacy_db_engine)
    db_migrations.migrate(db_engine=legacy_db_engine)
    assert _index_names(db_engine=legacy_db_engine) == after_first


def test_losing_the_create_race_is_not_a_crash(
    *,
    legacy_db_engine: sqlalchemy.Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two processes migrate -- the app and the orchestrator -- so one of them creates second.

    The snapshot is taken before the create, so the loser still believes the new index is
    missing and issues a CREATE for one that now exists. MySQL answers that with errno 1061,
    `Duplicate key name` -- measured, not assumed -- which must not fail a startup.
    """
    db_migrations.migrate(db_engine=legacy_db_engine)
    settled = _index_names(db_engine=legacy_db_engine)
    assert NEW in settled

    real = db_migrations._live_indexes
    reads = iter([settled - {NEW}])

    def stale_first_read(**kwargs: object) -> set[str]:
        return next(reads, None) or real(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(db_migrations, "_live_indexes", stale_first_read)
    # checkfirst would hide the race on its own, so this reproduces the pod that gets past it.
    monkeypatch.setattr(
        sqlalchemy.Index,
        "create",
        lambda self, bind, checkfirst=False: real_create(self, bind),
    )
    db_migrations.migrate(db_engine=legacy_db_engine)

    assert _index_names(db_engine=legacy_db_engine) == settled


def test_a_create_that_fails_for_any_other_reason_still_raises(
    *,
    legacy_db_engine: sqlalchemy.Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tolerating the race must not turn every create failure into a silent success.

    The create fails while the new index is genuinely absent, which is a real failure and not
    a lost race, so it has to surface rather than be logged and swallowed.
    """

    def refuse(self: object, bind: object, checkfirst: bool = False) -> None:
        raise sqlalchemy.exc.OperationalError("CREATE INDEX", {}, Exception("nope"))

    monkeypatch.setattr(sqlalchemy.Index, "create", refuse)
    with pytest.raises(sqlalchemy.exc.DatabaseError):
        db_migrations.migrate(db_engine=legacy_db_engine)
    assert NEW not in _index_names(db_engine=legacy_db_engine)


def test_losing_the_drop_race_is_not_a_crash(
    *,
    legacy_db_engine: sqlalchemy.Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two processes migrate -- the app and the orchestrator -- so one of them drops second.

    The snapshot is taken before the drop, so the loser still believes the old index is there
    and issues a DROP for an index that is already gone. That must not fail a startup.
    """
    db_migrations.migrate(db_engine=legacy_db_engine)
    settled = _index_names(db_engine=legacy_db_engine)
    assert OLD not in settled

    real = db_migrations._live_indexes
    reads = iter([settled | {OLD}])

    def stale_first_read(**kwargs: object) -> set[str]:
        return next(reads, None) or real(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(db_migrations, "_live_indexes", stale_first_read)
    db_migrations.migrate(db_engine=legacy_db_engine)

    assert _index_names(db_engine=legacy_db_engine) == settled


def test_a_drop_that_fails_for_any_other_reason_still_raises(
    *,
    legacy_db_engine: sqlalchemy.Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tolerating the race must not turn every drop failure into a silent success.

    The drop fails while the old index is genuinely still installed, which is a real failure
    and not a lost race, so it has to surface rather than be logged and swallowed.
    """

    def refuse(*args: object, **kwargs: object) -> None:
        raise sqlalchemy.exc.OperationalError("DROP INDEX", {}, Exception("nope"))

    monkeypatch.setattr(
        db_migrations.alembic_operations.Operations, "drop_index", refuse
    )
    with pytest.raises(sqlalchemy.exc.DatabaseError):
        db_migrations.migrate(db_engine=legacy_db_engine)
    assert OLD in _index_names(db_engine=legacy_db_engine)


def test_a_database_that_never_had_the_old_index_is_left_alone(
    *, db_engine: sqlalchemy.Engine
) -> None:
    """The fresh-install path: create_all already made the new index, and there is no drop."""
    before = _index_names(db_engine=db_engine)
    assert NEW in before
    db_migrations.migrate(db_engine=db_engine)
    assert _index_names(db_engine=db_engine) == before


def test_a_model_that_lost_the_index_fails_by_name(
    *, legacy_db_engine: sqlalchemy.Engine
) -> None:
    """The one case `next` has to answer for: the constant and `__table_args__` disagreeing.

    Restored in a finally because `__table_args__` is process-wide — a leak here would drop
    the index for whatever test ran next.
    """
    table = db_models.EmissionEvent.__table__
    index = next(i for i in table.indexes if i.name == NEW)
    table.indexes.discard(index)
    try:
        with pytest.raises(RuntimeError, match=NEW):
            db_migrations.migrate(db_engine=legacy_db_engine)
    finally:
        table.indexes.add(index)
    assert NEW in {i.name for i in table.indexes}
