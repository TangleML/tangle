"""Tests for the Alembic migrations and the schema upgrade path.

The three starting points `database_ops.upgrade_db_schema` has to handle:

    empty database ................ every revision runs
    pre-Alembic database .......... reconciled, stamped 0001, then upgraded
    Alembic-managed database ...... only the pending revisions run

The most important test here is `test_migrations_are_in_sync_with_the_models`,
which fails whenever `backend_types_sql.py` is changed without a matching
revision being added.
"""

import pathlib

from alembic import autogenerate
from alembic import command as alembic_command
from alembic import migration as alembic_migration
import pytest
import sqlalchemy

from cloud_pipelines_backend import backend_types_sql as bts

# Imported for its side effect, the same way `migrations/env.py` imports it:
# it declares four more tables on the shared `_TableBase`, and they are only
# part of `_TableBase.metadata` once it has been imported. Without this the
# tests below would compare against an incomplete schema.
from cloud_pipelines_backend import component_library_api_server  # noqa: F401
from cloud_pipelines_backend import database_ops

_BASELINE_REVISION = "0001"
_HEAD_REVISION = "0002"


def _get_current_revision(db_engine: sqlalchemy.Engine) -> str | None:
    with db_engine.connect() as connection:
        context = alembic_migration.MigrationContext.configure(connection)
        return context.get_current_revision()


def _get_schema_differences(db_engine: sqlalchemy.Engine) -> list:
    """Return the differences between the database and the models.

    An empty list means the schema is exactly what the models describe.
    """
    with db_engine.connect() as connection:
        context = alembic_migration.MigrationContext.configure(
            connection=connection,
            opts={
                "target_metadata": bts._TableBase.metadata,
                "compare_type": True,
            },
        )
        return autogenerate.compare_metadata(context, bts._TableBase.metadata)


def _run_alembic(db_engine: sqlalchemy.Engine, command, revision: str) -> None:
    config = database_ops._create_alembic_config()
    with db_engine.begin() as connection:
        config.attributes["connection"] = connection
        command(config, revision)


@pytest.fixture()
def db_engine(tmp_path: pathlib.Path) -> sqlalchemy.Engine:
    # A file database rather than `sqlite://` so that the migrations run over
    # more than one connection, as they do in a real deployment.
    return database_ops.create_db_engine(
        database_uri=f"sqlite:///{tmp_path / 'test.sqlite'}"
    )


def test_migrations_build_the_schema_from_an_empty_database(
    db_engine: sqlalchemy.Engine,
) -> None:
    database_ops.upgrade_db_schema(db_engine=db_engine)

    assert _get_current_revision(db_engine) == _HEAD_REVISION
    assert _get_schema_differences(db_engine) == []


def test_migrations_are_in_sync_with_the_models(
    db_engine: sqlalchemy.Engine,
) -> None:
    """Fail if the models were changed without adding a migration.

    If this fails, autogenerate a new revision -- see
    `cloud_pipelines_backend/migrations/README.md`.
    """
    database_ops.upgrade_db_schema(db_engine=db_engine)

    differences = _get_schema_differences(db_engine)
    assert differences == [], (
        "The models and the migrations have diverged."
        f" A new migration is needed for: {differences}"
    )


def test_upgrading_an_already_upgraded_database_does_nothing(
    db_engine: sqlalchemy.Engine,
) -> None:
    database_ops.upgrade_db_schema(db_engine=db_engine)
    database_ops.upgrade_db_schema(db_engine=db_engine)

    assert _get_current_revision(db_engine) == _HEAD_REVISION
    assert _get_schema_differences(db_engine) == []


def test_migrations_produce_the_same_schema_as_create_all(
    db_engine: sqlalchemy.Engine,
    tmp_path: pathlib.Path,
) -> None:
    """The migrated schema must match what the models create directly.

    `create_all` is still used by the tests and by anything that builds a
    throw-away database, so the two paths must not drift apart.
    """
    database_ops.upgrade_db_schema(db_engine=db_engine)
    create_all_engine = database_ops.create_db_engine(
        database_uri=f"sqlite:///{tmp_path / 'create_all.sqlite'}"
    )
    bts._TableBase.metadata.create_all(create_all_engine)

    def describe(engine: sqlalchemy.Engine) -> dict:
        inspector = sqlalchemy.inspect(engine)
        return {
            table_name: {
                "columns": sorted(
                    (column["name"], str(column["type"]), column["nullable"])
                    for column in inspector.get_columns(table_name)
                ),
                "indexes": sorted(
                    (index["name"], tuple(index["column_names"]), bool(index["unique"]))
                    for index in inspector.get_indexes(table_name)
                ),
                "primary_key": inspector.get_pk_constraint(table_name)[
                    "constrained_columns"
                ],
                "foreign_keys": sorted(
                    (
                        tuple(foreign_key["constrained_columns"]),
                        foreign_key["referred_table"],
                        tuple(foreign_key["referred_columns"]),
                    )
                    for foreign_key in inspector.get_foreign_keys(table_name)
                ),
            }
            # Alembic's bookkeeping table is not part of the models.
            for table_name in sorted(inspector.get_table_names())
            if table_name != database_ops._ALEMBIC_VERSION_TABLE_NAME
        }

    assert describe(db_engine) == describe(create_all_engine)


def test_pre_alembic_database_is_stamped_and_upgraded(
    db_engine: sqlalchemy.Engine,
) -> None:
    """A database created before Alembic must be adopted, not rebuilt.

    Such a database is simulated by migrating to the baseline revision and
    then removing the `alembic_version` table.
    """
    _run_alembic(db_engine, alembic_command.upgrade, _BASELINE_REVISION)
    with db_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(f"DROP TABLE {database_ops._ALEMBIC_VERSION_TABLE_NAME}")
        )
        # Rows that must survive the upgrade.
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO container_execution (id, status)"
                " VALUES ('container-execution-1', 'RUNNING')"
            )
        )
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO execution_node (id, task_spec)"
                " VALUES ('execution-node-1', '{}')"
            )
        )
    assert _get_current_revision(db_engine) is None

    database_ops.upgrade_db_schema(db_engine=db_engine)

    assert _get_current_revision(db_engine) == _HEAD_REVISION
    assert _get_schema_differences(db_engine) == []
    with db_engine.connect() as connection:
        # The pre-existing rows are still there, with NULL in the new columns.
        assert connection.execute(
            sqlalchemy.text("SELECT id, scheduling_group FROM container_execution")
        ).all() == [("container-execution-1", None)]
        assert connection.execute(
            sqlalchemy.text(
                "SELECT id, scheduling_group, last_processed_at FROM execution_node"
            )
        ).all() == [("execution-node-1", None, None)]


def test_empty_database_is_not_mistaken_for_a_pre_alembic_one(
    db_engine: sqlalchemy.Engine,
) -> None:
    assert not database_ops._is_pre_alembic_database(db_engine=db_engine)


def test_create_all_database_is_adopted_at_head(
    db_engine: sqlalchemy.Engine,
) -> None:
    """A database built by `create_all` is already current, not at baseline.

    Re-running the revisions against it would fail on tables that are already
    there, so it has to be stamped at `head` instead.
    """
    bts._TableBase.metadata.create_all(db_engine)
    assert database_ops._is_pre_alembic_database(db_engine=db_engine)

    database_ops.upgrade_db_schema(db_engine=db_engine)

    assert _get_current_revision(db_engine) == _HEAD_REVISION
    assert _get_schema_differences(db_engine) == []


def test_baseline_revision_covers_the_component_library_tables(
    db_engine: sqlalchemy.Engine,
) -> None:
    """The component library declares tables outside `backend_types_sql`.

    They share `_TableBase`, so `create_all` has always created them and the
    baseline has to include them.
    """
    _run_alembic(db_engine, alembic_command.upgrade, _BASELINE_REVISION)

    table_names = set(sqlalchemy.inspect(db_engine).get_table_names())
    assert {"component", "component_library", "published_component", "user"} <= (
        table_names
    )


def test_downgrade_and_upgrade_round_trip(db_engine: sqlalchemy.Engine) -> None:
    database_ops.upgrade_db_schema(db_engine=db_engine)

    _run_alembic(db_engine, alembic_command.downgrade, _BASELINE_REVISION)
    assert _get_current_revision(db_engine) == _BASELINE_REVISION
    table_names = set(sqlalchemy.inspect(db_engine).get_table_names())
    assert "execution_node_scheduling_group" not in table_names
    assert "container_execution_scheduling_group" not in table_names

    _run_alembic(db_engine, alembic_command.upgrade, "head")
    assert _get_current_revision(db_engine) == _HEAD_REVISION
    assert _get_schema_differences(db_engine) == []

    _run_alembic(db_engine, alembic_command.downgrade, "base")
    assert set(sqlalchemy.inspect(db_engine).get_table_names()) == {
        database_ops._ALEMBIC_VERSION_TABLE_NAME
    }


def test_in_memory_database_is_migrated() -> None:
    """`sqlite://` only survives for as long as its first connection.

    Several tests build their database this way, so the migrations must run on
    a connection handed to Alembic rather than one Alembic opens itself.
    """
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")

    assert _get_current_revision(db_engine) == _HEAD_REVISION
    assert _get_schema_differences(db_engine) == []


def test_index_names_fit_within_database_identifier_limits() -> None:
    """MySQL rejects identifiers over 64 characters; PostgreSQL truncates at 63.

    A truncated name is worse than a rejected one, because a later
    `DROP INDEX` then fails to find it.
    """
    too_long = {
        index.name
        for table in bts._TableBase.metadata.tables.values()
        for index in table.indexes
        if len(index.name) > 63
    }
    assert too_long == set()
