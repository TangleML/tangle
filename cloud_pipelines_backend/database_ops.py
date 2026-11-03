import logging
import pathlib

from alembic import command as alembic_command
from alembic import config as alembic_config
import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import database_migrations

_logger = logging.getLogger(__name__)

# Directory holding `env.py` and `versions/`.
_MIGRATIONS_DIR = pathlib.Path(__file__).parent / "migrations"

# The revision that reproduces the schema as it was just before Alembic was
# introduced. Databases created before then are stamped with it instead of
# running it. See `_upgrade_pre_alembic_schema_to_baseline`.
_BASELINE_REVISION = "0001"

# Alembic's bookkeeping table, holding the currently applied revision.
_ALEMBIC_VERSION_TABLE_NAME = "alembic_version"


def create_db_engine_and_migrate_db(
    *,
    database_uri: str,
    do_skip_backfill: bool = False,
    **kwargs,
) -> sqlalchemy.Engine:
    db_engine = create_db_engine(database_uri=database_uri, **kwargs)
    migrate_db(db_engine=db_engine, do_skip_backfill=do_skip_backfill)
    return db_engine


def initialize_and_migrate_db(
    *,
    db_engine: sqlalchemy.Engine,
    do_skip_backfill: bool = False,
) -> None:
    migrate_db(db_engine=db_engine, do_skip_backfill=do_skip_backfill)


def create_db_engine(
    database_uri: str,
    **kwargs,
) -> sqlalchemy.Engine:
    if database_uri.startswith("mysql://"):
        try:
            import MySQLdb  # noqa: F401
        except ImportError:
            # Using PyMySQL instead of missing MySQLdb
            database_uri = database_uri.replace("mysql://", "mysql+pymysql://")

    create_engine_kwargs = {}
    if database_uri == "sqlite://":
        create_engine_kwargs["poolclass"] = sqlalchemy.pool.StaticPool

    if database_uri.startswith("sqlite://"):
        # FastApi claims it's needed and safe: https://fastapi.tiangolo.com/tutorial/sql-databases/#create-an-engine
        create_engine_kwargs.setdefault("connect_args", {})["check_same_thread"] = False
        # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#using-a-memory-database-in-multiple-threads

    if create_engine_kwargs.get("poolclass") != sqlalchemy.pool.StaticPool:
        # Preventing the "MySQL server has gone away" error:
        # https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
        create_engine_kwargs["pool_recycle"] = 3600
        create_engine_kwargs["pool_pre_ping"] = True

    if kwargs:
        create_engine_kwargs.update(kwargs)

    db_engine = sqlalchemy.create_engine(
        url=database_uri,
        **create_engine_kwargs,
    )
    return db_engine


def _create_alembic_config() -> alembic_config.Config:
    """Build the Alembic config used for migrations run by the application.

    Equivalent to the repository-root `alembic.ini`, but without needing that
    file to be present next to the running process (or the process to have been
    started from the repository root).
    """
    config = alembic_config.Config()
    # Alembic parses option values with ConfigParser interpolation, in which
    # "%" is an escape character, so any "%" in the path has to be doubled.
    config.set_main_option("script_location", str(_MIGRATIONS_DIR).replace("%", "%%"))
    # The application has already set up logging; `env.py` must not replace it.
    config.attributes["configure_logging"] = False
    return config


def _get_model_table_names() -> set[str]:
    """Return every table name the models declare.

    Tables are declared across several modules that all share
    `bts._TableBase`, so they only show up in the shared metadata once their
    module has been imported. `migrations/env.py` imports the same set.
    """
    # Imported here rather than at module level to keep importing `database_ops`
    # from pulling in the component library and its dependencies.
    from . import component_library_api_server  # noqa: F401

    return set(bts._TableBase.metadata.tables)


def _is_pre_alembic_database(*, db_engine: sqlalchemy.Engine) -> bool:
    """Return whether this database has a schema that Alembic is not tracking.

    Such a database already has tables but no `alembic_version` table, so
    running the migrations against it as-is would fail trying to re-create
    tables that are already there.
    """
    table_names = set(sqlalchemy.inspect(db_engine).get_table_names())
    if _ALEMBIC_VERSION_TABLE_NAME in table_names:
        return False
    # An empty database is not "pre-Alembic" -- it just needs every revision.
    return bool(table_names & _get_model_table_names())


def _get_revision_to_stamp(*, db_engine: sqlalchemy.Engine) -> str:
    """Return the revision an untracked database should be adopted at.

    There are two ways a database can end up with tables but no
    `alembic_version`:

    * It was left behind by the pre-Alembic code, in which case its schema is
      the baseline and the later revisions still have to run.
    * It was built by `bts._TableBase.metadata.create_all()`, which produces
      the current schema in one step. Running any revision against it would
      fail on objects that are already there, so it is adopted at `head`.

    The two are told apart by whether every table the models declare is
    already present. That is enough while the revisions past the baseline add
    tables; a revision that only added a column to an existing table could not
    be distinguished this way. That is acceptable because this is a
    transitional path: once a database has been stamped it is never taken
    again, and databases left over from before Alembic are exactly the ones
    sitting at the baseline.
    """
    table_names = set(sqlalchemy.inspect(db_engine).get_table_names())
    missing_table_names = _get_model_table_names() - table_names
    if not missing_table_names:
        return "head"
    _logger.info(
        f"Database is missing {sorted(missing_table_names)},"
        " so it is treated as being at the baseline revision."
    )
    return _BASELINE_REVISION


def _upgrade_pre_alembic_schema_to_baseline(
    *,
    db_engine: sqlalchemy.Engine,
) -> None:
    """Add the objects an untracked database may be missing.

    These are the idempotent fixups that used to run on every startup, back
    when schema changes were applied ad hoc rather than by Alembic. They are
    still needed once per database, because a database that Alembic has never
    seen may predate the point at which these objects were added to the models
    -- and stamping it asserts that they are present.
    """
    # # Example:
    # sqlalchemy.Index(
    #     "ix_pipeline_run_created_by_created_at_desc",
    #     bts.PipelineRun.created_by,
    #     bts.PipelineRun.created_at.desc(),
    # ).create(db_engine, checkfirst=True)

    # index1 = sqlalchemy.Index(
    #     "ix_execution_node_container_execution_cache_key",
    #     bts.ExecutionNode.container_execution_cache_key,
    # )
    # index1.create(db_engine, checkfirst=True)
    # SqlAlchemy's Index constructor is broken and adds indexes to the table definition (even if they are duplicate)
    # See https://github.com/sqlalchemy/sqlalchemy/issues/12965
    # See https://github.com/sqlalchemy/sqlalchemy/discussions/12420
    # To work around that issue we either need to remove the index from the table
    # bts.ExecutionNode.__table__.indexes.remove(index1)
    # Or we need to avoid calling the Index constructor.

    for index in bts.ExecutionNode.__table__.indexes:
        if index.name in (
            bts.ExecutionNode._IX_EXECUTION_NODE_CACHE_KEY,
            "ix_execution_node_container_execution_id",
        ):
            index.create(db_engine, checkfirst=True)

    for index in bts.PipelineRunAnnotation.__table__.indexes:
        if index.name == bts.PipelineRunAnnotation._IX_ANNOTATION_RUN_ID_KEY_VALUE:
            index.create(db_engine, checkfirst=True)
            break

    database_migrations.migrate_secret_value_column(db_engine=db_engine)


def upgrade_db_schema(
    *,
    db_engine: sqlalchemy.Engine,
) -> None:
    """Create or upgrade the database schema to the latest Alembic revision.

    Handles all three starting points:

    * An empty database -- every revision runs, building the schema from
      scratch.
    * A database with tables but no `alembic_version` -- it is reconciled with
      the baseline schema, stamped at the revision it is actually at, and then
      upgraded. See `_get_revision_to_stamp`.
    * A database already under Alembic -- only the pending revisions run.
    """
    _logger.info("Enter upgrade DB schema")
    is_pre_alembic = _is_pre_alembic_database(db_engine=db_engine)
    revision_to_stamp = None
    if is_pre_alembic:
        # These fixups are idempotent, and are applied whichever revision the
        # database turns out to be at: a database that Alembic has never seen
        # may be missing objects that were added ad hoc before Alembic existed.
        _upgrade_pre_alembic_schema_to_baseline(db_engine=db_engine)
        revision_to_stamp = _get_revision_to_stamp(db_engine=db_engine)
        _logger.info(
            "Database is not tracked by Alembic."
            f" Adopting it at revision {revision_to_stamp!r}."
        )

    config = _create_alembic_config()
    # Alembic is given an already-open connection rather than a URL because an
    # in-memory SQLite database (used by the tests) only exists for as long as
    # the connection that created it.
    with db_engine.begin() as connection:
        config.attributes["connection"] = connection
        if revision_to_stamp is not None:
            alembic_command.stamp(config, revision_to_stamp)
        alembic_command.upgrade(config, "head")
    _logger.info("Exit upgrade DB schema")


def migrate_db(
    *,
    db_engine: sqlalchemy.Engine,
    do_skip_backfill: bool,
) -> None:
    _logger.info("Enter migrate DB")

    upgrade_db_schema(db_engine=db_engine)

    if do_skip_backfill:
        _logger.info("Skipping annotation backfills")
    else:
        with orm.Session(db_engine) as session:
            # Set transaction isolation level to be SERIALIZABLE so a transaction error
            # will be thrown if multiple backfills are happening at the same time.
            session.connection(
                execution_options={"isolation_level": "SERIALIZABLE"},
            )
            database_migrations.run_all_annotation_backfills(
                session=session,
            )

    _logger.info("Exit migrate DB")
