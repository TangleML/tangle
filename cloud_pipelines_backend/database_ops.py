import logging

import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import database_migrations

_logger = logging.getLogger(__name__)


def create_db_engine_and_migrate_db(
    *,
    database_uri: str,
    do_skip_backfill: bool = False,
    **kwargs,
) -> sqlalchemy.Engine:
    db_engine = create_db_engine(database_uri=database_uri, **kwargs)
    bts._TableBase.metadata.create_all(db_engine)
    migrate_db(db_engine=db_engine, do_skip_backfill=do_skip_backfill)
    return db_engine


def initialize_and_migrate_db(
    *,
    db_engine: sqlalchemy.Engine,
    do_skip_backfill: bool = False,
) -> None:
    bts._TableBase.metadata.create_all(db_engine)
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


def migrate_db(
    *,
    db_engine: sqlalchemy.Engine,
    do_skip_backfill: bool,
) -> None:
    _logger.info("Enter migrate DB")

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
