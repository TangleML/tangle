import logging

import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts

_logger = logging.getLogger(__name__)


def create_db_engine_and_migrate_db(
    database_uri: str,
    **kwargs,
) -> sqlalchemy.Engine:
    db_engine = create_db_engine(database_uri=database_uri, **kwargs)
    bts._TableBase.metadata.create_all(db_engine)
    migrate_db(db_engine=db_engine)
    return db_engine


def initialize_and_migrate_db(db_engine: sqlalchemy.Engine):
    bts._TableBase.metadata.create_all(db_engine)
    migrate_db(db_engine=db_engine)


def create_db_engine(
    database_uri: str,
    **kwargs,
) -> sqlalchemy.Engine:
    if database_uri.startswith("mysql://"):
        try:
            import MySQLdb
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


def migrate_db(db_engine: sqlalchemy.Engine):
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
        if index.name == "ix_execution_node_container_execution_cache_key":
            index.create(db_engine, checkfirst=True)

    _migrate_add_pipeline_name_column(db_engine)


def _migrate_add_pipeline_name_column(db_engine: sqlalchemy.Engine):
    """Add pipeline_name column to pipeline_run table and backfill from extra_data."""
    inspector = sqlalchemy.inspect(db_engine)
    columns = {c["name"] for c in inspector.get_columns("pipeline_run")}
    if "pipeline_name" in columns:
        return

    _logger.info("Migrating: Adding pipeline_name column to pipeline_run table")
    with db_engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(
                "ALTER TABLE pipeline_run ADD COLUMN pipeline_name VARCHAR(255)"
            )
        )

    for index in bts.PipelineRun.__table__.indexes:
        if index.name == "ix_pipeline_run_pipeline_name":
            index.create(db_engine, checkfirst=True)

    _logger.info("Migrating: Backfilling pipeline_name from extra_data")
    Session = orm.sessionmaker(bind=db_engine)
    batch_size = 1000
    with Session() as session:
        while True:
            runs = session.scalars(
                sqlalchemy.select(bts.PipelineRun)
                .where(
                    bts.PipelineRun.pipeline_name == None,
                    bts.PipelineRun.extra_data != None,
                )
                .limit(batch_size)
            ).all()
            if not runs:
                break
            for run in runs:
                if run.extra_data and "pipeline_name" in run.extra_data:
                    run.pipeline_name = run.extra_data["pipeline_name"]
            session.commit()
            if len(runs) < batch_size:
                break
    _logger.info("Migrating: pipeline_name backfill complete")
