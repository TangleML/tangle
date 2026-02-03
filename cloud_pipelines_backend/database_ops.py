import logging
import time

import sqlalchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine

from . import backend_types_sql as bts
from .instrumentation import metrics

logger = logging.getLogger(__name__)

# Slow query threshold for logging (in seconds)
_SLOW_QUERY_LOG_THRESHOLD = 1.0

# Slow query threshold for metrics (in seconds)
_SLOW_QUERY_METRIC_THRESHOLD = 0.01


@event.listens_for(Engine, "before_cursor_execute")
def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Record query start time."""
    context._query_start_time = time.time()


@event.listens_for(Engine, "after_cursor_execute")
def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Track slow queries and log very slow ones."""
    duration = time.time() - context._query_start_time

    # Only track queries that exceed the metric threshold (10ms)
    if duration > _SLOW_QUERY_METRIC_THRESHOLD:
        # Extract operation type from SQL statement
        operation = statement.strip().split()[0].upper() if statement else "UNKNOWN"
        metrics.track_database_query_duration(
            operation=operation,
            duration_seconds=duration,
        )

    # Log very slow queries with full SQL for debugging
    if duration > _SLOW_QUERY_LOG_THRESHOLD:
        logger.warning(
            "Slow database query detected",
            extra={
                "duration_seconds": duration,
                "operation": (
                    statement.strip().split()[0].upper() if statement else "UNKNOWN"
                ),
                "query": statement,
            },
        )


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
