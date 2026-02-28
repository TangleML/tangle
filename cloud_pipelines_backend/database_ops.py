import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import filter_query_sql


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
        if index.name == bts.IX_EXECUTION_NODE_CACHE_KEY:
            index.create(db_engine, checkfirst=True)
            break

    for index in bts.PipelineRunAnnotation.__table__.indexes:
        if index.name == bts.IX_ANNOTATION_RUN_ID_KEY_VALUE:
            index.create(db_engine, checkfirst=True)
            break

    backfill_created_by_annotations(db_engine=db_engine)


def is_annotation_key_already_backfilled(
    *,
    db_engine: sqlalchemy.Engine,
    key: str,
) -> bool:
    """Return True if at least one annotation with the given key exists."""
    with orm.Session(db_engine) as session:
        return session.query(
            sqlalchemy.exists(
                sqlalchemy.select(sqlalchemy.literal(1))
                .select_from(bts.PipelineRunAnnotation)
                .where(
                    bts.PipelineRunAnnotation.key == key,
                )
            )
        ).scalar()


def backfill_created_by_annotations(*, db_engine: sqlalchemy.Engine):
    """Copy pipeline_run.created_by into pipeline_run_annotation so
    annotation-based search works for created_by.

    Skips entirely if any created_by annotation key already exists (i.e. the
    write-path is populating them, so the backfill has already run or is
    no longer needed).
    """
    if is_annotation_key_already_backfilled(
        db_engine=db_engine, key=filter_query_sql.SystemKey.CREATED_BY
    ):
        return

    with orm.Session(db_engine) as session:
        stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
            ["pipeline_run_id", "key", "value"],
            sqlalchemy.select(
                bts.PipelineRun.id,
                sqlalchemy.literal(filter_query_sql.SystemKey.CREATED_BY),
                bts.PipelineRun.created_by,
            ).where(
                bts.PipelineRun.created_by.isnot(None),
                bts.PipelineRun.created_by != "",
            ),
        )
        session.execute(stmt)
        session.commit()
