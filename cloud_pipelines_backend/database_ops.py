import logging
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import component_structures as structures
from . import filter_query_sql

logger = logging.getLogger(__name__)


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

    for index in bts.PipelineRun.__table__.indexes:
        if index.name == bts.IX_PR_CREATED_AT_DESC_ID_DESC:
            index.create(db_engine, checkfirst=True)
            break

    backfill_created_by_annotations(db_engine=db_engine)
    backfill_pipeline_name_annotations(db_engine=db_engine)


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


def get_pipeline_name_from_task_spec(
    *,
    task_spec_dict: dict[str, Any],
) -> str | None:
    """Extract pipeline name from a task_spec dict via component_ref.spec.name.

    Traversal path:
        task_spec_dict -> TaskSpec -> component_ref -> spec -> name

    Returns None if any step in the chain is missing or parsing fails.
    """
    try:
        task_spec = structures.TaskSpec.from_json_dict(task_spec_dict)
    except Exception:
        return None
    spec = task_spec.component_ref.spec
    if spec is None:
        return None
    return spec.name or None


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


def _backfill_pipeline_names_from_extra_data(*, db_engine: sqlalchemy.Engine):
    """Phase 1: bulk SQL backfill from extra_data['pipeline_name'].

    INSERT INTO pipeline_run_annotation
    SELECT id, key, json_extract(extra_data, '$.pipeline_name')
    FROM pipeline_run
    WHERE json_extract(...) IS NOT NULL AND != ''

    SQLAlchemy's JSON path extraction is NULL-safe: returns SQL NULL
    when extra_data is NULL or the key is absent (no Python error).
    """
    with orm.Session(db_engine) as session:
        pipeline_name_expr = bts.PipelineRun.extra_data["pipeline_name"].as_string()
        stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
            ["pipeline_run_id", "key", "value"],
            sqlalchemy.select(
                bts.PipelineRun.id,
                sqlalchemy.literal(filter_query_sql.SystemKey.NAME),
                pipeline_name_expr,
            ).where(
                pipeline_name_expr.isnot(None),
                pipeline_name_expr != "",
            ),
        )
        session.execute(stmt)
        session.commit()


def _backfill_pipeline_names_from_component_spec(*, db_engine: sqlalchemy.Engine):
    """Phase 2: Python fallback for runs still missing a name annotation.

    Find the "delta" -- runs that still have no name annotation
    after Phase 1 -- using a LEFT JOIN anti-join pattern:

        SELECT pr.id, pr.root_execution_id
        FROM pipeline_run pr
        LEFT JOIN pipeline_run_annotation ann
            ON ann.pipeline_run_id = pr.id
            AND ann.key = 'system/pipeline_run.name'
        WHERE ann.pipeline_run_id IS NULL

    How the LEFT JOIN works:

        pipeline_run                pipeline_run_annotation
        +----+------------------+   +--------+---------------------------+-------+
        | id | root_exec_id     |   | run_id | key                       | value |
        +----+------------------+   +--------+---------------------------+-------+
        |  1 | exec_1           |   | 1      | system/pipeline_run.name  | foo   |
        |  2 | exec_2           |   | 3      | system/pipeline_run.name  | bar   |
        |  3 | exec_3           |   +--------+---------------------------+-------+
        |  4 | exec_4           |
        +----+------------------+

        LEFT JOIN result (ON run_id = id AND key = 'system/pipeline_run.name'):
        +----+------------------+------------+-----------+
        | id | root_exec_id     | ann.run_id | ann.value |
        +----+------------------+------------+-----------+
        |  1 | exec_1           | 1          | foo       | <- matched
        |  2 | exec_2           | NULL       | NULL      | <- no match
        |  3 | exec_3           | 3          | bar       | <- matched
        |  4 | exec_4           | NULL       | NULL      | <- no match
        +----+------------------+------------+-----------+

        + WHERE ann.pipeline_run_id IS NULL -> rows 2, 4 (the delta)

    For each delta run, load execution_node.task_spec and extract
    the name via:
        task_spec_dict -> TaskSpec -> component_ref -> spec -> name
    """
    key = filter_query_sql.SystemKey.NAME
    ann = bts.PipelineRunAnnotation
    with orm.Session(db_engine) as session:
        delta_query = (
            sqlalchemy.select(
                bts.PipelineRun.id,
                bts.PipelineRun.root_execution_id,
            )
            .outerjoin(
                ann,
                sqlalchemy.and_(
                    ann.pipeline_run_id == bts.PipelineRun.id,
                    ann.key == key,
                ),
            )
            .where(ann.pipeline_run_id.is_(None))
        )
        delta_rows = session.execute(delta_query).all()

        for run_id, root_execution_id in delta_rows:
            execution_node = session.get(bts.ExecutionNode, root_execution_id)
            if execution_node is None:
                logger.warning(
                    f"Backfill pipeline run name: run {run_id} has no "
                    f"execution node (root_execution_id={root_execution_id}), "
                    "skipping. TODO: consider inserting 'UNKNOWN'?"
                )
                continue
            name = get_pipeline_name_from_task_spec(
                task_spec_dict=execution_node.task_spec
            )
            if name:
                session.add(
                    bts.PipelineRunAnnotation(
                        pipeline_run_id=run_id, key=key, value=name
                    )
                )
            else:
                logger.warning(
                    f"Backfill pipeline run name: run {run_id} has no "
                    "resolvable pipeline name from task_spec "
                    f"(root_execution_id={root_execution_id}), "
                    "skipping. TODO: consider inserting 'UNKNOWN'?"
                )
        session.commit()


def backfill_pipeline_name_annotations(*, db_engine: sqlalchemy.Engine):
    """Backfill pipeline_run_annotation with pipeline names.

    Skips entirely if any name annotation already exists (i.e. the
    write-path is populating them, so the backfill has already run or is
    no longer needed).

    Phase 1 -- _backfill_pipeline_names_from_extra_data:
        Bulk SQL insert from extra_data['pipeline_name'].

    Phase 2 -- _backfill_pipeline_names_from_component_spec:
        Python fallback for runs Phase 1 missed (extra_data is NULL or
        missing the key). Resolves name via component_ref.spec.name.
    """
    if is_annotation_key_already_backfilled(
        db_engine=db_engine, key=filter_query_sql.SystemKey.NAME
    ):
        return

    _backfill_pipeline_names_from_extra_data(db_engine=db_engine)
    _backfill_pipeline_names_from_component_spec(db_engine=db_engine)
