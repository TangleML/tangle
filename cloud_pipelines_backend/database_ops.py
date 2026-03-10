
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
        if index.name in (
            bts.ExecutionNode._IX_EXECUTION_NODE_CACHE_KEY,
            "ix_execution_node_container_execution_id",
        ):
            index.create(db_engine, checkfirst=True)

    for index in bts.PipelineRunAnnotation.__table__.indexes:
        if index.name == bts.PipelineRunAnnotation._IX_ANNOTATION_RUN_ID_KEY_VALUE:
            index.create(db_engine, checkfirst=True)
            break

    _backfill_pipeline_run_created_by_annotations(db_engine=db_engine)
    _backfill_pipeline_run_name_annotations(db_engine=db_engine)


def _is_pipeline_run_annotation_key_already_backfilled(
    *,
    session: orm.Session,
    key: str,
) -> bool:
    """Return True if at least one annotation with the given key exists."""
    return session.query(
        sqlalchemy.exists(
            sqlalchemy.select(sqlalchemy.literal(1))
            .select_from(bts.PipelineRunAnnotation)
            .where(
                bts.PipelineRunAnnotation.key == key,
            )
        )
    ).scalar()


def _backfill_pipeline_run_created_by_annotations(
    *,
    db_engine: sqlalchemy.Engine,
) -> None:
    """Copy pipeline_run.created_by into pipeline_run_annotation so
    annotation-based search works for created_by.

    The check and insert run in a single session/transaction to avoid
    TOCTOU races between concurrent startup processes.

    Skips entirely if any created_by annotation key already exists (i.e. the
    write-path is populating them, so the backfill has already run or is
    no longer needed).
    """
    with orm.Session(db_engine) as session:
        if _is_pipeline_run_annotation_key_already_backfilled(
            session=session,
            key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
        ):
            return

        stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
            ["pipeline_run_id", "key", "value"],
            sqlalchemy.select(
                bts.PipelineRun.id,
                sqlalchemy.literal(
                    filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
                ),
                bts.PipelineRun.created_by,
            ).where(
                bts.PipelineRun.created_by.isnot(None),
                bts.PipelineRun.created_by != "",
            ),
        )
        session.execute(stmt)
        session.commit()


def _backfill_pipeline_names_from_extra_data(
    *,
    session: orm.Session,
) -> None:
    """Phase 1: bulk SQL backfill from extra_data['pipeline_name'].

    INSERT INTO pipeline_run_annotation
    SELECT id, key, json_extract(extra_data, '$.pipeline_name')
    FROM pipeline_run
    WHERE json_extract(...) IS NOT NULL

    Valid (creates annotation row):
        extra_data = {"pipeline_name": "my-pipeline"}  ->  value = "my-pipeline"
        extra_data = {"pipeline_name": ""}              ->  value = ""

    Skipped (no annotation row):
        extra_data = NULL                               ->  JSON_EXTRACT = NULL
        extra_data = {}                                 ->  key absent, NULL
        extra_data = {"pipeline_name": null}            ->  JSON_EXTRACT = NULL

    SQLAlchemy's JSON path extraction is NULL-safe: returns SQL NULL
    when extra_data is NULL or the key is absent (no Python error).
    """
    pipeline_name_expr = bts.PipelineRun.extra_data["pipeline_name"].as_string()
    truncated_name = sqlalchemy.func.substr(
        pipeline_name_expr,
        1,
        bts._STR_MAX_LENGTH,
    )
    stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
        ["pipeline_run_id", "key", "value"],
        sqlalchemy.select(
            bts.PipelineRun.id,
            sqlalchemy.literal(
                filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
            ),
            truncated_name,
        ).where(
            pipeline_name_expr.isnot(None),
        ),
    )
    session.execute(stmt)


def _backfill_pipeline_names_from_component_spec(
    *,
    session: orm.Session,
) -> None:
    """Phase 2: Bulk SQL fallback for runs still missing a name annotation.

    Extracts the pipeline name from each run's ExecutionNode via the
    JSON path:

        task_spec -> 'componentRef' -> 'spec' ->> 'name'

    Starting tables:

        pipeline_run                          execution_node
        +----+------------------+            +--------+-------------------------------------------+
        | id | root_execution_id|            | id     | task_spec (JSON)                          |
        +----+------------------+            +--------+-------------------------------------------+
        |  1 | exec_1           |            | exec_1 | {"componentRef":{"spec":{"name":"A"}}}    |
        |  2 | exec_2           |            | exec_2 | {"componentRef":{"spec":null}}            |
        |  3 | exec_3           |            | exec_3 | {"componentRef":{"spec":{"name":""}}}     |
        |  4 | exec_4           |            | exec_4 | {"componentRef":{"spec":{"name":"B"}}}    |
        |  5 | exec_99          |            +--------+-------------------------------------------+
        +----+------------------+            (no exec_99 row)

        pipeline_run_annotation (pre-existing)
        +--------+---------------------------+-------+
        | run_id | key                       | value |
        +--------+---------------------------+-------+
        | 1      | system/pipeline_run.name  | A     |
        | 3      | user/custom_tag           | hello |
        +--------+---------------------------+-------+

    Step 1 -- JOIN execution_node (INNER JOIN):
    Attaches task_spec to each run. Drops runs with no execution_node.

        FROM pipeline_run pr
        JOIN execution_node en ON en.id = pr.root_execution_id

        +----+--------+-------------------------------------------+
        | id | en.id  | en.task_spec                              |
        +----+--------+-------------------------------------------+
        |  1 | exec_1 | {"componentRef":{"spec":{"name":"A"}}}    |
        |  2 | exec_2 | {"componentRef":{"spec":null}}            |
        |  3 | exec_3 | {"componentRef":{"spec":{"name":""}}}     |
        |  4 | exec_4 | {"componentRef":{"spec":{"name":"B"}}}    |
        +----+--------+-------------------------------------------+
        (run 5 dropped -- exec_99 doesn't exist)

    Step 2a -- LEFT JOIN annotation:
    Attempts to match each run to an existing name annotation.

        LEFT JOIN pipeline_run_annotation ann
            ON ann.pipeline_run_id = pr.id
            AND ann.key = 'system/pipeline_run.name'

        +----+--------+------------------------------------------+------------------+----------+
        | id | en.id  | en.task_spec                             | ann.run_id       | ann.key  |
        +----+--------+------------------------------------------+------------------+----------+
        |  1 | exec_1 | {"componentRef":{"spec":{"name":"A"}}}   | 1                | sys/name |
        |  2 | exec_2 | {"componentRef":{"spec":null}}           | NULL             | NULL     |
        |  3 | exec_3 | {"componentRef":{"spec":{"name":""}}}    | NULL             | NULL     |
        |  4 | exec_4 | {"componentRef":{"spec":{"name":"B"}}}   | NULL             | NULL     |
        +----+--------+------------------------------------------+------------------+----------+
        (run 1 matched -- has 'system/pipeline_run.name' annotation)
        (run 3 NULL -- has 'user/custom_tag' but ON requires key = 'system/pipeline_run.name')

    Step 2b -- WHERE ann.pipeline_run_id IS NULL (anti-join filter):
    Keeps only runs where the LEFT JOIN found no match.

        WHERE ann.pipeline_run_id IS NULL

        +----+--------+-------------------------------------------+
        | id | en.id  | en.task_spec                              |
        +----+--------+-------------------------------------------+
        |  2 | exec_2 | {"componentRef":{"spec":null}}            |
        |  3 | exec_3 | {"componentRef":{"spec":{"name":""}}}     |
        |  4 | exec_4 | {"componentRef":{"spec":{"name":"B"}}}    |
        +----+--------+-------------------------------------------+
        (run 1 dropped -- ann.run_id was 1, not NULL)

    Step 3 -- JSON extraction + NULL filter:
    Extracts name from JSON path, keeps only non-null (empty string is allowed).

        WHERE task_spec->'componentRef'->'spec'->>'name' IS NOT NULL

        +----+-------------------------------------------+-----------+
        | id | en.task_spec                              | name_expr |
        +----+-------------------------------------------+-----------+
        |  2 | {"componentRef":{"spec":null}}            | NULL      | <- dropped
        |  3 | {"componentRef":{"spec":{"name":""}}}     | ""        | <- kept (empty string OK)
        |  4 | {"componentRef":{"spec":{"name":"B"}}}    | "B"       | <- kept
        +----+-------------------------------------------+-----------+

    Step 4 -- INSERT INTO pipeline_run_annotation:
    Inserts one row per surviving run.

        INSERT INTO pipeline_run_annotation (pipeline_run_id, key, value)
        +--------+---------------------------+-------+
        | run_id | key                       | value |
        +--------+---------------------------+-------+
        | 3      | system/pipeline_run.name  |       |
        | 4      | system/pipeline_run.name  | B     |
        +--------+---------------------------+-------+

    The JSON path is portable across databases via SQLAlchemy:
      - SQLite:      JSON_EXTRACT(task_spec, '$.componentRef.spec.name')
      - MySQL:       JSON_UNQUOTE(JSON_EXTRACT(...))
      - PostgreSQL:  task_spec -> 'componentRef' -> 'spec' ->> 'name'

    Any null at any depth (task_spec NULL, componentRef missing,
    spec null, name missing) produces SQL NULL, filtered out by
    IS NOT NULL. Empty string is allowed and will be inserted.
    """
    key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
    name_expr = bts.ExecutionNode.task_spec[
        ("componentRef", "spec", "name")
    ].as_string()
    truncated_name = sqlalchemy.func.substr(
        name_expr,
        1,
        bts._STR_MAX_LENGTH,
    )
    existing_ann = orm.aliased(bts.PipelineRunAnnotation)

    # Step 4: INSERT INTO pipeline_run_annotation
    stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
        ["pipeline_run_id", "key", "value"],
        sqlalchemy.select(
            bts.PipelineRun.id,
            sqlalchemy.literal(str(key)),
            truncated_name,
        )
        # Step 1: INNER JOIN execution_node
        .join(
            bts.ExecutionNode,
            bts.ExecutionNode.id == bts.PipelineRun.root_execution_id,
        )
        # Step 2a: LEFT JOIN existing annotation
        .outerjoin(
            existing_ann,
            sqlalchemy.and_(
                existing_ann.pipeline_run_id == bts.PipelineRun.id,
                existing_ann.key == key,
            ),
        ).where(
            # Step 2b: Anti-join — keep only runs with no existing annotation
            existing_ann.pipeline_run_id.is_(None),
            # Step 3: JSON extraction — keep only non-NULL names
            name_expr.isnot(None),
        ),
    )
    session.execute(stmt)


def _backfill_pipeline_run_name_annotations(
    *,
    db_engine: sqlalchemy.Engine,
) -> None:
    """Backfill pipeline_run_annotation with pipeline names.

    The check and both inserts run in a single session/transaction to
    avoid TOCTOU races between concurrent startup processes. If anything
    fails, the entire transaction rolls back automatically.

    Skips entirely if any name annotation already exists (i.e. the
    write-path is populating them, so the backfill has already run or is
    no longer needed).

    Phase 1 -- _backfill_pipeline_names_from_extra_data:
        Bulk SQL insert from extra_data['pipeline_name'].

    Phase 2 -- _backfill_pipeline_names_from_component_spec:
        Bulk SQL fallback for runs Phase 1 missed (extra_data is NULL or
        missing the key). Extracts name via JSON path
        task_spec -> componentRef -> spec -> name.

    Annotation creation rules (same for both phases):
        Creates row:  any non-NULL string, including empty string ""
        Skips row:    NULL at any depth in the JSON path
    """
    with orm.Session(db_engine) as session:
        if _is_pipeline_run_annotation_key_already_backfilled(
            session=session,
            key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
        ):
            return

        # execute() - rows in DB buffer for Phase 2
        _backfill_pipeline_names_from_extra_data(session=session)
        # Phase 2 sees Phase 1's rows via the shared transaction buffer.
        _backfill_pipeline_names_from_component_spec(session=session)
        # Both phases become permanent atomically.
        session.commit()
