import logging

import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import filter_query_sql

_logger = logging.getLogger(__name__)


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


def backfill_created_by_annotations(
    *,
    session: orm.Session,
    auto_commit: bool,
) -> int:
    """Idempotent backfill: copy pipeline_run.created_by into annotations.

    Source column: pipeline_run.created_by

    INSERT INTO pipeline_run_annotation
    SELECT pr.id, 'system/pipeline_run.created_by', COALESCE(pr.created_by, '')
    FROM pipeline_run pr
    LEFT JOIN pipeline_run_annotation ann
        ON ann.pipeline_run_id = pr.id
        AND ann.key = 'system/pipeline_run.created_by'
    WHERE ann.pipeline_run_id IS NULL

    Starting table:

        pipeline_run
        +----+------------+
        | id | created_by |
        +----+------------+
        |  1 | alice      |
        |  2 | NULL       |
        |  3 |            |  (empty string)
        |  4 | bob        |
        +----+------------+

        pipeline_run_annotation (pre-existing)
        +--------+--------------------------------------+-------+
        | run_id | key                                  | value |
        +--------+--------------------------------------+-------+
        | 1      | system/pipeline_run.created_by       | alice |
        +--------+--------------------------------------+-------+

    Step 1 -- LEFT JOIN annotation (anti-join):
    Finds runs missing a created_by annotation.

        +----+------------+--------------+
        | id | created_by | ann.run_id   |
        +----+------------+--------------+
        |  1 | alice      | 1            |  <- has annotation, SKIP
        |  2 | NULL       | NULL         |  <- missing, INSERT
        |  3 |            | NULL         |  <- missing, INSERT
        |  4 | bob        | NULL         |  <- missing, INSERT
        +----+------------+--------------+

    Step 2 -- INSERT with COALESCE:

        INSERT INTO pipeline_run_annotation (pipeline_run_id, key, value)
        +--------+--------------------------------------+-------+
        | run_id | key                                  | value |
        +--------+--------------------------------------+-------+
        | 2      | system/pipeline_run.created_by       |       |  (COALESCE(NULL, '') = '')
        | 3      | system/pipeline_run.created_by       |       |  ('' unchanged)
        | 4      | system/pipeline_run.created_by       | bob   |
        +--------+--------------------------------------+-------+

    Data parity: Every pipeline_run gets a created_by annotation.
      - NULL or empty created_by -> annotation value = "" (empty string)
      - Non-empty created_by -> annotation value = created_by

    Idempotent: Anti-join (LEFT JOIN + IS NULL) ensures runs that
    already have a created_by annotation are skipped. Safe to call
    multiple times -- never produces duplicates.

    Portable across databases via SQLAlchemy (SQLite, MySQL, PostgreSQL).
    COALESCE is ANSI SQL, supported by all three.

    Args:
        session: SQLAlchemy session. Caller controls the transaction
            when auto_commit=False.
        auto_commit: Must be explicitly set. True commits after insert.
            False defers commit to the caller (e.g. tangle OSS
            migrate_db batches all 3 then commits once).

    Returns:
        Number of annotation rows inserted.
    """
    _logger.info("Starting backfill for `created_by` annotations")

    key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
    existing_ann = orm.aliased(bts.PipelineRunAnnotation)

    stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
        ["pipeline_run_id", "key", "value"],
        sqlalchemy.select(
            bts.PipelineRun.id,
            sqlalchemy.literal(str(key)),
            # Step 2: COALESCE(created_by, '') — NULL/empty -> ""
            sqlalchemy.func.coalesce(bts.PipelineRun.created_by, ""),
        )
        # Step 1: LEFT JOIN annotation (anti-join) — find runs missing created_by
        .outerjoin(
            existing_ann,
            sqlalchemy.and_(
                existing_ann.pipeline_run_id == bts.PipelineRun.id,
                existing_ann.key == key,
            ),
        ).where(
            existing_ann.pipeline_run_id.is_(None),
        ),
    )
    result = session.execute(stmt)
    rowcount = result.rowcount
    _logger.info(
        "Backfill created_by (source: pipeline_run.created_by): "
        f"{rowcount} rows inserted"
    )
    if auto_commit:
        session.commit()
    return rowcount


def backfill_pipeline_names_from_extra_data(
    *,
    session: orm.Session,
    auto_commit: bool,
) -> int:
    """Idempotent backfill: extract pipeline name from extra_data JSON.

    Source path: pipeline_run.extra_data -> 'pipeline_name'

    INSERT INTO pipeline_run_annotation
    SELECT pr.id, 'system/pipeline_run.name',
           SUBSTR(JSON_EXTRACT(pr.extra_data, '$.pipeline_name'), 1, 255)
    FROM pipeline_run pr
    LEFT JOIN pipeline_run_annotation ann
        ON ann.pipeline_run_id = pr.id
        AND ann.key = 'system/pipeline_run.name'
    WHERE ann.pipeline_run_id IS NULL
      AND JSON_EXTRACT(pr.extra_data, '$.pipeline_name') IS NOT NULL

    Starting table:

        pipeline_run
        +----+--------------------------------------+
        | id | extra_data                           |
        +----+--------------------------------------+
        |  1 | {"pipeline_name": "my-pipeline"}     |
        |  2 | NULL                                 |
        |  3 | {}                                   |
        |  4 | {"pipeline_name": null}              |
        |  5 | {"pipeline_name": ""}                |
        |  6 | {"pipeline_name": "already-exists"}  |
        +----+--------------------------------------+

        pipeline_run_annotation (pre-existing)
        +--------+---------------------------+----------------+
        | run_id | key                       | value          |
        +--------+---------------------------+----------------+
        | 6      | system/pipeline_run.name  | already-exists |
        +--------+---------------------------+----------------+

    Step 1 -- LEFT JOIN annotation (anti-join):
    Finds runs missing a name annotation.

        Runs 1-5: ann.run_id IS NULL  -> candidates for insert
        Run 6:    ann.run_id = 6      -> SKIP (already has annotation)

    Step 2 -- JSON extraction + NULL filter:

        +----+--------------------------------------+------------+-----------+--------+
        | id | extra_data                           | ann.run_id | extracted | action |
        +----+--------------------------------------+------------+-----------+--------+
        |  1 | {"pipeline_name": "my-pipeline"}     | NULL       | "my-pipe" | INSERT |
        |  2 | NULL                                 | NULL       | NULL      | SKIP   |
        |  3 | {}                                   | NULL       | NULL      | SKIP   |
        |  4 | {"pipeline_name": null}              | NULL       | NULL      | SKIP   |
        |  5 | {"pipeline_name": ""}                | NULL       | ""        | INSERT |
        +----+--------------------------------------+------------+-----------+--------+

    Step 3 -- INSERT into annotations:

        +--------+---------------------------+-------------+
        | run_id | key                       | value       |
        +--------+---------------------------+-------------+
        | 1      | system/pipeline_run.name  | my-pipeline |
        | 5      | system/pipeline_run.name  |             |  (empty string)
        +--------+---------------------------+-------------+

    Valid (creates annotation row):
        extra_data = {"pipeline_name": "my-pipeline"}  ->  value = "my-pipeline"
        extra_data = {"pipeline_name": ""}              ->  value = ""

    Skipped (no annotation row):
        extra_data = NULL                               ->  JSON_EXTRACT = NULL
        extra_data = {}                                 ->  key absent, NULL
        extra_data = {"pipeline_name": null}            ->  JSON_EXTRACT = NULL

    Idempotent: Anti-join (LEFT JOIN + IS NULL) ensures runs that
    already have a name annotation are skipped. Safe to call multiple
    times -- never produces duplicates. Order-independent with
    backfill_pipeline_names_from_component_spec: either can run first.

    Values are truncated to 255 characters via SUBSTR to respect the
    VARCHAR(255) column limit on MySQL.

    Portable across databases: SQLAlchemy's JSON path extraction
    generates the correct SQL for each dialect:
      - SQLite:      JSON_EXTRACT(extra_data, '$.pipeline_name')
      - MySQL:       JSON_UNQUOTE(JSON_EXTRACT(...))
      - PostgreSQL:  extra_data ->> 'pipeline_name'

    Args:
        session: SQLAlchemy session. Caller controls the transaction
            when auto_commit=False.
        auto_commit: Must be explicitly set. True commits after insert.
            False defers commit to the caller.

    Returns:
        Number of annotation rows inserted.
    """
    _logger.info("Starting backfill for `pipeline_name` from `extra_data` annotations")

    key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
    # Step 2: JSON extraction — extra_data -> 'pipeline_name'
    pipeline_name_expr = bts.PipelineRun.extra_data["pipeline_name"].as_string()
    # Step 3: SUBSTR truncation for VARCHAR(255)
    truncated_name = sqlalchemy.func.substr(
        pipeline_name_expr,
        1,
        bts._STR_MAX_LENGTH,
    )
    existing_ann = orm.aliased(bts.PipelineRunAnnotation)

    stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
        ["pipeline_run_id", "key", "value"],
        sqlalchemy.select(
            bts.PipelineRun.id,
            sqlalchemy.literal(str(key)),
            truncated_name,
        )
        # Step 1: LEFT JOIN annotation (anti-join) — find runs missing name annotation
        .outerjoin(
            existing_ann,
            sqlalchemy.and_(
                existing_ann.pipeline_run_id == bts.PipelineRun.id,
                existing_ann.key == key,
            ),
        ).where(
            existing_ann.pipeline_run_id.is_(None),
            # Step 2 cont: NULL filter — skip NULL extra_data / missing key / null value
            pipeline_name_expr.isnot(None),
        ),
    )
    result = session.execute(stmt)
    rowcount = result.rowcount
    _logger.info(
        f"Backfill pipeline_name (source: extra_data): {rowcount} rows inserted"
    )
    if auto_commit:
        session.commit()
    return rowcount


def backfill_pipeline_names_from_component_spec(
    *,
    session: orm.Session,
    auto_commit: bool,
) -> int:
    """Idempotent backfill: extract pipeline name from component_spec JSON.

    Source path: execution_node.task_spec -> 'componentRef' -> 'spec' -> 'name'

    INSERT INTO pipeline_run_annotation
    SELECT pr.id, 'system/pipeline_run.name',
           SUBSTR(JSON_EXTRACT(en.task_spec, '$.componentRef.spec.name'), 1, 255)
    FROM pipeline_run pr
    JOIN execution_node en ON en.id = pr.root_execution_id
    LEFT JOIN pipeline_run_annotation ann
        ON ann.pipeline_run_id = pr.id
        AND ann.key = 'system/pipeline_run.name'
    WHERE ann.pipeline_run_id IS NULL
      AND JSON_EXTRACT(en.task_spec, '$.componentRef.spec.name') IS NOT NULL

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

    Idempotent: Anti-join (LEFT JOIN + IS NULL) ensures runs that
    already have a name annotation are skipped. Safe to call multiple
    times -- never produces duplicates. Order-independent with
    backfill_pipeline_names_from_extra_data: either can run first.

    Values are truncated to 255 characters via SUBSTR to respect the
    VARCHAR(255) column limit on MySQL.

    Args:
        session: SQLAlchemy session. Caller controls the transaction
            when auto_commit=False.
        auto_commit: Must be explicitly set. True commits after insert.
            False defers commit to the caller.

    Returns:
        Number of annotation rows inserted.
    """
    _logger.info("Starting backfill for `pipeline_name` from `component_spec` annotations")

    key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
    # Step 3: JSON extraction — task_spec -> 'componentRef' -> 'spec' -> 'name'
    name_expr = bts.ExecutionNode.task_spec[
        ("componentRef", "spec", "name")
    ].as_string()
    # Step 4: SUBSTR truncation for VARCHAR(255)
    truncated_name = sqlalchemy.func.substr(
        name_expr,
        1,
        bts._STR_MAX_LENGTH,
    )
    existing_ann = orm.aliased(bts.PipelineRunAnnotation)

    stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
        ["pipeline_run_id", "key", "value"],
        sqlalchemy.select(
            bts.PipelineRun.id,
            sqlalchemy.literal(str(key)),
            truncated_name,
        )
        # Step 1: INNER JOIN execution_node — attach task_spec, drop missing nodes
        .join(
            bts.ExecutionNode,
            bts.ExecutionNode.id == bts.PipelineRun.root_execution_id,
        )
        # Step 2a: LEFT JOIN annotation (anti-join) — find runs missing name annotation
        .outerjoin(
            existing_ann,
            sqlalchemy.and_(
                existing_ann.pipeline_run_id == bts.PipelineRun.id,
                existing_ann.key == key,
            ),
        ).where(
            # Step 2b: anti-join filter — keep only unmatched runs
            existing_ann.pipeline_run_id.is_(None),
            # Step 3 cont: NULL filter — any null at any JSON depth -> skip
            name_expr.isnot(None),
        ),
    )
    result = session.execute(stmt)
    rowcount = result.rowcount
    _logger.info(
        f"Backfill pipeline_name (source: component_spec): {rowcount} rows inserted"
    )
    if auto_commit:
        session.commit()
    return rowcount


def run_all_annotation_backfills(
    *,
    session: orm.Session,
    do_skip_already_backfilled: bool,
) -> None:
    """Run all annotation backfills in a single transaction.

    Called from migrate_db on application startup. Wraps all 3 backfill
    functions with a try-catch so that failures do not block startup or
    deployment.

    All 3 backfill functions are idempotent (anti-join ensures only
    missing rows are inserted, never duplicates). It is always safe to
    re-run them.

    Args:
        session: SQLAlchemy session. Commit is called once at the end
            after all 3 backfills succeed.
        do_skip_already_backfilled: Must be explicitly set.
            True  -- skip guards check whether annotations of each key
            already exist and skip the backfill if so (normal startup).
            False -- force re-run of all backfills. Safe because each
            backfill is idempotent (anti-join: only inserts rows that
            don't already exist, never duplicates).

    Order: component_spec is called before extra_data so it is the
    preferred source for pipeline names when both exist.
    """
    _logger.info("Enter backfill for annotations table")

    try:
        should_run_created_by = not do_skip_already_backfilled or (
            not _is_pipeline_run_annotation_key_already_backfilled(
                session=session,
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            )
        )
        if should_run_created_by:
            backfill_created_by_annotations(
                session=session,
                auto_commit=False,
            )

        should_run_names = not do_skip_already_backfilled or (
            not _is_pipeline_run_annotation_key_already_backfilled(
                session=session,
                key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
            )
        )
        if should_run_names:
            backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=False,
            )
            backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=False,
            )
            # TODO: Do we need a final catchall backfill that inserts empty string
            # for all pipeline names, which happens to not have a name in
            # component_spec nor extra_data?

        session.commit()
    except Exception:
        _logger.exception("Annotation backfill failed -- will retry on next restart")

    _logger.info("Exit backfill for annotations table")
