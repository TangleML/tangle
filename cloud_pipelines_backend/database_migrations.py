import dataclasses
import enum
import logging
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import filter_query_sql

_logger = logging.getLogger(__name__)


class BackfillStatus(str, enum.Enum):
    """Status of a single backfill step (Python 3.10-compatible StrEnum)."""

    SUCCESS = "success"
    SKIPPED = "skipped"


@dataclasses.dataclass
class BackfillStepResult:
    """Result of a single backfill step."""

    status: BackfillStatus
    rows_inserted: int = 0

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "rows_inserted": self.rows_inserted,
        }


@dataclasses.dataclass
class BackfillResult:
    """Aggregate result of all annotation backfills.

    All step fields are None until that step executes. On error the
    entire transaction is rolled back and ``error`` holds the message.
    """

    created_by: BackfillStepResult | None = None
    names_extra_data: BackfillStepResult | None = None
    names_component_spec: BackfillStepResult | None = None
    error: str | None = None

    def to_dict(
        self,
    ) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.created_by is not None:
            d["created_by"] = self.created_by.to_dict()
        if self.names_extra_data is not None:
            d["names_extra_data"] = self.names_extra_data.to_dict()
        if self.names_component_spec is not None:
            d["names_component_spec"] = self.names_component_spec.to_dict()
        if self.error is not None:
            d["error"] = self.error
        return d


def _count_missing_annotations_for_key(
    *,
    session: orm.Session,
    key: str,
) -> int:
    """Return the number of pipeline runs missing an annotation with the given key.

    Uses a LEFT JOIN anti-join to count runs missing the annotation in a
    single atomic query (no race window between separate count queries).

    SELECT COUNT(pr.id)
    FROM pipeline_run pr
    LEFT JOIN pipeline_run_annotation ann
        ON ann.pipeline_run_id = pr.id
        AND ann.key = :key
    WHERE ann.pipeline_run_id IS NULL

    Example tables:

        pipeline_run
        +------+
        | id   |
        +------+
        | run1 |
        | run2 |
        | run3 |
        +------+

        pipeline_run_annotation
        +----------------+--------------------------------------+-------+
        | pipeline_run_id| key                                  | value |
        +----------------+--------------------------------------+-------+
        | run1           | system/pipeline_run.created_by       | alice |
        | run3           | system/pipeline_run.created_by       | carol |
        | run2           | user/custom_tag                      | hello |
        +----------------+--------------------------------------+-------+

    Step 1 -- LEFT JOIN annotation with key filter:
    Attempts to match each run to an annotation with the given key.
    Unmatched runs get NULL in all annotation columns.

        +------+----------------+--------------------------------------+
        | pr.id| ann.run_id     | ann.key                              |
        +------+----------------+--------------------------------------+
        | run1 | run1           | system/pipeline_run.created_by       |
        | run2 | NULL           | NULL                                 |
        | run3 | run3           | system/pipeline_run.created_by       |
        +------+----------------+--------------------------------------+
        (run2 has 'user/custom_tag' but ON requires key = 'system/...')

    Step 2 -- WHERE ann.pipeline_run_id IS NULL (anti-join filter):
    Keeps only runs where the LEFT JOIN found no match.

        +------+----------------+
        | pr.id| ann.run_id     |
        +------+----------------+
        | run2 | NULL           |
        +------+----------------+

    Step 3 -- COUNT:

        COUNT = 1  ->  1 run needs backfill

    After backfill fills run2's annotation:

        COUNT = 0  ->  backfill complete

    Empty DB (0 runs):

        LEFT JOIN produces 0 rows  ->  COUNT = 0  ->  nothing to backfill

    A simple EXISTS check is insufficient because the API write path
    inserts annotations for new runs, so at least one annotation may
    exist even when older runs still lack them.
    """
    existing_ann = orm.aliased(bts.PipelineRunAnnotation)
    missing_count = (
        session.query(
            # Step 3: COUNT — number of runs without the annotation
            sqlalchemy.func.count(bts.PipelineRun.id)
        )
        # Step 1: LEFT JOIN annotation — match runs to annotations with the given key
        .outerjoin(
            existing_ann,
            sqlalchemy.and_(
                existing_ann.pipeline_run_id == bts.PipelineRun.id,
                existing_ann.key == key,
            ),
        )
        # Step 2: WHERE IS NULL — keep only runs missing the annotation
        .where(
            existing_ann.pipeline_run_id.is_(None),
        ).scalar()
    )
    return missing_count


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
    times -- never produces duplicates.

    Ordering: In run_all_annotation_backfills, this runs BEFORE
    backfill_pipeline_names_from_component_spec_and_parity so that
    extra_data values take priority. The parity catchall in
    component_spec_and_parity then fills any remaining gaps with
    empty strings.

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


def backfill_pipeline_names_from_component_spec_and_parity(
    *,
    session: orm.Session,
    auto_commit: bool,
) -> int:
    """Idempotent backfill: extract pipeline name from component_spec JSON.
    Uses COALESCE to fall back to empty string when the name is NULL,
    ensuring data parity (every pipeline_run gets a name annotation).

    Source path: execution_node.task_spec -> 'componentRef' -> 'spec' -> 'name'

    INSERT INTO pipeline_run_annotation
    SELECT pr.id, 'system/pipeline_run.name',
           COALESCE(SUBSTR(JSON_EXTRACT(en.task_spec,
               '$.componentRef.spec.name'), 1, 255), '')
    FROM pipeline_run pr
    LEFT JOIN execution_node en ON en.id = pr.root_execution_id
    LEFT JOIN pipeline_run_annotation ann
        ON ann.pipeline_run_id = pr.id
        AND ann.key = 'system/pipeline_run.name'
    WHERE ann.pipeline_run_id IS NULL

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

        pipeline_run_annotation (pre-existing from extra_data backfill)
        +--------+---------------------------+-------+
        | run_id | key                       | value |
        +--------+---------------------------+-------+
        | 1      | system/pipeline_run.name  | A     |
        | 3      | user/custom_tag           | hello |
        +--------+---------------------------+-------+

    Step 1 -- LEFT JOIN execution_node:
    Attaches task_spec to each run. Keeps runs without execution_node
    (they get NULL for all execution_node columns).

        FROM pipeline_run pr
        LEFT JOIN execution_node en ON en.id = pr.root_execution_id

        +----+--------+-------------------------------------------+
        | id | en.id  | en.task_spec                              |
        +----+--------+-------------------------------------------+
        |  1 | exec_1 | {"componentRef":{"spec":{"name":"A"}}}    |
        |  2 | exec_2 | {"componentRef":{"spec":null}}            |
        |  3 | exec_3 | {"componentRef":{"spec":{"name":""}}}     |
        |  4 | exec_4 | {"componentRef":{"spec":{"name":"B"}}}    |
        |  5 | NULL   | NULL                                      |
        +----+--------+-------------------------------------------+
        (run 5 kept with NULLs — LEFT JOIN preserves it)

    Step 2 -- LEFT JOIN annotation (anti-join):
    Attempts to match each run to an existing name annotation.

        LEFT JOIN pipeline_run_annotation ann
            ON ann.pipeline_run_id = pr.id
            AND ann.key = 'system/pipeline_run.name'

        +----+--------+------------------------------------------+----------+----------+
        | id | en.id  | en.task_spec                             | ann.r_id | ann.key  |
        +----+--------+------------------------------------------+----------+----------+
        |  1 | exec_1 | {"componentRef":{"spec":{"name":"A"}}}   | 1        | sys/name |
        |  2 | exec_2 | {"componentRef":{"spec":null}}           | NULL     | NULL     |
        |  3 | exec_3 | {"componentRef":{"spec":{"name":""}}}    | NULL     | NULL     |
        |  4 | exec_4 | {"componentRef":{"spec":{"name":"B"}}}   | NULL     | NULL     |
        |  5 | NULL   | NULL                                     | NULL     | NULL     |
        +----+--------+------------------------------------------+----------+----------+

    Step 3 -- WHERE ann.pipeline_run_id IS NULL (anti-join filter):
    Keeps only runs missing a name annotation.

        +----+--------+-------------------------------------------+
        | id | en.id  | en.task_spec                              |
        +----+--------+-------------------------------------------+
        |  2 | exec_2 | {"componentRef":{"spec":null}}            |
        |  3 | exec_3 | {"componentRef":{"spec":{"name":""}}}     |
        |  4 | exec_4 | {"componentRef":{"spec":{"name":"B"}}}    |
        |  5 | NULL   | NULL                                      |
        +----+--------+-------------------------------------------+

    Step 4 -- COALESCE(SUBSTR(name_expr, 1, 255), ''):
    Extracts name from JSON path. NULL at any depth → COALESCE → ''.

        +----+-------------------------------------------+-----------+----------+
        | id | en.task_spec                              | name_expr | COALESCE |
        +----+-------------------------------------------+-----------+----------+
        |  2 | {"componentRef":{"spec":null}}            | NULL      | ""       |
        |  3 | {"componentRef":{"spec":{"name":""}}}     | ""        | ""       |
        |  4 | {"componentRef":{"spec":{"name":"B"}}}    | "B"       | "B"      |
        |  5 | NULL                                      | NULL      | ""       |
        +----+-------------------------------------------+-----------+----------+

    Step 5 -- INSERT:

        +--------+---------------------------+-------+
        | run_id | key                       | value |
        +--------+---------------------------+-------+
        | 2      | system/pipeline_run.name  |       |
        | 3      | system/pipeline_run.name  |       |
        | 4      | system/pipeline_run.name  | B     |
        | 5      | system/pipeline_run.name  |       |
        +--------+---------------------------+-------+

    Data parity: After this single query, every pipeline_run has a name
    annotation. COUNT(annotations for key) == COUNT(pipeline_runs).

    The JSON path is portable across databases via SQLAlchemy:
      - SQLite:      JSON_EXTRACT(task_spec, '$.componentRef.spec.name')
      - MySQL:       JSON_UNQUOTE(JSON_EXTRACT(...))
      - PostgreSQL:  task_spec -> 'componentRef' -> 'spec' ->> 'name'

    Idempotent: Anti-join (LEFT JOIN + IS NULL) ensures runs that
    already have a name annotation are skipped. Safe to call multiple
    times -- never produces duplicates.

    Ordering: In run_all_annotation_backfills, extra_data runs FIRST
    so its values take priority. This function runs second and fills
    all remaining gaps — with component_spec names where available,
    or empty strings otherwise.

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
    _logger.info("Starting backfill for `pipeline_name` from `component_spec` + parity")

    key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

    # Step 4: JSON extraction — task_spec -> 'componentRef' -> 'spec' -> 'name'
    name_expr = bts.ExecutionNode.task_spec[
        ("componentRef", "spec", "name")
    ].as_string()
    # Step A4: SUBSTR truncation for VARCHAR(255)
    truncated_name = sqlalchemy.func.substr(
        name_expr,
        1,
        bts._STR_MAX_LENGTH,
    )
    # COALESCE: use extracted name when available, empty string otherwise
    name_or_empty = sqlalchemy.func.coalesce(truncated_name, sqlalchemy.literal(""))
    existing_ann = orm.aliased(bts.PipelineRunAnnotation)

    stmt = sqlalchemy.insert(bts.PipelineRunAnnotation).from_select(
        ["pipeline_run_id", "key", "value"],
        sqlalchemy.select(
            bts.PipelineRun.id,
            sqlalchemy.literal(str(key)),
            name_or_empty,
        )
        # Step 1: LEFT JOIN execution_node — attach task_spec, keep runs without nodes
        .outerjoin(
            bts.ExecutionNode,
            bts.ExecutionNode.id == bts.PipelineRun.root_execution_id,
        )
        # Step 2: LEFT JOIN annotation (anti-join) — find runs missing name annotation
        .outerjoin(
            existing_ann,
            sqlalchemy.and_(
                existing_ann.pipeline_run_id == bts.PipelineRun.id,
                existing_ann.key == key,
            ),
        ).where(
            # Step 3: anti-join filter — keep only runs without a name annotation
            existing_ann.pipeline_run_id.is_(None),
        ),
    )
    result = session.execute(stmt)
    rowcount = result.rowcount
    _logger.info(
        f"Backfill pipeline_name (component_spec + parity): {rowcount} rows inserted"
    )

    if auto_commit:
        session.commit()
    return rowcount


def run_all_annotation_backfills(
    *,
    session: orm.Session,
) -> BackfillResult:
    """Run all annotation backfills in a single transaction.

    Called from migrate_db on application startup and from admin API
    endpoints.  Wraps all 3 backfill functions with a try-catch so
    that failures do not block startup or deployment.  If any step
    fails the entire transaction is rolled back.

    All backfill functions are idempotent (anti-join ensures only
    missing rows are inserted, never duplicates). It is always safe to
    re-run them.

    Skip guards check whether every pipeline run already has an
    annotation for each key. The backfill is skipped only when ALL runs
    have the annotation. Partially-backfilled keys are re-run.

    Args:
        session: SQLAlchemy session. Commit is called once at the end
            after all 3 backfills succeed.

    Returns:
        BackfillResult. Each step is None until it executes, then
        holds a BackfillStepResult with status SUCCESS or SKIPPED.
        On error the transaction is rolled back and ``error`` holds
        the exception message (all step fields remain None).

    Order: extra_data runs first so its values take priority.
    component_spec_and_parity runs second and fills remaining gaps
    (with component_spec names or empty strings for parity).
    """
    _logger.info("Enter backfill for annotations table")

    result = BackfillResult()

    try:
        created_by_key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        missing_created_by = _count_missing_annotations_for_key(
            session=session,
            key=created_by_key,
        )
        if missing_created_by > 0:
            _logger.info(
                f"Backfill {created_by_key}: {missing_created_by} rows need backfilling"
            )
            count = backfill_created_by_annotations(
                session=session,
                auto_commit=False,
            )
            result.created_by = BackfillStepResult(
                status=BackfillStatus.SUCCESS,
                rows_inserted=count,
            )
        else:
            _logger.info(f"Backfill {created_by_key}: already complete, skipping")
            result.created_by = BackfillStepResult(
                status=BackfillStatus.SKIPPED,
            )

        pipeline_name_key = (
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        )
        missing_pipeline_name = _count_missing_annotations_for_key(
            session=session,
            key=pipeline_name_key,
        )
        if missing_pipeline_name > 0:
            _logger.info(
                f"Backfill {pipeline_name_key}: {missing_pipeline_name} rows need backfilling"
            )
            ed_count = backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=False,
            )
            result.names_extra_data = BackfillStepResult(
                status=BackfillStatus.SUCCESS,
                rows_inserted=ed_count,
            )
            cs_count = backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=False,
            )
            result.names_component_spec = BackfillStepResult(
                status=BackfillStatus.SUCCESS,
                rows_inserted=cs_count,
            )
        else:
            _logger.info(f"Backfill {pipeline_name_key}: already complete, skipping")
            result.names_extra_data = BackfillStepResult(
                status=BackfillStatus.SKIPPED,
            )
            result.names_component_spec = BackfillStepResult(
                status=BackfillStatus.SKIPPED,
            )

        session.commit()
    except Exception as exc:
        _logger.exception("Annotation backfill failed")
        session.rollback()
        result = BackfillResult(error=str(exc))

    _logger.info("Exit backfill for annotations table")
    return result
