"""Shared scaffolding for the scheduler schema tests.

A module rather than a conftest so these schema-shaped helpers can be imported by
name from the schema tests in this stack, and so a legacy-table fixture is
defined once instead of per test module.
"""

import ast
import pathlib

import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.scheduling.pipelines import (
    database_migrations as migrations,
)
from cloud_pipelines_backend.scheduling.pipelines import db_models
from tests.scheduling.pipelines.conftest import SAMPLE_PIPELINE_TASK_SPEC
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models

_DIGEST = "c" * 64


_LEGACY_SCHEDULE_TABLE_DDL = """
CREATE TABLE scheduled_pipeline_run (
    id VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    cron_expression VARCHAR(255) NOT NULL,
    timezone VARCHAR(255) NOT NULL,
    pipeline_task_spec JSON,
    pipeline_task_spec_from_pipeline_run_id VARCHAR(255),
    pipeline_task_spec_from_user_pipeline_id VARCHAR(20),
    paused BOOLEAN NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    last_run_at DATETIME,
    last_run_submission_result TEXT,
    extra_data JSON,
    PRIMARY KEY (id)
)
"""


def _empty_engine() -> sqlalchemy.Engine:
    return database_ops.create_db_engine(database_uri="sqlite://")


def _execute(engine: sqlalchemy.Engine, *statements: str) -> None:
    with engine.connect() as conn:
        for statement in statements:
            conn.execute(sqlalchemy.text(statement))
        conn.commit()


def _legacy_engine(*, with_target_pipeline_tables: bool = True) -> sqlalchemy.Engine:
    engine = _empty_engine()
    _execute(engine, _LEGACY_SCHEDULE_TABLE_DDL)
    if with_target_pipeline_tables:
        bts._TableBase.metadata.create_all(
            engine,
            tables=[
                user_pipeline_db_models.UserPipeline.__table__,
                user_pipeline_db_models.UserPipelineVersion.__table__,
            ],
        )
    return engine


def _target_engine() -> sqlalchemy.Engine:
    engine = _empty_engine()
    bts._TableBase.metadata.create_all(engine)
    return engine


def _insert_legacy_row(
    engine: sqlalchemy.Engine,
    *,
    schedule_id: str = "0" * 20,
    created_by: str = "a@b.com",
    spec_sql: str = "'{\"componentRef\": {}}'",
) -> None:
    _execute(
        engine,
        "INSERT INTO scheduled_pipeline_run"
        " (id, name, cron_expression, timezone, pipeline_task_spec, paused,"
        "  created_by, created_at, updated_at)"
        f" VALUES ('{schedule_id}', 'Legacy', '0 9 * * *', 'UTC', {spec_sql}, 0,"
        f" '{created_by}', '2026-01-01 00:00:00', '2026-01-01 00:00:00')",
    )


def _snapshot(engine: sqlalchemy.Engine) -> dict[str, object]:
    """A comparable view of the live shape.

    Reflected type objects are fresh instances per call, so stringify what we
    care about rather than comparing LiveShape directly.
    """
    with engine.connect() as conn:
        shape = migrations._inspect(conn=conn)
    return {
        "columns": {
            n: f"{c['type']}/{c.get('nullable')}" for n, c in shape.columns.items()
        },
        "indexes": sorted(
            f"{n}:{i.get('column_names')}:{i.get('unique')}"
            for n, i in shape.indexes.items()
        ),
        "checks": sorted(shape.checks),
        "foreign_keys": sorted(shape.foreign_keys),
    }


class _RecordingConnection:
    """Records DDL text instead of executing it, and answers GET_LOCK with 1.

    The emitters take a Connection and only call `execute`/`commit`, so this is
    enough to assert the exact MySQL text — ALGORITHM/LOCK clauses included —
    which a SQLite engine can never show and no test here can execute.
    """

    def __init__(
        self,
        scalar_results: list[int | None] | None = None,
        scalars_results: list[list[str]] | None = None,
        all_results: list[list[tuple[object, ...]]] | None = None,
    ) -> None:
        self.statements: list[str] = []
        #: Set by `invalidate()`. Kept off `statements` on purpose: the recycle
        #: is not SQL, and the order assertions here are about emitted text.
        self.invalidated = False
        #: Statements AND lifecycle events, interleaved. `statements` alone
        #: cannot show that the restore ran before the discard, and a bool alone
        #: cannot show it either -- a test asserting only "invalidated is True"
        #: passes with the discard moved ahead of the restore (pi-40).
        self.events: list[str] = []
        #: Answers consumed in order by `scalar()`. Anything not supplied falls
        #: back to 1, which is what GET_LOCK returns on success.
        self._scalar_results = list(scalar_results or [])
        #: Answers consumed in order by `scalars()`. Defaults to empty, which is
        #: what the orphan probe returns when the data is clean.
        self._scalars_results = list(scalars_results or [])
        #: Answers consumed in order by `all()`.
        self._all_results = list(all_results or [])

    def execute(
        self, statement: object, parameters: object = None
    ) -> "_RecordingConnection":
        if self.invalidated:
            # A discarded connection is gone; anything issued after it would
            # silently run on a fresh one, outside whatever bound or lock the
            # caller believed it was inside.
            raise AssertionError(
                f"SQL issued after the connection was discarded: {statement}"
            )
        self.statements.append(str(statement))
        self.events.append(str(statement))
        return self

    def commit(self) -> None:
        return None

    def invalidate(self) -> None:
        """`_bounded_metadata_lock_wait` recycles the connection it bounded."""
        self.invalidated = True
        self.events.append("<invalidate>")

    def scalar(self) -> int | None:
        if self._scalar_results:
            return self._scalar_results.pop(0)
        return 1

    def scalars(self) -> list[str]:
        if self._scalars_results:
            return self._scalars_results.pop(0)
        return []

    def all(self) -> list[tuple[object, ...]]:
        """Rows for the information_schema collation lookup.

        Defaults to a matching child/parent pair, because the collations being
        equal is the uninteresting case every other test wants to get past.
        """
        if self._all_results:
            return self._all_results.pop(0)
        return [
            (
                "scheduled_pipeline_run",
                "pipeline_task_spec_from_user_pipeline_id",
                "utf8mb4",
                "utf8mb4_0900_ai_ci",
            ),
            ("pipeline", "id", "utf8mb4", "utf8mb4_0900_ai_ci"),
        ]


def _code_without_docstrings(path: str) -> str:
    """Module source with every docstring removed.

    Structural assertions about what the code does not contain have to look at
    code. The prose deliberately names the things being prohibited -- "no COUNT,
    no GROUP BY" -- so grepping the raw file matches the prohibition itself.
    """
    tree = ast.parse(pathlib.Path(path).read_text())
    spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            continue
        body = getattr(node, "body", [])
        if not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            spans.append((first.lineno, first.end_lineno or first.lineno))
    lines = pathlib.Path(path).read_text().split("\n")
    drop = {number for start, end in spans for number in range(start, end + 1)}
    return "\n".join(
        line for index, line in enumerate(lines, start=1) if index not in drop
    )


def _seed_deployment(engine: sqlalchemy.Engine) -> str:
    with orm.Session(bind=engine) as session:
        pipeline = user_pipeline_db_models.UserPipeline(
            user_id="a@b.com",
            file_path="pipelines/daily_pulse.yaml",
            versioning_mode=user_pipeline_db_models.PipelineVersioningMode.FULL,
        )
        session.add(pipeline)
        session.flush()
        session.add(
            user_pipeline_db_models.UserPipelineVersion(
                pipeline_id=pipeline.id,
                version_key=_DIGEST,
                content_digest=_DIGEST,
                root_pipeline_task=SAMPLE_PIPELINE_TASK_SPEC,
            )
        )
        pipeline.current_version_key = _DIGEST
        session.commit()
        return pipeline.id


def _seed_reference_row(
    engine: sqlalchemy.Engine,
    *,
    pipeline_id: str,
    version_key: str | None = _DIGEST,
) -> None:
    with orm.Session(bind=engine) as session:
        session.add(
            db_models.ScheduledPipelineRun(
                name="Reference",
                cron_expression="0 9 * * *",
                created_by="a@b.com",
                pipeline_task_spec=None,
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
                pipeline_task_spec_from_user_pipeline_version_key=version_key,
            )
        )
        session.commit()
