"""Shared fixtures for scheduling/pipelines tests."""

import collections.abc
import contextlib
import copy
import datetime

import fastapi
import fastapi.testclient
import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_router, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.scheduling.pipelines import (
    api_routes,
    database_migrations,
    executor,
    services,
)

SAMPLE_PIPELINE_TASK_SPEC = {
    "componentRef": {
        "spec": {
            "name": "test-pipeline",
            "implementation": {
                "graph": {"tasks": {}},
            },
        }
    }
}

# What a legacy caller stored: a bare ComponentSpec — a pipeline —
# where the column wants a root task. Shape taken from the real example-rollup-daily
# blob, reduced. `TaskSpec.component_ref` is required, so this never parses.
BARE_COMPONENT_SPEC = {
    "name": "example market rollup",
    "implementation": {
        "graph": {"tasks": {}},
    },
}

DEFAULT_USER = "test@example.com"
OTHER_USER = "other@example.com"
ADMIN_USER = "admin@example.com"


def insert_source_less_schedule_row(
    *,
    db_engine: sqlalchemy.Engine,
    name: str,
    created_by: str = DEFAULT_USER,
) -> str:
    """Insert a row with neither an inline spec nor a pipeline reference.

    ``ck_scheduled_pipeline_run_source`` now forbids this shape, so the ORM
    cannot produce it. It still needs test coverage: it is exactly the documented
    rollback degradation — a reference schedule written by new code, then read by
    an older image — and rows predating the constraint can carry it. The executor
    must degrade gracefully rather than crash on one.

    SQLite's ``ignore_check_constraints`` pragma is the only way to write a row
    the schema rejects, which is the point: the shape is unreachable through
    every supported path.
    """
    schedule_id = bts.generate_unique_id()
    # Bound as a string: the row is written through the driver rather than the
    # ORM, so the UtcDateTime type is not in play to adapt a datetime object.
    now = (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(tzinfo=None)
        .isoformat(sep=" ")
    )
    with db_engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            sqlalchemy.text(
                "INSERT INTO scheduled_pipeline_run"
                " (id, name, cron_expression, timezone, pipeline_task_spec,"
                "  paused, created_by, created_at, updated_at)"
                " VALUES (:id, :name, '0 9 * * *', 'UTC', NULL,"
                "  0, :created_by, :now, :now)"
            ),
            {
                "id": schedule_id,
                "name": name,
                "created_by": created_by,
                "now": now,
            },
        )
        conn.commit()
    return schedule_id


def _make_user_details(
    *,
    name: str,
    admin: bool = False,
) -> api_router.UserDetails:
    return api_router.UserDetails(
        name=name,
        permissions=api_router.Permissions(read=True, write=True, admin=admin),
    )


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)

    def get_session() -> collections.abc.Iterator[orm.Session]:
        with orm.Session(autocommit=False, autoflush=False, bind=engine) as session:
            yield session

    executor._get_session = get_session
    return engine


@pytest.fixture()
def session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[orm.Session, None, None]:
    with orm.Session(bind=db_engine) as sess:
        yield sess


@pytest.fixture()
def scheduler_svc(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[services.SchedulerService, None, None]:
    svc = services.SchedulerService(get_session=executor._get_session)
    svc.start()
    yield svc
    svc.shutdown()


def ready_schema_report(
    *,
    db_engine: sqlalchemy.Engine,
) -> database_migrations.MigrationReport:
    """The real report for the test engine, whose tiers are open.

    Produced by running the actual migration rather than hand-building a report,
    so "open" here means what it means in production.
    """
    return database_migrations.migrate_db(db_engine=db_engine)


def path_ready_reference_closed_schema_report() -> database_migrations.MigrationReport:
    """Path tier open, saved-reference tier closed.

    Needed because every create now writes a path, so the path gate is evaluated
    first on every create and would otherwise mask the reference gate entirely --
    the reference code would be unreachable and its refusal untested. The two
    tiers depend on different indexes, so this combination is real rather than
    contrived: the unique index can be present while the reference index is not.
    """
    report = database_migrations.MigrationReport(dialect="sqlite")
    # Every step the path tier declares, rather than a hand-picked subset: a step
    # the tier gains later would otherwise be silently missing here, closing the
    # tier and making the reference gate unreachable all over again.
    for name in database_migrations._WRITE_TIER_STEPS["path_writes"]():
        report.add(name, database_migrations.StepStatus.ALREADY_PRESENT)
    # The reference index step is deliberately never reached: "not reached" is not
    # a pass, which is the behaviour being relied on here.
    return report


def insert_pipeline_run(
    *,
    db_engine: sqlalchemy.Engine,
    created_by: str | None = DEFAULT_USER,
    name: str = "referenced-run",
) -> str:
    """Insert a real ``PipelineRun`` so a run reference can be valid.

    Deliberately duplicated from `test_executor.py` rather than hoisted out of it:
    that module has an unrelated change pending on the parent branch, and moving a
    helper out of it would put this PR in conflict with that one over a file
    neither change needs to share.
    """
    spec = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
    spec["componentRef"]["spec"]["name"] = name
    with orm.Session(bind=db_engine) as session:
        run = bts.PipelineRun(
            root_execution=bts.ExecutionNode(task_spec=spec),
            created_by=created_by,
        )
        session.add(run)
        session.commit()
        return run.id


def insert_pathless_schedule_row(
    *,
    db_engine: sqlalchemy.Engine,
    name: str = "legacy-schedule",
    created_by: str = DEFAULT_USER,
    run_reference: str | None = None,
) -> str:
    """Insert a schedule with a NULL ``schedule_path``, bypassing the API.

    Required now that create demands a canonical path: a path-less row is no
    longer reachable through the API, but it is exactly what every pre-existing
    row looks like, and it is the only thing PATCH adoption can act on.

    ``run_reference`` produces a *reference-sourced* legacy row instead of an
    inline one. Needed to reach the source check-constraint: adding an inline spec
    to such a row makes it carry two sources, which is the realistic way a commit
    can fail for a reason that has nothing to do with the path.
    """
    with orm.Session(bind=db_engine) as session:
        schedule = api_routes.db_models.ScheduledPipelineRun(
            name=name,
            cron_expression="0 8 * * *",
            timezone="UTC",
            pipeline_task_spec=None if run_reference else SAMPLE_PIPELINE_TASK_SPEC,
            pipeline_task_spec_from_pipeline_run_id=run_reference,
            created_by=created_by,
        )
        session.add(schedule)
        session.commit()
        return schedule.id


def closed_schema_report() -> database_migrations.MigrationReport:
    """A report that reached no step, so every write tier is closed.

    This is the test seam for import-cached readiness. Production computes its
    report once per process and has no recompute path -- deliberately, because a
    pod's verdict must not drift mid-life -- so the only way to exercise a closed
    tier is to inject a closed report at wiring time.
    """
    return database_migrations.MigrationReport(dialect="sqlite")


def _build_app(
    *,
    scheduler_svc: services.SchedulerService,
    user_name: str,
    schema_report: database_migrations.MigrationReport,
    admin: bool = False,
) -> fastapi.FastAPI:
    app = fastapi.FastAPI()

    def user_details_getter() -> api_router.UserDetails:
        return _make_user_details(name=user_name, admin=admin)

    api_routes.setup_pipeline_schedule_routes(
        app=app,
        get_session=executor._get_session,
        scheduler_svc=scheduler_svc,
        user_details_getter=user_details_getter,
        schema_report=schema_report,
    )
    return app


@pytest.fixture()
def test_app(
    db_engine: sqlalchemy.Engine,
    scheduler_svc: services.SchedulerService,
) -> fastapi.FastAPI:
    return _build_app(
        scheduler_svc=scheduler_svc,
        user_name=DEFAULT_USER,
        schema_report=ready_schema_report(db_engine=db_engine),
    )


@pytest.fixture()
def client(
    test_app: fastapi.FastAPI,
) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(test_app)


#: A caller-parameterized client factory. See `client_for`.
ClientFactory = collections.abc.Callable[[str], fastapi.testclient.TestClient]


@pytest.fixture()
def client_for(
    db_engine: sqlalchemy.Engine,
    scheduler_svc: services.SchedulerService,
) -> ClientFactory:
    """Build a client for an ARBITRARY caller name, against the shared database.

    The fixed `client` / `other_user_client` pair binds caller identity to a
    constant, so a test can only vary the STORED owner. That is enough while the
    two are compared byte-for-byte and useless once they are not: `Jose` reading
    `jose`'s schedule is exactly the case the fixed fixtures cannot express, and
    it is the case the owner contract turns on.

    Independent axes, deliberately. Stored owner comes from
    `insert_schedule_row(created_by=...)` or from a create issued as one caller;
    calling owner comes from here. A helper that set both from one argument
    could not tell "the server matched these" from "these are the same string".

    Each call builds its own app because the caller is fixed at wiring time by
    `user_details_getter`; they share `db_engine` and `scheduler_svc`, so rows
    written through one are visible to the next.

    Cached per name so repeated calls in one test reuse a client rather than
    rebuilding the router, which also keeps `id`-identity assertions honest.
    """
    built: dict[str, fastapi.testclient.TestClient] = {}
    report = ready_schema_report(db_engine=db_engine)

    def factory(user_name: str) -> fastapi.testclient.TestClient:
        if user_name not in built:
            built[user_name] = fastapi.testclient.TestClient(
                _build_app(
                    scheduler_svc=scheduler_svc,
                    user_name=user_name,
                    schema_report=report,
                )
            )
        return built[user_name]

    return factory


@contextlib.contextmanager
def _folding_owner_column(
    *,
    db_engine: sqlalchemy.Engine,
    table: sqlalchemy.Table,
) -> collections.abc.Iterator[sqlalchemy.Engine]:
    """Rebuild one table with a case-FOLDING `created_by` column.

    What a MySQL deployment looks like: the owner column folds under the server
    default. SQLite's `COLLATE NOCASE` folds ASCII case for `=`, for `LIKE` and
    for a unique index, which is the same three places MySQL's `_ci` default
    folds it.

    Needed because the ordinary `db_engine` compares byte-for-byte, so every
    ownership assertion in this suite passes whether or not the server applies
    an exact-owner residual -- which is how one shipped to review. A test that
    cannot fail on the mutation it is written for is not evidence.

    Only `created_by` is swapped. Folding `schedule_path` as well would make the
    path-distinctness assertions vacuous while looking like they held.

    The type is swapped on the shared table object because `create_all` reads
    the model; the table is dropped and recreated on the SAME engine so
    `executor._get_session` still resolves; the original type is restored in a
    `finally`.
    """
    column = table.c["created_by"]
    original = column.type
    column.type = sqlalchemy.String(bts._STR_MAX_LENGTH, collation="NOCASE")
    try:
        table.drop(db_engine, checkfirst=True)
        table.create(db_engine)
        yield db_engine
    finally:
        column.type = original
        table.drop(db_engine, checkfirst=True)
        table.create(db_engine)


@pytest.fixture()
def folding_owner_db_engine(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Iterator[sqlalchemy.Engine]:
    """A folding `scheduled_pipeline_run.created_by`, with paths left exact."""
    with _folding_owner_column(
        db_engine=db_engine,
        table=api_routes.db_models.ScheduledPipelineRun.__table__,
    ) as engine:
        yield engine


@pytest.fixture()
def folding_run_owner_db_engine(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Iterator[sqlalchemy.Engine]:
    """The same, for `pipeline_run.created_by`.

    A separate table and a separate fixture because run references are
    authorized against `PipelineRun`, not against the schedule: a schedule owned
    by `Alice@` may reference a run the same person created as `alice@`, and
    that comparison happens on the run's own column under the run table's own
    collation.
    """
    with _folding_owner_column(
        db_engine=db_engine, table=bts.PipelineRun.__table__
    ) as engine:
        yield engine


@pytest.fixture()
def other_user_client(
    db_engine: sqlalchemy.Engine,
    scheduler_svc: services.SchedulerService,
) -> fastapi.testclient.TestClient:
    app = _build_app(
        scheduler_svc=scheduler_svc,
        user_name=OTHER_USER,
        schema_report=ready_schema_report(db_engine=db_engine),
    )
    return fastapi.testclient.TestClient(app)


@pytest.fixture()
def admin_client(
    db_engine: sqlalchemy.Engine,
    scheduler_svc: services.SchedulerService,
) -> fastapi.testclient.TestClient:
    app = _build_app(
        scheduler_svc=scheduler_svc,
        user_name=ADMIN_USER,
        admin=True,
        schema_report=ready_schema_report(db_engine=db_engine),
    )
    return fastapi.testclient.TestClient(app)


@pytest.fixture()
def closed_tier_client(
    db_engine: sqlalchemy.Engine,
    scheduler_svc: services.SchedulerService,
) -> fastapi.testclient.TestClient:
    """A client whose routes were wired with every write tier closed."""
    app = _build_app(
        scheduler_svc=scheduler_svc,
        user_name=DEFAULT_USER,
        schema_report=closed_schema_report(),
    )
    return fastapi.testclient.TestClient(app)


@pytest.fixture()
def reference_closed_client(
    db_engine: sqlalchemy.Engine,
    scheduler_svc: services.SchedulerService,
) -> fastapi.testclient.TestClient:
    """Path tier open, saved-reference tier closed."""
    app = _build_app(
        scheduler_svc=scheduler_svc,
        user_name=DEFAULT_USER,
        schema_report=path_ready_reference_closed_schema_report(),
    )
    return fastapi.testclient.TestClient(app)
