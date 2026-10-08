"""Shared fixtures for the projects tests.

Importing `projects.db_models` registers `workspace`, `project` and `project_resource` on
`bts._TableBase.metadata`, so `create_all` below builds them alongside the core Tangle tables.

Two engines: SQLite ignores foreign keys unless `PRAGMA foreign_keys=ON` is set per connection,
which `database_ops.create_db_engine` does not do, so `ON DELETE CASCADE` is inert on the
default engine while MySQL enforces it in production. Any claim about the *schema* enforcing
something has to be made on `fk_client`; `client` is where everything else lives.
"""

import collections.abc
import datetime

import fastapi
import fastapi.testclient
import pytest
import sqlalchemy
from cloud_pipelines_backend import (
    api_router,
    api_server_sql,
    component_structures,
    database_ops,
)
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.projects import api_routes, db_models
from cloud_pipelines_backend.user_pipelines import (
    db_models as user_pipeline_db_models,
)
from cloud_pipelines_backend.user_pipelines import pipeline_run_annotations
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm

db_models.register_db_tables()

DEFAULT_USER = "alice@example.com"
OTHER_USER = "bob@example.com"
ADMIN_USER = "admin@example.com"

# The workspaces every project fixture below lands in. Fixture data, not product data: nothing
# in the application creates a workspace on its own any more, so these are written straight into
# the table by `_add_workspaces`. Hardcoded rather than minted so a test can name one in a
# literal request body without looking it up first.
SANDBOX = "0a5b1c000000000000a1"
INTERNAL = "0a5b1c000000000000a2"
PUBLIC = "0a5b1c000000000000a3"

_FIXTURE_WORKSPACES = (
    (SANDBOX, "Sandbox"),
    (INTERNAL, "Internal"),
    (PUBLIC, "Public"),
)

# A well-formed id that names nothing.
MISSING_WORKSPACE_ID = "99999999999999999999"


def _add_workspaces(*, session: orm.Session) -> None:
    """Insert the fixture workspaces, leaving every optional column at its default.

    Through the ORM rather than `POST /api/workspaces/`, so the fixture that every project test
    depends on does not run the create route those tests are not about -- a bug in the route
    should fail its own tests, not every test in the package. `id` is assigned after construction
    because the column is `init=False` and would otherwise mint its own.
    """
    for workspace_id, name in _FIXTURE_WORKSPACES:
        workspace = db_models.Workspace(name=name)
        workspace.id = workspace_id
        session.add(workspace)


def _engine(*, enforce_foreign_keys: bool) -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    if enforce_foreign_keys:

        @sqlalchemy.event.listens_for(engine, "connect")
        def _enable_foreign_keys(dbapi_connection, _record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    bts._TableBase.metadata.create_all(engine)
    with orm.Session(bind=engine) as session, session.begin():
        _add_workspaces(session=session)
    return engine


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    return _engine(enforce_foreign_keys=False)


@pytest.fixture()
def fk_db_engine() -> sqlalchemy.Engine:
    """An engine with SQLite's foreign-key enforcement switched on."""
    return _engine(enforce_foreign_keys=True)


@pytest.fixture()
def session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Iterator[orm.Session]:
    with orm.Session(bind=db_engine) as sess:
        yield sess


def build_app(
    *,
    db_engine: sqlalchemy.Engine,
    user_name: str = DEFAULT_USER,
    can_write: bool = True,
    is_admin: bool = False,
    is_read_only: bool = False,
) -> fastapi.FastAPI:
    """An app with the project routes mounted and one fixed caller.

    Sessions bind to the same in-memory engine the `session` fixture uses, so a test can POST
    through the API and then assert on rows directly.

    `is_admin` defaults to False, so the ordinary `client` is the one that proves the workspace
    write routes are closed to a caller who has `write` and nothing more.
    """
    app = fastapi.FastAPI()

    def get_session() -> collections.abc.Iterator[orm.Session]:
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as sess:
            yield sess

    def user_details_getter() -> api_router.UserDetails:
        return api_router.UserDetails(
            name=user_name,
            permissions=api_router.Permissions(
                read=True, write=can_write, admin=is_admin
            ),
        )

    api_routes.setup_project_routes(
        app=app,
        get_session=get_session,
        user_details_getter=user_details_getter,
        is_read_only=is_read_only,
    )
    return app


@pytest.fixture()
def client(db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(build_app(db_engine=db_engine))


@pytest.fixture()
def fk_client(fk_db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    """The same caller on the engine that actually enforces foreign keys."""
    return fastapi.testclient.TestClient(build_app(db_engine=fk_db_engine))


@pytest.fixture()
def other_user_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    """A second caller. Reads everything the first can — there is no per-user scope here."""
    return fastapi.testclient.TestClient(
        build_app(db_engine=db_engine, user_name=OTHER_USER)
    )


@pytest.fixture()
def admin_client(db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    """The only caller that may write a workspace. Ordinary project writes work too."""
    return fastapi.testclient.TestClient(
        build_app(db_engine=db_engine, user_name=ADMIN_USER, is_admin=True)
    )


@pytest.fixture()
def read_only_admin_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    """An admin on a read-only deployment: 503 on a workspace write, not 201."""
    return fastapi.testclient.TestClient(
        build_app(
            db_engine=db_engine,
            user_name=ADMIN_USER,
            is_admin=True,
            is_read_only=True,
        )
    )


@pytest.fixture()
def read_only_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        build_app(db_engine=db_engine, is_read_only=True)
    )


@pytest.fixture()
def no_write_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        build_app(db_engine=db_engine, can_write=False)
    )


def add_pipeline(session: orm.Session, *, file_path: str, deleted: bool = False) -> str:
    """Insert a `user_pipelines` row directly and return its id.

    Not through `UserPipelineService`, which would drag a version, a digest and a content store
    into tests that only need a row to point at. All these tests care about is `deleted_at`.
    """
    with session.begin():
        pipeline = user_pipeline_db_models.UserPipeline(
            user_id=DEFAULT_USER, file_path=file_path
        )
        if deleted:
            pipeline.deleted_at = db_utils.utc_now()
        session.add(pipeline)
        session.flush()
        return pipeline.id


def add_run(
    session: orm.Session,
    *,
    name: str,
    project_id: str | None = None,
    age: datetime.timedelta | None = None,
) -> str:
    """Create a real pipeline run, optionally annotated with a project, and return its id.

    `age` backdates `created_at`, which is how a test puts a run outside the feed's default
    window. Left null the run is new, so every test that does not care stays inside it.

    Through `PipelineRunsApiService_Sql.create` rather than a hand-built row, because the
    mirroring is part of what is under test: an annotation only becomes filterable once
    `_mirror_pipeline_run_annotations` copies it into `pipeline_run_annotation`. That is also
    why the project key is an ordinary `tangleml.com/...` one -- the mirror silently skips
    `system/`-prefixed keys.
    """
    annotations = {"tangleml.com/source/test": "true"}
    if project_id is not None:
        annotations[pipeline_run_annotations.project_run_key(project_id)] = (
            pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
        )
    run = api_server_sql.PipelineRunsApiService_Sql().create(
        session=session,
        root_task=component_structures.TaskSpec.from_json_dict(
            {
                "componentRef": {
                    "spec": {
                        "name": name,
                        "implementation": {"graph": {"tasks": {}}},
                    }
                }
            }
        ),
        annotations=annotations,
        created_by=DEFAULT_USER,
    )
    # `create` commits and then refreshes, autobeginning a fresh transaction on the way out,
    # which a second call would find open and raise on. The response is a plain dataclass by
    # now, so ending it here costs nothing.
    session.rollback()
    if age is not None:
        # `create` stamps `created_at` itself, so an old run is made old afterwards. UPDATE
        # rather than touching the ORM object, which `rollback` above has already expired.
        session.execute(
            sqlalchemy.update(bts.PipelineRun)
            .where(bts.PipelineRun.id == run.id)
            .values(created_at=db_utils.utc_now() - age)
        )
        session.commit()
    return run.id


def create_project(
    client: fastapi.testclient.TestClient,
    *,
    name: str = "Retention modelling",
    workspace_id: str = SANDBOX,
    **extra,
) -> dict:
    response = client.post(
        "/api/projects/",
        json={"workspace_id": workspace_id, "name": name, **extra},
    )
    assert response.status_code == 201, response.text
    return response.json()


def create_resource(
    client: fastapi.testclient.TestClient,
    *,
    project_id: str,
    entity: str = "document",
    **extra,
) -> dict:
    """Create a resource, defaulting the content of a `document` so callers need not.

    A caller naming neither an `entity_id` nor a `payload` gets a placeholder payload rather
    than a 422, which keeps the tests that do not care about content free of boilerplate; the
    ones that do pass their own. A reference entity still has to supply its `entity_id` -- the
    placeholder cannot stand in for it, and a payload alongside it is the caller's to pass.
    """
    body = {"entity": entity, **extra}
    if "entity_id" not in body and "payload" not in body:
        body["payload"] = {"body": "placeholder"}
    response = client.post(f"/api/projects/{project_id}/resources/", json=body)
    assert response.status_code == 201, response.text
    return response.json()
