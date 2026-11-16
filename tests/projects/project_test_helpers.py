"""Helpers the project tests share: fixture data, an app builder and row factories.

Importing `db_models` registers `workspace`, `project` and `project_resource` on
`bts._TableBase.metadata`, so `create_all` below builds them alongside the core tables.

Two engines: SQLite ignores foreign keys unless `PRAGMA foreign_keys=ON` is set per connection,
which `database_ops.create_db_engine` does not do, so `ON DELETE CASCADE` is inert on the
default engine while MySQL enforces it. Any claim about the *schema* enforcing something has to
be made on the foreign-key engine; the default one is where everything else lives.
"""

import collections.abc
import datetime

import fastapi
import fastapi.testclient
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.projects import api_routes
from cloud_pipelines_backend.projects import db_models
from cloud_pipelines_backend.projects import run_annotations
from cloud_pipelines_backend.projects import services

db_models.register_db_tables()

DEFAULT_USER = "alice@example.com"
OTHER_USER = "bob@example.com"
ADMIN_USER = "admin@example.com"

# The workspaces every project fixture lands in. Fixture data, not product data: nothing in the
# application creates a workspace on its own, so these are written straight into the table by
# `_add_workspaces`. Hardcoded rather than minted so a test can name one in a literal request
# body without looking it up first.
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

# A stand-in for something a deployment stores and this repository does not: saved pipelines
# that a `pipeline` resource can point at, deleted softly. On its own metadata so it stays out
# of every other test's schema.
_deployment_metadata = sqlalchemy.MetaData()
saved_pipelines = sqlalchemy.Table(
    "saved_pipeline",
    _deployment_metadata,
    sqlalchemy.Column("id", sqlalchemy.String(20), primary_key=True),
    sqlalchemy.Column("file_path", sqlalchemy.String(255), nullable=False),
    sqlalchemy.Column("deleted_at", sqlalchemy.DateTime(timezone=True), nullable=True),
)

# What such a deployment passes to `setup_project_routes`, so that a resource pointing at a
# deleted pipeline drops out of lists and counts.
HIDE_DELETED_PIPELINES: services.HiddenReferents = {
    db_models.ProjectResourceEntity.PIPELINE: (
        sqlalchemy.select(1)
        .where(
            saved_pipelines.c.id == db_models.ProjectResource.entity_id,
            saved_pipelines.c.deleted_at.is_not(None),
        )
        .exists()
    )
}


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


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


def make_engine(*, enforce_foreign_keys: bool) -> sqlalchemy.Engine:
    """An in-memory database with every table built and the fixture workspaces in it."""
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    if enforce_foreign_keys:

        @sqlalchemy.event.listens_for(engine, "connect")
        def _enable_foreign_keys(dbapi_connection, _record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    bts._TableBase.metadata.create_all(engine)
    _deployment_metadata.create_all(engine)
    with orm.Session(bind=engine) as session, session.begin():
        _add_workspaces(session=session)
    return engine


def build_app(
    *,
    db_engine: sqlalchemy.Engine,
    user_name: str = DEFAULT_USER,
    can_write: bool = True,
    is_admin: bool = False,
    is_read_only: bool = False,
    hidden_referents: services.HiddenReferents | None = None,
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
        hidden_referents=hidden_referents,
    )
    return app


def add_pipeline(session: orm.Session, *, file_path: str, deleted: bool = False) -> str:
    """Insert a row into the stand-in `saved_pipeline` table and return its id.

    All these tests care about is `deleted_at`.
    """
    pipeline_id = bts.generate_unique_id()
    with session.begin():
        session.execute(
            sqlalchemy.insert(saved_pipelines).values(
                id=pipeline_id,
                file_path=file_path,
                deleted_at=utc_now() if deleted else None,
            )
        )
    return pipeline_id


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
        annotations[run_annotations.project_run_key(project_id)] = (
            run_annotations.PROJECT_MEMBERSHIP_VALUE
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
            .values(created_at=utc_now() - age)
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
