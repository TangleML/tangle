"""Shared fixtures for the triggers tests.

Importing triggers.db_models registers the three trigger tables on _TableBase.metadata
(the mapped classes register themselves when the module is imported), so create_all below
builds them alongside the core Tangle tables.
"""

import collections.abc
from typing import Any

import fastapi
import fastapi.testclient
import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_router, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.triggers import api_routes, db_models
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import db as db_utils

# Registering the tables is what makes them part of the schema built below; the reference
# also stops linters from flagging the import as unused.
db_models.register_db_tables()


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return engine


@pytest.fixture()
def fk_db_engine() -> sqlalchemy.Engine:
    """An engine with SQLite's foreign-key enforcement switched on.

    SQLite ignores foreign keys unless `PRAGMA foreign_keys=ON` is set per connection, and
    database_ops.create_db_engine does not set it — so ON DELETE CASCADE is inert on the
    default test engine while MySQL enforces it in production. Cascade behaviour is tested
    on this engine so the assertion is about the schema rather than about the pragma.
    """
    engine = database_ops.create_db_engine(database_uri="sqlite://")

    @sqlalchemy.event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    bts._TableBase.metadata.create_all(engine)
    return engine


@pytest.fixture()
def session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[orm.Session, None, None]:
    with orm.Session(bind=db_engine) as sess:
        yield sess


@pytest.fixture()
def fk_session(
    fk_db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[orm.Session, None, None]:
    """A session on the foreign-key-enforcing engine, for assertions about ON DELETE CASCADE."""
    with orm.Session(bind=fk_db_engine) as sess:
        yield sess


DEFAULT_USER = "test@example.com"
OTHER_USER = "other@example.com"
ADMIN_USER = "admin@example.com"

# Every subscription names a pipeline, so the route tests need one to exist before they can
# POST anything. Fixed rather than generated so the request payloads can name it as a literal;
# a real row rather than a plausible id, because the column is a foreign key and inventing one
# would only pass while SQLite leaves foreign keys switched off.
SEEDED_PIPELINE_ID = "11111111-1111-4111-8111-111111111111"
# A second pipeline of theirs, FULL-mode with real version rows, because nothing about a
# successful pin can be tested against a DISABLED pipeline: its only row is the mutable head,
# which the resolver refuses by design.
FULL_PIPELINE_ID = "22222222-2222-4222-8222-222222222222"
# Two immutable versions on it, keyed by their own content exactly as FULL mode keys
# them. Fixed literals rather than a real hash, so a payload can name one.
PINNABLE_VERSION = "a" * user_pipeline_db_models.DIGEST_LENGTH
OTHER_PINNABLE_VERSION = "b" * user_pipeline_db_models.DIGEST_LENGTH
UNKNOWN_VERSION = "c" * user_pipeline_db_models.DIGEST_LENGTH
# A live pipeline belonging to OTHER_USER. The ownership guard means each caller needs a
# target of their own; sharing one would make every other-user test a 404 about the wrong
# thing.
OTHER_USER_PIPELINE_ID = "33333333-3333-4333-8333-333333333333"
# Theirs, but soft-deleted: the tombstone still satisfies the foreign key, so this is the row
# that proves the guard is doing something the schema cannot.
DELETED_PIPELINE_ID = "44444444-4444-4444-8444-444444444444"


# A task spec a run can actually be built from. `{}` was enough while the target was inert;
# now that a trigger starts a real run from it, the seeded content has to be a valid pipeline.
RUNNABLE_TASK: dict[str, Any] = {
    "componentRef": {
        "spec": {
            "name": "seeded-target",
            "implementation": {"graph": {"tasks": {}}},
        }
    }
}


def _add_pipeline(
    *,
    sess: orm.Session,
    pipeline_id: str,
    user_id: str,
    file_path: str,
    versioning_mode: user_pipeline_db_models.PipelineVersioningMode = (
        user_pipeline_db_models.PipelineVersioningMode.DISABLED
    ),
    deleted_at: Any = None,
) -> None:
    """Insert one pipeline, with no current version named yet.

    `current_version_key` is deliberately not a parameter: `pipeline` and `pipeline_version`
    reference each other -- fk_pipeline_current_version_key one way,
    fk_pipeline_version_pipeline_id the other -- so on an engine that enforces foreign keys
    there is no order in which both rows can arrive already complete. `_seed_pipeline` names
    the versions in a second pass.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id=user_id,
        file_path=file_path,
        versioning_mode=versioning_mode,
        deleted_at=deleted_at,
    )
    # `id` is init=False with a uuid default_factory, so the fixed id is assigned rather
    # than passed; done before the flush, it is the value that reaches the insert.
    pipeline.id = pipeline_id
    sess.add(pipeline)


def _seed_pipeline(*, db_engine: sqlalchemy.Engine) -> None:
    """Insert the pipelines the route tests target, once per engine.

    Four of them, because the ownership and soft-delete guard turns "which pipeline" into part
    of what is being tested: one live target for the default caller, one FULL-mode target with
    pinnable versions, one owned by somebody else, and one tombstone.
    """
    with orm.Session(bind=db_engine) as sess:
        if (
            sess.get(user_pipeline_db_models.UserPipeline, SEEDED_PIPELINE_ID)
            is not None
        ):
            return
        _add_pipeline(
            sess=sess,
            pipeline_id=SEEDED_PIPELINE_ID,
            user_id=DEFAULT_USER,
            file_path="pipelines/retrain.py",
        )
        _add_pipeline(
            sess=sess,
            pipeline_id=FULL_PIPELINE_ID,
            user_id=DEFAULT_USER,
            file_path="pipelines/versioned.py",
            versioning_mode=user_pipeline_db_models.PipelineVersioningMode.FULL,
        )
        _add_pipeline(
            sess=sess,
            pipeline_id=OTHER_USER_PIPELINE_ID,
            user_id=OTHER_USER,
            file_path="pipelines/theirs.py",
        )
        _add_pipeline(
            sess=sess,
            pipeline_id=DELETED_PIPELINE_ID,
            user_id=DEFAULT_USER,
            file_path="pipelines/gone.py",
            deleted_at=db_utils.utc_now(),
        )
        for version in (PINNABLE_VERSION, OTHER_PINNABLE_VERSION):
            # version_key == content_digest is what FULL mode writes; the resolver's whole job
            # is that this stops being true for the DISABLED head.
            sess.add(
                user_pipeline_db_models.UserPipelineVersion(
                    pipeline_id=FULL_PIPELINE_ID,
                    version_key=version,
                    content_digest=version,
                    root_pipeline_task=RUNNABLE_TASK,
                )
            )
        # The DISABLED pipeline's single row, keyed by the reserved sentinel rather than by
        # its version. Pinning against it is the 422 the resolver exists to produce.
        sess.add(
            user_pipeline_db_models.UserPipelineVersion(
                pipeline_id=SEEDED_PIPELINE_ID,
                version_key=user_pipeline_db_models.CURRENT_VERSION_KEY,
                content_digest=PINNABLE_VERSION,
                root_pipeline_task=RUNNABLE_TASK,
            )
        )
        for pipeline_id in (OTHER_USER_PIPELINE_ID, DELETED_PIPELINE_ID):
            sess.add(
                user_pipeline_db_models.UserPipelineVersion(
                    pipeline_id=pipeline_id,
                    version_key=user_pipeline_db_models.CURRENT_VERSION_KEY,
                    content_digest=PINNABLE_VERSION,
                    root_pipeline_task=RUNNABLE_TASK,
                )
            )
        # Second half of the cycle: the version rows exist now, so each pipeline may name the
        # one it points at.
        sess.flush()
        current_version_by_pipeline = {
            SEEDED_PIPELINE_ID: user_pipeline_db_models.CURRENT_VERSION_KEY,
            FULL_PIPELINE_ID: OTHER_PINNABLE_VERSION,
            OTHER_USER_PIPELINE_ID: user_pipeline_db_models.CURRENT_VERSION_KEY,
            DELETED_PIPELINE_ID: user_pipeline_db_models.CURRENT_VERSION_KEY,
        }
        for pipeline_id, version_key in current_version_by_pipeline.items():
            pipeline = sess.get(user_pipeline_db_models.UserPipeline, pipeline_id)
            assert pipeline is not None
            pipeline.current_version_key = version_key
        sess.commit()


def _make_user_details(*, name: str, admin: bool = False) -> api_router.UserDetails:
    return api_router.UserDetails(
        name=name,
        permissions=api_router.Permissions(read=True, write=True, admin=admin),
    )


def _build_app(
    *,
    db_engine: sqlalchemy.Engine,
    user_name: str,
    admin: bool = False,
) -> fastapi.FastAPI:
    """An app with the trigger routes mounted and one fixed caller.

    Sessions are bound to the same in-memory engine the `session` fixture uses, so a test can
    POST through the API and then assert on rows directly.
    """
    app = fastapi.FastAPI()

    def get_session() -> collections.abc.Iterator[orm.Session]:
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as sess:
            yield sess

    def user_details_getter() -> api_router.UserDetails:
        return _make_user_details(name=user_name, admin=admin)

    _seed_pipeline(db_engine=db_engine)
    api_routes.setup_trigger_routes(
        app=app,
        get_session=get_session,
        user_details_getter=user_details_getter,
    )
    return app


@pytest.fixture()
def client(db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    """The creator: every subscription in a test is created by this caller."""
    return fastapi.testclient.TestClient(
        _build_app(db_engine=db_engine, user_name=DEFAULT_USER)
    )


@pytest.fixture()
def fk_client(fk_db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    """The creator again, on the engine that actually enforces foreign keys.

    The default engine leaves them off, so a route that writes a reference no pipeline row can
    satisfy still returns 201 there. MySQL and Postgres both enforce, so a test that needs to
    see the difference between "stored" and "storable" has to ask this client.
    """
    return fastapi.testclient.TestClient(
        _build_app(db_engine=fk_db_engine, user_name=DEFAULT_USER)
    )


@pytest.fixture()
def other_user_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    """A different, non-admin caller — the one that must get a 403 on write."""
    return fastapi.testclient.TestClient(
        _build_app(db_engine=db_engine, user_name=OTHER_USER)
    )


@pytest.fixture()
def admin_client(db_engine: sqlalchemy.Engine) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        _build_app(db_engine=db_engine, user_name=ADMIN_USER, admin=True)
    )


@pytest.fixture()
def unauthenticated_client(
    db_engine: sqlalchemy.Engine,
) -> fastapi.testclient.TestClient:
    """A client whose caller cannot be named.

    Mirrors app.create_user_details (app.py:198), which raises 401 on a falsy name rather than
    returning an anonymous user — so every route inherits the refusal from the dependency.
    """
    app = fastapi.FastAPI()
    _seed_pipeline(db_engine=db_engine)

    def get_session() -> collections.abc.Iterator[orm.Session]:
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as sess:
            yield sess

    def user_details_getter() -> api_router.UserDetails:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )

    api_routes.setup_trigger_routes(
        app=app,
        get_session=get_session,
        user_details_getter=user_details_getter,
    )
    return fastapi.testclient.TestClient(app)
