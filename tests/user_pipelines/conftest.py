import collections.abc

import fastapi
import fastapi.testclient
import pytest
import sqlalchemy
from cloud_pipelines_backend import api_router, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.user_pipelines import api_routes
from sqlalchemy import event, orm

DEFAULT_USER = "alice@example.com"
OTHER_USER = "bob@example.com"


def pipeline_task(*, name: str) -> dict:
    return {
        "componentRef": {
            "spec": {
                "name": name,
                "implementation": {"graph": {"tasks": {}}},
            }
        }
    }


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, connection_record) -> None:
        del connection_record
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    bts._TableBase.metadata.create_all(engine)
    return engine


@pytest.fixture()
def app(db_engine: sqlalchemy.Engine) -> fastapi.FastAPI:
    test_app = fastapi.FastAPI()

    def get_session() -> collections.abc.Iterator[orm.Session]:
        with orm.Session(bind=db_engine) as session:
            yield session

    def get_user_details(request: fastapi.Request) -> api_router.UserDetails:
        return api_router.UserDetails(
            name=request.headers.get("x-user", DEFAULT_USER),
            permissions=api_router.Permissions(
                read=request.headers.get("x-read", "true") == "true",
                write=request.headers.get("x-write", "true") == "true",
                admin=False,
            ),
        )

    api_routes.setup_user_pipeline_routes(
        app=test_app,
        get_session=get_session,
        user_details_getter=get_user_details,
    )
    return test_app


@pytest.fixture()
def client(app: fastapi.FastAPI) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(app)


@pytest.fixture()
def other_user_client(app: fastapi.FastAPI) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        app,
        headers={"x-user": OTHER_USER},
    )
