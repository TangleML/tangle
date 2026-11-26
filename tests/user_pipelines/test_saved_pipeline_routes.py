import collections.abc
import pathlib

import fastapi
import fastapi.testclient
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops


def _user_details(request: fastapi.Request) -> api_router.UserDetails:
    return api_router.UserDetails(
        name=request.headers.get("x-user", "admin"),
        permissions=api_router.Permissions(
            read=True,
            write=True,
            admin=request.headers.get("x-admin", "true") == "true",
        ),
    )


def _client() -> tuple[fastapi.testclient.TestClient, sqlalchemy.Engine]:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    database_ops.initialize_and_migrate_db(db_engine=db_engine)
    app = fastapi.FastAPI()
    api_router.setup_routes(
        app=app, db_engine=db_engine, user_details_getter=_user_details
    )
    return fastapi.testclient.TestClient(app), db_engine


def test_database_init_creates_the_saved_pipeline_tables() -> None:
    _, db_engine = _client()

    tables = set(sqlalchemy.inspect(db_engine).get_table_names())

    assert {"pipeline", "pipeline_version"} <= tables


def test_saved_pipeline_round_trip() -> None:
    client, _ = _client()

    saved = client.put(
        "/api/users/me/pipelines",
        params={"file_path": "first.yaml"},
        json={
            "root_pipeline_task": {
                "componentRef": {
                    "spec": {
                        "name": "First",
                        "implementation": {"graph": {"tasks": {}}},
                    }
                }
            }
        },
    )
    assert saved.status_code == 200
    search = client.post("/api/pipelines/search", json={})
    assert [row["id"] for row in search.json()["pipelines"]] == [saved.json()["id"]]


def _multi_tenant_client(
    *, tmp_path: pathlib.Path
) -> tuple[fastapi.testclient.TestClient, dict[str, sqlalchemy.Engine]]:
    """Builds an app with one migrated SQLite file per tenant, picked by the `x-user` header."""
    db_engines: dict[str, sqlalchemy.Engine] = {}

    def get_db_engine_for_tenant(tenant_id: str) -> sqlalchemy.Engine:
        """Creates and migrates a tenant's database on first use."""
        if tenant_id not in db_engines:
            db_engines[tenant_id] = database_ops.create_db_engine_and_migrate_db(
                database_uri=f"sqlite:///{tmp_path / tenant_id / 'db.sqlite'}"
            )
        return db_engines[tenant_id]

    def get_session(
        request: fastapi.Request,
    ) -> collections.abc.Iterator[orm.Session]:
        """Yields a session on the requesting tenant's database."""
        tenant_id = request.headers.get("x-user", "admin")
        (tmp_path / tenant_id).mkdir(exist_ok=True)
        with orm.Session(bind=get_db_engine_for_tenant(tenant_id)) as session:
            yield session

    app = fastapi.FastAPI()
    api_router._setup_routes_internal(
        app=app, get_session=get_session, user_details_getter=_user_details
    )
    return fastapi.testclient.TestClient(app), db_engines


def test_saved_pipelines_are_isolated_per_tenant_database(
    tmp_path: pathlib.Path,
) -> None:
    client, db_engines = _multi_tenant_client(tmp_path=tmp_path)
    body = {
        "root_pipeline_task": {
            "componentRef": {
                "spec": {"name": "P", "implementation": {"graph": {"tasks": {}}}}
            }
        }
    }

    alice = client.put(
        "/api/users/me/pipelines",
        params={"file_path": "p.yaml"},
        headers={"x-user": "alice"},
        json=body,
    )
    bob_search = client.post(
        "/api/pipelines/search", json={}, headers={"x-user": "bob"}
    )
    alice_search = client.post(
        "/api/pipelines/search", json={}, headers={"x-user": "alice"}
    )

    assert alice.status_code == 200
    assert bob_search.json()["pipelines"] == []
    assert [row["id"] for row in alice_search.json()["pipelines"]] == [
        alice.json()["id"]
    ]
    assert set(db_engines) == {"alice", "bob"}


def test_migrating_an_existing_tenant_database_again_keeps_its_pipelines(
    tmp_path: pathlib.Path,
) -> None:
    client, db_engines = _multi_tenant_client(tmp_path=tmp_path)
    client.put(
        "/api/users/me/pipelines",
        params={"file_path": "p.yaml"},
        headers={"x-user": "alice"},
        json={
            "root_pipeline_task": {
                "componentRef": {
                    "spec": {"name": "P", "implementation": {"graph": {"tasks": {}}}}
                }
            }
        },
    )
    db_engines["alice"].dispose()

    reopened = database_ops.create_db_engine_and_migrate_db(
        database_uri=f"sqlite:///{tmp_path / 'alice' / 'db.sqlite'}"
    )

    with reopened.connect() as connection:
        count = connection.execute(
            sqlalchemy.text("SELECT COUNT(*) FROM pipeline")
        ).scalar_one()
    assert count == 1


def test_database_without_saved_pipeline_tables_gains_them_on_migration(
    tmp_path: pathlib.Path,
) -> None:
    database_uri = f"sqlite:///{tmp_path / 'db.sqlite'}"
    legacy_engine = database_ops.create_db_engine(database_uri=database_uri)
    tables_before_saved_pipelines = [
        table
        for name, table in bts._TableBase.metadata.tables.items()
        if name not in ("pipeline", "pipeline_version")
    ]
    bts._TableBase.metadata.create_all(
        legacy_engine, tables=tables_before_saved_pipelines
    )
    legacy_engine.dispose()

    migrated = database_ops.create_db_engine_and_migrate_db(database_uri=database_uri)

    tables = set(sqlalchemy.inspect(migrated).get_table_names())
    assert {"pipeline", "pipeline_version"} <= tables
