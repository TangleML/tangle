import dataclasses
import pathlib

import fastapi
import pytest
import sqlalchemy
from sqlalchemy import orm
from starlette.testclient import TestClient

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors
from cloud_pipelines_backend import secret_store


class _FakeStore(secret_store.SecretStore):
    """An in-memory `SecretStore` that follows its contract, keyed by user and name."""

    def __init__(self) -> None:
        self.secrets: dict[tuple[str, str], secret_store.StoredSecret] = {}

    def create(self, *, secret: secret_store.StoredSecret) -> secret_store.StoredSecret:
        key = (secret.user_id, secret.secret_name)
        if key in self.secrets:
            raise errors.ItemAlreadyExistsError("exists")
        self.secrets[key] = secret
        return dataclasses.replace(secret, value=None)

    def update(self, *, secret: secret_store.StoredSecret) -> secret_store.StoredSecret:
        key = (secret.user_id, secret.secret_name)
        old = self.secrets.get(key)
        if old is None:
            raise errors.ItemNotFoundError("missing")
        self.secrets[key] = dataclasses.replace(
            secret,
            secret_name=old.secret_name,
            created_at=old.created_at,
            description=(
                old.description if secret.description is None else secret.description
            ),
        )
        return dataclasses.replace(self.secrets[key], value=None)

    def get(self, *, user_id: str, secret_name: str) -> str | None:
        secret = self.secrets.get((user_id, secret_name))
        return secret.value if secret else None

    def list_secrets(self, *, user_id: str) -> list[secret_store.StoredSecret]:
        return [
            dataclasses.replace(secret, value=None)
            for (owner, _), secret in self.secrets.items()
            if owner == user_id
        ]

    def delete(self, *, user_id: str, secret_name: str) -> None:
        if self.secrets.pop((user_id, secret_name), None) is None:
            raise errors.ItemNotFoundError("missing")


def _engine(*, tmp_path: pathlib.Path) -> sqlalchemy.Engine:
    """Returns a file database, so a second session sees only committed rows."""
    return database_ops.create_db_engine_and_migrate_db(
        database_uri=f"sqlite:///{tmp_path / 'db.sqlite'}"
    )


def _service(*, store: _FakeStore | None) -> api_server_sql.SecretsApiService:
    return api_server_sql.SecretsApiService(secret_store=store)


def _create(
    *,
    engine: sqlalchemy.Engine,
    store: _FakeStore | None,
    user_id: str = "alice",
    secret_name: str = "TOKEN",
    value: str = "v1",
    description: str | None = None,
) -> api_server_sql.SecretInfoResponse:
    with orm.Session(bind=engine) as session:
        return _service(store=store).create_secret(
            session=session,
            user_id=user_id,
            secret_name=secret_name,
            secret_value=value,
            description=description,
        )


def _update(
    *,
    engine: sqlalchemy.Engine,
    store: _FakeStore | None,
    value: str,
    description: str | None = None,
) -> api_server_sql.SecretInfoResponse:
    with orm.Session(bind=engine) as session:
        return _service(store=store).update_secret(
            session=session,
            user_id="alice",
            secret_name="TOKEN",
            secret_value=value,
            description=description,
        )


def _rows(*, engine: sqlalchemy.Engine) -> list[bts.Secret]:
    """Reads every committed row."""
    with orm.Session(bind=engine, expire_on_commit=False) as session:
        return list(session.scalars(sqlalchemy.select(bts.Secret)).all())


def test_without_store_the_table_works_as_before(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="v1")
    _update(engine=engine, store=None, value="v2")
    [row] = _rows(engine=engine)
    assert row.secret_value == "v2"
    with orm.Session(bind=engine) as session:
        _service(store=None).delete_secret(
            session=session, user_id="alice", secret_name="TOKEN"
        )
    assert _rows(engine=engine) == []


def test_create_saves_in_the_store_and_writes_no_row(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    response = _create(engine=engine, store=store, value="v1", description="d")
    assert store.get(user_id="alice", secret_name="TOKEN") == "v1"
    assert response.secret_name == "TOKEN"
    assert response.description == "d"
    assert _rows(engine=engine) == []


def _stored(*, value: str) -> secret_store.StoredSecret:
    now = api_server_sql._get_current_time()
    return secret_store.StoredSecret(
        user_id="alice",
        secret_name="TOKEN",
        value=value,
        created_at=now,
        updated_at=now,
    )


def _delete(*, engine: sqlalchemy.Engine, store: _FakeStore | None) -> None:
    with orm.Session(bind=engine) as session:
        _service(store=store).delete_secret(
            session=session, user_id="alice", secret_name="TOKEN"
        )


def test_create_of_a_name_still_in_the_table_conflicts(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="old")
    store = _FakeStore()
    with pytest.raises(errors.ItemAlreadyExistsError):
        _create(engine=engine, store=store, value="new")
    assert store.secrets == {}
    assert [row.secret_value for row in _rows(engine=engine)] == ["old"]


def test_update_of_a_row_updates_it_where_it_is(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="old")
    store = _FakeStore()
    _update(engine=engine, store=store, value="new")
    assert store.secrets == {}
    assert [row.secret_value for row in _rows(engine=engine)] == ["new"]


def test_update_of_a_secret_in_the_store_updates_the_store(
    tmp_path: pathlib.Path,
) -> None:
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    created = _create(engine=engine, store=store, value="old", description="d")
    response = _update(engine=engine, store=store, value="new")
    assert store.get(user_id="alice", secret_name="TOKEN") == "new"
    assert response.created_at == created.created_at
    assert response.description == "d"
    assert _rows(engine=engine) == []


def test_update_with_an_empty_description_keeps_the_stored_one(
    tmp_path: pathlib.Path,
) -> None:
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    _create(engine=engine, store=store, description="keep me")
    response = _update(engine=engine, store=store, value="v2", description="")
    assert response.description == "keep me"


def test_update_of_a_missing_secret_is_not_found(tmp_path: pathlib.Path) -> None:
    with pytest.raises(errors.ItemNotFoundError):
        _update(engine=_engine(tmp_path=tmp_path), store=_FakeStore(), value="v")


def test_delete_of_a_row_deletes_the_row(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None)
    _delete(engine=engine, store=_FakeStore())
    assert _rows(engine=engine) == []


def test_delete_of_a_row_also_deletes_a_copy_left_in_the_store(
    tmp_path: pathlib.Path,
) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None)
    store = _FakeStore()
    store.create(secret=_stored(value="copy"))
    _delete(engine=engine, store=store)
    assert store.secrets == {}
    assert _rows(engine=engine) == []


def test_delete_of_a_secret_in_the_store(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    _create(engine=engine, store=store)
    _delete(engine=engine, store=store)
    assert store.secrets == {}
    with pytest.raises(errors.ItemNotFoundError):
        _delete(engine=engine, store=store)


def test_list_reads_only_the_store(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, secret_name="IN_TABLE")
    store = _FakeStore()
    _create(engine=engine, store=store, secret_name="IN_STORE")
    _create(engine=engine, store=store, user_id="bob", secret_name="BOBS")
    with orm.Session(bind=engine) as session:
        listed = _service(store=store).list_secrets(session=session, user_id="alice")
    assert [secret.secret_name for secret in listed.secrets] == ["IN_STORE"]
    assert [row.secret_name for row in _rows(engine=engine)] == ["IN_TABLE"]


@pytest.mark.parametrize("with_store", [True, False])
def test_the_row_is_locked_only_with_a_store(
    tmp_path: pathlib.Path, with_store: bool
) -> None:
    """A backfill moving the row then waits; without a store the read stays as before."""
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None)
    store = _FakeStore() if with_store else None
    locks = []
    with orm.Session(bind=engine) as session:
        sqlalchemy.event.listen(
            session,
            "do_orm_execute",
            lambda state: locks.append(state.statement._for_update_arg is not None),
        )
        service = _service(store=store)
        service.update_secret(
            session=session, user_id="alice", secret_name="TOKEN", secret_value="v2"
        )
        session.expunge_all()
        service.delete_secret(session=session, user_id="alice", secret_name="TOKEN")
    assert locks[:1] + locks[-1:] == [with_store, with_store]


def test_stored_secret_never_shows_its_value() -> None:
    secret = _stored(value="hunter2")
    assert "hunter2" not in repr(secret)
    assert "hunter2" not in f"{secret}"


def _client(*, engine: sqlalchemy.Engine, store: _FakeStore | None) -> TestClient:
    """Returns an API client for alice, with the given store passed to setup_routes."""
    app = fastapi.FastAPI()
    api_router.setup_routes(
        app=app,
        db_engine=engine,
        user_details_getter=lambda: api_router.UserDetails(
            name="alice", permissions={"read": True, "write": True, "admin": False}
        ),
        secret_store=store,
    )
    return TestClient(app)


def test_setup_routes_passes_the_store_to_secret_routes(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    client = _client(engine=engine, store=store)
    created = client.post(
        "/api/secrets/", params={"secret_name": "TOKEN"}, json={"secret_value": "v1"}
    )
    assert created.status_code == 200, created.text
    again = client.post(
        "/api/secrets/", params={"secret_name": "TOKEN"}, json={"secret_value": "v2"}
    )
    assert again.status_code == 409, again.text
    listed = client.get("/api/secrets/")
    assert [s["secret_name"] for s in listed.json()["secrets"]] == ["TOKEN"]
    assert store.get(user_id="alice", secret_name="TOKEN") == "v1"
    assert _rows(engine=engine) == []
