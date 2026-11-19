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
    """An in-memory `SecretStore` that follows the base class's contract, keyed by user and name."""

    def __init__(self, *, fail_create: bool = False) -> None:
        self.secrets: dict[tuple[str, str], secret_store.StoredSecret] = {}
        self._fail_create = fail_create

    def create(self, *, secret: secret_store.StoredSecret) -> secret_store.StoredSecret:
        if self._fail_create:
            raise RuntimeError("store unavailable")
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


def test_create_of_a_name_still_in_the_table_moves_it_and_conflicts(
    tmp_path: pathlib.Path,
) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="old")
    store = _FakeStore()
    with pytest.raises(errors.ItemAlreadyExistsError):
        _create(engine=engine, store=store, value="new")
    assert store.get(user_id="alice", secret_name="TOKEN") == "old"
    assert _rows(engine=engine) == []


def test_update_moves_the_row_then_saves_the_new_value(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="old", description="from the row")
    [row] = _rows(engine=engine)
    store = _FakeStore()
    response = _update(engine=engine, store=store, value="new")
    assert store.get(user_id="alice", secret_name="TOKEN") == "new"
    assert response.created_at == row.created_at
    assert response.updated_at > row.updated_at
    assert response.description == "from the row"
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


def test_delete_removes_a_secret_still_in_the_table(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None)
    store = _FakeStore()
    with orm.Session(bind=engine) as session:
        _service(store=store).delete_secret(
            session=session, user_id="alice", secret_name="TOKEN"
        )
    assert store.secrets == {}
    assert _rows(engine=engine) == []
    with orm.Session(bind=engine) as session, pytest.raises(errors.ItemNotFoundError):
        _service(store=store).delete_secret(
            session=session, user_id="alice", secret_name="TOKEN"
        )


def test_list_moves_only_the_callers_rows(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, secret_name="A")
    _create(engine=engine, store=None, secret_name="B")
    _create(engine=engine, store=None, user_id="bob", secret_name="C")
    store = _FakeStore()
    _create(engine=engine, store=store, secret_name="D")
    with orm.Session(bind=engine) as session:
        listed = _service(store=store).list_secrets(session=session, user_id="alice")
    assert sorted(secret.secret_name for secret in listed.secrets) == ["A", "B", "D"]
    assert [(row.user_id, row.secret_name) for row in _rows(engine=engine)] == [
        ("bob", "C")
    ]


def test_move_rows_carries_every_column(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="v1", description="d")
    with orm.Session(bind=engine) as session:
        session.get(bts.Secret, ("alice", "TOKEN")).extra_data = {"k": "v"}
        session.commit()
    [row] = _rows(engine=engine)
    store = _FakeStore()
    with orm.Session(bind=engine) as session:
        secret_store.move_rows(
            session=session, secret_store=store, user_id="alice", secret_name="TOKEN"
        )
    assert store.secrets[("alice", "TOKEN")] == secret_store.StoredSecret(
        user_id="alice",
        secret_name="TOKEN",
        value="v1",
        description="d",
        created_at=row.created_at,
        updated_at=row.updated_at,
        extra_data={"k": "v"},
    )
    assert _rows(engine=engine) == []


def test_move_rows_keeps_the_row_when_the_store_fails(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="v1")
    with orm.Session(bind=engine) as session, pytest.raises(RuntimeError):
        secret_store.move_rows(
            session=session,
            secret_store=_FakeStore(fail_create=True),
            user_id="alice",
            secret_name="TOKEN",
        )
    [row] = _rows(engine=engine)
    assert row.secret_value == "v1"


def test_stored_secret_never_shows_its_value() -> None:
    secret = secret_store.StoredSecret(
        user_id="alice",
        secret_name="TOKEN",
        value="hunter2",
        created_at=api_server_sql._get_current_time(),
        updated_at=api_server_sql._get_current_time(),
    )
    assert "hunter2" not in repr(secret)
    assert "hunter2" not in f"{secret}"


def test_every_store_method_a_subclass_defines_moves_rows_first() -> None:
    for name in ("create", "update", "get", "list_secrets", "delete"):
        assert (
            getattr(_FakeStore, name).__wrapped__.__qualname__ == f"_FakeStore.{name}"
        )

    class _Incomplete(secret_store.SecretStore):
        def create(
            self, *, secret: secret_store.StoredSecret
        ) -> secret_store.StoredSecret:
            return secret

    with pytest.raises(TypeError, match="abstract"):
        _Incomplete()


def test_a_call_without_a_session_leaves_the_table_alone(
    tmp_path: pathlib.Path,
) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="v1")
    store = _FakeStore()
    assert store.get(user_id="alice", secret_name="TOKEN") is None
    assert len(_rows(engine=engine)) == 1


def test_get_with_a_session_moves_the_row_first(tmp_path: pathlib.Path) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="v1")
    store = _FakeStore()
    with orm.Session(bind=engine) as session:
        value = store.get(session=session, user_id="alice", secret_name="TOKEN")
    assert value == "v1"
    assert _rows(engine=engine) == []


def test_a_move_that_failed_before_deleting_the_row_finishes_next_time(
    tmp_path: pathlib.Path,
) -> None:
    engine = _engine(tmp_path=tmp_path)
    _create(engine=engine, store=None, value="old")
    [row] = _rows(engine=engine)
    store = _FakeStore()
    store.create(
        secret=secret_store.StoredSecret(
            user_id="alice",
            secret_name="TOKEN",
            value="old",
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
    )
    _update(engine=engine, store=store, value="new")
    assert store.get(user_id="alice", secret_name="TOKEN") == "new"
    assert _rows(engine=engine) == []


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
