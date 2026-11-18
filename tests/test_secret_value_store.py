import logging
import pathlib
from typing import Any

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


class _FakeStore:
    """Keeps values in memory, one numbered version per `save`, like a cloud secret manager."""

    def __init__(
        self,
        *,
        type: str = "fake",
        engine: sqlalchemy.Engine | None = None,
        fail_delete: bool = False,
    ) -> None:
        self.type = type
        self.versions: dict[str, str] = {}
        self.retired: list[str] = []
        # With an engine, `delete` records which version the committed row names at that moment.
        self._engine = engine
        self.row_version_at_delete: list[str | None] = []
        self._fail_delete = fail_delete

    def save(
        self, *, user_id: str, secret_name: str, value: str
    ) -> tuple[str, dict[str, Any]]:
        """Saves the value as the next version."""
        version = str(len(self.versions) + 1)
        self.versions[version] = value
        return "", {"version": version}

    def get(
        self, *, storage_details: dict[str, Any], user_id: str, secret_name: str
    ) -> str:
        """Returns the saved version."""
        return self.versions[storage_details["version"]]

    def delete(
        self, *, storage_details: dict[str, Any], user_id: str, secret_name: str
    ) -> None:
        """Retires the version, or fails when told to."""
        if self._engine is not None:
            with orm.Session(bind=self._engine) as session:
                row = session.get(bts.Secret, (user_id, secret_name))
                self.row_version_at_delete.append(
                    row.extra_data["storage"]["details"]["version"] if row else None
                )
        if self._fail_delete:
            raise RuntimeError("store unavailable")
        self.retired.append(storage_details["version"])


def _engine(*, tmp_path: pathlib.Path) -> sqlalchemy.Engine:
    """Returns a file database, so a second session sees only committed rows."""
    return database_ops.create_db_engine_and_migrate_db(
        database_uri=f"sqlite:///{tmp_path / 'db.sqlite'}"
    )


def _save(
    *,
    engine: sqlalchemy.Engine,
    store: _FakeStore | None,
    value: str,
    update: bool = False,
) -> None:
    """Creates or updates alice's TOKEN through the service."""
    service = api_server_sql.SecretsApiService(secret_value_store=store)
    method = service.update_secret if update else service.create_secret
    with orm.Session(bind=engine) as session:
        method(
            session=session, user_id="alice", secret_name="TOKEN", secret_value=value
        )


def _row(*, engine: sqlalchemy.Engine) -> bts.Secret | None:
    """Reads alice's TOKEN as committed."""
    with orm.Session(bind=engine, expire_on_commit=False) as session:
        return session.get(bts.Secret, ("alice", "TOKEN"))


def test_without_store_keeps_the_value_in_the_row(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    _save(engine=engine, store=None, value="s3cret")
    row = _row(engine=engine)
    assert row.secret_value == "s3cret"
    assert row.extra_data is None


def test_without_store_update_and_delete_work_as_before(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    _save(engine=engine, store=None, value="s3cret")
    _save(engine=engine, store=None, value="n3w", update=True)
    row = _row(engine=engine)
    assert (row.secret_value, row.extra_data) == ("n3w", None)
    with orm.Session(bind=engine) as session:
        api_server_sql.SecretsApiService().delete_secret(
            session=session, user_id="alice", secret_name="TOKEN"
        )
    assert _row(engine=engine) is None


def test_with_store_the_row_keeps_only_what_save_returned(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    _save(engine=engine, store=store, value="s3cret")
    row = _row(engine=engine)
    assert row.secret_value == ""
    assert row.extra_data == {"storage": {"type": "fake", "details": {"version": "1"}}}
    assert store.versions == {"1": "s3cret"}


def test_update_retires_the_previous_version_after_the_row_commits(
    tmp_path: pathlib.Path,
):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore(engine=engine)
    _save(engine=engine, store=store, value="v1")
    _save(engine=engine, store=store, value="v2", update=True)
    assert _row(engine=engine).extra_data["storage"]["details"] == {"version": "2"}
    assert store.retired == ["1"]
    # When version 1 was retired, the committed row already named version 2.
    assert store.row_version_at_delete == ["2"]


def test_delete_retires_the_version_after_the_row_is_gone(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore(engine=engine)
    _save(engine=engine, store=store, value="v1")
    with orm.Session(bind=engine) as session:
        api_server_sql.SecretsApiService(secret_value_store=store).delete_secret(
            session=session, user_id="alice", secret_name="TOKEN"
        )
    assert _row(engine=engine) is None
    assert store.retired == ["1"]
    assert store.row_version_at_delete == [None]


def test_a_plain_row_moves_into_the_store_on_update(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    _save(engine=engine, store=None, value="plain")
    _save(engine=engine, store=store, value="v2", update=True)
    row = _row(engine=engine)
    assert row.secret_value == ""
    assert row.extra_data["storage"]["details"] == {"version": "1"}
    assert store.retired == []


def test_other_extra_data_keys_survive(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    _save(engine=engine, store=None, value="plain")
    with orm.Session(bind=engine) as session:
        session.get(bts.Secret, ("alice", "TOKEN")).extra_data = {"owner_team": "data"}
        session.commit()
    _save(engine=engine, store=_FakeStore(), value="v2", update=True)
    extra_data = _row(engine=engine).extra_data
    assert extra_data["owner_team"] == "data"
    assert "storage" in extra_data


@pytest.mark.parametrize("server_store", [None, _FakeStore(type="other")])
def test_save_and_delete_refused_when_the_row_is_in_another_store(
    tmp_path: pathlib.Path, server_store: _FakeStore | None
):
    engine = _engine(tmp_path=tmp_path)
    _save(engine=engine, store=_FakeStore(), value="v1")
    before = _row(engine=engine)
    with pytest.raises(errors.SecretStorageTypeMismatchError, match="'fake' store"):
        _save(engine=engine, store=server_store, value="plain", update=True)
    with orm.Session(bind=engine) as session:
        with pytest.raises(errors.SecretStorageTypeMismatchError):
            api_server_sql.SecretsApiService(
                secret_value_store=server_store
            ).delete_secret(session=session, user_id="alice", secret_name="TOKEN")
    after = _row(engine=engine)
    assert (after.secret_value, after.extra_data) == (
        before.secret_value,
        before.extra_data,
    )
    if server_store is not None:
        assert server_store.versions == {}
        assert server_store.retired == []


def test_create_of_an_existing_name_saves_nothing_in_the_store(
    tmp_path: pathlib.Path,
):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    _save(engine=engine, store=store, value="v1")
    with pytest.raises(errors.ItemAlreadyExistsError):
        _save(engine=engine, store=store, value="v2")
    assert store.versions == {"1": "v1"}


def test_a_failed_retire_is_logged_and_the_update_stands(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore(fail_delete=True)
    _save(engine=engine, store=store, value="v1")
    with caplog.at_level(logging.ERROR):
        _save(engine=engine, store=store, value="v2", update=True)
    assert _row(engine=engine).extra_data["storage"]["details"] == {"version": "2"}
    assert "Could not retire the previous stored value of secret 'TOKEN'" in caplog.text
    assert "v1" not in caplog.text and "v2" not in caplog.text


def _client(*, engine: sqlalchemy.Engine, store: _FakeStore | None) -> TestClient:
    """Returns an API client for alice, with the given store passed to setup_routes."""
    app = fastapi.FastAPI()
    api_router.setup_routes(
        app=app,
        db_engine=engine,
        user_details_getter=lambda: api_router.UserDetails(
            name="alice", permissions={"read": True, "write": True, "admin": False}
        ),
        secret_value_store=store,
    )
    return TestClient(app)


def test_setup_routes_passes_the_store_to_secret_routes(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    store = _FakeStore()
    response = _client(engine=engine, store=store).post(
        "/api/secrets/",
        params={"secret_name": "TOKEN"},
        json={"secret_value": "s3cret"},
    )
    assert response.status_code == 200, response.text
    assert _row(engine=engine).secret_value == ""
    assert store.versions == {"1": "s3cret"}


def test_a_refusal_is_a_503_naming_both_stores(tmp_path: pathlib.Path):
    engine = _engine(tmp_path=tmp_path)
    _save(engine=engine, store=_FakeStore(), value="v1")
    response = _client(engine=engine, store=None).delete("/api/secrets/TOKEN")
    assert response.status_code == 503, response.text
    assert "'fake' store" in response.json()["message"]
    assert "None" in response.json()["message"]
    assert _row(engine=engine) is not None
