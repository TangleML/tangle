import pathlib
from typing import Any

import fastapi
from sqlalchemy import orm
from starlette.testclient import TestClient

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops


class _FakeCipher:
    type = "fake"

    def encrypt(
        self, *, plaintext: str, user_id: str, secret_name: str
    ) -> tuple[str, dict[str, Any]]:
        return f"{user_id}/{secret_name}/{plaintext[::-1]}", {"key_version": "1"}

    def decrypt(
        self,
        *,
        stored: str,
        details: dict[str, Any],
        user_id: str,
        secret_name: str,
    ) -> str:
        return stored.removeprefix(f"{user_id}/{secret_name}/")[::-1]


def _session() -> orm.Session:
    engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return orm.Session(bind=engine)


def _create(
    *, session: orm.Session, cipher: _FakeCipher | None, value: str = "s3cret"
) -> bts.Secret:
    api_server_sql.SecretsApiService(secret_value_cipher=cipher).create_secret(
        session=session, user_id="alice", secret_name="TOKEN", secret_value=value
    )
    return session.get(bts.Secret, ("alice", "TOKEN"))


def test_create_with_cipher_stores_ciphertext_and_entry():
    secret = _create(session=_session(), cipher=_FakeCipher())
    assert secret.secret_value == "alice/TOKEN/terc3s"
    assert secret.extra_data["encryption"]["type"] == "fake"
    assert secret.extra_data["encryption"]["details"] == {"key_version": "1"}
    assert secret.extra_data["encryption"]["encrypted_at"]


def test_update_with_cipher_re_encrypts():
    session = _session()
    _create(session=session, cipher=_FakeCipher())
    api_server_sql.SecretsApiService(secret_value_cipher=_FakeCipher()).update_secret(
        session=session, user_id="alice", secret_name="TOKEN", secret_value="new"
    )
    assert session.get(bts.Secret, ("alice", "TOKEN")).secret_value == "alice/TOKEN/wen"


def test_without_cipher_stores_plaintext_and_removes_stale_entry():
    session = _session()
    _create(session=session, cipher=_FakeCipher())
    api_server_sql.SecretsApiService().update_secret(
        session=session, user_id="alice", secret_name="TOKEN", secret_value="plain"
    )
    secret = session.get(bts.Secret, ("alice", "TOKEN"))
    assert secret.secret_value == "plain"
    assert "encryption" not in secret.extra_data


def test_without_cipher_leaves_extra_data_untouched():
    assert _create(session=_session(), cipher=None).extra_data is None


def test_other_extra_data_keys_survive():
    session = _session()
    secret = _create(session=session, cipher=None)
    secret.extra_data = {"owner_team": "data"}
    session.commit()
    api_server_sql.SecretsApiService(secret_value_cipher=_FakeCipher()).update_secret(
        session=session, user_id="alice", secret_name="TOKEN", secret_value="v2"
    )
    secret = session.get(bts.Secret, ("alice", "TOKEN"))
    assert secret.extra_data["owner_team"] == "data"
    assert "encryption" in secret.extra_data


def test_setup_routes_passes_cipher_to_secret_routes(tmp_path: pathlib.Path):
    engine = database_ops.create_db_engine_and_migrate_db(
        database_uri=f"sqlite:///{tmp_path / 'db.sqlite'}"
    )
    app = fastapi.FastAPI()
    api_router.setup_routes(
        app=app,
        db_engine=engine,
        user_details_getter=lambda: api_router.UserDetails(
            name="alice", permissions={"read": True, "write": True, "admin": False}
        ),
        secret_value_cipher=_FakeCipher(),
    )
    response = TestClient(app).post(
        "/api/secrets/",
        params={"secret_name": "TOKEN"},
        json={"secret_value": "s3cret"},
    )
    assert response.status_code == 200, response.text
    with orm.Session(bind=engine) as session:
        stored = session.get(bts.Secret, ("alice", "TOKEN")).secret_value
    assert stored == "alice/TOKEN/terc3s"
