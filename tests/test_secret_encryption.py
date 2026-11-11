import pathlib
from typing import Any
from unittest import mock

import fastapi
from sqlalchemy import orm
from starlette.testclient import TestClient

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


class _FakeCipher:
    def __init__(self, type: str = "fake"):
        self.type = type

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
        assert details == {"key_version": "1"}
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


def _launch_secret(
    *, write_cipher: _FakeCipher | None, read_cipher: _FakeCipher | None
) -> tuple[mock.MagicMock, bts.ExecutionNode]:
    engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    session_factory = lambda: orm.Session(bind=engine)
    api_server_sql.SecretsApiService(secret_value_cipher=write_cipher).create_secret(
        session=session_factory(),
        user_id="alice",
        secret_name="TOKEN",
        secret_value="s3cret",
    )
    component = structures.ComponentSpec(
        inputs=[structures.InputSpec(name="token")],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image="python")
        ),
    )
    task = structures.TaskSpec(
        component_ref=structures.ComponentReference(spec=component),
        arguments={
            "token": structures.DynamicDataArgument(
                dynamic_data={"secret": {"name": "TOKEN"}}
            )
        },
    )
    pipeline = structures.ComponentSpec(
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(tasks={"task": task})
        )
    )
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=structures.TaskSpec(
            component_ref=structures.ComponentReference(spec=pipeline)
        ),
        created_by="alice",
    )
    launch = mock.MagicMock(
        return_value=mock.MagicMock(
            status=launcher_interfaces.ContainerStatus.PENDING, to_dict=lambda: {}
        )
    )
    orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=mock.MagicMock(launch_container_task=launch),
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
        secret_value_cipher=read_cipher,
    ).process_each_queue_once()
    execution = (
        session_factory()
        .query(bts.ExecutionNode)
        .filter(bts.ExecutionNode.container_execution_status.is_not(None))
        .one()
    )
    return launch, execution


def _launched_value(launch: mock.MagicMock) -> str:
    return launch.call_args.kwargs["input_arguments"]["token"].value


def _system_error(execution: bts.ExecutionNode) -> str:
    assert (
        execution.container_execution_status
        == bts.ContainerExecutionStatus.SYSTEM_ERROR
    )
    return execution.extra_data[
        bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY
    ]


def test_launch_passes_plaintext_row_as_is():
    launch, _ = _launch_secret(write_cipher=None, read_cipher=_FakeCipher())
    assert _launched_value(launch) == "s3cret"


def test_launch_decrypts_with_matching_cipher():
    launch, _ = _launch_secret(write_cipher=_FakeCipher(), read_cipher=_FakeCipher())
    assert _launched_value(launch) == "s3cret"


def test_launch_fails_when_cipher_type_differs():
    launch, execution = _launch_secret(
        write_cipher=_FakeCipher(), read_cipher=_FakeCipher(type="other")
    )
    launch.assert_not_called()
    assert "encrypted with 'fake'" in _system_error(execution)


def test_launch_fails_without_cipher_for_encrypted_row():
    launch, execution = _launch_secret(write_cipher=_FakeCipher(), read_cipher=None)
    launch.assert_not_called()
    assert "configured cipher is None" in _system_error(execution)
