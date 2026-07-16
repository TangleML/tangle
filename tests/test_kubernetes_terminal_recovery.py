from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest import mock

import pytest
from kubernetes import client as k8s_client_lib
from kubernetes.client import exceptions as k8s_exceptions
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers

_NAMESPACE = "terminal-recovery"
_RESOURCE_NAME = "execution-resource"
_LOG_URI = "memory://execution.log"
_NOW = datetime.datetime.now(datetime.timezone.utc)


def _make_pod(status: interfaces.ContainerStatus, *, name: str = _RESOURCE_NAME):
    phase = {
        interfaces.ContainerStatus.PENDING: "Pending",
        interfaces.ContainerStatus.RUNNING: "Running",
        interfaces.ContainerStatus.SUCCEEDED: "Succeeded",
        interfaces.ContainerStatus.FAILED: "Failed",
    }[status]
    return k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name=name, namespace=_NAMESPACE),
        spec=k8s_client_lib.V1PodSpec(
            containers=[k8s_client_lib.V1Container(name="main")],
            restart_policy="Never",
        ),
        status=k8s_client_lib.V1PodStatus(phase=phase),
    )


def _make_job(status: interfaces.ContainerStatus):
    conditions = []
    if status == interfaces.ContainerStatus.SUCCEEDED:
        conditions.append(
            k8s_client_lib.V1JobCondition(
                type="Complete", status="True", last_transition_time=_NOW
            )
        )
    elif status == interfaces.ContainerStatus.FAILED:
        conditions.append(
            k8s_client_lib.V1JobCondition(
                type="Failed", status="True", last_transition_time=_NOW
            )
        )
    return k8s_client_lib.V1Job(
        metadata=k8s_client_lib.V1ObjectMeta(name=_RESOURCE_NAME, namespace=_NAMESPACE),
        spec=k8s_client_lib.V1JobSpec(
            completions=1,
            completion_mode="Indexed",
            template=k8s_client_lib.V1PodTemplateSpec(
                spec=k8s_client_lib.V1PodSpec(
                    containers=[k8s_client_lib.V1Container(name="main")],
                    restart_policy="Never",
                )
            ),
        ),
        status=k8s_client_lib.V1JobStatus(
            conditions=conditions,
            active=(1 if status == interfaces.ContainerStatus.RUNNING else None),
        ),
    )


def _make_kubernetes_resource(
    kind: str,
    status: interfaces.ContainerStatus,
    *,
    storage_provider=None,
):
    launcher = SimpleNamespace(
        _api_client=object(),
        _request_timeout=10,
        _storage_provider=storage_provider or mock.MagicMock(),
    )
    if kind == "Pod":
        return kubernetes_launchers.LaunchedKubernetesContainer(
            pod_name=_RESOURCE_NAME,
            namespace=_NAMESPACE,
            output_uris={},
            log_uri=_LOG_URI,
            debug_pod=_make_pod(status),
            launcher=launcher,
        )
    return kubernetes_launchers.LaunchedKubernetesJob(
        job_name=_RESOURCE_NAME,
        namespace=_NAMESPACE,
        output_uris={},
        log_uri=_LOG_URI,
        debug_job=_make_job(status),
        debug_pods={},
        launcher=launcher,
    )


def _install_kubernetes_api_mocks(monkeypatch, *, core_api=None, batch_api=None):
    core_api = core_api or mock.MagicMock()
    batch_api = batch_api or mock.MagicMock()
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", lambda *args, **kwargs: core_api)
    monkeypatch.setattr(k8s_client_lib, "BatchV1Api", lambda *args, **kwargs: batch_api)
    return core_api, batch_api


@pytest.mark.parametrize(
    ("kind", "cached_status"),
    [
        ("Pod", interfaces.ContainerStatus.RUNNING),
        ("Pod", interfaces.ContainerStatus.SUCCEEDED),
        ("Job", interfaces.ContainerStatus.RUNNING),
        ("Job", interfaces.ContainerStatus.FAILED),
    ],
)
def test_authoritative_get_404_raises_typed_launcher_error(
    monkeypatch, kind, cached_status
):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    api_error = k8s_exceptions.ApiException(status=404, reason="Not Found")
    api_error.body = "sensitive response body"
    if kind == "Pod":
        core_api.read_namespaced_pod.side_effect = api_error
    else:
        batch_api.read_namespaced_job.side_effect = api_error

    resource = _make_kubernetes_resource(kind, cached_status)
    with pytest.raises(interfaces.LauncherResourceNotFound) as exc_info:
        resource.get_refreshed()

    error = exc_info.value
    assert error.kind == kind
    assert error.namespace == _NAMESPACE
    assert error.name == _RESOURCE_NAME
    assert error.cached_status == cached_status
    assert str(error)
    assert "sensitive response body" not in str(error)
    assert error.__cause__ is None
    assert error.__suppress_context__


@pytest.mark.parametrize("kind", ["Pod", "Job"])
def test_authoritative_get_non_404_is_not_reclassified(monkeypatch, kind):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    api_error = k8s_exceptions.ApiException(status=500, reason="Server Error")
    if kind == "Pod":
        core_api.read_namespaced_pod.side_effect = api_error
    else:
        batch_api.read_namespaced_job.side_effect = api_error

    with pytest.raises(k8s_exceptions.ApiException) as exc_info:
        _make_kubernetes_resource(
            kind, interfaces.ContainerStatus.RUNNING
        ).get_refreshed()

    assert exc_info.value is api_error


@pytest.mark.parametrize(
    ("parent_status", "child_result"),
    [
        (interfaces.ContainerStatus.SUCCEEDED, []),
        (interfaces.ContainerStatus.FAILED, []),
        (
            interfaces.ContainerStatus.SUCCEEDED,
            k8s_exceptions.ApiException(status=404, reason="Not Found"),
        ),
        (
            interfaces.ContainerStatus.FAILED,
            k8s_exceptions.ApiException(status=404, reason="Not Found"),
        ),
        (
            interfaces.ContainerStatus.SUCCEEDED,
            k8s_exceptions.ApiException(status=500, reason="Server Error"),
        ),
        (
            interfaces.ContainerStatus.FAILED,
            k8s_exceptions.ApiException(status=500, reason="Server Error"),
        ),
    ],
)
def test_terminal_job_parent_truth_survives_child_list_diagnostics(
    monkeypatch, parent_status, child_result
):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    batch_api.read_namespaced_job.return_value = _make_job(parent_status)
    if isinstance(child_result, Exception):
        core_api.list_namespaced_pod.side_effect = child_result
    else:
        core_api.list_namespaced_pod.return_value = k8s_client_lib.V1PodList(
            items=child_result
        )

    refreshed = _make_kubernetes_resource(
        "Job", interfaces.ContainerStatus.RUNNING
    ).get_refreshed()

    assert refreshed.status == parent_status
    assert refreshed.exit_code == (
        0 if parent_status == interfaces.ContainerStatus.SUCCEEDED else None
    )
    assert refreshed.launcher_error_message is None


def test_terminal_job_retries_child_list_diagnostics(monkeypatch):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    batch_api.read_namespaced_job.return_value = _make_job(
        interfaces.ContainerStatus.SUCCEEDED
    )
    core_api.list_namespaced_pod.side_effect = [
        k8s_exceptions.ApiException(status=500, reason="Server Error"),
        k8s_client_lib.V1PodList(items=[]),
    ]

    refreshed = _make_kubernetes_resource(
        "Job", interfaces.ContainerStatus.RUNNING
    ).get_refreshed()

    assert refreshed.status == interfaces.ContainerStatus.SUCCEEDED
    assert core_api.list_namespaced_pod.call_count == 2


def test_nonterminal_job_child_list_failure_propagates(monkeypatch):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    batch_api.read_namespaced_job.return_value = _make_job(
        interfaces.ContainerStatus.RUNNING
    )
    api_error = k8s_exceptions.ApiException(status=404, reason="Not Found")
    core_api.list_namespaced_pod.side_effect = api_error

    with pytest.raises(k8s_exceptions.ApiException) as exc_info:
        _make_kubernetes_resource(
            "Job", interfaces.ContainerStatus.RUNNING
        ).get_refreshed()

    assert exc_info.value is api_error


class _MemoryStorageProvider:
    def __init__(self):
        self.uploads: dict[str, list[str]] = {}

    def make_uri(self, uri):
        uploads = self.uploads

        class _Writer:
            def upload_from_text(self, text):
                uploads.setdefault(uri, []).append(text)

        return SimpleNamespace(get_writer=lambda: _Writer())


def _log_response(text: str):
    return SimpleNamespace(data=text.encode("utf-8"), release_conn=mock.MagicMock())


def test_terminal_job_retries_child_log_diagnostics(monkeypatch):
    storage = _MemoryStorageProvider()
    resource = _make_kubernetes_resource(
        "Job", interfaces.ContainerStatus.SUCCEEDED, storage_provider=storage
    )
    resource._debug_pods = {
        "0": _make_pod(interfaces.ContainerStatus.SUCCEEDED, name="pod-0")
    }
    core_api, _ = _install_kubernetes_api_mocks(monkeypatch)
    core_api.read_namespaced_pod_log.side_effect = [
        k8s_exceptions.ApiException(status=500, reason="Server Error"),
        _log_response("2026-01-01T00:00:00Z completed\n"),
    ]

    resource.upload_log()

    assert core_api.read_namespaced_pod_log.call_count == 2
    assert storage.uploads[_LOG_URI] == ["2026-01-01T00:00:00Z completed\n"]


def test_terminal_job_does_not_suppress_invalid_cached_child_metadata(monkeypatch):
    resource = _make_kubernetes_resource("Job", interfaces.ContainerStatus.SUCCEEDED)
    malformed_pod = _make_pod(interfaces.ContainerStatus.SUCCEEDED)
    malformed_pod.metadata = None
    resource._debug_pods = {"0": malformed_pod}
    core_api, _ = _install_kubernetes_api_mocks(monkeypatch)

    with pytest.raises(ValueError, match="does not have a name"):
        resource.upload_log()

    core_api.read_namespaced_pod_log.assert_not_called()


def test_terminal_job_uploads_available_child_logs_when_another_log_fails(
    monkeypatch,
):
    storage = _MemoryStorageProvider()
    resource = _make_kubernetes_resource(
        "Job", interfaces.ContainerStatus.FAILED, storage_provider=storage
    )
    resource._debug_pods = {
        "0": _make_pod(interfaces.ContainerStatus.FAILED, name="pod-0"),
        "1": _make_pod(interfaces.ContainerStatus.SUCCEEDED, name="pod-1"),
    }
    core_api, _ = _install_kubernetes_api_mocks(monkeypatch)

    def read_log(*, name, **kwargs):
        if name == "pod-0":
            raise k8s_exceptions.ApiException(status=500, reason="Server Error")
        return _log_response("2026-01-01T00:00:00Z completed\n")

    core_api.read_namespaced_pod_log.side_effect = read_log

    resource.upload_log()

    assert storage.uploads[_LOG_URI] == ["2026-01-01T00:00:00Z completed\n"]
    assert storage.uploads[f"{_LOG_URI}.1"] == ["2026-01-01T00:00:00Z completed\n"]


@pytest.mark.parametrize("with_child", [False, True])
def test_job_upload_does_not_overwrite_log_when_no_child_log_is_read(
    monkeypatch, with_child
):
    storage = _MemoryStorageProvider()
    resource = _make_kubernetes_resource(
        "Job", interfaces.ContainerStatus.FAILED, storage_provider=storage
    )
    if with_child:
        resource._debug_pods = {
            "0": _make_pod(interfaces.ContainerStatus.FAILED, name="pod-0")
        }
        core_api, _ = _install_kubernetes_api_mocks(monkeypatch)
        core_api.read_namespaced_pod_log.side_effect = k8s_exceptions.ApiException(
            status=404, reason="Not Found"
        )

    resource.upload_log()

    assert storage.uploads == {}


@pytest.mark.parametrize("kind", ["Pod", "Job"])
def test_delete_404_is_idempotent_but_other_errors_propagate(monkeypatch, kind):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    delete = (
        core_api.delete_namespaced_pod
        if kind == "Pod"
        else batch_api.delete_namespaced_job
    )
    resource = _make_kubernetes_resource(kind, interfaces.ContainerStatus.RUNNING)

    delete.side_effect = k8s_exceptions.ApiException(status=404, reason="Not Found")
    resource.terminate()

    delete.side_effect = k8s_exceptions.ApiException(status=403, reason="Forbidden")
    with pytest.raises(k8s_exceptions.ApiException):
        resource.terminate()


class _FakeLaunchedContainer:
    def __init__(
        self,
        status: interfaces.ContainerStatus,
        *,
        upload_error: Exception | None = None,
    ):
        self.status = status
        self.exit_code = 0 if status == interfaces.ContainerStatus.SUCCEEDED else None
        self.started_at = _NOW
        self.ended_at = (
            _NOW
            if status
            in {
                interfaces.ContainerStatus.SUCCEEDED,
                interfaces.ContainerStatus.FAILED,
            }
            else None
        )
        self.launcher_error_message = None
        self.upload_error = upload_error
        self.upload_calls = 0
        self.terminate_calls = 0

    def to_dict(self):
        return {"fake": {"status": self.status.value}}

    def upload_log(self):
        self.upload_calls += 1
        if self.upload_error:
            raise self.upload_error

    def terminate(self):
        self.terminate_calls += 1


class _FakeLauncher:
    def __init__(self, cached, *, refreshed=None, refresh_error=None):
        self.cached = cached
        self.refreshed = refreshed or cached
        self.refresh_error = refresh_error
        self.refresh_calls = 0

    def deserialize_launched_container_from_dict(self, launcher_data):
        return self.cached

    def get_refreshed_launched_container_from_dict(self, launcher_data):
        self.refresh_calls += 1
        if self.refresh_error:
            raise self.refresh_error
        return self.refreshed


class _ResourceLauncher(_FakeLauncher):
    def get_refreshed_launched_container_from_dict(self, launcher_data):
        self.refresh_calls += 1
        return self.cached.get_refreshed()


def _new_session():
    engine = database_ops.create_db_engine_and_migrate_db(
        database_uri="sqlite://", do_skip_backfill=True
    )
    return orm.Session(engine)


def _persist_running_execution(
    session,
    snapshot,
    *,
    db_status=bts.ContainerExecutionStatus.RUNNING,
    desired_states=(None,),
    output_artifact_data_map=None,
):
    container_execution = bts.ContainerExecution(
        status=db_status,
        last_processed_at=_NOW,
        created_at=_NOW,
        launcher_data=snapshot.to_dict(),
        input_artifact_data_map={},
        output_artifact_data_map=output_artifact_data_map or {},
        log_uri=_LOG_URI,
    )
    execution_nodes = []
    for index, desired_state in enumerate(desired_states):
        extra_data = (
            {"desired_state": desired_state} if desired_state is not None else None
        )
        execution_nodes.append(
            bts.ExecutionNode(
                task_spec={},
                container_execution_status=db_status,
                container_execution_cache_key=f"cache-{index}",
                container_execution=container_execution,
                extra_data=extra_data,
            )
        )
    session.add_all(execution_nodes)
    session.commit()
    return container_execution.id, [node.id for node in execution_nodes]


def _make_orchestrator(session, launcher, *, storage_provider=None):
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=lambda: session,
        launcher=launcher,
        storage_provider=storage_provider or mock.MagicMock(),
        data_root_uri="memory://artifacts",
        logs_root_uri="memory://logs",
    )


def _process_running(session, launcher, *, storage_provider=None):
    _make_orchestrator(
        session, launcher, storage_provider=storage_provider
    ).internal_process_running_executions_queue(session)


@pytest.mark.parametrize(
    "db_status",
    [
        bts.ContainerExecutionStatus.PENDING,
        bts.ContainerExecutionStatus.RUNNING,
    ],
)
def test_cached_terminal_snapshot_resumes_without_refresh(db_status):
    session = _new_session()
    cached = _FakeLaunchedContainer(interfaces.ContainerStatus.SUCCEEDED)
    launcher = _FakeLauncher(cached, refresh_error=AssertionError("must not refresh"))
    execution_id, node_ids = _persist_running_execution(
        session, cached, db_status=db_status
    )

    _process_running(session, launcher)

    assert session.get(bts.ContainerExecution, execution_id).status == (
        bts.ContainerExecutionStatus.SUCCEEDED
    )
    assert session.get(bts.ExecutionNode, node_ids[0]).container_execution_status == (
        bts.ContainerExecutionStatus.SUCCEEDED
    )
    assert launcher.refresh_calls == 0


@pytest.mark.parametrize(
    ("kind", "terminal_status", "expected_status"),
    [
        (
            "Pod",
            interfaces.ContainerStatus.SUCCEEDED,
            bts.ContainerExecutionStatus.SUCCEEDED,
        ),
        (
            "Job",
            interfaces.ContainerStatus.FAILED,
            bts.ContainerExecutionStatus.FAILED,
        ),
    ],
)
def test_cached_terminal_snapshot_wins_over_later_cancellation(
    monkeypatch, kind, terminal_status, expected_status
):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    resource = _make_kubernetes_resource(kind, terminal_status)
    resource.upload_log = mock.MagicMock()
    launcher = _ResourceLauncher(resource)
    session = _new_session()
    execution_id, node_ids = _persist_running_execution(
        session, resource, desired_states=("TERMINATED",)
    )

    _process_running(session, launcher)

    assert session.get(bts.ContainerExecution, execution_id).status == expected_status
    assert session.get(bts.ExecutionNode, node_ids[0]).container_execution_status == (
        expected_status
    )
    assert launcher.refresh_calls == 0
    core_api.delete_namespaced_pod.assert_not_called()
    batch_api.delete_namespaced_job.assert_not_called()


@pytest.mark.parametrize("kind", ["Pod", "Job"])
def test_nonterminal_cancellation_precedes_refresh_and_delete_404_is_idempotent(
    monkeypatch, kind
):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    delete = (
        core_api.delete_namespaced_pod
        if kind == "Pod"
        else batch_api.delete_namespaced_job
    )
    delete.side_effect = k8s_exceptions.ApiException(status=404, reason="Not Found")
    resource = _make_kubernetes_resource(kind, interfaces.ContainerStatus.RUNNING)
    resource.upload_log = mock.MagicMock()
    launcher = _ResourceLauncher(resource)
    session = _new_session()
    execution_id, node_ids = _persist_running_execution(
        session, resource, desired_states=("TERMINATED",)
    )

    _process_running(session, launcher)

    assert session.get(bts.ContainerExecution, execution_id).status == (
        bts.ContainerExecutionStatus.CANCELLED
    )
    assert session.get(bts.ExecutionNode, node_ids[0]).container_execution_status == (
        bts.ContainerExecutionStatus.CANCELLED
    )
    assert launcher.refresh_calls == 0
    delete.assert_called_once()


@pytest.mark.parametrize("kind", ["Pod", "Job"])
def test_nonterminal_authoritative_404_becomes_system_error(monkeypatch, kind):
    core_api, batch_api = _install_kubernetes_api_mocks(monkeypatch)
    api_error = k8s_exceptions.ApiException(status=404, reason="Not Found")
    api_error.body = "sensitive response body"
    if kind == "Pod":
        core_api.read_namespaced_pod.side_effect = api_error
    else:
        batch_api.read_namespaced_job.side_effect = api_error
    resource = _make_kubernetes_resource(kind, interfaces.ContainerStatus.RUNNING)
    launcher = _ResourceLauncher(resource)
    session = _new_session()
    execution_id, node_ids = _persist_running_execution(session, resource)

    _process_running(session, launcher)

    assert session.get(bts.ContainerExecution, execution_id).status == (
        bts.ContainerExecutionStatus.SYSTEM_ERROR
    )
    node = session.get(bts.ExecutionNode, node_ids[0])
    assert node.container_execution_status == bts.ContainerExecutionStatus.SYSTEM_ERROR
    assert (
        "LauncherResourceNotFound"
        in node.extra_data[
            bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY
        ]
    )
    assert (
        "sensitive response body"
        not in node.extra_data[
            bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_FULL_KEY
        ]
    )


def test_db_status_drives_pending_to_running_transition():
    session = _new_session()
    cached = _FakeLaunchedContainer(interfaces.ContainerStatus.RUNNING)
    refreshed = _FakeLaunchedContainer(interfaces.ContainerStatus.RUNNING)
    launcher = _FakeLauncher(cached, refreshed=refreshed)
    execution_id, node_ids = _persist_running_execution(
        session, cached, db_status=bts.ContainerExecutionStatus.PENDING
    )

    _process_running(session, launcher)

    assert session.get(bts.ContainerExecution, execution_id).status == (
        bts.ContainerExecutionStatus.RUNNING
    )
    assert session.get(bts.ExecutionNode, node_ids[0]).container_execution_status == (
        bts.ContainerExecutionStatus.RUNNING
    )


@pytest.mark.parametrize("refresh_fails", [False, True])
def test_shared_cancellation_is_not_overwritten_by_refresh(refresh_fails):
    session = _new_session()
    cached = _FakeLaunchedContainer(interfaces.ContainerStatus.RUNNING)
    refreshed = _FakeLaunchedContainer(interfaces.ContainerStatus.SUCCEEDED)
    refresh_error = None
    if refresh_fails:
        refresh_error = interfaces.LauncherResourceNotFound(
            kind="Job",
            namespace=_NAMESPACE,
            name=_RESOURCE_NAME,
            cached_status=interfaces.ContainerStatus.RUNNING,
        )
    launcher = _FakeLauncher(cached, refreshed=refreshed, refresh_error=refresh_error)
    execution_id, node_ids = _persist_running_execution(
        session, cached, desired_states=("TERMINATED", None)
    )

    _process_running(session, launcher)

    assert session.get(bts.ExecutionNode, node_ids[0]).container_execution_status == (
        bts.ContainerExecutionStatus.CANCELLED
    )
    expected_active_status = (
        bts.ContainerExecutionStatus.SYSTEM_ERROR
        if refresh_fails
        else bts.ContainerExecutionStatus.SUCCEEDED
    )
    assert session.get(bts.ExecutionNode, node_ids[1]).container_execution_status == (
        expected_active_status
    )
    assert session.get(bts.ContainerExecution, execution_id).status == (
        expected_active_status
    )


def test_failed_log_upload_remains_failed(monkeypatch):
    monkeypatch.setattr(
        orchestrator_sql, "_retry", lambda function, **kwargs: function()
    )
    session = _new_session()
    cached = _FakeLaunchedContainer(
        interfaces.ContainerStatus.FAILED,
        upload_error=RuntimeError("log storage unavailable"),
    )
    launcher = _FakeLauncher(cached)
    execution_id, node_ids = _persist_running_execution(session, cached)

    _process_running(session, launcher)

    assert session.get(bts.ContainerExecution, execution_id).status == (
        bts.ContainerExecutionStatus.FAILED
    )
    assert session.get(bts.ExecutionNode, node_ids[0]).container_execution_status == (
        bts.ContainerExecutionStatus.FAILED
    )


def test_complete_job_with_missing_outputs_still_fails(monkeypatch):
    monkeypatch.setattr(
        orchestrator_sql, "_retry", lambda function, **kwargs: function()
    )
    session = _new_session()
    cached = _FakeLaunchedContainer(interfaces.ContainerStatus.SUCCEEDED)
    launcher = _FakeLauncher(cached)
    storage_provider = mock.MagicMock()
    storage_provider.make_uri.return_value.get_reader.return_value.exists.return_value = (
        False
    )
    execution_id, node_ids = _persist_running_execution(
        session,
        cached,
        output_artifact_data_map={"model": {"uri": "memory://missing-model"}},
    )

    _process_running(session, launcher, storage_provider=storage_provider)

    assert session.get(bts.ContainerExecution, execution_id).status == (
        bts.ContainerExecutionStatus.FAILED
    )
    node = session.get(bts.ExecutionNode, node_ids[0])
    assert node.container_execution_status == bts.ContainerExecutionStatus.FAILED
    assert (
        "missing outputs"
        in node.extra_data[
            bts.EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
        ]
    )
    assert launcher.refresh_calls == 0
