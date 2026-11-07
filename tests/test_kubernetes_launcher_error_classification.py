"""Tests for translating Kubernetes API errors into launcher errors."""

import json
import logging
from types import SimpleNamespace
from unittest import mock

import kubernetes.client.exceptions
import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers


def _api_exception(status: int) -> kubernetes.client.exceptions.ApiException:
    return kubernetes.client.exceptions.ApiException(status=status, reason="test")


# Verbatim shape of a production 500, with cluster identifiers replaced. The client
# decodes `http_resp.data` itself, so handing it bytes is what exercises the real path.
_KUBELET_DIAL_TIMEOUT_BODY = (
    '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
    '"message":"Get \\"https://10.0.0.1:10250/containerLogs/jobs-namespace/'
    'job-pod-0/main?timestamps=true\\": dial tcp 10.0.0.1:10250: i/o timeout",'
    '"code":500}'
)


class _FakeResponse:
    """Enough of a urllib3 response for the Kubernetes client's exception classes.

    Both header shapes are provided because the client changed how it reads them:
    older versions call `getheaders()`, newer ones read `.headers`. Older versions
    also assign the body without decoding, which is why `_extract_api_exception_body` has to
    cope with `bytes` as well as `str`.
    """

    status = 500
    reason = "Internal Server Error"
    headers: dict[str, str] = {}

    def __init__(self, data: bytes) -> None:
        self.data = data

    def getheaders(self) -> dict[str, str]:
        return self.headers


def _service_exception(body: str) -> kubernetes.client.exceptions.ServiceException:
    """The exception the Kubernetes client actually raises for a 500, built as it builds it."""
    return kubernetes.client.exceptions.ServiceException(
        http_resp=_FakeResponse(body.encode())
    )


class TestLauncherErrorFromApiException:
    def test_server_error_is_retriable(self) -> None:
        error = kubernetes_launchers._launcher_error_from_api_exception(
            _api_exception(500), message="Failed to refresh pod status"
        )
        assert isinstance(error, interfaces.LauncherError)
        assert error.is_retriable

    def test_service_unavailable_is_retriable(self) -> None:
        error = kubernetes_launchers._launcher_error_from_api_exception(
            _api_exception(503), message="Failed to refresh pod status"
        )
        assert error.is_retriable

    def test_not_found_is_a_missing_workload_error(self) -> None:
        error = kubernetes_launchers._launcher_error_from_api_exception(
            _api_exception(404), message="Failed to refresh pod status"
        )
        assert isinstance(error, interfaces.LaunchedContainerNotFoundError)
        assert not error.is_retriable

    def test_client_error_is_not_retriable(self) -> None:
        error = kubernetes_launchers._launcher_error_from_api_exception(
            _api_exception(403), message="Failed to refresh pod status"
        )
        assert not error.is_retriable


def _make_job_with_pod_log_error(
    api_exception: kubernetes.client.exceptions.ApiException,
) -> tuple[kubernetes_launchers.LaunchedKubernetesJob, mock.MagicMock]:
    """A ``LaunchedKubernetesJob`` whose pod-log reads raise ``api_exception``."""
    job = kubernetes_launchers.LaunchedKubernetesJob(
        job_name="job",
        namespace="ns",
        output_uris={},
        log_uri="file:///tmp/log",
        debug_job=mock.MagicMock(),
        launcher=mock.MagicMock(_request_timeout=10),
    )
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod_log.side_effect = api_exception
    return job, core_api


class TestGetLogByPodKey:
    """``_get_log_by_pod_key`` maps a vanished Pod (404) to "no logs", not an error.

    A Pod deleted mid-run (e.g. by the cluster-autoscaler) is retained in
    ``_debug_pods``, so every later read 404s. That must return ``None`` rather
    than propagate -- otherwise it fails the whole execution. Genuine client and
    server errors must still propagate.
    """

    def test_not_found_returns_none(self) -> None:
        job, core_api = _make_job_with_pod_log_error(_api_exception(404))
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            assert job._get_log_by_pod_key("pod-0") is None

    def test_forbidden_reraises(self) -> None:
        job, core_api = _make_job_with_pod_log_error(_api_exception(403))
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            with pytest.raises(kubernetes.client.exceptions.ApiException):
                job._get_log_by_pod_key("pod-0")

    def test_server_error_reraises(self) -> None:
        job, core_api = _make_job_with_pod_log_error(_api_exception(500))
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            with pytest.raises(kubernetes.client.exceptions.ApiException):
                job._get_log_by_pod_key("pod-0")

    def test_unreachable_kubelet_returns_none(self) -> None:
        """A node removed by the cluster-autoscaler takes the Pod's logs with it."""
        job, core_api = _make_job_with_pod_log_error(
            _service_exception(_KUBELET_DIAL_TIMEOUT_BODY)
        )
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            assert job._get_log_by_pod_key("pod-0") is None

    def test_differently_worded_kubelet_failure_still_reraises(self) -> None:
        """The API server's wording is not a contract; a non-match must not be swallowed."""
        job, core_api = _make_job_with_pod_log_error(
            _service_exception(
                '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
                '"message":"Get \\"https://10.0.0.1:10250/containerLogs/ns/pod-0/main\\": '
                'remote error: tls: internal error","code":500}'
            )
        )
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            with pytest.raises(kubernetes.client.exceptions.ApiException):
                job._get_log_by_pod_key("pod-0")

    def test_unrelated_server_error_body_still_reraises(self) -> None:
        """The match is narrow: only a failed kubelet dial on a log read."""
        job, core_api = _make_job_with_pod_log_error(
            _service_exception(
                '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
                '"message":"etcdserver: request timed out","code":500}'
            )
        )
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            with pytest.raises(kubernetes.client.exceptions.ApiException):
                job._get_log_by_pod_key("pod-0")


def _api_exception_with_body(
    status: int, body: str
) -> kubernetes.client.exceptions.ApiException:
    """An `ApiException` built from a response, the way the client builds it."""
    response = _FakeResponse(body.encode())
    response.status = status
    return kubernetes.client.exceptions.ApiException(http_resp=response)


def _status_body(*, message: str, code: int) -> str:
    return json.dumps(
        {
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": message,
            "code": code,
        }
    )


# Shaped like the API server's denial message; the policy text is made up.
_KYVERNO_DENIAL_MESSAGE = (
    'admission webhook "validate.kyverno.svc-fail" denied the request: \n\n'
    "resource Job/tangle-jobs/tangle-ce-execution-1 was blocked due to the "
    "following policies \n\nlauncher-migration:\n  deny-legacy-launcher: "
    "This cluster no longer accepts Tangle jobs.\n"
)
_GATEKEEPER_DENIAL_MESSAGE = (
    'admission webhook "validation.gatekeeper.sh" denied the request: '
    "[require-limits] container <main> has no resource limits"
)
_RBAC_FORBIDDEN_MESSAGE = (
    'jobs.batch is forbidden: User "system:serviceaccount:tangle:orchestrator" '
    'cannot create resource "jobs" in API group "batch" in the namespace "other"'
)
_UNREACHABLE_WEBHOOK_MESSAGE = (
    'Internal error occurred: failed calling webhook "validate.kyverno.svc-fail": '
    'failed to call webhook: Post "https://kyverno-svc.kyverno.svc:443/validate/fail'
    '?timeout=10s": context deadline exceeded'
)


def _denial() -> kubernetes.client.exceptions.ApiException:
    return _api_exception_with_body(
        400, _status_body(message=_KYVERNO_DENIAL_MESSAGE, code=400)
    )


class TestAdmissionDenialMessage:
    @pytest.mark.parametrize(
        ("status", "message"),
        [(400, _KYVERNO_DENIAL_MESSAGE), (403, _GATEKEEPER_DENIAL_MESSAGE)],
    )
    def test_webhook_denial_returns_its_message(
        self, status: int, message: str
    ) -> None:
        exception = _api_exception_with_body(
            status, _status_body(message=message, code=status)
        )
        assert kubernetes_launchers._admission_denial_message(exception) == message

    @pytest.mark.parametrize(
        "exception",
        [
            _api_exception_with_body(
                403, _status_body(message=_RBAC_FORBIDDEN_MESSAGE, code=403)
            ),
            _api_exception_with_body(
                500, _status_body(message=_UNREACHABLE_WEBHOOK_MESSAGE, code=500)
            ),
            _api_exception_with_body(
                500, _status_body(message=_KYVERNO_DENIAL_MESSAGE, code=500)
            ),
            _api_exception_with_body(400, _KYVERNO_DENIAL_MESSAGE),
            TimeoutError("timed out"),
        ],
        ids=[
            "rbac-forbidden",
            "unreachable-webhook",
            "denial-text-under-500",
            "body-not-json",
            "no-response",
        ],
    )
    def test_other_errors_are_not_denials(self, exception: Exception) -> None:
        assert kubernetes_launchers._admission_denial_message(exception) is None


def _launcher(launcher_class, monkeypatch, *, create_error: Exception):
    """A launcher, built without `__init__`, whose create requests raise `create_error`."""

    def create(namespace, body, _request_timeout):
        raise create_error

    monkeypatch.setattr(
        kubernetes_launchers.k8s_client_lib,
        "BatchV1Api",
        lambda api_client: SimpleNamespace(create_namespaced_job=create),
    )
    monkeypatch.setattr(
        kubernetes_launchers.k8s_client_lib,
        "CoreV1Api",
        lambda api_client: SimpleNamespace(create_namespaced_pod=create),
    )
    launcher = object.__new__(launcher_class)
    launcher._api_client = None
    launcher._request_timeout = (3, 30)
    launcher._namespace = "tangle-jobs"
    launcher._service_account_name = "tangle"
    launcher._pod_name_prefix = "task-"
    launcher._pod_labels = {}
    launcher._pod_annotations = {}
    launcher._pod_postprocessor = None
    launcher._prepare_kubernetes_pod = lambda **kwargs: k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(namespace=kwargs["pod_namespace"]),
        spec=k8s_client_lib.V1PodSpec(
            containers=[
                k8s_client_lib.V1Container(
                    name="main", command=["run", "--token=secret-value"]
                )
            ],
            node_selector={"pool": "spot"},
        ),
    )
    return launcher


def _launch(launcher) -> None:
    launcher.launch_container_task(
        component_spec=structures.ComponentSpec(
            implementation=structures.ContainerImplementation(
                container=structures.ContainerSpec(image="python:3.12")
            )
        ),
        input_arguments={},
        output_uris={},
        log_uri="memory://log",
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "execution-1"
        },
    )


_LAUNCHER_CLASSES = [
    kubernetes_launchers._KubernetesJobLauncher,
    kubernetes_launchers._KubernetesPodLauncher,
]


@pytest.mark.parametrize("launcher_class", _LAUNCHER_CLASSES)
class TestCreateRequestDenied:
    def test_denial_is_a_launch_rejection(self, monkeypatch, launcher_class) -> None:
        launcher = _launcher(launcher_class, monkeypatch, create_error=_denial())

        with pytest.raises(interfaces.LaunchRejectedError) as exc_info:
            _launch(launcher)

        assert exc_info.value.message == _KYVERNO_DENIAL_MESSAGE
        assert exc_info.value.reason == "admission_denied"
        assert not exc_info.value.is_retriable

    def test_denied_spec_is_logged_without_its_command_line(
        self, monkeypatch, caplog, launcher_class
    ) -> None:
        launcher = _launcher(launcher_class, monkeypatch, create_error=_denial())

        with (
            caplog.at_level(logging.WARNING, logger=kubernetes_launchers.__name__),
            pytest.raises(interfaces.LaunchRejectedError),
        ):
            _launch(launcher)

        [logged] = [
            record.getMessage()
            for record in caplog.records
            if record.getMessage().startswith("Admission denied")
        ]
        assert '"pool": "spot"' in logged
        assert "secret-value" not in logged

    @pytest.mark.parametrize(
        "create_error",
        [
            _api_exception_with_body(
                403, _status_body(message=_RBAC_FORBIDDEN_MESSAGE, code=403)
            ),
            TimeoutError("timed out"),
        ],
        ids=["rbac-forbidden", "timeout"],
    )
    def test_other_failures_stay_launcher_errors(
        self, monkeypatch, launcher_class, create_error: Exception
    ) -> None:
        launcher = _launcher(launcher_class, monkeypatch, create_error=create_error)

        with pytest.raises(interfaces.LauncherError) as exc_info:
            _launch(launcher)

        assert not isinstance(exc_info.value, interfaces.LaunchRejectedError)
