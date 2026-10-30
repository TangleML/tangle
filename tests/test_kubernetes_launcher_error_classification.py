"""Tests for translating Kubernetes API errors into launcher errors."""

from unittest import mock

import kubernetes.client.exceptions
import pytest

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
    also assign the body without decoding, which is why `_api_exception_body` has to
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
