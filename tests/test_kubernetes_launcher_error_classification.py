"""Tests for translating Kubernetes API errors into launcher errors."""

from unittest import mock

import kubernetes.client.exceptions
import pytest

from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers


def _api_exception(status: int) -> kubernetes.client.exceptions.ApiException:
    return kubernetes.client.exceptions.ApiException(status=status, reason="test")


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

    A Pod deleted mid-run (e.g. by the cluster-autoscaler or garbage collection)
    is retained in ``_debug_pods``, so every later read 404s. That must report
    the Pod as deleted rather than propagate -- otherwise it fails the whole
    execution. Genuine client and server errors must still propagate.
    """

    def test_not_found_returns_pod_deleted_sentinel(self) -> None:
        job, core_api = _make_job_with_pod_log_error(_api_exception(404))
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            assert job._get_log_by_pod_key("pod-0") is kubernetes_launchers._POD_DELETED

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


def _pod(name: str) -> mock.MagicMock:
    pod = mock.MagicMock()
    pod.metadata.name = name
    return pod


def _log_response(text: str) -> mock.MagicMock:
    """A ``read_namespaced_pod_log`` response as used with ``_preload_content=False``."""
    response = mock.MagicMock()
    response.data = text.encode("utf-8")
    return response


def _make_job_reading_pods(
    debug_pods: dict[str, mock.MagicMock],
) -> tuple[kubernetes_launchers.LaunchedKubernetesJob, dict[str, str], mock.MagicMock]:
    """A Job over ``debug_pods`` plus a dict capturing everything ``upload_log`` writes."""
    uploads: dict[str, str] = {}

    def _make_uri(uri: str) -> mock.MagicMock:
        writer = mock.MagicMock()
        writer.upload_from_text.side_effect = lambda text, u=uri: uploads.__setitem__(
            u, text
        )
        accessor = mock.MagicMock()
        accessor.get_writer.return_value = writer
        return accessor

    launcher = mock.MagicMock(_request_timeout=10)
    launcher._storage_provider.make_uri.side_effect = _make_uri
    job = kubernetes_launchers.LaunchedKubernetesJob(
        job_name="job",
        namespace="ns",
        output_uris={},
        log_uri="file:///tmp/log",
        debug_job=mock.MagicMock(),
        debug_pods=debug_pods,
        launcher=launcher,
    )
    return job, uploads, launcher


class TestVanishedPodLogPlaceholder:
    """When a Pod is deleted before its logs are captured, the persisted log is a
    human-readable notice rather than a blank -- but only when that is the *sole*
    reason there are no logs. A Pod that exists and printed nothing stays empty,
    and any recovered logs are persisted verbatim.
    """

    def test_deleted_pod_persists_notice(self) -> None:
        job, uploads, _ = _make_job_reading_pods({"0": _pod("pod-0")})
        core_api = mock.MagicMock()
        core_api.read_namespaced_pod_log.side_effect = _api_exception(404)
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            job.upload_log()
            assert job.get_log() == (
                kubernetes_launchers._LOGS_UNAVAILABLE_POD_DELETED_MESSAGE
            )
        assert (
            uploads["file:///tmp/log"]
            == kubernetes_launchers._LOGS_UNAVAILABLE_POD_DELETED_MESSAGE
        )
        # The notice is not written as a per-pod log.
        assert list(uploads) == ["file:///tmp/log"]

    def test_existing_pod_with_empty_output_stays_empty(self) -> None:
        job, uploads, _ = _make_job_reading_pods({"0": _pod("pod-0")})
        core_api = mock.MagicMock()
        core_api.read_namespaced_pod_log.return_value = _log_response("")
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            job.upload_log()
            assert job.get_log() == ""
        assert uploads["file:///tmp/log"] == ""

    def test_recovered_logs_are_not_replaced(self) -> None:
        job, uploads, _ = _make_job_reading_pods(
            {"0": _pod("pod-0"), "1": _pod("pod-1")}
        )
        core_api = mock.MagicMock()

        def _read(name: str, **kwargs: object) -> mock.MagicMock:
            if name == "pod-1":  # This Pod is gone...
                raise _api_exception(404)
            return _log_response("2026-01-01T00:00:00Z hello\n")  # ...but pod-0 logged.

        core_api.read_namespaced_pod_log.side_effect = _read
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            job.upload_log()
        # Partial logs win over the notice: we show what we have.
        assert "hello" in uploads["file:///tmp/log"]
        assert (
            kubernetes_launchers._LOGS_UNAVAILABLE_POD_DELETED_MESSAGE
            not in uploads["file:///tmp/log"]
        )
        assert uploads["file:///tmp/log.0"] == "2026-01-01T00:00:00Z hello\n"
