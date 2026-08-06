"""Tests for translating Kubernetes API errors into launcher errors."""

import kubernetes.client.exceptions

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
