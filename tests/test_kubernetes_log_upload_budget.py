"""Tests for the Kubernetes launchers' log-upload budget.

The orchestrator calls ``upload_log`` inside its own retry, so these tests pin
the two halves of the contract that keeps a dead node from holding its loop: the
log *read* happens at most once, and giving up does not raise.
"""

from unittest import mock

import kubernetes.client.exceptions
import pytest

from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import kubernetes_launchers

_LAUNCHED_CONTAINER_CLASSES = [
    kubernetes_launchers.LaunchedKubernetesContainer,
    kubernetes_launchers.LaunchedKubernetesJob,
]
_IDS = ["pod", "job"]


class TestBudget:
    """Both launchers call the helper with no arguments, so its defaults are the budget."""

    def test_the_defaults_are_one_attempt_within_ten_seconds(self) -> None:
        assert kubernetes_launchers._LOG_READ_MAX_RETRIES == 1
        assert kubernetes_launchers._LOG_READ_WAIT_SECONDS == 1.0
        assert kubernetes_launchers._LOG_READ_MAX_TOTAL_SECONDS == 10.0

    def test_calling_with_no_arguments_uses_them(self) -> None:
        read_log = mock.MagicMock(side_effect=RuntimeError("kubelet unreachable"))
        with mock.patch("time.sleep") as sleep:
            with pytest.raises(RuntimeError):
                kubernetes_launchers._read_log_within_budget(read_log)
        assert read_log.call_count == 1
        assert sleep.call_count == 0


class TestReadLogWithinBudget:
    """The helper itself, independent of either launcher."""

    def test_returns_the_log(self) -> None:
        assert (
            kubernetes_launchers._read_log_within_budget(
                lambda: "the log",
                max_retries=1,
                wait_seconds=1.0,
                max_total_seconds=10.0,
            )
            == "the log"
        )

    def test_a_single_attempt_does_not_sleep(self) -> None:
        read_log = mock.MagicMock(side_effect=RuntimeError("kubelet unreachable"))
        with mock.patch("time.sleep") as sleep:
            with pytest.raises(RuntimeError):
                kubernetes_launchers._read_log_within_budget(
                    read_log, max_retries=1, wait_seconds=1.0, max_total_seconds=10.0
                )
        assert read_log.call_count == 1
        assert sleep.call_count == 0

    def test_retries_without_a_trailing_sleep(self) -> None:
        """The sleep after the last failure only delays the caller."""
        read_log = mock.MagicMock(side_effect=RuntimeError("kubelet unreachable"))
        with mock.patch("time.sleep") as sleep:
            with pytest.raises(RuntimeError):
                kubernetes_launchers._read_log_within_budget(
                    read_log, max_retries=3, wait_seconds=1.0, max_total_seconds=None
                )
        assert read_log.call_count == 3
        assert sleep.call_count == 2

    def test_the_deadline_stops_retrying_early(self) -> None:
        read_log = mock.MagicMock(side_effect=RuntimeError("kubelet unreachable"))
        # Each attempt costs 1s of wall time, on top of the 1s wait.
        clock = iter([0.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0])
        with mock.patch("time.monotonic", lambda: next(clock)):
            with mock.patch("time.sleep") as sleep:
                with pytest.raises(RuntimeError):
                    kubernetes_launchers._read_log_within_budget(
                        read_log,
                        max_retries=99,
                        wait_seconds=1.0,
                        max_total_seconds=3.0,
                    )
        assert read_log.call_count == 2
        assert sleep.call_count == 1


def _reader_of(container) -> mock.MagicMock:
    """The one call that reaches the kubelet, for whichever class this is."""
    if isinstance(container, kubernetes_launchers.LaunchedKubernetesContainer):
        return container.get_log
    return container._get_all_logs


def _container_that_cannot_read_its_log(launched_container_class: type):
    """A real launched container, uninitialised, whose log read always fails.

    Built from the real class rather than a `spec` mock, so the budget read by
    `upload_log` is the class's own and a renamed attribute fails here.
    """
    container = object.__new__(launched_container_class)
    container._pod_name = "test-pod"
    container._job_name = "test-job"
    container._log_uri = "memory://log"
    container._get_launcher = mock.MagicMock()
    failure = RuntimeError("kubelet unreachable")
    if launched_container_class is kubernetes_launchers.LaunchedKubernetesContainer:
        container.get_log = mock.MagicMock(side_effect=failure)
    else:
        container._get_all_logs = mock.MagicMock(side_effect=failure)
    return container


class TestUploadLogGivesUp:
    @pytest.mark.parametrize(
        "launched_container_class", _LAUNCHED_CONTAINER_CLASSES, ids=_IDS
    )
    def test_a_lost_log_is_read_once_and_does_not_raise(
        self, launched_container_class: type
    ) -> None:
        """Raising here would cost the caller its own five attempts."""
        container = _container_that_cannot_read_its_log(launched_container_class)
        with mock.patch("time.sleep") as sleep:
            container.upload_log()
        assert _reader_of(container).call_count == 1
        assert sleep.call_count == 0

    @pytest.mark.parametrize(
        "launched_container_class", _LAUNCHED_CONTAINER_CLASSES, ids=_IDS
    )
    def test_nothing_is_written_when_the_log_is_lost(
        self, launched_container_class: type
    ) -> None:
        """An empty upload would overwrite a log an earlier attempt stored."""
        container = _container_that_cannot_read_its_log(launched_container_class)
        with mock.patch("time.sleep"):
            container.upload_log()
        container._get_launcher.assert_not_called()


class TestStorageWriteStillRaises:
    """A failed write to storage is worth the caller's retries; a dead node is not."""

    def test_pod_upload_propagates_a_storage_failure(self) -> None:
        container = object.__new__(kubernetes_launchers.LaunchedKubernetesContainer)
        container._pod_name = "test-pod"
        container._log_uri = "memory://log"
        container.get_log = mock.MagicMock(return_value="the log")
        launcher = mock.MagicMock()
        container._get_launcher = mock.MagicMock(return_value=launcher)
        writer = (
            launcher._storage_provider.make_uri.return_value.get_writer.return_value
        )
        writer.upload_from_text.side_effect = RuntimeError("storage unavailable")
        with pytest.raises(RuntimeError, match="storage unavailable"):
            container.upload_log()


# The body a real unreachable kubelet returns, with the cluster's own identifiers
# removed. Observed in production on the container-log endpoint.
_KUBELET_UNREACHABLE_BODY = (
    '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":'
    '"Get \\"https://<node-ip>:10250/containerLogs/<namespace>/<pod>/main'
    '?timestamps=true\\": dial tcp <node-ip>:10250: i/o timeout","code":500}'
)


class _FakeResponse:
    """Enough of an HTTP response for `ApiException`, across client versions.

    Older clients read the headers with `getheaders()`, newer ones with
    `getheader()`, and the body is stored undecoded.
    """

    def __init__(self, *, status: int, reason: str, data: bytes) -> None:
        self.status = status
        self.reason = reason
        self.data = data
        self.headers: dict[str, str] = {}

    def getheaders(self) -> dict[str, str]:
        return self.headers

    def getheader(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name, default)


def _kubelet_unreachable() -> kubernetes.client.exceptions.ApiException:
    """The exception a dead node actually produces, not a stand-in."""
    return kubernetes.client.exceptions.ApiException(
        http_resp=_FakeResponse(
            status=500,
            reason="Internal Server Error",
            data=_KUBELET_UNREACHABLE_BODY.encode(),
        )
    )


class TestARealKubeletFailureIsSwallowed:
    """Closes the gap between "the loop works" and "it catches what is thrown".

    These build the launched containers through their real constructors and let
    the real read path run, so the exception reaching the budget is one the
    Kubernetes client raised, not a `RuntimeError` a test invented.
    """

    def test_pod_log_read(self) -> None:
        container = kubernetes_launchers.LaunchedKubernetesContainer(
            pod_name="test-pod",
            namespace="test-ns",
            output_uris={},
            log_uri="file:///dev/null",
            debug_pod=mock.MagicMock(),
            launcher=mock.MagicMock(_request_timeout=10),
        )
        core_api = mock.MagicMock()
        core_api.read_namespaced_pod_log.side_effect = _kubelet_unreachable()
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
        ):
            with mock.patch("time.sleep") as sleep:
                container.upload_log()
        assert core_api.read_namespaced_pod_log.call_count == 1
        assert sleep.call_count == 0

    def test_job_log_read(self) -> None:
        job = kubernetes_launchers.LaunchedKubernetesJob(
            job_name="test-job",
            namespace="test-ns",
            output_uris={},
            log_uri="file:///dev/null",
            debug_job=mock.MagicMock(),
            launcher=mock.MagicMock(_request_timeout=10),
        )
        pod = mock.MagicMock()
        pod.metadata.name = "test-pod"
        core_api = mock.MagicMock()
        core_api.read_namespaced_pod_log.side_effect = _kubelet_unreachable()
        # Only the Pod listing is faked; the read below it is the real one.
        with mock.patch.object(job, "_get_all_attempt_pods", return_value={"0": [pod]}):
            with mock.patch.object(
                kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=core_api
            ):
                with mock.patch("time.sleep") as sleep:
                    job.upload_log()
        assert core_api.read_namespaced_pod_log.call_count == 1
        assert sleep.call_count == 0


class TestCostToTheCaller:
    """What the orchestrator's own retry does with the result, using the real `_retry`."""

    def test_a_lost_log_costs_one_request_instead_of_five(self) -> None:
        container = _container_that_cannot_read_its_log(
            kubernetes_launchers.LaunchedKubernetesJob
        )
        with mock.patch("time.sleep") as sleep:
            orchestrator_sql._retry(container.upload_log)
        assert container._get_all_logs.call_count == 1
        assert sleep.call_count == 0

    def test_the_same_failure_raised_would_have_cost_five(self) -> None:
        """Why the give-up returns: this is the behaviour it replaces."""
        raising_upload = mock.MagicMock(side_effect=RuntimeError("kubelet unreachable"))
        with mock.patch("time.sleep") as sleep:
            with pytest.raises(RuntimeError):
                orchestrator_sql._retry(raising_upload)
        assert raising_upload.call_count == 5
        assert sleep.call_count == 5
