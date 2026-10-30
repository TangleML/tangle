"""Tests for `orchestrator_sql._retry`."""

import typing
from unittest import mock

import pytest

from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers


class TestRetry:
    def test_returns_after_transient_failure(self) -> None:
        func = mock.MagicMock(side_effect=[RuntimeError("transient"), "ok"])
        with mock.patch.object(orchestrator_sql.time, "sleep"):
            assert orchestrator_sql._retry(func) == "ok"
        assert func.call_count == 2

    def test_raises_after_max_retries_without_trailing_sleep(self) -> None:
        func = mock.MagicMock(side_effect=RuntimeError("always"))
        with mock.patch.object(orchestrator_sql.time, "sleep") as sleep_mock:
            with pytest.raises(RuntimeError):
                orchestrator_sql._retry(func, max_retries=3)
        assert func.call_count == 3
        # One sleep between attempts, and none after the last one.
        assert sleep_mock.call_count == 2

    def test_stops_retrying_at_the_deadline(self) -> None:
        func = mock.MagicMock(side_effect=RuntimeError("always"))
        with (
            mock.patch.object(orchestrator_sql.time, "sleep"),
            # Start, then 6s and 12s elapsed after the 1st and 2nd failures. The
            # second attempt's sleep would end past the 10s deadline, so there is
            # no third attempt even though `max_retries` allows five.
            mock.patch.object(
                orchestrator_sql.time, "monotonic", side_effect=[0.0, 6.0, 12.0]
            ),
        ):
            with pytest.raises(RuntimeError):
                orchestrator_sql._retry(func, max_total_seconds=10)
        assert func.call_count == 2

    def test_max_retries_still_caps_attempts_under_a_deadline(self) -> None:
        """The two caps are independent: fast failures exhaust `max_retries` first."""
        func = mock.MagicMock(side_effect=RuntimeError("always"))
        with (
            mock.patch.object(orchestrator_sql.time, "sleep"),
            mock.patch.object(orchestrator_sql.time, "monotonic", return_value=0.0),
        ):
            with pytest.raises(RuntimeError):
                orchestrator_sql._retry(func, max_retries=2, max_total_seconds=3600)
        assert func.call_count == 2


class _FakeLaunchedContainer:
    """Only what `_upload_log_with_retries` reads."""

    def __init__(
        self,
        *,
        max_retries: int | None,
        max_total_seconds: float | None,
        wait_seconds: float | None = None,
    ) -> None:
        self.log_upload_max_retries = max_retries
        self.log_upload_wait_seconds = wait_seconds
        self.log_upload_max_total_seconds = max_total_seconds
        self.upload_log = mock.MagicMock(side_effect=RuntimeError("log is gone"))


class TestUploadLogWithRetries:
    def test_honours_the_launchers_caps(self) -> None:
        container = _FakeLaunchedContainer(max_retries=1, max_total_seconds=10.0)
        with mock.patch.object(orchestrator_sql.time, "sleep") as sleep_mock:
            with pytest.raises(RuntimeError):
                orchestrator_sql._upload_log_with_retries(container)
        # One attempt, so no retry and nothing to sleep between.
        assert container.upload_log.call_count == 1
        assert sleep_mock.call_count == 0

    def test_default_caps_preserve_five_attempts(self) -> None:
        """A launcher that sets no budget keeps the orchestrator's old behavior."""
        container = _FakeLaunchedContainer(max_retries=None, max_total_seconds=None)
        with mock.patch.object(orchestrator_sql.time, "sleep") as sleep_mock:
            with pytest.raises(RuntimeError):
                orchestrator_sql._upload_log_with_retries(container)
        assert container.upload_log.call_count == orchestrator_sql._DEFAULT_MAX_RETRIES
        assert (
            sleep_mock.call_args_list
            == [mock.call(orchestrator_sql._DEFAULT_WAIT_SECONDS)] * 4
        )

    def test_deadline_alone_still_caps_attempts(self) -> None:
        """A budget may set only the time cap; the attempt count falls back."""
        container = _FakeLaunchedContainer(max_retries=None, max_total_seconds=2.5)
        with (
            mock.patch.object(orchestrator_sql.time, "sleep"),
            mock.patch.object(
                orchestrator_sql.time, "monotonic", side_effect=[0.0, 1.0, 2.0]
            ),
        ):
            with pytest.raises(RuntimeError):
                orchestrator_sql._upload_log_with_retries(container)
        # The 2nd failure is at 2.0s; a 1s sleep would end past the 2.5s deadline.
        assert container.upload_log.call_count == 2

    def test_honours_the_launchers_wait_seconds(self) -> None:
        container = _FakeLaunchedContainer(
            max_retries=3, max_total_seconds=None, wait_seconds=0.25
        )
        with mock.patch.object(orchestrator_sql.time, "sleep") as sleep_mock:
            with pytest.raises(RuntimeError):
                orchestrator_sql._upload_log_with_retries(container)
        assert container.upload_log.call_count == 3
        assert sleep_mock.call_args_list == [mock.call(0.25)] * 2

    def test_wait_seconds_counts_against_the_deadline(self) -> None:
        """A shorter wait fits more attempts into the same time budget."""
        container = _FakeLaunchedContainer(
            max_retries=None, max_total_seconds=2.5, wait_seconds=0.1
        )
        with (
            mock.patch.object(orchestrator_sql.time, "sleep"),
            mock.patch.object(
                orchestrator_sql.time, "monotonic", side_effect=[0.0, 1.0, 2.0, 2.45]
            ),
        ):
            with pytest.raises(RuntimeError):
                orchestrator_sql._upload_log_with_retries(container)
        # At 2.0s a 0.1s wait still fits under 2.5s, where the default 1.0s wait
        # would have stopped; the 3rd failure at 2.45s + 0.1s does not.
        assert container.upload_log.call_count == 3


def _real_kubernetes_job() -> kubernetes_launchers.LaunchedKubernetesJob:
    return kubernetes_launchers.LaunchedKubernetesJob(
        job_name="job",
        namespace="ns",
        output_uris={},
        log_uri="file:///tmp/log",
        debug_job=mock.MagicMock(),
        launcher=mock.MagicMock(_request_timeout=10),
    )


def _real_kubernetes_pod() -> kubernetes_launchers.LaunchedKubernetesContainer:
    return kubernetes_launchers.LaunchedKubernetesContainer(
        pod_name="pod-0",
        namespace="ns",
        output_uris={},
        log_uri="file:///tmp/log",
        debug_pod=mock.MagicMock(),
        launcher=mock.MagicMock(_request_timeout=10),
    )


class TestRealLaunchedContainerBudgets:
    """The budgets reach `_retry` through real launched containers, not just a fake.

    Reading the attributes off the class proves they are set; only driving the
    orchestrator helper with a real instance proves they are the ones used.
    """

    @pytest.mark.parametrize(
        "make_launched_container",
        [_real_kubernetes_job, _real_kubernetes_pod],
        ids=["job", "pod"],
    )
    def test_kubernetes_upload_stops_after_one_attempt(
        self,
        make_launched_container: typing.Callable[[], interfaces.LaunchedContainer],
    ) -> None:
        launched_container = make_launched_container()
        upload_log_mock = mock.MagicMock(side_effect=RuntimeError("log is gone"))
        with (
            mock.patch.object(type(launched_container), "upload_log", upload_log_mock),
            mock.patch.object(orchestrator_sql.time, "sleep") as sleep_mock,
        ):
            with pytest.raises(RuntimeError):
                orchestrator_sql._upload_log_with_retries(launched_container)
        assert upload_log_mock.call_count == 1
        assert sleep_mock.call_count == 0


class TestLogUploadBudgets:
    """Both Kubernetes launchers bound how long a log upload can hold the loop.

    The orchestrator reads these off the launched container, so a wrong value here
    is invisible until a node disappears in production.
    """

    @pytest.mark.parametrize(
        "launched_container_class",
        [
            kubernetes_launchers.LaunchedKubernetesJob,
            kubernetes_launchers.LaunchedKubernetesContainer,
        ],
    )
    def test_kubernetes_launchers_share_one_budget(
        self, launched_container_class: type[interfaces.LaunchedContainer]
    ) -> None:
        assert launched_container_class.log_upload_max_retries == 1
        assert launched_container_class.log_upload_max_total_seconds == 10.0

    def test_other_platforms_keep_the_orchestrator_default(self) -> None:
        """Only the launchers that opt in get a budget; the rest are unchanged."""
        assert interfaces.LaunchedContainer.log_upload_max_retries is None
        assert interfaces.LaunchedContainer.log_upload_wait_seconds is None
        assert interfaces.LaunchedContainer.log_upload_max_total_seconds is None

    def test_no_wait_to_configure_with_a_single_attempt(self) -> None:
        """One attempt leaves no gap to wait in, so the wait stays unset."""
        assert (
            kubernetes_launchers.LaunchedKubernetesJob.log_upload_wait_seconds is None
        )
        assert (
            kubernetes_launchers.LaunchedKubernetesContainer.log_upload_wait_seconds
            is None
        )
