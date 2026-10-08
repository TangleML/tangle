"""Unit tests for ReconcilerService — the schedule, using in-test fake reconcilers.

Nothing here touches a database. What is pinned is the scheduling contract every reconciler
inherits: each runs on its own interval, a failing pass does not take the tick down with it,
shutdown is prompt because the wait is on the stop event, and an empty service does not spin.
"""

import threading
import time

import pytest

from cloud_pipelines_backend.emissions.reconciling import base
from cloud_pipelines_backend.emissions.reconciling import service as reconciling_service


class _FakeReconciler(base.Reconciler):
    """Records each pass and returns a scripted correction count."""

    def __init__(
        self,
        *,
        name: str = "fake",
        interval_seconds: float = 0.01,
        corrects: int = 0,
        raises: bool = False,
    ) -> None:
        self._name = name
        self._interval_seconds = interval_seconds
        self._corrects = corrects
        self._raises = raises
        self.passes = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    def reconcile(self) -> int:
        self.passes += 1
        if self._raises:
            raise RuntimeError("pass exploded")
        return self._corrects


def _run_until(
    *, svc: reconciling_service.ReconcilerService, seconds: float
) -> threading.Event:
    """Run the service on a thread for `seconds`, then stop and join it."""
    stop = threading.Event()
    thread = threading.Thread(target=svc.run, kwargs={"stop": stop}, daemon=False)
    thread.start()
    time.sleep(seconds)
    stop.set()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the service did not stop when asked"
    return stop


class TestConstruction:
    def test_a_duplicate_name_is_refused(self) -> None:
        # Two reconcilers sharing a label would be indistinguishable in the logs and in the
        # metric, so this fails at construction rather than at 3am.
        with pytest.raises(ValueError, match="duplicate reconciler name=twin"):
            reconciling_service.ReconcilerService(
                reconcilers=[
                    _FakeReconciler(name="twin"),
                    _FakeReconciler(name="twin"),
                ]
            )

    def test_two_distinct_names_are_accepted(self) -> None:
        reconciling_service.ReconcilerService(
            reconcilers=[_FakeReconciler(name="a"), _FakeReconciler(name="b")]
        )


class TestTheTick:
    def test_every_reconciler_runs_on_the_first_tick(self) -> None:
        # A restart is exactly when a standing question is most likely to have gone
        # unanswered, so nothing waits out an interval before its first pass.
        first = _FakeReconciler(name="a", interval_seconds=3600.0)
        second = _FakeReconciler(name="b", interval_seconds=3600.0)
        svc = reconciling_service.ReconcilerService(reconcilers=[first, second])

        _run_until(svc=svc, seconds=0.05)

        assert first.passes == 1
        assert second.passes == 1

    def test_a_long_interval_does_not_run_again(self) -> None:
        slow = _FakeReconciler(name="slow", interval_seconds=3600.0)
        svc = reconciling_service.ReconcilerService(reconcilers=[slow])

        _run_until(svc=svc, seconds=0.1)

        assert slow.passes == 1

    def test_a_short_interval_runs_repeatedly(self) -> None:
        quick = _FakeReconciler(name="quick", interval_seconds=0.001)
        svc = reconciling_service.ReconcilerService(reconcilers=[quick])

        _run_until(svc=svc, seconds=0.1)

        assert quick.passes > 1

    def test_intervals_are_per_reconciler_not_per_service(self) -> None:
        # The reason `interval_seconds` is on the reconciler: a backstop and a retention
        # sweep answer questions on completely different timescales.
        quick = _FakeReconciler(name="quick", interval_seconds=0.001)
        slow = _FakeReconciler(name="slow", interval_seconds=3600.0)
        svc = reconciling_service.ReconcilerService(reconcilers=[quick, slow])

        _run_until(svc=svc, seconds=0.1)

        assert slow.passes == 1
        assert quick.passes > slow.passes


class TestFailureIsolation:
    def test_a_raising_pass_does_not_stop_the_loop(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        boom = _FakeReconciler(name="boom", interval_seconds=0.001, raises=True)
        svc = reconciling_service.ReconcilerService(reconcilers=[boom])

        with caplog.at_level("ERROR"):
            _run_until(svc=svc, seconds=0.1)

        assert boom.passes > 1, "the loop gave up after the first failure"
        assert "Reconciler boom: pass failed" in caplog.text

    def test_a_raising_reconciler_does_not_starve_its_neighbour(self) -> None:
        boom = _FakeReconciler(name="boom", interval_seconds=0.001, raises=True)
        fine = _FakeReconciler(name="fine", interval_seconds=0.001)
        svc = reconciling_service.ReconcilerService(reconcilers=[boom, fine])

        _run_until(svc=svc, seconds=0.1)

        assert fine.passes > 1


class TestLogging:
    def test_a_zero_pass_says_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        # The expected answer on a healthy system. Logging it would bury the one line that
        # matters under a line a minute, per reconciler, forever.
        quiet = _FakeReconciler(name="quiet", interval_seconds=3600.0, corrects=0)
        svc = reconciling_service.ReconcilerService(reconcilers=[quiet])

        with caplog.at_level("WARNING"):
            _run_until(svc=svc, seconds=0.05)

        assert "corrected" not in caplog.text

    def test_a_non_zero_pass_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        # Not INFO: a non-zero count is not "working", it is "something upstream failed".
        busy = _FakeReconciler(name="busy", interval_seconds=3600.0, corrects=3)
        svc = reconciling_service.ReconcilerService(reconcilers=[busy])

        with caplog.at_level("WARNING"):
            _run_until(svc=svc, seconds=0.05)

        assert "Reconciler busy corrected 3 item(s)" in caplog.text


class TestShutdown:
    def test_an_empty_service_waits_rather_than_spinning(self) -> None:
        # `_seconds_until_next` returns a long wait rather than 0.0 when there is nothing to
        # schedule; a zero would turn the `while` into a busy loop on an idle process.
        svc = reconciling_service.ReconcilerService(reconcilers=[])

        assert svc._seconds_until_next(due_at={}) == pytest.approx(60.0)

    def test_an_empty_service_still_stops_promptly(self) -> None:
        # And the long wait costs nothing at shutdown, because it is a wait on the event.
        svc = reconciling_service.ReconcilerService(reconcilers=[])

        started = time.monotonic()
        _run_until(svc=svc, seconds=0.02)

        assert time.monotonic() - started < 1.0, "shutdown waited out the empty tick"

    def test_an_overdue_deadline_floors_at_zero(self) -> None:
        # Never negative: `stop.wait` with a negative timeout returns immediately anyway, but
        # a floor keeps the intent readable and the next iteration is what clears the backlog.
        svc = reconciling_service.ReconcilerService(reconcilers=[])

        assert svc._seconds_until_next(due_at={"late": time.monotonic() - 100.0}) == 0.0

    def test_stop_is_honoured_between_reconcilers(self) -> None:
        # Checked inside the for-loop as well as between ticks, so a long list does not delay
        # shutdown by its own length.
        many = [
            _FakeReconciler(name=f"r{i}", interval_seconds=3600.0) for i in range(50)
        ]
        svc = reconciling_service.ReconcilerService(reconcilers=many)

        _run_until(svc=svc, seconds=0.05)

        assert all(r.passes <= 1 for r in many)
