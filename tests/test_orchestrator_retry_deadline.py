from unittest import mock

import pytest

from cloud_pipelines_backend import orchestrator_sql


def test_retry_succeeds_before_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def func() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("try again")
        return "ok"

    monkeypatch.setattr(orchestrator_sql.time, "sleep", lambda _seconds: None)

    assert orchestrator_sql._retry(func, max_retries=3, max_elapsed_seconds=10) == "ok"
    assert calls == 2


def test_retry_deadline_caps_elapsed_time(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 100.0
    calls = 0

    def monotonic() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    def func() -> None:
        nonlocal calls, now
        calls += 1
        now += 0.9
        raise RuntimeError("still failing")

    monkeypatch.setattr(orchestrator_sql.time, "monotonic", monotonic)
    monkeypatch.setattr(orchestrator_sql.time, "sleep", sleep)

    with pytest.raises(orchestrator_sql.RetryDeadlineExceededError):
        orchestrator_sql._retry(
            func, max_retries=5, wait_seconds=1.0, max_elapsed_seconds=2
        )

    assert calls == 2


def test_max_retries_still_wins_before_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def func() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(orchestrator_sql.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="boom"):
        orchestrator_sql._retry(func, max_retries=2, max_elapsed_seconds=100)

    assert calls == 2


def test_retry_deadline_uses_environment_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(orchestrator_sql.ORCHESTRATOR_RETRY_DEADLINE_ENV, "2")

    with mock.patch.object(
        orchestrator_sql,
        "_configured_retry_deadline_seconds",
        wraps=orchestrator_sql._configured_retry_deadline_seconds,
    ) as configured_retry_deadline_seconds:
        orchestrator_sql._retry(lambda: "ok")

    configured_retry_deadline_seconds.assert_called_once_with()
