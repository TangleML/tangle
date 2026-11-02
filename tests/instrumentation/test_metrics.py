"""Tests for the orchestrator metric helpers."""

import datetime
from unittest import mock

import pytest

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.instrumentation import metrics


@pytest.fixture()
def refresh_duration(monkeypatch: pytest.MonkeyPatch) -> mock.MagicMock:
    histogram = mock.MagicMock()
    monkeypatch.setattr(metrics, "container_execution_refresh_duration", histogram)
    return histogram


@pytest.fixture()
def completion_lag_duration(monkeypatch: pytest.MonkeyPatch) -> mock.MagicMock:
    histogram = mock.MagicMock()
    monkeypatch.setattr(
        metrics, "container_execution_completion_lag_duration", histogram
    )
    return histogram


def _seconds_ago(seconds: float) -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
        seconds=seconds
    )


def test_refresh_duration_records_each_phase_in_seconds(refresh_duration):
    metrics.record_refresh_duration(
        queries_duration_ms=250, processing_duration_ms=1500
    )

    assert refresh_duration.record.call_args_list == [
        mock.call(0.25, attributes={"container_execution.refresh.phase": "queries"}),
        mock.call(1.5, attributes={"container_execution.refresh.phase": "processing"}),
    ]


def test_completion_lag_records_seconds_since_the_container_ended(
    completion_lag_duration,
):
    metrics.record_completion_lag(
        ended_at=_seconds_ago(42), status=bts.ContainerExecutionStatus.FAILED
    )

    (seconds,), kwargs = completion_lag_duration.record.call_args
    assert 42 <= seconds < 43
    assert kwargs == {"attributes": {"container_execution.status": "FAILED"}}


def test_completion_lag_clamps_an_end_time_in_the_future_to_zero(
    completion_lag_duration,
):
    metrics.record_completion_lag(
        ended_at=_seconds_ago(-5), status=bts.ContainerExecutionStatus.SUCCEEDED
    )

    (seconds,), _ = completion_lag_duration.record.call_args
    assert seconds == 0.0


def test_completion_lag_treats_a_naive_end_time_as_utc(completion_lag_duration):
    metrics.record_completion_lag(
        ended_at=_seconds_ago(42).replace(tzinfo=None),
        status=bts.ContainerExecutionStatus.SUCCEEDED,
    )

    (seconds,), _ = completion_lag_duration.record.call_args
    assert 42 <= seconds < 43


def test_completion_lag_skips_a_missing_end_time(completion_lag_duration):
    metrics.record_completion_lag(
        ended_at=None, status=bts.ContainerExecutionStatus.SUCCEEDED
    )

    completion_lag_duration.record.assert_not_called()
