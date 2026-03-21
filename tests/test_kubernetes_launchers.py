"""Tests for LaunchedKubernetesJob.ended_at — the property fixed to use
"Complete" instead of the incorrect "Succeeded" K8s Job condition type.
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest import mock

from cloud_pipelines_backend.launchers import kubernetes_launchers as kl


def _utc(
    *,
    year: int = 2026,
    month: int = 3,
    day: int = 20,
    hour: int = 12,
    minute: int = 0,
) -> datetime.datetime:
    return datetime.datetime(
        year, month, day, hour, minute, tzinfo=datetime.timezone.utc
    )


def _make_condition(
    *,
    type: str,
    status: str = "True",
    last_transition_time: datetime.datetime | None = None,
) -> mock.Mock:
    c = mock.Mock()
    c.type = type
    c.status = status
    c.last_transition_time = last_transition_time or _utc()
    return c


def _make_job(
    *,
    conditions: list[Any] | None = None,
    active: int | None = None,
    succeeded: int | None = None,
    failed: int | None = None,
    start_time: datetime.datetime | None = None,
    completions: int | None = 1,
) -> mock.Mock:
    job = mock.Mock()
    job.status = mock.Mock()
    job.status.conditions = conditions
    job.status.active = active
    job.status.succeeded = succeeded
    job.status.failed = failed
    job.status.start_time = start_time
    job.spec = mock.Mock()
    job.spec.completions = completions
    return job


def _make_launched_job(
    *,
    job: mock.Mock | None = None,
) -> kl.LaunchedKubernetesJob:
    if job is None:
        job = _make_job()
    return kl.LaunchedKubernetesJob(
        job_name="test-job",
        namespace="default",
        output_uris={},
        log_uri="gs://bucket/log",
        debug_job=job,
    )


class TestEndedAt:
    """Tests for LaunchedKubernetesJob.ended_at.

    This property reads job.status.conditions and returns the
    last_transition_time of the first terminal condition (Complete or Failed)
    with status=True.

    Code under test: kubernetes_launchers.py LaunchedKubernetesJob.ended_at
    """

    def test_returns_none_when_no_status(self) -> None:
        job = mock.Mock()
        job.status = None
        launched = _make_launched_job(job=job)
        assert launched.ended_at is None

    def test_returns_none_when_no_conditions(self) -> None:
        launched = _make_launched_job(job=_make_job(conditions=None))
        assert launched.ended_at is None

    def test_returns_none_when_empty_conditions(self) -> None:
        launched = _make_launched_job(job=_make_job(conditions=[]))
        assert launched.ended_at is None

    def test_returns_none_when_only_suspended_condition(self) -> None:
        """A Suspended=True condition is not terminal — ended_at stays None."""
        condition = _make_condition(type="Suspended", status="True")
        launched = _make_launched_job(job=_make_job(conditions=[condition]))
        assert launched.ended_at is None

    def test_returns_time_for_complete_condition(self) -> None:
        """Job finished successfully: condition type=Complete, status=True."""
        t = _utc(hour=14)
        condition = _make_condition(
            type="Complete", status="True", last_transition_time=t
        )
        launched = _make_launched_job(job=_make_job(conditions=[condition]))
        assert launched.ended_at == t

    def test_returns_time_for_failed_condition(self) -> None:
        """Job failed: condition type=Failed, status=True."""
        t = _utc(hour=15)
        condition = _make_condition(
            type="Failed", status="True", last_transition_time=t
        )
        launched = _make_launched_job(job=_make_job(conditions=[condition]))
        assert launched.ended_at == t

    def test_ignores_complete_condition_with_status_false(self) -> None:
        condition = _make_condition(type="Complete", status="False")
        launched = _make_launched_job(job=_make_job(conditions=[condition]))
        assert launched.ended_at is None

    def test_ignores_failed_condition_with_status_unknown(self) -> None:
        condition = _make_condition(type="Failed", status="Unknown")
        launched = _make_launched_job(job=_make_job(conditions=[condition]))
        assert launched.ended_at is None

    def test_does_not_match_succeeded_string(self) -> None:
        """Regression: 'Succeeded' is not a valid K8s Job condition type.
        The old code had condition.type in ("Succeeded", "Failed") which
        caused ended_at to always be None for successful jobs.
        """
        condition = _make_condition(type="Succeeded", status="True")
        launched = _make_launched_job(job=_make_job(conditions=[condition]))
        assert launched.ended_at is None

    def test_picks_terminal_condition_ignoring_suspended(self) -> None:
        """Real scenario: a job was suspended then resumed and completed.
        Conditions list has Suspended=True followed by Complete=True.
        ended_at should come from the Complete condition.
        """
        t_suspended = _utc(hour=10)
        t_complete = _utc(hour=14)
        conditions = [
            _make_condition(
                type="Suspended", status="True", last_transition_time=t_suspended
            ),
            _make_condition(
                type="Complete", status="True", last_transition_time=t_complete
            ),
        ]
        launched = _make_launched_job(job=_make_job(conditions=conditions))
        assert launched.ended_at == t_complete
