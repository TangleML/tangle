"""Tests for the queue-age gauges: what each one counts, and what it reads when empty."""

import datetime
import logging

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions.observability import (
    backlog_poller as emission_backlog_poller,
)
from tests.emissions.observability import probes
from cloud_pipelines_backend.utils import db as db_utils

_POLLER_LOGGER = "cloud_pipelines_backend.emissions.observability.backlog_poller"
_OLDEST_PENDING = "emission.oldest_pending_age"
_OLDEST_IN_PROGRESS = "emission.oldest_in_progress_age"
_READINESS = db_models.EmissionType.READINESS.value


def _insert_event(
    *,
    session_factory: orm.sessionmaker,
    execution_node_id: str,
    age_seconds: float,
    claimed_status: str = db_models.ClaimStatus.PENDING.value,
) -> str:
    """Insert one emission_event created `age_seconds` ago, in the given claim state.

    The dedupe unique index spans (execution_node_id, emission_type,
    container_execution_status), so every caller passes its own node id. created_at and claimed_status are assigned after construction, since the
    model fills both itself and neither is a constructor argument.
    """
    with session_factory() as session:
        row = db_models.EmissionEvent(
            execution_node_id=execution_node_id,
            container_execution_id="ce-1",
            container_execution_status="SUCCEEDED",
            pipeline_run_id="run-1",
            emission_type=_READINESS,
        )
        row.created_at = db_utils.utc_now() - datetime.timedelta(seconds=age_seconds)
        row.claimed_status = claimed_status
        session.add(row)
        session.flush()
        event_id = row.id
        session.commit()
    return event_id


class _BrokenSessionFactory:
    """A session factory whose every call fails, standing in for a database that is down."""

    def __call__(self) -> orm.Session:
        raise RuntimeError("boom opening a session")


class _EndTheLoop(BaseException):
    """Raised from a patched sleep to break out of run_loop(), which never returns.

    A BaseException so the loop's own `except Exception` cannot swallow it.
    """


class TestAgeSeconds:
    """The age arithmetic, which is done in Python because the SQL for it is dialect-specific."""

    def test_no_row_in_the_state_is_zero(
        self,
    ) -> None:
        age = emission_backlog_poller._age_seconds(oldest=None, now=db_utils.utc_now())

        assert age == 0.0

    def test_a_naive_timestamp_is_read_as_utc(
        self,
    ) -> None:
        # Both engines hand back a naive datetime for a column written as UTC, so this is the
        # shape the query actually returns rather than an edge case.
        now = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        naive = datetime.datetime(2026, 1, 1, 11, 59, 0)

        assert emission_backlog_poller._age_seconds(oldest=naive, now=now) == 60.0

    def test_a_row_created_after_the_reading_reports_zero_rather_than_negative(
        self,
    ) -> None:
        now = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        later = datetime.datetime(2026, 1, 1, 12, 0, 5, tzinfo=datetime.timezone.utc)

        assert emission_backlog_poller._age_seconds(oldest=later, now=now) == 0.0


class TestQueueAgeGauges:
    """Each gauge covers exactly one claim state, and an empty state reads 0."""

    def test_an_empty_queue_reports_zero_on_both_before_any_poll(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        # No poll is run: the seeded values are what a collection landing before the first
        # poll reads, and the point of seeding them is that it reads 0 rather than nothing.
        _poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        assert metrics.point(name=_OLDEST_PENDING).value == 0.0
        assert metrics.point(name=_OLDEST_IN_PROGRESS).value == 0.0

    def test_an_empty_queue_reports_zero_after_a_poll(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        poller.poll()

        assert metrics.point(name=_OLDEST_PENDING).value == 0.0
        assert metrics.point(name=_OLDEST_IN_PROGRESS).value == 0.0

    def test_a_pending_row_reports_its_age(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        _insert_event(
            session_factory=session_factory,
            execution_node_id="node-pending",
            age_seconds=120.0,
        )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        poller.poll()

        assert metrics.point(name=_OLDEST_PENDING).value == pytest.approx(120.0, abs=5)
        assert metrics.point(name=_OLDEST_IN_PROGRESS).value == 0.0

    def test_the_oldest_pending_row_is_the_one_reported(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        for node_id, age in [("node-new", 10.0), ("node-old", 600.0)]:
            _insert_event(
                session_factory=session_factory,
                execution_node_id=node_id,
                age_seconds=age,
            )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        poller.poll()

        assert metrics.point(name=_OLDEST_PENDING).value == pytest.approx(600.0, abs=5)

    def test_an_in_progress_row_counts_only_in_the_in_progress_gauge(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        _insert_event(
            session_factory=session_factory,
            execution_node_id="node-claimed",
            age_seconds=300.0,
            claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
        )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        poller.poll()

        assert metrics.point(name=_OLDEST_IN_PROGRESS).value == pytest.approx(
            300.0, abs=5
        )
        assert metrics.point(name=_OLDEST_PENDING).value == 0.0

    def test_a_settled_row_counts_in_neither_gauge(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        _insert_event(
            session_factory=session_factory,
            execution_node_id="node-settled",
            age_seconds=9000.0,
            claimed_status=db_models.ClaimStatus.SETTLED.value,
        )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        poller.poll()

        assert metrics.point(name=_OLDEST_PENDING).value == 0.0
        assert metrics.point(name=_OLDEST_IN_PROGRESS).value == 0.0

    def test_the_two_states_are_reported_apart(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        _insert_event(
            session_factory=session_factory,
            execution_node_id="node-waiting",
            age_seconds=60.0,
        )
        _insert_event(
            session_factory=session_factory,
            execution_node_id="node-working",
            age_seconds=900.0,
            claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
        )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)

        poller.poll()

        # Which half of the loop stopped is the whole reason these are two gauges: a claimed
        # row aging out says the handler is stuck, an unclaimed one says nothing is picking up.
        assert metrics.point(name=_OLDEST_PENDING).value == pytest.approx(60.0, abs=5)
        assert metrics.point(name=_OLDEST_IN_PROGRESS).value == pytest.approx(
            900.0, abs=5
        )

    def test_a_second_poll_replaces_the_reading_rather_than_adding_to_it(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        event_id = _insert_event(
            session_factory=session_factory,
            execution_node_id="node-drains",
            age_seconds=450.0,
        )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)
        poller.poll()
        assert metrics.point(name=_OLDEST_PENDING).value == pytest.approx(450.0, abs=5)

        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            row.claimed_status = db_models.ClaimStatus.SETTLED.value
            session.commit()
        poller.poll()

        assert metrics.point(name=_OLDEST_PENDING).value == 0.0


class TestPollFailures:
    """A database that cannot be read must not read as a queue with nothing in it."""

    def test_a_failing_poll_raises_to_its_caller(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        poller = emission_backlog_poller.BacklogPoller(
            session_factory=_BrokenSessionFactory()
        )

        with pytest.raises(RuntimeError, match="boom opening a session"):
            poller.poll()

    def test_a_failing_poll_leaves_the_last_reading_in_place(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        _insert_event(
            session_factory=session_factory,
            execution_node_id="node-stuck",
            age_seconds=200.0,
        )
        poller = emission_backlog_poller.BacklogPoller(session_factory=session_factory)
        poller.poll()

        poller._session_factory = _BrokenSessionFactory()
        with pytest.raises(RuntimeError):
            poller.poll()

        # Zeroing here would turn a database outage into an all-clear on the one signal that
        # is supposed to catch it.
        assert metrics.point(name=_OLDEST_PENDING).value == pytest.approx(200.0, abs=5)

    def test_the_loop_logs_a_failed_poll_and_keeps_polling(
        self,
        metrics: probes.MetricsProbe,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        poller = emission_backlog_poller.BacklogPoller(
            session_factory=_BrokenSessionFactory()
        )
        waits: list[float] = []

        def _fake_sleep(seconds: float) -> None:
            waits.append(seconds)
            if len(waits) == 2:
                raise _EndTheLoop

        monkeypatch.setattr(emission_backlog_poller.time, "sleep", _fake_sleep)

        with caplog.at_level(logging.ERROR, logger=_POLLER_LOGGER):
            with pytest.raises(_EndTheLoop):
                poller.run_loop()

        # Two failed polls, each logged, each followed by the normal interval: a database
        # that is down stops the readings from advancing, not the process reporting them.
        assert waits == [30.0, 30.0]
        assert caplog.text.count("Emission backlog poller: error polling DB") == 2
