"""What the waiting-age gauge reports: whose arrival, how old, and whose is left out.

Named test_trigger_staleness_poller so the module basename stays unique across the suite.
"""

import datetime
import itertools
import logging
import time
from typing import Any

import pytest
from sqlalchemy import orm

from tests.emissions.observability import probes
from cloud_pipelines_backend.triggers import db_models, event_state, service
from cloud_pipelines_backend.triggers.observability import metrics as trigger_metrics
from cloud_pipelines_backend.triggers.observability import (
    staleness_poller as trigger_staleness_poller,
)
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import db as db_utils

_POLLER_LOGGER = "cloud_pipelines_backend.triggers.observability.staleness_poller"
_GAUGE = "trigger.oldest_waiting_arrival_age"
_LAST_SUCCESS_GAUGE = "trigger.staleness_poll_last_success_age"
_SUBSCRIPTION = trigger_metrics.SUBSCRIPTION_ID_LABEL


def _all(*events: str, expire_seconds: int | None = None) -> dict[str, Any]:
    children: list[dict[str, Any]] = []
    for event in events:
        node: dict[str, Any] = {"event": event}
        if expire_seconds is not None:
            node["expire_seconds"] = expire_seconds
        children.append(node)
    return {"op": "all", "children": children}


# `pipeline` is unique on (user_id, file_path), so every pipeline this helper mints needs its
# own path; a shared one would fail the second _subscribe on the wrong table.
_pipeline_serial = itertools.count()

# A task spec a run can actually be built from. The gauge is about arrivals that have not
# triggered, so a target that cannot be run from would take a subscription out of the gauge
# for the wrong reason.
_RUNNABLE_TASK: dict[str, Any] = {
    "componentRef": {
        "spec": {
            "name": "triggered-target",
            "implementation": {"graph": {"tasks": {}}},
        }
    }
}


def _pipeline_id(session: orm.Session) -> str:
    """A real, runnable pipeline for a subscription to target, and its id.

    Real rather than an invented id: the target column is NOT NULL and a foreign key, and a
    plausible-looking string would only pass because this engine leaves SQLite's foreign keys
    switched off.

    Runnable rather than merely present, which is newer: a trigger starts a run from this
    pipeline's current version, so a row with no version at all now fails the trigger instead
    of being ignored.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id="test-owner",
        file_path=f"pipelines/auto-{next(_pipeline_serial)}.py",
    )
    session.add(pipeline)
    session.flush()
    session.add(
        user_pipeline_db_models.UserPipelineVersion(
            pipeline_id=pipeline.id,
            version_key=user_pipeline_db_models.CURRENT_VERSION_KEY,
            content_digest="d" * user_pipeline_db_models.DIGEST_LENGTH,
            root_pipeline_task=_RUNNABLE_TASK,
        )
    )
    session.flush()
    # Pointed at the version only after it exists. `current_version_key` is half of a composite
    # foreign key into `user_pipeline_version`, so setting it on the insert fails wherever
    # SQLite's foreign keys are switched on.
    pipeline.current_version_key = user_pipeline_db_models.CURRENT_VERSION_KEY
    session.flush()
    return pipeline.id


def _subscribe(
    *,
    session_factory: orm.sessionmaker,
    condition: dict[str, Any],
    name: str = "nightly-retrain",
    enabled: bool = True,
) -> str:
    """Create a subscription with its event states, and return its id."""
    with session_factory() as session:
        subscription = db_models.TriggerSubscription(
            name=name,
            definition={"name": name, "condition": condition},
            created_by="test-owner",
            enabled=enabled,
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
        )
        session.add(subscription)
        session.flush()
        event_state.sync(
            session=session,
            subscription_id=subscription.id,
            condition=condition,
        )
        subscription_id = subscription.id
        session.commit()
    return subscription_id


def _arrive(
    *,
    session_factory: orm.sessionmaker,
    event_name: str,
    emission_event_id: str,
    seconds_ago: float,
) -> None:
    """Record an arrival as though it had landed `seconds_ago`.

    The clock is moved rather than slept through: `record_event_and_maybe_start_runs` takes the
    instant it writes `filled_at` with, so a test can place an arrival an hour back without
    waiting an hour.
    """
    with session_factory() as session:
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name=event_name,
            emission_event_id=emission_event_id,
            now=db_utils.utc_now() - datetime.timedelta(seconds=seconds_ago),
        )


class _BrokenSessionFactory:
    """A session factory whose every call fails, standing in for a database that is down."""

    def __call__(self) -> orm.Session:
        raise RuntimeError("boom opening a session")


class _EndTheLoop(BaseException):
    """Raised from a patched sleep to break out of run_loop(), which never returns.

    A BaseException so the loop's own `except Exception` cannot swallow it.
    """


class TestAgeSeconds:
    """The age arithmetic, done in Python because the SQL for it is dialect-specific."""

    def test_a_naive_timestamp_is_read_as_utc(self) -> None:
        # Both engines hand back a naive datetime for a column written as UTC, so this is the
        # shape the query actually returns rather than an edge case.
        now = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        naive = datetime.datetime(2026, 1, 1, 11, 59, 0)

        assert trigger_staleness_poller._age_seconds(oldest=naive, now=now) == 60.0

    def test_an_arrival_stamped_after_the_reading_reports_zero(self) -> None:
        now = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        later = datetime.datetime(2026, 1, 1, 12, 0, 5, tzinfo=datetime.timezone.utc)

        assert trigger_staleness_poller._age_seconds(oldest=later, now=now) == 0.0


class TestWhatTheGaugeReports:
    def test_nothing_waiting_observes_nothing(
        self, metrics: probes.MetricsProbe, session_factory: orm.sessionmaker
    ) -> None:
        """Unlike the queue gauges there is no zero to report: a subscription with no arrival
        is not waiting on anything, and a series per idle subscription would be noise.
        """
        _subscribe(session_factory=session_factory, condition=_all("orders-ready"))
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )

        poller.poll()

        assert metrics.points(name=_GAUGE) == []

    def test_a_part_satisfied_subscription_reports_its_oldest_arrival(
        self, metrics: probes.MetricsProbe, session_factory: orm.sessionmaker
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready", "refunds-ready"),
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=3600,
        )
        _arrive(
            session_factory=session_factory,
            event_name="fx-ready",
            emission_event_id="em-2",
            seconds_ago=60,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )

        poller.poll()

        point = metrics.point(name=_GAUGE, attributes={_SUBSCRIPTION: subscription_id})
        assert point is not None
        assert point.value == pytest.approx(3600, abs=5)

    def test_triggering_takes_the_subscription_out_of_the_gauge(
        self, metrics: probes.MetricsProbe, session_factory: orm.sessionmaker
    ) -> None:
        """The mapping is rebound per poll, so a satisfied subscription stops being observed
        rather than holding its last age forever."""
        _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready"),
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=3600,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )
        poller.poll()
        assert len(metrics.points(name=_GAUGE)) == 1

        _arrive(
            session_factory=session_factory,
            event_name="fx-ready",
            emission_event_id="em-2",
            seconds_ago=0,
        )
        poller.poll()

        assert metrics.points(name=_GAUGE) == []

    def test_a_lapsed_arrival_is_not_aged(
        self, metrics: probes.MetricsProbe, session_factory: orm.sessionmaker
    ) -> None:
        """It no longer holds the condition part-open; `trigger.event_expired` reports it."""
        _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready", expire_seconds=1800),
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=3600,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )

        poller.poll()

        assert metrics.points(name=_GAUGE) == []

    def test_a_disabled_subscription_is_left_out(
        self, metrics: probes.MetricsProbe, session_factory: orm.sessionmaker
    ) -> None:
        """It banks arrivals for as long as it is off, which is the design, not a stall."""
        _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready"),
            enabled=False,
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=86400,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )

        poller.poll()

        assert metrics.points(name=_GAUGE) == []

    def test_each_waiting_subscription_gets_its_own_series(
        self, metrics: probes.MetricsProbe, session_factory: orm.sessionmaker
    ) -> None:
        first = _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready"),
            name="nightly-retrain",
        )
        second = _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "refunds-ready"),
            name="hourly-refresh",
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=120,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )

        poller.poll()

        assert len(metrics.points(name=_GAUGE)) == 2
        for subscription_id in (first, second):
            point = metrics.point(
                name=_GAUGE, attributes={_SUBSCRIPTION: subscription_id}
            )
            assert point.value == pytest.approx(120, abs=5)


class TestAPollThatFails:
    def test_a_failed_poll_keeps_the_last_reading_rather_than_clearing_it(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Reporting that nothing is waiting because the query broke is an all-clear."""
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready"),
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=600,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )
        poller.poll()
        poller._session_factory = _BrokenSessionFactory()

        with caplog.at_level(logging.ERROR, logger=_POLLER_LOGGER):
            with pytest.raises(_EndTheLoop):
                _run_one_loop(poller=poller)

        point = metrics.point(name=_GAUGE, attributes={_SUBSCRIPTION: subscription_id})
        assert point.value == pytest.approx(600, abs=5)
        assert "error polling DB" in caplog.text


class TestThePollerReportsItsOwnLiveness:
    """The ages above cannot report the poller stopping — they freeze and look healthy."""

    def test_a_poller_that_has_never_polled_ages_from_construction(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Not left unset: a first poll that fails must still read as stale, not as absent."""
        trigger_staleness_poller.StalenessPoller(session_factory=session_factory)

        point = metrics.point(name=_LAST_SUCCESS_GAUGE)
        assert point is not None
        assert point.value == pytest.approx(0, abs=5)

    def test_a_successful_poll_resets_it(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
    ) -> None:
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )
        poller._last_successful_poll_at = time.monotonic() - 600

        poller.poll()

        point = metrics.point(name=_LAST_SUCCESS_GAUGE)
        assert point.value == pytest.approx(0, abs=5)

    def test_a_dead_loop_keeps_ageing_while_the_arrival_ages_stay_frozen(
        self,
        metrics: probes.MetricsProbe,
        session_factory: orm.sessionmaker,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The frozen ages are the symptom nobody can see; this is the series that moves."""
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition=_all("orders-ready", "fx-ready"),
        )
        _arrive(
            session_factory=session_factory,
            event_name="orders-ready",
            emission_event_id="em-1",
            seconds_ago=600,
        )
        poller = trigger_staleness_poller.StalenessPoller(
            session_factory=session_factory
        )
        poller.poll()
        poller._session_factory = _BrokenSessionFactory()
        poller._last_successful_poll_at = time.monotonic() - 900

        with caplog.at_level(logging.ERROR, logger=_POLLER_LOGGER):
            with pytest.raises(_EndTheLoop):
                _run_one_loop(poller=poller)

        frozen = metrics.point(name=_GAUGE, attributes={_SUBSCRIPTION: subscription_id})
        assert frozen.value == pytest.approx(600, abs=5)
        last_success = metrics.point(name=_LAST_SUCCESS_GAUGE)
        assert last_success.value == pytest.approx(900, abs=5)


def _run_one_loop(*, poller: trigger_staleness_poller.StalenessPoller) -> None:
    """Run `run_loop` for exactly one iteration, by making its sleep end the loop.

    Args:
        poller: The poller to run one iteration of.

    Raises:
        _EndTheLoop: Always, from the patched sleep — the loop has no other exit.
    """

    def _stop(_seconds: float) -> None:
        raise _EndTheLoop

    original = trigger_staleness_poller.time.sleep
    trigger_staleness_poller.time.sleep = _stop  # type: ignore[assignment]
    try:
        poller.run_loop()
    finally:
        trigger_staleness_poller.time.sleep = original  # type: ignore[assignment]
