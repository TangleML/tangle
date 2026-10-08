"""How long subscriptions have been sitting part-satisfied, reported from outside the sink.

The gauge this exists for: per enabled subscription holding an arrival it has not triggered
off, the age of the oldest such arrival. A subscription that is waiting normally reads as a
small number that resets to nothing each time it triggers; one whose expiry is too short for
the real gap between its upstreams reads as a number that climbs, up to `expire_seconds`.

A second gauge reports how long ago this poller last finished a poll, because the first one
cannot: a dead loop keeps exporting its last good ages, unchanged and unalarming.

Polled here rather than recorded by the sink, following the emission backlog poller's
reasoning: a value computed only by the sink goes stale exactly when the sink is the thing
that has stopped, and a stale series is harder to alert on than a climbing one.

Disabled subscriptions are excluded. Since a disabled subscription keeps recording arrivals
and only withholds the run, its oldest arrival ages for as long as it stays switched off —
which is the intended behaviour, not a stall, and would otherwise be the loudest thing on the
dashboard.
"""

import datetime
import logging
import time
import typing

import sqlalchemy as sql
from opentelemetry import metrics as otel_metrics
from sqlalchemy import orm

from cloud_pipelines_backend.triggers import db_models
from cloud_pipelines_backend.triggers.observability import metrics as trigger_metrics
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

# How often the ages are re-read. Matches the emission backlog poller: these answer a question
# about minutes of lag, so sampling faster only costs queries.
_POLL_INTERVAL_SECONDS: typing.Final[float] = 30.0


def _age_seconds(
    *,
    oldest: datetime.datetime,
    now: datetime.datetime,
) -> float:
    """Turn the oldest `filled_at` of one subscription into an age in seconds.

    Subtracted here rather than in SQL because the two engines spell date arithmetic
    differently (SQLite `julianday`, MySQL `TIMESTAMPDIFF`) and nothing else on this path is
    dialect-specific. Both hand back a naive datetime for a column written as UTC, so a
    missing tzinfo is read as UTC rather than as an error.

    Args:
        oldest: The earliest `filled_at` among the subscription's live arrivals.
        now: The current time, passed in so every subscription ages against the same instant.

    Returns:
        The age in seconds, never negative.
    """
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=datetime.timezone.utc)
    return max(0.0, (now - oldest).total_seconds())


class StalenessPoller:
    """Polls the per-subscription waiting age on a timer, and reports its own liveness.

    Split the way the emission backlog poller is: the loop queries and caches, and the
    callback the SDK invokes only reads the cache. A collection cycle must never wait on the
    database.
    """

    def __init__(
        self,
        *,
        session_factory: typing.Callable[[], orm.Session],
        poll_interval_seconds: float = _POLL_INTERVAL_SECONDS,
    ) -> None:
        """Register both gauges and start with nothing waiting.

        Args:
            session_factory: Opens the session each poll runs in.
            poll_interval_seconds: How long to wait between polls.
        """
        self._session_factory = session_factory
        self._poll_interval_seconds = poll_interval_seconds
        # Rebound wholesale by each poll rather than mutated, so the callback on the
        # exporter's thread always reads one poll's complete answer. A subscription that has
        # triggered since the last poll drops out of the mapping and stops being observed,
        # which is what makes the series fall back to nothing rather than hold its last age.
        self._ages_by_subscription: dict[str, float] = {}
        # Monotonic, so a clock step cannot make the poller look either fresh or hours stale,
        # and seeded at construction rather than left unset: a poller whose very first poll
        # fails should climb from process start, not report nothing.
        self._last_successful_poll_at = time.monotonic()
        # Both built with their callbacks already attached, so holding the instruments for the
        # life of the poller is what keeps them observed.
        self._gauge = trigger_metrics.create_oldest_waiting_arrival_age_gauge(
            callback=self._observe_ages,
        )
        self._last_success_gauge = (
            trigger_metrics.create_staleness_poll_last_success_age_gauge(
                callback=self._observe_last_success_age,
            )
        )

    def run_loop(
        self,
    ) -> None:
        """Poll forever, logging and continuing past any failure.

        A failed poll leaves the previous ages in place rather than clearing them: reporting
        that nothing is waiting because the query broke would turn a database problem into an
        all-clear. Swallowing the failure is what makes the last-successful-poll gauge the
        only thing that moves when every poll fails, so that is where the alert goes.
        """
        while True:
            try:
                self.poll()
            except Exception:
                logger.exception("Trigger staleness poller: error polling DB")
            time.sleep(self._poll_interval_seconds)

    def poll(
        self,
    ) -> None:
        """Re-read every waiting subscription's oldest live arrival and cache the ages."""
        now = db_utils.utc_now()
        with self._session_factory() as session:
            oldest_by_subscription = self._oldest_live_arrivals(
                session=session, now=now
            )
        # CPython: rebinding an attribute is atomic under the GIL, so the callback can read
        # this on the exporter's thread without a lock.
        self._ages_by_subscription = {
            subscription_id: _age_seconds(oldest=oldest, now=now)
            for subscription_id, oldest in oldest_by_subscription.items()
        }
        # Last, so a poll that raises part-way through does not count as a success.
        self._last_successful_poll_at = time.monotonic()
        logger.debug(
            f"Trigger staleness poller: {len(self._ages_by_subscription)} "
            "subscription(s) holding an arrival"
        )

    def _oldest_live_arrivals(
        self,
        *,
        session: orm.Session,
        now: datetime.datetime,
    ) -> dict[str, datetime.datetime]:
        """The earliest live `filled_at` held by each enabled subscription.

        The freshness expression is the one `event_state.filled_events` evaluates against, so
        the gauge and the sink cannot disagree about which arrivals still count. A lapsed
        arrival is deliberately not aged here — it no longer holds the condition part-open,
        and `trigger.event_expired` is what reports it.

        This scans `trigger_event_state`, and a scan is the right plan: the gauge reports one
        value per waiting subscription, so the query has to visit every live arrival however
        it is indexed. The scan runs in primary-key order, which is `(subscription_id,
        event_name)`, so the GROUP BY is served for free with no sort or temp table. An index
        on `(filled_at)` would not be used — the predicate matches roughly half the rows, and
        a planner takes the scan over a B-tree at that selectivity.

        Size is bounded by configuration rather than by traffic: rows are written by `sync`
        when a subscription is created or edited, never per arrival. Measured on the real
        schema, 400k rows cost ~314ms per poll, or about 1% of one connection at a 30s
        interval, and `metrics_poller_main` runs one of these per deployment, not per replica.

        The ceiling that arrives first is the gauge, not this query. `_observe_ages` labels
        every observation with its subscription id, so the exported series count tracks the
        number of waiting subscriptions. If that ever reaches five figures, drop the label
        and export a summary — the emission backlog poller already exports unlabelled.

        Args:
            session: The open session to query within.
            now: The instant freshness is judged against.

        Returns:
            Subscription id -> its oldest live `filled_at`. Empty when nothing is waiting.
        """
        rows = session.execute(
            sql.select(
                db_models.TriggerEventState.subscription_id,
                sql.func.min(db_models.TriggerEventState.filled_at),
            )
            .join(
                db_models.TriggerSubscription,
                db_models.TriggerSubscription.id
                == db_models.TriggerEventState.subscription_id,
            )
            .where(
                db_models.TriggerSubscription.enabled.is_(True),
                db_models.TriggerEventState.filled_at.is_not(None),
                sql.or_(
                    db_models.TriggerEventState.expires_at.is_(None),
                    db_models.TriggerEventState.expires_at > now,
                ),
            )
            .group_by(db_models.TriggerEventState.subscription_id)
        ).all()
        return {subscription_id: oldest for subscription_id, oldest in rows}

    def _observe_ages(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report the cached age for each subscription holding a live arrival.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per waiting subscription, in seconds, labelled by subscription.
        """
        return [
            otel_metrics.Observation(
                age,
                attributes={trigger_metrics.SUBSCRIPTION_ID_LABEL: subscription_id},
            )
            for subscription_id, age in self._ages_by_subscription.items()
        ]

    def _observe_last_success_age(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report how long ago the last poll finished.

        Unlabelled: there is one poller per deployment, and a label would only invite a
        per-subscription reading of a process-level number.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation, in seconds since the last successful poll.
        """
        return [
            otel_metrics.Observation(
                time.monotonic() - self._last_successful_poll_at,
            )
        ]
