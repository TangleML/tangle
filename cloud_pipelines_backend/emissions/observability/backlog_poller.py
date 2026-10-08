"""How stale the emission queue is, reported from outside the consumer.

Two gauges, both the age of the oldest row in a claim state: `pending` for work nobody has
picked up, `in_progress` for work someone holds and has not settled. Together they say
whether emissions are flowing, and when they are not, which half of the loop stopped.

They are polled here rather than by the consumer on purpose. A gauge the consumer emits
cannot report the consumer's own absence — no process, no observation, and the series goes
stale rather than climbing, which is a harder thing to alert on than a number crossing a
threshold. Observed from another process, a consumer that is dead and one that is merely slow
look the same: the age climbs, and one threshold covers both.

This lives under `emissions/` rather than beside the core metrics poller because
`emissions/` imports the core models and not the other way round; querying an emission table from
the core models would invert that. `metrics_poller_main.py` is above both and imports each.
"""

import datetime
import logging
import time
import typing

import sqlalchemy as sql
from opentelemetry import metrics as otel_metrics
from sqlalchemy import orm

from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions.observability import environment
from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

# How often the ages are re-read. Matches the core metrics poller's interval: these answer a
# question about minutes of lag, so sampling them faster only costs queries.
_POLL_INTERVAL_SECONDS: typing.Final[float] = 30.0


def _age_seconds(
    *,
    oldest: datetime.datetime | None,
    now: datetime.datetime,
) -> float:
    """Turn the oldest `created_at` in a claim state into an age in seconds.

    The comparison is made here rather than in SQL because the two engines spell date
    arithmetic differently (SQLite `julianday`, MySQL `TIMESTAMPDIFF`) and nothing else on
    this path is dialect-specific. Both engines hand back a naive datetime for a column
    written as UTC, so a missing tzinfo is read as UTC rather than as an error.

    Args:
        oldest: The earliest `created_at` in the state, or None when no row is in it.
        now: The current time, passed in so both gauges age against the same instant.

    Returns:
        The age in seconds, or 0.0 when no row is in the state.
    """
    if oldest is None:
        return 0.0
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=datetime.timezone.utc)
    return max(0.0, (now - oldest).total_seconds())


class BacklogPoller:
    """Polls the two queue ages on a timer and reports each as a gauge.

    Split the way the core metrics poller is: the loop queries and caches, and the callbacks
    the SDK invokes only read the cache. A collection cycle must never wait on the database.
    """

    def __init__(
        self,
        *,
        session_factory: typing.Callable[[], orm.Session],
        poll_interval_seconds: float = _POLL_INTERVAL_SECONDS,
    ) -> None:
        """Register both gauges and start with an empty queue's readings.

        Args:
            session_factory: Opens the session each poll runs in.
            poll_interval_seconds: How long to wait between polls.
        """
        self._session_factory = session_factory
        self._poll_interval_seconds = poll_interval_seconds
        # Seeded rather than left unset so an empty queue reads 0 from the first collection
        # onwards. An absent series and a series at zero mean the same thing here, and only
        # one of them is easy to write an alert against.
        self._oldest_pending_age_seconds = 0.0
        self._oldest_in_progress_age_seconds = 0.0
        # Each gauge is built with its callback already attached, so holding the instrument
        # for the life of the poller is what keeps it observed.
        self._pending_age_gauge = emission_metrics.create_oldest_pending_age_gauge(
            callback=self._observe_oldest_pending_age,
        )
        self._in_progress_age_gauge = (
            emission_metrics.create_oldest_in_progress_age_gauge(
                callback=self._observe_oldest_in_progress_age,
            )
        )

    def run_loop(
        self,
    ) -> None:
        """Poll forever, logging and continuing past any failure.

        A failed poll leaves the previous ages in place rather than zeroing them: reporting
        a fresh queue because the query broke would turn a database problem into an all-clear.
        """
        while True:
            try:
                self.poll()
            except Exception:
                logger.exception("Emission backlog poller: error polling DB")
            time.sleep(self._poll_interval_seconds)

    def poll(
        self,
    ) -> None:
        """Re-read both ages and cache them."""
        now = db_utils.utc_now()
        with self._session_factory() as session:
            pending = self._oldest_created_at(
                session=session,
                claimed_status=db_models.ClaimStatus.PENDING,
            )
            in_progress = self._oldest_created_at(
                session=session,
                claimed_status=db_models.ClaimStatus.IN_PROGRESS,
            )
        # CPython: attribute assignment is atomic under the GIL, so the callbacks can read
        # these on the exporter's thread without a lock.
        self._oldest_pending_age_seconds = _age_seconds(oldest=pending, now=now)
        self._oldest_in_progress_age_seconds = _age_seconds(oldest=in_progress, now=now)
        logger.debug(
            f"Emission backlog poller: oldest pending"
            f" {self._oldest_pending_age_seconds:.1f}s, oldest in progress"
            f" {self._oldest_in_progress_age_seconds:.1f}s"
        )

    def _oldest_created_at(
        self,
        *,
        session: orm.Session,
        claimed_status: db_models.ClaimStatus,
    ) -> datetime.datetime | None:
        """The earliest `created_at` among rows in one claim state.

        Served by the `(claimed_status, created_at)` index: the equality seeks the state and
        the minimum is the first entry under it, so neither gauge scans the table.

        Args:
            session: The open session to query within.
            claimed_status: The claim state to take the minimum within.

        Returns:
            The earliest creation time in that state, or None when no row is in it.
        """
        return session.scalar(
            sql.select(sql.func.min(db_models.EmissionEvent.created_at)).where(
                db_models.EmissionEvent.claimed_status == claimed_status.value
            )
        )

    def _observe_oldest_pending_age(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report the cached age of the oldest unclaimed row.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation, in seconds.
        """
        return [
            otel_metrics.Observation(
                self._oldest_pending_age_seconds,
                environment.with_environment(attributes={}),
            )
        ]

    def _observe_oldest_in_progress_age(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report the cached age of the oldest claimed but unsettled row.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation, in seconds.
        """
        return [
            otel_metrics.Observation(
                self._oldest_in_progress_age_seconds,
                environment.with_environment(attributes={}),
            )
        ]
