"""What every quota group looks like right now, reported from outside the orchestrator.

Six gauges per group — occupancy, capacity, waiters, the age of the oldest waiter, the count of
ACTIVE claims and the age of the oldest of those — read on a timer and cached, with the SDK's
callbacks doing nothing but hand back the cache.

They are polled here rather than emitted by the orchestrator on purpose, and it is the same
argument `emissions/observability/backlog_poller.py` makes. A gauge the orchestrator emits
cannot report the orchestrator's own absence: no process, no observation, and the series goes
stale rather than climbing. Stale is the hardest condition of all to alert on, because the
number simply stops moving. Observed from here, an orchestrator that is dead and a group that
is merely stuck look identical — `oldest_waiter_age` climbs — and one threshold catches both.

Read together they separate the causes:

    oldest_waiter_age high, occupancy < capacity   promotion is broken, and the reconciler
                                                   (`quota/reconciler.py:42`) is not catching
                                                   it either
    oldest_waiter_age climbing, capacity = 0       somebody left the group paused
    occupancy > capacity                           over-subscription, most often an operator
                                                   lowering `capacity` under running work --
                                                   ordinary, and why `free_slots` clamps at
                                                   zero (`quota/promotion.py:185`). Only if
                                                   no such change was made is this the
                                                   delete-then-recreate window
    active_claims > occupancy, growing             settlement is broken -- the gate is
                                                   writing ACTIVE claims and nothing is
                                                   flipping them DONE

The last row is why `active_claims` exists beside `occupancy` rather than instead of it. Every
row above it needs `occupancy < capacity` or a capacity of zero, so a group that is simply full
matched nothing at all: a wedged slot, a settlement outage and a genuinely busy group produced
one identical picture. Reading the same fact off the claim table as well as the node status
splits the second of those out. It does not split the first -- see `create_oldest_active_age_gauge`.

This lives under `quota/` rather than beside the core metrics poller for the layering reason
`emissions/` gives: `quota/` imports the core models and not the reverse, and `metrics_poller_main.py`
sits above both.
"""

import datetime
import logging
import time
import typing

import sqlalchemy as sql
from opentelemetry import metrics as otel_metrics
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models, occupancy
from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

# How often every group is re-read. Matches the emission backlog poller: these answer a
# question about minutes of waiting, so sampling faster only costs queries.
_POLL_INTERVAL_SECONDS: typing.Final[float] = 30.0


class GroupReading(typing.NamedTuple):
    """One group's numbers, as of the last poll."""

    occupancy: int
    capacity: int
    waiters: int
    oldest_waiter_age_seconds: float
    active_claims: int
    oldest_active_age_seconds: float


def all_groups_query() -> sql.Select[
    tuple[
        str,
        int,
        int,
        int,
        datetime.datetime | None,
        int,
        datetime.datetime | None,
    ]
]:
    """Build the one query behind every gauge.

    One query rather than one per gauge, and one rather than one per group. The shape is a left join
    from `quota_group` out to its live claims and their nodes, with the three per-group
    numbers computed as conditional aggregates over that single scan:

        quota_group  --LEFT JOIN-->  quota_group_claim  --LEFT JOIN-->  execution_node
             |                        (live states only)                     |
             |                                                               |
             `-- GROUP BY group.id, counting three different things about the same rows

    The left joins are what make an empty group report zero instead of disappearing. An inner
    join would drop any group with no claims, and a gauge that vanishes when the condition
    clears makes "healthy" and "not being measured" the same picture on a dashboard.

    Both counts are the predicates the gate itself uses, imported from `quota/occupancy.py`
    rather than restated, so the dashboard and the gate cannot disagree about how full a group
    is or how many are queueing behind it. `waiters` used to restate its half as
    `state == WAITING` alone, which also matches a node `promote()` has already un-parked and
    the gate has not yet admitted -- STATES.md row 2 -- so a healthy group read as having a
    queue for as long as the orchestrator took to pick the node up.

    The claim-state narrowing rides on the join condition rather than the WHERE clause:
    in the WHERE clause it would turn the outer join back into an inner one and take the empty
    groups away again.

    Returns:
        A SELECT of (name, capacity, occupancy, waiters, oldest waiting `created_at`,
        active claims, oldest ACTIVE `updated_at`), one row per group that exists, including
        the ones nothing has ever claimed.
    """
    claim = db_models.QuotaGroupClaim
    # Read off the claim row, where occupancy is read off the node's container status. Not a
    # shared predicate with `quota/occupancy.py` for once, and deliberately so: the whole value
    # of this number is that it comes from the other table.
    is_active = claim.state == db_models.ClaimState.ACTIVE
    return (
        sql.select(
            db_models.QuotaGroup.name,
            db_models.QuotaGroup.capacity,
            sql.func.coalesce(
                sql.func.sum(
                    sql.case((occupancy.is_occupying_quota_group_slot, 1), else_=0)
                ),
                0,
            ).label("occupancy"),
            sql.func.coalesce(
                sql.func.sum(
                    sql.case((occupancy.is_parked_for_quota_group_slot, 1), else_=0)
                ),
                0,
            ).label("waiters"),
            sql.func.min(
                sql.case((occupancy.is_parked_for_quota_group_slot, claim.created_at))
            ).label("oldest_waiter"),
            sql.func.coalesce(sql.func.sum(sql.case((is_active, 1), else_=0)), 0).label(
                "active_claims"
            ),
            # `updated_at` and not `created_at`: the gate bumps it on the WAITING -> ACTIVE
            # flip, so for an ACTIVE claim it is the admission time, which is what an age of
            # in-progress work means. `created_at` would measure from when the node queued,
            # which is the waiter's clock, already reported as `oldest_waiter`.
            sql.func.min(sql.case((is_active, claim.updated_at))).label(
                "oldest_active"
            ),
        )
        .select_from(db_models.QuotaGroup)
        .outerjoin(
            claim,
            sql.and_(
                claim.quota_group_id == db_models.QuotaGroup.id,
                occupancy.claim_is_not_terminal,
            ),
        )
        .outerjoin(bts.ExecutionNode, bts.ExecutionNode.id == claim.execution_node_id)
        .group_by(
            db_models.QuotaGroup.id,
            db_models.QuotaGroup.name,
            db_models.QuotaGroup.capacity,
        )
    )


def _age_seconds(
    *,
    oldest: datetime.datetime | None,
    now: datetime.datetime,
) -> float:
    """Turn a claim timestamp into an age in seconds.

    Used for both age gauges: the oldest waiter's `created_at` and the oldest ACTIVE claim's
    `updated_at`. Which column is being aged is the caller's business; the arithmetic and the
    naive-datetime handling are the same either way.

    Computed here rather than in SQL because the two engines spell date arithmetic differently
    (SQLite `julianday`, MySQL `TIMESTAMPDIFF`) and nothing else on this path is
    dialect-specific. Both hand back a naive datetime for a column written as UTC, so a
    missing tzinfo is read as UTC rather than as an error.

    Args:
        oldest: The earliest timestamp among the group's matching claims, or None when it has
            none.
        now: The current time, passed in so every group ages against the same instant.

    Returns:
        The age in seconds, or 0.0 when there is nothing to age.
    """
    if oldest is None:
        return 0.0
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=datetime.timezone.utc)
    return max(0.0, (now - oldest).total_seconds())


class QuotaPoller:
    """Polls every group on a timer and reports its numbers as gauges.

    Split the way the emission backlog poller is: the loop queries and caches, and the
    callbacks the SDK invokes only read the cache. A collection cycle must never wait on the
    database — it runs on the exporter's thread, and a slow query there stalls every metric
    the process exports, not just these.
    """

    def __init__(
        self,
        *,
        session_factory: typing.Callable[[], orm.Session],
        poll_interval_seconds: float = _POLL_INTERVAL_SECONDS,
    ) -> None:
        """Register every gauge and start with nothing to report.

        Args:
            session_factory: Opens the session each poll runs in.
            poll_interval_seconds: How long to wait between polls.
        """
        self._session_factory = session_factory
        self._poll_interval_seconds = poll_interval_seconds
        # Replaced wholesale on every poll, never mutated in place. That is what makes a
        # deleted group stop being observed rather than freezing at its last reading: it is
        # simply absent from the next dict, so the next collection yields no observation for
        # it and the series ends instead of flat-lining.
        self._readings: dict[str, GroupReading] = {}
        # Each gauge is built with its callback already attached, so holding the instrument
        # for the life of the poller is what keeps it observed.
        self._occupancy_gauge = quota_metrics.create_occupancy_gauge(
            callback=self._observe_occupancy,
        )
        self._capacity_gauge = quota_metrics.create_capacity_gauge(
            callback=self._observe_capacity,
        )
        self._waiters_gauge = quota_metrics.create_waiters_gauge(
            callback=self._observe_waiters,
        )
        self._oldest_waiter_age_gauge = quota_metrics.create_oldest_waiter_age_gauge(
            callback=self._observe_oldest_waiter_age,
        )
        self._active_claims_gauge = quota_metrics.create_active_claims_gauge(
            callback=self._observe_active_claims,
        )
        self._oldest_active_age_gauge = quota_metrics.create_oldest_active_age_gauge(
            callback=self._observe_oldest_active_age,
        )

    def run_loop(
        self,
    ) -> None:
        """Poll forever, logging and continuing past any failure.

        A failed poll leaves the previous readings in place rather than clearing them:
        reporting every group as empty because the query broke would turn a database problem
        into an all-clear, which is the one wrong answer this poller exists to prevent.
        """
        while True:
            try:
                self.poll()
            except Exception:
                logger.exception("Quota poller: error polling DB")
            time.sleep(self._poll_interval_seconds)

    def poll(
        self,
    ) -> None:
        """Re-read every group and replace the cache with what came back."""
        now = db_utils.utc_now()
        with self._session_factory() as session:
            rows = session.execute(all_groups_query()).all()
        readings = {
            row.name: GroupReading(
                occupancy=int(row.occupancy),
                capacity=int(row.capacity),
                waiters=int(row.waiters),
                oldest_waiter_age_seconds=_age_seconds(
                    oldest=row.oldest_waiter, now=now
                ),
                active_claims=int(row.active_claims),
                oldest_active_age_seconds=_age_seconds(
                    oldest=row.oldest_active, now=now
                ),
            )
            for row in rows
        }
        # CPython: rebinding an attribute is atomic under the GIL, so the callbacks can read
        # this on the exporter's thread without a lock. Rebinding, not updating -- mutating
        # the dict in place would let a callback iterate it mid-write.
        self._readings = readings
        logger.debug(f"Quota poller: {len(readings)} group(s) read")

    def _observations(
        self,
        *,
        value_of: typing.Callable[[GroupReading], float],
    ) -> list[otel_metrics.Observation]:
        """Turn the cached readings into one observation per group.

        Args:
            value_of: Picks the number this gauge reports out of a group's reading.

        Returns:
            One observation per group in the cache, each labelled with the group's name.
            Groups whose number is zero are included on purpose — see `create_waiters_gauge`.
        """
        return [
            otel_metrics.Observation(
                value_of(reading),
                attributes={quota_metrics.QUOTA_GROUP_LABEL: name},
            )
            for name, reading in self._readings.items()
        ]

    def _observe_occupancy(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report each group's cached occupancy.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per known group.
        """
        return self._observations(value_of=lambda reading: reading.occupancy)

    def _observe_capacity(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report each group's configured capacity.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per known group.
        """
        return self._observations(value_of=lambda reading: reading.capacity)

    def _observe_waiters(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report how many nodes are parked on each group.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per known group, zero included.
        """
        return self._observations(value_of=lambda reading: reading.waiters)

    def _observe_oldest_waiter_age(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report how long each group's oldest waiter has been parked.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per known group, in seconds, zero for a group with no waiters.
        """
        return self._observations(
            value_of=lambda reading: reading.oldest_waiter_age_seconds
        )

    def _observe_active_claims(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report how many claims each group holds in `ACTIVE`.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per known group, zero included.
        """
        return self._observations(value_of=lambda reading: reading.active_claims)

    def _observe_oldest_active_age(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report how long each group's oldest ACTIVE claim has been running.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation per known group, in seconds, zero for a group with nothing running.
        """
        return self._observations(
            value_of=lambda reading: reading.oldest_active_age_seconds
        )
