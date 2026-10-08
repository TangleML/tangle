"""The level-triggered backstop for promotion.

Promotion is edge-triggered: the only automatic caller of `promote()` is the sink at
`emissions/handlers/quota/sinks/quota_group.py:106`, on a member completing. The emission row
is leased, so most lost edges redeliver -- but a poisoned handler has no redelivery, and a
parked node is invisible to everything else: it sits at UNINITIALIZED, which the orchestrator's
queued sweep deliberately does not select. One lost edge and the cohort waits forever while the
group reports free capacity.

This loop asks the standing question instead of watching for the edge. `promote()` re-derives
free slots on every call rather than replaying a delta, so running it against a healthy group
writes nothing.
"""

import datetime
import logging
import typing

from sqlalchemy import orm

from cloud_pipelines_backend.emissions import consumer as emissions_consumer
from cloud_pipelines_backend.emissions.reconciling import base as reconciling_base
from cloud_pipelines_backend.quota import promotion
from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.quota.observability import promotion_observer
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS: typing.Final[float] = 60.0
# One consumer lease plus one pass, and derived rather than chosen. A promotion emission whose
# consumer died mid-handle is redelivered when its claim lease expires
# (`emissions.consumer.CLAIM_EXPIRES_AFTER_SECONDS`, 300s). Firing at exactly that moment would
# race the redelivery: two promotions, and an alert announcing a lost edge that was one second
# from arriving on its own. The extra pass gives the redelivery time to land and clear the
# condition, so what is left when this fires really is an edge that is never coming.
_MIN_STALL_SECONDS: typing.Final[float] = (
    emissions_consumer.CLAIM_EXPIRES_AFTER_SECONDS + _INTERVAL_SECONDS
)


class PromotionReconciler(reconciling_base.Reconciler):
    """The standing question behind promotion. Holds nothing between passes.

    An earlier draft counted consecutive stalled passes in a `dict[str, float]` keyed by group
    id, because nothing in the database recorded when a node was parked. `parked_at` adds the
    column and the dict goes with it -- the clock is a `WHERE` clause now. Three things follow,
    and the third is the one that would have bitten: it survives a deploy instead of restarting
    every group's clock; it needs no re-learning window after a restart; and two replicas read
    the same `parked_at` off the same row rather than each counting a private copy of the same
    stall. Run these loops once per deployment rather than once per replica.
    """

    def __init__(
        self,
        *,
        session_factory: typing.Callable[[], orm.Session],
        interval_seconds: float = _INTERVAL_SECONDS,
        min_stall_seconds: float = _MIN_STALL_SECONDS,
    ) -> None:
        """Take the session factory to open one session per unit of work from.

        Args:
            session_factory: Called once for the scan and once per group corrected. Not
                one session for the pass: `promote()` locks the group row the gate also
                takes (`quota/interceptor.py:118`), so a pass-wide transaction would hold
                every corrected group's lock until the last one finished, and one wedged
                group would roll back the corrections already made. Closing the scan's
                session before any write also stops an empty pass -- the steady state --
                from leaving its read view open until the next tick.
            interval_seconds: Overridable for tests. The service owns the schedule; this only
                tells it how often to ask.
            min_stall_seconds: Overridable for tests, but a test that pins the real derivation
                should backdate `parked_at` instead -- an injected threshold only proves the
                code fires after whatever it was handed.
        """
        self._session_factory = session_factory
        self._interval_seconds = interval_seconds
        self._min_stall_seconds = min_stall_seconds

    @property
    def name(self) -> str:
        return "quota-promotion"

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    def reconcile(self) -> int:
        """Promote in every group that has held a parked node past the stall threshold.

        `db_utils.utc_now()` and not `time.monotonic()`, now that the other side of the
        comparison is a stored timestamp: both sides have to be the same clock, and
        `parked_at` is written by `utc_now` (`quota/interceptor.py:379`). That does expose
        this to a backwards NTP step in a way an in-memory counter would not -- but a step
        large enough to matter here has already broken the emission claim leases, which are
        the same kind of stored deadline (`emissions/consumer.py:223`). It is the schema's
        exposure, not a new one.

        Returns:
            How many nodes were un-parked, across every group.
        """
        cutoff = db_utils.utc_now() - datetime.timedelta(
            seconds=self._min_stall_seconds
        )
        with self._session_factory() as session:
            stalled = [
                (row.id, row.name)
                for row in session.execute(
                    promotion.stalled_groups_query(cutoff=cutoff)
                ).all()
            ]
        total = 0
        for group_id, group_name in stalled:
            # One transaction per group: a group that cannot promote must not take the rest
            # of the pass down with it.
            with self._session_factory() as session:
                # The commit goes inside the block, per `promoting`'s own contract: left
                # outside it, a pass whose commit failed would still have been counted.
                with promotion_observer.promoting(
                    quota_group=group_name,
                    trigger=quota_metrics.PromotionTrigger.RECONCILE,
                ) as promotion_pass:
                    promoted = promotion.promote(session=session, group_id=group_id)
                    promotion_pass.promoted(count=promoted)
                    session.commit()
            total += promoted
        if total:
            # Two lines fire on a non-zero pass: the service's uniform "corrected N item(s)"
            # and this one. Deliberate -- the uniform line is what an operator greps across
            # every reconciler, and this one is the only place that says what a non-zero
            # count *means* here, which is not "recovered" but "an edge was lost".
            logger.warning(
                f"Quota reconciler un-parked {total} node(s) after "
                f"{self._min_stall_seconds:.0f}s of stall -- a promotion edge was lost"
            )
        return total
