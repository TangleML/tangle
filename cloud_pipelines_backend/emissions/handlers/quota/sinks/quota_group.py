"""The quota sink: a node that ended un-parks the waiters its group can now afford.

This is the first sink in the emission path that **writes**. Every other one reads, or logs
and reports `ignore`, so two things that were incidental elsewhere are load-bearing here and
are named in `emit`.
"""

import logging

from sqlalchemy import exc as sql_exc
from sqlalchemy import orm

from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.dispatching.handlers.sinks import base as sinks_base
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)
from cloud_pipelines_backend.quota import claims, db_models, promotion
from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.quota.observability import promotion_observer

logger = logging.getLogger(__name__)


def _ignored(
    *,
    execution_node_id: str,
    reason: str,
    quota_group_id: str | None = None,
) -> handler_base.Outcome:
    """Build the ignore Outcome, whose only interesting field is the reason.

    Args:
        execution_node_id: The node the delivery was about.
        reason: Why nothing was done. Ends up on the emission event, so it is the field an
            operator greps when a node's slot looks stuck.
        quota_group_id: The group, when a claim was found. Absent when one was not.

    Returns:
        An ignore Outcome carrying those three as detail.
    """
    detail: dict[str, object] = {
        "sink": "quota_group",
        "execution_node_id": execution_node_id,
        "reason": reason,
    }
    if quota_group_id is not None:
        detail["quota_group_id"] = quota_group_id
    return handler_base.Outcome(status=handler_base.OutcomeStatus.IGNORE, detail=detail)


def _release_slot(*, session: orm.Session, claim: db_models.QuotaGroupClaim) -> str:
    """Close out one claim, handing its slot back to the group.

    The claim's whole life, and where this sits::

        WAITING --admit--> ACTIVE --node ends--> DONE
                                                  ^ terminal, and the row stays forever:
                                                    that is what makes the table a ledger

    Flushing here rather than leaving it to the commit::

        autoflush is OFF on this factory (emissions/consumer_main.py:122)

            with the flush        without it
            --------------        ----------
            UPDATE claim          (still pending in the session)
            SELECT  <- promote()  SELECT  <- promote() reads the table pre-UPDATE
            COMMIT                UPDATE, then COMMIT

    Both columns give the same answer -- occupancy counts *nodes*, not claims, and this
    node is already terminal -- so the flush is hygiene, not correctness. Deleting it
    fails no test.

    Args:
        session: The delivery's session. Flushed, not committed; the caller owns the commit.
        claim: The live claim to close out. Must not already be `DONE`.

    Returns:
        The group the slot went back to, read off the claim before it is closed.
    """
    group_id = claim.quota_group_id
    claim.state = db_models.ClaimState.DONE
    # Terminal, and not parked. Left set, a released claim would carry a timestamp that reads
    # as an ever-lengthening stall -- harmless today because the reconciler also requires
    # WAITING, but a stored lie waiting for the next reader who does not.
    claim.parked_at = None
    session.flush()
    return group_id


class QuotaGroupSink(sinks_base.Sink[quota_annotations.QuotaIntent]):
    """Promotes a quota group's oldest waiters once one of its members has ended.

    Holds no state beyond the session factory: everything it needs about the group is read
    from the database at delivery time, which is what makes redelivery safe.
    """

    def __init__(
        self,
        *,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Take the session factory to open one short session per delivery from.

        Args:
            session_factory: The consumer process's session factory. A factory rather than a
                session, because a sink outlives any one transaction and each delivery needs
                its own.
        """
        self._session_factory = session_factory

    def emit(
        self,
        *,
        intent: quota_annotations.QuotaIntent,
        execution_node_id: str,
    ) -> handler_base.Outcome:
        """Resolve the node's group through its claim row and promote that group's waiters.

        **The group is read off the claim, not off the intent.** `intent.quota_group` carries
        the name the node declared, but a name is not a durable identifier -- resolving by it
        would break the moment a group is renamed, and would silently promote the wrong group
        if a name were ever reused. The claim row holds `quota_group_id`, a foreign key, and
        the node id is on the event. So the lookup goes node -> claim -> group id.

        That path depends on a claim outliving the node's completion, which it does: nothing
        deletes a claim. This sink moves it to `DONE` instead, so the row stays readable as
        the ledger entry for that node. Occupancy never consults the state anyway -- it is
        read live from `execution_node.container_execution_status` -- so a claim this sink
        never reaches costs nothing beyond a stale-looking row.

        Commits in its own transaction, unlike the `before_commit` listener this replaces.
        Redelivery is handled explicitly: a claim already `DONE` is ignored rather than
        promoted a second time. That is a guard against double-counting, not against
        corruption -- `promote()` re-derives free slots from current state rather than
        replaying a delta, so running it twice is harmless either way.

        Args:
            intent: The validated quota intent. Read only for logging; see above.
            execution_node_id: The node whose completion freed the slot. The lookup key.

        Returns:
            A success Outcome naming how many waiters were promoted, or an ignore Outcome when
            the node held no claim or its claim was already released.

        Raises:
            handler_base.DeliveryIncomplete: The promotion hit a transient database failure and
                rolled back. Left unsettled deliberately so the row comes back after the lease.
        """
        with self._session_factory() as session:
            claim = claims.find_claim(
                session=session, execution_node_id=execution_node_id
            )
            if claim is None:
                # Not an error, and not rare: the interceptor is the only thing that writes
                # claims, and the orchestrator runs several checks before it that can end a node
                # outright -- upstream failed, conditionally disabled, cache hit, cancelled while
                # queued. Those still emit, so the claim is permanently absent, not late, and
                # reporting failure would retry a delivery that can never succeed.
                logger.info(
                    f"Quota sink: no claim for node={execution_node_id} "
                    f"group={intent.quota_group!r}; nothing to promote"
                )
                return _ignored(execution_node_id=execution_node_id, reason="no_claim")

            if claim.state is db_models.ClaimState.DONE:
                # A redelivery. The slot went back the first time round; promoting again
                # would be harmless but would report a second, phantom release.
                logger.info(
                    f"Quota sink: claim for node={execution_node_id} is already DONE; nothing to release"
                )
                return _ignored(
                    execution_node_id=execution_node_id,
                    reason="already_done",
                    quota_group_id=claim.quota_group_id,
                )

            #   one node ended
            #        |
            #        v
            #   _release_slot  ->  this claim: ACTIVE -> DONE      one slot back
            #        |
            #        v
            #   promote        ->  the oldest waiters: WAITING -> ACTIVE
            #                      as many as the freed capacity now affords, which is
            #                      why a release and a promotion are one transaction
            try:
                group_id = _release_slot(session=session, claim=claim)
                # The group's real name, for the metric label. Deliberately not `intent.quota_group`:
                # that is the name the node's spec declared, which is annotation-derived and may be
                # stale -- and a label taken from a spec is a label a typo can mint a series with.
                # This get is paid for by promote(), which does the same get and finds it already in
                # the identity map.
                group = session.get(db_models.QuotaGroup, group_id)
                with promotion_observer.promoting(
                    quota_group=(
                        group.name if group is not None else quota_metrics.MISSING_GROUP
                    ),
                    trigger=quota_metrics.PromotionTrigger.SINK,
                ) as promotion_pass:
                    promoted = promotion.promote(session=session, group_id=group_id)
                    promotion_pass.promoted(count=promoted)
                    # promote() deliberately does not commit -- it is written to run inside a
                    # caller's transaction. This sink is that caller now, so the commit is here,
                    # and inside the measured block so a failed commit does not leave a promotion
                    # counted that rolled back.
                    session.commit()
            except sql_exc.OperationalError as error:
                # Deadlock, lock-wait timeout, dropped connection. The release, the promotion
                # and their commit are one transaction, so nothing landed and every claim this
                # pass touched is still parked -- the redelivery redoes the block rather than
                # resuming it.
                #
                # Raised and not reported, because every Outcome settles the message and
                # promotion is edge-triggered: this completion produces exactly one edge, and
                # a settled message spends it on nothing.
                #
                # `OperationalError` and not bare `Exception`: a programming error should still
                # settle FAILED and land on the ledger, since redelivering it forever only
                # moves the bug into the queue.
                raise handler_base.DeliveryIncomplete(
                    f"quota sink: promote failed for node={execution_node_id}"
                ) from error

        logger.info(
            f"Quota sink: promoted {promoted} waiter(s) group_id={group_id} after node={execution_node_id} ended"
        )
        return handler_base.Outcome(
            status=handler_base.OutcomeStatus.SUCCESS,
            detail={
                "sink": "quota_group",
                "execution_node_id": execution_node_id,
                "quota_group_id": group_id,
                "promoted": promoted,
            },
        )
