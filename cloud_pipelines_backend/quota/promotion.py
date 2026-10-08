"""Un-parking: who gets the slot a finishing node just freed.

The gate in quota/interceptor.py is the only thing that *grants* a slot. This module never
grants one -- it only makes parked nodes visible to the orchestrator again, oldest first, and
lets them race for the slot at the gate like anyone else.

That distinction is what makes promotion **advisory**. It can un-park too many nodes and be
right anyway: the extras lose at the gate and re-park, keeping their `created_at` and so their
place in line. Nothing here has to be exact, which is why there is no lock and no CAS.

**Promotion does not change occupancy**, so it deliberately does not bump `quota_group.version`.
A promoted node sits at `QUEUED` with its claim still `WAITING`, and branch 2 of
quota/occupancy.py counts `QUEUED` only when the claim is `ACTIVE`. An admission holding a
stale read of this group is therefore still holding a correct one, and forcing it to retry
would be churn for no safety.

Callers: the quota emission sink (`emissions/handlers/quota/sinks/quota_group.py`), on any
group member reaching a terminal status, and the PATCH handler after a capacity edit -- the
sink is edge-triggered, so without the second caller, raising capacity promotes nobody.
"""

import dataclasses
import datetime
import enum
import logging
from typing import Final

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models, occupancy

_logger = logging.getLogger(__name__)

"""How many waiters one promote pass will examine.

The pass is oldest-first and level-triggered, so a capped pass is an *incremental* pass, not a
wrong one: the operator calls it again and it resumes where the cap bit. 500 keeps the `IN` list
in `_cancelled_node_ids` and the JSON body finite on exactly the backlogged group this endpoint
exists for.
"""
_MAX_REPORT_WAITERS: Final[int] = 500


def parked_nodes_query(
    *,
    group_id: str,
    slots: int,
) -> sql.Select[tuple[bts.ExecutionNode]]:
    """Build the oldest-first query for the nodes to un-park.

    Two filters, and the second is easy to leave out by mistake. A `WAITING` claim does **not**
    mean the node is parked: promotion moves the node to `QUEUED` and leaves the claim
    `WAITING` until the node wins at the gate, so between a promotion and the next orchestrator
    pass there are `WAITING` claims whose nodes are already running for the slot. Selecting on
    the claim alone would spend this call's whole budget re-promoting them and un-park nobody.

    Ordering is `claim.created_at ASC` -- the claim's clock, not the node's. `created_at` is
    insert-only and survives re-parking, so it is the node's place in line rather than the time
    of its most recent rejection.

    Args:
        group_id: The `quota_group.id` whose waiters to consider.
        slots: How many to return. Must already be clamped to >= 0.

    Returns:
        A SELECT of at most `slots` nodes, oldest claim first, ties broken by
        `execution_node_id` so the order is total and repeatable -- `created_at` is a
        whole-second DATETIME on MySQL and same-second arrivals are the common case, not the
        rare one.

        This orders *un-parking*, which is not the same as ordering *admission*. An un-parked
        node goes back to QUEUED and is then re-selected by the orchestrator with no ORDER BY
        at all (`internal_process_queued_executions_queue`, where both `.order_by` lines are
        commented out), after which it must still re-win the CAS. FIFO here buys a fair
        *release*, not a fair *start*, and nothing in this file can promise the latter.
    """
    return (
        sql.select(bts.ExecutionNode)
        .join(
            db_models.QuotaGroupClaim,
            db_models.QuotaGroupClaim.execution_node_id == bts.ExecutionNode.id,
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id == group_id,
            occupancy.is_parked_for_quota_group_slot,
        )
        .order_by(
            db_models.QuotaGroupClaim.created_at.asc(),
            db_models.QuotaGroupClaim.execution_node_id.asc(),
        )
        .limit(slots)
    )


def stalled_groups_query(*, cutoff: datetime.datetime) -> sql.Select[tuple[str]]:
    """Every group holding a node parked since before `cutoff` while a slot stands free.

    Three predicates, and none is redundant.

    `claim.parked_at <= cutoff` is the clock, and it measures the right thing: how long this
    node has been parked *without interruption*, because every re-park re-stamps it
    (`quota/interceptor.py:379`). A node the gate turned away a second ago therefore reads as
    a one-second stall, not as however long it has been queueing.

    `occupancy.is_parked_for_quota_group_slot` is the state, imported and not restated -- it
    is what the poller's question gets wrong, and the waiter gauge is fixed by importing the
    same name. A node that `promote()` has already un-parked keeps its claim at `WAITING`
    until the gate admits it, and in that window it counts as a waiter while occupying
    nothing (`quota/occupancy.py:76`) -- so `waiters > 0 AND capacity - occupancy > 0` is true
    of a perfectly healthy group for as long as it takes the orchestrator to pick the node up.

    The free-slot subquery is the third, and it is what keeps this off the gate's hot path.
    Without it every *saturated* group holding an old waiter matches on every tick -- the
    common case in a busy fleet, not a rare one -- and `promote()` would take that group's
    `FOR UPDATE` row lock once a minute only to find nothing to do. It re-uses
    `occupancy.is_occupying_quota_group_slot` rather than restating what occupied means; only
    the count-and-correlate skeleton is local, because `occupancy_query` takes a literal id
    and widening it to accept a column for one caller would be the worse trade.

    The subquery is a filter, not a decision. `promote()` re-derives free slots under the row
    lock, so a group whose last slot is taken between this scan and that call promotes nobody
    rather than over-promoting.

    Together the first two are `parked_nodes_query`'s predicate plus a clock, which is what
    keeps selection and action honest: the reconciler promotes on exactly the condition it
    selected on, rather than on a proxy for it.

    Args:
        cutoff: Parked at or before this instant counts as stalled. The caller derives it
            from one consumer claim lease plus one pass, so a promotion still in flight is
            never mistaken for a lost one.

    Returns:
        A SELECT of distinct `(quota_group.id, quota_group.name)`. Distinct because a group
        with three old waiters is one group to promote in, not three.
    """
    occupancy_of_this_group = (
        sql.select(sql.func.count())
        .select_from(db_models.QuotaGroupClaim)
        .join(
            bts.ExecutionNode,
            bts.ExecutionNode.id == db_models.QuotaGroupClaim.execution_node_id,
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id == db_models.QuotaGroup.id,
            occupancy.is_occupying_quota_group_slot,
        )
        .correlate(db_models.QuotaGroup)
        .scalar_subquery()
    )
    return (
        # The name as well as the id: the promotions counter labels `quota_group` with the
        # group's *name* at every other call site (`quota/api_routes.py:737`, `:978`, `:1015`,
        # and the sink at `emissions/handlers/quota/sinks/quota_group.py:195`), so selecting
        # only the id here would file the reconciler's passes under a second, opaque series
        # for the same group. It is one more column off a row the join already reaches.
        sql.select(db_models.QuotaGroup.id, db_models.QuotaGroup.name)
        .join(
            db_models.QuotaGroupClaim,
            db_models.QuotaGroupClaim.quota_group_id == db_models.QuotaGroup.id,
        )
        .join(
            bts.ExecutionNode,
            bts.ExecutionNode.id == db_models.QuotaGroupClaim.execution_node_id,
        )
        .where(
            occupancy.is_parked_for_quota_group_slot,
            db_models.QuotaGroupClaim.parked_at <= cutoff,
            occupancy_of_this_group < db_models.QuotaGroup.capacity,
        )
        .distinct()
    )


def free_slots(
    *,
    session: orm.Session,
    group: db_models.QuotaGroup,
    for_update: bool = False,
) -> int:
    """How many waiters this group can afford to un-park right now.

    Clamped at zero, and the clamp is not defensive -- it is reachable by the ordinary case of
    an operator lowering `capacity` below the number of nodes already running. The subtraction
    then goes negative and reaches MySQL as `LIMIT -7`, which is a syntax error rather than an
    empty result.

    Args:
        session: The caller's session.
        group: The group to size. Read for `capacity` and `id` only.
        for_update: Passed through to `count_occupancy`. True for a caller that promotes on
            the answer, False for one that reports it.

    Returns:
        `capacity - occupancy`, never below zero.
    """
    used = occupancy.count_occupancy(
        session=session, group_id=group.id, for_update=for_update
    )
    return max(0, group.capacity - used)


def promote(
    *,
    session: orm.Session,
    group_id: str,
) -> int:
    """Un-park the oldest waiters a group has room for.

    **Does not commit.** Every caller is already inside a transaction and decides for itself
    when to close it: the sink opens one short session per delivery and commits there, and the
    API handlers run in the request's transaction. Committing here would take that choice away
    from both.

    Since promotion moved onto the emissions path it no longer shares a transaction with the
    completion that triggered it, so a crash in between leaves a freed slot un-promoted. That
    gap is closed by the emission row rather than by a transaction: the row is durable and
    leased, so an undelivered promotion is redelivered once the lease expires.

    Idempotent **per node, not per group**, and the difference is deliberate. A node already
    at `QUEUED` is never selected twice. But a second call still promotes the *next* waiter,
    because a promoted node sits at `QUEUED` with a `WAITING` claim and occupancy does not
    count that (quota/occupancy.py, branch 2) -- so the slot it was given still reads as free.
    Repeated calls drain the queue a waiter at a time and the group ends up over-promoted.

                         node status        claim state     counted as occupied?
    park                 UNINITIALIZED      WAITING         no
    after promote        QUEUED             WAITING         no   <- the gap
    after winning gate   QUEUED             ACTIVE          yes

    Only the gate flips a claim to `ACTIVE`, so between a promotion and the next orchestrator
    pass the middle row is where every promoted node sits, holding no occupancy.

    That is the accepted trade, not an oversight. The extras lose at the gate and re-park with
    `created_at` intact, so they keep their place; counting them as occupied instead would let
    a promotion wave re-fill the group it was meant to drain, and every waiter it un-parked
    would be turned away by the occupancy its own promotion created.

    Safe to call after a capacity *decrease*: `free_slots` returns 0 and the query never runs.

    Only the node's status changes. The claim row is untouched and stays `WAITING`, because the
    node has not won anything yet; the gate flips it to `ACTIVE` if and when it does.

    Args:
        session: The caller's session. Not committed here.
        group_id: The group that may have room. A group deleted underneath us promotes nobody.

    Returns:
        How many nodes were moved to `QUEUED`.
    """
    # Same lock the gate takes, taken first here too, so the two paths cannot both believe
    # they saw the whole world. See `occupancy.occupancy_query` for why the reads below must
    # also be locking: blocking on this lock does not advance a REPEATABLE READ snapshot.
    group = session.get(db_models.QuotaGroup, group_id, with_for_update=True)
    if group is None:
        # Deleting a group un-parks its waiters in the same transaction, so there is nothing
        # left here to promote and no reason to treat this as an error.
        return 0

    slots = free_slots(session=session, group=group, for_update=True)
    if slots == 0:
        return 0

    # Lock order for both paths: quota_group -> quota_group_claim -> execution_node. No cycle,
    # so no deadlock. `of=` keeps the lock on the node rows this pass is about to write.
    #
    # The claim's clock is added to the query rather than baked into `parked_nodes_query`,
    # whose published shape is a select of nodes. It is what makes the log line answer "how
    # long had these been waiting" -- `created_at` is insert-only, so it is the node's place
    # in line and not the time of its most recent rejection.
    rows = session.execute(
        parked_nodes_query(group_id=group_id, slots=slots)
        .add_columns(db_models.QuotaGroupClaim.created_at)
        .with_for_update(of=bts.ExecutionNode)
    ).all()
    for node, _waiting_since in rows:
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED

    if rows:
        promoted = " ".join(
            f"{node.id}@{waiting_since.isoformat()}" for node, waiting_since in rows
        )
        _logger.info(
            f"Quota promote group={group.name} group_id={group_id} slots={slots} "
            f"promoted={len(rows)} nodes={promoted}"
        )
    return len(rows)


def release_waiters_in_group(
    *,
    session: orm.Session,
    group_id: str,
) -> int:
    """Un-park every parked node in one group, ignoring capacity.

    Why `DELETE /api/quota_groups/{key_kind}/{key}` does not strand anything. Deleting a group
    cascades its claims away (`ON DELETE CASCADE`), and a parked node whose claim vanished is
    invisible forever: it sits at `UNINITIALIZED`, which the queued sweep deliberately does not
    select, with no claim row left to tell anyone why. So the waiters go back on the launch
    path *before* the row goes.

    Capacity is deliberately not consulted. Every other read in this module clamps to free
    slots, because the cap is being enforced; here the cap is being deleted, so there is
    nothing left to enforce and a partial release would strand the remainder.

    **Does not commit.** The caller shares one transaction with the deletion, which is what
    makes "un-park then delete" atomic -- a rollback mid-delete leaves every node still parked
    and the group still there, never a node QUEUED with its claim already cascaded away.

    Args:
        session: The caller's session. Not committed here.
        group_id: The `quota_group.id` whose waiters should be released.

    Returns:
        How many nodes were moved to `QUEUED`.
    """
    parked = session.scalars(
        sql.select(bts.ExecutionNode)
        .join(
            db_models.QuotaGroupClaim,
            db_models.QuotaGroupClaim.execution_node_id == bts.ExecutionNode.id,
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id == group_id,
            occupancy.is_parked_for_quota_group_slot,
        )
    ).all()
    for node in parked:
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
    if parked:
        _logger.info(
            f"Released {len(parked)} parked node(s) from quota group {group_id}"
        )
    return len(parked)


class PromotionOutcome(str, enum.Enum):
    """Why one waiter did, or did not, get un-parked by a promotion pass.

    Only `PROMOTED` is good news, and it is the least interesting: an operator reaches for
    `POST /promote` because a group looks stuck, so the value of the report is in the rest,
    which say *what* is stuck.
    """

    PROMOTED = "PROMOTED"
    RELEASED_CANCELLED = "RELEASED_CANCELLED"
    RELEASED_GONE = "RELEASED_GONE"
    """Defensive, and deliberately untested: the claim's FK is `ON DELETE CASCADE`, so an orphan
    claim cannot exist while the schema is what it is. Reaching it from a test needs the
    database's own integrity switched off, which proves nothing about the running system. Kept
    because `POST /promote` is the endpoint you hit when a group is wedged, and "this claim
    points at nothing, I removed it" is exactly what that endpoint is for."""
    ALREADY_QUEUED = "ALREADY_QUEUED"
    NO_CAPACITY = "NO_CAPACITY"
    ERROR = "ERROR"


@dataclasses.dataclass(frozen=True, kw_only=True)
class NodeOutcome:
    """One waiter's line in the report."""

    execution_node_id: str
    outcome: PromotionOutcome


@dataclasses.dataclass(frozen=True, kw_only=True)
class PromotionReport:
    """What a level-triggered promotion pass found and did.

    `occupancy_after` is deliberately absent. Promotion does not change occupancy -- a promoted
    node sits at `QUEUED` with a `WAITING` claim, which quota/occupancy.py does not count -- so
    an after-reading would be the before-reading with extra ceremony.
    """

    quota_group_id: str
    quota_group: str
    capacity: int
    occupancy_before: int
    promoted: int
    """How many WAITING claims this pass looked at, capped at `_MAX_REPORT_WAITERS`."""
    waiters_examined: int
    """How many WAITING claims the cap left behind. Zero means the pass saw the whole queue.

    Not an error when non-zero: the pass is oldest-first and holds no cursor, so calling the
    endpoint again resumes from where the cap bit.
    """
    waiters_unexamined: int
    nodes: list[NodeOutcome]


def _cancelled_node_ids(
    *,
    session: orm.Session,
    nodes: list[bts.ExecutionNode],
) -> set[str]:
    """Which of these nodes belong to something already asked to terminate.

    Mirrors the orchestrator's own cancellation test at orchestrator_sql.py:606 -- a
    `desired_state` of `TERMINATED` on the execution itself, or on the `PipelineRun` at the root
    of its ancestry. The run is not reachable from the node directly, hence the join through
    `execution_ancestor`.

    Args:
        session: The caller's session.
        nodes: The candidates. An empty list short-circuits rather than emitting `IN ()`.

    Returns:
        The subset of node ids that are cancelled.
    """
    if not nodes:
        return set()

    cancelled = {
        node.id
        for node in nodes
        if (node.extra_data or {}).get("desired_state") == "TERMINATED"
    }
    rows = session.execute(
        sql.select(
            bts.ExecutionToAncestorExecutionLink.execution_id,
            bts.PipelineRun.extra_data,
        )
        .join(
            bts.PipelineRun,
            bts.PipelineRun.root_execution_id
            == bts.ExecutionToAncestorExecutionLink.ancestor_execution_id,
        )
        .where(
            bts.ExecutionToAncestorExecutionLink.execution_id.in_(
                [node.id for node in nodes]
            )
        )
    ).all()
    for node_id, run_extra_data in rows:
        if (run_extra_data or {}).get("desired_state") == "TERMINATED":
            cancelled.add(node_id)
    return cancelled


def promote_with_report(
    *,
    session: orm.Session,
    group: db_models.QuotaGroup,
) -> PromotionReport:
    """The level-triggered promotion pass behind `POST /api/quota_groups/{key_kind}/{key}/promote`.

    **Does not commit.** Same contract as `promote`: the request's transaction is the caller's
    to close.

    Three things it does that `promote` does not, all of them affordable only because a human
    asked for this one and the sink did not:

    1.  It walks up to `_MAX_REPORT_WAITERS` waiters, not the `slots`-sized prefix, so the
        report can say why the ones it did not promote are still there. Past that cap it stops
        and reports `waiters_unexamined`; being oldest-first and idempotent, calling it again
        resumes.
    2.  It releases dead waiters. A cancelled node is un-parked *and* has its claim deleted:
        un-parking is what lets the orchestrator see it at all (the queued sweep skips
        `UNINITIALIZED`), and the cancel check at orchestrator_sql.py:606 runs before the
        interceptor, so the node goes `CANCELLED` without ever launching. This is the S3
        remedy. A claim whose node no longer exists is deleted as garbage.
    3.  It counts nodes an earlier pass already promoted against the free slots, which makes
        this endpoint idempotent. `promote` deliberately does not: over-promotion is safe and
        self-correcting, but an operator hitting a button twice should not have to know that.

    `promote` is left alone rather than delegating to this. The sink is on the hot path and its
    job is to fill a freed slot, not to garbage-collect; the two agree on outcomes, and where
    they differ this one is merely tidier.

    Args:
        session: The caller's session. Not committed here.
        group: The group to sweep. Read for `id`, `name` and `capacity`.

    Returns:
        The per-node report.
    """
    occupancy_before = occupancy.count_occupancy(session=session, group_id=group.id)
    slots = max(0, group.capacity - occupancy_before)

    # Never below `slots`: the cap bounds the report, it does not refuse to fill capacity a
    # large group is entitled to.
    scan_limit = max(_MAX_REPORT_WAITERS, slots)
    waiters = session.execute(
        sql.select(db_models.QuotaGroupClaim, bts.ExecutionNode)
        .outerjoin(
            bts.ExecutionNode,
            bts.ExecutionNode.id == db_models.QuotaGroupClaim.execution_node_id,
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id == group.id,
            # Deliberately NOT `occupancy.is_parked_for_quota_group_slot`: this pass must also
            # see STATES.md row 2, already promoted and still racing at the gate, or the
            # ALREADY_QUEUED branch below never fires and the endpoint stops being idempotent.
            db_models.QuotaGroupClaim.state == db_models.ClaimState.WAITING,
        )
        .order_by(
            # Same total order as `parked_nodes_query`, and for the same reason: `created_at`
            # is a whole-second DATETIME on MySQL, so same-second arrivals are the common
            # case and this read reports whoever the storage engine happened to return first.
            # It is a report, so the cost of a tie is a promote/skip verdict that moves
            # between two identical calls.
            db_models.QuotaGroupClaim.created_at.asc(),
            db_models.QuotaGroupClaim.execution_node_id.asc(),
        )
        .limit(scan_limit)
    ).all()

    # Equality on both leading columns of `ix_quota_group_claim_state_created`
    # (`quota/db_models.py:229`), so this is an index-only range count: no rows, no join, no
    # sort. Cheaper by far than the rows it counts, which is the point of not fetching them.
    total_waiters = (
        session.scalar(
            sql.select(sql.func.count())
            .select_from(db_models.QuotaGroupClaim)
            .where(
                db_models.QuotaGroupClaim.quota_group_id == group.id,
                db_models.QuotaGroupClaim.state == db_models.ClaimState.WAITING,
            )
        )
        or 0
    )

    live_nodes = [node for _, node in waiters if node is not None]
    cancelled = _cancelled_node_ids(session=session, nodes=live_nodes)

    outcomes: list[NodeOutcome] = []
    promoted = 0
    for claim, node in waiters:
        node_id = claim.execution_node_id
        try:
            if node is None:
                # Unreachable through the database today (ON DELETE CASCADE). See
                # PromotionOutcome.RELEASED_GONE for why it is handled anyway and not tested.
                session.delete(claim)
                outcome = PromotionOutcome.RELEASED_GONE
            elif node_id in cancelled:
                if (
                    node.container_execution_status
                    == bts.ContainerExecutionStatus.UNINITIALIZED
                ):
                    node.container_execution_status = (
                        bts.ContainerExecutionStatus.QUEUED
                    )
                session.delete(claim)
                outcome = PromotionOutcome.RELEASED_CANCELLED
            elif (
                node.container_execution_status
                != bts.ContainerExecutionStatus.UNINITIALIZED
            ):
                # Promoted by an earlier pass and still racing at the gate. It holds no
                # occupancy yet, so its slot is spent here to stop a second call from handing
                # the same slot to a second node.
                slots -= 1
                outcome = PromotionOutcome.ALREADY_QUEUED
            elif slots > 0:
                node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
                slots -= 1
                promoted += 1
                outcome = PromotionOutcome.PROMOTED
            else:
                outcome = PromotionOutcome.NO_CAPACITY
        except Exception:
            # One bad waiter does not abort the pass: the operator called this because the
            # group is stuck, and the rest of the queue is still worth draining.
            _logger.exception(
                f"Quota promote pass failed for node {node_id} in group {group.id}"
            )
            outcome = PromotionOutcome.ERROR
        outcomes.append(NodeOutcome(execution_node_id=node_id, outcome=outcome))

    _logger.info(
        f"Quota promote report group_id={group.id} capacity={group.capacity}"
        f" occupancy_before={occupancy_before} waiters={len(outcomes)} promoted={promoted}"
    )
    return PromotionReport(
        quota_group_id=group.id,
        quota_group=group.name,
        capacity=group.capacity,
        occupancy_before=occupancy_before,
        promoted=promoted,
        waiters_examined=len(outcomes),
        # `max(0, ...)` because the count and the page are two reads. Under REPEATABLE READ
        # they agree, but a negative number in an operator report is worse than a floor.
        waiters_unexamined=max(0, total_waiters - len(outcomes)),
        nodes=outcomes,
    )
