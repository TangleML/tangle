"""How many slots in a group are being used right now — defined once, read from two clocks.

The gate reads it to decide whether to admit a node; the metric poller reads it to publish
`quota.occupancy`. They import the same predicates so a dashboard and an admission decision
cannot disagree about what "occupied" means.

**There is no counter.** Occupancy is derived from the nodes' live statuses on every read, so
there is no tally to decrement, no release path to forget, and nothing to reconcile after a
crash. A finished node keeps its claim row forever and that row is simply inert -- whether or
not the sink got round to marking it `DONE`.

The number is stale the moment it is read. What makes admission safe is the version CAS in
quota/interceptor.py, not the freshness of this count.
"""

from collections.abc import Sequence
from typing import Final

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models

# Branch 1's whitelist. A container exists right now: starting, running, or being torn down.
# All three are real load on the downstream system this group exists to protect.
#
# Deliberately excluded: SUCCEEDED, FAILED, CANCELLED, SKIPPED and the other terminal states
# hold nothing. UNINITIALIZED is the parked state — a node waiting for a slot is by definition
# not using one. QUEUED is the ambiguous one and branch 2 handles it.
STATUSES_HOLDING_A_CONTAINER: Final[tuple[bts.ContainerExecutionStatus, ...]] = (
    bts.ContainerExecutionStatus.PENDING,
    bts.ContainerExecutionStatus.RUNNING,
    bts.ContainerExecutionStatus.CANCELLING,
)

# Branch 1. Reads the node only: none of these statuses is reachable without having launched,
# and launching is only reachable by winning a slot, so the node's own status already proves
# it was admitted. The claim's state is not consulted and must not be.
node_holding_a_container: Final[sql.ColumnElement[bool]] = (
    bts.ExecutionNode.container_execution_status.in_(STATUSES_HOLDING_A_CONTAINER)
)

# Branch 2. Reads the node *and* the claim. No container yet, but this node won its slot at
# the gate and is on its way to launch.
#
# QUEUED alone cannot say this, which is the whole reason `state` exists as a column: two
# different nodes sit at QUEUED for opposite reasons.
#
#   won admission, about to launch     QUEUED + ACTIVE   -> occupies a slot
#   just un-parked, gate not run yet   QUEUED + WAITING  -> occupies nothing
#
# Counting the second would let a promotion wave immediately re-fill the group it was meant
# to drain, and every promoted waiter would be turned away by the occupancy its own promotion
# created.
node_admitted_to_quota_group_not_yet_launched: Final[sql.ColumnElement[bool]] = (
    sql.and_(
        bts.ExecutionNode.container_execution_status
        == bts.ContainerExecutionStatus.QUEUED,
        db_models.QuotaGroupClaim.state == db_models.ClaimState.ACTIVE,
    )
)

# `or_` of the two branches is the definition of occupied. All three names are singular: each
# is a predicate asked of one row at a time.
is_occupying_quota_group_slot: Final[sql.ColumnElement[bool]] = sql.or_(
    node_holding_a_container, node_admitted_to_quota_group_not_yet_launched
)

# STATES.md row 1: parked. The mirror of `is_occupying_quota_group_slot` -- this node has asked
# for a slot and nothing is yet on its way to giving it one.
#
# Both halves are required, and the node half is the one that gets left out. `promote()` moves
# the node UNINITIALIZED -> QUEUED and leaves the claim WAITING until the gate runs, so
# `state == WAITING` alone also matches STATES.md row 2, a node that has already been picked
# up. Counting row 2 as parked makes a gauge over-report the queue and makes a backstop act on
# a node that is mid-promotion.
is_parked_for_quota_group_slot: Final[sql.ColumnElement[bool]] = sql.and_(
    db_models.QuotaGroupClaim.state == db_models.ClaimState.WAITING,
    bts.ExecutionNode.container_execution_status
    == bts.ContainerExecutionStatus.UNINITIALIZED,
)

# Not part of the definition above -- an index hint, and the reason it is a separate name.
#
# `state` appears inside the `or_`, so a planner can only use the index's leading
# `quota_group_id` column and reads every claim the group ever had, DONE rows included, to
# join each one to its node. DONE will eventually be most of the table. Spelling the live
# states as a top-level conjunct restores the `(quota_group_id, state)` prefix.
#
# It cannot change the answer. A DONE claim is written only after its node reached a terminal
# status, so branch 1 is already false for it, and branch 2 requires ACTIVE. Stated as "not
# the ledger" rather than "== ACTIVE" on purpose: the stronger form would also drop a WAITING
# row, and the whole point of branch 1 is that the node's status is believed over the claim's.
claim_is_not_terminal: Final[sql.ColumnElement[bool]] = (
    db_models.QuotaGroupClaim.state.in_(db_models.LIVE_CLAIM_STATES)
)


def occupancy_query(
    *,
    group_id: str,
    for_update: bool = False,
) -> sql.Select[tuple[int]]:
    """Build the occupancy count query for one group.

    Starts from the claim rows the group has live — every node that has asked for a slot and
    not yet been released — and joins each to its node's **live** status. The join is the
    reason there is no counter: the truth is read from the node rather than from a tally that
    could drift, and it stays the truth even when the sink that writes DONE lags or never
    runs. `claim_is_not_terminal` narrows which rows are joined; it never decides the answer.

    A caller that is about to *decide* on this number must pass `for_update=True`. Probed on
    MySQL 8.0: a plain read issued after a blocking `SELECT ... FOR UPDATE` still returns the
    transaction's pre-lock snapshot, so ordering the two transactions is not enough on its own.
    `FOR SHARE` rather than `FOR UPDATE` -- the reader is not writing these rows, only counting
    them, and a shared lock still reads the latest committed version.

    Callers that only report -- `quota/observability/poller.py`, the list endpoints -- leave this
    False. A metrics poller must never take row locks on the admission path.

    Args:
        group_id: The `quota_group.id` to count. Not the name — the annotation resolves that.
        for_update: Take a shared row lock on every row the count reads. For callers that admit
            or promote on the result; leave False to report on it.

    Returns:
        A SELECT COUNT(*) yielding one row.
    """
    query = (
        sql.select(sql.func.count())
        .select_from(db_models.QuotaGroupClaim)
        .join(
            bts.ExecutionNode,
            bts.ExecutionNode.id == db_models.QuotaGroupClaim.execution_node_id,
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id == group_id,
            claim_is_not_terminal,
            is_occupying_quota_group_slot,
        )
    )
    return query.with_for_update(read=True) if for_update else query


def occupancy_by_group_query(
    *,
    group_ids: Sequence[str],
) -> sql.Select[tuple[str, int]]:
    """Build one grouped occupancy count covering many groups.

    Same predicates as `occupancy_query`, imported rather than restated, so the list endpoint
    and the gate cannot drift apart. Exists because rendering a page of groups one
    `count_occupancy` call at a time is a query per row.

    Groups with an occupancy of zero produce no row at all -- a GROUP BY only emits groups it
    saw rows for -- so callers must default a missing key to 0 rather than trusting the keys.

    Args:
        group_ids: The `quota_group.id` values to count. An empty sequence yields a query
            matching nothing, which is correct and still costs a round trip; callers with an
            empty page should skip it.

    Returns:
        A SELECT of (quota_group_id, count) with one row per group that has any occupancy.
    """
    return (
        sql.select(
            db_models.QuotaGroupClaim.quota_group_id,
            sql.func.count().label("occupancy"),
        )
        .select_from(db_models.QuotaGroupClaim)
        .join(
            bts.ExecutionNode,
            bts.ExecutionNode.id == db_models.QuotaGroupClaim.execution_node_id,
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id.in_(group_ids),
            claim_is_not_terminal,
            is_occupying_quota_group_slot,
        )
        .group_by(db_models.QuotaGroupClaim.quota_group_id)
    )


def count_occupancy(
    *,
    session: orm.Session,
    group_id: str,
    for_update: bool = False,
) -> int:
    """Count the slots in use in one group, right now.

    Args:
        session: The caller's session, so the read joins its transaction. Note what that does
            and does not buy: this sees the caller's own uncommitted claim **only if the caller
            has flushed it**. Every session that reaches here is built `autoflush=False`
            (`emissions/consumer_main.py:124`), so an `add`ed-but-unflushed claim is invisible
            to this SELECT and the group reads one slot emptier than it is. The gate is safe
            from that only because it counts before it writes -- the count at
            `quota/interceptor.py:119` runs ahead of the claim insert at `quota/interceptor.py:131`
            -- and it never flushes. A caller that writes a claim first must flush in between.
        group_id: The `quota_group.id` to count.
        for_update: Passed through to `occupancy_query`. True for a caller that decides on the
            count, False for one that reports it.

    Returns:
        The number of occupied slots. Zero for a group nobody has ever claimed, and zero for
        a group whose every member has finished.
    """
    return session.scalars(
        occupancy_query(group_id=group_id, for_update=for_update)
    ).one()
