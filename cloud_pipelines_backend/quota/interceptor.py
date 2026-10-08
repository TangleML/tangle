"""The admission gate: the orchestrator's chance to take a queued node off the launch path.

Wired in as the upstream `QueuedExecutionInterceptor` (`orchestrator_sql.py:42`) and called at
`orchestrator_sql.py:629`, once the node is known to be launchable — inputs present, not
conditionally skipped, no cache hit, not cancelled. Returning True means this module owns the
node from that point; returning False lets the orchestrator launch it as it always has.

Everything here is admission. Un-parking lives in quota/promotion.py, and the two are
deliberately asymmetric: promotion is advisory and every node it wakes is re-checked here.
"""

import datetime
import logging
from typing import Final, NamedTuple

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import claims, db_models, groups, occupancy
from cloud_pipelines_backend.quota.observability import gate_observer
from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.utils import db as db_utils

_logger: Final[logging.Logger] = logging.getLogger(__name__)

# How many times to re-read and retry after losing the version compare-and-set. Losing means
# another admission committed first, so a retry re-reads a capacity and an occupancy that have
# both moved. Exhausting the budget is not an error, and it is not evidence the group is full --
# every attempt that got that far saw room. The node is left QUEUED and simply tries again on the
# next orchestrator pass, which is the same outcome as arriving one moment later.
#
# Two, and deliberately not more, and with no sleep between attempts: this loop runs on the
# orchestrator's *only* queued-execution thread -- the sweep takes one row per pass and processes
# them serially -- so every attempt spent here is charged to every other execution waiting behind
# it. Backing off inside the loop would make that worse, not better; the cheap retry is the next
# orchestrator pass, which costs this node latency and costs the queue nothing.
DEFAULT_MAX_CAS_ATTEMPTS: Final[int] = 2


class QuotaGroupInterceptor:
    """Parks a node whose quota group is full, and lets every other node through.

    Implements the upstream `QueuedExecutionInterceptor` protocol structurally; it does not
    inherit from it, so the orchestrator stays unaware that quota groups exist.
    """

    def __init__(
        self,
        *,
        max_cas_attempts: int = DEFAULT_MAX_CAS_ATTEMPTS,
    ) -> None:
        """
        Args:
            max_cas_attempts: How many times to retry a lost compare-and-set before giving up
                and leaving the node QUEUED for the next orchestrator pass.
        """
        self._max_cas_attempts = max_cas_attempts

    def intercept(
        self,
        *,
        session: orm.Session,
        execution: bts.ExecutionNode,
    ) -> bool:
        """Decide whether this node launches now.

        Args:
            session: The orchestrator's session. Owned by the caller until this returns True.
            execution: The node about to be launched.

        Returns:
            True when the node must not launch -- parked, or left queued to retry later;
            False to launch it.
        """
        with gate_observer.gating() as gate:
            resolution = groups.resolve_group(session=session, execution=execution)
            if resolution.group is None:
                # Either the node declared no group, or it named one that does not exist. Both
                # launch ungated and write no claim row; resolve_group has already marked the
                # node in the second case. Only the second is a gate decision -- a node that
                # asked for nothing was never gated.
                if resolution.is_missing:
                    # The one place the unknown name reaches the log. The counter records
                    # this as quota_group="<missing>" instead, because a name from an
                    # annotation is typo- and attacker-controlled and would be unbounded
                    # label cardinality. The counter says how often; this says which node
                    # asked for what. quota/groups.py logs nothing by design.
                    _logger.warning(
                        f"Quota ungated node={execution.id} "
                        f"declared_group={resolution.declared_name!r}: no such group"
                    )
                    gate.ungated()
                return False

            return self._try_claim(
                session=session,
                execution=execution,
                group=resolution.group,
                gate=gate,
            )

    def _try_claim(
        self,
        *,
        session: orm.Session,
        execution: bts.ExecutionNode,
        group: db_models.QuotaGroup,
        gate: gate_observer.Gating,
    ) -> bool:
        """Take a slot in the group, or park the node holding its place in line.

        The compare-and-set on `quota_group.version` is the whole of the mutual exclusion.
        You cannot lock the absence of rows — what is being protected is a `COUNT(*)`, and two
        transactions can both read "3 of 4 used" and both admit. Bumping a version on a single
        row turns that aggregate check into a single-row write one of them must lose. There is
        no `SELECT ... FOR UPDATE`, which is a no-op on SQLite and would make the tests prove
        nothing.

        Args:
            session: The orchestrator's session.
            execution: The node asking for a slot.
            group: The group it named, already resolved.
            gate: The open measurement of this call, told which exit was taken.

        Returns:
            True when the node must not launch -- parked at capacity, or left queued after
            losing every compare-and-set. False when it won a slot.
        """
        group_id = group.id
        # Read once, here: every later exit is after a commit or a rollback that expires the
        # object, and re-reading `name` for a label would buy a SELECT per gate call.
        group_name = group.name
        gate.measuring(quota_group=group_name)

        for attempt in range(self._max_cas_attempts):
            # Re-read on every attempt. Losing the CAS means somebody else committed, so both
            # the capacity and the occupancy this attempt read are stale.
            # Held to commit. The park takes no slot, so it does no version CAS (see `_park`)
            # -- but it must not commit a park decided on an occupancy a concurrent release
            # has already changed. The lock orders this against `promotion.promote`, which
            # takes the same row first; the locking read below is what makes the count fresh.
            current = session.get(db_models.QuotaGroup, group_id, with_for_update=True)
            if current is None:
                # Deleted underneath us. Deleting a group un-parks its waiters on purpose, so
                # launching ungated is the same answer the deletion itself would have given.
                return False

            if self._already_holds_a_slot(
                session=session, execution=execution, group_id=group_id
            ):
                # A re-entry: this node won a slot earlier and came back through the gate,
                # because the launch it was admitted for failed and the sweep re-queued it.
                # Its own ACTIVE claim is part of the occupancy count below, so re-running the
                # check would let the node park itself -- and at capacity 1 with no other
                # member, nothing would ever complete to promote it again.
                return False

            count = occupancy.count_occupancy(
                session=session, group_id=group_id, for_update=True
            )
            if count >= current.capacity:
                outcome = self._park(
                    session=session,
                    execution=execution,
                    group_id=group_id,
                    group_name=group_name,
                )
                if not outcome.parked:
                    # Somebody committed a decision about this node between the sweep reading
                    # it QUEUED and the park writing to it -- a cancel, or the un-park a group
                    # delete does. Nothing was written, and nothing should be: the row is left
                    # as the winner left it and the next sweep decides again on that state.
                    # No verdict is named, so this exit shows up as duration without a
                    # decision. It is one of three that do -- the others are `:144` and `:154`,
                    # both above -- and `gate_observer.py:29` lists them. An exhausted
                    # compare-and-set is not among them; that is counted as CONTENDED.
                    return True
                gate.parked(quota_group=group_name, was_waiting=outcome.was_waiting)
                return True

            if self._bump_version(session=session, group=current):
                # Read the log fields before committing: commit expires the object, so
                # touching it afterwards costs an extra SELECT on every admission.
                capacity = current.capacity
                # The bump and the claim share a transaction, so a failed commit takes the
                # slot back with it.
                written = _claim_impl(
                    session=session,
                    execution_node_id=execution.id,
                    group_id=group_id,
                    state=db_models.ClaimState.ACTIVE,
                )
                session.commit()
                _logger.info(
                    f"Quota admit node={execution.id} group={group_name} "
                    f"occupancy={count}/{capacity} attempt={attempt + 1} "
                    f"waiting_since={_iso(written.waiting_since)}"
                )
                gate.admitted(quota_group=group_name)
                _record_wait(waiting_since=written.waiting_since, group_name=group_name)
                return False

            # Lost the race. The rollback is what makes the retry meaningful: continuing in
            # the same transaction would re-serve the same snapshot under REPEATABLE READ and
            # fail the compare-and-set forever.
            session.rollback()

        # Every attempt above saw room and lost the race anyway, so the one thing we have not
        # established is that the group is full. Parking would claim we had: it moves the node
        # to UNINITIALIZED, where only a member completion can wake it. Leave it QUEUED, which
        # the orchestrator re-polls every pass, and decline to launch it this time round.
        _logger.warning(
            f"Quota gate gave up after {self._max_cas_attempts} contended attempts; "
            f"leaving node={execution.id} queued for the next pass group_id={group_id}"
        )
        gate.contended(quota_group=group_name)
        return True

    @staticmethod
    def _already_holds_a_slot(
        *,
        session: orm.Session,
        execution: bts.ExecutionNode,
        group_id: str,
    ) -> bool:
        """Report whether this node already won a slot in this group.

        Args:
            session: The orchestrator's session.
            execution: The node asking.
            group_id: The group it is asking for.

        Returns:
            True when an ACTIVE claim for this node and this group already exists.
        """
        existing = claims.find_claim(session=session, execution_node_id=execution.id)
        return (
            existing is not None
            and existing.quota_group_id == group_id
            and existing.state is db_models.ClaimState.ACTIVE
        )

    @staticmethod
    def _bump_version(
        *,
        session: orm.Session,
        group: db_models.QuotaGroup,
    ) -> bool:
        """Run the compare-and-set on the group's version.

        Args:
            session: The orchestrator's session.
            group: The group as this attempt read it, carrying the version to match.

        Returns:
            True when this caller won the slot, False when another admission got there first.
        """
        result = session.execute(
            sql.update(db_models.QuotaGroup)
            .where(
                db_models.QuotaGroup.id == group.id,
                db_models.QuotaGroup.version == group.version,
            )
            .values(version=db_models.QuotaGroup.version + 1)
        )
        # Deliberately no expire of `group` here: session.execute(update(...)) defaults to
        # synchronize_session="auto", so the ORM has already written the new version into the
        # in-session object, and both of the caller's exits expire it regardless.
        return result.rowcount == 1

    @staticmethod
    def _park(
        *,
        session: orm.Session,
        execution: bts.ExecutionNode,
        group_id: str,
        group_name: str,
    ) -> "ParkOutcome":
        """Take the node off the launch path and record its place in line.

        No version bump: nothing was taken, so nobody else's read went stale.

        The status write is conditional, and that condition is the whole of the protection
        against parking a node somebody else has already decided about. UNINITIALIZED is a
        trapdoor -- the queued sweep does not select it, so only a promotion re-opens it --
        and both writers that race this gate end the possibility of a promotion:

        | Racing writer | What it commits | What an unconditional park would cost |
        | --- | --- | --- |
        | cancel, via `quota/cancel_requeue.py` | the run's `desired_state=TERMINATED`, and its parked nodes back to QUEUED | The node never reaches the sweep's cancel check (`orchestrator_sql.py:613`), so it stays UNINITIALIZED and its run never goes terminal |
        | deleting a group, via `quota/promotion.py` | the group row, and its waiters back to QUEUED | The node waits on a group that no longer exists, and no member can ever complete to wake it |

        Both conditions ride in the `WHERE` of the one `UPDATE` rather than in a `SELECT`
        this transaction runs first, because a read would not see the race it is looking
        for: under MySQL's REPEATABLE READ every read in this transaction is served from a
        snapshot taken before the racing commit, while an `UPDATE` matches against the
        latest committed rows and blocks on a row the racer still holds.

        Args:
            session: The orchestrator's session.
            execution: The node to park.
            group_id: The group it is waiting for.
            group_name: That group's name, for the log line.

        Returns:
            Whether the park was written at all, and -- when it was -- whether the node was
            already WAITING, which makes it a re-park: it had been promoted and did not get
            back in. The claim write is the only place that knows, because it is the one
            lookup of the existing row, so the answer is passed back up rather than bought
            with a second query.
        """
        written = _claim_impl(
            session=session,
            execution_node_id=execution.id,
            group_id=group_id,
            state=db_models.ClaimState.WAITING,
        )
        moved = session.execute(_park_status_update(node_id=execution.id))
        if moved.rowcount != 1:
            # The claim write above rides this rollback, which is the point: a node that was
            # not parked must not be left holding a place in the promotion queue either.
            session.rollback()
            _logger.warning(
                f"Quota park abandoned node={execution.id} group={group_name}: the node is "
                "no longer QUEUED, or its run has been cancelled, since the sweep read it"
            )
            return ParkOutcome(parked=False, was_waiting=False)
        # The park's `UPDATE` cannot fire the ORM hook that maintains the status history,
        # so the park records itself here. Left out, the status-transition metric bills the
        # whole quota wait to whatever state preceded the park.
        #
        # The re-read is locked on purpose: `extra_data` in this session is the sweep's
        # REPEATABLE READ snapshot, so a plain refresh would write back a stale document
        # and drop what another writer has committed since. The `UPDATE` above already
        # holds this row's lock, so this waits for nobody.
        session.refresh(execution, attribute_names=["extra_data"], with_for_update=True)
        # Writes the value the `UPDATE` already wrote -- the assignment is what fires the
        # hook. Delete it and `TestTheParkInStatusHistory` goes red.
        execution.container_execution_status = (
            bts.ContainerExecutionStatus.UNINITIALIZED
        )
        session.commit()
        was_waiting = written.previous_state is db_models.ClaimState.WAITING
        _logger.info(
            f"Quota {'re-park' if was_waiting else 'park'} node={execution.id} "
            f"group={group_name} waiting_since={_iso(written.waiting_since)}"
        )
        return ParkOutcome(parked=True, was_waiting=was_waiting)


class ParkOutcome(NamedTuple):
    """How a park attempt ended.

    `parked` is False only when the conditional status write matched no row, which means the
    node stopped being this gate's to move: it was cancelled, or un-parked by a group delete,
    after the sweep picked it up. Nothing was written in that case, not even the claim.
    """

    parked: bool
    was_waiting: bool


def _park_status_update(
    *,
    node_id: str,
) -> sql.Update:
    """Build the one statement that moves a node to UNINITIALIZED, if it is still parkable.

    Three conditions, all in the `WHERE` so the database evaluates them against the latest
    committed rows at write time:

    - the node is still QUEUED -- a promotion or a cancel that already moved it wins;
    - the node itself is not flagged for termination;
    - the run it belongs to is not flagged for termination.

    The last two are `NOT EXISTS` rather than a comparison on `extra_data` so that a row with
    no `extra_data` at all still parks: `JSON_EXTRACT(NULL, ...) != 'TERMINATED'` is NULL,
    which would match nothing and quietly stop parking every ordinary node.

    Together they are the same question `orchestrator_sql.py:606` asks before it reaches this
    gate -- asked again at the moment of the write, because between those two points a cancel
    can commit.

    Args:
        node_id: The node to park.

    Returns:
        The conditional UPDATE. A rowcount of 1 means the park is this transaction's to
        commit; 0 means somebody else decided this node's fate first.
    """
    terminated = "TERMINATED"
    run_is_cancelled = sql.exists(
        sql.select(bts.PipelineRun.id)
        .join(
            bts.ExecutionToAncestorExecutionLink,
            bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
            == bts.PipelineRun.root_execution_id,
        )
        .where(
            bts.ExecutionToAncestorExecutionLink.execution_id == node_id,
            bts.PipelineRun.extra_data["desired_state"].as_string() == terminated,
        )
    )
    # A predicate on the target row rather than an `EXISTS` over the same table: MySQL rejects
    # a subquery that reads the table an UPDATE is writing with error 1093, "You can't specify
    # target table 'execution_node' for update in FROM clause". SQLite accepts it, so this only
    # shows up against a real MySQL -- `tests/quota/test_quota_concurrency_mysql.py` is what
    # caught it. `is_distinct_from` keeps the NULL handling the `EXISTS` had: a node with no
    # `extra_data` at all is not cancelled, and must still park.
    node_is_not_cancelled = (
        bts.ExecutionNode.extra_data["desired_state"]
        .as_string()
        .is_distinct_from(terminated)
    )
    return (
        sql.update(bts.ExecutionNode)
        .where(
            bts.ExecutionNode.id == node_id,
            # Also what makes the rowcount trustworthy on MySQL, which reports changed rows
            # rather than matched ones: a row that matches this always changes.
            bts.ExecutionNode.container_execution_status
            == bts.ContainerExecutionStatus.QUEUED,
            node_is_not_cancelled,
            ~run_is_cancelled,
        )
        .values(container_execution_status=bts.ContainerExecutionStatus.UNINITIALIZED)
        # The caller commits or rolls back immediately, and both expire the node, so there is
        # nothing to gain from having the ORM synchronise the in-session object first.
        .execution_options(synchronize_session=False)
    )


def _record_wait(
    *,
    waiting_since: datetime.datetime | None,
    group_name: str,
) -> None:
    """Close the wait interval for a node that has just been admitted.

    The one place both endpoints are in hand, which is why the emit is here rather than in
    `quota/observability/`: `waiting_since` is the claim's `created_at` and it is already read
    for the log line, so the histogram costs a subtraction and no query.

    A node admitted on its first pass never waited and is skipped rather than recorded as
    zero. This distribution is meant to answer "is this capacity set correctly", and at a
    healthy capacity most admissions are first-pass, so counting them would bury every real
    queue time under a spike at zero.

    Args:
        waiting_since: The claim's `created_at`, or None for a node claiming for the first
            time.
        group_name: The group the node was waiting on.
    """
    if waiting_since is None:
        return
    if waiting_since.tzinfo is None:
        # Both engines hand back a naive datetime for a column written as UTC.
        waiting_since = waiting_since.replace(tzinfo=datetime.timezone.utc)
    quota_metrics.record(
        histogram=quota_metrics.duration_wait,
        # Floored: a clock adjustment between the insert and now must not record a negative
        # duration, which the SDK would drop and which reads as a lost sample.
        seconds=max(0.0, (db_utils.utc_now() - waiting_since).total_seconds()),
        quota_group=group_name,
    )


def _iso(
    moment: datetime.datetime | None,
) -> str:
    """Render a claim's timestamp for a log line, or say there was not one.

    Positional rather than keyword-only: it is a formatting detail used inside f-strings on
    this module's three hot log lines, where a keyword would be noise.

    Args:
        moment: The timestamp, or None for a node with no previous claim.

    Returns:
        The ISO-8601 form, or `none` for a node claiming for the first time.
    """
    return moment.isoformat() if moment is not None else "none"


class ClaimWrite(NamedTuple):
    """What `_claim_impl` found on the row before it wrote to it.

    Both fields are None for a node claiming for the first time. `waiting_since` is the
    claim's insert-only `created_at`, which is the node's place in the promotion queue rather
    than the time of its most recent rejection -- so it is the one number worth logging at
    every admission and every park.
    """

    previous_state: db_models.ClaimState | None
    waiting_since: datetime.datetime | None


def _claim_impl(
    *,
    session: orm.Session,
    execution_node_id: str,
    group_id: str,
    state: db_models.ClaimState,
) -> ClaimWrite:
    """Insert or update the node's single claim row.

    Written as select-then-write rather than a dialect upsert: SQLite and MySQL spell
    `ON CONFLICT` differently, and both deployments are live. The UNIQUE constraint on
    `execution_node_id` is still the backstop if two writers ever race here.

    Updating in place rather than deleting and re-inserting is load-bearing. `created_at` is
    insert-only, and it is the node's position in the promotion queue -- a re-parked node that
    lost a race must keep the place it has been holding, or a busy group would starve its
    oldest waiter indefinitely.

    The same in-place update is what makes a DONE row a problem: `uq_quota_group_claim_node`
    allows one row per node ever, so re-claiming would overwrite that node's ledger entry.
    It should be unreachable -- a node with a DONE claim has already ended, and the sweep that
    reaches this gate does not select ended nodes -- so it is recorded rather than refused.
    Raising here would fail a launch to protect bookkeeping.

    Args:
        session: The caller's session. Not committed here.
        execution_node_id: The node the claim belongs to.
        group_id: The group being claimed.
        state: The state to record.

    Returns:
        What was there before this call. The caller uses it to tell a first park from a
        re-park, and to log the node's place in line, without a second lookup.
    """
    existing = claims.find_claim(session=session, execution_node_id=execution_node_id)
    # One expression for the insert and the update below. A claim is parked exactly when it
    # is WAITING, so the column is derived from the state being written rather than tracked
    # alongside it and allowed to disagree. This is also the assignment that un-freezes
    # `updated_at` on a re-park -- see the column's comment.
    parked_at = db_utils.utc_now() if state is db_models.ClaimState.WAITING else None
    if existing is None:
        session.add(
            db_models.QuotaGroupClaim(
                quota_group_id=group_id,
                execution_node_id=execution_node_id,
                state=state,
                parked_at=parked_at,
            )
        )
        return ClaimWrite(previous_state=None, waiting_since=None)
    if existing.state is db_models.ClaimState.DONE:
        # Leaves a trace on the row it is about to overwrite: this is the only way a
        # terminal claim moves, and it means something upstream re-ran an ended node.
        reclaims = list(existing.extra_data.get("reclaimed_from_done", []))
        reclaims.append(db_utils.utc_now().isoformat())
        existing.extra_data["reclaimed_from_done"] = reclaims
        _logger.warning(
            f"Quota re-claim of a DONE claim node={execution_node_id} "
            f"group_id={group_id} state={state.value}; ledger entry overwritten"
        )
    written = ClaimWrite(
        previous_state=existing.state, waiting_since=existing.created_at
    )
    # A node that changed groups replaces its claim rather than holding two.
    existing.quota_group_id = group_id
    existing.state = state
    existing.parked_at = parked_at
    return written
