"""Unit tests for quota.occupancy.

The sharp edge is `QUEUED`. Every other status settles the question on its own — a container
either exists or it does not — but two nodes sit at QUEUED for opposite reasons, and only the
claim's `state` tells them apart. Getting that backwards would let a promotion wave re-fill
the group it was meant to drain, so it is pinned here from both sides.
"""

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models, occupancy

Status = bts.ContainerExecutionStatus


def _make_group(
    *,
    session: orm.Session,
    name: str = "bq",
    capacity: int = 2,
) -> db_models.QuotaGroup:
    group = db_models.QuotaGroup(
        name=name, capacity=capacity, created_by="test@example.com"
    )
    session.add(group)
    session.commit()
    return group


def _make_claim(
    *,
    session: orm.Session,
    group: db_models.QuotaGroup,
    node_id: str,
    status: Status,
    state: db_models.ClaimState,
) -> None:
    """One node in one group, at a given node status and claim state.

    The pair is what occupancy reads, so the tests always set both together.
    """
    node = bts.ExecutionNode(task_spec={})
    node.id = node_id
    node.container_execution_status = status
    session.add(node)
    # Flushed before the claim, not merely added first. There is deliberately no ORM
    # relationship() between the two -- CASCADE is DB-level only -- so SQLAlchemy does not
    # know the claim depends on the node and is free to INSERT them in either order. Under
    # the production autoflush=False that loses a coin toss against the foreign key.
    session.flush()
    session.add(
        db_models.QuotaGroupClaim(
            quota_group_id=group.id, execution_node_id=node_id, state=state
        )
    )
    session.commit()


class TestTheQueuedDistinction:
    """QUEUED + ACTIVE occupies a slot; QUEUED + WAITING does not."""

    def test_queued_and_active_counts(self, session: orm.Session) -> None:
        # Won admission at the gate, not yet launched. The slot is spoken for.
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            node_id="node-1",
            status=Status.QUEUED,
            state=db_models.ClaimState.ACTIVE,
        )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 1

    def test_queued_and_waiting_does_not_count(self, session: orm.Session) -> None:
        # Just un-parked by a promotion; the gate has not run yet. If this counted, the
        # promotion would create the occupancy that turns its own waiter away.
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            node_id="node-1",
            status=Status.QUEUED,
            state=db_models.ClaimState.WAITING,
        )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 0

    def test_a_promotion_wave_does_not_fill_the_group_it_drains(
        self, session: orm.Session
    ) -> None:
        # The scenario the distinction exists for, end to end: capacity 2, one running
        # member, three waiters all un-parked to QUEUED at once. Occupancy must still read
        # 1, or none of the three could be admitted.
        group = _make_group(session=session, capacity=2)
        _make_claim(
            session=session,
            group=group,
            node_id="running",
            status=Status.RUNNING,
            state=db_models.ClaimState.ACTIVE,
        )
        for index in range(3):
            _make_claim(
                session=session,
                group=group,
                node_id=f"promoted-{index}",
                status=Status.QUEUED,
                state=db_models.ClaimState.WAITING,
            )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 1


class TestStatusesThatSettleItAlone:
    @pytest.mark.parametrize("status", list(occupancy.STATUSES_HOLDING_A_CONTAINER))
    def test_a_container_holds_a_slot_whatever_the_claim_says(
        self, session: orm.Session, status: Status
    ) -> None:
        # These are unreachable without having launched, and launching is unreachable
        # without having won, so the node's own status is proof enough. Pairing them with a
        # WAITING claim — which should never happen — must still count, because the load on
        # the downstream system is real either way.
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            node_id="node-1",
            status=status,
            state=db_models.ClaimState.WAITING,
        )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 1

    @pytest.mark.parametrize(
        "status",
        [
            Status.SUCCEEDED,
            Status.FAILED,
            Status.CANCELLED,
            Status.SKIPPED,
            Status.SYSTEM_ERROR,
            Status.INVALID,
        ],
    )
    def test_a_finished_node_holds_nothing_though_its_claim_stays_active(
        self, session: orm.Session, status: Status
    ) -> None:
        # There is no release path: the claim row stays ACTIVE for the life of the node.
        # It is inert precisely because this query reads the node, not the claim.
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            node_id="node-1",
            status=status,
            state=db_models.ClaimState.ACTIVE,
        )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 0

    def test_a_parked_node_holds_nothing(self, session: orm.Session) -> None:
        # UNINITIALIZED is the parked state. A node waiting for a slot is by definition not
        # using one.
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            node_id="node-1",
            status=Status.UNINITIALIZED,
            state=db_models.ClaimState.WAITING,
        )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 0

    def test_waiting_for_upstream_holds_nothing(self, session: orm.Session) -> None:
        # Not in the whitelist and not QUEUED: no container exists, so no slot is held.
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            node_id="node-1",
            status=Status.WAITING_FOR_UPSTREAM,
            state=db_models.ClaimState.ACTIVE,
        )
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 0


class TestScoping:
    def test_an_unclaimed_group_reads_zero(self, session: orm.Session) -> None:
        group = _make_group(session=session)
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 0

    def test_an_unknown_group_id_reads_zero_rather_than_raising(
        self, session: orm.Session
    ) -> None:
        # A group deleted between the gate's lookup and this read must not take the
        # orchestrator's launch path down.
        assert occupancy.count_occupancy(session=session, group_id="nope") == 0

    def test_another_group_s_load_is_not_counted(self, session: orm.Session) -> None:
        mine = _make_group(session=session, name="mine")
        theirs = _make_group(session=session, name="theirs")
        _make_claim(
            session=session,
            group=theirs,
            node_id="node-1",
            status=Status.RUNNING,
            state=db_models.ClaimState.ACTIVE,
        )
        assert occupancy.count_occupancy(session=session, group_id=mine.id) == 0
        assert occupancy.count_occupancy(session=session, group_id=theirs.id) == 1

    def test_an_ungated_node_is_invisible(self, session: orm.Session) -> None:
        # No claim row, so the query never reaches it: the join starts from the claims. A
        # node whose group did not exist runs without consuming anyone's capacity.
        group = _make_group(session=session)
        node = bts.ExecutionNode(task_spec={})
        node.id = "ungated"
        node.container_execution_status = Status.RUNNING
        session.add(node)
        session.commit()
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 0

    def test_the_gate_sees_its_own_uncommitted_claim(
        self, session: orm.Session
    ) -> None:
        # The gate writes its claim and then re-reads occupancy inside one transaction, so
        # the read must include work that has been flushed but not committed.
        group = _make_group(session=session)
        node = bts.ExecutionNode(task_spec={})
        node.id = "node-1"
        node.container_execution_status = Status.QUEUED
        session.add(node)
        session.flush()  # no relationship(), so the FK needs the node inserted first
        session.add(
            db_models.QuotaGroupClaim(
                quota_group_id=group.id,
                execution_node_id="node-1",
                state=db_models.ClaimState.ACTIVE,
            )
        )
        session.flush()
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 1


class TestTheWhitelistItself:
    def test_the_whitelist_is_exactly_the_three_live_statuses(self) -> None:
        # A guard on the list, not a restatement of it: adding a status here changes what
        # every group's capacity means, so it should not happen by accident.
        assert occupancy.STATUSES_HOLDING_A_CONTAINER == (
            Status.PENDING,
            Status.RUNNING,
            Status.CANCELLING,
        )

    def test_no_terminal_status_is_in_the_whitelist(self) -> None:
        assert not (
            set(occupancy.STATUSES_HOLDING_A_CONTAINER) & bts.CONTAINER_STATUSES_ENDED
        )

    def test_queued_is_not_in_the_whitelist(self) -> None:
        # It is branch 2's job, conditional on the claim. Putting it here would count
        # waiters as occupants.
        assert Status.QUEUED not in occupancy.STATUSES_HOLDING_A_CONTAINER
