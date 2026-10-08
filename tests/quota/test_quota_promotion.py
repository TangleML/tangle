"""Unit tests for quota.promotion — un-parking.

Pinned here: promotion goes oldest-first by the claim's immutable `created_at`, the slot count
is clamped so a lowered capacity cannot produce `LIMIT -7`, and over-promotion self-corrects.

What used to live here and does not any more: the firing rule. Deciding *when* a completion
frees a slot moved onto the emissions path, so it is pinned in
tests/emissions/handlers/quota/ -- the rule itself in test_quota_annotations.py
(QuotaIntent.matches) and the promotion it triggers in sinks/test_quota_group.py.
"""

import datetime
from typing import Any

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.handlers.quota import annotations
from cloud_pipelines_backend.quota import db_models, interceptor, promotion

KEY = annotations.QUOTA_GROUP_KEY
Status = bts.ContainerExecutionStatus
EPOCH = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


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


def _make_node(
    *,
    session: orm.Session,
    node_id: str,
    group_name: str | None = "bq",
    status: Status = Status.QUEUED,
) -> bts.ExecutionNode:
    task_spec: dict[str, Any] = {}
    if group_name is not None:
        task_spec["annotations"] = {KEY: group_name}
    node = bts.ExecutionNode(task_spec=task_spec)
    node.id = node_id
    node.container_execution_status = status
    session.add(node)
    session.commit()
    return node


def _park(
    *,
    session: orm.Session,
    group_id: str,
    node_id: str,
    offset_seconds: int = 0,
) -> db_models.QuotaGroupClaim:
    """A node parked at UNINITIALIZED with a WAITING claim of a controlled age.

    `created_at` is an insert-time Python default, so claims made in the same millisecond
    would tie and the oldest-first assertion would be testing the tie-break rather than the
    ordering. Setting it explicitly is what makes "oldest" mean something.
    """
    node = _make_node(session=session, node_id=node_id, status=Status.UNINITIALIZED)
    claim = db_models.QuotaGroupClaim(
        quota_group_id=group_id,
        execution_node_id=node.id,
        state=db_models.ClaimState.WAITING,
    )
    session.add(claim)
    session.commit()
    claim.created_at = EPOCH + datetime.timedelta(seconds=offset_seconds)
    session.commit()
    return claim


def _status_of(*, session: orm.Session, node_id: str) -> Status:
    session.expire_all()
    node = session.get(bts.ExecutionNode, node_id)
    assert node is not None
    return node.container_execution_status


def _claim_of(*, session: orm.Session, node_id: str) -> db_models.QuotaGroupClaim:
    claim = session.scalars(
        sql.select(db_models.QuotaGroupClaim).where(
            db_models.QuotaGroupClaim.execution_node_id == node_id
        )
    ).one_or_none()
    assert claim is not None
    return claim


class TestFreeSlots:
    """`max(0, capacity - occupancy)` — and the clamp is reachable, not defensive."""

    def test_an_empty_group_offers_every_slot(self, session: orm.Session) -> None:
        group = _make_group(session=session, capacity=3)
        assert promotion.free_slots(session=session, group=group) == 3

    def test_occupancy_is_subtracted(self, session: orm.Session) -> None:
        group = _make_group(session=session, capacity=3)
        node = _make_node(session=session, node_id="running", status=Status.RUNNING)
        session.add(
            db_models.QuotaGroupClaim(
                quota_group_id=group.id,
                execution_node_id=node.id,
                state=db_models.ClaimState.ACTIVE,
            )
        )
        session.commit()
        assert promotion.free_slots(session=session, group=group) == 2

    def test_capacity_lowered_below_the_live_count_clamps_to_zero(
        self, session: orm.Session
    ) -> None:
        """The negative case is an ordinary operator action, not a corrupt state.

        Without the clamp this reaches MySQL as `LIMIT -7`, which is a syntax error rather
        than an empty result.
        """
        group = _make_group(session=session, capacity=3)
        for i in range(3):
            node = _make_node(
                session=session, node_id=f"running{i}", status=Status.RUNNING
            )
            session.add(
                db_models.QuotaGroupClaim(
                    quota_group_id=group.id,
                    execution_node_id=node.id,
                    state=db_models.ClaimState.ACTIVE,
                )
            )
        session.commit()
        group.capacity = 1
        session.commit()
        assert group.capacity - 3 < 0, "the guard is pointless if this is not negative"
        assert promotion.free_slots(session=session, group=group) == 0


class TestPromoteOldestFirst:
    def test_the_oldest_claim_is_promoted_first(self, session: orm.Session) -> None:
        group = _make_group(session=session, capacity=1)
        _park(
            session=session,
            group_id=group.id,
            node_id="newest",
            offset_seconds=300,
        )
        _park(
            session=session,
            group_id=group.id,
            node_id="oldest",
            offset_seconds=0,
        )
        _park(
            session=session,
            group_id=group.id,
            node_id="middle",
            offset_seconds=60,
        )

        assert promotion.promote(session=session, group_id=group.id) == 1
        session.commit()

        assert _status_of(session=session, node_id="oldest") is Status.QUEUED
        assert _status_of(session=session, node_id="middle") is Status.UNINITIALIZED
        assert _status_of(session=session, node_id="newest") is Status.UNINITIALIZED

    def test_same_second_claims_are_broken_by_node_id(self) -> None:
        """FIFO must stay a total order when the clock cannot separate two claims.

        `created_at` is a whole-second DATETIME on MySQL, so a burst of claims arriving in the
        same second is the ordinary case rather than the rare one, and without a tiebreak the
        winner is whatever the storage engine hands back first.

        This asserts the SQL rather than the outcome, and that is deliberate. A behavioural
        version of this test passes with the tiebreak deleted: the tests run on SQLite, which
        satisfies the ORDER BY from `ix_quota_group_claim_state_created` and so walks the
        claims in `(created_at, execution_node_id)` order whether or not the query asked for
        it. Only the emitted clause can tell the two versions apart.
        """
        compiled = str(promotion.parked_nodes_query(group_id="g", slots=3))
        assert (
            "ORDER BY quota_group_claim.created_at ASC, quota_group_claim.execution_node_id ASC"
            in compiled
        )

    def test_only_as_many_as_there_are_slots(self, session: orm.Session) -> None:
        group = _make_group(session=session, capacity=2)
        for i in range(5):
            _park(
                session=session,
                group_id=group.id,
                node_id=f"n{i}",
                offset_seconds=i,
            )
        assert promotion.promote(session=session, group_id=group.id) == 2

    def test_the_claim_is_left_waiting(self, session: orm.Session) -> None:
        """Promotion un-parks; it does not admit. Only the gate writes ACTIVE."""
        group = _make_group(session=session, capacity=1)
        _park(session=session, group_id=group.id, node_id="n0")
        promotion.promote(session=session, group_id=group.id)
        session.commit()
        assert _claim_of(session=session, node_id="n0").state is (
            db_models.ClaimState.WAITING
        )

    def test_a_deleted_group_promotes_nobody(self, session: orm.Session) -> None:
        assert promotion.promote(session=session, group_id="gone") == 0


class TestPromotionIsIdempotent:
    def test_the_same_node_is_never_promoted_twice(self, session: orm.Session) -> None:
        """An already-QUEUED node is not selected, so the budget is not spent on it.

        This is the filter that is easy to omit: the claim is still WAITING after a
        promotion, so selecting on the claim alone would re-promote the same node forever
        and never reach the one still parked behind it.

                             node status        claim state     counted as occupied?
        park                 UNINITIALIZED      WAITING         no
        after promote        QUEUED             WAITING         no   <- the gap
        after winning gate   QUEUED             ACTIVE          yes

        The middle row is why the second call still has a slot to spend; the status filter
        is why it spends it on the next waiter rather than on this one again.
        """
        group = _make_group(session=session, capacity=1)
        _park(
            session=session,
            group_id=group.id,
            node_id="first",
            offset_seconds=0,
        )
        _park(
            session=session,
            group_id=group.id,
            node_id="second",
            offset_seconds=60,
        )

        assert promotion.promote(session=session, group_id=group.id) == 1
        session.commit()
        assert promotion.promote(session=session, group_id=group.id) == 1
        session.commit()

        # The second call moved the *next* waiter, not the first one again.
        assert _status_of(session=session, node_id="first") is Status.QUEUED
        assert _status_of(session=session, node_id="second") is Status.QUEUED

    def test_a_second_call_over_promotes_because_a_waiter_holds_no_occupancy(
        self, session: orm.Session
    ) -> None:
        """Promotion is idempotent per node, not per group — by design.

        A promoted node is QUEUED with a WAITING claim, which occupancy branch 2 does not
        count, so its slot still reads as free and the next call hands the same slot out
        again. Both losers re-park with `created_at` intact. Counting them instead would let
        a promotion wave re-fill the group it was meant to drain.

        Pinned because it looks like a bug in a log and is not one: capacity 1, two nodes
        un-parked.
        """
        group = _make_group(session=session, capacity=1)
        _park(
            session=session,
            group_id=group.id,
            node_id="first",
            offset_seconds=0,
        )
        _park(
            session=session,
            group_id=group.id,
            node_id="second",
            offset_seconds=60,
        )

        promotion.promote(session=session, group_id=group.id)
        session.commit()
        assert (
            promotion.free_slots(session=session, group=group) == 1
        ), "a promoted waiter must not read as occupancy"
        promotion.promote(session=session, group_id=group.id)
        session.commit()

        queued = [
            n
            for n in ("first", "second")
            if _status_of(session=session, node_id=n) is Status.QUEUED
        ]
        assert queued == [
            "first",
            "second",
        ], "capacity 1, two un-parked: over-promotion"

    def test_over_promotion_self_corrects_because_created_at_survives(
        self, session: orm.Session
    ) -> None:
        """A promoted node that loses at the gate re-parks and keeps its place in line."""
        group = _make_group(session=session, capacity=1)
        claim = _park(
            session=session,
            group_id=group.id,
            node_id="loser",
            offset_seconds=0,
        )
        original_created_at = claim.created_at

        promotion.promote(session=session, group_id=group.id)
        session.commit()
        # It loses at the gate and re-parks.
        node = session.get(bts.ExecutionNode, "loser")
        assert node is not None
        node.container_execution_status = Status.UNINITIALIZED
        session.commit()

        assert _claim_of(session=session, node_id="loser").created_at == (
            original_created_at
        ), "the re-parked node lost its place in line"


class TestClaimAgeSurvivesReParking:
    """`created_at` is the node's place in line, so nothing may move it.

    This is what makes over-promotion safe to accept: a node that is un-parked, loses at the
    gate and re-parks has to come back to the *front* of the queue, not the back. Reset it and
    a busy group starves its oldest waiter indefinitely — a fairness bug that shows up as
    "that job never ran" weeks later, with nothing in the logs.
    """

    def test_a_re_park_keeps_created_at_and_moves_only_updated_at(
        self, session: orm.Session
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _park(session=session, group_id=group.id, node_id="n0")
        claim = _claim_of(session=session, node_id="n0")
        created_at, first_updated_at = claim.created_at, claim.updated_at

        # Won the slot: the shared claim write path flips the state in place.
        interceptor._claim_impl(
            session=session,
            execution_node_id="n0",
            group_id=group.id,
            state=db_models.ClaimState.ACTIVE,
        )
        session.commit()
        claim = _claim_of(session=session, node_id="n0")
        assert claim.created_at == created_at
        assert claim.updated_at > first_updated_at, "updated_at must track the write"

        # Lost a later race and re-parked.
        interceptor._claim_impl(
            session=session,
            execution_node_id="n0",
            group_id=group.id,
            state=db_models.ClaimState.WAITING,
        )
        session.commit()
        claim = _claim_of(session=session, node_id="n0")
        assert (
            claim.created_at == created_at
        ), "the re-parked node lost its place in line"
        assert claim.state is db_models.ClaimState.WAITING

    def test_a_re_park_is_a_write_and_updated_at_moves(
        self, session: orm.Session
    ) -> None:
        """Parking an already-parked node writes the row, so `updated_at` advances.

        This test asserted the opposite until `parked_at` arrived, and the reversal is the
        point of the column rather than a side effect of it. WAITING over WAITING and the
        same group id over itself leaves nothing dirty, SQLAlchemy issues no UPDATE, and
        `onupdate` is a hook on a write that never happens -- so the row used to stand still
        through an entire park cycle. `quota/interceptor.py:379` now assigns a fresh
        timestamp, which is always a changed value, so the UPDATE is always emitted.

        Strictly greater rather than a frozen clock: `updated_at`'s `onupdate` captured
        `utc_now` when the model class was defined (`quota/db_models.py:185`), so it cannot
        be monkeypatched out from under the column the way `parked_at` can. What matters
        here is that it moved at all.
        """
        group = _make_group(session=session, capacity=1)
        _park(session=session, group_id=group.id, node_id="n0")
        before = _claim_of(session=session, node_id="n0").updated_at

        interceptor._claim_impl(
            session=session,
            execution_node_id="n0",
            group_id=group.id,
            state=db_models.ClaimState.WAITING,
        )
        session.commit()
        assert _claim_of(session=session, node_id="n0").updated_at > before
