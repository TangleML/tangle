"""Unit tests for the quota sink — the first sink on the emission path that writes.

Four things are pinned, and the first is the one the whole design turns on:

1. **The group is resolved through the claim row, never through `intent.quota_group`.** A
   test puts a name nothing answers to on the intent and the promotion still lands, which is
   the property a name-based lookup would not have.
2. A node with no claim is an `ignore`, not a failure — that node was never admitted, so
   there is nothing to promote and retrying could not help.
3. The sink commits. `promotion.promote()` deliberately does not, so if the sink stopped
   committing the promotion would be silently discarded when the session closed.
4. **The finished node's claim is moved to `DONE`, and a claim already `DONE` is ignored.**
   That row is the group's ledger entry for that node, and the state is what keeps it out of
   every hot query. A redelivery must not report a second release.
"""

import collections.abc
import datetime

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)
from cloud_pipelines_backend.emissions.handlers.quota.sinks import quota_group
from cloud_pipelines_backend.quota import db_models

Status = bts.ContainerExecutionStatus


@pytest.fixture()
def session_factory() -> collections.abc.Generator[orm.sessionmaker, None, None]:
    """A factory over one shared in-memory database.

    Sharing it is load-bearing, and it is not this fixture that arranges it:
    `create_db_engine` selects `StaticPool` for the exact URI "sqlite://"
    (`database_ops.py:46`). Without that, the session the sink opens for itself would get a
    *different* in-memory database, find no claim, and every test here would pass by
    reporting the no-claim branch.
    """
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    # autoflush=False mirrors production; see tests/quota/conftest.py for why that is not
    # cosmetic.
    yield orm.sessionmaker(bind=engine, autoflush=False, autocommit=False)
    engine.dispose()


def _seed(
    *,
    session_factory: orm.sessionmaker,
    capacity: int = 1,
    group_name: str = "bq",
) -> tuple[str, str, str]:
    """One group at capacity: a member that just ended, and a parked waiter behind it.

    Returns:
        (group_id, finished_node_id, waiting_node_id)
    """
    with session_factory() as session:
        group = db_models.QuotaGroup(
            name=group_name, capacity=capacity, created_by="test@example.com"
        )
        session.add(group)
        session.commit()

        finished = bts.ExecutionNode(task_spec={})
        finished.id = "finished"
        # Already terminal: the emission row is written after the status change, so by the
        # time the sink runs the node no longer occupies a slot.
        finished.container_execution_status = Status.SUCCEEDED

        waiter = bts.ExecutionNode(task_spec={})
        waiter.id = "waiter"
        waiter.container_execution_status = Status.UNINITIALIZED
        session.add_all([finished, waiter])
        session.commit()

        session.add_all(
            [
                db_models.QuotaGroupClaim(
                    quota_group_id=group.id,
                    execution_node_id="finished",
                    state=db_models.ClaimState.ACTIVE,
                ),
                db_models.QuotaGroupClaim(
                    quota_group_id=group.id,
                    execution_node_id="waiter",
                    state=db_models.ClaimState.WAITING,
                ),
            ]
        )
        session.commit()
        return group.id, "finished", "waiter"


def _group_id_of(*, session: orm.Session, name: str = "bq") -> str:
    return (
        session.scalars(
            sqlalchemy.select(db_models.QuotaGroup).where(
                db_models.QuotaGroup.name == name
            )
        )
        .one()
        .id
    )


def _status_of(*, session_factory: orm.sessionmaker, node_id: str) -> Status:
    """Read back in a fresh session: `promote()` never commits, so this proves the sink did."""
    with session_factory() as session:
        node = session.get(bts.ExecutionNode, node_id)
        assert node is not None
        return node.container_execution_status


def _claim_state_of(
    *, session_factory: orm.sessionmaker, node_id: str
) -> db_models.ClaimState:
    with session_factory() as session:
        claim = session.scalars(
            sqlalchemy.select(db_models.QuotaGroupClaim).where(
                db_models.QuotaGroupClaim.execution_node_id == node_id
            )
        ).one()
        return claim.state


class TestThePromotionHappens:
    def test_the_waiter_is_requeued_and_its_claim_left_waiting(
        self, session_factory: orm.sessionmaker
    ) -> None:
        group_id, finished, waiter = _seed(session_factory=session_factory)
        sink = quota_group.QuotaGroupSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail == {
            "sink": "quota_group",
            "execution_node_id": finished,
            "quota_group_id": group_id,
            "promoted": 1,
        }
        assert (
            _status_of(session_factory=session_factory, node_id=waiter) is Status.QUEUED
        )
        # Promotion is advisory: it only re-queues. The claim stays WAITING because the node
        # still has to race at the gate, which is what actually grants the slot.
        assert (
            _claim_state_of(session_factory=session_factory, node_id=waiter)
            is db_models.ClaimState.WAITING
        )


class TestTheGroupComesFromTheClaimNotTheIntent:
    def test_a_stale_group_name_on_the_intent_does_not_misroute(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """The load-bearing test for Appendix E.

        The sink's `emit` is an abstract method that requires an intent, but we do not route on
        the intent's group name -- we use `execution_node_id` to find the node's claim instead,
        in case the name is wrong. Renaming is not possible through the API, so the realistic
        source of a wrong name is a group deleted and recreated under the same name while an
        emission for the old one was still in flight.
        """
        _, finished, waiter = _seed(session_factory=session_factory)

        outcome = quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(
                quota_group="a-name-nothing-answers-to"
            ),
            execution_node_id=finished,
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail["promoted"] == 1
        assert (
            _status_of(session_factory=session_factory, node_id=waiter) is Status.QUEUED
        )

    def test_the_node_id_selects_the_group_when_two_exist(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """Two groups, each with a waiter. Only the finished node's own group promotes."""
        group_id, finished, waiter = _seed(session_factory=session_factory)
        with session_factory() as session:
            other = db_models.QuotaGroup(
                name="other", capacity=1, created_by="test@example.com"
            )
            session.add(other)
            session.commit()
            bystander = bts.ExecutionNode(task_spec={})
            bystander.id = "bystander"
            bystander.container_execution_status = Status.UNINITIALIZED
            session.add(bystander)
            session.commit()
            session.add(
                db_models.QuotaGroupClaim(
                    quota_group_id=other.id,
                    execution_node_id="bystander",
                    state=db_models.ClaimState.WAITING,
                )
            )
            session.commit()

        outcome = quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        assert outcome.detail["quota_group_id"] == group_id
        assert (
            _status_of(session_factory=session_factory, node_id=waiter) is Status.QUEUED
        )
        # The other group's waiter is untouched: nothing in it ended.
        assert (
            _status_of(session_factory=session_factory, node_id="bystander")
            is Status.UNINITIALIZED
        )


class TestTheClaimIsReleased:
    def test_the_finished_node_s_claim_becomes_done(
        self, session_factory: orm.sessionmaker
    ) -> None:
        _, finished, _ = _seed(session_factory=session_factory)

        quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        assert (
            _claim_state_of(session_factory=session_factory, node_id=finished)
            is db_models.ClaimState.DONE
        )

    def test_the_row_is_kept_rather_than_deleted(
        self, session_factory: orm.sessionmaker
    ) -> None:
        # The table is a ledger: one row per node that ever used the group, and DONE is how a
        # finished node is told apart from a live one without deleting the record.
        _, finished, _ = _seed(session_factory=session_factory)

        quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        with session_factory() as session:
            assert (
                session.get(
                    db_models.QuotaGroupClaim,
                    (_group_id_of(session=session), finished),
                )
                is not None
            )

    def test_a_redelivery_is_ignored_rather_than_promoting_again(
        self, session_factory: orm.sessionmaker
    ) -> None:
        # Delivery is at-least-once. Promoting twice would be harmless -- promote() re-derives
        # free slots -- but it would report a release that never happened.
        group_id, finished, _ = _seed(session_factory=session_factory, capacity=2)
        sink = quota_group.QuotaGroupSink(session_factory=session_factory)
        sink.emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        second = sink.emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        assert second.status is handler_base.OutcomeStatus.IGNORE
        assert second.detail == {
            "sink": "quota_group",
            "execution_node_id": finished,
            "quota_group_id": group_id,
            "reason": "already_done",
        }

    def test_the_release_is_committed(self, session_factory: orm.sessionmaker) -> None:
        # Read back through a fresh session, like the promotion assertions: the state change
        # and the promotion share the sink's one commit, so either both land or neither does.
        _, finished, waiter = _seed(session_factory=session_factory)

        quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        with session_factory() as session:
            claim = session.scalars(
                sqlalchemy.select(db_models.QuotaGroupClaim).where(
                    db_models.QuotaGroupClaim.execution_node_id == finished
                )
            ).one()
            assert claim.state is db_models.ClaimState.DONE
        assert (
            _status_of(session_factory=session_factory, node_id=waiter) is Status.QUEUED
        )


class TestTheParkedTimestampIsCleared:
    """H11b. `parked_at` is how long a node has been waiting, so a terminal claim must not
    keep one -- read literally, a released claim would report a stall that grows forever.
    """

    def test_a_claim_released_from_waiting_loses_its_timestamp(
        self, session_factory: orm.sessionmaker
    ) -> None:
        # The case that actually produces a stale value: a node that is cancelled or fails
        # while still parked never gets admitted, so nothing on the admission path clears the
        # stamp. Its claim goes WAITING -> DONE straight through this sink.
        group_id, _, waiter = _seed(session_factory=session_factory)
        with session_factory() as session:
            claim = session.get(db_models.QuotaGroupClaim, (group_id, waiter))
            claim.parked_at = datetime.datetime(
                2024, 1, 1, tzinfo=datetime.timezone.utc
            )
            node = session.get(bts.ExecutionNode, waiter)
            node.container_execution_status = Status.FAILED
            session.commit()

        quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=waiter,
        )

        with session_factory() as session:
            claim = session.get(db_models.QuotaGroupClaim, (group_id, waiter))
            assert claim.state is db_models.ClaimState.DONE
            assert claim.parked_at is None


class TestANodeWithNoClaim:
    def test_it_is_ignored_rather_than_failed(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """A node that named a group which does not exist launches ungated and writes no
        claim, so it reaches the sink with nothing to promote. Reporting failure would retry
        a delivery that can never succeed."""
        _seed(session_factory=session_factory)

        outcome = quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id="never-admitted",
        )

        assert outcome.status is handler_base.OutcomeStatus.IGNORE
        assert outcome.detail["reason"] == "no_claim"

    def test_it_promotes_nobody(self, session_factory: orm.sessionmaker) -> None:
        _, _, waiter = _seed(session_factory=session_factory)
        quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id="never-admitted",
        )
        assert (
            _status_of(session_factory=session_factory, node_id=waiter)
            is Status.UNINITIALIZED
        )


class TestAFullGroup:
    def test_promotes_nobody_and_still_reports_success(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """The delivery worked; there was simply no room. That is a success reporting zero,
        not a failure, or the row would be redelivered until the lease gave up."""
        _, finished, waiter = _seed(session_factory=session_factory, capacity=1)
        with session_factory() as session:
            # A second member still running, so the freed slot is immediately re-occupied.
            other = bts.ExecutionNode(task_spec={})
            other.id = "still-running"
            other.container_execution_status = Status.RUNNING
            session.add(other)
            session.commit()
            claim = session.scalars(
                sqlalchemy.select(db_models.QuotaGroupClaim).where(
                    db_models.QuotaGroupClaim.execution_node_id == "waiter"
                )
            ).one()
            session.add(
                db_models.QuotaGroupClaim(
                    quota_group_id=claim.quota_group_id,
                    execution_node_id="still-running",
                    state=db_models.ClaimState.ACTIVE,
                )
            )
            session.commit()

        outcome = quota_group.QuotaGroupSink(session_factory=session_factory).emit(
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            execution_node_id=finished,
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail["promoted"] == 0
        assert (
            _status_of(session_factory=session_factory, node_id=waiter)
            is Status.UNINITIALIZED
        )


def _deadlocking_promote(*, session: orm.Session, group_id: str) -> int:
    """Stand in for `promotion.promote`: re-queue the waiter, then lose the connection.

    The write before the raise is what lets the test tell a rollback from a no-op -- without
    it, "the waiter is still parked" would pass on a sink that never rolled back at all.
    """
    session.execute(
        sqlalchemy.update(bts.ExecutionNode)
        .where(bts.ExecutionNode.id == "waiter")
        .values(container_execution_status=Status.QUEUED)
    )
    raise sqlalchemy.exc.OperationalError(
        "UPDATE quota_group SET version", {}, Exception("deadlock found")
    )


def _exploding_promote(*, session: orm.Session, group_id: str) -> int:
    """Stand in for a bug, not a deadlock. Redelivering this forever moves it into the queue."""
    raise ValueError("promote() has a bug")


class TestATransientDatabaseFailure:
    """The delivery is left unsettled, because promotion is edge-triggered.

    A `failed` verdict settles the message, and this completion produces exactly one promotion
    edge -- settling spends it on nothing and strands the cohort until somebody notices.
    """

    def test_it_raises_delivery_incomplete_and_rolls_the_promotion_back(
        self, session_factory: orm.sessionmaker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, finished, waiter = _seed(session_factory=session_factory)
        monkeypatch.setattr(quota_group.promotion, "promote", _deadlocking_promote)

        with pytest.raises(handler_base.DeliveryIncomplete, match=finished):
            quota_group.QuotaGroupSink(session_factory=session_factory).emit(
                intent=quota_annotations.QuotaIntent(quota_group="bq"),
                execution_node_id=finished,
            )

        assert (
            _status_of(session_factory=session_factory, node_id=waiter)
            is Status.UNINITIALIZED
        )
        # Still ACTIVE, so the redelivery redoes the whole block rather than resuming it.
        assert (
            _claim_state_of(session_factory=session_factory, node_id=finished)
            is db_models.ClaimState.ACTIVE
        )

    def test_a_programming_error_is_left_to_settle_failed(
        self, session_factory: orm.sessionmaker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The `except` is narrow on purpose: only a transient failure earns a redelivery."""
        _, finished, _ = _seed(session_factory=session_factory)
        monkeypatch.setattr(quota_group.promotion, "promote", _exploding_promote)

        with pytest.raises(ValueError, match="has a bug"):
            quota_group.QuotaGroupSink(session_factory=session_factory).emit(
                intent=quota_annotations.QuotaIntent(quota_group="bq"),
                execution_node_id=finished,
            )
