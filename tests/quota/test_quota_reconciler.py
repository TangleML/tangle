"""Unit tests for quota.reconciler — the level-triggered backstop behind promotion.

Two properties carry this file, and the second is the one that makes a backstop safe to run
on a timer at all:

1. **A cohort stranded by a lost promotion edge is un-parked.** That is the failure the
   reconciler exists for: nothing else looks at a parked node, because it sits at
   UNINITIALIZED and the orchestrator's queued sweep does not select it.
2. **A second pass over the same fleet writes nothing and says nothing.** `promote()`
   re-derives free slots rather than replaying a delta, so idempotence is a property of the
   query rather than of a guard someone has to remember.

The tests that would fail loudest if the selection predicate were weakened are the quiet
ones: a saturated group and a node already mid-promotion must not be touched. Both are
groups that a naive `waiters > 0` backstop would promote in every minute forever.
"""

import collections.abc
import contextlib
import datetime
import logging
import typing

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.quota import db_models, reconciler
from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.utils import db as db_utils

Status = bts.ContainerExecutionStatus


@pytest.fixture()
def session_factory() -> collections.abc.Generator[orm.sessionmaker, None, None]:
    """A factory over one shared in-memory database.

    Sharing it is load-bearing and it is `create_db_engine` that arranges it, by selecting
    `StaticPool` for the exact URI "sqlite://" (`database_ops.py:46`). Without that, the
    session the reconciler opens for its scan would get a *different* in-memory database,
    find no stalled groups, and every test here would pass by reconciling an empty fleet.
    """
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    # autoflush=False mirrors production; see tests/quota/conftest.py for why that is not
    # cosmetic.
    yield orm.sessionmaker(bind=engine, autoflush=False, autocommit=False)
    engine.dispose()


def _make_group(
    *,
    session_factory: orm.sessionmaker,
    name: str = "bq",
    capacity: int = 1,
) -> str:
    with session_factory() as session:
        group = db_models.QuotaGroup(
            name=name, capacity=capacity, created_by="test@example.com"
        )
        session.add(group)
        session.commit()
        return group.id


def _park(
    *,
    session_factory: orm.sessionmaker,
    group_id: str,
    node_id: str,
    stalled_for_seconds: float,
) -> None:
    """A node parked at UNINITIALIZED with a WAITING claim of a controlled stall age.

    `parked_at` is backdated rather than the threshold being lowered, which is the difference
    between pinning the real derivation and proving the code fires after whatever number it
    was handed.
    """
    with session_factory() as session:
        node = bts.ExecutionNode(task_spec={})
        node.id = node_id
        node.container_execution_status = Status.UNINITIALIZED
        session.add(node)
        session.commit()
        session.add(
            db_models.QuotaGroupClaim(
                quota_group_id=group_id,
                execution_node_id=node_id,
                state=db_models.ClaimState.WAITING,
                parked_at=db_utils.utc_now()
                - datetime.timedelta(seconds=stalled_for_seconds),
            )
        )
        session.commit()


def _occupy(
    *,
    session_factory: orm.sessionmaker,
    group_id: str,
    node_id: str,
) -> None:
    """A node actually running in the group — the thing that makes it saturated."""
    with session_factory() as session:
        node = bts.ExecutionNode(task_spec={})
        node.id = node_id
        node.container_execution_status = Status.RUNNING
        session.add(node)
        session.commit()
        session.add(
            db_models.QuotaGroupClaim(
                quota_group_id=group_id,
                execution_node_id=node_id,
                state=db_models.ClaimState.ACTIVE,
            )
        )
        session.commit()


def _status_of(*, session_factory: orm.sessionmaker, node_id: str) -> Status:
    """Read back in a fresh session: it is the reconciler that must have committed."""
    with session_factory() as session:
        node = session.get(bts.ExecutionNode, node_id)
        assert node is not None
        return node.container_execution_status


def _stalled(*, session_factory: orm.sessionmaker) -> reconciler.PromotionReconciler:
    return reconciler.PromotionReconciler(session_factory=session_factory)


@pytest.fixture()
def promote_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every group `reconcile()` opens a transaction for, and still do the work.

    Needed because the return count cannot see what the free-slot predicate buys:
    `promote()` clamps to free slots, so a group selected pointlessly returns 0 and reads
    exactly like a group that was never selected. What it costs is a `FOR UPDATE` row lock
    on the gate's hot path, and the only way to assert on that is to count the calls.
    """
    calls: list[str] = []
    real_promote = reconciler.promotion.promote

    def _recording(*, session: orm.Session, group_id: str) -> int:
        calls.append(group_id)
        return real_promote(session=session, group_id=group_id)

    monkeypatch.setattr(reconciler.promotion, "promote", _recording)
    return calls


@pytest.fixture()
def promoting_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, quota_metrics.PromotionTrigger]]:
    """Record the `(quota_group, trigger)` each pass is observed under, and still observe it.

    The label cannot be read off the reconciler's return value or its log line, and the OTel
    probe lives behind a conftest in `tests/quota/observability/`. A spy on the observer is
    the smallest thing that sees what the counter will be labelled with.

    Returns:
        One entry per observed pass, in order.
    """
    calls: list[tuple[str, quota_metrics.PromotionTrigger]] = []
    real_promoting = reconciler.promotion_observer.promoting

    @contextlib.contextmanager
    def _recording(
        *, quota_group: str, trigger: quota_metrics.PromotionTrigger
    ) -> collections.abc.Iterator[typing.Any]:
        calls.append((quota_group, trigger))
        with real_promoting(quota_group=quota_group, trigger=trigger) as pass_:
            yield pass_

    monkeypatch.setattr(reconciler.promotion_observer, "promoting", _recording)
    return calls


# One second past the threshold, so a test that says "stalled" is stalled by the real
# derivation and not by a number chosen to be comfortably large.
_PAST_THRESHOLD = reconciler._MIN_STALL_SECONDS + 1
_WITHIN_THRESHOLD = reconciler._MIN_STALL_SECONDS - 1


class TestTheBackstopFires:
    """The lost-edge case, which is the only reason this class exists."""

    def test_a_stranded_cohort_is_promoted(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """A free slot and a long-parked waiter is a lost edge by construction.

        Nothing here completes a member: the group simply has room and a node that has been
        waiting longer than any redelivery could take.
        """
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="stranded",
            stalled_for_seconds=_PAST_THRESHOLD,
        )

        assert _stalled(session_factory=session_factory).reconcile() == 1
        assert (
            _status_of(session_factory=session_factory, node_id="stranded")
            is Status.QUEUED
        )

    def test_it_promotes_only_as_many_as_the_group_can_afford(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """Capacity is still the cap. A backstop that ignored it would be the outage."""
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _occupy(
            session_factory=session_factory,
            group_id=group_id,
            node_id="running",
        )
        for i in range(3):
            _park(
                session_factory=session_factory,
                group_id=group_id,
                node_id=f"waiter{i}",
                stalled_for_seconds=_PAST_THRESHOLD,
            )

        assert _stalled(session_factory=session_factory).reconcile() == 1

    def test_a_second_tick_is_a_no_op(self, session_factory: orm.sessionmaker) -> None:
        """Idempotent by construction: the first pass cleared the condition it selected on."""
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="stranded",
            stalled_for_seconds=_PAST_THRESHOLD,
        )
        subject = _stalled(session_factory=session_factory)

        assert subject.reconcile() == 1
        assert subject.reconcile() == 0

    def test_every_stalled_group_is_corrected_not_just_the_first(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """One pass, every group. A backstop that fixed one per minute would fall behind."""
        for name in ("bq", "spark"):
            group_id = _make_group(
                session_factory=session_factory, name=name, capacity=1
            )
            _park(
                session_factory=session_factory,
                group_id=group_id,
                node_id=f"{name}-waiter",
                stalled_for_seconds=_PAST_THRESHOLD,
            )

        assert _stalled(session_factory=session_factory).reconcile() == 2

    def test_a_group_is_corrected_once_however_many_waiters_it_has(
        self, session_factory: orm.sessionmaker, promote_calls: list[str]
    ) -> None:
        """Three stalled waiters are one group to promote in, not three.

        Without the DISTINCT the count still reads 2, because `promote()` clamps to free
        slots on its first call and finds nothing on the next two. What it costs is three
        `FOR UPDATE` row locks where one was needed, so the load-bearing assertion is on the
        calls rather than on the count.
        """
        group_id = _make_group(session_factory=session_factory, capacity=2)
        for i in range(3):
            _park(
                session_factory=session_factory,
                group_id=group_id,
                node_id=f"waiter{i}",
                stalled_for_seconds=_PAST_THRESHOLD,
            )

        assert _stalled(session_factory=session_factory).reconcile() == 2
        assert promote_calls == [group_id]
        # Oldest-first is by the claim's `created_at`, not by how long it has been parked,
        # so the one left behind is the one inserted last.
        assert (
            _status_of(session_factory=session_factory, node_id="waiter2")
            is Status.UNINITIALIZED
        )


class TestTheBackstopStaysQuiet:
    """The cases a naive `waiters > 0` backstop would promote in, every minute, forever."""

    def test_a_recently_parked_node_is_left_alone(
        self, session_factory: orm.sessionmaker, promote_calls: list[str]
    ) -> None:
        """Inside the threshold a redelivery may still be coming; racing it is the bug."""
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="fresh",
            stalled_for_seconds=_WITHIN_THRESHOLD,
        )

        assert _stalled(session_factory=session_factory).reconcile() == 0
        assert promote_calls == []
        assert (
            _status_of(session_factory=session_factory, node_id="fresh")
            is Status.UNINITIALIZED
        )

    def test_a_saturated_group_is_never_selected(
        self, session_factory: orm.sessionmaker, promote_calls: list[str]
    ) -> None:
        """The ordinary state of a busy fleet, and it must cost no row lock.

        A long queue behind a full group is not a lost edge, it is a queue. Selecting it
        would take this group's `FOR UPDATE` lock on the gate's hot path once a minute to
        discover there was nothing to do.
        """
        group_id = _make_group(session_factory=session_factory, capacity=1)
        _occupy(
            session_factory=session_factory,
            group_id=group_id,
            node_id="running",
        )
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="queued-behind-it",
            stalled_for_seconds=_PAST_THRESHOLD * 10,
        )

        assert _stalled(session_factory=session_factory).reconcile() == 0
        assert promote_calls == [], "a saturated group cost a row lock for nothing"

    def test_a_node_already_mid_promotion_is_not_a_waiter(
        self, session_factory: orm.sessionmaker, promote_calls: list[str]
    ) -> None:
        """QUEUED + WAITING is a node `promote()` already un-parked, gate not yet run.

        Its claim still reads WAITING, so a predicate that looked at the claim alone would
        promote it a second time. The node half of `is_parked_for_quota_group_slot` is what
        keeps that from happening.
        """
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="mid-promotion",
            stalled_for_seconds=_PAST_THRESHOLD,
        )
        with session_factory() as session:
            node = session.get(bts.ExecutionNode, "mid-promotion")
            assert node is not None
            node.container_execution_status = Status.QUEUED
            session.commit()

        assert _stalled(session_factory=session_factory).reconcile() == 0
        assert promote_calls == []

    def test_a_never_parked_claim_is_not_selected_on_a_null_clock(
        self, session_factory: orm.sessionmaker, promote_calls: list[str]
    ) -> None:
        """`parked_at IS NULL` must not compare as stalled.

        A claim admitted straight through never parks, so its clock is NULL. In SQL that
        makes `parked_at <= cutoff` unknown rather than true — this test is what fails if
        the predicate is ever rewritten as a coalesce.
        """
        group_id = _make_group(session_factory=session_factory, capacity=2)
        with session_factory() as session:
            node = bts.ExecutionNode(task_spec={})
            node.id = "never-parked"
            node.container_execution_status = Status.UNINITIALIZED
            session.add(node)
            session.commit()
            session.add(
                db_models.QuotaGroupClaim(
                    quota_group_id=group_id,
                    execution_node_id="never-parked",
                    state=db_models.ClaimState.WAITING,
                )
            )
            session.commit()

        assert _stalled(session_factory=session_factory).reconcile() == 0
        assert promote_calls == []

    def test_an_empty_fleet_is_a_no_op(self, session_factory: orm.sessionmaker) -> None:
        assert _stalled(session_factory=session_factory).reconcile() == 0


class TestWhatItSays:
    """A backstop that fires is news. A backstop that finds nothing is not."""

    def test_a_correction_is_logged_as_a_warning(
        self,
        session_factory: orm.sessionmaker,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Every fire means an edge was genuinely lost, so WARNING is the right level."""
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="stranded",
            stalled_for_seconds=_PAST_THRESHOLD,
        )

        with caplog.at_level(logging.WARNING, logger=reconciler.__name__):
            _stalled(session_factory=session_factory).reconcile()

        assert "un-parked 1 node(s)" in caplog.text
        assert "a promotion edge was lost" in caplog.text

    def test_a_quiet_pass_says_nothing(
        self,
        session_factory: orm.sessionmaker,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The steady state runs 1440 times a day; it must not narrate any of them."""
        with caplog.at_level(logging.DEBUG, logger=reconciler.__name__):
            assert _stalled(session_factory=session_factory).reconcile() == 0

        assert caplog.records == []


class TestWhatItReports:
    """The pass is counted under a label an operator can join to the other four triggers."""

    def test_a_pass_is_counted_under_the_reconcile_trigger(
        self,
        session_factory: orm.sessionmaker,
        promoting_calls: list[tuple[str, quota_metrics.PromotionTrigger]],
    ) -> None:
        group_id = _make_group(session_factory=session_factory, capacity=2)
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="stranded",
            stalled_for_seconds=_PAST_THRESHOLD,
        )

        _stalled(session_factory=session_factory).reconcile()

        assert promoting_calls == [("bq", quota_metrics.PromotionTrigger.RECONCILE)]

    def test_the_group_is_labelled_by_name_not_by_id(
        self,
        session_factory: orm.sessionmaker,
        promoting_calls: list[tuple[str, quota_metrics.PromotionTrigger]],
    ) -> None:
        # Found by review: the draft labelled with `group_id`, while every other promoting()
        # call site passes the group's name. Ids here would file the backstop's passes under
        # a second series for the same group, and no dashboard would join the two. Asserted
        # separately from the trigger because it is the half that looks right either way.
        group_id = _make_group(
            session_factory=session_factory, name="gpu-pool", capacity=2
        )
        _park(
            session_factory=session_factory,
            group_id=group_id,
            node_id="stranded",
            stalled_for_seconds=_PAST_THRESHOLD,
        )

        _stalled(session_factory=session_factory).reconcile()

        labelled = promoting_calls[0][0]
        assert labelled == "gpu-pool"
        assert labelled != group_id

    def test_a_quiet_pass_counts_nothing(
        self,
        session_factory: orm.sessionmaker,
        promoting_calls: list[tuple[str, quota_metrics.PromotionTrigger]],
    ) -> None:
        # The steady state. A counter that ticks on every empty pass would make "RECONCILE
        # moved" mean nothing, which is the one thing this series has to mean.
        _make_group(session_factory=session_factory, capacity=2)

        assert _stalled(session_factory=session_factory).reconcile() == 0

        assert promoting_calls == []


class TestOneTransactionPerGroup:
    """The obligation the ABC names, and the reason the factory is on the reconciler."""

    def test_a_group_that_raises_does_not_undo_the_group_before_it(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The first group's promotion is committed before the second is attempted.

        A single session for the whole pass would lose it: the exception would leave one
        uncommitted transaction holding both groups' work, and the un-parked node would
        silently roll back.
        """
        for name in ("bq", "spark"):
            group_id = _make_group(
                session_factory=session_factory, name=name, capacity=1
            )
            _park(
                session_factory=session_factory,
                group_id=group_id,
                node_id=f"{name}-waiter",
                stalled_for_seconds=_PAST_THRESHOLD,
            )

        real_promote = reconciler.promotion.promote
        seen: list[str] = []

        def _promote_then_explode(*, session: orm.Session, group_id: str) -> int:
            seen.append(group_id)
            if len(seen) == 2:
                raise RuntimeError("second group is wedged")
            return real_promote(session=session, group_id=group_id)

        monkeypatch.setattr(reconciler.promotion, "promote", _promote_then_explode)

        with pytest.raises(RuntimeError, match="second group is wedged"):
            _stalled(session_factory=session_factory).reconcile()

        promoted = [
            node_id
            for node_id in ("bq-waiter", "spark-waiter")
            if _status_of(session_factory=session_factory, node_id=node_id)
            is Status.QUEUED
        ]
        assert len(promoted) == 1, "the first group's commit did not survive"
