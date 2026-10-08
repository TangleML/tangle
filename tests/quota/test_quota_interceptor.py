"""Unit tests for quota.interceptor — the admission gate.

What is pinned here: a node under the cap launches, a node at the cap parks, `capacity = 0`
parks everything, a node that loses the version compare-and-set re-reads rather than
proceeding on the numbers it lost with, a park is abandoned rather than written when somebody
else has already decided the node's fate, and each exit reaches the observer it should.

Those last ones stop at the observer's seam rather than at an OTel reader. The gate names a
verdict and hands a duration over; whether that reaches a counter is
`tests/quota/observability/`'s question, asked there against a real in-memory reader. Split
that way because the gate's own contract is which method it calls at which exit, and a test
that went all the way to the metric would fail for either side's reasons.
"""

import datetime
import sys
from typing import Any, Callable, Iterator

import pytest
import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.handlers.quota import annotations
from cloud_pipelines_backend.quota import (
    db_models,
    groups,
    interceptor,
    occupancy,
    promotion,
)

KEY = annotations.QUOTA_GROUP_KEY
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


def _attach_to_a_run(
    *,
    session: orm.Session,
    node: bts.ExecutionNode,
    cancelled: bool = False,
) -> bts.PipelineRun:
    """Give a node the rows the park guard looks at: a root, a run, and the ancestor link.

    Most tests in this file have no run at all, which is the other half of the guard worth
    covering: `NOT EXISTS` on a run that is not there must still let the node park.

    Args:
        session: The test session.
        node: The node to hang off the new run.
        cancelled: Whether the run carries `desired_state = "TERMINATED"`, as `terminate()`
            writes it.

    Returns:
        The run, so a test can cancel it later.
    """
    root = bts.ExecutionNode(task_spec={})
    session.add(root)
    session.flush()
    run = bts.PipelineRun(root_execution=root, created_by="test@example.com")
    if cancelled:
        run.extra_data = {"desired_state": "TERMINATED"}
    session.add(run)
    session.add(
        bts.ExecutionToAncestorExecutionLink(ancestor_execution=root, execution=node)
    )
    session.commit()
    return run


def _status_in_the_database(*, session: orm.Session, node_id: str) -> Status:
    """Read the node's status past the identity map, as the next sweep would."""
    return session.scalars(
        sql.select(bts.ExecutionNode.container_execution_status).where(
            bts.ExecutionNode.id == node_id
        )
    ).one()


def _status_history(*, session: orm.Session, node_id: str) -> list[str]:
    """The node's recorded status hops, oldest first, read past the identity map."""
    extra_data = session.scalars(
        sql.select(bts.ExecutionNode.extra_data).where(bts.ExecutionNode.id == node_id)
    ).one()
    return [
        entry["status"]
        for entry in (extra_data or {}).get(
            bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY, []
        )
    ]


def _claim_of(
    *, session: orm.Session, node_id: str
) -> db_models.QuotaGroupClaim | None:
    return session.scalars(
        sql.select(db_models.QuotaGroupClaim).where(
            db_models.QuotaGroupClaim.execution_node_id == node_id
        )
    ).one_or_none()


@pytest.fixture()
def gate() -> interceptor.QuotaGroupInterceptor:
    return interceptor.QuotaGroupInterceptor()


@pytest.fixture()
def status_history_hook() -> Iterator[None]:
    """Register the ORM hook that maintains `container_execution_status_history`.

    The hook belongs to the orchestrator module, which registers it as an import side
    effect. Nothing else under `tests/quota` imports that module, so without this fixture
    every history assertion in this file would pass against an empty list -- and leaving it
    registered for the rest of the pytest session would change the behaviour of suites that
    never asked for it. Registering explicitly rather than relying on the import makes it
    work for the second test in the class as well as the first: the import is cached, so a
    fixture that removed what the import had added could never put it back.

    Ownership has to be decided across the import, not after it. On the first use in a fresh
    process the import below is itself what registers the hook, so a `contains()` check made
    afterwards reports the listener as somebody else's and teardown leaves it behind.
    """
    module_name = "cloud_pipelines_backend.orchestrator_sql"
    was_imported = module_name in sys.modules

    from cloud_pipelines_backend import orchestrator_sql

    target = bts.ExecutionNode.container_execution_status
    hook = orchestrator_sql._handle_container_execution_status_set
    if not sql.event.contains(target, "set", hook):
        sql.event.listen(target, "set", hook)
        ours = True
    else:
        # Registered already. It is this fixture's to remove only if the import above is
        # what put it there.
        ours = not was_imported
    try:
        yield
    finally:
        if ours:
            sql.event.remove(target, "set", hook)
        if not was_imported:
            # The import above is what registered the hook, so it has to be gone again.
            # That is the only moment the leak is observable, and scoping the check to it
            # keeps the canary from misfiring if anything else imports the module first.
            assert not sql.event.contains(
                target, "set", hook
            ), "the status-history hook outlived its fixture"


class TestUngated:
    def test_a_node_with_no_annotation_launches_and_claims_nothing(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        node = _make_node(session=session, node_id="node-1", group_name=None)

        assert gate.intercept(session=session, execution=node) is False
        assert node.container_execution_status is Status.QUEUED
        assert _claim_of(session=session, node_id="node-1") is None

    def test_a_node_naming_an_unknown_group_launches_and_is_marked(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        node = _make_node(session=session, node_id="node-1", group_name="typo")

        assert gate.intercept(session=session, execution=node) is False
        assert _claim_of(session=session, node_id="node-1") is None
        assert node.extra_data[groups.MISSING_GROUP_MARKER] == "typo"

    def test_a_group_deleted_mid_gate_launches_ungated(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # Deleting a group un-parks its waiters on purpose, so launching is the same answer
        # the deletion itself would have given.
        group = _make_group(session=session)
        node = _make_node(session=session, node_id="node-1")
        session.delete(group)
        session.commit()

        assert gate.intercept(session=session, execution=node) is False


class TestAdmitUnderCap:
    def test_the_first_node_launches_and_claims_active(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")

        assert gate.intercept(session=session, execution=node) is False
        assert node.container_execution_status is Status.QUEUED
        claim = _claim_of(session=session, node_id="node-1")
        assert claim is not None
        assert claim.state is db_models.ClaimState.ACTIVE

    def test_admission_bumps_the_version(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        group = _make_group(session=session, capacity=2)
        assert group.version == 0
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="node-1"),
        )

        session.refresh(group)
        assert group.version == 1

    def test_the_admission_is_committed_not_merely_flushed(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # The slot must be durable before the orchestrator goes off to create a container,
        # or a crash mid-launch would hand the same slot out twice.
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")
        gate.intercept(session=session, execution=node)

        session.rollback()
        claim = _claim_of(session=session, node_id="node-1")
        assert claim is not None
        assert claim.state is db_models.ClaimState.ACTIVE


class TestParkAtCap:
    def test_the_node_over_the_cap_parks(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        _make_group(session=session, capacity=1)
        first = _make_node(session=session, node_id="node-1")
        second = _make_node(session=session, node_id="node-2")

        assert gate.intercept(session=session, execution=first) is False
        assert gate.intercept(session=session, execution=second) is True

        assert second.container_execution_status is Status.UNINITIALIZED
        claim = _claim_of(session=session, node_id="node-2")
        assert claim is not None
        assert claim.state is db_models.ClaimState.WAITING

    def test_parking_does_not_bump_the_version(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # Nothing was taken, so nobody else's read went stale.
        group = _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="node-1"),
        )
        session.refresh(group)
        after_admit = group.version

        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="node-2"),
        )
        session.refresh(group)
        assert group.version == after_admit

    def test_the_park_is_committed(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # The interceptor owns the node once it returns True, and the orchestrator returns
        # without committing anything of its own.
        _make_group(session=session, capacity=0)
        node = _make_node(session=session, node_id="node-1")
        gate.intercept(session=session, execution=node)

        session.rollback()
        session.expunge_all()
        reloaded = session.get(bts.ExecutionNode, "node-1")
        assert reloaded is not None
        assert reloaded.container_execution_status is Status.UNINITIALIZED

    def test_a_finished_member_frees_its_slot_for_the_next_arrival(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # There is no release path: the finished node keeps its ACTIVE claim and the slot is
        # freed only because occupancy reads the node's status.
        _make_group(session=session, capacity=1)
        first = _make_node(session=session, node_id="node-1")
        gate.intercept(session=session, execution=first)
        first.container_execution_status = Status.SUCCEEDED
        session.commit()

        second = _make_node(session=session, node_id="node-2")
        assert gate.intercept(session=session, execution=second) is False
        assert _claim_of(session=session, node_id="node-1").state is (
            db_models.ClaimState.ACTIVE
        )


class TestTheParkInStatusHistory:
    """The park has to appear in the node's status history, not just in its status column.

    The park's status write is a conditional `UPDATE`, which bypasses the ORM hook that
    maintains the history. Before `_record_park_in_history`, a node that waited three hours
    for a slot had a history reading `QUEUED -> PENDING` across the original `QUEUED`
    timestamp, and the status-transition metric billed the whole wait as queue time -- which
    is what pages on the QUEUED alert. Nothing reads the history to decide anything, so this
    is an observability contract and these tests are the only thing holding it.
    """

    def test_a_park_appends_uninitialized(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        status_history_hook: None,
    ) -> None:
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")

        assert gate.intercept(session=session, execution=waiter) is True

        assert _status_history(session=session, node_id="waiter") == [
            "QUEUED",
            "UNINITIALIZED",
        ]

    def test_the_whole_wait_reads_as_four_hops_not_two(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        status_history_hook: None,
    ) -> None:
        # The journey the metric is derived from: queued, parked, promoted back to queued,
        # then launched. The hop that matters is the last one -- `QUEUED -> PENDING` is now
        # the gap between the promotion and the launch, not between the first sweep and the
        # launch, so the quota wait sits in its own `QUEUED -> UNINITIALIZED` sample instead.
        group = _make_group(session=session, capacity=1)
        holder = _make_node(session=session, node_id="holder")
        assert gate.intercept(session=session, execution=holder) is False
        waiter = _make_node(session=session, node_id="waiter")
        assert gate.intercept(session=session, execution=waiter) is True

        holder.container_execution_status = Status.SUCCEEDED
        session.commit()
        assert promotion.promote(session=session, group_id=group.id) == 1
        session.commit()

        assert gate.intercept(session=session, execution=waiter) is False
        waiter.container_execution_status = Status.PENDING
        session.commit()

        assert _status_history(session=session, node_id="waiter") == [
            "QUEUED",
            "UNINITIALIZED",
            "QUEUED",
            "PENDING",
        ]

    def test_a_re_park_appends_a_second_uninitialized(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        status_history_hook: None,
    ) -> None:
        # Promoted into a group that filled up again before the sweep reached it. The hook
        # drops an entry equal to the last one, so only a real promotion in between makes
        # this second park visible -- which is the shape that proves the entry is the park's
        # and not a duplicate of the first.
        group = _make_group(session=session, capacity=1)
        holder = _make_node(session=session, node_id="holder")
        gate.intercept(session=session, execution=holder)
        waiter = _make_node(session=session, node_id="waiter")
        gate.intercept(session=session, execution=waiter)

        holder.container_execution_status = Status.SUCCEEDED
        session.commit()
        promotion.promote(session=session, group_id=group.id)
        session.commit()
        # Somebody else took the slot between the promotion and this sweep.
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="interloper"),
        )

        assert gate.intercept(session=session, execution=waiter) is True

        assert _status_history(session=session, node_id="waiter") == [
            "QUEUED",
            "UNINITIALIZED",
            "QUEUED",
            "UNINITIALIZED",
        ]

    def test_an_abandoned_park_writes_no_history_entry(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        status_history_hook: None,
    ) -> None:
        # The park rolled back, so the node is not parked and the history must not say it
        # was. The entry is written before the commit, which is exactly where a rollback can
        # still take it back -- and the ordering that makes that true is worth pinning.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        _attach_to_a_run(session=session, node=waiter, cancelled=True)

        assert gate.intercept(session=session, execution=waiter) is True

        assert _status_history(session=session, node_id="waiter") == ["QUEUED"]

    def test_the_park_keeps_extra_data_written_since_the_sweeps_read(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        status_history_hook: None,
    ) -> None:
        # `extra_data` is one JSON document shared by several writers, and the hook rewrites
        # the whole of it. The sweep's read of this node is already old by the time the gate
        # writes, so the park re-reads the row under its own lock first; without that
        # re-read it would write its stale copy back and silently drop the key below.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        assert waiter.extra_data is not None  # load it, as the sweep's read would

        with orm.Session(bind=session.get_bind()) as other:
            node = other.get(bts.ExecutionNode, "waiter")
            assert node is not None
            node.extra_data = {
                **(node.extra_data or {}),
                "written_by": "someone",
            }
            other.commit()

        assert gate.intercept(session=session, execution=waiter) is True

        extra_data = session.scalars(
            sql.select(bts.ExecutionNode.extra_data).where(
                bts.ExecutionNode.id == "waiter"
            )
        ).one()
        assert extra_data["written_by"] == "someone"
        assert _status_history(session=session, node_id="waiter") == [
            "QUEUED",
            "UNINITIALIZED",
        ]


class TestAParkThatMustNotBeWritten:
    """The park is a trapdoor, so it is conditional.

    UNINITIALIZED is not selected by the queued sweep, so only a promotion re-opens it -- and
    the two writers that race this gate, a cancel and a group delete, both end the possibility
    of one. A park written on top of either strands the node forever: the run never goes
    terminal, and its claim never releases. Every test here is the same shape -- the group is
    full, so the gate wants to park, and something has already decided otherwise.
    """

    def test_a_cancelled_run_stops_the_park(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # The exact strand seen live: run 01a083d96be6ca1560e2 sat at UNINITIALIZED with a
        # WAITING claim and desired_state=TERMINATED, invisible to every sweep.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        _attach_to_a_run(session=session, node=waiter, cancelled=True)

        # Still True: not launching is right either way. What changes is the row left behind.
        assert gate.intercept(session=session, execution=waiter) is True
        assert (
            _status_in_the_database(session=session, node_id="waiter") is Status.QUEUED
        )

    def test_a_node_flagged_for_termination_stops_the_park(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # `terminate()` flags the run and each of its live executions, and the orchestrator
        # honours either flag (orchestrator_sql.py:606). So does the guard.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        waiter.extra_data = {"desired_state": "TERMINATED"}
        session.commit()

        assert gate.intercept(session=session, execution=waiter) is True
        assert (
            _status_in_the_database(session=session, node_id="waiter") is Status.QUEUED
        )

    def test_a_node_no_longer_queued_stops_the_park(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # The sweep read this node QUEUED; by the time the gate writes, somebody has moved it.
        # synchronize_session=False leaves the in-session object saying QUEUED, which is what
        # a stale sweep read looks like -- the guard has to catch it at the row, not in memory.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        session.execute(
            sql.update(bts.ExecutionNode)
            .where(bts.ExecutionNode.id == "waiter")
            .values(container_execution_status=Status.CANCELLED)
            .execution_options(synchronize_session=False)
        )
        session.commit()

        assert gate.intercept(session=session, execution=waiter) is True
        assert (
            _status_in_the_database(session=session, node_id="waiter")
            is Status.CANCELLED
        )

    def test_an_abandoned_park_writes_no_claim(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # A WAITING claim is a place in the promotion queue. A node that was never parked has
        # no business holding one -- and on a cancelled run nothing will ever come to clear it.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        _attach_to_a_run(session=session, node=waiter, cancelled=True)

        gate.intercept(session=session, execution=waiter)

        session.rollback()
        assert _claim_of(session=session, node_id="waiter") is None

    def test_an_abandoned_park_keeps_an_earlier_claim_as_it_was(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # A re-park of a promoted node: the claim row already exists, so "writes no claim"
        # is not enough -- the abandoned attempt must not touch the row it found either.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        assert gate.intercept(session=session, execution=waiter) is True
        first_claim = _claim_of(session=session, node_id="waiter")
        assert first_claim is not None
        waiting_since = first_claim.created_at

        # Promoted, then cancelled before the gate could re-park it.
        waiter.container_execution_status = Status.QUEUED
        session.commit()
        _attach_to_a_run(session=session, node=waiter, cancelled=True)

        assert gate.intercept(session=session, execution=waiter) is True
        session.rollback()
        claim = _claim_of(session=session, node_id="waiter")
        assert claim is not None
        assert claim.state is db_models.ClaimState.WAITING
        assert claim.created_at == waiting_since
        assert (
            _status_in_the_database(session=session, node_id="waiter") is Status.QUEUED
        )

    def test_the_guard_is_read_at_the_write_not_before_the_gate_started(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The interleaving that produced the strand: the cancel lands mid-gate.

        Cancelling before `intercept()` is called would be caught by any guard, including one
        that read `desired_state` once on the way in. The cancel here commits *after* the gate
        has read the node and counted occupancy, which is the only ordering that distinguishes
        a check folded into the `UPDATE` from a check done earlier and trusted.

        The cancel goes straight at the row rather than through a second Session for the
        reason `TestTheVersionCas` documents: `sqlite://` is pooled with StaticPool, so a
        second session would share this one's connection and transaction.
        """
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        run = _attach_to_a_run(session=session, node=waiter)

        real = occupancy.count_occupancy

        def _cancel_mid_gate(**kwargs: Any) -> int:
            count = real(**kwargs)
            session.execute(
                sql.update(bts.PipelineRun)
                .where(bts.PipelineRun.id == run.id)
                .values(extra_data={"desired_state": "TERMINATED"})
                .execution_options(synchronize_session=False)
            )
            return count

        monkeypatch.setattr(interceptor.occupancy, "count_occupancy", _cancel_mid_gate)

        assert gate.intercept(session=session, execution=waiter) is True
        assert (
            _status_in_the_database(session=session, node_id="waiter") is Status.QUEUED
        )
        assert _claim_of(session=session, node_id="waiter") is None


class TestAParkThatMustStillBeWritten:
    """The other half of the guard: it must not stop an ordinary park.

    Every other park test in this file has no run rows at all, so these two cover the shapes
    where the `NOT EXISTS` subqueries have something to look at and must still come out false.
    """

    def test_a_node_on_a_live_run_parks(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        _attach_to_a_run(session=session, node=waiter)

        assert gate.intercept(session=session, execution=waiter) is True
        assert (
            _status_in_the_database(session=session, node_id="waiter")
            is Status.UNINITIALIZED
        )
        claim = _claim_of(session=session, node_id="waiter")
        assert claim is not None
        assert claim.state is db_models.ClaimState.WAITING

    def test_a_run_flagged_with_something_other_than_termination_parks(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # `extra_data` is a shared bag; the guard keys on one value in it, not on its presence.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        run = _attach_to_a_run(session=session, node=waiter)
        run.extra_data = {"desired_state": "RUNNING"}
        session.commit()

        assert gate.intercept(session=session, execution=waiter) is True
        assert (
            _status_in_the_database(session=session, node_id="waiter")
            is Status.UNINITIALIZED
        )


class TestCapacityZero:
    def test_everything_parks(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # The kill switch. Legal, and it parks the very first arrival.
        _make_group(session=session, capacity=0)

        for index in range(3):
            node = _make_node(session=session, node_id=f"node-{index}")
            assert gate.intercept(session=session, execution=node) is True
            assert node.container_execution_status is Status.UNINITIALIZED

    def test_no_version_is_ever_bumped(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        group = _make_group(session=session, capacity=0)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="node-1"),
        )
        session.refresh(group)
        assert group.version == 0


class TestReEntry:
    def test_a_node_that_already_holds_a_slot_is_not_made_to_win_it_twice(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # A launch that failed leaves the node QUEUED with an ACTIVE claim, and the sweep
        # sends it back through the gate. Its own claim is part of the occupancy count, so
        # re-running the check would let the node park itself -- at capacity 1 with no other
        # member, nothing would ever complete to promote it again.
        group = _make_group(session=session, capacity=1)
        node = _make_node(session=session, node_id="node-1")
        assert gate.intercept(session=session, execution=node) is False
        session.refresh(group)
        version_after_first = group.version

        assert gate.intercept(session=session, execution=node) is False
        assert node.container_execution_status is Status.QUEUED
        session.refresh(group)
        assert group.version == version_after_first

    def test_a_parked_node_re_checked_at_a_still_full_group_re_parks(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # A WAITING claim is not a slot, so this node goes through the full check again.
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        assert gate.intercept(session=session, execution=waiter) is True

        waiter.container_execution_status = Status.QUEUED
        session.commit()
        assert gate.intercept(session=session, execution=waiter) is True
        assert waiter.container_execution_status is Status.UNINITIALIZED


class TestParkedAt:
    """The clock the promotion backstop reads.

    `parked_at` is derived from the state being written (`quota/interceptor.py:379`) rather
    than tracked beside it, so these pin the derivation at both writers -- the INSERT on a
    first park and the in-place UPDATE on a re-park -- and at admission, where it must go
    back to NULL.
    """

    @staticmethod
    def _freeze(*, monkeypatch: pytest.MonkeyPatch, at: datetime.datetime) -> None:
        """Pin `utc_now` so a re-stamp is provable rather than probable.

        Real time would work on SQLite, where the column keeps microseconds, and would be a
        coin toss on MySQL, where `DATETIME` is whole-second and both parks in a fast test
        land inside one tick. The assertion is about which clock reading was written, not
        about how long the test took.
        """
        monkeypatch.setattr(interceptor.db_utils, "utc_now", lambda: at)

    def test_a_first_park_stamps_it(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        parked = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        self._freeze(monkeypatch=monkeypatch, at=parked)

        assert (
            gate.intercept(
                session=session,
                execution=_make_node(session=session, node_id="waiter"),
            )
            is True
        )

        claim = _claim_of(session=session, node_id="waiter")
        assert claim is not None
        assert claim.state is db_models.ClaimState.WAITING
        assert claim.parked_at == parked

    def test_a_re_park_re_stamps_it(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The whole reason the column exists. A re-park writes WAITING over WAITING and the
        # same group id over itself, so the row is not dirty and `updated_at`'s `onupdate`
        # never fires -- an explicit fresh timestamp is always a changed value, so it does.
        first = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        second = datetime.datetime(2026, 1, 1, 0, 5, tzinfo=datetime.timezone.utc)
        _make_group(session=session, capacity=1)
        gate.intercept(
            session=session,
            execution=_make_node(session=session, node_id="holder"),
        )
        waiter = _make_node(session=session, node_id="waiter")
        self._freeze(monkeypatch=monkeypatch, at=first)
        assert gate.intercept(session=session, execution=waiter) is True

        waiter.container_execution_status = Status.QUEUED
        session.commit()
        self._freeze(monkeypatch=monkeypatch, at=second)
        assert gate.intercept(session=session, execution=waiter) is True

        session.commit()
        claim = _claim_of(session=session, node_id="waiter")
        assert claim is not None
        assert claim.parked_at == second

    def test_admission_clears_it(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The waiter parks behind a full group, the holder's slot frees, and the same node is
        # re-checked and admitted. Left set, its stale timestamp would read to the backstop as
        # a node that has been parked since before the cutoff.
        _make_group(session=session, capacity=1)
        holder = _make_node(session=session, node_id="holder")
        gate.intercept(session=session, execution=holder)
        waiter = _make_node(session=session, node_id="waiter")
        self._freeze(
            monkeypatch=monkeypatch,
            at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        )
        assert gate.intercept(session=session, execution=waiter) is True
        assert _claim_of(session=session, node_id="waiter").parked_at is not None

        holder.container_execution_status = Status.SUCCEEDED
        waiter.container_execution_status = Status.QUEUED
        session.commit()
        assert gate.intercept(session=session, execution=waiter) is False

        session.commit()
        claim = _claim_of(session=session, node_id="waiter")
        assert claim is not None
        assert claim.state is db_models.ClaimState.ACTIVE
        assert claim.parked_at is None


class TestATerminalClaim:
    """`uq_quota_group_claim_node` allows one row per node ever, so the ledger entry and any
    re-claim by the same node are the same row. This is the collision that creates."""

    def test_a_done_claim_does_not_read_as_holding_a_slot(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # The re-entry short circuit keys on ACTIVE. If it keyed on "a claim exists", a node
        # whose slot had already gone back would be waved through without winning one.
        group = _make_group(session=session, capacity=1)
        node = _make_node(session=session, node_id="node-1")
        assert gate.intercept(session=session, execution=node) is False
        claim = _claim_of(session=session, node_id="node-1")
        assert claim is not None
        claim.state = db_models.ClaimState.DONE
        session.commit()

        # Capacity is free again -- the node's own container is what held it, and the test
        # never launched one -- so this is a fresh admission, not a short circuit.
        assert gate.intercept(session=session, execution=node) is False
        session.refresh(group)
        assert group.version == 2

    def test_re_claiming_a_done_row_leaves_a_trace(
        self, session: orm.Session, gate: interceptor.QuotaGroupInterceptor
    ) -> None:
        # Should be unreachable: a DONE claim means the node ended, and an ended node is not
        # swept back to the gate. It is recorded rather than refused because failing a launch
        # to protect bookkeeping is the worse trade -- but the overwrite must not be silent,
        # since it destroys that node's ledger entry.
        _make_group(session=session, capacity=1)
        node = _make_node(session=session, node_id="node-1")
        gate.intercept(session=session, execution=node)
        claim = _claim_of(session=session, node_id="node-1")
        assert claim is not None
        claim.state = db_models.ClaimState.DONE
        session.commit()

        gate.intercept(session=session, execution=node)

        reclaimed = _claim_of(session=session, node_id="node-1")
        assert reclaimed is not None
        assert reclaimed.state is db_models.ClaimState.ACTIVE
        assert len(reclaimed.extra_data["reclaimed_from_done"]) == 1


class TestTheVersionCas:
    @staticmethod
    def _steal_the_version_once(
        *, session: orm.Session, real: Callable[..., int]
    ) -> Callable[..., int]:
        """Make the next occupancy read be followed by somebody else's version bump.

        A second Session cannot stand in for a competing admitter here: `sqlite://` is
        pooled with StaticPool, so both sessions would share one connection and one
        transaction. Bumping the row underneath the ORM object produces the state that
        matters -- the version this attempt is about to compare against is no longer the
        version in the row -- which is exactly what losing the race feels like.
        """
        state = {"stolen": False}

        def _counting(**kwargs: Any) -> int:
            count = real(**kwargs)
            if not state["stolen"]:
                state["stolen"] = True
                session.execute(
                    sql.update(db_models.QuotaGroup).values(
                        version=db_models.QuotaGroup.version + 1
                    )
                    # Without this the ORM helpfully syncs the new version back into the
                    # in-session object, and the compare-and-set would match after all --
                    # which a genuinely competing transaction could never do.
                    .execution_options(synchronize_session=False)
                )
            return count

        return _counting

    def test_the_loser_re_reads_and_then_admits(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")

        calls: list[int] = []
        real = occupancy.count_occupancy
        stealing = self._steal_the_version_once(session=session, real=real)

        def _tracked(**kwargs: Any) -> int:
            calls.append(1)
            return stealing(**kwargs)

        monkeypatch.setattr(interceptor.occupancy, "count_occupancy", _tracked)

        assert gate.intercept(session=session, execution=node) is False
        # Two reads: the attempt that lost, and the retry that won. One read would mean the
        # gate proceeded on numbers it had already been told were stale.
        assert len(calls) == 2
        claim = _claim_of(session=session, node_id="node-1")
        assert claim is not None
        assert claim.state is db_models.ClaimState.ACTIVE

    def test_a_lost_cas_leaves_no_half_written_claim(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The rollback on a lost attempt is what keeps the bump and the claim atomic.
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")
        real = occupancy.count_occupancy

        def _always_steal(**kwargs: Any) -> int:
            count = real(**kwargs)
            session.execute(
                sql.update(db_models.QuotaGroup)
                .values(version=db_models.QuotaGroup.version + 1)
                .execution_options(synchronize_session=False)
            )
            return count

        monkeypatch.setattr(interceptor.occupancy, "count_occupancy", _always_steal)

        # Never wins, so every attempt is rolled back. What the name promises is exactly what
        # is left behind: no claim row at all, not a claim without its matching version bump.
        assert gate.intercept(session=session, execution=node) is True
        assert _claim_of(session=session, node_id="node-1") is None

    def test_a_lost_attempt_rolls_back_before_retrying(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The one guarantee here that SQLite cannot demonstrate by its consequences.

        Under MySQL's REPEATABLE READ, retrying inside the losing transaction re-serves the
        same snapshot, so the compare-and-set fails forever and the node parks even though a
        slot is free. SQLite has no such snapshot, so removing the rollback changes nothing
        observable -- which is exactly why the call is asserted directly rather than through
        an outcome that would stay green without it.
        """
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")

        rollbacks: list[int] = []
        real_rollback = session.rollback

        def _counting_rollback() -> None:
            rollbacks.append(1)
            real_rollback()

        monkeypatch.setattr(session, "rollback", _counting_rollback)

        real = occupancy.count_occupancy
        stealing = self._steal_the_version_once(session=session, real=real)
        monkeypatch.setattr(interceptor.occupancy, "count_occupancy", stealing)

        assert gate.intercept(session=session, execution=node) is False
        assert len(rollbacks) == 1

    def test_the_retry_budget_is_finite(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")
        real = occupancy.count_occupancy
        calls: list[int] = []

        def _always_steal(**kwargs: Any) -> int:
            calls.append(1)
            count = real(**kwargs)
            session.execute(
                sql.update(db_models.QuotaGroup)
                .values(version=db_models.QuotaGroup.version + 1)
                .execution_options(synchronize_session=False)
            )
            return count

        monkeypatch.setattr(interceptor.occupancy, "count_occupancy", _always_steal)
        gate = interceptor.QuotaGroupInterceptor(max_cas_attempts=3)

        assert gate.intercept(session=session, execution=node) is True
        assert len(calls) == 3


class TestCasExhaustion:
    """What the gate does when it never wins the compare-and-set.

    Split out from TestTheVersionCas on purpose. Losing every attempt is a *decision*, not a
    mechanism. Reaching here means every attempt saw room, so the group was never shown to be
    full -- and the gate writes nothing rather than recording a wait it cannot justify.
    """

    @staticmethod
    def _never_win(*, session: orm.Session) -> Callable[..., int]:
        """Bump the version after every occupancy read, so no attempt can ever win.

        See TestTheVersionCas._steal_the_version_once for why the bump goes straight at the
        row with synchronize_session=False rather than through a second Session.
        """
        real = occupancy.count_occupancy

        def _always_steal(**kwargs: Any) -> int:
            count = real(**kwargs)
            session.execute(
                sql.update(db_models.QuotaGroup)
                .values(version=db_models.QuotaGroup.version + 1)
                .execution_options(synchronize_session=False)
            )
            return count

        return _always_steal

    def test_exhaustion_leaves_the_node_queued_for_the_next_pass(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The group has room -- capacity 2, nobody in it -- and every attempt saw that room.
        # QUEUED is the status the orchestrator re-polls, so the node simply tries again.
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")
        monkeypatch.setattr(
            interceptor.occupancy,
            "count_occupancy",
            self._never_win(session=session),
        )

        assert gate.intercept(session=session, execution=node) is True
        assert node.container_execution_status is Status.QUEUED

    def test_exhaustion_writes_no_claim(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A WAITING claim means "turned away from a full group". Nothing here established that,
        # and a claim written on contention alone would hold a place in the promotion queue
        # that the node never earned -- and would make the re-entry check see a stale wait.
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")
        monkeypatch.setattr(
            interceptor.occupancy,
            "count_occupancy",
            self._never_win(session=session),
        )
        gate.intercept(session=session, execution=node)

        session.rollback()
        session.expunge_all()
        reloaded = session.get(bts.ExecutionNode, "node-1")
        assert reloaded is not None
        assert reloaded.container_execution_status is Status.QUEUED
        assert _claim_of(session=session, node_id="node-1") is None


def _wait_seconds_recorded(
    *,
    monkeypatch: pytest.MonkeyPatch,
) -> list[float]:
    """Capture the wait durations the gate records, and only those.

    Filtered by instrument rather than by call count: `record` is one shared helper, and the
    gate's own duration goes through it on the very same admission, so an unfiltered spy
    collects two numbers and the assertion passes or fails on their order.

    Args:
        monkeypatch: Used to swap the module-level `record`.

    Returns:
        A list that fills with the seconds recorded against `duration_wait` as the test runs.
    """
    seconds: list[float] = []

    def _spy(**kwargs: Any) -> None:
        if kwargs["histogram"] is interceptor.quota_metrics.duration_wait:
            seconds.append(kwargs["seconds"])

    monkeypatch.setattr(interceptor.quota_metrics, "record", _spy)
    return seconds


class TestWhatTheGateTellsTheObserver:
    """Which exit names which verdict, and the one interval the gate can close."""

    def test_exhaustion_is_named_as_contention_and_not_as_a_park(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # It used to name nothing, which put it in the same silence as an exception and an
        # abandoned park. Sustained contention and a broken gate ask for opposite responses --
        # raise the CAS budget, or wake somebody up.
        named: list[str] = []
        monkeypatch.setattr(
            interceptor.gate_observer.Gating,
            "contended",
            lambda _self, *, quota_group: named.append(quota_group),
        )
        _make_group(session=session, capacity=2)
        node = _make_node(session=session, node_id="node-1")
        monkeypatch.setattr(
            interceptor.occupancy,
            "count_occupancy",
            TestCasExhaustion._never_win(session=session),
        )

        assert gate.intercept(session=session, execution=node) is True
        assert named == ["bq"]

    def test_an_admission_after_a_wait_records_how_long_the_wait_was(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The number that says whether a capacity is set correctly, which no gauge can: a p50
        # of 200ms and a p50 of 40 minutes are both a waiters gauge reading 1.
        recorded = _wait_seconds_recorded(monkeypatch=monkeypatch)
        _make_group(session=session, capacity=1)
        holder = _make_node(session=session, node_id="holder")
        gate.intercept(session=session, execution=holder)
        waiter = _make_node(session=session, node_id="waiter")
        gate.intercept(session=session, execution=waiter)
        # Backdate the claim the park just wrote, then free the slot and let the waiter in.
        claim = _claim_of(session=session, node_id="waiter")
        claim.created_at = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(seconds=90)
        holder_claim = _claim_of(session=session, node_id="holder")
        holder_claim.state = db_models.ClaimState.DONE
        waiter.container_execution_status = Status.QUEUED
        session.commit()

        assert gate.intercept(session=session, execution=waiter) is False
        assert recorded == pytest.approx([90], abs=5)

    def test_a_node_admitted_on_its_first_pass_records_no_wait_at_all(
        self,
        session: orm.Session,
        gate: interceptor.QuotaGroupInterceptor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Not recorded as zero. At a healthy capacity most admissions are first-pass, so
        # counting them would bury every real queue time under a spike at the origin and the
        # distribution would answer a different question from the one it is for.
        recorded = _wait_seconds_recorded(monkeypatch=monkeypatch)
        _make_group(session=session, capacity=1)
        node = _make_node(session=session, node_id="node-1")

        assert gate.intercept(session=session, execution=node) is False
        assert recorded == []
