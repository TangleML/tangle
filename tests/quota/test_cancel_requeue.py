"""Unit tests for quota.cancel_requeue -- un-parking a cancelled run's parked executions.

Pinned here: a cancel moves parked nodes back to `QUEUED` and leaves every other status
alone, the move rides the cancel's own commit so a rejected cancel leaves nothing behind,
and the patch installed on `PipelineRunsApiService_Sql.terminate` is both idempotent and
loud about a submodule that has changed under it.

What is deliberately *not* pinned here: what the orchestrator does with the re-queued node
afterwards. Turning `QUEUED` + `desired_state == "TERMINATED"` into `CANCELLED` is
pre-existing upstream behaviour, covered by the backend's own tests; this module's only job
is to make the node visible again.
"""

import collections
import inspect
from typing import Any

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql, errors
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import cancel_requeue

Status = bts.ContainerExecutionStatus
OWNER = "owner@example.com"


def _make_node(
    *,
    session: orm.Session,
    status: Status | None = None,
) -> bts.ExecutionNode:
    node = bts.ExecutionNode(task_spec={}, container_execution_status=status)
    session.add(node)
    session.flush()
    return node


def _make_run(
    *,
    session: orm.Session,
    child_statuses: list[Status],
    created_by: str | None = OWNER,
) -> bts.PipelineRun:
    """A run whose root has one linked child per given status.

    `descendants` is a secondary relationship over `execution_ancestor`, so a child is only
    a descendant once the link row exists -- building the nodes alone would give every test
    an empty traversal and a vacuous pass.
    """
    root = _make_node(session=session)
    run = bts.PipelineRun(root_execution=root, created_by=created_by)
    session.add(run)
    session.flush()
    for status in child_statuses:
        child = _make_node(session=session, status=status)
        session.add(
            bts.ExecutionToAncestorExecutionLink(
                ancestor_execution=root, execution=child
            )
        )
    session.commit()
    return run


def _statuses_of(
    *, session: orm.Session, run: bts.PipelineRun
) -> collections.Counter[Status]:
    """How many descendants sit in each status.

    A `Counter` rather than a list because `descendants` has no `order_by` and node ids are
    random, so row order is arbitrary -- a list assertion would pass or fail by luck.

    No `expire_all()` here: these tests read the session mid-transaction, and expiring would
    throw away the very uncommitted change under test.

    Args:
        session: Unused -- the run is already attached to it -- but kept so every helper
            here is called the same way.
        run: The run whose descendants are counted.
    """
    return collections.Counter(
        node.container_execution_status for node in run.root_execution.descendants
    )


@pytest.fixture()
def installed_patch() -> Any:
    """Install the patch for one test and put the original method back afterwards.

    The patch lives on a class imported from the submodule, so leaving it in place would
    leak into every later test in the session -- including ones that assert the *unpatched*
    behaviour of `terminate()`.
    """
    original = api_server_sql.PipelineRunsApiService_Sql.terminate
    cancel_requeue.install()
    yield
    api_server_sql.PipelineRunsApiService_Sql.terminate = original


def _unpark(*, session: orm.Session, run: bts.PipelineRun) -> None:
    """Call the interceptor the way the patch does, without going through `terminate()`."""
    cancel_requeue.QuotaUnparkOnTerminate().on_terminate(
        session=session, pipeline_run=run
    )


# --- the traversal itself ----------------------------------------------------


class TestOnTerminate:
    """`on_terminate` moves exactly the invisible nodes and nothing else."""

    def test_a_parked_descendant_moves_to_queued(self, session: orm.Session) -> None:
        run = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])

        _unpark(session=session, run=run)

        assert _statuses_of(session=session, run=run) == collections.Counter(
            [Status.QUEUED]
        )

    def test_every_other_status_is_left_alone(self, session: orm.Session) -> None:
        """Only `UNINITIALIZED` is invisible to the sweep, so only it needs moving.

        Re-queueing a RUNNING or SUCCEEDED node would be a genuine regression: the
        orchestrator would treat it as a fresh launch candidate.
        """
        untouched = [
            Status.QUEUED,
            Status.WAITING_FOR_UPSTREAM,
            Status.PENDING,
            Status.RUNNING,
            Status.SUCCEEDED,
            Status.FAILED,
            Status.CANCELLED,
            Status.SKIPPED,
        ]
        run = _make_run(session=session, child_statuses=untouched)

        _unpark(session=session, run=run)

        assert _statuses_of(session=session, run=run) == collections.Counter(untouched)

    def test_only_the_parked_ones_move_in_a_mixed_run(
        self, session: orm.Session
    ) -> None:
        run = _make_run(
            session=session,
            child_statuses=[
                Status.RUNNING,
                Status.UNINITIALIZED,
                Status.SUCCEEDED,
                Status.UNINITIALIZED,
            ],
        )

        _unpark(session=session, run=run)

        assert _statuses_of(session=session, run=run) == collections.Counter(
            [Status.RUNNING, Status.QUEUED, Status.SUCCEEDED, Status.QUEUED]
        )

    def test_a_run_with_no_descendants_is_not_an_error(
        self, session: orm.Session
    ) -> None:
        run = _make_run(session=session, child_statuses=[])

        _unpark(session=session, run=run)

        assert _statuses_of(session=session, run=run) == collections.Counter()

    def test_nothing_is_committed(self, session: orm.Session) -> None:
        """The caller owns the transaction, which is what makes the un-park atomic.

        Committing here would open a window in which the node is launchable but the cancel
        flag has not been written -- exactly long enough for the orchestrator to start it.
        """
        run = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])

        _unpark(session=session, run=run)
        session.rollback()

        assert _statuses_of(session=session, run=run) == collections.Counter(
            [Status.UNINITIALIZED]
        )

    def test_another_run_is_untouched(self, session: orm.Session) -> None:
        cancelled = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])
        bystander = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])

        _unpark(session=session, run=cancelled)

        assert _statuses_of(session=session, run=bystander) == collections.Counter(
            [Status.UNINITIALIZED]
        )


# --- the patch ---------------------------------------------------------------


class TestInstall:
    """Cancelling through the real service method must un-park, and fail safely."""

    def test_cancelling_a_run_unparks_it(
        self, session: orm.Session, installed_patch: None
    ) -> None:
        run = _make_run(
            session=session,
            child_statuses=[Status.UNINITIALIZED, Status.RUNNING],
        )
        service = api_server_sql.PipelineRunsApiService_Sql()

        service.terminate(session=session, id=run.id, terminated_by=OWNER)

        assert _statuses_of(session=session, run=run) == collections.Counter(
            [Status.QUEUED, Status.RUNNING]
        )
        assert run.extra_data is not None
        assert run.extra_data["desired_state"] == "TERMINATED"

    def test_the_unparked_node_is_also_flagged_for_termination(
        self, session: orm.Session, installed_patch: None
    ) -> None:
        """The two halves must land together, or the node is launchable and unflagged.

        Because the un-park runs first, `terminate()`'s own filter -- which matches QUEUED
        -- now sees the node and writes `desired_state` on it. That is the whole reason the
        patch goes before the original rather than after it.
        """
        run = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])
        service = api_server_sql.PipelineRunsApiService_Sql()

        service.terminate(session=session, id=run.id, terminated_by=OWNER)

        session.expire_all()
        node = run.root_execution.descendants[0]
        assert node.container_execution_status == Status.QUEUED
        assert node.extra_data is not None
        assert node.extra_data["desired_state"] == "TERMINATED"

    def test_a_rejected_cancel_leaves_the_node_parked(
        self, session: orm.Session, installed_patch: None
    ) -> None:
        """A caller who does not own the run must change nothing at all.

        The un-park has already happened in the session when the permission check raises,
        so the guarantee comes from the transaction: no commit, no change.
        """
        run = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])
        service = api_server_sql.PipelineRunsApiService_Sql()

        with pytest.raises(errors.PermissionError):
            service.terminate(
                session=session,
                id=run.id,
                terminated_by="someone-else@example.com",
            )
        session.rollback()

        assert _statuses_of(session=session, run=run) == collections.Counter(
            [Status.UNINITIALIZED]
        )

    def test_an_unknown_run_still_raises(
        self, session: orm.Session, installed_patch: None
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()

        with pytest.raises(errors.ItemNotFoundError):
            service.terminate(session=session, id="no-such-run", terminated_by=OWNER)

    def test_skip_user_check_is_forwarded(
        self, session: orm.Session, installed_patch: None
    ) -> None:
        """The replacement forwards positionally, so a dropped argument shows up here."""
        run = _make_run(session=session, child_statuses=[Status.UNINITIALIZED])
        service = api_server_sql.PipelineRunsApiService_Sql()

        service.terminate(
            session=session,
            id=run.id,
            terminated_by="someone-else@example.com",
            skip_user_check=True,
        )

        assert _statuses_of(session=session, run=run) == collections.Counter(
            [Status.QUEUED]
        )

    def test_installing_twice_does_not_double_wrap(
        self, session: orm.Session, installed_patch: None
    ) -> None:
        first = api_server_sql.PipelineRunsApiService_Sql.terminate

        cancel_requeue.install()

        assert api_server_sql.PipelineRunsApiService_Sql.terminate is first

    def test_the_signature_is_preserved_for_callers(
        self, installed_patch: None
    ) -> None:
        """Keyword callers -- including the API route -- must keep working."""
        parameters = tuple(
            inspect.signature(
                api_server_sql.PipelineRunsApiService_Sql.terminate
            ).parameters
        )
        assert parameters == (
            "self",
            "session",
            "id",
            "terminated_by",
            "skip_user_check",
        )


class TestSignatureGuard:
    """A submodule bump that moves `terminate()` must fail loudly, not silently."""

    def test_a_changed_signature_refuses_to_install(self) -> None:
        original = api_server_sql.PipelineRunsApiService_Sql.terminate

        def terminate_with_a_new_parameter(
            self: Any,
            session: orm.Session,
            id: str,  # noqa: A002
            terminated_by: str | None = None,
            skip_user_check: bool = False,
            reason: str | None = None,
        ) -> None: ...

        api_server_sql.PipelineRunsApiService_Sql.terminate = (
            terminate_with_a_new_parameter
        )
        try:
            with pytest.raises(RuntimeError, match="changed"):
                cancel_requeue.install()
        finally:
            api_server_sql.PipelineRunsApiService_Sql.terminate = original

    def test_the_current_submodule_signature_is_the_expected_one(self) -> None:
        """Guards the guard: if this fails, the constant is stale, not the submodule."""
        cancel_requeue._assert_terminate_signature_unchanged(
            original=api_server_sql.PipelineRunsApiService_Sql.terminate
        )
