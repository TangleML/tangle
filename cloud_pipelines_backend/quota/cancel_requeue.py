"""Un-parking on cancel: giving a cancelled run's parked executions a way to die.

**The bug.** A parked execution sits at `UNINITIALIZED`, and the orchestrator sweep only
selects `QUEUED`. `terminate()` only flags QUEUED / WAITING_FOR_UPSTREAM / PENDING /
RUNNING, so it walks straight past a parked node:

    cancel -> node stays UNINITIALIZED -> never swept -> run never terminal
           -> claim stays WAITING      -> slot never freed -> group wedged

**The fix.** Put parked descendants back to `QUEUED` just before the cancel is written.
Everything after that is pre-existing upstream code:

    QUEUED + desired_state=TERMINATED -> swept -> CANCELLED, downstream skipped
    (decided before the quota gate, so nothing launches; CANCELLED frees the claim)

**Why a patch.** `terminate()` lives in the backend submodule and
`api_router.setup_routes` constructs `PipelineRunsApiService_Sql()` itself -- nothing to
subclass, nothing to inject. `QuotaUnparkOnTerminate` is shaped as the upstream
`RunTerminationInterceptor` protocol, so adopting the real hook is a wiring change.

TODO(quota-groups): when the upstream seam lands, delete `install()` and instead pass
`run_termination_interceptor=QuotaUnparkOnTerminate()` to `api_router.setup_routes`.
"""

import functools
import inspect
import logging
import typing

from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts

_logger = logging.getLogger(__name__)


class QuotaUnparkOnTerminate:
    """Un-park a cancelled run's parked executions, inside the cancelling transaction.

    Shaped as the upstream `RunTerminationInterceptor` protocol before that protocol
    exists, so switching from the patch in `install()` to a real injected hook is a
    wiring change and touches nothing else.
    """

    def on_terminate(
        self,
        *,
        session: orm.Session,
        pipeline_run: bts.PipelineRun,
    ) -> None:
        """Move this run's parked executions back onto the launch path.

        Walks `root_execution.descendants`, the same traversal `terminate()` uses, so the
        two stay in lockstep: every execution `terminate()` flags is an execution this
        considers, and the only ones it moves are the parked ones `terminate()` cannot see.

        Nothing is committed. The caller's transaction owns the change, which is what
        makes the un-park atomic with the cancel -- there is no window in which the run is
        `TERMINATED` but one of its nodes is still invisible.

        Args:
            session: The session the cancel is being written in. Unused -- the run is
                already attached to it -- but part of the protocol's shape.
            pipeline_run: The run being cancelled.
        """
        parked = [
            node
            for node in pipeline_run.root_execution.descendants
            if node.container_execution_status
            == bts.ContainerExecutionStatus.UNINITIALIZED
        ]
        for node in parked:
            node.container_execution_status = bts.ContainerExecutionStatus.QUEUED

        if parked:
            _logger.info(
                f"Quota un-park on cancel run_id={pipeline_run.id} "
                f"unparked={len(parked)} nodes={[node.id for node in parked]}"
            )


def _assert_terminate_signature_unchanged(
    *,
    original: typing.Callable[..., typing.Any],
) -> None:
    """Fail at install time if the submodule's `terminate()` has moved under us.

    A patch's one real hazard is drifting quietly. The replacement forwards positionally,
    so a new, removed or reordered parameter would not raise -- it would pass the wrong
    value, or ignore one that has started to matter. This turns that into a startup error
    naming the file to fix.

    Args:
        original: The unpatched `PipelineRunsApiService_Sql.terminate`.

    Raises:
        RuntimeError: If the parameter list is not the one this module was written against.
    """
    # The exact parameter list the replacement below calls positionally.
    expected = ("self", "session", "id", "terminated_by", "skip_user_check")
    actual = tuple(inspect.signature(original).parameters)
    if actual != expected:
        raise RuntimeError(
            "The backend submodule changed PipelineRunsApiService_Sql.terminate, which "
            "quota/cancel_requeue.py patches. Re-check that file before bumping. "
            f"expected={expected} actual={actual}"
        )


def install() -> None:
    """Patch `PipelineRunsApiService_Sql.terminate` to un-park before it cancels.

    The un-park runs *before* the original, not after, and that ordering is the whole
    safety argument:

    - The original commits at the end, so the un-park rides that same commit and is
      atomic with the cancel.
    - If the original raises -- unknown run, or a caller who does not own it -- the
      request fails before any commit, the session is closed by FastAPI's dependency
      without one, and the un-park is rolled back with everything else.

    Patching the class rather than an instance is deliberate: `setup_routes` builds its
    own `PipelineRunsApiService_Sql()`, and three more are constructed ad hoc elsewhere
    in this service. A class-level patch covers all of them, including any added later.

    Calling this twice is a no-op, so import order and test setup cannot double-wrap.
    """
    # Set on the replacement below once installed, and read back off whatever is currently
    # bound to the class. That is what makes this idempotent: a second call -- from a
    # re-import, a test, or a future second entry point -- sees the marker and returns
    # instead of wrapping the wrapper and un-parking twice.
    patch_marker = "_quota_unpark_on_terminate"
    original = api_server_sql.PipelineRunsApiService_Sql.terminate
    if getattr(original, patch_marker, False):
        return

    _assert_terminate_signature_unchanged(original=original)
    interceptor = QuotaUnparkOnTerminate()

    @functools.wraps(original)
    def terminate_with_unpark(
        self: api_server_sql.PipelineRunsApiService_Sql,
        session: orm.Session,
        id: bts.IdType,  # noqa: A002  # mirrors the upstream signature; callers pass it by name
        terminated_by: str | None = None,
        skip_user_check: bool = False,
    ) -> typing.Any:
        pipeline_run = session.get(bts.PipelineRun, id)
        if pipeline_run is not None:
            interceptor.on_terminate(session=session, pipeline_run=pipeline_run)
        return original(self, session, id, terminated_by, skip_user_check)

    setattr(terminate_with_unpark, patch_marker, True)
    api_server_sql.PipelineRunsApiService_Sql.terminate = terminate_with_unpark
    _logger.info(
        "Quota un-park on cancel installed on PipelineRunsApiService_Sql.terminate"
    )
