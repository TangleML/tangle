"""Derives whether a pipeline run has ended and, if so, whether it ended cleanly.

An emission row records only what the producer can observe atomically about a single node:
the node, its container execution, the status it changed to, the id of the run that owns it,
and the emission type. Aggregate run state is derived here instead, at read time, from the
run's executions as they stand when a reader asks.
"""

import dataclasses
import enum

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts


class RunEndedStatus(str, enum.Enum):
    """The aggregate status of a run that has ended.

    The members are bound to `ContainerExecutionStatus` values so the two vocabularies
    cannot drift: a run's status is spelled exactly like the execution statuses it is
    derived from.
    """

    SUCCEEDED = bts.ContainerExecutionStatus.SUCCEEDED.value
    FAILED = bts.ContainerExecutionStatus.FAILED.value


# The execution statuses that leave a run clean. SKIPPED belongs here because it is the
# conditional-execution path: a node whose condition did not hold is skipped by design, not
# failed. Every other ended status (FAILED, CANCELLED, SYSTEM_ERROR, INVALID) is counted in
# status_stats too, so a run containing one of them cannot reach the clean total below.
# Clean is necessary but not sufficient: a run also needs one execution that succeeded.
_CLEAN_STATUSES: frozenset[str] = frozenset(
    {
        bts.ContainerExecutionStatus.SUCCEEDED.value,
        bts.ContainerExecutionStatus.SKIPPED.value,
    }
)


@dataclasses.dataclass(frozen=True)
class RunEndedState:
    """Whether a run has ended and, when it has, how it ended.

    Attributes:
        has_ended: True only when every execution in the run is in an ended status.
        status: the aggregate status, set only when `has_ended` is True and None otherwise.
    """

    has_ended: bool
    status: RunEndedStatus | None


class RunStatusCache:
    """Whether each pipeline run has ended, caching only the verdicts that cannot change.

    An ended run stays ended, so that verdict is reused for the life of the process; "still
    running" is never cached, because it goes stale a moment later.

    One exception: the admin route `PUT /api/admin/execution_node/{id}/status`
    (`cloud_pipelines_backend/api_router.py`) can force a node out of a terminal status,
    and that is not seen here until the process restarts. Every other write only advances a
    status.

    Unbounded on purpose: ~200 bytes per ended run, and every deployment restarts the process.

    Answers through one `PipelineRunsApiService_Sql`, whose per-status counts are what the API
    server itself reports for a run, so both answer the question the same way.
    """

    def __init__(self) -> None:
        """Start with an empty cache."""
        self._run_summary_service = api_server_sql.PipelineRunsApiService_Sql()
        # Run id -> terminal verdict. Runs still in flight are deliberately absent.
        self._ended: dict[str, RunEndedState] = {}

    def has_ended(
        self,
        *,
        session: orm.Session,
        pipeline_run_id: str | None,
    ) -> RunEndedState:
        """Return whether the run has ended and, if so, its aggregate status.

        A run is SUCCEEDED when every one of its executions is in (SUCCEEDED, SKIPPED) and
        at least one of them succeeded, and FAILED otherwise. A run whose executions are all
        skipped is therefore FAILED: no work was carried out, so there is no success to
        report even though nothing went wrong.

        Takes the run id rather than a node id: a caller holding an emission row already has
        `pipeline_run_id` on it, so resolving a node to its run stays the producer's job.

        Args:
            session: the session to read through, passed per call because a caller's session may
                carry pending state only it can see; a caller wanting its own uncommitted writes
                counted must flush them first.
            pipeline_run_id: the run to resolve, or None when the caller has no run id.

        Returns:
            A RunEndedState. `has_ended=False, status=None` when there is no run id, the run
            does not exist, it has no executions, or at least one execution is still in
            flight; otherwise `has_ended=True` with the aggregate status.
        """
        if pipeline_run_id is None:
            return RunEndedState(has_ended=False, status=None)

        state = self._ended.get(pipeline_run_id)
        if state is not None:
            return state

        state = self._resolve(session=session, pipeline_run_id=pipeline_run_id)
        # Only terminal verdicts are stored; see the class docstring.
        if state.has_ended:
            self._ended[pipeline_run_id] = state
        return state

    def _resolve(
        self,
        *,
        session: orm.Session,
        pipeline_run_id: str,
    ) -> RunEndedState:
        """Read the run's executions and derive its ended state, always querying.

        Args:
            session: the session to read through.
            pipeline_run_id: the run to resolve; never None here, the caller has checked.

        Returns:
            The run's ended state as this session currently sees it.
        """
        root_execution_id = session.scalar(
            sql.select(bts.PipelineRun.root_execution_id).where(
                bts.PipelineRun.id == pipeline_run_id
            )
        )
        if root_execution_id is None:
            return RunEndedState(has_ended=False, status=None)

        # status_stats is keyed by the status `.value` string, e.g. {"SUCCEEDED": 3,
        # "FAILED": 1}; summary carries the totals the "has ended" test needs.
        status_stats, summary = (
            self._run_summary_service._get_execution_stats_and_summary(
                session=session,
                root_execution_id=root_execution_id,
            )
        )
        if summary.total_executions == 0 or not summary.has_ended:
            return RunEndedState(has_ended=False, status=None)

        clean_count = sum(status_stats.get(status, 0) for status in _CLEAN_STATUSES)
        succeeded_count = status_stats.get(
            bts.ContainerExecutionStatus.SUCCEEDED.value, 0
        )
        status = (
            RunEndedStatus.SUCCEEDED
            if clean_count == summary.total_executions and succeeded_count > 0
            else RunEndedStatus.FAILED
        )
        return RunEndedState(has_ended=True, status=status)
