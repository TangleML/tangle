"""Utilities for computing and updating the denormalized PipelineRun.status column.

Pipeline run status is the highest-priority ContainerExecutionStatus among all
descendant execution nodes, using the priority mapping below (lower number wins).
"""

import logging

import sqlalchemy as sql
from sqlalchemy import orm

from . import backend_types_sql as bts

_logger = logging.getLogger(__name__)

STATUS_PRIORITY: dict[bts.ContainerExecutionStatus, int] = {
    bts.ContainerExecutionStatus.SYSTEM_ERROR: 1,
    bts.ContainerExecutionStatus.FAILED: 2,
    bts.ContainerExecutionStatus.INVALID: 3,
    bts.ContainerExecutionStatus.CANCELLING: 4,
    bts.ContainerExecutionStatus.CANCELLED: 5,
    bts.ContainerExecutionStatus.RUNNING: 6,
    bts.ContainerExecutionStatus.PENDING: 7,
    bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM: 8,
    bts.ContainerExecutionStatus.QUEUED: 9,
    bts.ContainerExecutionStatus.UNINITIALIZED: 10,
    bts.ContainerExecutionStatus.SKIPPED: 11,
    bts.ContainerExecutionStatus.SUCCEEDED: 12,
}

PRIORITY_TO_STATUS: dict[int, bts.ContainerExecutionStatus] = {
    v: k for k, v in STATUS_PRIORITY.items()
}


def update_pipeline_run_status(
    session: orm.Session,
    execution_id: bts.IdType,
) -> None:
    """Recompute and persist pipeline run status for the run containing this execution.

    Finds the pipeline run via the execution_ancestor table (or direct root match),
    then aggregates all descendant execution statuses using the priority system.
    """
    pipeline_run = _find_pipeline_run_for_execution(session, execution_id)
    if pipeline_run is None:
        return

    _recompute_and_persist_status(session, pipeline_run)


def _find_pipeline_run_for_execution(
    session: orm.Session,
    execution_id: bts.IdType,
) -> bts.PipelineRun | None:
    """Find the pipeline run that owns the given execution node."""
    # The execution might be the root itself
    pipeline_run = session.scalar(
        sql.select(bts.PipelineRun).where(
            bts.PipelineRun.root_execution_id == execution_id
        )
    )
    if pipeline_run is not None:
        return pipeline_run

    # Otherwise, look through the ancestor table
    return session.scalar(
        sql.select(bts.PipelineRun)
        .join(
            bts.ExecutionToAncestorExecutionLink,
            bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
            == bts.PipelineRun.root_execution_id,
        )
        .where(bts.ExecutionToAncestorExecutionLink.execution_id == execution_id)
        .limit(1)
    )


def _recompute_and_persist_status(
    session: orm.Session,
    pipeline_run: bts.PipelineRun,
) -> None:
    """Aggregate descendant execution statuses and update the pipeline run."""
    rows = (
        session.execute(
            sql.select(
                bts.ExecutionNode.container_execution_status,
                sql.func.count().label("cnt"),
            )
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.execution_id
                == bts.ExecutionNode.id,
            )
            .where(
                bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                == pipeline_run.root_execution_id
            )
            .where(bts.ExecutionNode.container_execution_status != None)
            .group_by(bts.ExecutionNode.container_execution_status)
        )
        .tuples()
        .all()
    )

    if not rows:
        return

    best_priority = 99
    total = 0
    ended = 0
    for status, count in rows:
        priority = STATUS_PRIORITY.get(status, 99)
        if priority < best_priority:
            best_priority = priority
        total += count
        if status in bts.CONTAINER_STATUSES_ENDED:
            ended += count

    new_status = PRIORITY_TO_STATUS.get(best_priority)
    new_has_ended = total > 0 and ended == total

    pipeline_run.status = new_status
    pipeline_run.has_ended = new_has_ended
