import dataclasses
import datetime
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import orm

from .. import backend_types_sql as bts
from .. import component_structures as structures
from . import filter_query_sql

_DEFAULT_PAGE_SIZE: Final[int] = 10
_PIPELINE_NAME_EXTRA_DATA_KEY: Final[str] = "pipeline_name"


@dataclasses.dataclass(kw_only=True)
class PipelineRunResponse:
    id: bts.IdType
    root_execution_id: bts.IdType
    annotations: dict[str, Any] | None = None
    created_by: str | None = None
    created_at: datetime.datetime | None = None
    pipeline_name: str | None = None
    execution_status_stats: dict[str, int] | None = None

    @classmethod
    def from_db(cls, pipeline_run: bts.PipelineRun) -> "PipelineRunResponse":
        return PipelineRunResponse(
            id=pipeline_run.id,
            root_execution_id=pipeline_run.root_execution_id,
            annotations=pipeline_run.annotations,
            created_by=pipeline_run.created_by,
            created_at=pipeline_run.created_at,
        )


@dataclasses.dataclass(kw_only=True)
class ListPipelineJobsResponse:
    pipeline_runs: list[PipelineRunResponse]
    next_page_token: str | None = None


def get_pipeline_name_from_task_spec(
    *,
    task_spec_dict: dict[str, Any],
) -> str | None:
    """Extract pipeline name from a task_spec dict via component_ref.spec.name.

    Traversal path:
        task_spec_dict -> TaskSpec -> component_ref -> spec -> name

    Returns None if any step in the chain is missing or parsing fails.
    """
    try:
        task_spec = structures.TaskSpec.from_json_dict(task_spec_dict)
    except Exception:
        return None
    spec = task_spec.component_ref.spec
    if spec is None:
        return None
    return spec.name or None


def _query_pipeline_runs(
    *,
    session: orm.Session,
    where_clauses: list[sql.ColumnElement],
    offset: int,
    page_size: int,
) -> list[bts.PipelineRun]:
    return list(
        session.scalars(
            sql.select(bts.PipelineRun)
            .where(*where_clauses)
            .order_by(bts.PipelineRun.created_at.desc())
            .offset(offset)
            .limit(page_size)
        ).all()
    )


def _calculate_execution_status_stats(
    *,
    session: orm.Session,
    root_execution_id: bts.IdType,
) -> dict[bts.ContainerExecutionStatus, int]:
    query = (
        sql.select(
            bts.ExecutionNode.container_execution_status,
            sql.func.count().label("count"),
        )
        .join(
            bts.ExecutionToAncestorExecutionLink,
            bts.ExecutionToAncestorExecutionLink.execution_id == bts.ExecutionNode.id,
        )
        .where(
            bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
            == root_execution_id
        )
        .where(bts.ExecutionNode.container_execution_status != None)
        .group_by(
            bts.ExecutionNode.container_execution_status,
        )
    )
    execution_status_stat_rows = session.execute(query).tuples().all()
    return dict(execution_status_stat_rows)


def _create_pipeline_run_response(
    *,
    session: orm.Session,
    pipeline_run: bts.PipelineRun,
    include_pipeline_names: bool,
    include_execution_stats: bool,
) -> PipelineRunResponse:
    response = PipelineRunResponse.from_db(pipeline_run)
    if include_pipeline_names:
        pipeline_name = None
        extra_data = pipeline_run.extra_data or {}
        if _PIPELINE_NAME_EXTRA_DATA_KEY in extra_data:
            pipeline_name = extra_data[_PIPELINE_NAME_EXTRA_DATA_KEY]
        else:
            execution_node = session.get(
                bts.ExecutionNode, pipeline_run.root_execution_id
            )
            if execution_node:
                pipeline_name = get_pipeline_name_from_task_spec(
                    task_spec_dict=execution_node.task_spec
                )
        response.pipeline_name = pipeline_name
    if include_execution_stats:
        execution_status_stats = _calculate_execution_status_stats(
            session=session, root_execution_id=pipeline_run.root_execution_id
        )
        response.execution_status_stats = {
            status.value: count for status, count in execution_status_stats.items()
        }
    return response


def get_pipeline_runs(
    *,
    session: orm.Session,
    page_token: str | None = None,
    filter: str | None = None,
    filter_query: str | None = None,
    current_user: str | None = None,
    include_pipeline_names: bool = False,
    include_execution_stats: bool = False,
) -> ListPipelineJobsResponse:
    where_clauses, offset, next_token = filter_query_sql.build_list_filters(
        filter_value=filter,
        filter_query_value=filter_query,
        page_token_value=page_token,
        current_user=current_user,
        page_size=_DEFAULT_PAGE_SIZE,
    )

    pipeline_runs = _query_pipeline_runs(
        session=session,
        where_clauses=where_clauses,
        offset=offset,
        page_size=_DEFAULT_PAGE_SIZE,
    )

    next_page_token = next_token if len(pipeline_runs) >= _DEFAULT_PAGE_SIZE else None

    return ListPipelineJobsResponse(
        pipeline_runs=[
            _create_pipeline_run_response(
                session=session,
                pipeline_run=pipeline_run,
                include_pipeline_names=include_pipeline_names,
                include_execution_stats=include_execution_stats,
            )
            for pipeline_run in pipeline_runs
        ],
        next_page_token=next_page_token,
    )
