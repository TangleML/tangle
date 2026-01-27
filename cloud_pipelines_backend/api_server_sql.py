import base64
import dataclasses
import datetime
import enum
import json
import logging
import typing
from typing import Any, Optional

import pydantic  # type: ignore[import-unresolved]

if typing.TYPE_CHECKING:
    from cloud_pipelines.orchestration.storage_providers import (
        interfaces as storage_provider_interfaces,
    )
    from .launchers import interfaces as launcher_interfaces


_logger = logging.getLogger(__name__)

T = typing.TypeVar("T")


class ApiServiceError(RuntimeError):
    pass


def _get_current_time() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


from . import component_structures as structures
from . import backend_types_sql as bts
from . import errors
from .errors import ItemNotFoundError

# ==== Annotation Filter Types for Search ====


class GroupOperator(enum.StrEnum):
    """Logical operators for combining filters in a group."""

    AND = "and"
    OR = "or"


class FilterOperator(enum.StrEnum):
    """Operators for filtering by annotation key or value."""

    CONTAINS = "contains"  # Contains substring
    EQUALS = "equals"  # Equals exact string
    IN_SET = "in_set"  # Is in a set of values


class TextFilter(pydantic.BaseModel):
    """Filter by text pattern.

    Examples:
        - TextFilter(operator=FilterOperator.EQUALS, text="environment")
        - TextFilter(operator=FilterOperator.CONTAINS, text="env")
        - TextFilter(operator=FilterOperator.IN_SET, texts=["prod", "staging"])
        - TextFilter(operator=FilterOperator.EQUALS, text="test", negate=True)
    """

    operator: FilterOperator
    text: str | None = None  # For EQUALS, CONTAINS operators
    texts: list[str] | None = None  # For IN_SET operator
    negate: bool = False  # If True, negates the operation


class AnnotationFilter(pydantic.BaseModel):
    """Filter annotations by key and optionally by value.

    Examples:
        - AnnotationFilter(key=TextFilter(operator=FilterOperator.EQUALS, text="environment"))
          → Find runs that have an "environment" annotation key
        - AnnotationFilter(
              key=TextFilter(operator=FilterOperator.EQUALS, text="environment"),
              value=TextFilter(operator=FilterOperator.EQUALS, text="production")
          )
          → Find runs with annotation key "environment" and value "production"
    """

    key: TextFilter
    value: TextFilter | None = None


class AnnotationFilterGroup(pydantic.BaseModel):
    """A flat group of AnnotationFilters combined with AND/OR logic.

    Each AnnotationFilter matches a single annotation row (key + optional value).
    The group operator determines how multiple filters are combined.

    Examples:
        - AnnotationFilterGroup(filters=[...])
          → Defaults to OR logic (match ANY filter)
        - AnnotationFilterGroup(operator=GroupOperator.AND, filters=[...])
          → All filters must match (match ALL filters)
        - AnnotationFilterGroup(operator=GroupOperator.OR, filters=[...])
          → At least one filter must match

    SQL Pattern:
        filters=[AF(key="env", value="prod"), AF(key="team")]
        → EXISTS(key='env' AND value='prod') OR EXISTS(key='team')
    """

    filters: list[AnnotationFilter]
    # Operator defaults to None, which is treated as OR logic.
    operator: GroupOperator | None = None


# ==== PipelineJobService
@dataclasses.dataclass(kw_only=True)
class PipelineRunResponse:
    id: bts.IdType
    root_execution_id: bts.IdType
    annotations: dict[str, Any] | None = None
    # status: "PipelineJobStatus"
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


class GetPipelineRunResponse(PipelineRunResponse):
    pass


@dataclasses.dataclass(kw_only=True)
class ListPipelineJobsResponse:
    pipeline_runs: list[PipelineRunResponse]
    next_page_token: str | None = None
    debug_where_clause: str | None = (
        None  # Populated when debug_where_clause=True in search()
    )


import sqlalchemy as sql
from sqlalchemy import orm


def _compile_where_clauses_to_string(
    session: orm.Session,
    where_clauses: list[sql.ColumnElement[bool]],
) -> str:
    """Compile WHERE clauses to a SQL string for debugging.

    Uses the dialect from the session's engine (SQLite, MySQL, etc.)
    and inlines literal values for readability.
    """
    if not where_clauses:
        return "(no where clauses)"

    # Combine all clauses with AND
    combined = sql.and_(*where_clauses) if len(where_clauses) > 1 else where_clauses[0]

    # Get dialect from session's engine
    dialect = session.bind.dialect if session.bind else None

    try:
        compiled = combined.compile(
            dialect=dialect,
            compile_kwargs={"literal_binds": True},
        )
        return str(compiled)
    except Exception as e:
        # Fallback if literal_binds fails (e.g., for complex types)
        try:
            compiled = combined.compile(dialect=dialect)
            return f"{compiled} [params: {compiled.params}]"
        except Exception:
            return f"(failed to compile: {e})"


class PipelineRunsApiService_Sql:
    PIPELINE_NAME_EXTRA_DATA_KEY = "pipeline_name"

    def _build_annotation_filter(
        self,
        *,
        filter: TextFilter,
        column: sql.ColumnElement[str],
    ) -> sql.ColumnElement[bool]:
        """Build a SQLAlchemy condition for a TextFilter.

        Args:
            filter: TextFilter with operator, text/texts, and optional negate flag.
            column: The SQLAlchemy column to apply the filter to.

        Returns:
            A SQLAlchemy condition expression for the filter.
        """
        if filter.operator == FilterOperator.EQUALS:
            if filter.text is None:
                raise ValueError("EQUALS operator requires 'text' to be set")
            condition = column == filter.text
        elif filter.operator == FilterOperator.CONTAINS:
            if filter.text is None:
                raise ValueError("CONTAINS operator requires 'text' to be set")
            condition = column.contains(filter.text)
        elif filter.operator == FilterOperator.IN_SET:
            if not filter.texts:
                raise ValueError("IN_SET operator requires 'texts' to be set")
            condition = column.in_(filter.texts)
        else:
            raise ValueError(f"Unknown FilterOperator: {filter.operator}")

        if filter.negate:
            condition = ~condition

        return condition

    def _build_annotation_where_clauses(
        self,
        *,
        filters: AnnotationFilterGroup | None,
    ) -> list[sql.ColumnElement[bool]]:
        """Build WHERE clauses from AnnotationFilterGroup.

        For annotation filters, each AnnotationFilter creates an EXISTS subquery
        that checks the key (and optionally value) on the SAME annotation row.
        Multiple AnnotationFilters are combined with the group operator (OR by default).

        Example: filters=[AF(key="env", value="prod"), AF(key="team", value="ml")]
        Produces: EXISTS(...key='env' AND value='prod') OR EXISTS(...key='team' AND value='ml')
        """
        where_clauses: list[sql.ColumnElement[bool]] = []

        if filters is None or not filters.filters:
            return where_clauses

        # Build EXISTS clause for each AnnotationFilter
        exists_clauses: list[sql.ColumnElement[bool]] = []
        for af in filters.filters:
            # Base subquery joining to parent PipelineRun
            subquery = sql.select(bts.PipelineRunAnnotation).where(
                bts.PipelineRunAnnotation.pipeline_run_id == bts.PipelineRun.id
            )

            # Add key filter (required)
            subquery = subquery.where(
                self._build_annotation_filter(
                    filter=af.key, column=bts.PipelineRunAnnotation.key
                )
            )

            # Add value filter (optional) - same row semantics
            if af.value is not None:
                subquery = subquery.where(
                    self._build_annotation_filter(
                        filter=af.value, column=bts.PipelineRunAnnotation.value
                    )
                )

            exists_clauses.append(subquery.exists())

        # Combine EXISTS clauses with group operator
        if len(exists_clauses) == 1:
            where_clauses.append(exists_clauses[0])
        elif filters.operator == GroupOperator.AND:
            where_clauses.append(sql.and_(*exists_clauses))
        else:
            # Default to OR if operator is not specified
            where_clauses.append(sql.or_(*exists_clauses))

        return where_clauses

    def create(
        self,
        session: orm.Session,
        root_task: structures.TaskSpec,
        # Component library to avoid repeating component specs inside task specs
        components: Optional[list[structures.ComponentReference]] = None,
        # Arbitrary metadata. Can be used to specify user.
        annotations: Optional[dict[str, Any]] = None,
        created_by: str | None = None,
    ) -> PipelineRunResponse:
        # TODO: Validate the pipeline spec
        # TODO: Load and validate all components
        # TODO: Fetch missing components and populate component specs

        pipeline_name = root_task.component_ref.spec.name

        with session.begin():

            root_execution_node = _recursively_create_all_executions_and_artifacts_root(
                session=session,
                root_task_spec=root_task,
            )

            # Store into DB.
            current_time = _get_current_time()
            pipeline_run = bts.PipelineRun(
                root_execution=root_execution_node,
                created_at=current_time,
                updated_at=current_time,
                annotations=annotations,
                created_by=created_by,
                extra_data={
                    self.PIPELINE_NAME_EXTRA_DATA_KEY: pipeline_name,
                },
            )
            session.add(pipeline_run)
            session.commit()

        session.refresh(pipeline_run)
        return PipelineRunResponse.from_db(pipeline_run)

    def get(self, session: orm.Session, id: bts.IdType) -> PipelineRunResponse:
        pipeline_run = session.get(bts.PipelineRun, id)
        if not pipeline_run:
            raise ItemNotFoundError(f"Pipeline run {id} not found.")
        return PipelineRunResponse.from_db(pipeline_run)

    def terminate(
        self,
        session: orm.Session,
        id: bts.IdType,
        terminated_by: str | None = None,
        skip_user_check: bool = False,
    ):
        pipeline_run = session.get(bts.PipelineRun, id)
        if not pipeline_run:
            raise ItemNotFoundError(f"Pipeline run {id} not found.")
        if not skip_user_check and (terminated_by != pipeline_run.created_by):
            raise errors.PermissionError(
                f"The pipeline run {id} was started by {pipeline_run.created_by} and cannot be terminated by {terminated_by}"
            )
        # Marking the pipeline run for termination
        if pipeline_run.extra_data is None:
            pipeline_run.extra_data = {}
        pipeline_run.extra_data["desired_state"] = "TERMINATED"

        # Marking all running executions belonging to the run for termination
        running_execution_nodes = [
            execution_node
            for execution_node in pipeline_run.root_execution.descendants
            if execution_node.container_execution_status
            in (
                bts.ContainerExecutionStatus.QUEUED,
                bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM,
                bts.ContainerExecutionStatus.PENDING,
                bts.ContainerExecutionStatus.RUNNING,
            )
        ]
        for execution_node in running_execution_nodes:
            if execution_node.extra_data is None:
                execution_node.extra_data = {}
            execution_node.extra_data["desired_state"] = "TERMINATED"
        session.commit()

    # Note: This method must be last to not shadow the "list" type
    def list(
        self,
        *,
        session: orm.Session,
        page_token: str | None = None,
        filter: str | None = None,
        current_user: str | None = None,
        annotation_filter: str | None = None,
        include_pipeline_names: bool = False,
        include_execution_stats: bool = False,
        debug_where_clause: bool = False,
    ) -> ListPipelineJobsResponse:
        """List pipeline runs with optional filtering.

        **Parameters:**

        - **filter**: Simple key:value filter string (e.g., "created_by:alice")
        - **annotation_filter**: JSON string for annotation filtering
        - **page_token**: Pagination token from previous response
        - **include_pipeline_names**: Include pipeline names in response
        - **include_execution_stats**: Include execution statistics
        - **debug_where_clause**: If True, includes compiled SQL WHERE clause

        **Returns:** `ListPipelineJobsResponse` with matching pipeline runs.

        ---

        ## API Usage

        ### Annotation Filter JSON Format

        The `annotation_filter` parameter accepts a JSON-encoded string.

        ### TextFilter Operators

        | Operator | Description |
        |----------|-------------|
        | `equals` | Exactly matches string (`text` field) |
        | `contains` | Contains substring (`text` field) |
        | `in_set` | Matches one of multiple values (`texts` field) |

        All filters support `negate: true` to invert the condition.

        ---

        ### Example 1: Key contains substring

        Find runs with annotation key containing "env":

        ```json
        {"filters": [{"key": {"operator": "contains", "text": "env"}}]}
        ```

        ### Example 2: Key-value pair with in_set

        Find runs where key is "environment" and value is one of ["prod", "staging"]:

        ```json
        {
          "filters": [
            {
              "key": {"operator": "equals", "text": "environment"},
              "value": {"operator": "in_set", "texts": ["prod", "staging"]}
            }
          ]
        }
        ```

        ### Example 3: Multiple filters with OR (default) + negate

        Find runs where key contains "env" OR key is NOT "deprecated":

        ```json
        {
          "filters": [
            {"key": {"operator": "contains", "text": "env"}},
            {"key": {"operator": "equals", "text": "deprecated", "negate": true}}
          ]
        }
        ```

        ### Example 4: Multiple filters with AND

        Find runs that have BOTH conditions:

        ```json
        {
          "operator": "and",
          "filters": [
            {
              "key": {"operator": "equals", "text": "environment"},
              "value": {"operator": "contains", "text": "prod"}
            },
            {"key": {"operator": "contains", "text": "team"}}
          ]
        }
        ```

        ---

        **Performance Note:**

        WHERE clause ordering: Simple column filters (indexed) are applied first,
        then EXISTS subqueries (annotation filters). Database optimizer may reorder,
        but explicit ordering documents intent and helps simpler optimizers.
        """
        page_token_dict = _decode_page_token(page_token)
        OFFSET_KEY = "offset"
        offset = page_token_dict.get(OFFSET_KEY, 0)
        page_size = 10

        FILTER_KEY = "filter"
        if page_token:
            filter = page_token_dict.get(FILTER_KEY, None)

        # Parse annotation_filter JSON string if provided
        parsed_annotation_filter: AnnotationFilterGroup | None = None
        if annotation_filter:
            try:
                parsed_annotation_filter = AnnotationFilterGroup.model_validate_json(
                    annotation_filter
                )
            except pydantic.ValidationError as e:
                raise ApiServiceError(f"Invalid annotation_filter: {e}")

        where_clauses: list[sql.ColumnElement[bool]] = []
        parsed_filter = _parse_filter(filter) if filter else {}
        for key, value in parsed_filter.items():
            if key == "_text":
                raise NotImplementedError("Text search is not implemented yet.")
            elif key == "created_by":
                if value == "me":
                    if current_user is None:
                        current_user = ""
                    value = current_user
                    # TODO: Maybe make this a bit more robust.
                    # We need to change the filter since it goes into the next_page_token.
                    filter = filter.replace(
                        "created_by:me", f"created_by:{current_user}"
                    )
                if value:
                    where_clauses.append(bts.PipelineRun.created_by == value)
                else:
                    where_clauses.append(bts.PipelineRun.created_by == None)
            else:
                raise NotImplementedError(f"Unsupported filter {filter}.")

        if parsed_annotation_filter:
            annotation_clauses = self._build_annotation_where_clauses(
                filters=parsed_annotation_filter
            )
            where_clauses.extend(annotation_clauses)

        pipeline_runs = list(
            session.scalars(
                sql.select(bts.PipelineRun)
                .where(*where_clauses)
                .order_by(bts.PipelineRun.created_at.desc())
                .offset(offset)
                .limit(page_size)
            ).all()
        )

        next_page_offset = offset + page_size
        next_page_token_dict = {OFFSET_KEY: next_page_offset, FILTER_KEY: filter}
        next_page_token = _encode_page_token(next_page_token_dict)
        if len(pipeline_runs) < page_size:
            next_page_token = None

        def create_pipeline_run_response(
            pipeline_run: bts.PipelineRun,
        ) -> PipelineRunResponse:
            response = PipelineRunResponse.from_db(pipeline_run)
            if include_pipeline_names:
                pipeline_name = None
                extra_data = pipeline_run.extra_data or {}
                if self.PIPELINE_NAME_EXTRA_DATA_KEY in extra_data:
                    pipeline_name = extra_data[self.PIPELINE_NAME_EXTRA_DATA_KEY]
                else:
                    execution_node = session.get(
                        bts.ExecutionNode, pipeline_run.root_execution_id
                    )
                    if execution_node:
                        task_spec = structures.TaskSpec.from_json_dict(
                            execution_node.task_spec
                        )
                        component_spec = task_spec.component_ref.spec
                        if component_spec:
                            pipeline_name = component_spec.name
                response.pipeline_name = pipeline_name
            if include_execution_stats:
                execution_status_stats = self._calculate_execution_status_stats(
                    session=session, root_execution_id=pipeline_run.root_execution_id
                )
                response.execution_status_stats = {
                    status.value: count
                    for status, count in execution_status_stats.items()
                }
            return response

        response = ListPipelineJobsResponse(
            pipeline_runs=[
                create_pipeline_run_response(pipeline_run)
                for pipeline_run in pipeline_runs
            ],
            next_page_token=next_page_token,
        )

        if debug_where_clause:
            response.debug_where_clause = _compile_where_clauses_to_string(
                session=session,
                where_clauses=where_clauses,
            )

        return response

    def _calculate_execution_status_stats(
        self, session: orm.Session, root_execution_id: bts.IdType
    ) -> dict[bts.ContainerExecutionStatus, int]:
        query = (
            sql.select(
                bts.ExecutionNode.container_execution_status,
                sql.func.count().label("count"),
            )
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.execution_id
                == bts.ExecutionNode.id,
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
        execution_status_stats = dict(execution_status_stat_rows)

        return execution_status_stats

    def list_annotations(
        self,
        *,
        session: orm.Session,
        id: bts.IdType,
    ) -> dict[str, str | None]:
        # pipeline_run = session.get(bts.PipelineRun, id)
        # if not pipeline_run:
        #     raise ItemNotFoundError(f"Pipeline run {id} not found.")
        annotations = {
            ann.key: ann.value
            for ann in session.scalars(
                sql.select(bts.PipelineRunAnnotation).where(
                    bts.PipelineRunAnnotation.pipeline_run_id == id
                )
            )
        }
        return annotations

    def set_annotation(
        self,
        *,
        session: orm.Session,
        id: bts.IdType,
        key: str,
        value: str | None = None,
        user_name: str | None = None,
        skip_user_check: bool = False,
    ):
        pipeline_run = session.get(bts.PipelineRun, id)
        if not pipeline_run:
            raise ItemNotFoundError(f"Pipeline run {id} not found.")
        if not skip_user_check and (user_name != pipeline_run.created_by):
            raise errors.PermissionError(
                f"The pipeline run {id} was started by {pipeline_run.created_by} and cannot be changed by {user_name}"
            )
        pipeline_run_annotation = bts.PipelineRunAnnotation(
            pipeline_run_id=id, key=key, value=value
        )
        session.merge(pipeline_run_annotation)
        session.commit()

    def delete_annotation(
        self,
        *,
        session: orm.Session,
        id: bts.IdType,
        key: str,
        user_name: str | None = None,
        skip_user_check: bool = False,
    ):
        pipeline_run = session.get(bts.PipelineRun, id)
        if not pipeline_run:
            raise ItemNotFoundError(f"Pipeline run {id} not found.")
        if not skip_user_check and (user_name != pipeline_run.created_by):
            raise errors.PermissionError(
                f"The pipeline run {id} was started by {pipeline_run.created_by} and cannot be changed by {user_name}"
            )

        existing_annotation = session.get(bts.PipelineRunAnnotation, (id, key))
        session.delete(existing_annotation)
        session.commit()


def _decode_page_token(page_token: str) -> dict[str, Any]:
    return json.loads(base64.b64decode(page_token)) if page_token else {}


def _encode_page_token(page_token_dict: dict[str, Any]) -> str:
    return (base64.b64encode(json.dumps(page_token_dict).encode("utf8"))).decode(
        "utf-8"
    )


def _parse_filter(filter: str) -> dict[str, str]:
    # TODO: Improve
    parts = filter.strip().split()
    parsed_filter = {}
    for part in parts:
        key, sep, value = part.partition(":")
        if sep:
            parsed_filter[key] = value
        else:
            parsed_filter.setdefault("_text", "")
            parsed_filter["_text"] += part
    return parsed_filter


# ========== ExecutionNodeApiService_Sql


# TODO: Use _storage_provider.calculate_hash(path)
# Hashing of constant arguments should the use same algorithm as caching of the output artifacts.
def _calculate_hash(s: str) -> str:
    import hashlib

    return "md5=" + hashlib.md5(s.encode("utf-8")).hexdigest()


def _split_type_spec(
    type_spec: structures.TypeSpecType | None,
) -> typing.Tuple[str | None, dict[str, Any] | None]:
    if type_spec is None:
        return None, None
    if isinstance(type_spec, str):
        return type_spec, None
    if isinstance(type_spec, typing.Mapping):
        kv_pairs = list(type_spec.items())
        if len(kv_pairs) == 1:
            type_name, type_properties = kv_pairs[1]
            if isinstance(type_name, str) and isinstance(
                type_properties, typing.Mapping
            ):
                return type_name, dict(type_properties)
    raise TypeError(f"Unsupported kind of type spec: {type_spec}")


# def _construct_constant_data_info(value: str) -> DataInfo:
#     return DataInfo(
#         total_size=len(value),
#         is_dir=False,
#         hash=_calculate_hash(value),
#     )


def _construct_constant_artifact_data(value: str) -> bts.ArtifactData:
    # FIX: !!!
    # raise NotImplementedError("MUST insert into session. Need to de-duplicate")
    artifact_data = bts.ArtifactData(
        total_size=len(value),
        is_dir=False,
        hash=_calculate_hash(value),
        value=value,
        created_at=_get_current_time(),
    )
    return artifact_data


def _construct_constant_artifact_node(
    value: str,
    artifact_type: structures.TypeSpecType | None = None,
):
    type_name, type_properties = _split_type_spec(artifact_type)
    artifact_node = bts.ArtifactNode(
        type_name=type_name,
        type_properties=type_properties,
        artifact_data=_construct_constant_artifact_data(value=value),
        had_data_in_past=True,
    )
    return artifact_node


def _construct_constant_artifact_node_and_add_to_session(
    session: orm.Session,
    value: str,
    artifact_type: structures.TypeSpecType | None = None,
):
    # FIX: !!!
    # raise NotImplementedError("MUST insert into session. Need to de-duplicate")
    artifact_node = _construct_constant_artifact_node(
        value=value, artifact_type=artifact_type
    )
    session.add(artifact_node.artifact_data)
    session.add(artifact_node)
    return artifact_node


# ? Do we need an association table between PipelineJob and ExecutionNode


# ============


@dataclasses.dataclass(kw_only=True)
class GetExecutionInfoResponse:
    id: bts.IdType
    task_spec: structures.TaskSpec
    parent_execution_id: bts.IdType | None = None
    child_task_execution_ids: dict[str, bts.IdType]
    pipeline_run_id: bts.IdType | None = None
    # ancestor_breadcrumbs: list[tuple[str, str]]
    input_artifacts: dict[str, "ArtifactNodeIdResponse"] | None = None
    output_artifacts: dict[str, "ArtifactNodeIdResponse"] | None = None


@dataclasses.dataclass
class ArtifactNodeIdResponse:
    id: bts.IdType


@dataclasses.dataclass
class GetGraphExecutionStateResponse:
    child_execution_status_stats: dict[bts.IdType, dict[str, int]]
    pass


@dataclasses.dataclass(kw_only=True)
class GetExecutionArtifactsResponse:
    input_artifacts: dict[str, "ArtifactNodeResponse"] | None = None
    output_artifacts: dict[str, "ArtifactNodeResponse"] | None = None


@dataclasses.dataclass
class GetContainerExecutionStateResponse:
    status: bts.ContainerExecutionStatus
    exit_code: int | None = None
    started_at: datetime.datetime | None = None
    ended_at: datetime.datetime | None = None
    debug_info: dict | None = None


@dataclasses.dataclass(kw_only=True)
class GetContainerExecutionLogResponse:
    log_text: str | None = None
    system_error_exception_full: str | None = None
    orchestration_error_message: str | None = None


class ExecutionNodesApiService_Sql:

    def get(self, session: orm.Session, id: bts.IdType) -> GetExecutionInfoResponse:
        execution_node = session.get(bts.ExecutionNode, id)
        if execution_node is None:
            raise ItemNotFoundError(f"Execution with {id=} does not exist.")

        parent_pipeline_run_id = session.scalar(
            sql.select(bts.PipelineRun.id).where(
                bts.PipelineRun.root_execution_id == id
            )
        )

        ancestor_pipeline_run_id = session.scalar(
            sql.select(bts.PipelineRun.id)
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                == bts.PipelineRun.root_execution_id,
            )
            .where(bts.ExecutionToAncestorExecutionLink.execution_id == id)
        )
        pipeline_run_id = parent_pipeline_run_id or ancestor_pipeline_run_id

        child_executions = execution_node.child_executions
        child_task_execution_ids = {
            child_execution.task_id_in_parent_execution
            or "<missing>": child_execution.id
            for child_execution in child_executions
        }
        input_artifacts = {
            input_name: ArtifactNodeIdResponse(id=artifact_id)
            for input_name, artifact_id in session.execute(
                sql.select(
                    bts.InputArtifactLink.input_name, bts.InputArtifactLink.artifact_id
                ).where(bts.InputArtifactLink.execution_id == id)
            ).tuples()
        }
        output_artifacts = {
            output_name: ArtifactNodeIdResponse(id=artifact_id)
            for output_name, artifact_id in session.execute(
                sql.select(
                    bts.OutputArtifactLink.output_name,
                    bts.OutputArtifactLink.artifact_id,
                ).where(bts.OutputArtifactLink.execution_id == id)
            ).tuples()
        }
        return GetExecutionInfoResponse(
            id=execution_node.id,
            task_spec=structures.TaskSpec.from_json_dict(execution_node.task_spec),
            parent_execution_id=execution_node.parent_execution_id,
            pipeline_run_id=pipeline_run_id,
            child_task_execution_ids=child_task_execution_ids,
            input_artifacts=input_artifacts,
            output_artifacts=output_artifacts,
        )

    def get_graph_execution_state(
        self, session: orm.Session, id: bts.IdType
    ) -> GetGraphExecutionStateResponse:
        ExecutionNode_Child = orm.aliased(
            bts.ExecutionNode, name="child_execution_node"
        )
        ExecutionNode_Descendant = orm.aliased(
            bts.ExecutionNode, name="descendant_execution_node"
        )
        # # We cannot use this query since ContainerExecution do not exist
        # # for not yet started container execution nodes.
        # query = (
        #     sql.select(
        #         ExecutionNode_Child.id.label("child_execution_id"),
        #         bts.ContainerExecution.status,
        #         sql.func.count().label("count"),
        #     )
        #     .where(ExecutionNode_Child.parent_execution_id == id)
        #     .join(
        #         bts.ExecutionToAncestorExecutionLink,
        #         bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
        #         == ExecutionNode_Child.id,
        #     )
        #     .join(
        #         ExecutionNode_Descendant,
        #         ExecutionNode_Descendant.id
        #         == bts.ExecutionToAncestorExecutionLink.execution_id,
        #     )
        #     .join(
        #         bts.ContainerExecution,
        #         bts.ContainerExecution.id
        #         == ExecutionNode_Descendant.container_execution_id,
        #     )
        #     .group_by(
        #         ExecutionNode_Child.id,
        #         bts.ContainerExecution.status,
        #     )
        # )
        child_descendants_query = (
            sql.select(
                ExecutionNode_Child.id.label("child_execution_id"),
                ExecutionNode_Descendant.container_execution_status,
                sql.func.count().label("count"),
            )
            .where(ExecutionNode_Child.parent_execution_id == id)
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                == ExecutionNode_Child.id,
            )
            .join(
                ExecutionNode_Descendant,
                ExecutionNode_Descendant.id
                == bts.ExecutionToAncestorExecutionLink.execution_id,
            )
            .where(ExecutionNode_Descendant.container_execution_status != None)
            .group_by(
                ExecutionNode_Child.id,
                ExecutionNode_Descendant.container_execution_status,
            )
        )
        direct_container_children_query = (
            sql.select(
                ExecutionNode_Child.id.label("child_execution_id"),
                ExecutionNode_Child.container_execution_status,
                sql.func.count().label("count"),
            )
            .where(ExecutionNode_Child.parent_execution_id == id)
            .where(ExecutionNode_Child.container_execution_status != None)
            .group_by(
                ExecutionNode_Child.id,
                ExecutionNode_Child.container_execution_status,
            )
        )
        child_descendants_execution_stat_rows = session.execute(
            child_descendants_query
        ).all()
        child_container_execution_stat_rows = session.execute(
            direct_container_children_query
        ).all()
        child_execution_stat_rows = tuple(
            child_descendants_execution_stat_rows
        ) + tuple(child_container_execution_stat_rows)
        child_execution_status_stats: dict[bts.IdType, dict[str, int]] = {}

        for row in child_execution_stat_rows:
            child_execution_id, status, count = row.tuple()
            status_stats = child_execution_status_stats.setdefault(
                child_execution_id, {}
            )
            status_stats[status.value] = count
        return GetGraphExecutionStateResponse(
            child_execution_status_stats=child_execution_status_stats,
        )

    def get_container_execution_state(
        self, session: orm.Session, id: bts.IdType
    ) -> GetContainerExecutionStateResponse:
        execution = session.get(bts.ExecutionNode, id)
        if not execution:
            raise ItemNotFoundError(f"Execution with {id=} does not exist.")
        container_execution = execution.container_execution
        if not container_execution:
            raise RuntimeError(
                f"Execution with {id=} does not have container execution information."
            )
        return GetContainerExecutionStateResponse(
            status=container_execution.status,
            exit_code=container_execution.exit_code,
            started_at=container_execution.started_at,
            ended_at=container_execution.ended_at,
            debug_info=container_execution.launcher_data,
        )

    def get_artifacts(
        self, session: orm.Session, id: bts.IdType
    ) -> GetExecutionArtifactsResponse:
        if not session.scalar(
            sql.select(sql.exists().where(bts.ExecutionNode.id == id))
        ):
            raise ItemNotFoundError(f"Execution with {id=} does not exist.")

        input_artifact_links = session.scalars(
            sql.select(bts.InputArtifactLink)
            .where(bts.InputArtifactLink.execution_id == id)
            .options(
                orm.joinedload(bts.InputArtifactLink.artifact).joinedload(
                    bts.ArtifactNode.artifact_data
                )
            )
        )
        output_artifact_links = session.scalars(
            sql.select(bts.OutputArtifactLink)
            .where(bts.OutputArtifactLink.execution_id == id)
            .options(
                orm.joinedload(bts.OutputArtifactLink.artifact).joinedload(
                    bts.ArtifactNode.artifact_data
                )
            )
        )

        input_artifacts = {
            input_artifact_link.input_name: ArtifactNodeResponse.from_db(
                input_artifact_link.artifact
            )
            for input_artifact_link in input_artifact_links
        }
        output_artifacts = {
            output_artifact_link.output_name: ArtifactNodeResponse.from_db(
                output_artifact_link.artifact
            )
            for output_artifact_link in output_artifact_links
        }
        return GetExecutionArtifactsResponse(
            input_artifacts=input_artifacts,
            output_artifacts=output_artifacts,
        )

    def get_container_execution_log(
        self,
        session: orm.Session,
        id: bts.IdType,
        container_launcher: "launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer] | None" = None,
    ) -> GetContainerExecutionLogResponse:
        execution = session.get(bts.ExecutionNode, id)
        if not execution:
            raise ItemNotFoundError(f"Execution with {id=} does not exist.")
        container_execution = execution.container_execution
        execution_extra_data = execution.extra_data or {}
        system_error_exception_full = execution_extra_data.get(
            bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_FULL_KEY
        )
        orchestration_error_message = execution_extra_data.get(
            bts.EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
        )
        # Temporarily putting the orchestration error into the system error field for compatibility.
        system_error_exception_full = (
            system_error_exception_full or orchestration_error_message
        )
        if not container_execution:
            if (
                execution.container_execution_status
                == bts.ContainerExecutionStatus.SYSTEM_ERROR
            ):
                return GetContainerExecutionLogResponse(
                    system_error_exception_full=system_error_exception_full,
                    orchestration_error_message=orchestration_error_message,
                )
            raise RuntimeError(
                f"Execution with {id=} does not have container execution information."
            )
        log_text: str | None = None
        if container_execution.status in (
            bts.ContainerExecutionStatus.SUCCEEDED,
            bts.ContainerExecutionStatus.FAILED,
            bts.ContainerExecutionStatus.SYSTEM_ERROR,
        ):
            try:
                # Returning completed log
                if not container_execution.log_uri:
                    raise RuntimeError(
                        f"Container execution {container_execution.id=} does not have log_uri. Impossible."
                    )
                # TODO: Make the ContainerLauncher._storage_provider part of the public interface or create a better solution for log retrieval
                # Try getting the configured storage provider from the launcher so that it has correct access credentials.
                storage_provider = (
                    getattr(container_launcher, "_storage_provider", None)
                    if container_launcher
                    else None
                )
                log_text = _read_container_execution_log_from_uri(
                    log_uri=container_execution.log_uri,
                    storage_provider=storage_provider,
                )
            except:
                # Do not raise exception if the execution is in SYSTEM_ERROR state
                # We want to return the system error exception.
                if (
                    container_execution.status
                    != bts.ContainerExecutionStatus.SYSTEM_ERROR
                ):
                    raise
        elif container_execution.status == bts.ContainerExecutionStatus.RUNNING:
            if not container_launcher:
                raise ApiServiceError(
                    f"Reading log of an unfinished container requires `container_launcher`."
                )
            if not container_execution.launcher_data:
                raise ApiServiceError(
                    f"Execution does not have container launcher data."
                )

            launched_container = (
                container_launcher.deserialize_launched_container_from_dict(
                    container_execution.launcher_data
                )
            )
            log_text = launched_container.get_log()

        return GetContainerExecutionLogResponse(
            log_text=log_text,
            system_error_exception_full=system_error_exception_full,
            orchestration_error_message=orchestration_error_message,
        )

    def stream_container_execution_log(
        self,
        session: orm.Session,
        container_launcher: "launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer]",
        execution_id: bts.IdType,
    ) -> typing.Iterator[str]:
        execution = session.get(bts.ExecutionNode, execution_id)
        if not execution:
            raise ItemNotFoundError(f"Execution with {execution_id=} does not exist.")
        container_execution = execution.container_execution
        if not container_execution:
            raise ApiServiceError(
                f"Execution does not have container execution information."
            )
        if not container_execution.launcher_data:
            raise ApiServiceError(
                f"Execution does not have container launcher information."
            )
        if container_execution.status == bts.ContainerExecutionStatus.RUNNING:
            launched_container = (
                container_launcher.deserialize_launched_container_from_dict(
                    container_execution.launcher_data
                )
            )
            return launched_container.stream_log_lines()
        else:
            if not container_execution.log_uri:
                raise RuntimeError(
                    f"Container execution {container_execution.id=} does not have log_uri. Impossible."
                )
            # TODO: Make the ContainerLauncher._storage_provider part of the public interface or create a better solution for log retrieval
            # Try getting the configured storage provider from the launcher so that it has correct access credentials.
            storage_provider = (
                getattr(container_launcher, "_storage_provider", None)
                if container_launcher
                else None
            )
            log_text = _read_container_execution_log_from_uri(
                log_uri=container_execution.log_uri,
                storage_provider=storage_provider,
            )
            return (line + "\n" for line in log_text.split("\n"))


def _read_container_execution_log_from_uri(
    log_uri: str,
    storage_provider: "storage_provider_interfaces.StorageProvider | None" = None,
) -> str:
    if ".." in log_uri:
        raise ValueError(
            f"_read_container_execution_log_from_uri: log_uri contains '..': {log_uri=}"
        )

    if storage_provider:
        # TODO: Switch to storage_provider.parse_uri_get_accessor
        uri_accessor = storage_provider.make_uri(log_uri)
        log_text = uri_accessor.get_reader().download_as_text()
        return log_text

    if "://" not in log_uri:
        # Consider the URL to be an absolute local path (`/path` or `C:\path` or `C:/path`)
        with open(log_uri, "r") as reader:
            return reader.read()
    elif log_uri.startswith("gs://"):
        # TODO: Switch to using storage providers.
        from google.cloud import storage

        gcs_client = storage.Client()
        blob = storage.Blob.from_string(log_uri, client=gcs_client)
        log_text = blob.download_as_text()
        return log_text
    elif log_uri.startswith("hf://"):
        from cloud_pipelines_backend.storage_providers import huggingface_repo_storage

        storage_provider = huggingface_repo_storage.HuggingFaceRepoStorageProvider()
        uri_accessor = storage_provider.parse_uri_get_accessor(uri_string=log_uri)
        log_text = uri_accessor.get_reader().download_as_text()
        return log_text
    else:
        raise NotImplementedError(
            f"Only logs in local storage or Google Cloud Storage are supported. But got {log_uri=}."
        )


@dataclasses.dataclass(kw_only=True)
class ArtifactNodeResponse:
    id: bts.IdType
    # had_data_in_past: bool = False
    # may_have_data_in_future: bool = True
    type_name: str | None = None
    type_properties: dict[str, Any] | None = None
    producer_execution_id: bts.IdType | None = None
    producer_output_name: str | None = None
    # artifact_data_id: bts.IdType | None = None
    artifact_data: "ArtifactDataResponse | None" = None

    @classmethod
    def from_db(cls, artifact_node: bts.ArtifactNode) -> "ArtifactNodeResponse":
        result = ArtifactNodeResponse(
            **{
                field.name: getattr(artifact_node, field.name)
                for field in dataclasses.fields(ArtifactNodeResponse)
            }
        )
        if artifact_node.artifact_data:
            result.artifact_data = ArtifactDataResponse.from_db(
                artifact_data=artifact_node.artifact_data
            )
        return result


@dataclasses.dataclass(kw_only=True)
class ArtifactDataResponse:
    total_size: int
    is_dir: bool
    # hash: str
    # At least one of `uri` or `value` must be set
    uri: str | None = None
    # Small constant value
    value: str | None = None
    # created_at: datetime.datetime | None = None
    # deleted_at: datetime.datetime | None = None

    @classmethod
    def from_db(cls, artifact_data: bts.ArtifactData) -> "ArtifactDataResponse":
        return ArtifactDataResponse(
            **{
                field.name: getattr(artifact_data, field.name)
                for field in dataclasses.fields(ArtifactDataResponse)
            }
        )


@dataclasses.dataclass(kw_only=True)
class GetArtifactInfoResponse:
    id: bts.IdType
    artifact_data: bts.ArtifactData | None = None


@dataclasses.dataclass(kw_only=True)
class GetArtifactSignedUrlResponse:
    signed_url: str


class ArtifactNodesApiService_Sql:

    def get(self, session: orm.Session, id: bts.IdType) -> GetArtifactInfoResponse:
        artifact_node = session.get(bts.ArtifactNode, id)
        if artifact_node is None:
            raise ItemNotFoundError(f"Artifact with {id=} does not exist.")
        artifact_data = artifact_node.artifact_data
        result = GetArtifactInfoResponse(id=artifact_node.id)
        if artifact_data:
            result.artifact_data = artifact_data
        return result

    def get_signed_artifact_url(
        self, session: orm.Session, id: bts.IdType
    ) -> GetArtifactSignedUrlResponse:
        artifact_data = session.scalar(
            sql.select(bts.ArtifactData)
            .join(bts.ArtifactNode)
            .where(bts.ArtifactNode.id == id)
        )
        if not artifact_data:
            raise ItemNotFoundError(f"Artifact node with {id=} does not exist.")
        if not artifact_data.uri:
            raise ValueError(f"Artifact node with {id=} does not have artifact URI.")
        if artifact_data.is_dir:
            raise ValueError(f"Cannot generate signer URL for a directory artifact.")
        if not artifact_data.uri.startswith("gs://"):
            raise ValueError(
                f"The get_signed_artifact_url method only supports Google Cloud Storage URIs, but got {artifact_data.uri=}."
            )

        from google.cloud import storage
        from google import auth

        # Avoiding error: "you need a private key to sign credentials."
        # "the credentials you are currently using <class 'google.auth.compute_engine.credentials.Credentials'> just contains a token.
        # "see https://googleapis.dev/python/google-api-core/latest/auth.html#setting-up-a-service-account for more details."
        credentials = auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform.read-only"]
        )[0]
        storage_client = storage.Client(credentials=credentials)
        blob = storage.Blob.from_string(uri=artifact_data.uri, client=storage_client)
        signed_url = blob.generate_signed_url(
            # Expiration is required. Max expiration value is 7 days.
            expiration=datetime.timedelta(days=7)
        )
        return GetArtifactSignedUrlResponse(signed_url=signed_url)


# ============

# Idea for how to add deep nested graph:
# First: Recursively create all task execution nodes and create their output artifacts
# Then: For each execution node starting from root:
#       Set/create input argument artifacts
#       If the node is a graph, process the node's children
# ---
# No. Decided to first do topological sort and then 1-stage generation.


def _recursively_create_all_executions_and_artifacts_root(
    session: orm.Session,
    root_task_spec: structures.TaskSpec,
) -> bts.ExecutionNode:
    input_artifact_nodes: dict[str, bts.ArtifactNode] = {}

    root_component_spec = root_task_spec.component_ref.spec
    if not root_component_spec:
        raise ApiServiceError(
            f"root_task_spec.component_ref.spec is empty. {root_task_spec=}"
        )
    input_specs = {
        input_spec.name: input_spec for input_spec in root_component_spec.inputs or []
    }
    for input_name, input_argument in (root_task_spec.arguments or {}).items():
        input_spec = input_specs.get(input_name)
        if not input_spec:
            raise ApiServiceError(
                f"Argument given for non-existing input '{input_name}'. {root_task_spec=}"
            )
        if isinstance(
            input_argument,
            (
                structures.GraphInputArgument,
                structures.TaskOutputArgument,
            ),
        ):
            raise ApiServiceError(
                f"root task arguments can only be constants, but got {input_name}={input_argument}. {root_task_spec=}"
            )
        elif not isinstance(input_argument, str):
            raise ApiServiceError(
                f"root task constant argument must be a string, but got {input_name}={input_argument}. {root_task_spec=}"
            )
        # TODO: Support constant input artifacts (artifact IDs)
        if input_argument is not None:
            input_artifact_nodes[input_name] = (
                # _construct_constant_artifact_node_and_add_to_session(
                #     session=session, value=input_argument, artifact_type=input_spec.type
                # )
                _construct_constant_artifact_node(
                    value=input_argument, artifact_type=input_spec.type
                )
            )
            # This constant artifact won't be added to the DB
            # TODO: Actually, they will be added...
            # We don't need to link this input artifact here. It will be handled downstream.

    root_execution_node = _recursively_create_all_executions_and_artifacts(
        session=session,
        root_task_spec=root_task_spec,
        input_artifact_nodes=input_artifact_nodes,
        ancestors=[],
    )
    return root_execution_node


def _recursively_create_all_executions_and_artifacts(
    session: orm.Session,
    root_task_spec: structures.TaskSpec,
    input_artifact_nodes: dict[str, bts.ArtifactNode],
    ancestors: list[bts.ExecutionNode],
) -> bts.ExecutionNode:
    root_component_spec = root_task_spec.component_ref.spec
    if not root_component_spec:
        raise ApiServiceError(
            f"root_task.component_ref.spec is empty. {root_task_spec=}"
        )

    implementation = root_component_spec.implementation
    if not implementation:
        raise ApiServiceError(
            f"component_spec.implementation is empty. {root_task_spec=}"
        )

    root_execution_node = bts.ExecutionNode(
        task_spec=root_task_spec.to_json_dict(),
        # child_task_id_to_execution_node=None,
    )
    session.add(root_execution_node)
    for ancestor in ancestors:
        ancestor_link = bts.ExecutionToAncestorExecutionLink(
            execution=root_execution_node,
            ancestor_execution=ancestor,
        )
        session.add(ancestor_link)

    # FIX: Handle ExecutionNode.constant_arguments
    # We do not touch root_task_spec.arguments. We use graph_input_artifact_nodes instead
    constant_input_artifacts: dict[str, bts.ArtifactData] = {}
    input_artifact_nodes = dict(input_artifact_nodes)
    for input_spec in root_component_spec.inputs or []:
        input_artifact_node = input_artifact_nodes.get(input_spec.name)
        if input_artifact_node is None and not input_spec.optional:
            if input_spec.default:
                input_artifact_node = (
                    _construct_constant_artifact_node_and_add_to_session(
                        session=session,
                        value=input_spec.default,
                        artifact_type=input_spec.type,
                    )
                )
                # # Not adding constant inputs to the DB. We'll add them to `ExecutionNode.constant_arguments`
                # input_artifact_node = (
                #     _construct_constant_artifact_node(
                #         value=input_spec.default,
                #         artifact_type=input_spec.type,
                #     )
                # )
                # This constant artifact won't be added to the DB
                # result_artifact_nodes.append(artifact_node)
                input_artifact_nodes[input_spec.name] = input_artifact_node
            else:
                raise ApiServiceError(
                    f"Task has a required input {input_spec.name}, but no upstream artifact and no default value. {root_task_spec=}"
                )
        if input_artifact_node:
            # if input_artifact_node.artifact_data:
            #     # Not adding constant inputs to the DB. We'll add them to `ExecutionNode.constant_arguments`
            #     constant_input_artifacts[input_spec.name] = input_artifact_node.artifact_data
            # else:
            input_artifact_link = bts.InputArtifactLink(
                execution=root_execution_node,
                input_name=input_spec.name,
                artifact=input_artifact_node,
            )
            session.add(input_artifact_link)

    if isinstance(implementation, structures.ContainerImplementation):
        for output_spec in root_component_spec.outputs or []:
            artifact_node = bts.ArtifactNode(
                producer_execution=root_execution_node,
                producer_output_name=output_spec.name,
                # TODO: Improve type handling
                type_name=_split_type_spec(output_spec.type)[0],
                type_properties=_split_type_spec(output_spec.type)[1],
                artifact_data=None,
            )
            session.add(artifact_node)

            output_artifact_link = bts.OutputArtifactLink(
                execution=root_execution_node,
                output_name=output_spec.name,
                artifact=artifact_node,
            )
            session.add(output_artifact_link)

        # FIX!: Create ContainerExecution here. (Beware of caching.)
        # # container_spec = implementation.container
        # container_execution_node = ContainerExecutionNode(
        #     id=...,
        #     status=ContainerExecutionStatus.WaitingForUpstream
        # )
        # root_execution_node.container_execution_id = container_execution_node.id
        # Done: Maybe set WAITING_FOR_UPSTREAM ourselves.
        root_execution_node.container_execution_status = (
            bts.ContainerExecutionStatus.QUEUED
            if all(
                artifact_node.artifact_data
                for artifact_node in input_artifact_nodes.values()
            )
            else bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
        )
    elif isinstance(implementation, structures.GraphImplementation):
        # Processing child tasks
        graph_spec = implementation.graph

        # task_id_to_execution_node: dict[str, bts.ExecutionNode] = {}
        task_output_artifact_nodes: dict[str, dict[str, bts.ArtifactNode]] = {}

        # Implementation design:
        # We need to either toposort tasks or delay processing input arguments
        # until ALL execution nodes and output artifacts are constructed (recursively).
        # Let's try to use topological sort.
        # This will also allow us to test for cycles.

        child_tasks = _toposort_tasks(graph_spec.tasks)

        # Processing child task input artifacts
        for child_task_id, child_task_spec in child_tasks.items():
            child_component_spec = child_task_spec.component_ref.spec
            if not child_component_spec:
                raise ApiServiceError(
                    f"child_task_spec.component_ref.spec is empty. {child_task_spec=}"
                )
            child_task_input_artifact_nodes: dict[str, bts.ArtifactNode] = {}
            for input_spec in child_component_spec.inputs or []:
                input_argument = (child_task_spec.arguments or {}).get(input_spec.name)
                input_artifact_node: bts.ArtifactNode | None = None
                if input_argument is None and not input_spec.optional:
                    # Not failing on unconnected required input if there is a default value
                    if input_spec.default is None:
                        raise ApiServiceError(
                            f"Task has a required input '{input_spec.name}', but no upstream artifact and no default value. {child_task_spec=}"
                        )
                    else:
                        input_argument = input_spec.default
                if input_argument is None:
                    pass
                elif isinstance(input_argument, structures.GraphInputArgument):
                    input_artifact_node = input_artifact_nodes.get(
                        input_argument.graph_input.input_name
                    )
                    if input_artifact_node is None:
                        # Warning: unconnected upstream
                        # TODO: Support using upstream graph input's default value when needed for required input (non-trivial feature).
                        # Feature: "Unconnected upstream with optional default"
                        pass
                elif isinstance(input_argument, structures.TaskOutputArgument):
                    task_output_source = input_argument.task_output
                    input_artifact_node = task_output_artifact_nodes[
                        task_output_source.task_id
                    ][task_output_source.output_name]
                elif isinstance(input_argument, str):
                    input_artifact_node = (
                        _construct_constant_artifact_node_and_add_to_session(
                            session=session,
                            value=input_argument,
                            artifact_type=input_spec.type,
                        )
                    )
                    # Not adding constant inputs to the DB. We'll add them to `ExecutionNode.constant_arguments`
                    # input_artifact_node = (
                    #     _construct_constant_artifact_node(
                    #         value=input_argument,
                    #         artifact_type=input_spec.type,
                    #     )
                    # )
                else:
                    raise ApiServiceError(
                        f"Unexpected task argument: {input_spec.name}={input_argument}. {child_task_spec=}"
                    )
                if input_artifact_node:
                    child_task_input_artifact_nodes[input_spec.name] = (
                        input_artifact_node
                    )

            # Creating child task nodes and their output artifacts
            child_execution_node = _recursively_create_all_executions_and_artifacts(
                session=session,
                root_task_spec=child_task_spec,
                input_artifact_nodes=child_task_input_artifact_nodes,
                ancestors=ancestors + [root_execution_node],
            )
            child_execution_node.parent_execution = root_execution_node
            child_execution_node.task_id_in_parent_execution = child_task_id
            # task_id_to_execution_node[child_task_id] = child_execution_node
            # TODO: ! Ensure this relationship works properly (is populated and does not query DB),
            task_output_artifact_nodes[child_task_id] = {
                link.output_name: link.artifact
                for link in child_execution_node.output_artifact_links
            }

        # Processing root graph output artifacts
        for output_name, output_source in (graph_spec.output_values or {}).items():
            if not isinstance(output_source, structures.TaskOutputArgument):
                raise ApiServiceError(
                    f"graph_spec.output_values values can only be of type TaskOutputArgument, but got {output_source=}"
                )
            task_output_source = output_source.task_output
            source_artifact = task_output_artifact_nodes[task_output_source.task_id][
                task_output_source.output_name
            ]
            output_artifact_link = bts.OutputArtifactLink(
                execution=root_execution_node,
                output_name=output_name,
                artifact=source_artifact,
            )
            session.add(output_artifact_link)
    else:
        raise ApiServiceError(
            f"Unknown ComponentSpec.implementation. {root_component_spec=}"
        )
    return root_execution_node


def _toposort_tasks(
    tasks: typing.Mapping[str, structures.TaskSpec],
) -> dict[str, structures.TaskSpec]:
    # Checking task output references and preparing the dependency table
    task_dependencies: dict[str, dict] = {}
    for task_id, task in tasks.items():
        # Using dict instead of set to stabilize the ordering
        dependencies: dict[str, bool] = {}
        task_dependencies[task_id] = dependencies
        if task.arguments is not None:
            for argument in task.arguments.values():
                if isinstance(argument, structures.TaskOutputArgument):
                    dependencies[argument.task_output.task_id] = True
                    if argument.task_output.task_id not in tasks:
                        raise TypeError(
                            'Argument "{}" references non-existing task.'.format(
                                argument
                            )
                        )

    # Topologically sorting tasks to detect cycles
    task_dependents = {k: {} for k in task_dependencies.keys()}
    for task_id, dependencies in task_dependencies.items():
        for dependency in dependencies:
            task_dependents[dependency][task_id] = True
    task_number_of_remaining_dependencies = {
        k: len(v) for k, v in task_dependencies.items()
    }
    sorted_tasks = {}  # Python dictionaries preserve order now

    def process_task(task_id):
        if (
            task_number_of_remaining_dependencies[task_id] == 0
            and task_id not in sorted_tasks
        ):
            sorted_tasks[task_id] = tasks[task_id]
            for dependent_task in task_dependents[task_id]:
                task_number_of_remaining_dependencies[dependent_task] -= 1
                process_task(dependent_task)

    for task_id in task_dependencies.keys():
        process_task(task_id)
    if len(sorted_tasks) != len(task_dependencies):
        tasks_with_unsatisfied_dependencies = {
            k: v for k, v in task_number_of_remaining_dependencies.items() if v > 0
        }
        task_with_minimal_number_of_unsatisfied_dependencies = min(
            tasks_with_unsatisfied_dependencies.keys(),
            key=lambda task_id: tasks_with_unsatisfied_dependencies[task_id],
        )
        raise ValueError(
            'Task "{}" has cyclical dependency.'.format(
                task_with_minimal_number_of_unsatisfied_dependencies
            )
        )

    return sorted_tasks


def _sqlalchemy_object_to_dict(obj) -> dict:
    d = dict(obj.__dict__)
    d.pop("_sa_instance_state", None)
    return d


# ================
# 2025-02-15

# Idea: There are high-level abstract execution nodes. Some of them link to ContainerExecution (or possibly multiple of them is thee are retries.)
# There are edges in the DB. Different kinds of edges: Artifact passing edges, execution-to-parent-execution edges etc.
# Idea: When [container?] execution finishes, the system processes/activates/notifies the edges. For example, sets downstream execution's input artifacts.
# We could have a dedicated queue for edges (the upstream only "activates" the edges for processing), but we can start with the execution completion actively processing everything synchronously.
# There is a risk of race condition. To somewhat mitigate this risk, the code that wants to check the state and add the edge should add the edge first, then try to process it.


# class ArtifactPromiseEnum(enum.Enum):
#     NOT_CREATED_YET = 1
#     CREATED = 2
#     WILL_NEVER_BE_CREATED = 3  # = Upstream failed/skipped/cancelled
#     UPSTREAM_NOT_CONNECTED = 4
#     # Flags: have_existed, may_exist_in_future


# # There need to be special artifact: UnconnectedArtifactWithDefaultValue
# # If input is optional, it's treated as unconnected.
# # If input is required, it's treated as the default value.
# class UnconnectedArtifactWithDefaultValue:
#     default_value: str
