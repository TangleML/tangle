import dataclasses
import datetime
import enum
import typing
from typing import Any

import sqlalchemy as sql
from sqlalchemy import orm
from sqlalchemy.ext import mutable

IdType: typing.TypeAlias = str


class ContainerExecutionStatus(str, enum.Enum):
    INVALID = "INVALID"  # Compatibility with Vertex AI CustomJob
    UNINITIALIZED = "UNINITIALIZED"  # Remove
    QUEUED = "QUEUED"  # Before WAITING_FOR_UPSTREAM or STARTING
    # READY_TO_START = "READY_TO_START"  # Input artifacts ready, but no job ID
    WAITING_FOR_UPSTREAM = "WAITING_FOR_UPSTREAM"
    # STARTING = "STARTING"
    PENDING = "PENDING"  # == Starting
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    # UPSTREAM_FAILED = "UPSTREAM_FAILED"
    # CONDITIONALLY_SKIPPED = "CONDITIONALLY_SKIPPED"
    SYSTEM_ERROR = "SYSTEM_ERROR"

    # new
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    # UPSTREAM_FAILED_OR_SKIPPED = "UPSTREAM_FAILED_OR_SKIPPED"
    SKIPPED = "SKIPPED"


CONTAINER_STATUSES_ENDED = {
    ContainerExecutionStatus.INVALID,
    ContainerExecutionStatus.SUCCEEDED,
    ContainerExecutionStatus.FAILED,
    ContainerExecutionStatus.SYSTEM_ERROR,
    ContainerExecutionStatus.CANCELLED,
    ContainerExecutionStatus.SKIPPED,
}


def generate_unique_id() -> str:
    """Generates a 10-byte (20 hex chars) unique ID.session.add

    The ID is 6 bytes of millisecond-precision time plus 4 random bytes.
    """
    import os
    import time

    random_bytes = os.urandom(4)
    nanoseconds = time.time_ns()
    milliseconds = nanoseconds // 1_000_000

    return ("%012x" % milliseconds) + random_bytes.hex()


id_column = orm.mapped_column(
    sql.String(20), primary_key=True, init=False, insert_default=generate_unique_id
)

# # Needed to put a union type into DB
# class SqlIOTypeStruct(_BaseModel):
#     type: structures.TypeSpecType
# No. We'll represent TypeSpecType as name:str + properties:dict
# Supported cases:
# * type: "name"
# * type: {"name": {...}}
# We drop support for following cases:
# * `type: [...]`
# * `type: {"a": ..., "b": ...}`cases
# If needed they can be represented using `weirdType: ...`


class UtcDateTime(sql.TypeDecorator):
    """A DateTime type that ensures UTC timezone on read.

    sql.DateTime(timezone=True) is not enough to solve the issue.
    """

    impl = sql.DateTime(timezone=True)
    cache_ok = True

    def process_result_value(
        self, value: datetime.datetime | None, dialect: sql.Dialect
    ) -> datetime.datetime | None:
        if value is not None:
            if value.tzinfo is None:
                # Interpreting naive timestamp as UTC
                return value.replace(tzinfo=datetime.timezone.utc)
            # Converting timezone-aware timestamp to UTC
            return value.astimezone(datetime.timezone.utc)
        return value


class _TableBase(orm.MappedAsDataclass, orm.DeclarativeBase, kw_only=True):
    # Not really needed due to kw_only=True
    _: dataclasses.KW_ONLY

    # The mutable.MutableDict.as_mutable construct ensures that changes to dictionaries are picked up.
    # This is very important when making changes to `extra_data` dictionaries.
    type_annotation_map = {
        dict: mutable.MutableDict.as_mutable(sql.JSON),
        list: mutable.MutableList.as_mutable(sql.JSON),
        # List: sql.JSON,
        # List[int]: sql.JSON,
        list[int]: mutable.MutableList.as_mutable(sql.JSON),
        list[str]: mutable.MutableList.as_mutable(sql.JSON),
        # List[DataVersionDict]: sql.JSON,
        dict[str, Any]: mutable.MutableDict.as_mutable(sql.JSON),
        # structures._BaseModel: sql.JSON,
        # structures.ComponentSpec: sql.JSON,
        # structures.TaskSpec: sql.JSON,
        # structures.TypeSpecType: sql.JSON,  # !!! Gotcha. This causes str map to JSON
        # SqlIOTypeStruct: sql.JSON,
        # PipelineSpec: sql.JSON,
        str: sql.String(255),
        datetime.datetime: UtcDateTime,  # sql.DateTime(timezone=True) is not enough
    }


# ! Note: Set repr=False in most relationships.
# When a relationship has repr=True (default), printing the object causes DB queries (potentially deep) to fetch the related entities.


class PipelineRun(_TableBase):
    __tablename__ = "pipeline_run"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
    # pipeline_spec: orm.Mapped[dict[str, Any]]
    root_execution_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), init=False
    )
    root_execution: orm.Mapped["ExecutionNode"] = orm.relationship(repr=False)
    annotations: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # status: "PipelineJobStatus"
    # root_execution_summary: "ExecutionSummary"
    created_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    created_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    updated_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    # start_time: datetime.datetime | None = None
    # end_time: datetime.datetime | None = None
    # For version control. We can calculate diff and draw a pipeline evolution graph.
    # Can be stored in annotations though.
    parent_pipeline_id: orm.Mapped[IdType | None] = orm.mapped_column(default=None)

    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    __table_args__ = (
        sql.Index(
            "ix_pipeline_run_created_at_desc",
            created_at.desc(),
        ),
        sql.Index(
            "ix_pipeline_run_created_by_created_at_desc",
            created_by,
            created_at.desc(),
        ),
    )


class ArtifactData(_TableBase):
    __tablename__ = "artifact_data"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
    # Default SQL integer type is only 32 bit. So it cannot record size of more than 2GB.
    total_size: orm.Mapped[int] = orm.mapped_column(sql.BigInteger())
    is_dir: orm.Mapped[bool]
    hash: orm.Mapped[str]
    # At least one of `uri` or `value` must be set
    uri: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # Small constant value
    # sql.String(65535) is translated to VARCHAR(65535) which fails on MySQL:
    # "Column length too big for column 'value' (max = 16383); use BLOB or TEXT instead"
    value: orm.Mapped[str | None] = orm.mapped_column(sql.Text(), default=None)
    created_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    # TODO: Think about a race condition where an artifact is purged right when it's going to be reused.
    deleted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)

    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    # artifact_nodes: orm.Mapped[list["ArtifactNode"]] = orm.relationship(
    #     default=None, repr=False, back_populates="artifact_data"
    # )


class ArtifactNode(_TableBase):
    __tablename__ = "artifact_node"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
    had_data_in_past: orm.Mapped[bool] = orm.mapped_column(default=False)
    may_have_data_in_future: orm.Mapped[bool] = orm.mapped_column(default=True)
    # artifact_type: orm.Mapped[structures.TypeSpecType | None]
    type_name: orm.Mapped[str | None] = orm.mapped_column(default=None)
    type_properties: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # producer: ExecutionOutputArtifactSource | None = None
    producer_execution_id: orm.Mapped[IdType | None] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), init=False
    )
    producer_execution: orm.Mapped["ExecutionNode | None"] = orm.relationship(
        # back_populates="output_artifacts",
        default=None,
        repr=False,
    )
    producer_output_name: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # artifact_data: ArtifactData | None = None
    artifact_data_id: orm.Mapped[IdType | None] = orm.mapped_column(
        sql.ForeignKey("artifact_data.id"), init=False
    )
    artifact_data: orm.Mapped[ArtifactData | None] = orm.relationship(
        default=None, repr=False
    )

    upstream_execution_links: orm.Mapped[list["OutputArtifactLink"]] = orm.relationship(
        back_populates="artifact", default_factory=list, repr=False
    )
    downstream_execution_links: orm.Mapped[list["InputArtifactLink"]] = (
        orm.relationship(back_populates="artifact", default_factory=list, repr=False)
    )
    downstream_executions: orm.Mapped[list["ExecutionNode"]] = orm.relationship(
        secondary="input_artifact_link", viewonly=True, default_factory=list, repr=False
    )

    # x_producer_execution_output_link_id: orm.Mapped[IdType | None]
    # x_producer_execution_output_link: orm.Mapped["SqlOutputArtifactLink | None"] = (
    #     orm.relationship(back_populates="artifact")
    # )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)


class InputArtifactLink(_TableBase):
    __tablename__ = "input_artifact_link"
    execution_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), primary_key=True, init=False
    )
    input_name: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    artifact_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("artifact_node.id"), init=False
    )

    execution: orm.Mapped["ExecutionNode"] = orm.relationship(
        back_populates="input_artifact_links"
    )
    artifact: orm.Mapped[ArtifactNode] = orm.relationship(
        back_populates="downstream_execution_links"
    )
    # Should we include artifact node status here?


class OutputArtifactLink(_TableBase):
    __tablename__ = "output_artifact_link"
    execution_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), primary_key=True, init=False
    )
    output_name: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    artifact_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("artifact_node.id"), init=False
    )

    execution: orm.Mapped["ExecutionNode"] = orm.relationship(
        back_populates="output_artifact_links"
    )
    artifact: orm.Mapped[ArtifactNode] = orm.relationship(
        back_populates="upstream_execution_links"
    )
    # Should we include artifact node status here?


class ExecutionToAncestorExecutionLink(_TableBase):
    __tablename__ = "execution_ancestor"
    ancestor_execution_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), primary_key=True, init=False
    )
    execution_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), primary_key=True, init=False
    )
    ancestor_execution: orm.Mapped["ExecutionNode"] = orm.relationship(
        foreign_keys=[ancestor_execution_id]
    )
    execution: orm.Mapped["ExecutionNode"] = orm.relationship(
        foreign_keys=[execution_id]
    )


# ! Problem: With MongoDB, multiple executions can have same output artifact
# (e.g. graph execution returning artifact produced by a container execution).
# But it's now not possible since ArtifactNode can only link to a single producer execution.
# So we need to jump through extra hoops to make the relationship many-to-many again.
class ExecutionNode(_TableBase):
    __tablename__ = "execution_node"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
    # input_artifacts
    # task_id: str | None = None
    # task_spec: structures.TaskSpec
    # task_spec: orm.Mapped[structures.TaskSpec]
    task_spec: orm.Mapped[dict[str, Any]]
    # input_arguments: dict[str, ArtifactNode]

    # constant_arguments: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
    #     default=None
    # )  # dict[str, ArtifactData]

    input_artifact_links: orm.Mapped[list[InputArtifactLink]] = orm.relationship(
        back_populates="execution",
        init=False,
        default_factory=list,
        repr=False,
    )
    # outputs: dict[str, ArtifactNode]
    # output_artifacts: dict[str, ArtifactNodeWithOptionalId]  # Or should we use IDs?
    output_artifact_links: orm.Mapped[list[OutputArtifactLink]] = orm.relationship(
        back_populates="execution",
        init=False,
        default_factory=list,
        repr=False,
    )
    output_artifact_nodes: orm.Mapped[list["ArtifactNode"]] = orm.relationship(
        secondary="output_artifact_link",
        init=False,
        default_factory=list,
        repr=False,
        viewonly=True,
    )

    parent_execution_id: orm.Mapped[IdType | None] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), default=None, init=False
    )
    task_id_in_parent_execution: orm.Mapped[str | None] = orm.mapped_column(
        default=None
    )
    # !!! remote_side=[id] is important. Without it, the relationship somehow works the opposite way.
    parent_execution: orm.Mapped["ExecutionNode"] = orm.relationship(
        remote_side=[id],
        back_populates="child_executions",
        default=None,
        repr=False,
    )
    # parent_execution: "SqlExecutionNode" = sqlmodel.Relationship()
    child_executions: orm.Mapped[list["ExecutionNode"]] = orm.relationship(
        remote_side=[parent_execution_id],
        back_populates="parent_execution",
        default_factory=list,
        init=False,
        repr=False,
    )

    # updated_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)

    # execution_kind = orm.Mapped[typing.Literal["CONTAINER", "GRAPH"]]

    # Graph nodes only
    # child_task_execution_nodes: dict[str, "ExecutionNode"] | None = None
    # child_task_execution_node_ids: dict[str, str] | None = None
    # graph_execution_details: GraphExecutionDetails | None = None
    # graph_execution_details__child_task_execution_node_ids: dict[str, str]
    # graph_execution_details__child_task_execution_links: list["SqlExecutionToChildExecutionLink"] = sqlmodel.Relationship(back_populates="parent_execution_id")
    # graph_execution_details__child_task_execution_nodes: list["SqlExecutionNode"] = sqlmodel.Relationship()
    # graph_execution_details__child_execution_summaries: dict[str, ExecutionSummary]

    # Container nodes only
    # container_execution_id: str | None = None
    # container_execution_details: ContainerExecutionDetails | None = None
    # container_execution_details__status: ContainerExecutionStatus
    # container_execution_details__container_execution_id: orm.Mapped[IdType | None]
    # container_execution_details__last_processed_time: datetime.datetime | None = None
    # container_execution_details__cache_key: str | None = None
    container_execution_id: orm.Mapped[IdType | None] = orm.mapped_column(
        sql.ForeignKey("container_execution.id"), default=None, init=False
    )
    container_execution: orm.Mapped["ContainerExecution"] = orm.relationship(
        default=None, repr=False, back_populates="execution_nodes"
    )
    # TODO: Do we need this? It's denormalized.
    container_execution_status: orm.Mapped[ContainerExecutionStatus | None] = (
        orm.mapped_column(default=None, index=True)
    )
    container_execution_cache_key: orm.Mapped[str | None] = orm.mapped_column(
        index=True, default=None
    )

    # ? UX-only de-normalized
    # For breadcrumbs navigation
    # parent_execution_node_ids: list[str] | None = None
    # For breadcrumbs navigation
    # parent_task_ids: list[str] | None = None

    # ! Cannot have pipeline_run_id foreign key since it will cause circular referencing with PipelineJob
    # pipeline_run_id: orm.Mapped[IdType | None] = orm.mapped_column(
    #     sql.ForeignKey("pipeline_run.id")
    # )

    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    ancestors: orm.Mapped[list["ExecutionNode"]] = orm.relationship(
        secondary=ExecutionToAncestorExecutionLink.__table__,
        primaryjoin=id == ExecutionToAncestorExecutionLink.execution_id,
        secondaryjoin=id == ExecutionToAncestorExecutionLink.ancestor_execution_id,
        viewonly=True,
        default_factory=list,
        init=False,
        repr=False,
    )
    descendants: orm.Mapped[list["ExecutionNode"]] = orm.relationship(
        secondary=ExecutionToAncestorExecutionLink.__table__,
        primaryjoin=id == ExecutionToAncestorExecutionLink.ancestor_execution_id,
        secondaryjoin=id == ExecutionToAncestorExecutionLink.execution_id,
        viewonly=True,
        default_factory=list,
        init=False,
        repr=False,
    )


EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY = (
    "system_error_exception_message"
)
EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_FULL_KEY = (
    "system_error_exception_full"
)
EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY = (
    "orchestration_error_message"
)
EXECUTION_NODE_EXTRA_DATA_DYNAMIC_DATA_ARGUMENTS_KEY = "dynamic_data_arguments"
CONTAINER_EXECUTION_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY = (
    "orchestration_error_message"
)


# Not needed. We can use the ExecutionToAncestorExecutionLink.ancestor_execution_id == PipelineRun.root_execution_id
# class ExecutionToPipelineRunLink(_TableBase):
#     __tablename__ = "execution_pipeline_run"
#     execution_id: orm.Mapped[IdType] = orm.mapped_column(
#         sql.ForeignKey("execution_node.id"), primary_key=True, init=False
#     )
#     pipeline_run_id: orm.Mapped[IdType] = orm.mapped_column(
#         sql.ForeignKey("pipeline_run.id"), primary_key=True, init=False
#     )
#     execution: orm.Mapped["ExecutionNode"] = orm.relationship()
#     pipeline_run: orm.Mapped["PipelineRun"] = orm.relationship()


class ContainerExecution(_TableBase):
    __tablename__ = "container_execution"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
    # task_spec: orm.Mapped[dict[str, Any]]
    status: orm.Mapped[ContainerExecutionStatus] = orm.mapped_column(index=True)
    last_processed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        default=None
    )
    created_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    updated_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    started_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    ended_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    exit_code: orm.Mapped[int | None] = orm.mapped_column(default=None)

    launcher_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    # Do we need these?
    input_artifact_data_map: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        default=None
    )
    output_artifact_data_map: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        default=None
    )
    log_uri: orm.Mapped[str | None] = orm.mapped_column(default=None)

    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    execution_nodes: orm.Mapped[list[ExecutionNode]] = orm.relationship(
        default_factory=list, repr=False, back_populates="container_execution"
    )

    __table_args__ = (
        sql.Index(
            "ix_container_execution_status_last_processed_at",
            status,
            last_processed_at,
        ),
    )


class PipelineRunAnnotation(_TableBase):
    __tablename__ = "pipeline_run_annotation"
    pipeline_run_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey(PipelineRun.id),
        primary_key=True,
        index=True,
    )
    pipeline_run: orm.Mapped[PipelineRun] = orm.relationship(repr=False, init=False)
    key: orm.Mapped[str] = orm.mapped_column(default=None, primary_key=True)
    value: orm.Mapped[str | None] = orm.mapped_column(default=None)


class Secret(_TableBase):
    __tablename__ = "secret"
    user_id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    secret_name: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    secret_value: orm.Mapped[str]
    created_at: orm.Mapped[datetime.datetime]
    updated_at: orm.Mapped[datetime.datetime]
    expires_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    description: orm.Mapped[str | None] = orm.mapped_column(default=None)
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
