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
    INVALID = "INVALID"
    UNINITIALIZED = "UNINITIALIZED"
    QUEUED = "QUEUED"
    WAITING_FOR_UPSTREAM = "WAITING_FOR_UPSTREAM"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
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
    root_execution_id: orm.Mapped[IdType] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), init=False
    )
    root_execution: orm.Mapped["ExecutionNode"] = orm.relationship(repr=False)
    annotations: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    created_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    created_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    updated_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
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


class ArtifactNode(_TableBase):
    __tablename__ = "artifact_node"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
    had_data_in_past: orm.Mapped[bool] = orm.mapped_column(default=False)
    may_have_data_in_future: orm.Mapped[bool] = orm.mapped_column(default=True)
    type_name: orm.Mapped[str | None] = orm.mapped_column(default=None)
    type_properties: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    producer_execution_id: orm.Mapped[IdType | None] = orm.mapped_column(
        sql.ForeignKey("execution_node.id"), init=False
    )
    producer_execution: orm.Mapped["ExecutionNode | None"] = orm.relationship(
        default=None,
        repr=False,
    )
    producer_output_name: orm.Mapped[str | None] = orm.mapped_column(default=None)
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
    task_spec: orm.Mapped[dict[str, Any]]

    input_artifact_links: orm.Mapped[list[InputArtifactLink]] = orm.relationship(
        back_populates="execution",
        init=False,
        default_factory=list,
        repr=False,
    )
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
    child_executions: orm.Mapped[list["ExecutionNode"]] = orm.relationship(
        remote_side=[parent_execution_id],
        back_populates="parent_execution",
        default_factory=list,
        init=False,
        repr=False,
    )

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


class ContainerExecution(_TableBase):
    __tablename__ = "container_execution"
    id: orm.Mapped[IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=generate_unique_id
    )
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
