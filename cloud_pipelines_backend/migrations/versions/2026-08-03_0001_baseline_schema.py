"""Baseline schema

Reproduces the schema as it stood immediately before Alembic was introduced,
i.e. the tables that `bts._TableBase.metadata.create_all()` used to create.

This revision exists so that databases can be built from scratch by Alembic
alone. Databases that predate Alembic already have this schema and are stamped
with this revision rather than running it -- see
`database_ops.upgrade_db_schema`.

Revision ID: 0001
Revises:
Create Date: 2026-08-03

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# The status enum is stored by name. It is spelled out here rather than
# imported from `backend_types_sql` so that this revision keeps describing the
# schema as of this point in history even when the Python enum gains members.
_CONTAINER_EXECUTION_STATUS = sa.Enum(
    "INVALID",
    "UNINITIALIZED",
    "QUEUED",
    "WAITING_FOR_UPSTREAM",
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "SYSTEM_ERROR",
    "CANCELLING",
    "CANCELLED",
    "SKIPPED",
    name="containerexecutionstatus",
)


def upgrade() -> None:
    op.create_table(
        "artifact_data",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("total_size", sa.BigInteger(), nullable=False),
        sa.Column("is_dir", sa.Boolean(), nullable=False),
        sa.Column("hash", sa.String(length=255), nullable=False),
        sa.Column("uri", sa.String(length=255), nullable=True),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "container_execution",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("status", _CONTAINER_EXECUTION_STATUS, nullable=False),
        sa.Column("last_processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("exit_code", sa.Integer(), nullable=True),
        sa.Column("launcher_data", sa.JSON(), nullable=True),
        sa.Column("input_artifact_data_map", sa.JSON(), nullable=True),
        sa.Column("output_artifact_data_map", sa.JSON(), nullable=True),
        sa.Column("log_uri", sa.String(length=255), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_container_execution_status"),
        "container_execution",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_container_execution_status_last_processed_at",
        "container_execution",
        ["status", "last_processed_at"],
        unique=False,
    )
    op.create_table(
        "secret",
        sa.Column("user_id", sa.String(length=255), nullable=False),
        sa.Column("secret_name", sa.String(length=255), nullable=False),
        # Widened from VARCHAR(255) to TEXT by
        # `database_migrations.migrate_secret_value_column`, which still runs
        # for databases that predate this baseline.
        sa.Column("secret_value", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("user_id", "secret_name"),
    )
    op.create_index(op.f("ix_secret_user_id"), "secret", ["user_id"], unique=False)
    op.create_table(
        "user_settings",
        sa.Column("user_id", sa.String(length=255), nullable=False),
        sa.Column("settings", sa.JSON(), nullable=False),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_table(
        "execution_node",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("task_spec", sa.JSON(), nullable=False),
        sa.Column("parent_execution_id", sa.String(length=255), nullable=True),
        sa.Column("task_id_in_parent_execution", sa.String(length=255), nullable=True),
        sa.Column("container_execution_id", sa.String(length=255), nullable=True),
        sa.Column(
            "container_execution_status", _CONTAINER_EXECUTION_STATUS, nullable=True
        ),
        sa.Column(
            "container_execution_cache_key", sa.String(length=255), nullable=True
        ),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["container_execution_id"], ["container_execution.id"]),
        sa.ForeignKeyConstraint(["parent_execution_id"], ["execution_node.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_execution_node_container_execution_cache_key"),
        "execution_node",
        ["container_execution_cache_key"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_node_container_execution_id"),
        "execution_node",
        ["container_execution_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_node_container_execution_status"),
        "execution_node",
        ["container_execution_status"],
        unique=False,
    )
    op.create_table(
        "artifact_node",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("had_data_in_past", sa.Boolean(), nullable=False),
        sa.Column("may_have_data_in_future", sa.Boolean(), nullable=False),
        sa.Column("type_name", sa.String(length=255), nullable=True),
        sa.Column("type_properties", sa.JSON(), nullable=True),
        sa.Column("producer_execution_id", sa.String(length=255), nullable=True),
        sa.Column("producer_output_name", sa.String(length=255), nullable=True),
        sa.Column("artifact_data_id", sa.String(length=255), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["artifact_data_id"], ["artifact_data.id"]),
        sa.ForeignKeyConstraint(["producer_execution_id"], ["execution_node.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "execution_ancestor",
        sa.Column("ancestor_execution_id", sa.String(length=255), nullable=False),
        sa.Column("execution_id", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["ancestor_execution_id"], ["execution_node.id"]),
        sa.ForeignKeyConstraint(["execution_id"], ["execution_node.id"]),
        sa.PrimaryKeyConstraint("ancestor_execution_id", "execution_id"),
    )
    op.create_table(
        "pipeline_run",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("root_execution_id", sa.String(length=255), nullable=False),
        sa.Column("annotations", sa.JSON(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("parent_pipeline_id", sa.String(length=255), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["root_execution_id"], ["execution_node.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_pipeline_run_created_at_desc",
        "pipeline_run",
        [sa.literal_column("created_at DESC")],
        unique=False,
    )
    op.create_index(
        "ix_pipeline_run_created_by_created_at_desc",
        "pipeline_run",
        ["created_by", sa.literal_column("created_at DESC")],
        unique=False,
    )
    op.create_table(
        "input_artifact_link",
        sa.Column("execution_id", sa.String(length=255), nullable=False),
        sa.Column("input_name", sa.String(length=255), nullable=False),
        sa.Column("artifact_id", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifact_node.id"]),
        sa.ForeignKeyConstraint(["execution_id"], ["execution_node.id"]),
        sa.PrimaryKeyConstraint("execution_id", "input_name"),
    )
    op.create_table(
        "output_artifact_link",
        sa.Column("execution_id", sa.String(length=255), nullable=False),
        sa.Column("output_name", sa.String(length=255), nullable=False),
        sa.Column("artifact_id", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifact_node.id"]),
        sa.ForeignKeyConstraint(["execution_id"], ["execution_node.id"]),
        sa.PrimaryKeyConstraint("execution_id", "output_name"),
    )
    op.create_table(
        "pipeline_run_annotation",
        sa.Column("pipeline_run_id", sa.String(length=255), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("value", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["pipeline_run_id"], ["pipeline_run.id"]),
        sa.PrimaryKeyConstraint("pipeline_run_id", "key"),
    )
    op.create_index(
        op.f("ix_pipeline_run_annotation_pipeline_run_id"),
        "pipeline_run_annotation",
        ["pipeline_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_pipeline_run_annotation_pipeline_run_id_key_value",
        "pipeline_run_annotation",
        ["pipeline_run_id", "key", "value"],
        unique=False,
    )

    # The component library tables. They are declared in
    # `component_library_api_server.py` rather than `backend_types_sql.py`, but
    # share the same `_TableBase` and therefore the same schema.
    op.create_table(
        "component",
        sa.Column("digest", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("spec", sa.JSON(), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("digest"),
    )
    op.create_table(
        "component_library",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("root_folder", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_by", sa.String(length=255), nullable=True),
        sa.Column("extra_owners", sa.JSON(), nullable=True),
        sa.Column("hide_from_search", sa.Boolean(), nullable=False),
        sa.Column("url", sa.String(length=255), nullable=True),
        sa.Column("annotations", sa.JSON(), nullable=True),
        sa.Column("component_count", sa.Integer(), nullable=False),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "published_component",
        sa.Column("digest", sa.String(length=255), nullable=False),
        sa.Column("published_by", sa.String(length=255), nullable=False),
        sa.Column("deprecated", sa.Boolean(), nullable=False),
        sa.Column("superseded_by", sa.String(length=255), nullable=True),
        sa.Column("url", sa.String(length=255), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("digest", "published_by"),
    )
    op.create_table(
        "user",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("component_library_pin_ids", sa.JSON(), nullable=True),
        sa.Column("extra_data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("user")
    op.drop_table("published_component")
    op.drop_table("component_library")
    op.drop_table("component")
    op.drop_index(
        "ix_pipeline_run_annotation_pipeline_run_id_key_value",
        table_name="pipeline_run_annotation",
    )
    op.drop_index(
        op.f("ix_pipeline_run_annotation_pipeline_run_id"),
        table_name="pipeline_run_annotation",
    )
    op.drop_table("pipeline_run_annotation")
    op.drop_table("output_artifact_link")
    op.drop_table("input_artifact_link")
    op.drop_index(
        "ix_pipeline_run_created_by_created_at_desc", table_name="pipeline_run"
    )
    op.drop_index("ix_pipeline_run_created_at_desc", table_name="pipeline_run")
    op.drop_table("pipeline_run")
    op.drop_table("execution_ancestor")
    op.drop_table("artifact_node")
    op.drop_index(
        op.f("ix_execution_node_container_execution_status"),
        table_name="execution_node",
    )
    op.drop_index(
        op.f("ix_execution_node_container_execution_id"), table_name="execution_node"
    )
    op.drop_index(
        op.f("ix_execution_node_container_execution_cache_key"),
        table_name="execution_node",
    )
    op.drop_table("execution_node")
    op.drop_table("user_settings")
    op.drop_index(op.f("ix_secret_user_id"), table_name="secret")
    op.drop_table("secret")
    op.drop_index(
        "ix_container_execution_status_last_processed_at",
        table_name="container_execution",
    )
    op.drop_index(
        op.f("ix_container_execution_status"), table_name="container_execution"
    )
    op.drop_table("container_execution")
    op.drop_table("artifact_data")
    # MySQL and SQLite store the enum inline in the table definition, but
    # PostgreSQL creates a standalone type that outlives the tables using it.
    _CONTAINER_EXECUTION_STATUS.drop(op.get_bind(), checkfirst=True)
