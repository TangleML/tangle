"""Database models for stable pipelines and their version representations.

``UserPipeline`` owns the stable UUID and points to a ``UserPipelineVersion``
by ``version_key``. Full versioning uses the canonical content digest as the
key; disabled versioning uses the reserved mutable-head key ``current`` while
retaining the real SHA-256 digest in ``content_digest``.
"""

import datetime
import enum
import logging
import uuid
from typing import Any, Final

import sqlalchemy as sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm

logger = logging.getLogger(__name__)

MAX_FILE_PATH_LENGTH: Final[int] = bts._STR_MAX_LENGTH
DIGEST_LENGTH: Final[int] = 64
CURRENT_VERSION_KEY: Final[str] = "current"
# The width of a hyphenated UUID string. Exported because every API that accepts a pipeline id
# has to bound its input at the same number, and a bound that disagrees with the column turns a
# caller's mistake into a 500 at flush time instead of a 422 at the edge.
PIPELINE_ID_LENGTH: Final[int] = 36


class PipelineVersioningMode(str, enum.Enum):
    DISABLED = "disabled"
    FULL = "full"


def _generate_pipeline_id() -> str:
    """Assign a pipeline's stable identity once, when the ORM object is created."""
    return str(uuid.uuid4())


class UserPipeline(bts._TableBase):
    """A stable user/file identity whose current content points at a version."""

    __tablename__ = "pipeline"

    # This UUID belongs to the stable pipeline identity. Version updates only move
    # current_version_key; they never replace this primary key.
    id: orm.Mapped[str] = orm.mapped_column(
        sql.String(PIPELINE_ID_LENGTH),
        primary_key=True,
        init=False,
        default_factory=_generate_pipeline_id,
    )
    user_id: orm.Mapped[str] = orm.mapped_column()
    file_path: orm.Mapped[str] = orm.mapped_column(sql.String(MAX_FILE_PATH_LENGTH))
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    current_version_key: orm.Mapped[str | None] = orm.mapped_column(
        sql.String(DIGEST_LENGTH),
        default=None,
    )
    versioning_mode: orm.Mapped[PipelineVersioningMode] = orm.mapped_column(
        sql.Enum(
            PipelineVersioningMode,
            values_callable=lambda enum_class: [member.value for member in enum_class],
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
            name="ck_pipeline_versioning_mode",
            length=bts._STR_MAX_LENGTH,
        ),
        default=PipelineVersioningMode.DISABLED,
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    deleted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)

    __table_args__ = (
        sql.UniqueConstraint(
            "user_id",
            "file_path",
            name="uq_pipeline_user_id_file_path",
        ),
        sql.ForeignKeyConstraint(
            ["id", "current_version_key"],
            ["pipeline_version.pipeline_id", "pipeline_version.version_key"],
            name="fk_pipeline_current_version_key",
        ),
        sql.Index(
            "ix_pipeline_user_id_updated_at_desc_id_desc",
            user_id,
            updated_at.desc(),
            id.desc(),
        ),
    )


class UserPipelineVersion(bts._TableBase):
    """A full immutable version or the disabled-mode mutable head."""

    __tablename__ = "pipeline_version"

    pipeline_id: orm.Mapped[str] = orm.mapped_column(
        sql.String(PIPELINE_ID_LENGTH),
        sql.ForeignKey(
            "pipeline.id",
            ondelete="CASCADE",
            name="fk_pipeline_version_pipeline_id",
        ),
        primary_key=True,
    )
    version_key: orm.Mapped[str] = orm.mapped_column(
        sql.String(DIGEST_LENGTH),
        primary_key=True,
    )
    content_digest: orm.Mapped[str] = orm.mapped_column(sql.String(DIGEST_LENGTH))
    root_pipeline_task: orm.Mapped[dict[str, Any]] = orm.mapped_column()
    pipeline_run_annotations: orm.Mapped[dict[str, str]] = orm.mapped_column(
        sql.JSON,
        default_factory=dict,
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    __table_args__ = (
        sql.Index(
            "ix_pipeline_version_pipeline_id_created_at_desc_version_key_desc",
            pipeline_id,
            created_at.desc(),
            version_key.desc(),
        ),
    )


def register_db_tables() -> None:
    """Ensure the models are imported before shared metadata is created."""
    logger.info(
        "Versioned user pipeline tables registered: %s, %s",
        UserPipeline.__tablename__,
        UserPipelineVersion.__tablename__,
    )
