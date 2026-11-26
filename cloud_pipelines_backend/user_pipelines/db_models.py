import datetime
import enum
import logging
import uuid
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import orm

from .. import backend_types_sql as bts

logger = logging.getLogger(__name__)

MAX_FILE_PATH_LENGTH: Final[int] = bts._STR_MAX_LENGTH
DIGEST_LENGTH: Final[int] = 64
CURRENT_VERSION_KEY: Final[str] = "current"
PIPELINE_ID_LENGTH: Final[int] = 36


class PipelineVersioningMode(str, enum.Enum):
    DISABLED = "disabled"
    FULL = "full"


def utc_now() -> datetime.datetime:
    """The current time in UTC."""
    return datetime.datetime.now(datetime.UTC)


def _generate_pipeline_id() -> str:
    """A new pipeline id."""
    return str(uuid.uuid4())


class UserPipeline(bts._TableBase):
    """A saved pipeline, identified by id and by owner plus file path."""

    __tablename__ = "pipeline"

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
        insert_default=utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=utc_now,
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
    """A pipeline's content: keyed by digest, or `current` when versioning is disabled."""

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
        insert_default=utc_now,
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
    """Marks the saved-pipeline models as part of the shared table metadata."""
    logger.info(
        "Versioned user pipeline tables registered: %s, %s",
        UserPipeline.__tablename__,
        UserPipelineVersion.__tablename__,
    )
