"""Notice DB table.

The table is registered on the shared `bts._TableBase.metadata`, so
`metadata.create_all()` creates it the same way the upstream table is created.
"""

import datetime
import enum
import logging
from typing import Any

import sqlalchemy as sql
from cloud_pipelines_backend import backend_types_sql as bts
from sqlalchemy import orm

logger = logging.getLogger(__name__)


class NoticeVariant(str, enum.Enum):
    INFO = "info"
    WARNING = "warning"
    SUCCESS = "success"
    ERROR = "error"


class Notice(bts._TableBase):
    """Site-wide announcement notice shown to all users.

    `title` and `action_text` are plain text. `body` is untrusted Markdown that is
    stored verbatim and never sanitized by the backend, so renderers must disable
    raw HTML and must not trust the link targets inside it. `action_url` is the
    optional primary action and is restricted to absolute http(s) URLs.

    Notices are never hard-deleted: `deleted_at` marks a notice as removed.
    """

    __tablename__ = "notice"
    id: orm.Mapped[bts.IdType] = orm.mapped_column(
        primary_key=True, init=False, insert_default=bts.generate_unique_id
    )
    title: orm.Mapped[str]
    # `str` is mapped to VARCHAR(255) by default, which is too short for the body.
    body: orm.Mapped[str] = orm.mapped_column(sql.Text())
    variant: orm.Mapped[NoticeVariant] = orm.mapped_column(
        # `values_callable` makes the DB store the enum values ("warning")
        # instead of the default enum member names ("WARNING").
        sql.Enum(
            NoticeVariant,
            values_callable=lambda enum_class: [member.value for member in enum_class],
        )
    )
    # `mapped_column()` (despite having no arguments) is needed so that the columns
    # can be referenced in `__table_args__` below.
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column()
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column()
    action_url: orm.Mapped[str | None] = orm.mapped_column(
        sql.String(2048), default=None
    )
    action_text: orm.Mapped[str | None] = orm.mapped_column(default=None)
    starts_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    ends_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    is_enabled: orm.Mapped[bool] = orm.mapped_column(default=True)
    is_dismissible: orm.Mapped[bool] = orm.mapped_column(default=False)
    created_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    updated_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    deleted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    __table_args__ = (
        # Used by the "active notices" query.
        sql.Index(
            "ix_notice_is_enabled_deleted_at_starts_at_ends_at",
            is_enabled,
            deleted_at,
            starts_at,
            ends_at,
        ),
        # Used by the admin notice list.
        sql.Index(
            "ix_notice_created_at_desc",
            created_at.desc(),
        ),
    )


def register_db_tables() -> None:
    """Explicitly import this module so Notice is registered
    with _TableBase.metadata before create_all() runs."""
    logger.info(f"Notice tables registered: {Notice.__tablename__}")
