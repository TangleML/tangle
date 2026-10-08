"""Notice API service, request and response types.

Validation failures raise the core API's `errors.ApiValidationError` /
`errors.ItemNotFoundError`, whose exception handlers (422 / 404) are already
registered by `api_router.setup_routes`.
"""

import dataclasses
import datetime
import urllib.parse
from typing import Any

import pydantic
import sqlalchemy as sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import errors
from cloud_pipelines_backend.notices import db_models
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm

MAX_NOTICE_TITLE_LENGTH = 120
MAX_NOTICE_BODY_LENGTH = 5000
MAX_NOTICE_ACTION_TEXT_LENGTH = 80
# Must not exceed the length of the `Notice.action_url` column.
MAX_NOTICE_ACTION_URL_LENGTH = 2048


def _convert_datetime_to_naive_utc(
    *,
    value: datetime.datetime | None,
) -> datetime.datetime | None:
    """Converts a datetime to the "naive UTC" format that the DB stores.

    Same conversion as `filter_query_sql._time_range_to_clause`: an aware datetime is
    converted to UTC and then stripped of its timezone label, so that a `starts_at` of
    `2026-01-01T12:00:00+02:00` is stored as `10:00`, not as `12:00`. A naive datetime
    is already in the stored format, so it is returned unchanged (`bts.UtcDateTime`
    makes the same assumption when reading such values back).
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def _validate_notice_title(*, title: str) -> str:
    title = (title or "").strip()
    if not title:
        raise errors.ApiValidationError("Notice title must not be empty.")
    if len(title) > MAX_NOTICE_TITLE_LENGTH:
        raise errors.ApiValidationError(
            f"Notice title must be at most {MAX_NOTICE_TITLE_LENGTH} characters, but got {len(title)}."
        )
    return title


def _validate_notice_body(*, body: str) -> str:
    body = (body or "").strip()
    if not body:
        raise errors.ApiValidationError("Notice body must not be empty.")
    if len(body) > MAX_NOTICE_BODY_LENGTH:
        raise errors.ApiValidationError(
            f"Notice body must be at most {MAX_NOTICE_BODY_LENGTH} characters, but got {len(body)}."
        )
    return body


def _validate_notice_action_url(*, action_url: str | None) -> str | None:
    if action_url is None:
        return None
    action_url = action_url.strip()
    if not action_url:
        return None
    if len(action_url) > MAX_NOTICE_ACTION_URL_LENGTH:
        raise errors.ApiValidationError(
            f"Notice action URL must be at most {MAX_NOTICE_ACTION_URL_LENGTH} characters, but got {len(action_url)}."
        )
    parsed_url = urllib.parse.urlparse(action_url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
        raise errors.ApiValidationError(
            f"Notice action URL must be an absolute http or https URL, but got {action_url!r}."
        )
    return action_url


def _validate_notice_action_text(*, action_text: str | None) -> str | None:
    if action_text is None:
        return None
    action_text = action_text.strip()
    if not action_text:
        return None
    if len(action_text) > MAX_NOTICE_ACTION_TEXT_LENGTH:
        raise errors.ApiValidationError(
            f"Notice action text must be at most {MAX_NOTICE_ACTION_TEXT_LENGTH} characters, but got {len(action_text)}."
        )
    return action_text


def _validate_notice_cross_field_constraints(*, notice: db_models.Notice) -> None:
    if notice.action_text and not notice.action_url:
        raise errors.ApiValidationError(
            "Notice action text is only meaningful together with an action URL."
        )
    # A value loaded from the DB is timezone-aware, while a value that was just
    # assigned is naive, so both are normalized before being compared.
    starts_at = _convert_datetime_to_naive_utc(value=notice.starts_at)
    ends_at = _convert_datetime_to_naive_utc(value=notice.ends_at)
    if starts_at is not None and ends_at is not None and ends_at <= starts_at:
        raise errors.ApiValidationError(
            f"Notice ends_at ({ends_at}) must be after starts_at ({starts_at})."
        )


def _get_public_notice_fields(*, notice: db_models.Notice) -> dict[str, Any]:
    return dict(
        id=notice.id,
        title=notice.title,
        body=notice.body,
        variant=notice.variant,
        action_url=notice.action_url,
        action_text=notice.action_text,
        starts_at=notice.starts_at,
        ends_at=notice.ends_at,
        is_dismissible=notice.is_dismissible,
        created_at=notice.created_at,
        updated_at=notice.updated_at,
    )


@dataclasses.dataclass(kw_only=True)
class NoticeResponse:
    """A notice as exposed to all users.

    `body` is untrusted Markdown that the backend does not sanitize (see `db_models.Notice`).
    """

    id: bts.IdType
    title: str
    body: str
    variant: db_models.NoticeVariant
    action_url: str | None = None
    action_text: str | None = None
    starts_at: datetime.datetime | None = None
    ends_at: datetime.datetime | None = None
    is_dismissible: bool = False
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None

    @staticmethod
    def from_db(*, notice: db_models.Notice) -> "NoticeResponse":
        return NoticeResponse(**_get_public_notice_fields(notice=notice))


@dataclasses.dataclass(kw_only=True)
class AdminNoticeResponse(NoticeResponse):
    """A notice including the fields that are only exposed to admins."""

    is_enabled: bool = True
    created_by: str | None = None
    updated_by: str | None = None
    deleted_at: datetime.datetime | None = None

    @staticmethod
    def from_db(*, notice: db_models.Notice) -> "AdminNoticeResponse":
        return AdminNoticeResponse(
            **_get_public_notice_fields(notice=notice),
            is_enabled=notice.is_enabled,
            created_by=notice.created_by,
            updated_by=notice.updated_by,
            deleted_at=notice.deleted_at,
        )


@dataclasses.dataclass(kw_only=True)
class ListNoticesResponse:
    notices: list[NoticeResponse]


@dataclasses.dataclass(kw_only=True)
class ListAdminNoticesResponse:
    notices: list[AdminNoticeResponse]


@dataclasses.dataclass(kw_only=True)
class CreateNoticeRequest:
    title: str
    body: str
    variant: db_models.NoticeVariant
    action_url: str | None = None
    action_text: str | None = None
    # `AwareDatetime` requires timezone info (e.g. "2026-01-01T00:00:00Z"), so that
    # the intended point in time is never guessed. Same as `filter_query_models`.
    starts_at: pydantic.AwareDatetime | None = None
    ends_at: pydantic.AwareDatetime | None = None
    is_enabled: bool = True
    is_dismissible: bool = False


@dataclasses.dataclass(kw_only=True)
class UpdateNoticeRequest:
    """A partial notice update. The fields that are not set (None) are not changed."""

    title: str | None = None
    body: str | None = None
    variant: db_models.NoticeVariant | None = None
    action_url: str | None = None
    action_text: str | None = None
    starts_at: pydantic.AwareDatetime | None = None
    ends_at: pydantic.AwareDatetime | None = None
    is_enabled: bool | None = None
    is_dismissible: bool | None = None


class NoticesApiService_Sql:
    def list_active(self, *, session: orm.Session) -> ListNoticesResponse:
        current_time = db_utils.utc_now()
        query = (
            sql.select(db_models.Notice)
            .where(
                db_models.Notice.deleted_at.is_(None),
                # `== True` (not `is True`) is what builds a SQL clause here.
                db_models.Notice.is_enabled == True,  # noqa: E712
                sql.or_(
                    db_models.Notice.starts_at.is_(None),
                    db_models.Notice.starts_at <= current_time,
                ),
                sql.or_(
                    db_models.Notice.ends_at.is_(None),
                    db_models.Notice.ends_at > current_time,
                ),
            )
            .order_by(
                # `starts_at DESC NULLS LAST`. The NULLS LAST modifier is not
                # portable (MySQL does not support it), so the notices without
                # `starts_at` are sorted last explicitly (False sorts before True).
                db_models.Notice.starts_at.is_(None),
                db_models.Notice.starts_at.desc(),
                db_models.Notice.created_at.desc(),
                # MySQL `DATETIME` only has second precision, so `id` is needed to
                # keep the order of notices with equal timestamps deterministic.
                db_models.Notice.id,
            )
        )
        notices = session.scalars(query).all()
        return ListNoticesResponse(
            notices=[NoticeResponse.from_db(notice=notice) for notice in notices]
        )

    def list_all(
        self, *, session: orm.Session, include_deleted: bool = False
    ) -> ListAdminNoticesResponse:
        query = sql.select(db_models.Notice).order_by(
            db_models.Notice.created_at.desc(), db_models.Notice.id
        )
        if not include_deleted:
            query = query.where(db_models.Notice.deleted_at.is_(None))
        notices = session.scalars(query).all()
        return ListAdminNoticesResponse(
            notices=[AdminNoticeResponse.from_db(notice=notice) for notice in notices]
        )

    def get(self, *, session: orm.Session, id: bts.IdType) -> AdminNoticeResponse:
        notice_row = session.get(db_models.Notice, id)
        if not notice_row:
            raise errors.ItemNotFoundError(f"Notice with {id=} does not exist.")
        return AdminNoticeResponse.from_db(notice=notice_row)

    def create(
        self,
        *,
        session: orm.Session,
        notice: CreateNoticeRequest,
        user_name: str | None = None,
    ) -> AdminNoticeResponse:
        current_time = db_utils.utc_now()
        notice_row = db_models.Notice(
            title=_validate_notice_title(title=notice.title),
            body=_validate_notice_body(body=notice.body),
            variant=notice.variant,
            action_url=_validate_notice_action_url(action_url=notice.action_url),
            action_text=_validate_notice_action_text(action_text=notice.action_text),
            starts_at=_convert_datetime_to_naive_utc(value=notice.starts_at),
            ends_at=_convert_datetime_to_naive_utc(value=notice.ends_at),
            is_enabled=notice.is_enabled,
            is_dismissible=notice.is_dismissible,
            created_by=user_name,
            updated_by=user_name,
            created_at=current_time,
            updated_at=current_time,
        )
        _validate_notice_cross_field_constraints(notice=notice_row)
        session.add(notice_row)
        session.commit()
        session.refresh(notice_row)
        return AdminNoticeResponse.from_db(notice=notice_row)

    def update(
        self,
        *,
        session: orm.Session,
        id: bts.IdType,
        notice: UpdateNoticeRequest,
        user_name: str | None = None,
    ) -> AdminNoticeResponse:
        notice_row = session.get(db_models.Notice, id)
        if not notice_row:
            raise errors.ItemNotFoundError(f"Notice with {id=} does not exist.")
        if notice_row.deleted_at is not None:
            raise errors.ApiValidationError(
                f"Notice with {id=} is deleted and cannot be updated."
            )
        if notice.title is not None:
            notice_row.title = _validate_notice_title(title=notice.title)
        if notice.body is not None:
            notice_row.body = _validate_notice_body(body=notice.body)
        if notice.variant is not None:
            notice_row.variant = notice.variant
        if notice.action_url is not None:
            notice_row.action_url = _validate_notice_action_url(
                action_url=notice.action_url
            )
        if notice.action_text is not None:
            notice_row.action_text = _validate_notice_action_text(
                action_text=notice.action_text
            )
        if notice.starts_at is not None:
            notice_row.starts_at = _convert_datetime_to_naive_utc(
                value=notice.starts_at
            )
        if notice.ends_at is not None:
            notice_row.ends_at = _convert_datetime_to_naive_utc(value=notice.ends_at)
        if notice.is_enabled is not None:
            notice_row.is_enabled = notice.is_enabled
        if notice.is_dismissible is not None:
            notice_row.is_dismissible = notice.is_dismissible
        _validate_notice_cross_field_constraints(notice=notice_row)
        notice_row.updated_by = user_name
        notice_row.updated_at = db_utils.utc_now()
        session.commit()
        session.refresh(notice_row)
        return AdminNoticeResponse.from_db(notice=notice_row)

    def delete(
        self,
        *,
        session: orm.Session,
        id: bts.IdType,
        user_name: str | None = None,
    ) -> AdminNoticeResponse:
        """Soft-deletes the notice. Notices are never removed from the DB."""
        notice_row = session.get(db_models.Notice, id)
        if not notice_row:
            raise errors.ItemNotFoundError(f"Notice with {id=} does not exist.")
        if notice_row.deleted_at is None:
            current_time = db_utils.utc_now()
            notice_row.deleted_at = current_time
            notice_row.updated_by = user_name
            notice_row.updated_at = current_time
            session.commit()
            session.refresh(notice_row)
        return AdminNoticeResponse.from_db(notice=notice_row)
