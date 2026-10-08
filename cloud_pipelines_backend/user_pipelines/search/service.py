"""Search active saved definitions independently of pipeline runs."""

import base64
import datetime
import uuid
from typing import Any, Literal

import pydantic
import sqlalchemy as sql
from cloud_pipelines_backend.user_pipelines import db_models
from cloud_pipelines_backend.user_pipelines.errors import (
    PipelineValidationError,
)
from cloud_pipelines_backend.user_pipelines.search import filters
from sqlalchemy import orm

SortField = Literal["name", "updated_at"]
SortDirection = Literal["asc", "desc"]


class _SearchCursor(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    updated_at: pydantic.AwareDatetime
    id: uuid.UUID
    filter_query: str | None
    current_user: str
    sort_field: SortField
    sort_direction: SortDirection
    sort_name: str | None = None

    @pydantic.model_validator(mode="after")
    def _validate_name_boundary(self) -> "_SearchCursor":
        if (self.sort_field == "name") != (self.sort_name is not None):
            raise ValueError("Name cursors require a name boundary")
        return self


def _decode_cursor(page_token: str) -> _SearchCursor:
    try:
        if not page_token:
            raise ValueError("Empty token")
        data = base64.b64decode(
            page_token.encode("ascii"), altchars=b"-_", validate=True
        )
        cursor = _SearchCursor.model_validate_json(data)
        cursor.updated_at = cursor.updated_at.astimezone(datetime.timezone.utc)
        return cursor
    except (ValueError, OverflowError) as exc:
        raise PipelineValidationError("Invalid search page_token.") from exc


def _encode_cursor(
    *,
    updated_at: datetime.datetime,
    pipeline_id: str,
    filter_query: str | None,
    current_user: str,
    sort_field: SortField,
    sort_direction: SortDirection,
    sort_name: str | None,
) -> str:
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    cursor = _SearchCursor(
        updated_at=updated_at.astimezone(datetime.timezone.utc),
        id=uuid.UUID(pipeline_id),
        filter_query=filter_query,
        current_user=current_user,
        sort_field=sort_field,
        sort_direction=sort_direction,
        sort_name=sort_name,
    )
    return base64.urlsafe_b64encode(cursor.model_dump_json().encode()).decode()


def search_pipelines(
    *,
    session: orm.Session,
    current_user: str,
    filter_query: str | None,
    page_size: int,
    page_token: str | None,
    sort_field: SortField | None = None,
    sort_direction: SortDirection | None = None,
) -> tuple[list[dict[str, Any]], int, str | None]:
    """Return summaries, a filtered total, and a continuation for a live query.

    Permission checks belong to the caller. Tokens retain filters and bind the
    ``me`` alias to that caller; they never grant access to a resource.
    """
    if not 1 <= page_size <= 100:
        raise PipelineValidationError("page_size must be between 1 and 100.")
    if sort_field is not None and sort_field not in ("name", "updated_at"):
        raise PipelineValidationError("sort_field must be 'name' or 'updated_at'.")
    if sort_direction is not None and sort_direction not in ("asc", "desc"):
        raise PipelineValidationError("sort_direction must be 'asc' or 'desc'.")

    query = filters.parse_filter_query(filter_query)
    effective_filter = filter_query
    cursor = _decode_cursor(page_token) if page_token is not None else None
    if cursor is not None:
        if cursor.current_user != current_user:
            raise PipelineValidationError(
                "Search page_token belongs to a different caller."
            )
        saved_query = filters.parse_filter_query(cursor.filter_query)
        if filter_query is not None and query != saved_query:
            raise PipelineValidationError(
                "filter_query does not match page_token; restart search without the token."
            )
        if (sort_field is not None and sort_field != cursor.sort_field) or (
            sort_direction is not None and sort_direction != cursor.sort_direction
        ):
            raise PipelineValidationError(
                "Sorting does not match page_token; restart search without the token."
            )
        query = saved_query
        effective_filter = cursor.filter_query
        sort_field = cursor.sort_field
        sort_direction = cursor.sort_direction

    sort_field = sort_field or "updated_at"
    sort_direction = sort_direction or "desc"

    predicate = filters.compile_filter_query(
        query,
        current_user=current_user,
        dialect_name=session.get_bind().dialect.name,
    )
    pipeline = db_models.UserPipeline
    version = db_models.UserPipelineVersion
    statement = (
        sql.select(
            pipeline.id,
            pipeline.user_id,
            pipeline.file_path,
            filters.pipeline_name_column().label("pipeline_name"),
            pipeline.created_at,
            pipeline.updated_at,
            version.content_digest.label("current_version"),
            pipeline.versioning_mode,
        )
        .select_from(pipeline)
        .join(
            version,
            sql.and_(
                version.pipeline_id == pipeline.id,
                version.version_key == pipeline.current_version_key,
            ),
        )
        .where(pipeline.deleted_at.is_(None), predicate)
    )
    # Count the same filtered relation before applying the page boundary.
    total_count = session.scalar(statement.with_only_columns(sql.func.count()))
    sort_column = pipeline.updated_at
    if sort_field == "name":
        sort_column = sql.func.lower(
            sql.func.coalesce(
                sql.func.nullif(filters.pipeline_name_column(), ""),
                pipeline.file_path,
            )
        )
        # Retain the database's comparison value; Python lower() can differ.
        statement = statement.add_columns(sort_column.label("_sort_name"))
    if cursor is not None:
        # Tuple literals do not inherit the column's UTC type decorator.
        boundary = (
            cursor.sort_name
            if sort_field == "name"
            else cursor.updated_at.replace(tzinfo=None)
        )
        position = sql.tuple_(sort_column, pipeline.id)
        cursor_position = sql.tuple_(sql.literal(boundary), sql.literal(str(cursor.id)))
        statement = statement.where(
            position < cursor_position
            if sort_direction == "desc"
            else position > cursor_position
        )
    order = sql.desc if sort_direction == "desc" else sql.asc
    rows = (
        session.execute(
            statement.order_by(order(sort_column), order(pipeline.id)).limit(
                page_size + 1
            )
        )
        .mappings()
        .all()
    )
    results = [dict(row) for row in rows[:page_size]]
    next_page_token = None
    if len(rows) > page_size:
        last = results[-1]
        next_page_token = _encode_cursor(
            updated_at=last["updated_at"],
            pipeline_id=last["id"],
            # Preserve the validated text: canonical datetime serialization can
            # expand a filter past the input limit and break its next page.
            filter_query=effective_filter,
            current_user=current_user,
            sort_field=sort_field,
            sort_direction=sort_direction,
            sort_name=last.get("_sort_name"),
        )
    for result in results:
        result.pop("_sort_name", None)
    return results, total_count or 0, next_page_token
