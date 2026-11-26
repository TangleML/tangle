import base64
import datetime
import json
import uuid
from typing import Any, Literal

import pydantic
import sqlalchemy as sql
from sqlalchemy import orm

from .. import filter_query_models as filters
from . import db_models, errors

SortField = Literal["name", "updated_at"]
SortDirection = Literal["asc", "desc"]

_PIPELINE = db_models.UserPipeline
_VERSION = db_models.UserPipelineVersion
_PIPELINE_NAME = _VERSION.extra_data["pipeline_name"].as_string()
_EQUALITY = (
    filters.KeyExistsPredicate,
    filters.ValueEqualsPredicate,
    filters.ValueInPredicate,
)
_TEXT = (*_EQUALITY, filters.ValueContainsPredicate)
_TIME = (filters.TimeRangePredicate,)
_FIELDS = {
    "system/pipeline.id": (_PIPELINE.id, _EQUALITY),
    "system/pipeline.user_id": (_PIPELINE.user_id, _EQUALITY),
    "system/pipeline.name": (_PIPELINE_NAME, _TEXT),
    "system/pipeline.file_path": (_PIPELINE.file_path, _TEXT),
    "system/pipeline.versioning_mode": (_PIPELINE.versioning_mode, _EQUALITY),
    "system/pipeline.date.created_at": (_PIPELINE.created_at, _TIME),
    "system/pipeline.date.updated_at": (_PIPELINE.updated_at, _TIME),
}


def search_pipelines(
    *,
    session: orm.Session,
    current_user: str,
    filter_query: str | None,
    page_size: int,
    page_token: str | None,
    sort_field: SortField | None = None,
    sort_direction: SortDirection | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Searches live pipelines' current versions; the token keeps the filter and sort."""
    try:
        token = json.loads(base64.urlsafe_b64decode(page_token)) if page_token else {}
        offset = int(token.get("offset", 0))
    except (ValueError, TypeError, AttributeError) as exc:
        raise errors.PipelineValidationError("Invalid search page_token.") from exc
    filter_query = token.get("filter_query", filter_query)
    sort_field = token.get("sort_field", sort_field or "updated_at")
    sort_direction = token.get("sort_direction", sort_direction or "desc")

    sort_column = (
        sql.func.lower(
            sql.func.coalesce(sql.func.nullif(_PIPELINE_NAME, ""), _PIPELINE.file_path)
        )
        if sort_field == "name"
        else _PIPELINE.updated_at
    )
    order = sql.desc if sort_direction == "desc" else sql.asc
    statement = (
        sql.select(
            _PIPELINE.id,
            _PIPELINE.user_id,
            _PIPELINE.file_path,
            _PIPELINE_NAME.label("pipeline_name"),
            _PIPELINE.created_at,
            _PIPELINE.updated_at,
            _VERSION.content_digest.label("current_version"),
            _PIPELINE.versioning_mode,
        )
        .join(
            _VERSION,
            sql.and_(
                _VERSION.pipeline_id == _PIPELINE.id,
                _VERSION.version_key == _PIPELINE.current_version_key,
            ),
        )
        .where(
            _PIPELINE.deleted_at.is_(None),
            _compile_query(
                _parse_query(filter_query),
                current_user=current_user,
                dialect_name=session.get_bind().dialect.name,
            ),
        )
        .order_by(order(sort_column), order(_PIPELINE.id))
        .offset(offset)
        .limit(page_size + 1)
    )
    rows = [dict(row) for row in session.execute(statement).mappings()]
    next_page_token = None
    if len(rows) > page_size:
        next_page_token = base64.urlsafe_b64encode(
            json.dumps(
                {
                    "offset": offset + page_size,
                    "filter_query": filter_query,
                    "sort_field": sort_field,
                    "sort_direction": sort_direction,
                }
            ).encode()
        ).decode()
    return rows[:page_size], next_page_token


def _parse_query(filter_query: str | None) -> filters.FilterQuery | None:
    """Parses a JSON filter query."""
    if filter_query is None:
        return None
    try:
        return filters.FilterQuery.model_validate_json(filter_query)
    except pydantic.ValidationError as exc:
        raise errors.PipelineValidationError(
            f"Invalid filter_query: {exc.errors(include_url=False)}"
        ) from exc


def _resolve_value(*, key: str, value: str, current_user: str) -> str:
    """Resolves `me` and normalizes ids and modes."""
    try:
        if key == "system/pipeline.user_id" and value == "me":
            return current_user
        if key == "system/pipeline.id":
            return str(uuid.UUID(value))
        if key == "system/pipeline.versioning_mode":
            return db_models.PipelineVersioningMode(value).value
    except ValueError as exc:
        raise errors.PipelineValidationError(
            f"Invalid value for {key!r}: {value!r}."
        ) from exc
    return value


def _annotation_column(*, key: str, dialect_name: str) -> sql.ColumnElement[str]:
    """The value of a pipeline component annotation."""
    if dialect_name == "sqlite":
        annotations = sql.func.json_each(
            _VERSION.root_pipeline_task, "$.componentRef.spec.metadata.annotations"
        ).table_valued(sql.column("key", sql.String), sql.column("value", sql.String))
        return (
            sql.select(annotations.c.value)
            .where(annotations.c.key == key)
            .correlate(_VERSION)
            .scalar_subquery()
        )
    return _VERSION.root_pipeline_task[
        ("componentRef", "spec", "metadata", "annotations", json.dumps(key)[1:-1])
    ].as_string()


def _utc_naive(value: datetime.datetime) -> datetime.datetime:
    """Converts a bound to naive UTC, as stored."""
    return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def _compile_leaf(
    predicate: filters.LeafPredicate, *, current_user: str, dialect_name: str
) -> sql.ColumnElement[bool]:
    """Compiles one field or annotation predicate."""
    key = predicate.key
    is_annotation = not key.startswith("system/")
    if is_annotation:
        column, supported = (
            _annotation_column(key=key, dialect_name=dialect_name),
            _TEXT,
        )
    elif key in _FIELDS:
        column, supported = _FIELDS[key]
    else:
        raise errors.PipelineValidationError(
            f"Unsupported pipeline search field: {key!r}."
        )
    if not isinstance(predicate, supported):
        raise errors.PipelineValidationError(f"Unsupported predicate for {key!r}.")

    def resolve(value: str) -> str:
        return _resolve_value(key=key, value=value, current_user=current_user)

    if isinstance(predicate, filters.KeyExistsPredicate):
        return column.is_not(None)
    if isinstance(predicate, filters.ValueEqualsPredicate):
        expression = column == resolve(predicate.value_equals.value)
    elif isinstance(predicate, filters.ValueInPredicate):
        expression = column.in_([resolve(value) for value in predicate.value_in.values])
    elif isinstance(predicate, filters.ValueContainsPredicate):
        expression = column.icontains(
            predicate.value_contains.value_substring, autoescape=True
        )
    else:
        bounds = predicate.time_range
        expression = sql.and_(
            *([column >= _utc_naive(bounds.start_time)] if bounds.start_time else []),
            *([column < _utc_naive(bounds.end_time)] if bounds.end_time else []),
        )
    if is_annotation or key == "system/pipeline.name":
        return sql.func.coalesce(expression, sql.false())
    return expression


def _compile_query(
    query: filters.FilterQuery | filters.Predicate | None,
    *,
    current_user: str,
    dialect_name: str,
) -> sql.ColumnElement[bool]:
    """Compiles a filter query or predicate to a WHERE clause."""
    if query is None:
        return sql.true()
    if isinstance(query, filters.NotPredicate):
        return sql.not_(
            _compile_query(
                query.not_, current_user=current_user, dialect_name=dialect_name
            )
        )
    if isinstance(
        query, (filters.FilterQuery, filters.AndPredicate, filters.OrPredicate)
    ):
        children = getattr(query, "and_", None)
        combine = sql.and_ if children is not None else sql.or_
        if children is None:
            children = query.or_
        return combine(
            *(
                _compile_query(
                    child, current_user=current_user, dialect_name=dialect_name
                )
                for child in children or []
            )
        )
    return _compile_leaf(query, current_user=current_user, dialect_name=dialect_name)
