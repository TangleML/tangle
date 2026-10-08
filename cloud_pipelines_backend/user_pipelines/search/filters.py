"""Compile the shared filter grammar against saved pipeline fields.

The caller supplies the authenticated identity and joins each stable pipeline to
its current version. This module does not make access-control decisions.
"""

import datetime
import json
import uuid
from typing import Any, Final

import pydantic
import sqlalchemy as sql
from cloud_pipelines_backend import filter_query_models as filters
from cloud_pipelines_backend.user_pipelines.db_models import (
    PipelineVersioningMode,
    UserPipeline,
    UserPipelineVersion,
)
from cloud_pipelines_backend.user_pipelines.errors import (
    PipelineValidationError,
)

MAX_FILTER_QUERY_LENGTH: Final[int] = 16_384
MAX_FILTER_QUERY_DEPTH: Final[int] = 16
MAX_FILTER_PREDICATES: Final[int] = 100
MAX_FILTER_LIST_ITEMS: Final[int] = 100


def pipeline_name_column() -> sql.ColumnElement[str]:
    """Return the display name from the version selected by the caller's join."""
    return UserPipelineVersion.extra_data["pipeline_name"].as_string()


_EQUALITY_PREDICATES = (
    filters.KeyExistsPredicate,
    filters.ValueEqualsPredicate,
    filters.ValueInPredicate,
)
_TEXT_PREDICATES = (*_EQUALITY_PREDICATES, filters.ValueContainsPredicate)
_SEARCH_FIELDS = {
    "system/pipeline.id": (UserPipeline.id, _EQUALITY_PREDICATES),
    "system/pipeline.user_id": (UserPipeline.user_id, _EQUALITY_PREDICATES),
    "system/pipeline.name": (pipeline_name_column(), _TEXT_PREDICATES),
    "system/pipeline.file_path": (UserPipeline.file_path, _TEXT_PREDICATES),
    "system/pipeline.versioning_mode": (
        UserPipeline.versioning_mode,
        _EQUALITY_PREDICATES,
    ),
    "system/pipeline.date.created_at": (
        UserPipeline.created_at,
        (filters.TimeRangePredicate,),
    ),
    "system/pipeline.date.updated_at": (
        UserPipeline.updated_at,
        (filters.TimeRangePredicate,),
    ),
}
_LEAF_OPERATORS = frozenset(
    {"key_exists", "value_equals", "value_in", "value_contains", "time_range"}
)


def _validate_query_limits(value: Any) -> None:
    """Bound nested input before invoking the recursive Pydantic union."""
    pending = [(value, 1)]
    predicates = 0
    while pending:
        node, depth = pending.pop()
        if not isinstance(node, (dict, list)):
            continue
        if depth > MAX_FILTER_QUERY_DEPTH:
            raise PipelineValidationError(
                f"filter_query cannot exceed {MAX_FILTER_QUERY_DEPTH} nested JSON levels."
            )
        if isinstance(node, dict):
            predicates += bool(_LEAF_OPERATORS.intersection(node))
            children = node.values()
        else:
            if len(node) > MAX_FILTER_LIST_ITEMS:
                raise PipelineValidationError(
                    f"filter_query lists cannot exceed {MAX_FILTER_LIST_ITEMS} items."
                )
            children = node
        if predicates > MAX_FILTER_PREDICATES:
            raise PipelineValidationError(
                f"filter_query cannot exceed {MAX_FILTER_PREDICATES} predicates."
            )
        pending.extend((child, depth + 1) for child in children)


def parse_filter_query(value: str | None) -> filters.FilterQuery | None:
    """Parse a bounded JSON filter, returning concise public validation errors."""
    if value is None:
        return None
    if len(value) > MAX_FILTER_QUERY_LENGTH:
        raise PipelineValidationError(
            f"filter_query cannot exceed {MAX_FILTER_QUERY_LENGTH} characters."
        )
    try:
        decoded = json.loads(value)
    except (ValueError, RecursionError) as exc:
        raise PipelineValidationError("filter_query must contain valid JSON.") from exc
    _validate_query_limits(decoded)
    try:
        return filters.FilterQuery.model_validate(decoded)
    except pydantic.ValidationError as exc:
        raise PipelineValidationError(
            "filter_query must match the filter grammar with exactly one 'and' or 'or' root."
        ) from exc


def _resolve_value(*, key: str, value: str, current_user: str) -> str:
    if key == "system/pipeline.user_id" and value == "me":
        return current_user
    if key == "system/pipeline.id":
        try:
            return str(uuid.UUID(value))
        except ValueError as exc:
            raise PipelineValidationError(
                "Values for system/pipeline.id must be valid UUIDs."
            ) from exc
    if key == "system/pipeline.versioning_mode":
        try:
            return PipelineVersioningMode(value).value
        except ValueError as exc:
            raise PipelineValidationError(
                "Values for system/pipeline.versioning_mode must be 'disabled' or 'full'."
            ) from exc
    return value


def _utc_naive(value: datetime.datetime) -> datetime.datetime:
    try:
        return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    except OverflowError as exc:
        raise PipelineValidationError(
            "Time range bounds must remain within years 1 through 9999 in UTC."
        ) from exc


def _annotation_column(*, key: str, dialect_name: str) -> sql.ColumnElement[str]:
    if dialect_name == "sqlite":
        # Older SQLite versions cannot resolve quoted keys through JSON paths.
        # Iterate only the annotations object and compare its decoded keys as
        # bound strings, so quotes, dots, and backslashes stay literal.
        annotations = sql.func.json_each(
            UserPipelineVersion.root_pipeline_task,
            "$.componentRef.spec.metadata.annotations",
        ).table_valued(sql.column("key", sql.String), sql.column("value", sql.String))
        return (
            sql.select(annotations.c.value)
            .where(annotations.c.key == key)
            .correlate(UserPipelineVersion)
            .scalar_subquery()
        )
    # MySQL wraps JSON path elements in quotes without escaping their contents.
    escaped_key = json.dumps(key)[1:-1]
    return UserPipelineVersion.root_pipeline_task[
        ("componentRef", "spec", "metadata", "annotations", escaped_key)
    ].as_string()


def _compile_leaf(
    predicate: filters.LeafPredicate, *, current_user: str, dialect_name: str
) -> sql.ColumnElement[bool]:
    key = predicate.key
    field = _SEARCH_FIELDS.get(key)
    is_annotation = not key.startswith("system/")
    if is_annotation:
        field = (
            # Search annotations displayed on the saved pipeline, not defaults
            # for future runs stored in pipeline_run_annotations.
            _annotation_column(key=key, dialect_name=dialect_name),
            _TEXT_PREDICATES,
        )
    if field is None:
        raise PipelineValidationError(f"Unsupported pipeline search field: {key!r}.")
    column, supported = field
    if not isinstance(predicate, supported):
        raise PipelineValidationError(
            f"{next(iter(predicate.model_dump(by_alias=True)))} is not supported for {key!r}."
        )
    if isinstance(predicate, filters.KeyExistsPredicate):
        return column.is_not(None)
    if isinstance(predicate, filters.ValueEqualsPredicate):
        expression = column == _resolve_value(
            key=key,
            value=predicate.value_equals.value,
            current_user=current_user,
        )
    elif isinstance(predicate, filters.ValueInPredicate):
        expression = column.in_(
            [
                _resolve_value(key=key, value=value, current_user=current_user)
                for value in predicate.value_in.values
            ]
        )
    elif isinstance(predicate, filters.ValueContainsPredicate):
        # Escape LIKE metacharacters so the supplied value is a literal substring.
        expression = column.icontains(
            predicate.value_contains.value_substring, autoescape=True
        )
    else:
        bounds = predicate.time_range
        clauses = []
        if bounds.start_time is not None:
            clauses.append(column >= _utc_naive(bounds.start_time))
        if bounds.end_time is not None:
            clauses.append(column < _utc_naive(bounds.end_time))
        expression = sql.and_(*clauses)
    # Equality/in deliberately use the database column's own collation, as do
    # the existing pipeline ownership and identity comparisons.
    if is_annotation or key == "system/pipeline.name":
        # Missing JSON values fail positives and satisfy their negations.
        # Keep nonnullable fields bare so indexed comparisons remain usable.
        return sql.func.coalesce(expression, sql.false())
    return expression


def _compile_predicate(
    predicate: filters.Predicate, *, current_user: str, dialect_name: str
) -> sql.ColumnElement[bool]:
    if isinstance(predicate, filters.AndPredicate):
        combine, children = sql.and_, predicate.and_
    elif isinstance(predicate, filters.OrPredicate):
        combine, children = sql.or_, predicate.or_
    elif isinstance(predicate, filters.NotPredicate):
        return sql.not_(
            _compile_leaf(
                predicate.not_,
                current_user=current_user,
                dialect_name=dialect_name,
            )
        )
    else:
        return _compile_leaf(
            predicate, current_user=current_user, dialect_name=dialect_name
        )
    return combine(
        *(
            _compile_predicate(
                child, current_user=current_user, dialect_name=dialect_name
            )
            for child in children
        )
    )


def compile_filter_query(
    query: filters.FilterQuery | None, *, current_user: str, dialect_name: str
) -> sql.ColumnElement[bool]:
    """Compile a parsed query without modifying it or adding access constraints."""
    if query is None:
        return sql.true()
    predicates = query.and_ if query.and_ is not None else query.or_
    clauses = [
        _compile_predicate(
            predicate, current_user=current_user, dialect_name=dialect_name
        )
        for predicate in predicates or []
    ]
    return sql.and_(*clauses) if query.and_ is not None else sql.or_(*clauses)
