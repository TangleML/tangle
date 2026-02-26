import datetime
import json
import enum
from typing import Final

import sqlalchemy as sql

from . import backend_types_sql as bts
from . import errors
from . import filter_query_models

SYSTEM_KEY_PREFIX: Final[str] = "system/"
_PIPELINE_RUN_KEY_PREFIX: Final[str] = f"{SYSTEM_KEY_PREFIX}pipeline_run."


class SystemKey(enum.StrEnum):
    CREATED_BY = f"{_PIPELINE_RUN_KEY_PREFIX}created_by"
    NAME = f"{_PIPELINE_RUN_KEY_PREFIX}name"
    CREATED_AT = f"{_PIPELINE_RUN_KEY_PREFIX}date.created_at"


SYSTEM_KEY_SUPPORTED_PREDICATES: dict[SystemKey, set[type]] = {
    SystemKey.CREATED_BY: {
        filter_query_models.KeyExistsPredicate,
        filter_query_models.ValueEqualsPredicate,
        filter_query_models.ValueInPredicate,
    },
    SystemKey.NAME: {
        filter_query_models.KeyExistsPredicate,
        filter_query_models.ValueEqualsPredicate,
        filter_query_models.ValueContainsPredicate,
        filter_query_models.ValueInPredicate,
    },
    SystemKey.CREATED_AT: {
        filter_query_models.TimeRangePredicate,
    },
}

# ---------------------------------------------------------------------------
# Cursor encode / decode
# ---------------------------------------------------------------------------

CURSOR_SEPARATOR: Final[str] = "~"


def encode_cursor(created_at: datetime.datetime, run_id: str) -> str:
    """Encode the last row's position as a tilde-separated cursor string.

    The created_at from PipelineRun is naive UTC (no UtcDateTime decorator on
    this column). We stamp it as UTC here so the cursor string is
    timezone-explicit for readability and correctness.
    decode_cursor() normalizes back to naive UTC for DB comparison.
    """
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=datetime.timezone.utc)
    return f"{created_at.isoformat()}{CURSOR_SEPARATOR}{run_id}"


def decode_cursor(cursor: str | None) -> tuple[datetime.datetime, str] | None:
    """Parse a tilde-separated cursor string into (created_at, run_id).

    Returns None for empty/missing cursors. Raises InvalidPageTokenError
    for unrecognized formats (e.g. legacy base64 tokens).
    """
    if not cursor:
        return None
    if CURSOR_SEPARATOR not in cursor:
        raise errors.InvalidPageTokenError(
            f"Unrecognized page_token format. "
            f"Expected 'created_at~id' cursor. token={cursor[:20]}... (truncated)"
        )
    # maxsplit=1: split on first ~ only, so run_id can safely contain ~
    created_at_str, run_id = cursor.split(CURSOR_SEPARATOR, 1)
    created_at = datetime.datetime.fromisoformat(created_at_str)
    # Normalize to naive UTC to match DB storage format (PipelineRun.created_at
    # is plain DateTime, not UtcDateTime -- stores/returns naive datetimes).
    if created_at.tzinfo is not None:
        created_at = created_at.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return created_at, run_id


def maybe_next_page_token(
    *,
    rows: list[bts.PipelineRun],
    page_size: int,
) -> str | None:
    """Return a cursor token for the next page, or None if this is the last page."""
    if len(rows) < page_size:
        return None
    last = rows[page_size - 1]
    return encode_cursor(last.created_at, last.id)


# ---------------------------------------------------------------------------
# SystemKey validation and resolution
# ---------------------------------------------------------------------------


def _get_predicate_key(*, predicate: filter_query_models.Predicate) -> str | None:
    """Extract the annotation key from a leaf predicate, or None for logical operators."""
    match predicate:
        case filter_query_models.KeyExistsPredicate():
            return predicate.key_exists.key
        case filter_query_models.ValueEqualsPredicate():
            return predicate.value_equals.key
        case filter_query_models.ValueContainsPredicate():
            return predicate.value_contains.key
        case filter_query_models.ValueInPredicate():
            return predicate.value_in.key
        case filter_query_models.TimeRangePredicate():
            return predicate.time_range.key
        case _:
            return None


def _check_predicate_allowed(*, predicate: filter_query_models.Predicate) -> None:
    """Raise if a system key is used with an unsupported predicate type."""
    key = _get_predicate_key(predicate=predicate)
    if key is None:
        return

    try:
        system_key = SystemKey(key)
    except ValueError:
        return

    supported = SYSTEM_KEY_SUPPORTED_PREDICATES.get(system_key, set())
    if type(predicate) not in supported:
        raise errors.InvalidAnnotationKeyError(
            f"Predicate {type(predicate).__name__} is not supported "
            f"for system key {system_key!r}. "
            f"Supported: {[t.__name__ for t in supported]}"
        )


def _resolve_system_key_value(
    *,
    key: str,
    value: str,
    current_user: str | None,
) -> str:
    """Resolve special placeholder values for system keys."""
    if key == SystemKey.CREATED_BY and value == "me":
        return current_user if current_user is not None else ""
    return value


def _maybe_resolve_system_values(
    *,
    predicate: filter_query_models.ValueEqualsPredicate,
    current_user: str | None,
) -> filter_query_models.ValueEqualsPredicate:
    """Resolve special values in a ValueEqualsPredicate."""
    key = predicate.value_equals.key
    value = predicate.value_equals.value
    resolved = _resolve_system_key_value(
        key=key,
        value=value,
        current_user=current_user,
    )
    if resolved != value:
        return filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(key=key, value=resolved)
        )
    return predicate


def _validate_and_resolve_predicate(
    *,
    predicate: filter_query_models.Predicate,
    current_user: str | None,
) -> filter_query_models.Predicate:
    """Validate system key support, then resolve special values."""
    _check_predicate_allowed(predicate=predicate)
    if isinstance(predicate, filter_query_models.ValueEqualsPredicate):
        return _maybe_resolve_system_values(
            predicate=predicate,
            current_user=current_user,
        )
    return predicate


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_list_filters(
    *,
    filter_value: str | None,
    filter_query_value: str | None,
    cursor_value: str | None,
    current_user: str | None,
) -> list[sql.ColumnElement]:
    """Build WHERE clauses from filters and cursor."""
    if filter_value and filter_query_value:
        raise errors.MutuallyExclusiveFilterError(
            "Cannot use both 'filter' and 'filter_query'. Use one or the other."
        )

    if filter_value:
        filter_query_value = _convert_legacy_filter_to_filter_query(
            filter_value=filter_value,
        )

    where_clauses: list[sql.ColumnElement] = []
    if filter_query_value:
        parsed = filter_query_models.FilterQuery.model_validate_json(filter_query_value)
        where_clauses.append(
            filter_query_to_where_clause(
                filter_query=parsed,
                current_user=current_user,
            )
        )

    cursor = decode_cursor(cursor_value)
    if cursor:
        cursor_created_at, cursor_id = cursor
        where_clauses.append(
            sql.tuple_(bts.PipelineRun.created_at, bts.PipelineRun.id)
            < sql.tuple_(
                sql.literal(cursor_created_at),
                sql.literal(cursor_id),
            )
        )

    return where_clauses


def filter_query_to_where_clause(
    *,
    filter_query: filter_query_models.FilterQuery,
    current_user: str | None = None,
) -> sql.ColumnElement:
    predicates = filter_query.and_ or filter_query.or_
    is_and = filter_query.and_ is not None
    clauses = [
        _predicate_to_clause(predicate=p, current_user=current_user) for p in predicates
    ]
    return sql.and_(*clauses) if is_and else sql.or_(*clauses)


# ---------------------------------------------------------------------------
# Legacy filter parsing
# ---------------------------------------------------------------------------


def _parse_filter(filter: str) -> dict[str, str]:
    # TODO: Improve
    parts = filter.strip().split()
    parsed_filter = {}
    for part in parts:
        key, sep, value = part.partition(":")
        if sep:
            parsed_filter[key] = value
        else:
            parsed_filter.setdefault("_text", "")
            parsed_filter["_text"] += part
    return parsed_filter


def _convert_legacy_filter_to_filter_query(
    *,
    filter_value: str,
) -> str:
    """Convert a legacy ``filter`` string to an equivalent ``filter_query`` JSON string.

    Only ``created_by`` is supported. ``"me"`` is NOT resolved here — the
    downstream ``_maybe_resolve_system_values`` handles that.
    """
    parsed = _parse_filter(filter_value)
    predicates: list[dict] = []
    for key, value in parsed.items():
        if key == "_text":
            raise NotImplementedError("Text search is not implemented yet.")
        elif key == "created_by":
            if not value:
                raise errors.ApiValidationError(
                    "Legacy filter 'created_by' requires a non-empty value."
                )
            predicates.append(
                {"value_equals": {"key": SystemKey.CREATED_BY, "value": value}}
            )
        else:
            raise NotImplementedError(f"Unsupported filter {filter_value}.")
    return json.dumps({"and": predicates})


# ---------------------------------------------------------------------------
# Predicate dispatch
# ---------------------------------------------------------------------------


def _predicate_to_clause(
    *,
    predicate: filter_query_models.Predicate,
    current_user: str | None = None,
) -> sql.ColumnElement:
    predicate = _validate_and_resolve_predicate(
        predicate=predicate,
        current_user=current_user,
    )

    match predicate:
        case filter_query_models.AndPredicate():
            return sql.and_(
                *[
                    _predicate_to_clause(predicate=p, current_user=current_user)
                    for p in predicate.and_
                ]
            )
        case filter_query_models.OrPredicate():
            return sql.or_(
                *[
                    _predicate_to_clause(predicate=p, current_user=current_user)
                    for p in predicate.or_
                ]
            )
        case filter_query_models.NotPredicate():
            return sql.not_(
                _predicate_to_clause(
                    predicate=predicate.not_, current_user=current_user
                )
            )
        case filter_query_models.KeyExistsPredicate():
            return _key_exists_to_clause(predicate=predicate)
        case filter_query_models.ValueEqualsPredicate():
            return _value_equals_to_clause(predicate=predicate)
        case filter_query_models.ValueContainsPredicate():
            return _value_contains_to_clause(predicate=predicate)
        case filter_query_models.ValueInPredicate():
            return _value_in_to_clause(predicate=predicate)
        case filter_query_models.TimeRangePredicate():
            return _time_range_to_clause(predicate=predicate)
        case _:
            raise NotImplementedError(
                f"Predicate type {type(predicate).__name__} is not yet implemented."
            )


# ---------------------------------------------------------------------------
# Generic annotation-based leaf predicates
# ---------------------------------------------------------------------------


def _annotation_exists(*, conditions: list[sql.ColumnElement]) -> sql.ColumnElement:
    return sql.exists(
        sql.select(bts.PipelineRunAnnotation.pipeline_run_id).where(
            bts.PipelineRunAnnotation.pipeline_run_id == bts.PipelineRun.id,
            *conditions,
        )
    )


def _key_exists_to_clause(
    *, predicate: filter_query_models.KeyExistsPredicate
) -> sql.ColumnElement:
    return _annotation_exists(
        conditions=[bts.PipelineRunAnnotation.key == predicate.key_exists.key],
    )


def _value_equals_to_clause(
    *, predicate: filter_query_models.ValueEqualsPredicate
) -> sql.ColumnElement:
    return _annotation_exists(
        conditions=[
            bts.PipelineRunAnnotation.key == predicate.value_equals.key,
            bts.PipelineRunAnnotation.value == predicate.value_equals.value,
        ],
    )


def _value_contains_to_clause(
    *, predicate: filter_query_models.ValueContainsPredicate
) -> sql.ColumnElement:
    return _annotation_exists(
        conditions=[
            bts.PipelineRunAnnotation.key == predicate.value_contains.key,
            bts.PipelineRunAnnotation.value.contains(
                predicate.value_contains.value_substring
            ),
        ],
    )


def _value_in_to_clause(
    *, predicate: filter_query_models.ValueInPredicate
) -> sql.ColumnElement:
    return _annotation_exists(
        conditions=[
            bts.PipelineRunAnnotation.key == predicate.value_in.key,
            bts.PipelineRunAnnotation.value.in_(predicate.value_in.values),
        ],
    )


# ---------------------------------------------------------------------------
# Column-based predicates (bypass annotation table)
# ---------------------------------------------------------------------------


def _time_range_to_clause(
    *, predicate: filter_query_models.TimeRangePredicate
) -> sql.ColumnElement:
    """Build a WHERE clause for pipeline_run.created_at from a time range.

    Pydantic's AwareDatetime preserves the original timezone offset, so we
    must normalize to naive UTC before comparing against the DB column.

    The DB stores "naive UTC" datetimes -- the values represent UTC but carry
    no timezone label. For example, the DB stores '2024-01-01 02:30:00', not
    '2024-01-01 02:30:00+00:00'. The UtcDateTime type decorator (in
    backend_types_sql.py) strips tzinfo on write and re-attaches UTC on read.

    Conversion pipeline for input '2024-01-01T08:00:00+05:30':

      API request (JSON string)
        '2024-01-01T08:00:00+05:30'
              |
              v
      Pydantic AwareDatetime (preserves offset)
        datetime(2024, 1, 1, 8, 0, 0, tzinfo=+05:30)
              |
              v  .astimezone(utc) -- converts 08:00 - 05:30 = 02:30
      UTC-aware datetime
        datetime(2024, 1, 1, 2, 30, 0, tzinfo=UTC)
              |
              v  .replace(tzinfo=None) -- strips timezone label
      Naive datetime
        datetime(2024, 1, 1, 2, 30, 0)
              |
              v  SQLAlchemy literal_binds -- adds microsecond precision
      SQL string
        '2024-01-01 02:30:00.000000'  <-- matches DB storage format
    """
    tr = predicate.time_range
    if tr.key != SystemKey.CREATED_AT:
        raise errors.InvalidAnnotationKeyError(
            f"time_range only supports key {SystemKey.CREATED_AT!r}, got {tr.key!r}"
        )
    # Convert aware datetimes to naive UTC to match DB storage format.
    start_utc = tr.start_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    clauses: list[sql.ColumnElement] = [bts.PipelineRun.created_at >= start_utc]
    if tr.end_time is not None:
        end_utc = tr.end_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        clauses.append(bts.PipelineRun.created_at < end_utc)
    return sql.and_(*clauses)
