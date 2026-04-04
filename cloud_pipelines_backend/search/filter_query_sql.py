import base64
import datetime
import json
import enum
from typing import Any, Final

import sqlalchemy as sql

from .. import backend_types_sql as bts
from .. import errors
from . import filter_query_models

SYSTEM_KEY_PREFIX: Final[str] = "system/"
_PIPELINE_RUN_KEY_PREFIX: Final[str] = f"{SYSTEM_KEY_PREFIX}pipeline_run."


class PipelineRunAnnotationSystemKey(str, enum.Enum):
    # Python <3.11 compat (no enum.StrEnum). Without these overrides:
    #   __str__:  str(MEMBER) returns 'ClassName.MEMBER' instead of the value,
    #             breaking sqlalchemy.literal(str(key)) in backfill queries.
    #   __hash__: hash(MEMBER) != hash("value"), breaking dict lookups like
    #             annotations[MEMBER] when the dict has plain string keys.
    __hash__ = str.__hash__
    __str__ = str.__str__

    CREATED_BY = f"{_PIPELINE_RUN_KEY_PREFIX}created_by"
    PIPELINE_NAME = f"{_PIPELINE_RUN_KEY_PREFIX}name"
    CREATED_AT = f"{_PIPELINE_RUN_KEY_PREFIX}date.created_at"


SYSTEM_KEY_SUPPORTED_PREDICATES: dict[PipelineRunAnnotationSystemKey, set[type]] = {
    PipelineRunAnnotationSystemKey.CREATED_BY: {
        filter_query_models.KeyExistsPredicate,
        filter_query_models.ValueEqualsPredicate,
        filter_query_models.ValueInPredicate,
    },
    PipelineRunAnnotationSystemKey.PIPELINE_NAME: {
        filter_query_models.KeyExistsPredicate,
        filter_query_models.ValueEqualsPredicate,
        filter_query_models.ValueContainsPredicate,
        filter_query_models.ValueInPredicate,
    },
    PipelineRunAnnotationSystemKey.CREATED_AT: {
        filter_query_models.TimeRangePredicate,
    },
}

# ---------------------------------------------------------------------------
# Page-token helpers
# ---------------------------------------------------------------------------

_PAGE_TOKEN_OFFSET_KEY: Final[str] = "offset"
_PAGE_TOKEN_FILTER_KEY: Final[str] = "filter"
_PAGE_TOKEN_FILTER_QUERY_KEY: Final[str] = "filter_query"


def _encode_page_token(*, page_token_dict: dict[str, Any]) -> str:
    return base64.b64encode(json.dumps(page_token_dict).encode("utf-8")).decode("utf-8")


def _decode_page_token(*, page_token: str | None) -> dict[str, Any]:
    return json.loads(base64.b64decode(page_token)) if page_token else {}


def _resolve_filter_value(
    *,
    filter: str | None,
    filter_query: str | None,
    page_token: str | None,
) -> tuple[str | None, str | None, int]:
    """Decode page_token and return the effective (filter_value, filter_query_value, offset).

    If a page_token is present, its stored values take precedence over the
    raw parameters (the token carries resolved values forward across pages).
    """
    page_token_dict = _decode_page_token(page_token=page_token)
    offset = page_token_dict.get(_PAGE_TOKEN_OFFSET_KEY, 0)
    if page_token:
        filter = page_token_dict.get(_PAGE_TOKEN_FILTER_KEY)
        filter_query = page_token_dict.get(_PAGE_TOKEN_FILTER_QUERY_KEY)
    return filter, filter_query, offset


# ---------------------------------------------------------------------------
# PipelineRunAnnotationSystemKey validation and resolution
# ---------------------------------------------------------------------------


def _check_predicate_allowed(*, predicate: filter_query_models.Predicate) -> None:
    """Raise if a system key is used with an unsupported predicate type."""
    if not isinstance(predicate, filter_query_models.KeyPredicateBase):
        return
    key = predicate.key

    try:
        system_key = PipelineRunAnnotationSystemKey(key)
    except ValueError:
        return

    supported = SYSTEM_KEY_SUPPORTED_PREDICATES.get(system_key, set())
    if type(predicate) not in supported:
        raise errors.ApiValidationError(
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
    if key == PipelineRunAnnotationSystemKey.CREATED_BY and value == "me":
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
    page_token_value: str | None,
    current_user: str | None,
    page_size: int,
) -> tuple[list[sql.ColumnElement], int, str]:
    """Resolve pagination token, legacy filter, and filter_query into WHERE clauses.

    Returns (where_clauses, offset, next_page_token_encoded).
    """
    if filter_value and filter_query_value:
        raise errors.ApiValidationError(
            "Cannot use both 'filter' and 'filter_query'. Use one or the other."
        )

    filter_value, filter_query_value, offset = _resolve_filter_value(
        filter=filter_value,
        filter_query=filter_query_value,
        page_token=page_token_value,
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

    next_page_token = _encode_page_token(
        page_token_dict={
            _PAGE_TOKEN_OFFSET_KEY: offset + page_size,
            _PAGE_TOKEN_FILTER_QUERY_KEY: filter_query_value,
        }
    )

    return where_clauses, offset, next_page_token


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
                {
                    "value_equals": {
                        "key": PipelineRunAnnotationSystemKey.CREATED_BY,
                        "value": value,
                    }
                }
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
    if tr.key != PipelineRunAnnotationSystemKey.CREATED_AT:
        raise errors.ApiValidationError(
            "time_range only supports key "
            f"{PipelineRunAnnotationSystemKey.CREATED_AT!r}, got {tr.key!r}"
        )
    # Convert aware datetimes to naive UTC to match DB storage format.
    clauses: list[sql.ColumnElement] = []
    if tr.start_time is not None:
        start_utc = tr.start_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        clauses.append(bts.PipelineRun.created_at >= start_utc)
    if tr.end_time is not None:
        end_utc = tr.end_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        clauses.append(bts.PipelineRun.created_at < end_utc)
    return sql.and_(*clauses)
