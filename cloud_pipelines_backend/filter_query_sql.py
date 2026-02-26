import base64
import dataclasses
import json
import enum
from typing import Final

import sqlalchemy as sql

from . import backend_types_sql as bts
from . import errors
from . import filter_query_models

SYSTEM_KEY_PREFIX: Final[str] = "system/"


class SystemKey(enum.StrEnum):
    CREATED_BY = f"{SYSTEM_KEY_PREFIX}pipeline_run.created_by"


SYSTEM_KEY_SUPPORTED_PREDICATES: dict[SystemKey, set[type]] = {
    SystemKey.CREATED_BY: {
        filter_query_models.KeyExistsPredicate,
        filter_query_models.ValueEqualsPredicate,
        filter_query_models.ValueInPredicate,
    },
}

# ---------------------------------------------------------------------------
# PageToken
# ---------------------------------------------------------------------------


@dataclasses.dataclass(kw_only=True)
class PageToken:
    offset: int = 0
    filter: str | None = None
    filter_query: str | None = None

    def encode(self) -> str:
        return base64.b64encode(
            json.dumps(dataclasses.asdict(self)).encode("utf-8")
        ).decode("utf-8")

    @classmethod
    def decode(cls, token: str | None) -> "PageToken":
        if not token:
            return cls()
        return cls(**json.loads(base64.b64decode(token)))


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
    page_token_value: str | None,
    current_user: str | None,
    page_size: int,
) -> tuple[list[sql.ColumnElement], int, PageToken]:
    """Resolve pagination token, legacy filter, and filter_query into WHERE clauses.

    Returns (where_clauses, offset, next_page_token).
    """
    if filter_value and filter_query_value:
        raise errors.MutuallyExclusiveFilterError(
            "Cannot use both 'filter' and 'filter_query'. Use one or the other."
        )

    page_token = PageToken.decode(page_token_value)
    offset = page_token.offset
    filter_value = page_token.filter if page_token_value else filter_value
    filter_query_value = (
        page_token.filter_query if page_token_value else filter_query_value
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

    next_page_token = PageToken(
        offset=offset + page_size,
        filter=None,
        filter_query=filter_query_value,
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
        case _:
            # TODO: TimeRangePredicate -- not supported currently, will be supported in the future.
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
