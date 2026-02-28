import base64
import dataclasses
import json

import sqlalchemy as sql

from . import backend_types_sql as bts
from . import errors
from . import filter_query_models

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

    where_clauses, resolved_filter = _build_filter_where_clauses(
        filter_value=filter_value,
        current_user=current_user,
    )

    if filter_query_value:
        parsed = filter_query_models.FilterQuery.model_validate_json(filter_query_value)
        where_clauses.append(filter_query_to_where_clause(filter_query=parsed))

    next_page_token = PageToken(
        offset=offset + page_size,
        filter=resolved_filter,
        filter_query=filter_query_value,
    )

    return where_clauses, offset, next_page_token


def filter_query_to_where_clause(
    *,
    filter_query: filter_query_models.FilterQuery,
) -> sql.ColumnElement:
    predicates = filter_query.and_ or filter_query.or_
    is_and = filter_query.and_ is not None
    clauses = [_predicate_to_clause(predicate=p) for p in predicates]
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


def _build_filter_where_clauses(
    *,
    filter_value: str | None,
    current_user: str | None,
) -> tuple[list[sql.ColumnElement], str | None]:
    """Parse a filter string into SQLAlchemy WHERE clauses.

    Returns (where_clauses, next_page_filter_value). The second value is the
    filter string with shorthand values resolved (e.g. "created_by:me" becomes
    "created_by:alice@example.com") so it can be embedded in the next page token.
    """
    where_clauses: list[sql.ColumnElement] = []
    parsed_filter = _parse_filter(filter_value) if filter_value else {}
    for key, value in parsed_filter.items():
        if key == "_text":
            raise NotImplementedError("Text search is not implemented yet.")
        elif key == "created_by":
            if value == "me":
                if current_user is None:
                    current_user = ""
                value = current_user
                # TODO: Maybe make this a bit more robust.
                # We need to change the filter since it goes into the next_page_token.
                filter_value = filter_value.replace(
                    "created_by:me", f"created_by:{current_user}"
                )
            if value:
                where_clauses.append(bts.PipelineRun.created_by == value)
            else:
                where_clauses.append(bts.PipelineRun.created_by == None)
        else:
            raise NotImplementedError(f"Unsupported filter {filter_value}.")
    return where_clauses, filter_value


# ---------------------------------------------------------------------------
# Predicate dispatch
# ---------------------------------------------------------------------------


def _predicate_to_clause(
    *,
    predicate,
) -> sql.ColumnElement:
    match predicate:
        case filter_query_models.AndPredicate():
            return sql.and_(
                *[_predicate_to_clause(predicate=p) for p in predicate.and_]
            )
        case filter_query_models.OrPredicate():
            return sql.or_(*[_predicate_to_clause(predicate=p) for p in predicate.or_])
        case filter_query_models.NotPredicate():
            return sql.not_(_predicate_to_clause(predicate=predicate.not_))
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
