"""Translate ComponentSearchQuery models into Elasticsearch DSL.

This module converts the Pydantic predicate tree into an ES query body,
handling filters, semantic (KNN) queries, pagination via ``search_after``,
and ``_source`` field selection.
"""

from collections.abc import Callable
from typing import Any, Final

import fastapi
from starlette import status as http_status

from cloud_pipelines_backend import filter_query_models
from cloud_pipelines_backend.search.elasticsearch import elastic_search_api
from cloud_pipelines_backend.search.published_components import (
    filter_query_models as component_models,
)

# Required for OR semantics — without it, ES treats
# "should" clauses as optional (zero matches is OK).
_MINIMUM_SHOULD_MATCH: Final[int] = 1

# ---------------------------------------------------------------------------
# Predicate → ES clause
# ---------------------------------------------------------------------------


def predicate_to_es_query(
    *,
    predicate: component_models.ComponentPredicate,
) -> dict[str, Any]:
    """Convert a single predicate to an ES query clause.

    Handles leaf predicates directly and recurses for ``and``/``or``/``not``.
    """
    match predicate:
        case filter_query_models.KeyExistsPredicate():
            return {"exists": {"field": predicate.key_exists.key}}
        case filter_query_models.ValueContainsPredicate():
            return {
                "wildcard": {
                    f"{predicate.value_contains.key}.keyword": (
                        f"*{predicate.value_contains.value_substring}*"
                    ),
                },
            }
        case filter_query_models.ValueEqualsPredicate():
            return {
                "term": {
                    f"{predicate.value_equals.key}.keyword": predicate.value_equals.value,
                },
            }
        case filter_query_models.ValueInPredicate():
            return {
                "terms": {
                    f"{predicate.value_in.key}.keyword": predicate.value_in.values,
                },
            }
        case component_models.ValueFuzzyPredicate():
            return {
                "fuzzy": {
                    predicate.value_fuzzy.key: {
                        "value": predicate.value_fuzzy.value,
                        # AUTO lets ES pick a reasonable typo tolerance based
                        # on word length. Hardcoded to prevent misuse by callers.
                        "fuzziness": "AUTO",
                    },
                },
            }
        case component_models.ValueMatchPredicate():
            return {
                "match": {
                    predicate.value_match.key: predicate.value_match.query,
                },
            }
        case component_models.ValueRegexPredicate():
            return {
                "regexp": {
                    f"{predicate.value_regex.key}.keyword": predicate.value_regex.pattern,
                },
            }
        case component_models.ValueEqualsCaseInsensitivePredicate():
            inner = predicate.value_equals_case_insensitive
            return {
                "term": {
                    f"{inner.key}.keyword": {
                        "value": inner.value,
                        "case_insensitive": True,
                    },
                },
            }
        case component_models.ValueContainsCaseInsensitivePredicate():
            inner = predicate.value_contains_case_insensitive
            return {
                "wildcard": {
                    f"{inner.key}.keyword": {
                        "value": f"*{inner.value_substring}*",
                        "case_insensitive": True,
                    },
                },
            }
        case component_models.ValueInCaseInsensitivePredicate():
            inner = predicate.value_in_case_insensitive
            return {
                "bool": {
                    "should": [
                        {
                            "term": {
                                f"{inner.key}.keyword": {
                                    "value": v,
                                    "case_insensitive": True,
                                },
                            },
                        }
                        for v in inner.values
                    ],
                    "minimum_should_match": _MINIMUM_SHOULD_MATCH,
                },
            }
        case component_models.ValueRegexCaseInsensitivePredicate():
            inner = predicate.value_regex_case_insensitive
            return {
                "regexp": {
                    f"{inner.key}.keyword": {
                        "value": inner.pattern,
                        "case_insensitive": True,
                    },
                },
            }
        case component_models.ComponentNotPredicate():
            return {
                "bool": {
                    "must_not": [predicate_to_es_query(predicate=predicate.not_)],
                },
            }
        case component_models.ComponentAndPredicate():
            return {
                "bool": {
                    "must": [
                        predicate_to_es_query(predicate=p) for p in predicate.and_
                    ],
                },
            }
        case component_models.ComponentOrPredicate():
            return {
                "bool": {
                    "should": [
                        predicate_to_es_query(predicate=p) for p in predicate.or_
                    ],
                    "minimum_should_match": _MINIMUM_SHOULD_MATCH,
                },
            }
        case _:
            raise NotImplementedError(
                f"Predicate type {type(predicate).__name__} is not yet implemented."
            )


# ---------------------------------------------------------------------------
# ComponentFilterQuery → ES query body
# ---------------------------------------------------------------------------


def component_filter_to_es(
    *,
    filter_query: component_models.ComponentFilterQuery,
) -> dict[str, Any]:
    """Convert a ``ComponentFilterQuery`` to ``{"query": {"bool": ...}}``."""
    if filter_query.and_ is not None:
        clauses = [predicate_to_es_query(predicate=p) for p in filter_query.and_]
        return {"query": {"bool": {"must": clauses}}}

    if filter_query.or_ is not None:
        clauses = [predicate_to_es_query(predicate=p) for p in filter_query.or_]
        return {
            "query": {
                "bool": {
                    "should": clauses,
                    "minimum_should_match": _MINIMUM_SHOULD_MATCH,
                },
            },
        }

    raise fastapi.HTTPException(
        status_code=http_status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail="ComponentFilterQuery must have 'and' or 'or'.",
    )


# ---------------------------------------------------------------------------
# Semantic field alias resolution
# ---------------------------------------------------------------------------


def _resolve_semantic_field(
    *,
    field: str,
) -> str:
    """Resolve a friendly alias to the full ES vector field name.

    Falls back to the original value if not found in the alias dict,
    allowing full ES field names to pass through unchanged.
    """
    return elastic_search_api._SEMANTIC_ALIAS_TO_FIELD.get(field, field)


# ---------------------------------------------------------------------------
# Full search query → ES request body
# ---------------------------------------------------------------------------


def component_search_to_es(
    *,
    query: component_models.ComponentSearchQuery,
    embed_fn: Callable[[str], list[float]],
) -> dict[str, Any]:
    """Build the complete ES request body from a ``ComponentSearchQuery``.

    The returned dict contains:
    - ``_source``: field selection (defaults to digest, name, published_by)
    - ``query``: bool filter from predicates (if any)
    - ``knn``: semantic / KNN search (if any)
    - ``size``: page size
    - ``sort``: two-entry array — primary sort by ``_score`` using the
      caller's ``sort_order``, and a tiebreaker on ``digest.keyword``
      (always ascending). The tiebreaker is required for deterministic
      ``search_after`` pagination; without it, documents sharing the same
      score could be skipped or duplicated across pages.
    - ``search_after``: cursor values (only when ``page_token`` is provided)
    """
    body: dict[str, Any] = {}

    # _source field selection — always include core fields so the response
    # builder can populate digest/name/published_by without KeyError.
    if query.fields is not None:
        core = set(component_models._DEFAULT_SOURCE_FIELDS)
        source_fields = sorted(core | set(query.fields))
    else:
        source_fields = component_models._DEFAULT_SOURCE_FIELDS
    body["_source"] = source_fields

    # Filter predicates
    if query.filter is not None:
        filter_body = component_filter_to_es(filter_query=query.filter)
        body.update(filter_body)

    # Semantic / KNN search
    if query.semantic:
        for sem in query.semantic:
            vector = embed_fn(sem.knn_query)
            body["knn"] = {
                "field": _resolve_semantic_field(field=sem.field),
                "query_vector": vector,
                "k": sem.knn_k,
            }

    # Size
    body["size"] = query.size

    # Sort: primary by _score, tiebreaker by digest.keyword (always asc)
    body["sort"] = [
        {"_score": {"order": query.sort_order.value}},
        {"digest.keyword": {"order": component_models.SortOrder.ASC.value}},
    ]

    # Pagination via search_after
    if query.page_token is not None:
        search_after = query.page_token.get("search_after")
        if search_after is not None:
            body["search_after"] = search_after

    return body


# ---------------------------------------------------------------------------
# Pagination token builder
# ---------------------------------------------------------------------------


def build_next_page_token(
    *,
    hits: list[dict[str, Any]],
    size: int,
) -> dict[str, Any] | None:
    """Build the ``next_page_token`` from ES response hits.

    Returns ``{"search_after": <last hit's sort values>}`` when there may be
    more pages.  Returns ``None`` when ``len(hits) < size`` (last page) or
    when ``hits`` is empty.
    """
    if not hits or len(hits) < size:
        return None
    last_sort = hits[-1].get("sort")
    if last_sort is None:
        return None
    return {"search_after": last_sort}
