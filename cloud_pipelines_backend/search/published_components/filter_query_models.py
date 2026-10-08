"""Component search Pydantic models.

Extends the OSS filter_query_models with new predicates (fuzzy, match, regex),
semantic search, pagination, and response models for the component search API.
"""

import enum
from typing import Any

import pydantic

from cloud_pipelines_backend import filter_query_models

# ---------------------------------------------------------------------------
# New leaf argument models
# ---------------------------------------------------------------------------


class ValueFuzzy(filter_query_models._BaseModel):
    key: filter_query_models.NonEmptyStr
    value: filter_query_models.NonEmptyStr


class ValueMatch(filter_query_models._BaseModel):
    key: filter_query_models.NonEmptyStr
    query: filter_query_models.NonEmptyStr


class ValueRegex(filter_query_models._BaseModel):
    key: filter_query_models.NonEmptyStr
    pattern: filter_query_models.NonEmptyStr


# ---------------------------------------------------------------------------
# New predicate wrappers (follow existing KeyPredicateBase pattern)
# ---------------------------------------------------------------------------


class ValueFuzzyPredicate(filter_query_models.KeyPredicateBase):
    value_fuzzy: ValueFuzzy

    @property
    def key(self) -> str:
        return self.value_fuzzy.key


class ValueMatchPredicate(filter_query_models.KeyPredicateBase):
    value_match: ValueMatch

    @property
    def key(self) -> str:
        return self.value_match.key


class ValueRegexPredicate(filter_query_models.KeyPredicateBase):
    value_regex: ValueRegex

    @property
    def key(self) -> str:
        return self.value_regex.key


# ---------------------------------------------------------------------------
# Case-insensitive predicate wrappers
# ---------------------------------------------------------------------------


class ValueEqualsCaseInsensitivePredicate(filter_query_models.KeyPredicateBase):
    value_equals_case_insensitive: filter_query_models.ValueEquals

    @property
    def key(self) -> str:
        return self.value_equals_case_insensitive.key


class ValueContainsCaseInsensitivePredicate(filter_query_models.KeyPredicateBase):
    value_contains_case_insensitive: filter_query_models.ValueContains

    @property
    def key(self) -> str:
        return self.value_contains_case_insensitive.key


class ValueInCaseInsensitivePredicate(filter_query_models.KeyPredicateBase):
    value_in_case_insensitive: filter_query_models.ValueIn

    @property
    def key(self) -> str:
        return self.value_in_case_insensitive.key


class ValueRegexCaseInsensitivePredicate(filter_query_models.KeyPredicateBase):
    value_regex_case_insensitive: ValueRegex

    @property
    def key(self) -> str:
        return self.value_regex_case_insensitive.key


# ---------------------------------------------------------------------------
# Extended leaf union (original leaves minus TimeRange, plus new ones)
# ---------------------------------------------------------------------------

ComponentLeafPredicate = (
    filter_query_models.KeyExistsPredicate
    | filter_query_models.ValueContainsPredicate
    | filter_query_models.ValueInPredicate
    | filter_query_models.ValueEqualsPredicate
    | ValueFuzzyPredicate
    | ValueMatchPredicate
    | ValueRegexPredicate
    | ValueEqualsCaseInsensitivePredicate
    | ValueContainsCaseInsensitivePredicate
    | ValueInCaseInsensitivePredicate
    | ValueRegexCaseInsensitivePredicate
)


# ---------------------------------------------------------------------------
# New recursive predicates referencing ComponentPredicate
# ---------------------------------------------------------------------------


class ComponentNotPredicate(filter_query_models._BaseModel):
    not_: ComponentLeafPredicate = pydantic.Field(alias="not")


class ComponentAndPredicate(filter_query_models._BaseModel):
    and_: list["ComponentPredicate"] = pydantic.Field(alias="and", min_length=1)


class ComponentOrPredicate(filter_query_models._BaseModel):
    or_: list["ComponentPredicate"] = pydantic.Field(alias="or", min_length=1)


ComponentPredicate = (
    ComponentLeafPredicate
    | ComponentNotPredicate
    | ComponentAndPredicate
    | ComponentOrPredicate
)

ComponentAndPredicate.model_rebuild()
ComponentOrPredicate.model_rebuild()


class ComponentFilterQuery(filter_query_models._BaseModel):
    """Root filter: must be exactly one of ``{"and": [...]}`` or ``{"or": [...]}``.``"""

    and_: list[ComponentPredicate] | None = pydantic.Field(
        None,
        alias="and",
        min_length=1,
    )
    or_: list[ComponentPredicate] | None = pydantic.Field(
        None,
        alias="or",
        min_length=1,
    )

    @pydantic.model_validator(mode="after")
    def _exactly_one_root_operator(self) -> "ComponentFilterQuery":
        has_and = self.and_ is not None
        has_or = self.or_ is not None
        if has_and == has_or:
            raise ValueError(
                "ComponentFilterQuery root must have exactly one of 'and' or 'or'."
            )
        return self


# ---------------------------------------------------------------------------
# Search query (filter + semantic)
# ---------------------------------------------------------------------------


class ComponentSemanticSearch(filter_query_models._BaseModel):
    field: filter_query_models.NonEmptyStr
    knn_query: filter_query_models.NonEmptyStr
    knn_k: int = pydantic.Field(default=20, ge=1, le=500)


class SortOrder(str, enum.Enum):
    ASC = "asc"
    DESC = "desc"


class ComponentSearchQuery(filter_query_models._BaseModel):
    filter: ComponentFilterQuery | None = None
    semantic: list[ComponentSemanticSearch] | None = None
    fields: list[str] | None = None
    size: int = pydantic.Field(default=20, ge=1, le=500)
    sort_order: SortOrder = SortOrder.DESC
    page_token: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

# Default ES _source fields when ``fields`` is None.
_DEFAULT_SOURCE_FIELDS = ["digest", "name", "published_by"]


class ComponentSearchResult(filter_query_models._BaseModel):
    # (digest, published_by) is the composite primary key for a published
    # component. Use both to uniquely identify a component.
    digest: str
    name: str
    published_by: str
    score: float | None = None
    extra_fields: dict[str, Any] | None = None


class ComponentSearchResponse(filter_query_models._BaseModel):
    results: list[ComponentSearchResult]
    total: int
    next_page_token: dict[str, Any] | None = None
