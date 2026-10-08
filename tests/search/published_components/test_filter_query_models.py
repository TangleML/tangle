"""Tests for search/published_components/filter_query_models.py."""

import pydantic
import pytest

from cloud_pipelines_backend.search.published_components import (
    filter_query_models as models,
)

# ---------------------------------------------------------------------------
# New predicate validation
# ---------------------------------------------------------------------------


class TestValueFuzzyPredicate:
    def test_valid(self) -> None:
        pred = models.ValueFuzzyPredicate.model_validate(
            {"value_fuzzy": {"key": "name", "value": "filtr"}}
        )
        assert pred.key == "name"

    def test_empty_value_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueFuzzyPredicate.model_validate(
                {"value_fuzzy": {"key": "name", "value": ""}}
            )

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueFuzzyPredicate.model_validate(
                {"value_fuzzy": {"key": "", "value": "test"}}
            )

    def test_no_fuzziness_field(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueFuzzyPredicate.model_validate(
                {
                    "value_fuzzy": {
                        "key": "name",
                        "value": "filtr",
                        "fuzziness": "AUTO",
                    }
                }
            )


class TestValueMatchPredicate:
    def test_valid(self) -> None:
        pred = models.ValueMatchPredicate.model_validate(
            {"value_match": {"key": "name", "query": "train model"}}
        )
        assert pred.key == "name"

    def test_empty_query_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueMatchPredicate.model_validate(
                {"value_match": {"key": "name", "query": ""}}
            )

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueMatchPredicate.model_validate(
                {"value_match": {"key": "", "query": "train model"}}
            )


class TestValueRegexPredicate:
    def test_valid(self) -> None:
        pred = models.ValueRegexPredicate.model_validate(
            {"value_regex": {"key": "name", "pattern": "Filter.*v[0-9]+"}}
        )
        assert pred.key == "name"

    def test_empty_pattern_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueRegexPredicate.model_validate(
                {"value_regex": {"key": "name", "pattern": ""}}
            )

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueRegexPredicate.model_validate(
                {"value_regex": {"key": "", "pattern": "Filter.*"}}
            )


# ---------------------------------------------------------------------------
# Case-insensitive predicate validation
# ---------------------------------------------------------------------------


class TestValueEqualsCaseInsensitivePredicate:
    def test_valid(self) -> None:
        pred = models.ValueEqualsCaseInsensitivePredicate.model_validate(
            {
                "value_equals_case_insensitive": {
                    "key": "published_by",
                    "value": "alice",
                }
            }
        )
        assert pred.key == "published_by"

    def test_empty_value_accepted(self) -> None:
        pred = models.ValueEqualsCaseInsensitivePredicate.model_validate(
            {"value_equals_case_insensitive": {"key": "name", "value": ""}}
        )
        assert pred.value_equals_case_insensitive.value == ""

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueEqualsCaseInsensitivePredicate.model_validate(
                {"value_equals_case_insensitive": {"key": "", "value": "alice"}}
            )


class TestValueContainsCaseInsensitivePredicate:
    def test_valid(self) -> None:
        pred = models.ValueContainsCaseInsensitivePredicate.model_validate(
            {
                "value_contains_case_insensitive": {
                    "key": "name",
                    "value_substring": "filter",
                }
            }
        )
        assert pred.key == "name"

    def test_empty_substring_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueContainsCaseInsensitivePredicate.model_validate(
                {
                    "value_contains_case_insensitive": {
                        "key": "name",
                        "value_substring": "",
                    }
                }
            )

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueContainsCaseInsensitivePredicate.model_validate(
                {
                    "value_contains_case_insensitive": {
                        "key": "",
                        "value_substring": "filter",
                    }
                }
            )


class TestValueInCaseInsensitivePredicate:
    def test_valid(self) -> None:
        pred = models.ValueInCaseInsensitivePredicate.model_validate(
            {
                "value_in_case_insensitive": {
                    "key": "published_by",
                    "values": ["alice", "bob"],
                }
            }
        )
        assert pred.key == "published_by"

    def test_empty_values_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueInCaseInsensitivePredicate.model_validate(
                {
                    "value_in_case_insensitive": {
                        "key": "published_by",
                        "values": [],
                    }
                }
            )

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueInCaseInsensitivePredicate.model_validate(
                {"value_in_case_insensitive": {"key": "", "values": ["alice"]}}
            )


class TestValueRegexCaseInsensitivePredicate:
    def test_valid(self) -> None:
        pred = models.ValueRegexCaseInsensitivePredicate.model_validate(
            {
                "value_regex_case_insensitive": {
                    "key": "name",
                    "pattern": "Filter.*",
                }
            }
        )
        assert pred.key == "name"

    def test_empty_pattern_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueRegexCaseInsensitivePredicate.model_validate(
                {"value_regex_case_insensitive": {"key": "name", "pattern": ""}}
            )

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ValueRegexCaseInsensitivePredicate.model_validate(
                {
                    "value_regex_case_insensitive": {
                        "key": "",
                        "pattern": "Filter.*",
                    }
                }
            )


# ---------------------------------------------------------------------------
# Combined predicates (new + existing in same query)
# ---------------------------------------------------------------------------


class TestComponentFilterQuery:
    def test_and_with_mixed_predicates(self) -> None:
        fq = models.ComponentFilterQuery.model_validate(
            {
                "and": [
                    {"value_match": {"key": "name", "query": "train"}},
                    {"value_equals": {"key": "published_by", "value": "alice"}},
                    {"value_fuzzy": {"key": "name", "value": "filtr"}},
                    {
                        "value_contains": {
                            "key": "name",
                            "value_substring": "xgb",
                        }
                    },
                ]
            }
        )
        assert fq.and_ is not None
        assert len(fq.and_) == 4
        assert isinstance(fq.and_[0], models.ValueMatchPredicate)
        assert isinstance(fq.and_[1], models.filter_query_models.ValueEqualsPredicate)
        assert isinstance(fq.and_[2], models.ValueFuzzyPredicate)
        assert isinstance(fq.and_[3], models.filter_query_models.ValueContainsPredicate)

    def test_or_with_remaining_predicates(self) -> None:
        fq = models.ComponentFilterQuery.model_validate(
            {
                "or": [
                    {"key_exists": {"key": "spec.description"}},
                    {"value_in": {"key": "published_by", "values": ["a", "b"]}},
                    {"value_regex": {"key": "name", "pattern": "v[0-9]+"}},
                ]
            }
        )
        assert fq.or_ is not None
        assert fq.and_ is None
        assert len(fq.or_) == 3
        assert isinstance(fq.or_[0], models.filter_query_models.KeyExistsPredicate)
        assert isinstance(fq.or_[1], models.filter_query_models.ValueInPredicate)
        assert isinstance(fq.or_[2], models.ValueRegexPredicate)

    def test_must_have_exactly_one_root(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentFilterQuery.model_validate({})

    def test_cannot_have_both_and_or(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentFilterQuery.model_validate(
                {
                    "and": [{"key_exists": {"key": "name"}}],
                    "or": [{"key_exists": {"key": "name"}}],
                }
            )

    def test_empty_and_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentFilterQuery.model_validate({"and": []})

    def test_empty_or_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentFilterQuery.model_validate({"or": []})

    def test_nested_not(self) -> None:
        fq = models.ComponentFilterQuery.model_validate(
            {
                "and": [
                    {"not": {"value_fuzzy": {"key": "name", "value": "test"}}},
                    {"value_match": {"key": "name", "query": "train"}},
                ]
            }
        )
        assert fq.and_ is not None
        assert len(fq.and_) == 2
        assert isinstance(fq.and_[0], models.ComponentNotPredicate)
        assert isinstance(fq.and_[1], models.ValueMatchPredicate)

    def test_deeply_nested_and_or_not(self) -> None:
        """Exercises 2 levels of nesting: and → and/or/not."""
        fq = models.ComponentFilterQuery.model_validate(
            {
                "and": [
                    {"and": [{"value_match": {"key": "name", "query": "train"}}]},
                    {"or": [{"value_fuzzy": {"key": "name", "value": "filtr"}}]},
                    {
                        "not": {
                            "value_equals": {
                                "key": "published_by",
                                "value": "bot",
                            }
                        }
                    },
                ]
            }
        )
        assert fq.and_ is not None
        assert len(fq.and_) == 3

        assert isinstance(fq.and_[0], models.ComponentAndPredicate)
        assert isinstance(fq.and_[1], models.ComponentOrPredicate)
        assert isinstance(fq.and_[2], models.ComponentNotPredicate)

    def test_mixed_case_sensitive_and_insensitive(self) -> None:
        fq = models.ComponentFilterQuery.model_validate(
            {
                "and": [
                    {"value_equals": {"key": "published_by", "value": "alice"}},
                    {
                        "value_equals_case_insensitive": {
                            "key": "published_by",
                            "value": "bob",
                        }
                    },
                    {
                        "value_contains_case_insensitive": {
                            "key": "name",
                            "value_substring": "train",
                        }
                    },
                    {
                        "value_in_case_insensitive": {
                            "key": "published_by",
                            "values": ["x", "y"],
                        }
                    },
                    {
                        "value_regex_case_insensitive": {
                            "key": "name",
                            "pattern": "Filter.*",
                        }
                    },
                ]
            }
        )
        assert fq.and_ is not None
        assert len(fq.and_) == 5
        assert isinstance(fq.and_[0], models.filter_query_models.ValueEqualsPredicate)
        assert isinstance(fq.and_[1], models.ValueEqualsCaseInsensitivePredicate)
        assert isinstance(fq.and_[2], models.ValueContainsCaseInsensitivePredicate)
        assert isinstance(fq.and_[3], models.ValueInCaseInsensitivePredicate)
        assert isinstance(fq.and_[4], models.ValueRegexCaseInsensitivePredicate)


# ---------------------------------------------------------------------------
# ComponentSemanticSearch validation
# ---------------------------------------------------------------------------


class TestComponentSemanticSearch:
    def test_default_k(self) -> None:
        sem = models.ComponentSemanticSearch(field="vec_field", knn_query="test")
        assert sem.knn_k == 20

    def test_custom_k(self) -> None:
        sem = models.ComponentSemanticSearch(
            field="vec_field", knn_query="test", knn_k=100
        )
        assert sem.knn_k == 100

    def test_k_zero_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentSemanticSearch(field="vec_field", knn_query="test", knn_k=0)

    def test_k_over_max_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentSemanticSearch(
                field="vec_field", knn_query="test", knn_k=501
            )


# ---------------------------------------------------------------------------
# ComponentSearchQuery validation
# ---------------------------------------------------------------------------


class TestComponentSearchQuery:
    def test_defaults(self) -> None:
        q = models.ComponentSearchQuery()
        assert q.size == 20
        assert q.sort_order == models.SortOrder.DESC
        assert q.page_token is None
        assert q.filter is None
        assert q.semantic is None
        assert q.fields is None

    def test_size_min_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentSearchQuery(size=0)

    def test_size_negative_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentSearchQuery(size=-1)

    def test_size_over_max_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentSearchQuery(size=501)

    def test_size_boundaries_accepted(self) -> None:
        q1 = models.ComponentSearchQuery(size=1)
        assert q1.size == 1
        q500 = models.ComponentSearchQuery(size=500)
        assert q500.size == 500

    def test_page_token_none(self) -> None:
        q = models.ComponentSearchQuery(page_token=None)
        assert q.page_token is None

    def test_page_token_valid_dict(self) -> None:
        token = {"search_after": [0.72, "def456"]}
        q = models.ComponentSearchQuery(page_token=token)
        assert q.page_token == token

    def test_sort_order_asc(self) -> None:
        q = models.ComponentSearchQuery(sort_order="asc")
        assert q.sort_order == models.SortOrder.ASC

    def test_sort_order_desc(self) -> None:
        q = models.ComponentSearchQuery(sort_order="desc")
        assert q.sort_order == models.SortOrder.DESC

    def test_sort_order_invalid_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            models.ComponentSearchQuery(sort_order="invalid")
