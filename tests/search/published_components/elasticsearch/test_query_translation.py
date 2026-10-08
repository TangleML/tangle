"""Snapshot tests for search/published_components/elasticsearch/query_translation.py.

Each test constructs a ``ComponentSearchQuery``, runs it through the
translator, and asserts the exact ES DSL dict output.
"""

import pytest

from cloud_pipelines_backend.search.published_components import (
    filter_query_models as models,
)
from cloud_pipelines_backend.search.published_components.elasticsearch import (
    query_translation,
)

_MOCK_VECTOR = [0.0] * 10


def _mock_embed_fn(text: str) -> list[float]:
    return _MOCK_VECTOR


# ---------------------------------------------------------------------------
# Semantic field alias resolution tests
# ---------------------------------------------------------------------------


class TestResolveSemanticField:
    def test_alias_resolves_to_full_field(self) -> None:
        result = query_translation._resolve_semantic_field(field="full_spec")
        assert result == "full_spec_vector__embedding-model__3072"

    def test_name_and_description_alias(self) -> None:
        result = query_translation._resolve_semantic_field(field="name_and_description")
        assert result == "name_and_description_vector__embedding-model__3072"

    def test_full_es_field_name_passes_through(self) -> None:
        full_name = "full_spec_vector__embedding-model__3072"
        result = query_translation._resolve_semantic_field(field=full_name)
        assert result == full_name

    def test_unknown_field_passes_through(self) -> None:
        result = query_translation._resolve_semantic_field(field="some_other_vector")
        assert result == "some_other_vector"


# ---------------------------------------------------------------------------
# Individual predicate tests
# ---------------------------------------------------------------------------


class TestPredicateToEsQuery:
    def test_value_fuzzy(self) -> None:
        pred = models.ValueFuzzyPredicate.model_validate(
            {"value_fuzzy": {"key": "name", "value": "filtr"}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "fuzzy": {"name": {"value": "filtr", "fuzziness": "AUTO"}},
        }

    def test_value_match(self) -> None:
        pred = models.ValueMatchPredicate.model_validate(
            {"value_match": {"key": "name", "query": "train model"}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {"match": {"name": "train model"}}

    def test_value_regex(self) -> None:
        pred = models.ValueRegexPredicate.model_validate(
            {"value_regex": {"key": "name", "pattern": "Filter.*v[0-9]+"}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {"regexp": {"name.keyword": "Filter.*v[0-9]+"}}

    def test_value_equals(self) -> None:
        from cloud_pipelines_backend import filter_query_models as base

        pred = base.ValueEqualsPredicate.model_validate(
            {"value_equals": {"key": "published_by", "value": "alice"}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {"term": {"published_by.keyword": "alice"}}

    def test_value_contains(self) -> None:
        from cloud_pipelines_backend import filter_query_models as base

        pred = base.ValueContainsPredicate.model_validate(
            {"value_contains": {"key": "name", "value_substring": "filter"}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {"wildcard": {"name.keyword": "*filter*"}}

    def test_value_in(self) -> None:
        from cloud_pipelines_backend import filter_query_models as base

        pred = base.ValueInPredicate.model_validate(
            {"value_in": {"key": "published_by", "values": ["alice", "bob"]}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {"terms": {"published_by.keyword": ["alice", "bob"]}}

    def test_key_exists(self) -> None:
        from cloud_pipelines_backend import filter_query_models as base

        pred = base.KeyExistsPredicate.model_validate(
            {"key_exists": {"key": "spec.description"}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {"exists": {"field": "spec.description"}}

    def test_value_equals_case_insensitive(self) -> None:
        pred = models.ValueEqualsCaseInsensitivePredicate.model_validate(
            {
                "value_equals_case_insensitive": {
                    "key": "published_by",
                    "value": "alice",
                }
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "term": {
                "published_by.keyword": {
                    "value": "alice",
                    "case_insensitive": True,
                },
            },
        }

    def test_value_contains_case_insensitive(self) -> None:
        pred = models.ValueContainsCaseInsensitivePredicate.model_validate(
            {
                "value_contains_case_insensitive": {
                    "key": "name",
                    "value_substring": "filter",
                }
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "wildcard": {
                "name.keyword": {
                    "value": "*filter*",
                    "case_insensitive": True,
                },
            },
        }

    def test_value_in_case_insensitive(self) -> None:
        pred = models.ValueInCaseInsensitivePredicate.model_validate(
            {
                "value_in_case_insensitive": {
                    "key": "published_by",
                    "values": ["alice", "bob"],
                }
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "bool": {
                "should": [
                    {
                        "term": {
                            "published_by.keyword": {
                                "value": "alice",
                                "case_insensitive": True,
                            },
                        },
                    },
                    {
                        "term": {
                            "published_by.keyword": {
                                "value": "bob",
                                "case_insensitive": True,
                            },
                        },
                    },
                ],
                "minimum_should_match": 1,
            },
        }

    def test_value_regex_case_insensitive(self) -> None:
        pred = models.ValueRegexCaseInsensitivePredicate.model_validate(
            {
                "value_regex_case_insensitive": {
                    "key": "name",
                    "pattern": "Filter.*v[0-9]+",
                }
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "regexp": {
                "name.keyword": {
                    "value": "Filter.*v[0-9]+",
                    "case_insensitive": True,
                },
            },
        }

    def test_unknown_predicate_raises_not_implemented(self) -> None:
        """An unrecognised predicate type raises NotImplementedError."""

        class FakePredicate:
            pass

        with pytest.raises(NotImplementedError, match="FakePredicate"):
            query_translation.predicate_to_es_query(predicate=FakePredicate())


# ---------------------------------------------------------------------------
# Boolean (nested) predicate tests
# ---------------------------------------------------------------------------


class TestBooleanPredicates:
    def test_not_predicate(self) -> None:
        pred = models.ComponentNotPredicate.model_validate(
            {"not": {"value_fuzzy": {"key": "name", "value": "test"}}}
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "bool": {
                "must_not": [
                    {"fuzzy": {"name": {"value": "test", "fuzziness": "AUTO"}}},
                ],
            },
        }

    def test_and_predicate(self) -> None:
        pred = models.ComponentAndPredicate.model_validate(
            {
                "and": [
                    {"value_match": {"key": "name", "query": "train"}},
                    {"value_equals": {"key": "published_by", "value": "alice"}},
                ]
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "bool": {
                "must": [
                    {"match": {"name": "train"}},
                    {"term": {"published_by.keyword": "alice"}},
                ],
            },
        }

    def test_or_predicate(self) -> None:
        pred = models.ComponentOrPredicate.model_validate(
            {
                "or": [
                    {"value_regex": {"key": "name", "pattern": "filter.*"}},
                    {"key_exists": {"key": "spec.description"}},
                ]
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "bool": {
                "should": [
                    {"regexp": {"name.keyword": "filter.*"}},
                    {"exists": {"field": "spec.description"}},
                ],
                "minimum_should_match": 1,
            },
        }

    def test_nested_and_or_not_with_case_insensitive(self) -> None:
        pred = models.ComponentAndPredicate.model_validate(
            {
                "and": [
                    {
                        "and": [
                            {"value_match": {"key": "name", "query": "train"}},
                            {
                                "value_in_case_insensitive": {
                                    "key": "published_by",
                                    "values": ["alice", "bob"],
                                }
                            },
                        ]
                    },
                    {
                        "or": [
                            {"value_fuzzy": {"key": "name", "value": "filtr"}},
                            {
                                "value_in_case_insensitive": {
                                    "key": "published_by",
                                    "values": ["carol", "dave"],
                                }
                            },
                        ]
                    },
                    {
                        "not": {
                            "value_equals": {
                                "key": "published_by",
                                "value": "bot@example.com",
                            }
                        }
                    },
                ]
            }
        )
        result = query_translation.predicate_to_es_query(predicate=pred)
        assert result == {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "must": [
                                {"match": {"name": "train"}},
                                {
                                    "bool": {
                                        "should": [
                                            {
                                                "term": {
                                                    "published_by.keyword": {
                                                        "value": "alice",
                                                        "case_insensitive": True,
                                                    },
                                                },
                                            },
                                            {
                                                "term": {
                                                    "published_by.keyword": {
                                                        "value": "bob",
                                                        "case_insensitive": True,
                                                    },
                                                },
                                            },
                                        ],
                                        "minimum_should_match": 1,
                                    },
                                },
                            ],
                        },
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "fuzzy": {
                                        "name": {
                                            "value": "filtr",
                                            "fuzziness": "AUTO",
                                        },
                                    },
                                },
                                {
                                    "bool": {
                                        "should": [
                                            {
                                                "term": {
                                                    "published_by.keyword": {
                                                        "value": "carol",
                                                        "case_insensitive": True,
                                                    },
                                                },
                                            },
                                            {
                                                "term": {
                                                    "published_by.keyword": {
                                                        "value": "dave",
                                                        "case_insensitive": True,
                                                    },
                                                },
                                            },
                                        ],
                                        "minimum_should_match": 1,
                                    },
                                },
                            ],
                            "minimum_should_match": 1,
                        },
                    },
                    {
                        "bool": {
                            "must_not": [
                                {
                                    "term": {
                                        "published_by.keyword": "bot@example.com",
                                    },
                                },
                            ],
                        },
                    },
                ],
            },
        }


# ---------------------------------------------------------------------------
# Full search query tests
# ---------------------------------------------------------------------------


class TestComponentSearchToEs:
    def test_filter_only(self) -> None:
        query = models.ComponentSearchQuery(
            filter=models.ComponentFilterQuery.model_validate(
                {"and": [{"value_fuzzy": {"key": "name", "value": "filtr"}}]}
            ),
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "query": {
                "bool": {
                    "must": [
                        {"fuzzy": {"name": {"value": "filtr", "fuzziness": "AUTO"}}},
                    ],
                },
            },
            "size": 20,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }

    def test_semantic_only(self) -> None:
        query = models.ComponentSearchQuery(
            semantic=[
                models.ComponentSemanticSearch(
                    field="name_and_desc_vector",
                    knn_query="clean data",
                ),
            ],
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "knn": {
                "field": "name_and_desc_vector",
                "query_vector": _MOCK_VECTOR,
                "k": 20,
            },
            "size": 20,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }

    def test_semantic_with_alias(self) -> None:
        """Alias 'full_spec' resolves to the full ES vector field name."""
        query = models.ComponentSearchQuery(
            semantic=[
                models.ComponentSemanticSearch(
                    field="full_spec",
                    knn_query="clean data",
                ),
            ],
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result["knn"]["field"] == "full_spec_vector__embedding-model__3072"

    def test_hybrid_filter_and_semantic(self) -> None:
        query = models.ComponentSearchQuery(
            filter=models.ComponentFilterQuery.model_validate(
                {"and": [{"value_match": {"key": "name", "query": "train model"}}]}
            ),
            semantic=[
                models.ComponentSemanticSearch(
                    field="name_and_desc_vector",
                    knn_query="clean data",
                    knn_k=100,
                ),
            ],
            size=50,
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": "train model"}},
                    ],
                },
            },
            "knn": {
                "field": "name_and_desc_vector",
                "query_vector": _MOCK_VECTOR,
                "k": 100,
            },
            "size": 50,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }

    def test_custom_fields(self) -> None:
        query = models.ComponentSearchQuery(
            fields=["digest", "name", "published_by", "spec"],
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by", "spec"],
            "size": 20,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }

    def test_custom_fields_merges_core_fields(self) -> None:
        """Core fields are always included in _source even if user omits them."""
        query = models.ComponentSearchQuery(
            fields=["spec.description"],
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by", "spec.description"],
            "size": 20,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }


# ---------------------------------------------------------------------------
# Pagination tests
# ---------------------------------------------------------------------------


class TestPagination:
    def test_no_page_token_omits_search_after(self) -> None:
        query = models.ComponentSearchQuery(
            filter=models.ComponentFilterQuery.model_validate(
                {"and": [{"value_match": {"key": "name", "query": "filter"}}]}
            ),
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": "filter"}},
                    ],
                },
            },
            "size": 20,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }

    def test_page_token_adds_search_after(self) -> None:
        query = models.ComponentSearchQuery(
            filter=models.ComponentFilterQuery.model_validate(
                {"and": [{"value_match": {"key": "name", "query": "filter"}}]}
            ),
            page_token={"search_after": [0.72, "def456"]},
        )
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": "filter"}},
                    ],
                },
            },
            "size": 20,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
            "search_after": [0.72, "def456"],
        }

    def test_sort_order_asc(self) -> None:
        query = models.ComponentSearchQuery(sort_order=models.SortOrder.ASC)
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "size": 20,
            "sort": [
                {"_score": {"order": "asc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }

    def test_custom_size(self) -> None:
        query = models.ComponentSearchQuery(size=100)
        result = query_translation.component_search_to_es(
            query=query,
            embed_fn=_mock_embed_fn,
        )
        assert result == {
            "_source": ["digest", "name", "published_by"],
            "size": 100,
            "sort": [
                {"_score": {"order": "desc"}},
                {"digest.keyword": {"order": "asc"}},
            ],
        }


# ---------------------------------------------------------------------------
# build_next_page_token tests
# ---------------------------------------------------------------------------


class TestBuildNextPageToken:
    def test_returns_token_from_last_hit(self) -> None:
        hits = [
            {"_source": {"name": "A"}, "sort": [0.95, "abc"]},
            {"_source": {"name": "B"}, "sort": [0.72, "def"]},
        ]
        token = query_translation.build_next_page_token(hits=hits, size=2)
        assert token == {"search_after": [0.72, "def"]}

    def test_returns_none_when_less_than_size(self) -> None:
        hits = [
            {"_source": {"name": "A"}, "sort": [0.95, "abc"]},
        ]
        token = query_translation.build_next_page_token(hits=hits, size=5)
        assert token is None

    def test_returns_none_when_empty(self) -> None:
        token = query_translation.build_next_page_token(hits=[], size=20)
        assert token is None

    def test_returns_none_when_no_sort(self) -> None:
        hits = [{"_source": {"name": "A"}}]
        token = query_translation.build_next_page_token(hits=hits, size=1)
        assert token is None
