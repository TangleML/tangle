"""Tests for search.published_components.api_routes — request parsing and response models."""

import json
import os
from unittest import mock

import elasticsearch
import fastapi
import fastapi.testclient
import pytest
from starlette import status as http_status

from cloud_pipelines_backend.search.published_components import api_routes
from cloud_pipelines_backend.search.published_components import (
    filter_query_models as models,
)


class TestParseQueryFromBody:
    """Verify that parse_query_from_body handles valid JSON, invalid JSON,
    and Pydantic validation errors correctly."""

    def test_valid_filter_query(self) -> None:
        body = json.dumps(
            {
                "filter": {"and": [{"value_match": {"key": "name", "query": "train"}}]},
                "size": 10,
            }
        ).encode()

        result = api_routes.parse_query_from_body(raw_body=body)

        assert result.size == 10
        assert result.filter is not None

    def test_valid_minimal_query(self) -> None:
        """Empty body uses all defaults."""
        body = b"{}"

        result = api_routes.parse_query_from_body(raw_body=body)

        assert result.size == 20
        assert result.filter is None
        assert result.semantic is None
        assert result.sort_order.value == "desc"

    def test_valid_semantic_query(self) -> None:
        body = json.dumps(
            {
                "semantic": [{"field": "vec_field", "knn_query": "clean data"}],
            }
        ).encode()

        result = api_routes.parse_query_from_body(raw_body=body)

        assert result.semantic is not None
        assert len(result.semantic) == 1
        assert result.semantic[0].field == "vec_field"
        assert result.semantic[0].knn_query == "clean data"
        assert result.semantic[0].knn_k == 20

    def test_invalid_json_raises_422(self) -> None:
        body = b"not valid json {{"

        with pytest.raises(fastapi.HTTPException) as exc_info:
            api_routes.parse_query_from_body(raw_body=body)

        assert exc_info.value.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "Invalid JSON body" in exc_info.value.detail

    def test_empty_body_raises_422(self) -> None:
        body = b""

        with pytest.raises(fastapi.HTTPException) as exc_info:
            api_routes.parse_query_from_body(raw_body=body)

        assert exc_info.value.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "Invalid JSON body" in exc_info.value.detail

    def test_validation_error_size_too_large(self) -> None:
        body = json.dumps({"size": 9999}).encode()

        with pytest.raises(fastapi.HTTPException) as exc_info:
            api_routes.parse_query_from_body(raw_body=body)

        assert exc_info.value.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_validation_error_size_zero(self) -> None:
        body = json.dumps({"size": 0}).encode()

        with pytest.raises(fastapi.HTTPException) as exc_info:
            api_routes.parse_query_from_body(raw_body=body)

        assert exc_info.value.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_non_dict_body_raises_422(self) -> None:
        """A JSON array is valid JSON but not a valid query object."""
        body = b"[1, 2, 3]"

        with pytest.raises(fastapi.HTTPException) as exc_info:
            api_routes.parse_query_from_body(raw_body=body)

        assert exc_info.value.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_content_type_irrelevant(self) -> None:
        """The function only takes bytes — works regardless of how the
        caller received them (application/json or text/plain)."""
        json_str = '{"filter": {"and": [{"key_exists": {"key": "name"}}]}}'

        result = api_routes.parse_query_from_body(raw_body=json_str.encode("utf-8"))

        assert result.filter is not None

    def test_fields_and_sort_order(self) -> None:
        body = json.dumps(
            {
                "fields": ["digest", "name"],
                "sort_order": "asc",
                "size": 5,
            }
        ).encode()

        result = api_routes.parse_query_from_body(raw_body=body)

        assert result.fields == ["digest", "name"]
        assert result.sort_order.value == "asc"
        assert result.size == 5


class TestBuildResultsFromHits:
    """Verify build_results_from_hits maps ES hits to ComponentSearchResult."""

    _HIT_CORE_ONLY = {
        "_source": {
            "digest": "abc123",
            "name": "Train",
            "published_by": "user@example.com",
        },
        "_score": 3.5,
    }

    _HIT_WITH_EXTRAS = {
        "_source": {
            "digest": "abc123",
            "name": "Train",
            "published_by": "user@example.com",
            "name_and_description": "Train a model",
            "spec.description": "Trains an ML model on input data",
        },
        "_score": 2.0,
    }

    def test_without_extra_fields(self) -> None:
        results = api_routes.build_results_from_hits(
            hits=[self._HIT_CORE_ONLY],
            include_extra_fields=False,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 3.5,
                "extra_fields": None,
            },
        ]

    def test_with_extra_fields(self) -> None:
        results = api_routes.build_results_from_hits(
            hits=[self._HIT_WITH_EXTRAS],
            include_extra_fields=True,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 2.0,
                "extra_fields": {
                    "name_and_description": "Train a model",
                    "spec.description": "Trains an ML model on input data",
                },
            },
        ]

    def test_extra_fields_excluded_when_flag_false(self) -> None:
        """Even if _source has extra keys, they are ignored when flag is False."""
        results = api_routes.build_results_from_hits(
            hits=[self._HIT_WITH_EXTRAS],
            include_extra_fields=False,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 2.0,
                "extra_fields": None,
            },
        ]

    def test_no_extra_fields_when_source_only_has_core(self) -> None:
        """When include_extra_fields is True but _source has nothing extra."""
        results = api_routes.build_results_from_hits(
            hits=[self._HIT_CORE_ONLY],
            include_extra_fields=True,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 3.5,
                "extra_fields": None,
            },
        ]

    def test_multiple_hits(self) -> None:
        results = api_routes.build_results_from_hits(
            hits=[self._HIT_CORE_ONLY, self._HIT_WITH_EXTRAS],
            include_extra_fields=True,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 3.5,
                "extra_fields": None,
            },
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 2.0,
                "extra_fields": {
                    "name_and_description": "Train a model",
                    "spec.description": "Trains an ML model on input data",
                },
            },
        ]

    def test_empty_hits(self) -> None:
        results = api_routes.build_results_from_hits(
            hits=[],
            include_extra_fields=True,
        )

        assert [r.model_dump() for r in results] == []

    def test_missing_core_field_uses_default(self) -> None:
        """A hit missing a core field gets an empty-string default instead of KeyError."""
        hit = {
            "_source": {"digest": "abc123", "name": "Train"},
            "_score": 1.0,
        }
        results = api_routes.build_results_from_hits(
            hits=[hit],
            include_extra_fields=False,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "",
                "score": 1.0,
                "extra_fields": None,
            },
        ]

    def test_extra_fields_none_when_all_user_fields_empty(self) -> None:
        """If _source has only core fields, extra_fields is None."""
        hit = {
            "_source": {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
            },
            "_score": 1.5,
        }
        results = api_routes.build_results_from_hits(
            hits=[hit],
            include_extra_fields=True,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 1.5,
                "extra_fields": None,
            },
        ]

    def test_extra_fields_partial_user_fields(self) -> None:
        """Only user-requested fields that have values appear in extra_fields."""
        hit = {
            "_source": {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "spec.description": "Trains a model",
            },
            "_score": 2.0,
        }
        results = api_routes.build_results_from_hits(
            hits=[hit],
            include_extra_fields=True,
        )

        assert [r.model_dump() for r in results] == [
            {
                "digest": "abc123",
                "name": "Train",
                "published_by": "user@example.com",
                "score": 2.0,
                "extra_fields": {"spec.description": "Trains a model"},
            },
        ]


class TestDerefSchema:
    """Verify _deref_schema inlines $defs/$ref for OpenAPI 3.0 compatibility."""

    def test_inlines_simple_ref(self) -> None:
        schema = {
            "$defs": {
                "Foo": {
                    "type": "object",
                    "properties": {"x": {"type": "integer"}},
                },
            },
            "type": "object",
            "properties": {
                "bar": {"$ref": "#/$defs/Foo"},
            },
        }

        assert api_routes._deref_schema(schema=schema) == {
            "type": "object",
            "properties": {
                "bar": {
                    "type": "object",
                    "properties": {"x": {"type": "integer"}},
                },
            },
        }

    def test_inlines_nested_refs(self) -> None:
        schema = {
            "$defs": {
                "Inner": {"type": "string"},
                "Outer": {
                    "type": "object",
                    "properties": {"val": {"$ref": "#/$defs/Inner"}},
                },
            },
            "properties": {"top": {"$ref": "#/$defs/Outer"}},
        }

        assert api_routes._deref_schema(schema=schema) == {
            "properties": {
                "top": {
                    "type": "object",
                    "properties": {"val": {"type": "string"}},
                },
            },
        }

    def test_inlines_ref_in_array(self) -> None:
        schema = {
            "$defs": {
                "Item": {"type": "integer"},
            },
            "type": "array",
            "items": {"$ref": "#/$defs/Item"},
        }

        assert api_routes._deref_schema(schema=schema) == {
            "type": "array",
            "items": {"type": "integer"},
        }

    def test_no_defs_passthrough(self) -> None:
        schema = {"type": "object", "properties": {"a": {"type": "string"}}}

        assert api_routes._deref_schema(schema=schema) == {
            "type": "object",
            "properties": {"a": {"type": "string"}},
        }

    def test_unknown_ref_replaced_with_empty(self) -> None:
        """Refs not found in $defs are replaced with empty dict."""
        schema = {
            "$defs": {},
            "properties": {"x": {"$ref": "#/components/schemas/External"}},
        }

        assert api_routes._deref_schema(schema=schema) == {
            "properties": {"x": {}},
        }

    def test_component_search_query_has_no_refs(self) -> None:
        """The real model schema should be fully dereferenced with no $ref remaining."""
        result = api_routes._deref_schema(
            schema=models.ComponentSearchQuery.model_json_schema(),
        )

        result_str = json.dumps(result)
        assert "$ref" not in result_str
        assert "$defs" not in result_str


# ---------------------------------------------------------------------------
# Semantic fallback endpoint tests
# ---------------------------------------------------------------------------

_MOCK_ES_URL = "http://mock-es:9200"

_MOCK_MAPPING_RESPONSE = {
    "published_components": {
        "mappings": {
            "properties": {
                "name": {
                    "type": "text",
                    "meta": {"description": "Component name"},
                },
                "published_by": {
                    "type": "text",
                    "meta": {"description": "Publisher email"},
                },
                "digest": {
                    "type": "text",
                    "meta": {"description": "Content hash"},
                },
                "name_and_description_vector__embedding-model__3072": {
                    "type": "dense_vector",
                    "meta": {"description": "Semantic: name + desc"},
                },
            },
        },
    },
}

_MOCK_COUNT_RESPONSE = {"count": 100}

_MOCK_SEARCH_RESPONSE = {
    "hits": {
        "hits": [
            {
                "_source": {
                    "digest": "abc",
                    "name": "Test",
                    "published_by": "user@example.com",
                },
                "_score": 1.0,
            },
        ],
        "total": {"value": 1},
    },
}


def _build_test_client() -> fastapi.testclient.TestClient:
    """Create a FastAPI app with component search routes for testing."""
    test_app = fastapi.FastAPI()
    api_routes.setup_component_search_routes(
        app=test_app,
        es_client_factory=lambda: elasticsearch.Elasticsearch(
            hosts=[os.environ["ELASTICSEARCH_URL"]]
        ),
        embedding_function_getter=lambda: (
            (lambda text: [0.1] * 3072)
            if os.environ.get("TEST_EMBEDDING_TOKEN")
            else None
        ),
        semantic_unavailable_detail="Cannot perform semantic search: TEST_EMBEDDING_TOKEN environment variable not configured.",
        semantic_unavailable_notice="Semantic search is unavailable (TEST_EMBEDDING_TOKEN environment variable not configured).",
    )
    return fastapi.testclient.TestClient(test_app)


class TestSemanticFallback:
    """Verify graceful degradation when TEST_EMBEDDING_TOKEN is not set."""

    def test_text_search_works_without_ai_token(self) -> None:
        """Filter-only queries succeed even when TEST_EMBEDDING_TOKEN is absent."""
        mock_es = mock.MagicMock()
        mock_es.search.return_value = _MOCK_SEARCH_RESPONSE
        client = _build_test_client()

        with (
            mock.patch("elasticsearch.Elasticsearch", return_value=mock_es),
            mock.patch.dict(
                "os.environ",
                {"ELASTICSEARCH_URL": _MOCK_ES_URL},
                clear=False,
            ),
        ):
            import os

            os.environ.pop("TEST_EMBEDDING_TOKEN", None)

            response = client.post(
                "/api/published_components/experimental/search",
                content=json.dumps(
                    {
                        "filter": {
                            "and": [
                                {
                                    "value_match": {
                                        "key": "name",
                                        "query": "test",
                                    }
                                }
                            ]
                        },
                    }
                ),
                headers={"Content-Type": "application/json"},
            )

        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert data["total"] == 1

    def test_semantic_search_rejected_without_ai_token(self) -> None:
        """Semantic queries return 422 when TEST_EMBEDDING_TOKEN is absent."""
        client = _build_test_client()

        with mock.patch.dict(
            "os.environ",
            {"ELASTICSEARCH_URL": _MOCK_ES_URL},
            clear=False,
        ):
            import os

            os.environ.pop("TEST_EMBEDDING_TOKEN", None)

            response = client.post(
                "/api/published_components/experimental/search",
                content=json.dumps(
                    {
                        "semantic": [
                            {
                                "field": "name_and_description",
                                "knn_query": "clean data",
                            }
                        ],
                    }
                ),
                headers={"Content-Type": "application/json"},
            )

        assert response.status_code == http_status.HTTP_422_UNPROCESSABLE_CONTENT
        detail = response.json()["detail"]
        assert detail == (
            "Cannot perform semantic search:"
            " TEST_EMBEDDING_TOKEN environment variable not configured."
        )

    def test_schema_omits_semantic_without_ai_token(self) -> None:
        """Schema excludes semantic fields and includes notices when token missing."""
        mock_es = mock.MagicMock()
        mock_es.indices.get_mapping.return_value = _MOCK_MAPPING_RESPONSE
        mock_es.count.return_value = _MOCK_COUNT_RESPONSE

        client = _build_test_client()

        with (
            mock.patch("elasticsearch.Elasticsearch", return_value=mock_es),
            mock.patch.dict(
                "os.environ",
                {"ELASTICSEARCH_URL": _MOCK_ES_URL},
                clear=False,
            ),
        ):
            import os

            os.environ.pop("TEST_EMBEDDING_TOKEN", None)

            response = client.get(
                "/api/published_components/experimental/search/schema",
            )

        assert response.status_code == 200
        data = response.json()
        field_types = [f["type"] for f in data["fields"]]
        assert "semantic" not in field_types
        assert data["notices"] == [
            "Semantic search is unavailable"
            " (TEST_EMBEDDING_TOKEN environment variable not configured)."
        ]

    def test_schema_includes_semantic_with_ai_token(self) -> None:
        """Schema includes semantic fields and no notices when token is set."""
        mock_es = mock.MagicMock()
        mock_es.indices.get_mapping.return_value = _MOCK_MAPPING_RESPONSE
        mock_es.count.return_value = _MOCK_COUNT_RESPONSE

        client = _build_test_client()

        with (
            mock.patch("elasticsearch.Elasticsearch", return_value=mock_es),
            mock.patch.dict(
                "os.environ",
                {
                    "ELASTICSEARCH_URL": _MOCK_ES_URL,
                    "TEST_EMBEDDING_TOKEN": "test-token",
                },
                clear=False,
            ),
        ):
            response = client.get(
                "/api/published_components/experimental/search/schema",
            )

        assert response.status_code == 200
        data = response.json()
        field_types = [f["type"] for f in data["fields"]]
        assert "semantic" in field_types
        assert data["notices"] is None
