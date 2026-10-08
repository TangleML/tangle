"""Unit tests for embedding functions in elastic_search_api.py."""

import datetime
import json
from unittest import mock

import elasticsearch
import fastapi
import pytest
from fastapi.testclient import TestClient

from cloud_pipelines_backend.search.elasticsearch import elastic_search_api

_MOCK_VECTOR = [0.1] * 3072


def _mock_es_client():
    """Return a MagicMock that behaves like elasticsearch.Elasticsearch."""
    return mock.MagicMock(spec=elasticsearch.Elasticsearch)


# ---------------------------------------------------------------------------
# _build_name_and_description_text
# ---------------------------------------------------------------------------


class TestBuildNameAndDescriptionText:
    def test_name_and_description(self) -> None:
        assert (
            elastic_search_api._build_name_and_description_text(
                component_spec_dict={
                    "name": "Train",
                    "description": "Trains a model",
                },
            )
            == "Train\n\nTrains a model"
        )

    def test_name_only(self) -> None:
        assert (
            elastic_search_api._build_name_and_description_text(
                component_spec_dict={"name": "Download"},
            )
            == "Download"
        )

    def test_description_only(self) -> None:
        assert (
            elastic_search_api._build_name_and_description_text(
                component_spec_dict={"description": "Downloads a file"},
            )
            == "Downloads a file"
        )

    def test_neither(self) -> None:
        assert (
            elastic_search_api._build_name_and_description_text(
                component_spec_dict={"inputs": [{"name": "data"}]},
            )
            == ""
        )


# ---------------------------------------------------------------------------
# _field_mapping_with_description
# ---------------------------------------------------------------------------


class TestFieldMappingWithDescription:
    def test_returns_text_mapping_with_keyword_subfield(self) -> None:
        result = elastic_search_api._field_mapping_with_description(
            field_type="text",
            description="Component name",
        )
        assert result == {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            "meta": {"description": "Component name"},
        }


# ---------------------------------------------------------------------------
# _parse_text_conflict_field
# ---------------------------------------------------------------------------


class TestParseTextConflictField:
    def test_extracts_field_path_for_text_type(self) -> None:
        error = elasticsearch.BadRequestError(
            message="document_parsing_exception",
            meta=mock.MagicMock(status=400),
            body={
                "error": {
                    "reason": "[1:321] failed to parse field [spec.inputs.type] of type [text] "
                    "in document with id 'abc123'"
                }
            },
        )
        result = elastic_search_api._parse_text_conflict_field(error=error)
        assert result == "spec.inputs.type"

    def test_returns_none_for_non_text_type(self) -> None:
        error = elasticsearch.BadRequestError(
            message="document_parsing_exception",
            meta=mock.MagicMock(status=400),
            body={
                "error": {
                    "reason": "failed to parse field [spec.count] of type [integer]"
                }
            },
        )
        result = elastic_search_api._parse_text_conflict_field(error=error)
        assert result is None

    def test_returns_none_for_unrelated_error(self) -> None:
        error = elasticsearch.BadRequestError(
            message="search_phase_execution_exception",
            meta=mock.MagicMock(status=400),
            body={},
        )
        result = elastic_search_api._parse_text_conflict_field(error=error)
        assert result is None


# ---------------------------------------------------------------------------
# _retry_with_sanitized_spec
# ---------------------------------------------------------------------------


class TestRetryWithSanitizedSpec:
    def _make_text_conflict_error(
        self, *, field_path: str
    ) -> elasticsearch.BadRequestError:
        return elasticsearch.BadRequestError(
            message="document_parsing_exception",
            meta=mock.MagicMock(status=400),
            body={
                "error": {
                    "reason": f"failed to parse field [{field_path}] of type [text]"
                }
            },
        )

    def test_successful_retry(self) -> None:
        client = _mock_es_client()
        text_field_paths: set[str] = set()
        document: dict[str, object] = {
            "spec": {"inputs": [{"type": {"JsonObject": {"data_type": "proto:Input"}}}]}
        }
        error = self._make_text_conflict_error(field_path="spec.inputs.type")

        result = elastic_search_api._retry_with_sanitized_spec(
            client=client,
            index_name="test_index",
            document_id="doc1",
            document=document,
            text_field_paths=text_field_paths,
            error=error,
        )

        assert result is True
        assert text_field_paths == {"spec.inputs.type"}
        assert document == {
            "spec": {
                "inputs": [
                    {
                        "type": json.dumps(
                            {"JsonObject": {"data_type": "proto:Input"}},
                            sort_keys=True,
                        )
                    }
                ]
            }
        }
        client.update.assert_called_once_with(
            index="test_index",
            id="doc1",
            doc=document,
            doc_as_upsert=True,
        )

    def test_adds_path_for_future_documents(self) -> None:
        """The conflicting path is added to text_field_paths so future docs are pre-sanitized."""
        client = _mock_es_client()
        text_field_paths: set[str] = set()
        document: dict[str, object] = {
            "spec": {"inputs": [{"type": {"nested": "obj"}}]}
        }
        error = self._make_text_conflict_error(field_path="spec.inputs.type")

        elastic_search_api._retry_with_sanitized_spec(
            client=client,
            index_name="idx",
            document_id="d1",
            document=document,
            text_field_paths=text_field_paths,
            error=error,
        )

        assert text_field_paths == {"spec.inputs.type"}
        assert document == {
            "spec": {
                "inputs": [{"type": json.dumps({"nested": "obj"}, sort_keys=True)}]
            }
        }

    def test_returns_false_for_non_text_conflict(self) -> None:
        client = _mock_es_client()
        document: dict[str, object] = {"spec": {}}
        error = elasticsearch.BadRequestError(
            message="some_other_error",
            meta=mock.MagicMock(status=400),
            body={"error": {"reason": "something else entirely"}},
        )

        result = elastic_search_api._retry_with_sanitized_spec(
            client=client,
            index_name="idx",
            document_id="d1",
            document=document,
            text_field_paths=set(),
            error=error,
        )

        assert result is False
        assert document == {"spec": {}}
        client.update.assert_not_called()

    def test_retries_even_if_path_already_known(self) -> None:
        """Even if the path is already known, still sanitize and retry."""
        client = _mock_es_client()
        text_field_paths = {"spec.inputs.type"}
        document: dict[str, object] = {
            "spec": {"inputs": {"type": {"nested": "value"}}},
        }
        error = self._make_text_conflict_error(field_path="spec.inputs.type")

        result = elastic_search_api._retry_with_sanitized_spec(
            client=client,
            index_name="idx",
            document_id="d1",
            document=document,
            text_field_paths=text_field_paths,
            error=error,
        )

        assert result is True
        assert text_field_paths == {"spec.inputs.type"}
        assert document == {
            "spec": {
                "inputs": {"type": json.dumps({"nested": "value"}, sort_keys=True)}
            },
        }
        client.update.assert_called_once()

    def test_returns_false_if_retry_fails(self) -> None:
        client = _mock_es_client()
        client.update.side_effect = RuntimeError("still broken")
        document: dict[str, object] = {
            "spec": {"inputs": [{"type": {"nested": "obj"}}]}
        }
        error = self._make_text_conflict_error(field_path="spec.inputs.type")

        result = elastic_search_api._retry_with_sanitized_spec(
            client=client,
            index_name="idx",
            document_id="d1",
            document=document,
            text_field_paths=set(),
            error=error,
        )

        assert result is False
        assert document == {
            "spec": {
                "inputs": [{"type": json.dumps({"nested": "obj"}, sort_keys=True)}]
            }
        }


# ---------------------------------------------------------------------------
# _sanitize_spec_with_mapping
# ---------------------------------------------------------------------------


class TestSanitizeSpecWithMapping:
    """Test every JSON-deserializable type at a text-mapped field path."""

    _TEXT_PATHS = {"spec.inputs.type"}

    def test_str_at_text_path_unchanged(self) -> None:
        obj = {"inputs": [{"type": "String"}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": "String"}]}

    def test_int_at_text_path_unchanged(self) -> None:
        obj = {"inputs": [{"type": 42}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": 42}]}

    def test_float_at_text_path_unchanged(self) -> None:
        obj = {"inputs": [{"type": 3.14}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": 3.14}]}

    def test_bool_at_text_path_unchanged(self) -> None:
        obj = {"inputs": [{"type": True}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": True}]}

    def test_none_at_text_path_unchanged(self) -> None:
        obj = {"inputs": [{"type": None}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": None}]}

    def test_dict_at_text_path_stringified(self) -> None:
        nested = {"JsonObject": {"data_type": "proto:tfx.components.example_gen.Input"}}
        obj = {"inputs": [{"type": nested}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": json.dumps(nested, sort_keys=True)}]}

    def test_list_at_text_path_stringified(self) -> None:
        list_val = ["a", "b", "c"]
        obj = {"inputs": [{"type": list_val}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"type": json.dumps(list_val, sort_keys=True)}]}

    def test_dict_at_unmapped_path_passes_through(self) -> None:
        nested = {"some": "object"}
        obj = {"inputs": [{"metadata": nested}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"metadata": {"some": "object"}}]}

    def test_list_at_unmapped_path_passes_through(self) -> None:
        list_val = ["x", "y"]
        obj = {"inputs": [{"tags": list_val}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=self._TEXT_PATHS,
            current_path="spec",
        )
        assert result == {"inputs": [{"tags": ["x", "y"]}]}

    def test_nested_list_of_objects_with_mixed_types(self) -> None:
        """Array where some items have string type and some have dict type."""
        obj = {
            "inputs": [
                {"name": "input_base", "type": "String"},
                {
                    "name": "input_config",
                    "type": {"JsonObject": {"data_type": "proto:Input"}},
                },
            ]
        }
        text_paths = {"spec.inputs.type", "spec.inputs.name"}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=text_paths,
            current_path="spec",
        )
        assert result == {
            "inputs": [
                {"name": "input_base", "type": "String"},
                {
                    "name": "input_config",
                    "type": json.dumps(
                        {"JsonObject": {"data_type": "proto:Input"}},
                        sort_keys=True,
                    ),
                },
            ]
        }

    def test_empty_text_field_paths(self) -> None:
        """No text paths → everything passes through unchanged."""
        nested = {"JsonObject": {"data_type": "proto:Input"}}
        obj = {"inputs": [{"type": nested}]}
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=obj,
            text_field_paths=set(),
            current_path="spec",
        )
        assert result == {"inputs": [{"type": nested}]}

    def test_real_world_tfx_component(self) -> None:
        """CsvExampleGen-like spec: dict type fields stringified, string type fields unchanged."""
        text_paths = {
            "spec.inputs.name",
            "spec.inputs.type",
            "spec.outputs.name",
            "spec.outputs.type",
            "spec.name",
        }
        spec = {
            "name": "CsvExampleGen",
            "inputs": [
                {"name": "input_base", "type": "String"},
                {
                    "name": "input_config",
                    "type": {
                        "JsonObject": {
                            "data_type": "proto:tfx.components.example_gen.Input"
                        }
                    },
                },
                {
                    "name": "range_config",
                    "type": {
                        "JsonObject": {"data_type": "proto:tfx.configs.RangeConfig"}
                    },
                    "optional": True,
                },
            ],
            "outputs": [{"name": "examples", "type": "Examples"}],
        }
        result = elastic_search_api._sanitize_spec_with_mapping(
            obj=spec,
            text_field_paths=text_paths,
            current_path="spec",
        )
        assert result == {
            "name": "CsvExampleGen",
            "inputs": [
                {"name": "input_base", "type": "String"},
                {
                    "name": "input_config",
                    "type": json.dumps(
                        {
                            "JsonObject": {
                                "data_type": "proto:tfx.components.example_gen.Input"
                            }
                        },
                        sort_keys=True,
                    ),
                },
                {
                    "name": "range_config",
                    "type": json.dumps(
                        {"JsonObject": {"data_type": "proto:tfx.configs.RangeConfig"}},
                        sort_keys=True,
                    ),
                    "optional": True,
                },
            ],
            "outputs": [{"name": "examples", "type": "Examples"}],
        }


# ---------------------------------------------------------------------------
# _embed_and_cache_vector
# ---------------------------------------------------------------------------


class TestEmbedAndCacheVector:
    """Tests for the generic _embed_and_cache_vector core function."""

    def test_cache_hit_skips_embed_call(self) -> None:
        """Vector missing on doc → cache hit → uses cached embedding, no API call."""
        client = _mock_es_client()
        client.get.side_effect = [
            mock.MagicMock(body={"_source": {}}),
            mock.MagicMock(
                body={
                    "_source": {
                        elastic_search_api.ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME: _MOCK_VECTOR,
                    }
                }
            ),
        ]

        with mock.patch.object(elastic_search_api, "_embed_texts") as mock_embed:
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello world",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
            )
            mock_embed.assert_not_called()

        client.update.assert_called_once()
        assert client.update.call_args[1]["doc"] == {"my_vector": _MOCK_VECTOR}

    def test_recreate_skips_doc_check(self) -> None:
        """recreate_embeddings=True never calls client.get for the doc vector check."""
        client = _mock_es_client()

        with mock.patch.object(
            elastic_search_api, "_embed_texts", return_value=[_MOCK_VECTOR]
        ):
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello world",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
                recreate_embeddings=True,
            )

        for call in client.get.call_args_list:
            assert call[1].get("source") is None, (
                "recreate_embeddings=True should not check the doc "
                "for existing vectors"
            )

    def test_recreate_skips_cache_lookup(self) -> None:
        """recreate_embeddings=True never looks up the cache index."""
        client = _mock_es_client()

        with mock.patch.object(
            elastic_search_api, "_embed_texts", return_value=[_MOCK_VECTOR]
        ):
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello world",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
                recreate_embeddings=True,
            )

        for call in client.get.call_args_list:
            assert (
                call[1].get("index") != "cache_index"
            ), "recreate_embeddings=True should not look up the cache"

    def test_recreate_always_calls_api(self) -> None:
        """recreate_embeddings=True always calls the embedding API."""
        client = _mock_es_client()

        with mock.patch.object(
            elastic_search_api, "_embed_texts", return_value=[_MOCK_VECTOR]
        ) as mock_embed:
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello world",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
                recreate_embeddings=True,
            )
            mock_embed.assert_called_once()

    def test_recreate_overwrites_existing_cache_entry(self) -> None:
        """recreate_embeddings=True overwrites a pre-existing cache entry via client.index."""
        client = _mock_es_client()
        new_vector = [0.9] * 3072

        with mock.patch.object(
            elastic_search_api, "_embed_texts", return_value=[new_vector]
        ):
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello world",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
                recreate_embeddings=True,
            )

        # client.index is an upsert — overwrites any existing cache doc with same _id
        client.index.assert_called_once()
        cache_doc = client.index.call_args[1]["document"]
        assert (
            cache_doc[elastic_search_api.ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME]
            == new_vector[: elastic_search_api.es_embedding_size]
        )

        # Also updates the main doc with the fresh vector
        client.update.assert_called_once()
        assert client.update.call_args[1]["doc"] == {
            "my_vector": new_vector[: elastic_search_api.es_embedding_size]
        }

    def test_recreate_false_with_existing_vector_does_not_call_api(
        self,
    ) -> None:
        """Sanity check: recreate_embeddings=False + vector present → no API call."""
        client = _mock_es_client()
        client.get.return_value = mock.MagicMock(
            body={"_source": {"my_vector": _MOCK_VECTOR}}
        )

        with mock.patch.object(elastic_search_api, "_embed_texts") as mock_embed:
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello world",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
                recreate_embeddings=False,
            )
            mock_embed.assert_not_called()

        client.update.assert_not_called()

    def test_cache_miss_embeds_caches_and_updates(self) -> None:
        """Happy-path: cache miss → embed → cache store → doc update.

        Verifies all arguments (index, id, field, cache doc) in one test.
        """
        client = _mock_es_client()
        client.get.side_effect = elasticsearch.NotFoundError(
            message="not found", meta=mock.MagicMock(), body={}
        )

        with mock.patch.object(
            elastic_search_api, "_embed_texts", return_value=[_MOCK_VECTOR]
        ) as mock_embed:
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="my_index",
                document_id="comp_abc",
                text="hello world",
                vector_field_name="custom_vector",
                cache_index_name="custom_cache",
                recreate_embeddings=True,
            )
            mock_embed.assert_called_once()

        client.index.assert_called_once()
        cache_kwargs = client.index.call_args[1]
        assert cache_kwargs["index"] == "custom_cache"
        cache_doc = cache_kwargs["document"]
        assert (
            cache_doc[elastic_search_api.ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME]
            == _MOCK_VECTOR
        )
        assert (
            cache_doc[elastic_search_api.ES_EMBEDDINGS_CACHE_TEXT_PROPERTY_NAME]
            == "hello world"
        )

        client.update.assert_called_once()
        update_kwargs = client.update.call_args[1]
        assert update_kwargs["index"] == "my_index"
        assert update_kwargs["id"] == "comp_abc"
        assert update_kwargs["doc"] == {"custom_vector": _MOCK_VECTOR}

    def test_skip_existing_when_vector_present(self) -> None:
        client = _mock_es_client()
        client.get.return_value = mock.MagicMock(
            body={"_source": {"my_vector": _MOCK_VECTOR}}
        )

        with mock.patch.object(elastic_search_api, "_embed_texts") as mock_embed:
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
            )
            mock_embed.assert_not_called()

        client.update.assert_not_called()

    def test_recreate_embeds_even_if_vector_present(self) -> None:
        client = _mock_es_client()

        def _mock_get(*args, **kwargs):
            if "source" in kwargs:
                return mock.MagicMock(body={"_source": {"my_vector": _MOCK_VECTOR}})
            raise elasticsearch.NotFoundError(
                message="not found", meta=mock.MagicMock(), body={}
            )

        client.get.side_effect = _mock_get

        with mock.patch.object(
            elastic_search_api, "_embed_texts", return_value=[_MOCK_VECTOR]
        ):
            elastic_search_api._embed_and_cache_vector(
                client=client,
                index="test_index",
                document_id="doc1",
                text="hello",
                vector_field_name="my_vector",
                cache_index_name="cache_index",
                recreate_embeddings=True,
            )

        client.update.assert_called_once()


# ---------------------------------------------------------------------------
# _elasticsearch_add_component_embedding_vector_to_index
# ---------------------------------------------------------------------------


class TestAddComponentEmbeddingVectors:
    """Tests for the orchestrator that calls _embed_and_cache_vector for both vectors."""

    def test_calls_embed_and_cache_twice(self) -> None:
        client = _mock_es_client()

        with mock.patch.object(
            elastic_search_api, "_embed_and_cache_vector"
        ) as mock_core:
            elastic_search_api._elasticsearch_add_component_embedding_vector_to_index(
                client=client,
                index="test_index",
                document_id="doc1",
                component_spec_dict={
                    "name": "Train",
                    "description": "Trains a model",
                },
            )
            assert mock_core.call_count == 2

            call_1 = mock_core.call_args_list[0][1]
            assert (
                call_1["vector_field_name"]
                == elastic_search_api.name_and_description_vector_property_name
            )
            assert (
                call_1["cache_index_name"]
                == elastic_search_api.es_embeddings_cache_index_name
            )
            assert call_1["text"] == "Train\n\nTrains a model"

            call_2 = mock_core.call_args_list[1][1]
            assert (
                call_2["vector_field_name"]
                == elastic_search_api.full_spec_vector_property_name
            )
            assert (
                call_2["cache_index_name"]
                == elastic_search_api.full_spec_embeddings_cache_index_name
            )
            assert call_2["text"] == json.dumps(
                {"name": "Train", "description": "Trains a model"}
            )

        update_call = client.update.call_args
        assert update_call[1]["doc"] == {
            "name_and_description": "Train\n\nTrains a model"
        }

    def test_first_vector_error_does_not_skip_second(self) -> None:
        """If the first _embed_and_cache_vector fails, the second is still attempted"""
        client = _mock_es_client()

        call_count = 0

        def _side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("AI Proxy error")
            return elastic_search_api.EmbedStatus.EMBEDDED

        with mock.patch.object(
            elastic_search_api,
            "_embed_and_cache_vector",
            side_effect=_side_effect,
        ):
            statuses = elastic_search_api._elasticsearch_add_component_embedding_vector_to_index(
                client=client,
                index="test_index",
                document_id="doc1",
                component_spec_dict={
                    "name": "Train",
                    "description": "Trains a model",
                },
            )

        assert len(statuses) == 2
        assert statuses[0][1] == elastic_search_api.EmbedStatus.ERROR
        assert statuses[1][1] == elastic_search_api.EmbedStatus.EMBEDDED


# ---------------------------------------------------------------------------
# IndexingProgress
# ---------------------------------------------------------------------------


class TestIndexingProgress:
    def test_initial_state(self) -> None:
        progress = elastic_search_api.IndexingProgress()
        assert progress.status == elastic_search_api.IndexingStatus.IDLE
        assert progress.total_published_components == 0
        assert progress.indexing_success == 0
        assert progress.indexing_errors == 0
        assert progress.started_at is None
        assert progress.finished_at is None
        assert progress.error_message is None

    def test_reset_clears_all_counters(self) -> None:
        progress = elastic_search_api.IndexingProgress()
        progress.status = elastic_search_api.IndexingStatus.DONE
        progress.total_published_components = 791
        progress.indexing_success = 42
        progress.indexing_errors = 3
        progress.embed_stats.record(
            field_name="vec_a",
            status=elastic_search_api.EmbedStatus.ERROR,
        )
        progress.finished_at = datetime.datetime(2026, 1, 1)
        progress.error_message = "old error"

        progress.reset()

        assert progress.status == elastic_search_api.IndexingStatus.IN_PROGRESS
        assert progress.total_published_components == 0
        assert progress.indexing_success == 0
        assert progress.indexing_errors == 0
        assert progress.embed_stats.to_dict() == {}
        assert progress.started_at is not None
        assert progress.finished_at is None
        assert progress.error_message is None

    def test_to_dict_shape(self) -> None:
        progress = elastic_search_api.IndexingProgress()
        result = progress.to_dict()

        assert result == {
            "status": "idle",
            "total_published_components": 0,
            "indexing": {"success": 0, "errors": 0},
            "embedding": {"success": 0, "errors": 0},
            "embed_stats": {"glossary": elastic_search_api._EMBED_STATS_GLOSSARY},
            "started_at": None,
            "finished_at": None,
            "error_message": None,
        }

    def test_embedding_aggregate_computed_from_embed_stats(self) -> None:
        progress = elastic_search_api.IndexingProgress()
        progress.status = elastic_search_api.IndexingStatus.DONE
        progress.total_published_components = 10
        progress.indexing_success = 10
        progress.indexing_errors = 0
        progress.started_at = datetime.datetime(2026, 5, 27, 11, 0, 0)
        progress.finished_at = datetime.datetime(2026, 5, 27, 11, 2, 0)

        progress.embed_stats.record(
            field_name="vec_a",
            status=elastic_search_api.EmbedStatus.EMBEDDED,
        )
        progress.embed_stats.record(
            field_name="vec_a",
            status=elastic_search_api.EmbedStatus.CACHE_HIT,
        )
        progress.embed_stats.record(
            field_name="vec_a",
            status=elastic_search_api.EmbedStatus.SKIPPED,
        )
        progress.embed_stats.record(
            field_name="vec_a",
            status=elastic_search_api.EmbedStatus.ERROR,
        )
        progress.embed_stats.record(
            field_name="vec_b",
            status=elastic_search_api.EmbedStatus.ERROR,
        )
        progress.embed_stats.record(
            field_name="vec_b",
            status=elastic_search_api.EmbedStatus.EMBEDDED,
        )

        result = progress.to_dict()

        assert result == {
            "status": "done",
            "total_published_components": 10,
            "indexing": {"success": 10, "errors": 0},
            "embedding": {"success": 4, "errors": 2},
            "embed_stats": {
                "vec_a": {
                    "embedded": 1,
                    "cache_hit": 1,
                    "skipped": 1,
                    "error": 1,
                },
                "vec_b": {
                    "embedded": 1,
                    "cache_hit": 0,
                    "skipped": 0,
                    "error": 1,
                },
                "glossary": elastic_search_api._EMBED_STATS_GLOSSARY,
            },
            "started_at": "2026-05-27T11:00:00",
            "finished_at": "2026-05-27T11:02:00",
            "error_message": None,
        }


# ---------------------------------------------------------------------------
# EmbedStatsCollector.to_dict
# ---------------------------------------------------------------------------


class TestEmbedStatsCollectorToDict:
    def test_empty(self) -> None:
        collector = elastic_search_api.EmbedStatsCollector()
        assert collector.to_dict() == {}

    def test_with_records(self) -> None:
        collector = elastic_search_api.EmbedStatsCollector()
        collector.record(
            field_name="field_a",
            status=elastic_search_api.EmbedStatus.EMBEDDED,
        )
        collector.record(
            field_name="field_a",
            status=elastic_search_api.EmbedStatus.ERROR,
        )
        collector.record(
            field_name="field_b",
            status=elastic_search_api.EmbedStatus.CACHE_HIT,
        )

        result = collector.to_dict()
        assert result == {
            "field_a": {
                "embedded": 1,
                "cache_hit": 0,
                "skipped": 0,
                "error": 1,
            },
            "field_b": {
                "embedded": 0,
                "cache_hit": 1,
                "skipped": 0,
                "error": 0,
            },
        }

    def test_total_success(self) -> None:
        collector = elastic_search_api.EmbedStatsCollector()
        collector.record(field_name="a", status=elastic_search_api.EmbedStatus.EMBEDDED)
        collector.record(
            field_name="a", status=elastic_search_api.EmbedStatus.CACHE_HIT
        )
        collector.record(field_name="b", status=elastic_search_api.EmbedStatus.SKIPPED)
        collector.record(field_name="b", status=elastic_search_api.EmbedStatus.ERROR)
        assert collector.total_success() == 3
        assert collector.total_errors() == 1

    def test_total_errors_empty(self) -> None:
        collector = elastic_search_api.EmbedStatsCollector()
        assert collector.total_success() == 0
        assert collector.total_errors() == 0


# ---------------------------------------------------------------------------
# Background indexing API (409, 202, status endpoint)
# ---------------------------------------------------------------------------


class TestBackgroundIndexingAPI:
    """Tests for the POST index_published_components and GET indexing_status endpoints."""

    @pytest.fixture(autouse=True)
    def _reset_global_progress(self) -> None:
        """Ensure each test starts with a clean global _indexing_progress."""
        progress = elastic_search_api._indexing_progress
        progress.status = elastic_search_api.IndexingStatus.IDLE
        progress.total_published_components = 0
        progress.indexing_success = 0
        progress.indexing_errors = 0
        progress.embed_stats = elastic_search_api.EmbedStatsCollector()
        progress.started_at = None
        progress.finished_at = None
        progress.error_message = None

    def test_409_when_already_in_progress(self) -> None:
        """Calling indexing while already in progress returns 409."""
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        elastic_search_api._indexing_progress.status = (
            elastic_search_api.IndexingStatus.IN_PROGRESS
        )

        response = client.post(
            "/api/admin/elasticsearch/index_published_components",
        )
        assert response.status_code == 409
        assert "already in progress" in response.json()["detail"]

    def test_202_accepted(self) -> None:
        """Calling indexing when idle returns 202 and enqueues the task."""
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        response = client.post(
            "/api/admin/elasticsearch/index_published_components",
        )
        assert response.status_code == 202
        body = response.json()
        assert body["message"] == "Indexing started"
        assert "indexing_status" in body["status_url"]

    def test_status_endpoint_returns_progress(self) -> None:
        """GET /indexing_status returns the current progress."""
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        elastic_search_api._indexing_progress.status = (
            elastic_search_api.IndexingStatus.DONE
        )
        elastic_search_api._indexing_progress.total_published_components = 51
        elastic_search_api._indexing_progress.indexing_success = 50
        elastic_search_api._indexing_progress.indexing_errors = 1
        elastic_search_api._indexing_progress.started_at = datetime.datetime(
            2026,
            5,
            27,
            11,
            0,
            0,
        )
        elastic_search_api._indexing_progress.finished_at = datetime.datetime(
            2026,
            5,
            27,
            11,
            2,
            0,
        )

        response = client.get(
            "/api/experimental/elasticsearch/indexing_status",
        )
        assert response.status_code == 200
        body = response.json()
        assert body == {
            "status": "done",
            "total_published_components": 51,
            "indexing": {"success": 50, "errors": 1},
            "embedding": {"success": 0, "errors": 0},
            "embed_stats": {"glossary": elastic_search_api._EMBED_STATS_GLOSSARY},
            "started_at": "2026-05-27T11:00:00",
            "finished_at": "2026-05-27T11:02:00",
            "error_message": None,
        }


# ---------------------------------------------------------------------------
# _get_es_client
# ---------------------------------------------------------------------------


class TestGetEsClient:
    def test_raises_503_when_url_not_configured(self) -> None:
        with mock.patch.object(elastic_search_api, "_es_client_factory", None):
            with pytest.raises(fastapi.HTTPException) as exc_info:
                elastic_search_api._get_es_client()
            assert exc_info.value.status_code == 503

    def test_returns_client_when_url_configured(self) -> None:
        with mock.patch.object(
            elastic_search_api,
            "_es_client_factory",
            lambda: elasticsearch.Elasticsearch(hosts=["http://localhost:9200"]),
        ):
            client = elastic_search_api._get_es_client()
            assert isinstance(client, elasticsearch.Elasticsearch)


# ---------------------------------------------------------------------------
# Exception handlers (errors.py)
# ---------------------------------------------------------------------------


class TestElasticsearchExceptionHandlers:
    def test_api_error_returns_original_status(self) -> None:
        """elasticsearch.ApiError (e.g., 400 BadRequest) returns the original status code."""
        from cloud_pipelines_backend.search.elasticsearch import (
            errors as es_errors,
        )

        app = fastapi.FastAPI()
        es_errors.register_elasticsearch_exception_handlers(app=app)

        @app.get("/trigger-api-error")
        def trigger():
            raise elasticsearch.BadRequestError(
                message="search_phase_execution_exception",
                meta=mock.MagicMock(status=400),
                body={
                    "error": {
                        "root_cause": [
                            {
                                "type": "query_shard_exception",
                                "reason": "No mapping found for [digest.keyword] in order to sort on",
                            }
                        ],
                        "type": "search_phase_execution_exception",
                        "reason": "all shards failed",
                    }
                },
            )

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/trigger-api-error")

        assert response.status_code == 400
        body = response.json()
        assert body == {
            "error": "search_phase_execution_exception",
            "reason": "No mapping found for [digest.keyword] in order to sort on",
            "timestamp": body["timestamp"],
        }

    def test_transport_error_returns_503(self) -> None:
        """elasticsearch.TransportError returns 503."""
        from cloud_pipelines_backend.search.elasticsearch import (
            errors as es_errors,
        )

        app = fastapi.FastAPI()
        es_errors.register_elasticsearch_exception_handlers(app=app)

        @app.get("/trigger-transport-error")
        def trigger():
            raise elasticsearch.TransportError("Connection refused")

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/trigger-transport-error")

        assert response.status_code == 503
        body = response.json()
        assert body == {
            "error": "transport_error",
            "reason": "Connection refused",
            "timestamp": body["timestamp"],
        }


# ---------------------------------------------------------------------------
# Admin guard integration
# ---------------------------------------------------------------------------


class TestAdminGuards:
    @staticmethod
    def _make_admin_checker(*, is_admin: bool):
        def ensure_admin():
            if not is_admin:
                raise RuntimeError("User is not an admin user")

        return ensure_admin

    def test_indexing_endpoint_blocked_without_admin(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
            ensure_admin_user=self._make_admin_checker(is_admin=False),
        )
        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/api/admin/elasticsearch/index_published_components",
        )
        assert response.status_code == 500

    def test_indexing_endpoint_allowed_with_admin(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
            ensure_admin_user=self._make_admin_checker(is_admin=True),
        )
        client = TestClient(app)

        response = client.post(
            "/api/admin/elasticsearch/index_published_components",
        )
        assert response.status_code == 202

    def test_delete_index_blocked_without_admin(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
            ensure_admin_user=self._make_admin_checker(is_admin=False),
        )
        client = TestClient(app, raise_server_exceptions=False)

        response = client.delete(
            "/api/admin/experimental/elasticsearch/indices/test_index",
        )
        assert response.status_code == 500

    def test_delete_index_allowed_with_admin(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
            ensure_admin_user=self._make_admin_checker(is_admin=True),
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        mock_es.indices.delete.return_value = {"acknowledged": True}

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.delete(
                "/api/admin/experimental/elasticsearch/indices/test_index",
            )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Index management endpoints (list / get / delete)
# ---------------------------------------------------------------------------


def _build_test_client_with_es_mock(*, es_mock=None):
    """Build a TestClient with a patched ES client."""
    app = fastapi.FastAPI()
    db_engine = mock.MagicMock()
    elastic_search_api.setup_elastic_search_routes(
        app=app,
        db_engine=db_engine,
    )
    return TestClient(app), es_mock


def _mock_es_client_unspecced():
    """Return a MagicMock without spec to allow nested attribute access (cat, indices)."""
    return mock.MagicMock()


def _setup_index_mock(
    *,
    mock_es,
    index_name: str,
    properties: dict,
    count_values: list[int],
    health: str = "green",
    status: str = "open",
    store_size: str = "1mb",
) -> None:
    mock_es.cat.indices.return_value = [
        {
            "index": index_name,
            "health": health,
            "status": status,
            "store.size": store_size,
            "creation.date.string": "2026-05-28T07:30:00.000Z",
        },
    ]
    mock_es.indices.get_mapping.return_value = {
        index_name: {"mappings": {"properties": properties}},
    }
    mock_es.indices.get_settings.return_value = {
        index_name: {"settings": {"index": {"number_of_shards": "1"}}},
    }
    mock_es.indices.get_alias.return_value = {
        index_name: {"aliases": {}},
    }
    mock_es.count.side_effect = [{"count": c} for c in count_values]


class TestListIndices:
    def test_returns_indices_with_field_counts(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        _setup_index_mock(
            mock_es=mock_es,
            index_name="published_components",
            properties={"name": {"type": "text"}, "digest": {"type": "text"}},
            count_values=[100, 95, 100],
        )

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.get(
                "/api/experimental/elasticsearch/indices",
            )

        assert response.status_code == 200
        assert response.json() == [
            {
                "index": "published_components",
                "health": "green",
                "status": "open",
                "store_size": "1mb",
                "created_at": "2026-05-28T07:30:00.000Z",
                "docs_count": 100,
                "fields_count": 2,
                "field_doc_counts": {"name": 95, "digest": 100},
            },
        ]

    def test_excludes_internal_indices_by_default(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        # First call: list all index names; second call: cat metadata for the one kept
        mock_es.cat.indices.side_effect = [
            [
                {"index": "published_components"},
                {"index": ".ds-monitoring-2026.05.28"},
                {"index": ".internal.alerts-default-000001"},
            ],
            [
                {
                    "index": "published_components",
                    "health": "green",
                    "status": "open",
                    "store.size": "1mb",
                    "creation.date.string": "2026-05-28T07:30:00.000Z",
                },
            ],
        ]
        mock_es.indices.get_mapping.return_value = {
            "published_components": {
                "mappings": {"properties": {"name": {"type": "text"}}}
            },
        }
        mock_es.count.side_effect = [{"count": 10}, {"count": 10}]

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.get("/api/experimental/elasticsearch/indices")

        assert response.status_code == 200
        assert response.json() == [
            {
                "index": "published_components",
                "health": "green",
                "status": "open",
                "store_size": "1mb",
                "created_at": "2026-05-28T07:30:00.000Z",
                "docs_count": 10,
                "fields_count": 1,
                "field_doc_counts": {"name": 10},
            },
        ]

    def test_includes_internal_indices_when_requested(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        mock_es.cat.indices.side_effect = [
            [
                {"index": "published_components"},
                {"index": ".ds-monitoring"},
            ],
            [
                {
                    "index": "published_components",
                    "health": "green",
                    "status": "open",
                    "store.size": "1mb",
                    "creation.date.string": "2026-05-28T07:30:00.000Z",
                },
            ],
            [
                {
                    "index": ".ds-monitoring",
                    "health": "green",
                    "status": "open",
                    "store.size": "500kb",
                    "creation.date.string": "2026-05-25T00:00:00.000Z",
                },
            ],
        ]
        mock_es.indices.get_mapping.side_effect = [
            {"published_components": {"mappings": {"properties": {}}}},
            {".ds-monitoring": {"mappings": {"properties": {}}}},
        ]
        mock_es.count.side_effect = [{"count": 10}, {"count": 5}]

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.get(
                "/api/experimental/elasticsearch/indices?include_internal=true"
            )

        assert response.status_code == 200
        assert response.json() == [
            {
                "index": "published_components",
                "health": "green",
                "status": "open",
                "store_size": "1mb",
                "created_at": "2026-05-28T07:30:00.000Z",
                "docs_count": 10,
                "fields_count": 0,
                "field_doc_counts": {},
            },
            {
                "index": ".ds-monitoring",
                "health": "green",
                "status": "open",
                "store_size": "500kb",
                "created_at": "2026-05-25T00:00:00.000Z",
                "docs_count": 5,
                "fields_count": 0,
                "field_doc_counts": {},
            },
        ]


class TestGetIndex:
    def test_returns_single_index_non_verbose(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        _setup_index_mock(
            mock_es=mock_es,
            index_name="my_index",
            properties={"title": {"type": "text"}},
            count_values=[42, 42],
        )

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.get(
                "/api/experimental/elasticsearch/indices/my_index",
            )

        assert response.status_code == 200
        assert response.json() == {
            "index": "my_index",
            "health": "green",
            "status": "open",
            "store_size": "1mb",
            "created_at": "2026-05-28T07:30:00.000Z",
            "docs_count": 42,
            "fields_count": 1,
            "field_doc_counts": {"title": 42},
        }

    def test_verbose_includes_fields_settings_aliases(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        _setup_index_mock(
            mock_es=mock_es,
            index_name="my_index",
            properties={"title": {"type": "text"}},
            count_values=[42, 42],
        )

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.get(
                "/api/experimental/elasticsearch/indices/my_index?verbose=true",
            )

        assert response.status_code == 200
        assert response.json() == {
            "index": "my_index",
            "health": "green",
            "status": "open",
            "store_size": "1mb",
            "created_at": "2026-05-28T07:30:00.000Z",
            "docs_count": 42,
            "fields_count": 1,
            "field_doc_counts": {"title": 42},
            "fields": {"title": {"type": "text"}},
            "settings": {"index": {"number_of_shards": "1"}},
            "aliases": {},
        }


class TestDeleteIndex:
    def test_delete_single_index(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        mock_es.indices.delete.return_value = {"acknowledged": True}

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.delete(
                "/api/admin/experimental/elasticsearch/indices/test_index",
            )

        assert response.status_code == 200
        assert response.json() == {"index": "test_index", "deleted": True}

    def test_delete_nonexistent_index(self) -> None:
        app = fastapi.FastAPI()
        db_engine = mock.MagicMock()
        elastic_search_api.setup_elastic_search_routes(
            app=app,
            db_engine=db_engine,
        )
        client = TestClient(app)

        mock_es = _mock_es_client_unspecced()
        mock_es.indices.delete.return_value = {}

        with mock.patch.object(
            elastic_search_api,
            "_get_es_client",
            return_value=mock_es,
        ):
            response = client.delete(
                "/api/admin/experimental/elasticsearch/indices/gone_index",
            )

        assert response.status_code == 200
        assert response.json() == {"index": "gone_index", "deleted": False}
