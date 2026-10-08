"""Tests for search/published_components/elasticsearch/schema.py."""

from cloud_pipelines_backend.search.published_components.elasticsearch import (
    schema,
)

# ---------------------------------------------------------------------------
# flatten_mapping (ES mapping → FieldInfo) tests
# ---------------------------------------------------------------------------


_SAMPLE_PROPERTIES = {
    "name": {
        "type": "text",
        "meta": {"description": "Component name"},
        "fields": {
            "keyword": {
                "type": "keyword",
            },
        },
    },
    "published_by": {
        "type": "text",
        "meta": {"description": "Publisher email"},
    },
    "digest": {
        "type": "text",
        "meta": {"description": "Component content hash"},
    },
    "spec": {
        "properties": {
            "description": {
                "type": "text",
            },
            "inputs": {
                "properties": {
                    "name": {
                        "type": "text",
                    },
                },
            },
        },
    },
    "name_and_description_vector__embedding-model__3072": {
        "type": "dense_vector",
        "meta": {
            "description": "Semantic search: name + description",
            "search_type": "semantic",
        },
    },
}


class TestFlattenMapping:
    def test_flatten_mapping(self) -> None:
        fields = schema.flatten_mapping(properties=_SAMPLE_PROPERTIES)
        result = [f.model_dump() for f in fields]
        assert result == [
            {"path": "name", "type": "text", "description": "Component name"},
            {
                "path": "published_by",
                "type": "text",
                "description": "Publisher email",
            },
            {
                "path": "digest",
                "type": "text",
                "description": "Component content hash",
            },
            {"path": "spec.description", "type": "text", "description": None},
            {"path": "spec.inputs.name", "type": "text", "description": None},
            {
                "path": "name_and_description",
                "type": "semantic",
                "description": "Semantic search: name + description",
            },
        ]

    def test_semantic_alias_for_known_vector_field(self) -> None:
        """Known vector fields are replaced with their friendly alias."""
        properties = {
            "name_and_description_vector__embedding-model__3072": {
                "type": "dense_vector",
                "meta": {"description": "Semantic search: name + description"},
            },
            "full_spec_vector__embedding-model__3072": {
                "type": "dense_vector",
                "meta": {"description": "Semantic search: full component spec"},
            },
        }
        fields = schema.flatten_mapping(properties=properties)
        paths = [f.path for f in fields]
        assert paths == ["name_and_description", "full_spec"]

    def test_flatten_mapping_excludes_semantic_when_disabled(self) -> None:
        """When include_semantic=False, semantic fields are omitted entirely."""
        fields = schema.flatten_mapping(
            properties=_SAMPLE_PROPERTIES,
            include_semantic=False,
        )
        types = [f.type for f in fields]
        assert "semantic" not in types
        assert (
            len(fields) == 5
        )  # name, published_by, digest, spec.description, spec.inputs.name

    def test_unknown_semantic_field_keeps_original_path(self) -> None:
        """Semantic fields not in the alias dict keep their full ES name."""
        properties = {
            "some_other_vector__model__dims": {
                "type": "dense_vector",
                "meta": {"description": "Unknown vector"},
            },
        }
        fields = schema.flatten_mapping(properties=properties)
        assert fields[0].path == "some_other_vector__model__dims"


# ---------------------------------------------------------------------------
# _map_es_type tests
# ---------------------------------------------------------------------------


class TestMapEsType:
    def test_dense_vector(self) -> None:
        assert schema._map_es_type(es_type="dense_vector") == "semantic"

    def test_sparse_vector(self) -> None:
        assert schema._map_es_type(es_type="sparse_vector") == "semantic"

    def test_semantic_text(self) -> None:
        assert schema._map_es_type(es_type="semantic_text") == "semantic"

    def test_keyword(self) -> None:
        assert schema._map_es_type(es_type="keyword") == "keyword"

    def test_text(self) -> None:
        assert schema._map_es_type(es_type="text") == "text"

    def test_other_type_defaults_to_text(self) -> None:
        assert schema._map_es_type(es_type="long") == "text"
