"""Elasticsearch schema discovery and field flattener.

Retrieves the ES index mapping, flattens nested properties into dot-notated
paths, and categorises each field as ``"text"``, ``"keyword"``, or
``"semantic"`` for frontend consumption.
"""

import enum

import elasticsearch

from cloud_pipelines_backend import filter_query_models
from cloud_pipelines_backend.search.elasticsearch import elastic_search_api


class MappedFieldType(str, enum.Enum):
    TEXT = "text"
    KEYWORD = "keyword"
    SEMANTIC = "semantic"


# ES types that collapse into the "semantic" category.
_SEMANTIC_ES_TYPES = frozenset({"dense_vector", "sparse_vector", "semantic_text"})


class FieldInfo(filter_query_models._BaseModel):
    path: str
    type: str
    description: str | None = None


class SchemaResponse(filter_query_models._BaseModel):
    fields: list[FieldInfo]
    total_indexed: int
    notices: list[str] | None = None


def _map_es_type(*, es_type: str) -> MappedFieldType:
    """Map an Elasticsearch field type to one of ``text``, ``keyword``, or ``semantic``."""
    if es_type in _SEMANTIC_ES_TYPES:
        return MappedFieldType.SEMANTIC
    if es_type == "keyword":
        return MappedFieldType.KEYWORD
    return MappedFieldType.TEXT


def flatten_mapping(
    *,
    properties: dict,
    prefix: str = "",
    include_semantic: bool = True,
) -> list[FieldInfo]:
    """Recursively flatten ES mapping properties into dot-notated ``FieldInfo`` entries."""
    fields: list[FieldInfo] = []
    semantic_reverse_field_to_alias = {
        v: k for k, v in elastic_search_api._SEMANTIC_ALIAS_TO_FIELD.items()
    }

    for field_name, field_def in properties.items():
        full_path = f"{prefix}{field_name}" if not prefix else f"{prefix}.{field_name}"
        es_type = field_def.get("type")

        if es_type:
            meta = field_def.get("meta", {})
            description = meta.get("description")
            mapped_type = _map_es_type(es_type=es_type)

            if mapped_type == MappedFieldType.SEMANTIC and not include_semantic:
                continue

            display_path = full_path
            if (
                mapped_type == MappedFieldType.SEMANTIC
                and full_path in semantic_reverse_field_to_alias
            ):
                display_path = semantic_reverse_field_to_alias[full_path]
            fields.append(
                FieldInfo(
                    path=display_path,
                    type=mapped_type,
                    description=description,
                )
            )

        # Recurse into nested properties (e.g. spec.inputs.name)
        nested_props = field_def.get("properties")
        if nested_props:
            fields.extend(
                flatten_mapping(
                    properties=nested_props,
                    prefix=full_path,
                    include_semantic=include_semantic,
                )
            )

        # Skip multi-fields (e.g. name.keyword) — these are ES internals
        # that the translator handles automatically. Clients only need the
        # top-level field name.

    return fields


def get_index_schema(
    *,
    es_client: elasticsearch.Elasticsearch,
    index_name: str,
    include_semantic: bool = True,
    semantic_unavailable_notice: str = "Semantic search is unavailable (embedding provider is not configured).",
) -> SchemaResponse:
    """Fetch the ES schema (field types, descriptions) and document count for an index."""
    mapping_response = es_client.indices.get_mapping(index=index_name)
    index_mapping = mapping_response[index_name]["mappings"]
    properties = index_mapping.get("properties", {})

    fields = flatten_mapping(
        properties=properties,
        include_semantic=include_semantic,
    )

    count_response = es_client.count(index=index_name)
    total_indexed = count_response["count"]

    notices = None
    if not include_semantic:
        notices = [semantic_unavailable_notice]

    return SchemaResponse(
        fields=fields,
        total_indexed=total_indexed,
        notices=notices,
    )
