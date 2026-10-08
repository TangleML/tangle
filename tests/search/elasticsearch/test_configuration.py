"""Provider injection keeps indexing and search aliases consistent."""

from cloud_pipelines_backend.search.elasticsearch import elastic_search_api
from cloud_pipelines_backend.search.published_components.elasticsearch import (
    schema,
)


def test_embedding_configuration_updates_index_names_aliases_and_provider(
    monkeypatch,
):
    fields = (
        "es_embedding_model_name",
        "es_embedding_size",
        "embedding_type_id",
        "name_and_description_vector_property_name",
        "full_spec_vector_property_name",
        "es_embeddings_cache_index_name",
        "full_spec_embeddings_cache_index_name",
        "_embedding_function",
    )
    for name in fields:
        monkeypatch.setattr(elastic_search_api, name, getattr(elastic_search_api, name))
    monkeypatch.setattr(
        elastic_search_api,
        "_SEMANTIC_ALIAS_TO_FIELD",
        dict(elastic_search_api._SEMANTIC_ALIAS_TO_FIELD),
    )
    calls = []

    def embed(**kwargs):
        calls.append(kwargs)
        return [[0.2]]

    elastic_search_api.configure_embeddings(
        model_name="provider:model", dimensions=9000, embedding_function=embed
    )
    assert elastic_search_api.es_embedding_size == 4096
    assert (
        elastic_search_api.full_spec_vector_property_name
        == "full_spec_vector__provider_model__4096"
    )
    assert (
        elastic_search_api.es_embeddings_cache_index_name
        == "embeddings__provider_model__4096"
    )
    assert elastic_search_api._embed_texts("provider:model", ["query"]) == [[0.2]]
    assert calls == [{"embedding_model": "provider:model", "texts": ["query"]}]
    assert (
        schema.flatten_mapping(
            properties={
                elastic_search_api.full_spec_vector_property_name: {
                    "type": "dense_vector"
                }
            }
        )[0].path
        == "full_spec"
    )
