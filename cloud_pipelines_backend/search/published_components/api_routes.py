"""FastAPI route handlers for the component search API."""

import copy
import json
from collections.abc import Callable
from typing import Any, Final

import elasticsearch
import fastapi
import pydantic
from starlette import status as http_status

from cloud_pipelines_backend.search.published_components import (
    filter_query_models as models,
)
from cloud_pipelines_backend.search.published_components.elasticsearch import (
    query_translation,
)
from cloud_pipelines_backend.search.published_components.elasticsearch import (
    schema as schema_module,
)

_ES_INDEX_NAME: Final[str] = "published_components"


def _deref_schema(*, schema: dict[str, Any]) -> dict[str, Any]:
    """Recursively inline all ``$defs``/``$ref`` so Swagger UI (OpenAPI 3.0) can render it.

    Pydantic v2 emits JSON Schema 2020-12 with ``$defs`` and ``$ref: "#/$defs/..."``
    but Swagger UI only understands ``#/components/schemas/...`` references.
    Recursive schemas (e.g. ComponentAndPredicate -> ComponentPredicate -> ComponentAndPredicate)
    are inlined up to ``_MAX_DEPTH`` levels deep; beyond that the ``$ref`` is dropped.
    """
    _MAX_DEPTH: Final[int] = 4
    defs = schema.pop("$defs", {})

    def _resolve(node: Any, *, depth: int = 0) -> Any:
        if isinstance(node, dict):
            if "$ref" in node:
                ref_name = node["$ref"].rsplit("/", 1)[-1]
                if ref_name in defs and depth < _MAX_DEPTH:
                    return _resolve(copy.deepcopy(defs[ref_name]), depth=depth + 1)
                return {}
            return {k: _resolve(v, depth=depth) for k, v in node.items()}
        if isinstance(node, list):
            return [_resolve(item, depth=depth) for item in node]
        return node

    return _resolve(schema)


_CORE_FIELDS = frozenset({"digest", "name", "published_by"})


def build_results_from_hits(
    *,
    hits: list[dict],
    include_extra_fields: bool,
) -> list[models.ComponentSearchResult]:
    """Map raw ES hits into ComponentSearchResult objects.

    Core fields (digest, name, published_by) are always populated via
    ``.get()`` with empty-string defaults so a missing key never causes a
    crash.  When ``include_extra_fields`` is True, any ``_source`` keys
    beyond the core fields are placed in ``extra_fields``; if none of the
    extra keys carry a value the field is set to ``None``.
    """
    results: list[models.ComponentSearchResult] = []
    for hit in hits:
        source = hit.get("_source", {})
        extra: dict[str, Any] | None = None
        if include_extra_fields:
            extra_data = {k: v for k, v in source.items() if k not in _CORE_FIELDS}
            extra = extra_data if extra_data else None
        results.append(
            models.ComponentSearchResult(
                digest=source.get("digest", ""),
                name=source.get("name", ""),
                published_by=source.get("published_by", ""),
                score=hit.get("_score"),
                extra_fields=extra,
            )
        )
    return results


def parse_query_from_body(
    *,
    raw_body: bytes,
) -> models.ComponentSearchQuery:
    """Parse raw request bytes into a ComponentSearchQuery.

    Accepts both application/json and text/plain content types.
    text/plain also supports clients that send JSON without a CORS preflight.
    """
    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise fastapi.HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid JSON body: {exc}",
        )
    if not isinstance(data, dict):
        raise fastapi.HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Request body must be a JSON object.",
        )
    try:
        return models.ComponentSearchQuery(**data)
    except pydantic.ValidationError as exc:
        raise fastapi.HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=exc.errors(),
        )


def setup_component_search_routes(
    *,
    app: fastapi.FastAPI,
    es_client_factory: Callable[[], elasticsearch.Elasticsearch],
    embedding_function_getter: Callable[[], Callable[[str], list[float]] | None],
    semantic_unavailable_detail: str = "Cannot perform semantic search: embedding provider is not configured.",
    semantic_unavailable_notice: str = "Semantic search is unavailable (embedding provider is not configured).",
) -> None:
    """Register component search endpoints on the FastAPI app."""

    # Raw request parsing accepts JSON sent as either application/json or text/plain.
    # Add the schema explicitly so interactive API documentation retains a JSON editor.
    @app.post(
        "/api/published_components/experimental/search",
        tags=["components"],
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": _deref_schema(
                            schema=models.ComponentSearchQuery.model_json_schema(),
                        ),
                    },
                },
            },
        },
    )
    async def search_components(
        *,
        request: fastapi.Request,
    ) -> models.ComponentSearchResponse:
        raw_body = await request.body()
        query = parse_query_from_body(raw_body=raw_body)
        es_client = es_client_factory()
        embed_fn = embedding_function_getter()
        if query.semantic and embed_fn is None:
            raise fastapi.HTTPException(
                status_code=http_status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=semantic_unavailable_detail,
            )

        es_body = query_translation.component_search_to_es(
            query=query,
            embed_fn=embed_fn,
        )

        response = es_client.search(index=_ES_INDEX_NAME, body=es_body)
        hits = response["hits"]["hits"]
        total = response["hits"]["total"]["value"]

        results = build_results_from_hits(
            hits=hits,
            include_extra_fields=query.fields is not None,
        )

        next_token = query_translation.build_next_page_token(
            hits=hits,
            size=query.size,
        )

        return models.ComponentSearchResponse(
            results=results,
            total=total,
            next_page_token=next_token,
        )

    @app.get(
        "/api/published_components/experimental/search/schema",
        tags=["components"],
        response_model=schema_module.SchemaResponse,
    )
    async def get_component_search_schema() -> schema_module.SchemaResponse:
        es_client = es_client_factory()
        embed_fn = embedding_function_getter()

        return schema_module.get_index_schema(
            es_client=es_client,
            index_name=_ES_INDEX_NAME,
            include_semantic=embed_fn is not None,
            semantic_unavailable_notice=semantic_unavailable_notice,
        )
