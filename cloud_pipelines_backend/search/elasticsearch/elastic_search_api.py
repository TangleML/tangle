import asyncio
import dataclasses
import datetime
import hashlib
import json
import logging
import re
from typing import Any, Callable

import elasticsearch
import fastapi
import sqlalchemy as sql
from sqlalchemy import orm
from starlette import status as http_status

from cloud_pipelines_backend import (
    component_library_api_server as component_api,
)
from cloud_pipelines_backend._compat import UTC, StrEnum

logger = logging.getLogger(__name__)


class IndexingStatus(StrEnum):
    """Overall status of the background indexing task."""

    IDLE = "idle"  # No run has ever happened (initial state)
    IN_PROGRESS = "in_progress"  # Background task is currently running
    DONE = "done"  # Last run completed successfully
    ERROR = "error"  # Last run ended with an unhandled exception


class EmbedStatus(StrEnum):
    EMBEDDED = "embedded"
    CACHE_HIT = "cache_hit"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclasses.dataclass
class EmbedFieldStats:
    embedded: int = 0
    cache_hit: int = 0
    skipped: int = 0
    error: int = 0

    def record(self, *, status: EmbedStatus) -> None:
        setattr(self, status.value, getattr(self, status.value) + 1)


class EmbedStatsCollector:
    """Statistics for indexing embedding vectors"""

    def __init__(self) -> None:
        self._by_field: dict[str, EmbedFieldStats] = {}

    def record(self, *, field_name: str, status: EmbedStatus) -> None:
        if field_name not in self._by_field:
            self._by_field[field_name] = EmbedFieldStats()
        self._by_field[field_name].record(status=status)

    def log_summary(self) -> None:
        for field_name, stats in self._by_field.items():
            logger.info(
                f"Embedding stats for {field_name}:"
                f" {stats.embedded} embedded,"
                f" {stats.cache_hit} cache hits,"
                f" {stats.skipped} skipped,"
                f" {stats.error} errors"
            )

    def total_success(self) -> int:
        return sum(
            s.embedded + s.cache_hit + s.skipped for s in self._by_field.values()
        )

    def total_errors(self) -> int:
        return sum(s.error for s in self._by_field.values())

    def to_dict(self) -> dict[str, dict[str, int]]:
        return {
            field_name: dataclasses.asdict(stats)
            for field_name, stats in self._by_field.items()
        }


_EMBED_STATS_GLOSSARY: dict[str, str] = {
    "embedded": "Fresh embedding created via AI Proxy and written to document. Happens on cache miss, or when recreate_embeddings=true.",
    "cache_hit": "Embedding found in cache index by sha256(text) — reused without calling AI Proxy. Only when recreate_embeddings=false.",
    "skipped": "Vector already exists on the document — no action taken. Only when recreate_embeddings=false.",
    "error": "AI Proxy call failed (e.g., 401 Unauthorized) or returned empty embedding. Vector not written.",
}


@dataclasses.dataclass
class IndexingProgress:
    """Tracks progress of the background indexing task."""

    status: IndexingStatus = IndexingStatus.IDLE
    total_published_components: int = 0
    indexing_success: int = 0
    indexing_errors: int = 0
    embed_stats: EmbedStatsCollector = dataclasses.field(
        default_factory=EmbedStatsCollector,
    )
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    error_message: str | None = None

    def reset(self) -> None:
        self.status = IndexingStatus.IN_PROGRESS
        self.total_published_components = 0
        self.indexing_success = 0
        self.indexing_errors = 0
        self.embed_stats = EmbedStatsCollector()
        self.started_at = datetime.datetime.now(UTC)
        self.finished_at = None
        self.error_message = None

    def to_dict(self) -> dict[str, Any]:
        embed_dict = self.embed_stats.to_dict()
        return {
            "status": self.status.value,
            "total_published_components": self.total_published_components,
            "indexing": {
                "success": self.indexing_success,
                "errors": self.indexing_errors,
            },
            "embedding": {
                "success": self.embed_stats.total_success(),
                "errors": self.embed_stats.total_errors(),
            },
            "embed_stats": {**embed_dict, "glossary": _EMBED_STATS_GLOSSARY},
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error_message": self.error_message,
        }


_indexing_progress = IndexingProgress()


es_index_name = "published_components"
ES_EMBEDDINGS_CACHE_TEXT_PROPERTY_NAME = "text"
ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME = "embedding"
es_embedding_model_name = "embedding-model"
es_embedding_size = 3072
_ES_MAX_NUMBER_OF_VECTOR_DIMENSIONS = 4096
_embedding_function: Callable[..., list[list[float]]] | None = None
_es_client_factory: Callable[[], elasticsearch.Elasticsearch] | None = None

es_use_embeddings = True
embedding_type_id = f"{es_embedding_model_name}__{es_embedding_size}".replace(":", "_")
name_and_description_vector_property_name = (
    f"name_and_description_vector__{embedding_type_id}"
)
full_spec_vector_property_name = f"full_spec_vector__{embedding_type_id}"
# es_embeddings_cache_index_name = "embeddings_1"
es_embeddings_cache_index_name = f"embeddings__{embedding_type_id}"
full_spec_embeddings_cache_index_name = f"embeddings_full_spec__{embedding_type_id}"

_SEMANTIC_ALIAS_TO_FIELD: dict[str, str] = {
    "name_and_description": name_and_description_vector_property_name,
    "full_spec": full_spec_vector_property_name,
}


def _vector_mapping(*, description: str, source_fields: str) -> dict[str, Any]:
    """Return a ``dense_vector`` mapping dict for ``put_mapping``.

    Only ``description`` and ``source_fields`` vary between vectors;
    everything else (dims, similarity, model) is shared.
    """
    return {
        "type": "dense_vector",
        "dims": es_embedding_size,
        "index": True,
        "similarity": "cosine",
        "meta": {
            "description": description,
            "source_fields": source_fields,
            "model": es_embedding_model_name,
            "search_type": "semantic",
        },
    }


def _field_mapping_with_description(
    *,
    field_type: str,
    description: str,
) -> dict[str, object]:
    return {
        "type": field_type,
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        "meta": {"description": description},
    }


def _sanitize_spec_with_mapping(
    *,
    obj: object,
    text_field_paths: set[str],
    current_path: str = "",
) -> object:
    """Stringify dict/list values where the ES mapping expects ``text``.

    Fields not in *text_field_paths* pass through unchanged so that
    unknown mapping conflicts surface as ES errors for investigation.

    Example (``spec.inputs.type`` in text_field_paths)::

        {"type": {"JsonObject": {"data_type": "proto:Input"}}}
        → {"type": '{"JsonObject": {"data_type": "proto:Input"}}'}
    """
    if isinstance(obj, dict):
        result: dict[str, object] = {}
        for k, v in obj.items():
            field_path = f"{current_path}.{k}" if current_path else k
            if isinstance(v, (dict, list)) and field_path in text_field_paths:
                logger.warning(
                    "Flattening %s to string at path=%s to match ES text mapping",
                    type(v).__name__,
                    field_path,
                )
                result[k] = json.dumps(v, sort_keys=True)
            else:
                result[k] = _sanitize_spec_with_mapping(
                    obj=v,
                    text_field_paths=text_field_paths,
                    current_path=field_path,
                )
        return result
    if isinstance(obj, list):
        return [
            _sanitize_spec_with_mapping(
                obj=item,
                text_field_paths=text_field_paths,
                current_path=current_path,
            )
            for item in obj
        ]
    return obj


_PARSING_EXCEPTION_PATTERN = re.compile(
    r"failed to parse field \[([^\]]+)\] of type \[(\w+)\]"
)


def _parse_text_conflict_field(
    *,
    error: elasticsearch.BadRequestError,
) -> str | None:
    """Extract the field path from a ``document_parsing_exception`` if the mapped type is ``text``.

    Example: ``"failed to parse field [spec.inputs.type] of type [text]"`` → ``"spec.inputs.type"``

    Returns the field path or ``None`` if the error is not a text-mapping conflict.
    """
    body = getattr(error, "body", None)
    if isinstance(body, dict):
        reason = body.get("error", {}).get("reason", "")
        match = _PARSING_EXCEPTION_PATTERN.search(reason)
        if match and match.group(2) == "text":
            return match.group(1)
    return None


def _retry_with_sanitized_spec(
    *,
    client: elasticsearch.Elasticsearch,
    index_name: str,
    document_id: str,
    document: dict[str, object],
    text_field_paths: set[str],
    error: elasticsearch.BadRequestError,
) -> bool:
    """Try to recover from a text-mapping conflict by sanitizing and retrying.

    Parses the conflicting field from *error*, adds it to *text_field_paths*
    (mutated in place for future documents), re-sanitizes ``document["spec"]``,
    and retries the update. Returns ``True`` if the retry succeeded.
    """
    conflict_path = _parse_text_conflict_field(error=error)
    if not conflict_path:
        return False
    text_field_paths.add(conflict_path)
    logger.warning(
        "Text mapping conflict for doc_id=%s at path=%s, sanitizing and retrying",
        document_id,
        conflict_path,
    )
    document["spec"] = _sanitize_spec_with_mapping(
        obj=document["spec"],
        text_field_paths=text_field_paths,
        current_path="spec",
    )
    try:
        client.update(
            index=index_name,
            id=document_id,
            doc=document,
            doc_as_upsert=True,
        )
        return True
    except Exception:
        logger.exception(
            "Retry failed for doc_id=%s after sanitizing path=%s",
            document_id,
            conflict_path,
        )
        return False


def configure_elasticsearch(
    *, client_factory: Callable[[], elasticsearch.Elasticsearch]
) -> None:
    """Configure the client factory used by the indexing and raw-query routes."""
    global _es_client_factory
    _es_client_factory = client_factory


def configure_embeddings(
    *,
    model_name: str,
    dimensions: int,
    embedding_function: Callable[..., list[list[float]]],
) -> None:
    """Configure the embedding provider while preserving stable index field names."""
    global es_embedding_model_name, es_embedding_size, embedding_type_id
    global name_and_description_vector_property_name, full_spec_vector_property_name
    global es_embeddings_cache_index_name, full_spec_embeddings_cache_index_name
    global _embedding_function
    es_embedding_model_name = model_name
    es_embedding_size = min(dimensions, _ES_MAX_NUMBER_OF_VECTOR_DIMENSIONS)
    embedding_type_id = f"{model_name}__{es_embedding_size}".replace(":", "_")
    name_and_description_vector_property_name = (
        f"name_and_description_vector__{embedding_type_id}"
    )
    full_spec_vector_property_name = f"full_spec_vector__{embedding_type_id}"
    es_embeddings_cache_index_name = f"embeddings__{embedding_type_id}"
    full_spec_embeddings_cache_index_name = f"embeddings_full_spec__{embedding_type_id}"
    _SEMANTIC_ALIAS_TO_FIELD.update(
        name_and_description=name_and_description_vector_property_name,
        full_spec=full_spec_vector_property_name,
    )
    _embedding_function = embedding_function


def _get_es_client() -> elasticsearch.Elasticsearch:
    if _es_client_factory is None:
        raise fastapi.HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Elasticsearch is not configured",
        )
    return _es_client_factory()


async def _get_field_doc_counts(
    *,
    client: elasticsearch.Elasticsearch,
    index_name: str,
    field_names: list[str],
) -> dict[str, int]:
    """Count docs with each field populated"""

    async def _count_field(*, field: str) -> tuple[str, int]:
        result = await asyncio.to_thread(
            client.count,
            index=index_name,
            body={"query": {"exists": {"field": field}}},
        )
        return field, result.get("count", 0)

    pairs = await asyncio.gather(
        *[_count_field(field=f) for f in field_names],
    )
    return dict(pairs)


def setup_elastic_search_routes(
    *,
    app: fastapi.FastAPI,
    db_engine: sql.Engine,
    ensure_admin_user: Callable[..., None] | None = None,
) -> None:
    admin_deps: list[object] = (
        [fastapi.Depends(ensure_admin_user)] if ensure_admin_user else []
    )

    @app.get(
        "/api/experimental/elasticsearch/server_info",
        tags=["elasticsearch"],
    )
    async def elasticsearch_server_info() -> dict:
        es_client = _get_es_client()
        return dict(es_client.info().body)

    @app.post(
        "/api/experimental/elasticsearch/search",
        tags=["elasticsearch"],
    )
    async def elasticsearch_knn_query(
        *,
        search_query_body: dict,
        knn_query: str | None = None,
        knn_k: int = 10,
        knn_num_candidates: int | None = None,
        index_name: str | None = es_index_name,
    ) -> dict:
        es_client = _get_es_client()

        if knn_query:
            knn: dict = search_query_body.setdefault("knn", {})
            knn.setdefault("field", name_and_description_vector_property_name)
            knn.setdefault("k", knn_k)
            if knn_num_candidates:
                knn["num_candidates"] = knn_num_candidates

            query_vector = _embed_texts(
                embedding_model=es_embedding_model_name, texts=[knn_query]
            )[0]
            knn["query_vector"] = query_vector

        response = es_client.search(index=index_name, body=search_query_body)
        return response.body

    def _run_indexing(
        *,
        index_name: str,
        use_embeddings: bool,
        recreate_embeddings: bool,
    ) -> None:
        """Background task that performs the actual indexing loop.

        Updates ``_indexing_progress`` as it works so the status endpoint
        can report live metrics.
        """
        try:
            client = _get_es_client()
            logger.info(
                f"Background indexing started: embeddings={use_embeddings}, recreate={recreate_embeddings}"
            )

            if not client.indices.exists(index=index_name):
                client.indices.create(index=index_name)

            client.indices.put_mapping(
                index=index_name,
                properties={
                    "name": _field_mapping_with_description(
                        field_type="text",
                        description="Component name",
                    ),
                    "published_by": _field_mapping_with_description(
                        field_type="text",
                        description="Publisher email",
                    ),
                    "digest": _field_mapping_with_description(
                        field_type="text",
                        description="Component content hash (unique identifier)",
                    ),
                },
            )

            if use_embeddings:
                client.indices.put_mapping(
                    index=index_name,
                    properties={
                        name_and_description_vector_property_name: _vector_mapping(
                            description="Semantic search: name + description",
                            source_fields="name, description",
                        ),
                        full_spec_vector_property_name: _vector_mapping(
                            description="Semantic search: full component spec",
                            source_fields="name, desc, inputs, outputs, annotations",
                        ),
                    },
                )

                for cache_idx in (
                    es_embeddings_cache_index_name,
                    full_spec_embeddings_cache_index_name,
                ):
                    if not client.indices.exists(index=cache_idx):
                        client.indices.create(index=cache_idx)
                    client.indices.put_mapping(
                        index=cache_idx,
                        properties={
                            ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME: {
                                "type": "dense_vector",
                                "dims": es_embedding_size,
                                "index": True,
                                "similarity": "cosine",
                            },
                        },
                    )

                mapping = client.indices.get_mapping(index=index_name)
                logger.debug(f"ES mapping: {mapping.body}")

            text_field_paths: set[str] = set()

            with orm.Session(
                autocommit=False, autoflush=False, bind=db_engine
            ) as session:
                query = (
                    sql.select(
                        component_api.ComponentRow,
                        component_api.PublishedComponentRow,
                    )
                    .join(
                        component_api.PublishedComponentRow,
                        component_api.ComponentRow.digest
                        == component_api.PublishedComponentRow.digest,
                    )
                    .where(
                        component_api.PublishedComponentRow.deprecated == sql.false()
                    )
                )
                rows = list(session.execute(query).tuples())
                _indexing_progress.total_published_components = len(rows)

                for component_row, published_component_row in rows:
                    id = (
                        f"{component_row.digest}_"
                        f"{published_component_row.published_by}"
                    )

                    # --- Text indexing ---
                    try:
                        spec = (
                            dict(component_row.spec)
                            if component_row.spec
                            else json.loads(component_row.text)
                        )
                        del spec["implementation"]
                        annotations: dict = spec.get("metadata", {}).get(
                            "annotations", {}
                        )
                        for annotation_name in (
                            "python_dependencies",
                            "python_original_code",
                        ):
                            annotations.pop(annotation_name, None)
                        document = dict(
                            digest=component_row.digest,
                            name=published_component_row.name,
                            published_by=published_component_row.published_by,
                            spec=spec,
                        )
                        logger.debug(f"Indexing text fields for doc_id={id}")
                        client.update(
                            index=index_name,
                            id=id,
                            doc=document,
                            doc_as_upsert=True,
                        )
                        _indexing_progress.indexing_success += 1
                    except elasticsearch.BadRequestError as exc:
                        retried = _retry_with_sanitized_spec(
                            client=client,
                            index_name=index_name,
                            document_id=id,
                            document=document,
                            text_field_paths=text_field_paths,
                            error=exc,
                        )
                        if retried:
                            _indexing_progress.indexing_success += 1
                            continue
                        _indexing_progress.indexing_errors += 1
                        logger.exception(f"ES BadRequestError indexing doc_id={id}")
                        continue
                    except Exception:
                        _indexing_progress.indexing_errors += 1
                        logger.exception(f"Unexpected error indexing doc_id={id}")
                        continue

                    # --- Embedding (independent from text) ---
                    if use_embeddings:
                        try:
                            statuses = (
                                _elasticsearch_add_component_embedding_vector_to_index(
                                    client=client,
                                    index=index_name,
                                    document_id=id,
                                    component_spec_dict=spec,
                                    recreate_embeddings=recreate_embeddings,
                                )
                            )
                            for field_name, status in statuses:
                                _indexing_progress.embed_stats.record(
                                    field_name=field_name,
                                    status=status,
                                )
                        except Exception:
                            logger.exception(f"Error embedding doc_id={id}")

            client.indices.refresh(index=index_name)

            logger.info(
                f"Indexing complete:"
                f" {_indexing_progress.indexing_success} text indexed,"
                f" {_indexing_progress.indexing_errors} text errors,"
                f" {_indexing_progress.embed_stats.total_success()} embedding successes,"
                f" {_indexing_progress.embed_stats.total_errors()} embedding errors"
            )
            if use_embeddings:
                _indexing_progress.embed_stats.log_summary()

            _indexing_progress.status = IndexingStatus.DONE
            _indexing_progress.finished_at = datetime.datetime.now(UTC)

        except Exception as exc:
            _indexing_progress.status = IndexingStatus.ERROR
            _indexing_progress.error_message = str(exc)
            _indexing_progress.finished_at = datetime.datetime.now(UTC)
            logger.exception("Background indexing task failed")

    @app.post(
        "/api/admin/elasticsearch/index_published_components",
        status_code=http_status.HTTP_202_ACCEPTED,
        tags=["elasticsearch-admin"],
        dependencies=admin_deps,
    )
    # sync def is fine: no async I/O, just queues a background task and returns.
    def elasticsearch_index_all(
        background_tasks: fastapi.BackgroundTasks,
        index_name: str | None = es_index_name,
        es_use_embeddings: bool = False,
        recreate_embeddings: bool = False,
    ) -> dict:
        if _indexing_progress.status == IndexingStatus.IN_PROGRESS:
            raise fastapi.HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail="Indexing is already in progress.",
            )

        _indexing_progress.reset()

        # The sync ES indexing loop (~2 min) must not run inline or it
        # blocks the event loop, starving the K8s readiness probe → 503.
        # BackgroundTasks runs _run_indexing after the 202 response is
        # sent, keeping the event loop free for health checks.
        # TODO: Consider replacing BackgroundTasks + polling with a
        # StreamingResponse that yields each result as it's indexed,
        # giving the caller a real-time stream (works with sync generators).
        background_tasks.add_task(
            _run_indexing,
            index_name=index_name,
            use_embeddings=es_use_embeddings,
            recreate_embeddings=recreate_embeddings,
        )

        return {
            "message": "Indexing started",
            "status_url": "/api/experimental/elasticsearch/indexing_status",
        }

    @app.get(
        "/api/experimental/elasticsearch/indexing_status",
        tags=["elasticsearch"],
    )
    async def elasticsearch_indexing_status() -> dict:
        return _indexing_progress.to_dict()

    # CAT = Compact and Aligned Text — flat tabular ES API
    _CAT_COLUMNS = "index,health,status,store.size,creation.date.string"

    async def _build_index_info(
        *,
        client: elasticsearch.Elasticsearch,
        index_name: str,
        verbose: bool = False,
    ) -> dict[str, Any]:
        coros = [
            asyncio.to_thread(
                client.cat.indices,
                index=index_name,
                format="json",
                h=_CAT_COLUMNS,
            ),
            asyncio.to_thread(client.indices.get_mapping, index=index_name),
            asyncio.to_thread(client.count, index=index_name),
        ]
        if verbose:
            coros.append(
                asyncio.to_thread(client.indices.get_settings, index=index_name)
            )
            coros.append(asyncio.to_thread(client.indices.get_alias, index=index_name))

        results = await asyncio.gather(*coros)
        cat_result = results[0]
        mapping_raw = results[1]
        count_result = results[2]

        properties = (
            mapping_raw.get(index_name, {}).get("mappings", {}).get("properties", {})
        )
        field_names = list(properties.keys())
        field_doc_counts = await _get_field_doc_counts(
            client=client,
            index_name=index_name,
            field_names=field_names,
        )
        cat = cat_result[0] if cat_result else {}
        info: dict[str, Any] = {
            "index": index_name,
            "health": cat.get("health"),
            "status": cat.get("status"),
            "store_size": cat.get("store.size"),
            "created_at": cat.get("creation.date.string"),
            "docs_count": count_result.get("count", 0),
            "fields_count": len(field_names),
            "field_doc_counts": field_doc_counts,
        }
        if verbose:
            settings_raw = results[3]
            aliases_raw = results[4]
            info["fields"] = properties
            info["settings"] = settings_raw.get(index_name, {}).get("settings", {})
            info["aliases"] = aliases_raw.get(index_name, {}).get("aliases", {})
        return info

    @app.get(
        "/api/experimental/elasticsearch/indices",
        tags=["elasticsearch"],
    )
    async def elasticsearch_list_indices(
        *,
        include_internal: bool = False,
        verbose: bool = False,
    ) -> list[dict[str, Any]]:
        client = _get_es_client()
        cat_result = await asyncio.to_thread(
            client.cat.indices,
            format="json",
            h="index",
        )
        index_names = [
            idx["index"]
            for idx in cat_result
            if include_internal or not idx["index"].startswith(".")
        ]
        return [
            await _build_index_info(client=client, index_name=name, verbose=verbose)
            for name in index_names
        ]

    @app.get(
        "/api/experimental/elasticsearch/indices/{index_name}",
        tags=["elasticsearch"],
    )
    async def elasticsearch_get_index(
        *, index_name: str, verbose: bool = False
    ) -> dict[str, Any]:
        client = _get_es_client()
        return await _build_index_info(
            client=client, index_name=index_name, verbose=verbose
        )

    @app.delete(
        "/api/admin/experimental/elasticsearch/indices/{index_name}",
        tags=["elasticsearch-admin"],
        dependencies=admin_deps,
    )
    async def elasticsearch_delete_index(*, index_name: str) -> dict:
        client = _get_es_client()
        # ignore=[404]: don't throw if the index was already deleted
        result = await asyncio.to_thread(
            client.indices.delete,
            index=index_name,
            ignore=[404],
        )
        return {
            "index": index_name,
            "deleted": result.get("acknowledged", False),
        }


def _embed_texts(embedding_model: str, texts: list[str]):
    if _embedding_function is None:
        raise RuntimeError("Embedding provider is not configured.")
    return _embedding_function(embedding_model=embedding_model, texts=texts)


def _calculate_hash_digest(text: str) -> str:
    data = text.encode("utf-8")
    data = data.replace(b"\r\n", b"\n")  # Normalizing line endings
    digest = hashlib.sha256(data).hexdigest()
    return digest


def _embed_and_cache_vector(
    *,
    client: elasticsearch.Elasticsearch,
    index: str,
    document_id: str,
    text: str,
    vector_field_name: str,
    cache_index_name: str,
    recreate_embeddings: bool = False,
    refresh: bool | str | None = None,
) -> EmbedStatus:
    """Embed *text*, cache the result, and store the vector on *document_id*.

    Generic core for any embedding vector — callers provide the text to
    embed, the target vector field, and the cache index.
    """
    embedding = None
    text_hash = _calculate_hash_digest(text)

    # When not forcing re-embed: skip if vector exists, or reuse cached embedding
    if not recreate_embeddings:
        # Check 1: vector already on the doc → nothing to do
        try:
            entry: dict = client.get(
                index=index,
                id=document_id,
                source=[vector_field_name],
            ).body
            if entry["_source"].get(vector_field_name):
                logger.debug(
                    f"Skipping doc_id={document_id} field={vector_field_name}: vector already exists"
                )
                return EmbedStatus.SKIPPED
        except elasticsearch.NotFoundError:
            pass  # Document not indexed yet — proceed to embed

        # Check 2: cache hit by sha256(text) → reuse without calling AI Proxy
        try:
            cache_entry: dict = client.get(
                index=cache_index_name,
                id=text_hash,
            ).body
            embedding = cache_entry["_source"][
                ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME
            ]
            logger.debug(
                f"Cache hit for doc_id={document_id} field={vector_field_name}"
            )
        except elasticsearch.NotFoundError:
            pass  # Cache miss — will embed below

    # Cache miss or recreate_embeddings=True → call AI Proxy and update cache
    if embedding is None:
        logger.debug(
            f"Calling AI Proxy for doc_id={document_id} field={vector_field_name}"
        )
        embedding = _embed_texts(
            embedding_model=es_embedding_model_name,
            texts=[text],
        )[0]
        if embedding:
            embedding = embedding[:es_embedding_size]
            client.index(
                index=cache_index_name,
                id=text_hash,
                document={
                    ES_EMBEDDINGS_CACHE_TEXT_PROPERTY_NAME: text,
                    ES_EMBEDDINGS_CACHE_EMBEDDING_PROPERTY_NAME: embedding,
                },
            )
        else:
            logger.warning(
                f"AI Proxy returned empty embedding for doc_id={document_id} field={vector_field_name}"
            )
            return EmbedStatus.ERROR

    # Write the vector to the main doc
    if embedding:
        client.update(
            index=index,
            id=document_id,
            doc={vector_field_name: embedding},
            refresh=refresh,
        )

    return (
        EmbedStatus.CACHE_HIT
        if not recreate_embeddings and embedding
        else EmbedStatus.EMBEDDED
    )


def _build_name_and_description_text(
    *,
    component_spec_dict: dict[str, Any],
) -> str:
    """Build the ``name_and_description`` text from the component spec."""
    name: str | None = component_spec_dict.get("name")
    description: str | None = component_spec_dict.get("description")
    result = name or ""
    if name and description:
        result += "\n\n"
    if description:
        result += description
    return result.strip()


def _elasticsearch_add_component_embedding_vector_to_index(
    *,
    client: elasticsearch.Elasticsearch,
    index: str,
    document_id: str,
    component_spec_dict: dict[str, Any],
    recreate_embeddings: bool = False,
    refresh: bool | str | None = None,
) -> list[tuple[str, EmbedStatus]]:
    """Orchestrate all embedding vectors for a single component.

    Currently produces two vectors:
    1. ``name_and_description_vector`` — from name + description text
    2. ``full_spec_vector`` — from the full component spec JSON

    Returns a list of (vector_field_name, status) tuples.
    """
    name_and_description = _build_name_and_description_text(
        component_spec_dict=component_spec_dict,
    )

    client.update(
        index=index,
        id=document_id,
        doc={"name_and_description": name_and_description},
    )

    statuses: list[tuple[str, EmbedStatus]] = []

    # Each vector is wrapped individually so one failing does not skip the other.
    vectors_to_embed = [
        (
            name_and_description_vector_property_name,
            name_and_description,
            es_embeddings_cache_index_name,
        ),
        (
            full_spec_vector_property_name,
            json.dumps(component_spec_dict),
            full_spec_embeddings_cache_index_name,
        ),
    ]

    for vector_field, text, cache_index in vectors_to_embed:
        try:
            status = _embed_and_cache_vector(
                client=client,
                index=index,
                document_id=document_id,
                text=text,
                vector_field_name=vector_field,
                cache_index_name=cache_index,
                recreate_embeddings=recreate_embeddings,
                refresh=refresh,
            )
            statuses.append((vector_field, status))
        except Exception:
            logger.exception(
                f"Error embedding doc_id={document_id} field={vector_field}"
            )
            statuses.append((vector_field, EmbedStatus.ERROR))

    return statuses
