import datetime
import logging

import elasticsearch
import fastapi
import fastapi.responses
from starlette import status as http_status

from cloud_pipelines_backend._compat import UTC

logger = logging.getLogger(__name__)


def _extract_reason(*, exc: elasticsearch.ApiError) -> str:
    """Extract the human-readable reason from an ES error response body."""
    if isinstance(exc.body, dict) and "error" in exc.body:
        error = exc.body["error"]
        if isinstance(error, dict):
            root_causes = error.get("root_cause", [])
            if root_causes:
                return root_causes[0].get("reason", exc.message)
        else:
            return str(error)
    return exc.message


def register_elasticsearch_exception_handlers(*, app: fastapi.FastAPI) -> None:
    """Register global handlers that map ES exceptions to HTTP responses.

    elasticsearch.ApiError covers all HTTP-level errors (400, 401, 403, 404).
    elasticsearch.TransportError covers connection/network errors (timeouts, SSL).
    """

    @app.exception_handler(elasticsearch.ApiError)
    async def handle_es_api_error(
        request: fastapi.Request,
        exc: elasticsearch.ApiError,
    ) -> fastapi.responses.JSONResponse:
        timestamp = datetime.datetime.now(UTC).isoformat()
        logger.exception(f"Elasticsearch API error on {request.url} at {timestamp}")
        return fastapi.responses.JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.message,
                "reason": _extract_reason(exc=exc),
                "timestamp": timestamp,
            },
        )

    @app.exception_handler(elasticsearch.TransportError)
    async def handle_es_transport_error(
        request: fastapi.Request,
        exc: elasticsearch.TransportError,
    ) -> fastapi.responses.JSONResponse:
        timestamp = datetime.datetime.now(UTC).isoformat()
        logger.exception(
            f"Elasticsearch transport error on {request.url} at {timestamp}"
        )
        return fastapi.responses.JSONResponse(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "transport_error",
                "reason": str(exc),
                "timestamp": timestamp,
            },
        )
