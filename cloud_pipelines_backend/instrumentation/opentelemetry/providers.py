"""
OpenTelemetry provider setup.

Provides entry points to configure OpenTelemetry providers.
"""

import logging

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config
from cloud_pipelines_backend.instrumentation.opentelemetry import tracing

_logger = logging.getLogger(__name__)


def setup(service_name: str | None = None) -> None:
    """
    Configure global OpenTelemetry providers (traces, metrics).

    No-op if TANGLE_OTEL_EXPORTER_ENDPOINT is not set.

    Use this for non-FastAPI entrypoints (e.g. orchestrators, workers) that
    need telemetry but have no ASGI app to auto-instrument.

    Args:
        service_name: Override the default service name reported to the collector.
    """
    try:
        otel_config = config.resolve(service_name=service_name)
    except Exception as e:
        _logger.exception("Failed to resolve OpenTelemetry configuration")
        return

    if otel_config is None:
        return

    tracing.setup(
        endpoint=otel_config.endpoint,
        protocol=otel_config.protocol,
        service_name=otel_config.service_name,
    )

    # TODO: Setup metrics provider once it's available
