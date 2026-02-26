"""
OpenTelemetry provider setup.

Provides entry points to configure OpenTelemetry providers.
"""

import logging

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config
from cloud_pipelines_backend.instrumentation.opentelemetry import metrics
from cloud_pipelines_backend.instrumentation.opentelemetry import tracing

_logger = logging.getLogger(__name__)


def setup(
    service_name: str | None = None,
    service_version: str | None = None,
) -> None:
    """
    Configure global OpenTelemetry providers (traces, metrics).

    No-op if no signal-specific exporter endpoints are set.

    Use this for non-FastAPI entrypoints (e.g. orchestrators, workers) that
    need telemetry but have no ASGI app to auto-instrument.

    Args:
        service_name: Override the default service name reported to the collector.
        service_version: Override the default service version (e.g. git revision).
    """
    try:
        otel_config = config.resolve(
            service_name=service_name,
            service_version=service_version,
        )
    except Exception as e:
        _logger.exception("Failed to resolve OpenTelemetry configuration")
        return

    if otel_config is None:
        return

    if otel_config.trace_exporter:
        tracing.setup(
            endpoint=otel_config.trace_exporter.endpoint,
            protocol=otel_config.trace_exporter.protocol,
            service_name=otel_config.service_name,
            service_version=otel_config.service_version,
        )

    if otel_config.metrics:
        metrics.setup(
            endpoint=otel_config.metrics.exporter.endpoint,
            protocol=otel_config.metrics.exporter.protocol,
            service_name=otel_config.service_name,
            service_version=otel_config.service_version,
            temporality=otel_config.metrics.temporality,
        )
