"""
OpenTelemetry tracing configuration.

This module sets up the global tracer provider with an OTLP exporter.
"""

import logging

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc import (
    trace_exporter as otel_grpc_trace_exporter,
)
from opentelemetry.exporter.otlp.proto.http import (
    trace_exporter as otel_http_trace_exporter,
)
from opentelemetry.sdk import resources as otel_resources
from opentelemetry.sdk import trace as otel_trace
from opentelemetry.sdk.trace import export as otel_trace_export

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config

_logger = logging.getLogger(__name__)


def setup(endpoint: str, protocol: str, service_name: str) -> None:
    """
    Configure the global OpenTelemetry tracer provider.

    Args:
        endpoint: The OTLP collector endpoint URL.
        protocol: The exporter protocol ("grpc" or "http").
        service_name: The service name reported to the collector.
    """
    try:
        _logger.info(
            f"Configuring OpenTelemetry tracing, endpoint={endpoint}, "
            f"protocol={protocol}, service_name={service_name}"
        )

        if protocol == config.ExporterProtocol.GRPC:
            otel_exporter = otel_grpc_trace_exporter.OTLPSpanExporter(endpoint=endpoint)
        else:
            otel_exporter = otel_http_trace_exporter.OTLPSpanExporter(endpoint=endpoint)

        resource = otel_resources.Resource(
            attributes={otel_resources.SERVICE_NAME: service_name}
        )
        tracer_provider = otel_trace.TracerProvider(resource=resource)
        span_processor = otel_trace_export.BatchSpanProcessor(otel_exporter)
        tracer_provider.add_span_processor(span_processor)
        trace.set_tracer_provider(tracer_provider)

        _logger.info("OpenTelemetry tracing configured successfully.")
    except Exception as e:
        _logger.exception("Failed to configure OpenTelemetry tracing")
