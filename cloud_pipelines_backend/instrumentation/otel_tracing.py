"""
OpenTelemetry tracing configuration for FastAPI applications.

This module sets up distributed tracing with OTLP exporter for sending traces
to an OpenTelemetry collector endpoint.
"""

import logging
import os

import fastapi
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc import (
    trace_exporter as otel_grpc_trace_exporter,
)
from opentelemetry.exporter.otlp.proto.http import (
    trace_exporter as otel_http_trace_exporter,
)
from opentelemetry.instrumentation import fastapi as otel_fastapi
from opentelemetry.sdk import resources as otel_resources
from opentelemetry.sdk import trace as otel_trace
from opentelemetry.sdk.trace import export as otel_trace_export

_logger = logging.getLogger(__name__)

_OTEL_PROTOCOL_GRPC = "grpc"
_OTEL_PROTOCOL_HTTP = "http"
_OTEL_PROTOCOLS = (_OTEL_PROTOCOL_GRPC, _OTEL_PROTOCOL_HTTP)


def setup_api_tracing(app: fastapi.FastAPI) -> None:
    """
    Configure OpenTelemetry tracing for a FastAPI application.

    Args:
        app: The FastAPI application instance to instrument

    Environment Variables:
        TANGLE_OTEL_EXPORTER_ENDPOINT: The endpoint URL for the OTLP collector
                 (e.g., "http://localhost:4317")
        TANGLE_ENV: Environment name to include in service name
                 (defaults to "development")
        TANGLE_OTEL_EXPORTER_PROTOCOL: The protocol to use for the OTLP exporter
                 (defaults to "grpc", can be "http")
    """
    otel_endpoint = os.environ.get("TANGLE_OTEL_EXPORTER_ENDPOINT")
    if not otel_endpoint:
        return

    app_env = os.environ.get("TANGLE_ENV", "development")
    otel_protocol = os.environ.get("TANGLE_OTEL_EXPORTER_PROTOCOL", _OTEL_PROTOCOL_GRPC)
    service_name = f"tangle-{app_env}"

    try:
        _logger.info(
            f"Configuring OpenTelemetry tracing, endpoint={otel_endpoint}, protocol={otel_protocol}, service_name={service_name}"
        )

        _validate_otel_config(otel_endpoint, otel_protocol)

        if otel_protocol == _OTEL_PROTOCOL_GRPC:
            otel_exporter = otel_grpc_trace_exporter.OTLPSpanExporter(endpoint=otel_endpoint)
        else:
            otel_exporter = otel_http_trace_exporter.OTLPSpanExporter(endpoint=otel_endpoint)  # type: ignore[assignment]

        resource = otel_resources.Resource(attributes={otel_resources.SERVICE_NAME: service_name})
        tracer_provider = otel_trace.TracerProvider(resource=resource)
        span_processor = otel_trace_export.BatchSpanProcessor(otel_exporter)
        tracer_provider.add_span_processor(span_processor)
        trace.set_tracer_provider(tracer_provider)

        # FastAPI auto-instrumentation docs:
        # https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html
        otel_fastapi.FastAPIInstrumentor.instrument_app(app)

        _logger.info("OpenTelemetry tracing configured successfully.")

    except Exception as e:
        _logger.exception(f"Failed to configure OpenTelemetry tracing: {e}")


def _validate_otel_config(otel_endpoint: str, otel_protocol: str) -> None:
    """Validate OTel configuration. Raises ValueError if invalid."""
    if not otel_endpoint.startswith(("http://", "https://")):
        raise ValueError(
            f"Invalid OTel endpoint format: {otel_endpoint}. Expected format: http://<host>:<port> or https://<host>:<port>"
        )
    if otel_protocol not in _OTEL_PROTOCOLS:
        raise ValueError(
            f"Invalid OTel protocol: {otel_protocol}. Expected values: {_OTEL_PROTOCOL_GRPC}, {_OTEL_PROTOCOL_HTTP}"
        )
