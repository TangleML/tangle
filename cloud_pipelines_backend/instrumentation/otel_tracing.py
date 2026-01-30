"""
OpenTelemetry tracing configuration for FastAPI applications.

This module sets up distributed tracing with OTLP exporter for sending traces
to an OpenTelemetry collector endpoint specified via OTEL_EXPORTER_OTLP_ENDPOINT.
"""

import logging
import os

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter as GRPCSpanExporter,
)
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)


def setup_api_tracing(app: FastAPI) -> None:
    """
    Configure OpenTelemetry tracing for a FastAPI application.

    Args:
        app: The FastAPI application instance to instrument

    Environment Variables:
        OTEL_EXPORTER_OTLP_ENDPOINT: The endpoint URL for the OTLP collector
                                     (e.g., "http://localhost:4317")
                                     If not set, tracing will not be exported.
        APP_ENV: Optional environment name to include in service name
                 (defaults to "development")
    """
    # Get OTLP endpoint from environment variable
    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    if not otlp_endpoint:
        logger.warning(
            "OTEL_EXPORTER_OTLP_ENDPOINT not configured. "
            "Tracing will not be exported. Set the environment variable to enable trace export."
        )
        return

    try:
        # Build service name with environment suffix
        app_env = os.environ.get("APP_ENV", "development")
        service_name = f"tangle-{app_env}"

        # Create a resource identifying this service
        resource = Resource(attributes={SERVICE_NAME: service_name})

        # Create the OTLP exporter
        otlp_exporter = GRPCSpanExporter(
            endpoint=otlp_endpoint,
        )

        # Create and configure the tracer provider
        tracer_provider = TracerProvider(resource=resource)

        # Add a batch span processor to export spans in batches
        # This improves performance by reducing network overhead
        span_processor = BatchSpanProcessor(otlp_exporter)
        tracer_provider.add_span_processor(span_processor)

        # Set the global tracer provider
        trace.set_tracer_provider(tracer_provider)

        # Instrument the FastAPI application
        # This automatically creates spans for all incoming HTTP requests
        FastAPIInstrumentor.instrument_app(app)

        logger.info(
            f"OpenTelemetry tracing configured successfully. "
            f"Service: {service_name}, Endpoint: {otlp_endpoint}"
        )

    except Exception as e:
        logger.error(f"Failed to configure OpenTelemetry tracing: {e}", exc_info=True)
        # Don't raise the exception - we don't want tracing setup failures
        # to prevent the application from starting
