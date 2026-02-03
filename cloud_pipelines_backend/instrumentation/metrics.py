"""
OpenTelemetry metrics configuration and HTTP request metrics middleware.

This module sets up OpenTelemetry metrics and provides middleware to track
HTTP request counts and durations.
"""

import logging
import os
import time
from typing import Callable

from fastapi import FastAPI, Request, Response
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter as GRPCMetricExporter,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

# Global meter instance
_meter = None


def setup_metrics(app: FastAPI) -> None:
    """
    Configure OpenTelemetry metrics for a FastAPI application.

    Args:
        app: The FastAPI application instance

    Environment Variables:
        OTEL_EXPORTER_OTLP_ENDPOINT: The endpoint URL for the OTLP collector
                                     (e.g., "http://localhost:4317")
                                     If not set, metrics will not be exported.
        APP_ENV: Optional environment name to include in service name
                 (defaults to "development")
    """
    global _meter

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

    if not otlp_endpoint:
        logger.warning(
            "OTEL_EXPORTER_OTLP_ENDPOINT not configured. "
            "Metrics will not be exported. Set the environment variable to enable metric export."
        )
        return

    try:
        app_env = os.environ.get("APP_ENV", "development")
        service_name = f"tangle-{app_env}"

        resource = Resource(attributes={SERVICE_NAME: service_name})

        # Create OTLP metric exporter
        metric_exporter = GRPCMetricExporter(endpoint=otlp_endpoint)

        # Create metric reader with 60 second export interval
        metric_reader = PeriodicExportingMetricReader(
            metric_exporter,
            export_interval_millis=60000,
        )

        # Create and set the meter provider
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )
        metrics.set_meter_provider(meter_provider)

        # Get meter for this module
        _meter = metrics.get_meter(__name__)

        logger.info(
            f"OpenTelemetry metrics configured successfully. "
            f"Service: {service_name}, Endpoint: {otlp_endpoint}"
        )

    except Exception as e:
        logger.error(f"Failed to configure OpenTelemetry metrics: {e}", exc_info=True)


def get_meter():
    """Get the global meter instance."""
    return _meter


class HTTPMetricsMiddleware(BaseHTTPMiddleware):
    """
    Middleware to track HTTP request metrics.

    Tracks:
    - http_requests_total: Counter of requests by method, endpoint, status_code
    - http_request_duration_seconds: Histogram of request durations by method, endpoint
    """

    def __init__(self, app: FastAPI):
        super().__init__(app)
        meter = get_meter()

        if meter is None:
            logger.warning(
                "Metrics not configured, HTTPMetricsMiddleware will not track metrics"
            )
            self._requests_counter = None
            self._duration_histogram = None
            return

        # Create metrics
        self._requests_counter = meter.create_counter(
            name="http_requests_total",
            description="Total number of HTTP requests",
            unit="1",
        )

        self._duration_histogram = meter.create_histogram(
            name="http_request_duration_seconds",
            description="Duration of HTTP requests in seconds",
            unit="s",
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if self._requests_counter is None:
            # Metrics not configured, skip tracking
            return await call_next(request)

        start_time = time.time()

        # Execute the request
        response = await call_next(request)

        # Calculate duration
        duration = time.time() - start_time

        # Extract endpoint from route
        endpoint = request.url.path
        if request.scope.get("route"):
            endpoint = request.scope["route"].path

        # Record metrics
        request_labels = {
            "method": request.method,
            "endpoint": endpoint,
            "status_code": str(response.status_code),
        }

        duration_labels = {
            "method": request.method,
            "endpoint": endpoint,
        }

        self._requests_counter.add(1, request_labels)
        self._duration_histogram.record(duration, duration_labels)

        return response
