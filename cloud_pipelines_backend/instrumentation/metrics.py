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


# Pipeline Run Metrics
_pipeline_runs_counter = None
_pipeline_run_duration_histogram = None


def get_pipeline_metrics():
    """Get or create pipeline run metrics."""
    global _pipeline_runs_counter, _pipeline_run_duration_histogram

    meter = get_meter()
    if meter is None:
        return None, None

    if _pipeline_runs_counter is None:
        _pipeline_runs_counter = meter.create_counter(
            name="pipeline_runs_total",
            description="Total number of pipeline runs by status",
            unit="1",
        )

    if _pipeline_run_duration_histogram is None:
        _pipeline_run_duration_histogram = meter.create_histogram(
            name="pipeline_run_duration_seconds",
            description="Duration of pipeline runs in seconds",
            unit="s",
        )

    return _pipeline_runs_counter, _pipeline_run_duration_histogram


def track_pipeline_created(created_by: str | None = None):
    """Track pipeline run creation."""
    counter, _ = get_pipeline_metrics()
    if counter:
        counter.add(
            1,
            {
                "status": "running",
                "created_by": created_by or "unknown",
            },
        )


def track_pipeline_completed(
    status: str,
    created_by: str | None = None,
    duration_seconds: float | None = None,
):
    """
    Track pipeline run completion.

    Args:
        status: Terminal status (succeeded/failed/cancelled)
        created_by: Username who created the pipeline
        duration_seconds: Total pipeline duration from creation to completion
    """
    counter, histogram = get_pipeline_metrics()

    if counter:
        counter.add(
            1,
            {
                "status": status,
                "created_by": created_by or "unknown",
            },
        )

    if histogram and duration_seconds is not None:
        histogram.record(
            duration_seconds,
            {
                "status": status,
            },
        )


# Orchestrator Queue Metrics
_orchestrator_processing_errors_counter = None
_orchestrator_executions_processed_counter = None


def get_orchestrator_queue_metrics():
    """Get or create orchestrator queue metrics."""
    global _orchestrator_processing_errors_counter, _orchestrator_executions_processed_counter

    meter = get_meter()
    if meter is None:
        return None, None

    if _orchestrator_processing_errors_counter is None:
        _orchestrator_processing_errors_counter = meter.create_counter(
            name="orchestrator_queue_processing_errors_total",
            description="Total number of orchestrator queue processing errors",
            unit="1",
        )

    if _orchestrator_executions_processed_counter is None:
        _orchestrator_executions_processed_counter = meter.create_counter(
            name="orchestrator_executions_processed_total",
            description="Total number of executions processed by orchestrator queues",
            unit="1",
        )

    return (
        _orchestrator_processing_errors_counter,
        _orchestrator_executions_processed_counter,
    )


def track_queue_processing_error(queue_type: str):
    """Track orchestrator queue processing error."""
    error_counter, _ = get_orchestrator_queue_metrics()
    if error_counter:
        error_counter.add(1, {"queue_type": queue_type})


def track_executions_processed(queue_type: str, found_work: bool):
    """
    Track executions processed by orchestrator queue.

    Args:
        queue_type: Type of queue (queued/running)
        found_work: Whether the queue found work to process
    """
    _, processed_counter = get_orchestrator_queue_metrics()
    if processed_counter:
        processed_counter.add(
            1,
            {
                "queue_type": queue_type,
                "found_work": str(found_work).lower(),
            },
        )


# Execution Node Metrics
_execution_nodes_counter = None
_execution_node_duration_histogram = None
_execution_cache_hits_counter = None


def get_execution_node_metrics():
    """Get or create execution node metrics."""
    global _execution_nodes_counter, _execution_node_duration_histogram, _execution_cache_hits_counter

    meter = get_meter()
    if meter is None:
        return None, None, None

    if _execution_nodes_counter is None:
        _execution_nodes_counter = meter.create_counter(
            name="execution_nodes_total",
            description="Total number of execution nodes by terminal status",
            unit="1",
        )

    if _execution_node_duration_histogram is None:
        _execution_node_duration_histogram = meter.create_histogram(
            name="execution_node_duration_seconds",
            description="Duration of execution nodes in seconds",
            unit="s",
        )

    if _execution_cache_hits_counter is None:
        _execution_cache_hits_counter = meter.create_counter(
            name="execution_cache_hits_total",
            description="Total number of execution cache hits",
            unit="1",
        )

    return (
        _execution_nodes_counter,
        _execution_node_duration_histogram,
        _execution_cache_hits_counter,
    )


def track_execution_completed(status: str, duration_seconds: float | None = None):
    """
    Track execution node completion.

    Args:
        status: Terminal status (succeeded/failed/skipped/system_error/cancelled)
        duration_seconds: Total execution duration from creation to terminal state
    """
    counter, histogram, _ = get_execution_node_metrics()

    if counter:
        counter.add(1, {"status": status})

    if histogram and duration_seconds is not None:
        histogram.record(duration_seconds, {"status": status})


def track_cache_hit():
    """Track execution cache hit."""
    _, _, cache_counter = get_execution_node_metrics()
    if cache_counter:
        cache_counter.add(1)


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
