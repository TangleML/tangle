"""
OpenTelemetry auto-instrumentation for FastAPI applications.

Instrumentation is only activated when at least one OpenTelemetry SDK
provider (traces, metrics, or logs) has been configured globally.
"""

import fastapi
import logging

from opentelemetry.instrumentation import fastapi as otel_fastapi

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import providers

_logger = logging.getLogger(__name__)


def instrument_fastapi(app: fastapi.FastAPI) -> None:
    """
    Apply OpenTelemetry auto-instrumentation to a FastAPI application.

    No-op if no OpenTelemetry SDK providers have been configured globally,
    since there would be no backend to receive the telemetry data.

    See: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html

    Args:
        app: The FastAPI application instance to instrument.
    """
    if not providers.has_configured_providers():
        _logger.debug(
            "Skipping FastAPI auto-instrumentation: no OpenTelemetry providers configured"
        )
        return

    try:
        otel_fastapi.FastAPIInstrumentor.instrument_app(app)
        _logger.info("FastAPI auto-instrumentation enabled")
    except Exception as e:
        _logger.exception("Failed to apply FastAPI auto-instrumentation")
