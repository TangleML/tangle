"""
OpenTelemetry provider state checks.

Queries the global OpenTelemetry SDK state to determine which
providers have been configured.
"""

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk import trace as otel_sdk_trace


def has_configured_providers() -> bool:
    """Check whether any OpenTelemetry SDK providers have been configured globally.

    Logs provider is omitted while the OpenTelemetry Logs API remains experimental.
    """
    return isinstance(
        trace.get_tracer_provider(), otel_sdk_trace.TracerProvider
    ) or isinstance(otel_metrics.get_meter_provider(), otel_sdk_metrics.MeterProvider)
