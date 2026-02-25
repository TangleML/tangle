"""
OpenTelemetry instrumentation public API.

Usage::

    from cloud_pipelines_backend.instrumentation import opentelemetry as otel

    otel.setup_providers()
    otel.instrument_fastapi(app)
"""

from cloud_pipelines_backend.instrumentation.opentelemetry import auto_instrumentation
from cloud_pipelines_backend.instrumentation.opentelemetry import metrics
from cloud_pipelines_backend.instrumentation.opentelemetry import providers
from cloud_pipelines_backend.instrumentation.opentelemetry import tracing

instrument_fastapi = auto_instrumentation.instrument_fastapi
setup_metrics = metrics.setup
setup_providers = providers.setup
setup_tracing = tracing.setup

__all__ = [
    "instrument_fastapi",
    "setup_metrics",
    "setup_providers",
    "setup_tracing",
]
