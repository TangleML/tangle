"""
OpenTelemetry instrumentation public API.

Usage::

    from cloud_pipelines_backend.instrumentation import opentelemetry as otel

    otel.setup_providers()
    otel.instrument_fastapi(app)
"""

from . import auto_instrumentation
from . import metrics
from . import providers
from . import tracing

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
