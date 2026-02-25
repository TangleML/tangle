import pytest
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace


@pytest.fixture(autouse=True)
def reset_otel_providers():
    """Reset global OTel providers between tests.

    OTel only allows set_tracer_provider / set_meter_provider to be called
    once per process.  We reset the internal guards so each test gets a
    clean slate.
    """
    yield
    trace._TRACER_PROVIDER_SET_ONCE._done = False
    trace._TRACER_PROVIDER = trace.ProxyTracerProvider()
    otel_metrics._internal._METER_PROVIDER_SET_ONCE._done = False
    otel_metrics._internal._METER_PROVIDER = None
