import pytest
from opentelemetry import trace


@pytest.fixture(autouse=True)
def reset_otel_tracer_provider():
    """Reset the global OTel tracer provider between tests.

    OTel only allows set_tracer_provider to be called once per process.
    We reset the internal guard so each test gets a clean slate.
    """
    yield
    trace._TRACER_PROVIDER_SET_ONCE._done = False
    trace._TRACER_PROVIDER = trace.ProxyTracerProvider()
