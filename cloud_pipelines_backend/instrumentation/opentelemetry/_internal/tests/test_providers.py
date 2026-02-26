"""Tests for the OpenTelemetry provider state checks."""

from opentelemetry import trace
from opentelemetry.sdk import trace as otel_sdk_trace

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import providers


class TestHasConfiguredProviders:
    """Tests for providers.has_configured_providers()."""

    def test_returns_false_with_default_provider(self):
        assert providers.has_configured_providers() is False

    def test_returns_true_with_sdk_tracer_provider(self):
        trace.set_tracer_provider(otel_sdk_trace.TracerProvider())

        assert providers.has_configured_providers() is True
