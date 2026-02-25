"""Tests for the OpenTelemetry providers setup module."""

from unittest import mock

from opentelemetry import trace
from opentelemetry.sdk import trace as otel_sdk_trace

from cloud_pipelines_backend.instrumentation.opentelemetry import providers


class TestProvidersSetup:
    """Tests for providers.setup()."""

    def test_noop_when_no_exporters_configured(self, monkeypatch):
        monkeypatch.delenv("TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", raising=False)

        providers.setup()

        assert not isinstance(
            trace.get_tracer_provider(), otel_sdk_trace.TracerProvider
        )

    def test_configures_tracer_provider(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.delenv("TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL", raising=False)

        providers.setup()

        assert isinstance(trace.get_tracer_provider(), otel_sdk_trace.TracerProvider)

    def test_passes_custom_service_name(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )

        providers.setup(service_name="oasis-orchestrator")

        provider = trace.get_tracer_provider()
        assert provider.resource.attributes["service.name"] == "oasis-orchestrator"

    def test_passes_custom_service_version(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )

        providers.setup(service_name="test-service", service_version="abc123")

        provider = trace.get_tracer_provider()
        assert provider.resource.attributes["service.version"] == "abc123"

    def test_catches_validation_errors(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "bad-endpoint")

        providers.setup()

        assert not isinstance(
            trace.get_tracer_provider(), otel_sdk_trace.TracerProvider
        )

    def test_catches_config_resolution_errors(self):
        with mock.patch(
            "cloud_pipelines_backend.instrumentation.opentelemetry._internal.config.resolve",
            side_effect=RuntimeError("unexpected"),
        ):
            providers.setup()

        assert not isinstance(
            trace.get_tracer_provider(), otel_sdk_trace.TracerProvider
        )
