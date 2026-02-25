"""Tests for the OpenTelemetry tracing module."""

from unittest import mock

from opentelemetry import trace
from opentelemetry.sdk import trace as otel_sdk_trace

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config
from cloud_pipelines_backend.instrumentation.opentelemetry import tracing


class TestTracingSetup:
    """Tests for tracing.setup()."""

    def test_sets_global_tracer_provider_with_grpc(self):
        tracing.setup(
            endpoint="http://localhost:4317",
            protocol=config.ExporterProtocol.GRPC,
            service_name="test-service",
        )

        provider = trace.get_tracer_provider()
        assert isinstance(provider, otel_sdk_trace.TracerProvider)

    def test_sets_global_tracer_provider_with_http(self):
        tracing.setup(
            endpoint="http://localhost:4318",
            protocol=config.ExporterProtocol.HTTP,
            service_name="test-service",
        )

        provider = trace.get_tracer_provider()
        assert isinstance(provider, otel_sdk_trace.TracerProvider)

    def test_service_name_is_set_on_resource(self):
        tracing.setup(
            endpoint="http://localhost:4317",
            protocol=config.ExporterProtocol.GRPC,
            service_name="my-service",
        )

        provider = trace.get_tracer_provider()
        assert provider.resource.attributes["service.name"] == "my-service"

    def test_catches_exporter_exception(self):
        with mock.patch(
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter",
            side_effect=RuntimeError("connection failed"),
        ):
            tracing.setup(
                endpoint="http://localhost:4317",
                protocol=config.ExporterProtocol.GRPC,
                service_name="test-service",
            )

        assert not isinstance(
            trace.get_tracer_provider(), otel_sdk_trace.TracerProvider
        )
