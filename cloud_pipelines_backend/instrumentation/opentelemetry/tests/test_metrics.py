"""Tests for the OpenTelemetry metrics module."""

from unittest import mock

from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk import metrics as otel_sdk_metrics

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config
from cloud_pipelines_backend.instrumentation.opentelemetry import metrics


class TestMetricsSetup:
    """Tests for metrics.setup()."""

    def test_sets_global_meter_provider_with_grpc(self):
        metrics.setup(
            endpoint="http://localhost:4317",
            protocol=config.ExporterProtocol.GRPC,
            service_name="test-service",
        )

        provider = otel_metrics.get_meter_provider()
        assert isinstance(provider, otel_sdk_metrics.MeterProvider)

    def test_sets_global_meter_provider_with_http(self):
        metrics.setup(
            endpoint="http://localhost:4318",
            protocol=config.ExporterProtocol.HTTP,
            service_name="test-service",
        )

        provider = otel_metrics.get_meter_provider()
        assert isinstance(provider, otel_sdk_metrics.MeterProvider)

    def test_service_name_is_set_on_resource(self):
        metrics.setup(
            endpoint="http://localhost:4317",
            protocol=config.ExporterProtocol.GRPC,
            service_name="my-service",
        )

        provider = otel_metrics.get_meter_provider()
        assert provider._sdk_config.resource.attributes["service.name"] == "my-service"

    def test_service_version_is_set_on_resource(self):
        metrics.setup(
            endpoint="http://localhost:4317",
            protocol=config.ExporterProtocol.GRPC,
            service_name="my-service",
            service_version="abc123",
        )

        provider = otel_metrics.get_meter_provider()
        assert provider._sdk_config.resource.attributes["service.version"] == "abc123"

    def test_service_version_omitted_when_none(self):
        metrics.setup(
            endpoint="http://localhost:4317",
            protocol=config.ExporterProtocol.GRPC,
            service_name="my-service",
        )

        provider = otel_metrics.get_meter_provider()
        assert "service.version" not in provider._sdk_config.resource.attributes

    def test_catches_exporter_exception(self):
        with mock.patch(
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter.OTLPMetricExporter",
            side_effect=RuntimeError("connection failed"),
        ):
            metrics.setup(
                endpoint="http://localhost:4317",
                protocol=config.ExporterProtocol.GRPC,
                service_name="test-service",
            )

        assert not isinstance(
            otel_metrics.get_meter_provider(), otel_sdk_metrics.MeterProvider
        )
