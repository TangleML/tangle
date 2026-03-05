"""Tests for the OpenTelemetry configuration module."""

import pytest

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import (
    configuration,
)


class TestExporterProtocol:
    """Tests for configuration.ExporterProtocol enum."""

    def test_grpc_value(self):
        assert configuration.ExporterProtocol.GRPC == "grpc"

    def test_http_value(self):
        assert configuration.ExporterProtocol.HTTP == "http"

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            configuration.ExporterProtocol("websocket")


class TestAggregationTemporality:
    """Tests for AggregationTemporality enum."""

    def test_delta_value(self):
        assert configuration.AggregationTemporality.DELTA == "delta"

    def test_cumulative_value(self):
        assert configuration.AggregationTemporality.CUMULATIVE == "cumulative"

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            configuration.AggregationTemporality("invalid")


class TestResolve:
    """Tests for configuration.resolve()."""

    def test_returns_none_when_no_exporters_configured(self, monkeypatch):
        monkeypatch.delenv(configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, raising=False)
        monkeypatch.delenv(configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, raising=False)

        result = configuration.resolve()

        assert result is None

    def test_returns_config_with_defaults(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.delenv(configuration.EnvVar.TRACE_EXPORTER_PROTOCOL, raising=False)
        monkeypatch.delenv(configuration.EnvVar.ENV, raising=False)
        monkeypatch.delenv(configuration.EnvVar.SERVICE_VERSION, raising=False)

        result = configuration.resolve()

        assert result is not None
        assert result.trace_exporter.endpoint == "http://localhost:4317"
        assert result.trace_exporter.protocol == configuration.ExporterProtocol.GRPC
        assert result.service_name == "tangle-unknown"
        assert result.service_version == "unknown"

    def test_uses_custom_service_name(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve(service_name="my-api")

        assert result.service_name == "my-api"

    def test_service_name_includes_tangle_env(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv(configuration.EnvVar.ENV, "production")

        result = configuration.resolve()

        assert result.service_name == "tangle-production"

    def test_respects_http_protocol(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4318"
        )
        monkeypatch.setenv(configuration.EnvVar.TRACE_EXPORTER_PROTOCOL, "http")

        result = configuration.resolve()

        assert result.trace_exporter.protocol == configuration.ExporterProtocol.HTTP

    def test_respects_grpc_protocol(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv(configuration.EnvVar.TRACE_EXPORTER_PROTOCOL, "grpc")

        result = configuration.resolve()

        assert result.trace_exporter.protocol == configuration.ExporterProtocol.GRPC

    def test_raises_on_invalid_endpoint_format(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "localhost:4317"
        )

        with pytest.raises(ValueError, match="Invalid OTel endpoint format"):
            configuration.resolve()

    def test_raises_on_invalid_protocol(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv(configuration.EnvVar.TRACE_EXPORTER_PROTOCOL, "websocket")

        with pytest.raises(ValueError, match="Invalid OTel protocol"):
            configuration.resolve()

    def test_accepts_https_endpoint(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT,
            "https://collector.example.com:4317",
        )

        result = configuration.resolve()

        assert result.trace_exporter.endpoint == "https://collector.example.com:4317"

    def test_uses_custom_service_version(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve(service_version="abc123")

        assert result.service_version == "abc123"

    def test_service_version_from_env(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv(configuration.EnvVar.SERVICE_VERSION, "def456")

        result = configuration.resolve()

        assert result.service_version == "def456"

    def test_service_version_defaults_to_unknown(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.delenv(configuration.EnvVar.SERVICE_VERSION, raising=False)

        result = configuration.resolve()

        assert result.service_version == "unknown"

    def test_config_is_frozen(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve()

        with pytest.raises(AttributeError):
            result.service_name = "other"

    def test_exporter_config_is_frozen(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve()

        with pytest.raises(AttributeError):
            result.trace_exporter.endpoint = "http://other:4317"

    def test_config_requires_keyword_arguments(self):
        with pytest.raises(TypeError):
            configuration.OtelConfig("my-service", "unknown")

    def test_exporter_config_requires_keyword_arguments(self):
        with pytest.raises(TypeError):
            configuration.ExporterConfig("http://localhost:4317", "grpc")


class TestMetricsExporterResolve:
    """Tests for metrics exporter resolution in configuration.resolve()."""

    def test_metrics_none_when_metric_endpoint_not_set(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.delenv(configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, raising=False)

        result = configuration.resolve()

        assert result is not None
        assert result.metrics is None

    def test_resolves_metrics_exporter(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve()

        assert result.metrics is not None
        assert result.metrics.exporter.endpoint == "http://localhost:4317"
        assert result.metrics.exporter.protocol == configuration.ExporterProtocol.GRPC

    def test_resolves_metrics_http_protocol(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4318"
        )
        monkeypatch.setenv(configuration.EnvVar.METRIC_EXPORTER_PROTOCOL, "http")

        result = configuration.resolve()

        assert result.metrics.exporter.protocol == configuration.ExporterProtocol.HTTP

    def test_returns_config_with_only_metrics(self, monkeypatch):
        monkeypatch.delenv(configuration.EnvVar.TRACE_EXPORTER_ENDPOINT, raising=False)
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve()

        assert result is not None
        assert result.trace_exporter is None
        assert result.metrics is not None

    def test_raises_on_invalid_metrics_endpoint(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "bad-endpoint"
        )

        with pytest.raises(ValueError, match="Invalid OTel endpoint format"):
            configuration.resolve()

    def test_raises_on_invalid_metrics_protocol(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv(configuration.EnvVar.METRIC_EXPORTER_PROTOCOL, "websocket")

        with pytest.raises(ValueError, match="Invalid OTel protocol"):
            configuration.resolve()


class TestMetricsTemporalityResolve:
    """Tests for metrics temporality env var resolution."""

    def test_defaults(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )

        result = configuration.resolve()
        t = result.metrics.temporality

        assert t.counter == configuration.AggregationTemporality.DELTA
        assert t.observable_counter == configuration.AggregationTemporality.DELTA
        assert t.up_down_counter == configuration.AggregationTemporality.CUMULATIVE
        assert (
            t.observable_up_down_counter
            == configuration.AggregationTemporality.CUMULATIVE
        )
        assert t.histogram == configuration.AggregationTemporality.DELTA

    def test_override_counter_temporality(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_COUNTER", "cumulative")

        result = configuration.resolve()

        assert (
            result.metrics.temporality.counter
            == configuration.AggregationTemporality.CUMULATIVE
        )

    def test_override_histogram_temporality(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_HISTOGRAM", "cumulative")

        result = configuration.resolve()

        assert (
            result.metrics.temporality.histogram
            == configuration.AggregationTemporality.CUMULATIVE
        )

    def test_override_all_temporalities(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_COUNTER", "cumulative")
        monkeypatch.setenv(
            "TANGLE_OTEL_METRICS_TEMPORALITY_OBSERVABLE_COUNTER", "cumulative"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_UP_DOWN_COUNTER", "delta")
        monkeypatch.setenv(
            "TANGLE_OTEL_METRICS_TEMPORALITY_OBSERVABLE_UP_DOWN_COUNTER", "delta"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_HISTOGRAM", "cumulative")

        result = configuration.resolve()
        t = result.metrics.temporality

        assert t.counter == configuration.AggregationTemporality.CUMULATIVE
        assert t.observable_counter == configuration.AggregationTemporality.CUMULATIVE
        assert t.up_down_counter == configuration.AggregationTemporality.DELTA
        assert (
            t.observable_up_down_counter == configuration.AggregationTemporality.DELTA
        )
        assert t.histogram == configuration.AggregationTemporality.CUMULATIVE

    def test_raises_on_invalid_temporality(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_COUNTER", "invalid")

        with pytest.raises(ValueError, match="Invalid OTel metrics temporality"):
            configuration.resolve()

    def test_temporality_case_insensitive(self, monkeypatch):
        monkeypatch.setenv(
            configuration.EnvVar.METRIC_EXPORTER_ENDPOINT, "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_COUNTER", "DELTA")
        monkeypatch.setenv("TANGLE_OTEL_METRICS_TEMPORALITY_HISTOGRAM", "Cumulative")

        result = configuration.resolve()

        assert (
            result.metrics.temporality.counter
            == configuration.AggregationTemporality.DELTA
        )
        assert (
            result.metrics.temporality.histogram
            == configuration.AggregationTemporality.CUMULATIVE
        )
