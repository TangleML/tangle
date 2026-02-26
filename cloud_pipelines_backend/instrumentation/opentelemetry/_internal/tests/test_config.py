"""Tests for the OpenTelemetry config module."""

import pytest

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config


class TestExporterProtocol:
    """Tests for config.ExporterProtocol enum."""

    def test_grpc_value(self):
        assert config.ExporterProtocol.GRPC == "grpc"

    def test_http_value(self):
        assert config.ExporterProtocol.HTTP == "http"

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            config.ExporterProtocol("websocket")


class TestResolve:
    """Tests for config.resolve()."""

    def test_returns_none_when_no_exporters_configured(self, monkeypatch):
        monkeypatch.delenv("TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", raising=False)

        result = config.resolve()

        assert result is None

    def test_returns_config_with_defaults(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.delenv("TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL", raising=False)
        monkeypatch.delenv("TANGLE_ENV", raising=False)
        monkeypatch.delenv("TANGLE_SERVICE_VERSION", raising=False)

        result = config.resolve()

        assert result is not None
        assert result.trace_exporter.endpoint == "http://localhost:4317"
        assert result.trace_exporter.protocol == config.ExporterProtocol.GRPC
        assert result.service_name == "tangle-unknown"
        assert result.service_version == "unknown"

    def test_uses_custom_service_name(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )

        result = config.resolve(service_name="oasis-api")

        assert result.service_name == "oasis-api"

    def test_service_name_includes_tangle_env(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_ENV", "production")

        result = config.resolve()

        assert result.service_name == "tangle-production"

    def test_respects_http_protocol(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4318"
        )
        monkeypatch.setenv("TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL", "http")

        result = config.resolve()

        assert result.trace_exporter.protocol == config.ExporterProtocol.HTTP

    def test_respects_grpc_protocol(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL", "grpc")

        result = config.resolve()

        assert result.trace_exporter.protocol == config.ExporterProtocol.GRPC

    def test_raises_on_invalid_endpoint_format(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "localhost:4317")

        with pytest.raises(ValueError, match="Invalid OTel endpoint format"):
            config.resolve()

    def test_raises_on_invalid_protocol(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL", "websocket")

        with pytest.raises(ValueError, match="Invalid OTel protocol"):
            config.resolve()

    def test_accepts_https_endpoint(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "https://collector.example.com:4317"
        )

        result = config.resolve()

        assert result.trace_exporter.endpoint == "https://collector.example.com:4317"

    def test_uses_custom_service_version(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )

        result = config.resolve(service_version="abc123")

        assert result.service_version == "abc123"

    def test_service_version_from_env(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.setenv("TANGLE_SERVICE_VERSION", "def456")

        result = config.resolve()

        assert result.service_version == "def456"

    def test_service_version_defaults_to_unknown(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )
        monkeypatch.delenv("TANGLE_SERVICE_VERSION", raising=False)

        result = config.resolve()

        assert result.service_version == "unknown"

    def test_config_is_frozen(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )

        result = config.resolve()

        with pytest.raises(AttributeError):
            result.service_name = "other"

    def test_exporter_config_is_frozen(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT", "http://localhost:4317"
        )

        result = config.resolve()

        with pytest.raises(AttributeError):
            result.trace_exporter.endpoint = "http://other:4317"

    def test_config_requires_keyword_arguments(self):
        with pytest.raises(TypeError):
            config.OtelConfig("my-service", "unknown")

    def test_exporter_config_requires_keyword_arguments(self):
        with pytest.raises(TypeError):
            config.ExporterConfig("http://localhost:4317", "grpc")
