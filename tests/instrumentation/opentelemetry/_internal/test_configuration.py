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


class TestResolve:
    """Tests for configuration.resolve()."""

    def test_returns_none_when_endpoint_not_set(self, monkeypatch):
        monkeypatch.delenv("TANGLE_OTEL_EXPORTER_ENDPOINT", raising=False)

        result = configuration.resolve()

        assert result is None

    def test_returns_config_with_defaults(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")
        monkeypatch.delenv("TANGLE_OTEL_EXPORTER_PROTOCOL", raising=False)
        monkeypatch.delenv("TANGLE_ENV", raising=False)
        monkeypatch.delenv("TANGLE_SERVICE_VERSION", raising=False)

        result = configuration.resolve()

        assert result is not None
        assert result.endpoint == "http://localhost:4317"
        assert result.protocol == configuration.ExporterProtocol.GRPC
        assert result.service_name == "tangle-unknown"
        assert result.service_version == "unknown"

    def test_uses_custom_service_name(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")

        result = configuration.resolve(service_name="my-api")

        assert result.service_name == "my-api"

    def test_service_name_includes_tangle_env(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")
        monkeypatch.setenv("TANGLE_ENV", "production")

        result = configuration.resolve()

        assert result.service_name == "tangle-production"

    def test_respects_http_protocol(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4318")
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_PROTOCOL", "http")

        result = configuration.resolve()

        assert result.protocol == configuration.ExporterProtocol.HTTP

    def test_respects_grpc_protocol(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_PROTOCOL", "grpc")

        result = configuration.resolve()

        assert result.protocol == configuration.ExporterProtocol.GRPC

    def test_raises_on_invalid_endpoint_format(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "localhost:4317")

        with pytest.raises(ValueError, match="Invalid OTel endpoint format"):
            configuration.resolve()

    def test_raises_on_invalid_protocol(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_PROTOCOL", "websocket")

        with pytest.raises(ValueError, match="Invalid OTel protocol"):
            configuration.resolve()

    def test_accepts_https_endpoint(self, monkeypatch):
        monkeypatch.setenv(
            "TANGLE_OTEL_EXPORTER_ENDPOINT", "https://collector.example.com:4317"
        )

        result = configuration.resolve()

        assert result.endpoint == "https://collector.example.com:4317"

    def test_uses_custom_service_version(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")

        result = configuration.resolve(service_version="abc123")

        assert result.service_version == "abc123"

    def test_service_version_from_env(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")
        monkeypatch.setenv("TANGLE_SERVICE_VERSION", "def456")

        result = configuration.resolve()

        assert result.service_version == "def456"

    def test_service_version_defaults_to_unknown(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")
        monkeypatch.delenv("TANGLE_SERVICE_VERSION", raising=False)

        result = configuration.resolve()

        assert result.service_version == "unknown"

    def test_config_is_frozen(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")

        result = configuration.resolve()

        with pytest.raises(AttributeError):
            result.endpoint = "http://other:4317"

    def test_config_requires_keyword_arguments(self, monkeypatch):
        monkeypatch.setenv("TANGLE_OTEL_EXPORTER_ENDPOINT", "http://localhost:4317")

        result = configuration.resolve()

        with pytest.raises(TypeError):
            configuration.OtelConfig(
                result.endpoint, result.protocol, result.service_name
            )
