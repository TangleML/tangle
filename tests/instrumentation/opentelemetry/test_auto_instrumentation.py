"""Tests for the OpenTelemetry auto-instrumentation module."""

from unittest import mock

import fastapi
from opentelemetry import trace
from opentelemetry.sdk import trace as otel_sdk_trace

from cloud_pipelines_backend.instrumentation.opentelemetry import auto_instrumentation


class TestInstrumentFastapi:
    """Tests for auto_instrumentation.instrument_fastapi()."""

    def test_skips_when_no_providers_configured(self):
        app = fastapi.FastAPI()

        with (
            mock.patch(
                "cloud_pipelines_backend.instrumentation.opentelemetry._internal.providers.has_configured_providers",
                return_value=False,
            ) as mock_check,
            mock.patch(
                "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor.instrument_app"
            ) as mock_instrument,
        ):
            auto_instrumentation.instrument_fastapi(app)

            mock_check.assert_called_once()
            mock_instrument.assert_not_called()

    def test_instruments_when_providers_configured(self):
        app = fastapi.FastAPI()

        with (
            mock.patch(
                "cloud_pipelines_backend.instrumentation.opentelemetry._internal.providers.has_configured_providers",
                return_value=True,
            ),
            mock.patch(
                "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor.instrument_app"
            ) as mock_instrument,
        ):
            auto_instrumentation.instrument_fastapi(app)

            mock_instrument.assert_called_once_with(app)

    def test_catches_instrumentation_exception(self):
        app = fastapi.FastAPI()

        with (
            mock.patch(
                "cloud_pipelines_backend.instrumentation.opentelemetry._internal.providers.has_configured_providers",
                return_value=True,
            ),
            mock.patch(
                "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor.instrument_app",
                side_effect=RuntimeError("instrumentation failed"),
            ),
        ):
            auto_instrumentation.instrument_fastapi(app)


class TestInstrumentFastapiIntegration:
    """Integration tests for auto_instrumentation.instrument_fastapi()."""

    def test_instruments_app_with_real_provider(self):
        trace.set_tracer_provider(otel_sdk_trace.TracerProvider())
        app = fastapi.FastAPI()

        auto_instrumentation.instrument_fastapi(app)

        assert getattr(app, "_is_instrumented_by_opentelemetry", False) is True

    def test_skips_with_no_real_provider(self):
        app = fastapi.FastAPI()

        auto_instrumentation.instrument_fastapi(app)

        assert getattr(app, "_is_instrumented_by_opentelemetry", False) is False
