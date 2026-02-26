"""Tests for the OpenTelemetry temporality mapping module."""

from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk.metrics import export as otel_metrics_export

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config
from cloud_pipelines_backend.instrumentation.opentelemetry._internal import temporality


class TestBuildPreferredTemporality:
    """Tests for temporality.build_preferred_temporality()."""

    def test_default_temporality_config_values(self):
        t = config.MetricsTemporalityConfig()

        assert t.counter == "delta"
        assert t.observable_counter == "delta"
        assert t.up_down_counter == "cumulative"
        assert t.observable_up_down_counter == "cumulative"
        assert t.histogram == "delta"

    def test_maps_defaults_to_sdk_types(self):
        result = temporality.build_preferred_temporality(
            config.MetricsTemporalityConfig()
        )

        assert (
            result[otel_sdk_metrics.Counter]
            == otel_metrics_export.AggregationTemporality.DELTA
        )
        assert (
            result[otel_sdk_metrics.ObservableCounter]
            == otel_metrics_export.AggregationTemporality.DELTA
        )
        assert (
            result[otel_sdk_metrics.UpDownCounter]
            == otel_metrics_export.AggregationTemporality.CUMULATIVE
        )
        assert (
            result[otel_sdk_metrics.ObservableUpDownCounter]
            == otel_metrics_export.AggregationTemporality.CUMULATIVE
        )
        assert (
            result[otel_sdk_metrics.Histogram]
            == otel_metrics_export.AggregationTemporality.DELTA
        )

    def test_maps_overrides(self):
        result = temporality.build_preferred_temporality(
            config.MetricsTemporalityConfig(
                counter="cumulative",
                histogram="cumulative",
            )
        )

        assert (
            result[otel_sdk_metrics.Counter]
            == otel_metrics_export.AggregationTemporality.CUMULATIVE
        )
        assert (
            result[otel_sdk_metrics.Histogram]
            == otel_metrics_export.AggregationTemporality.CUMULATIVE
        )
