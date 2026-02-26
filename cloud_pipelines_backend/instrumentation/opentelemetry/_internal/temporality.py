"""
Maps string-based temporality configuration to OTel SDK types.
"""

from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk.metrics import export as otel_metrics_export

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config

_TEMPORALITY_MAP = {
    config.AggregationTemporality.DELTA: otel_metrics_export.AggregationTemporality.DELTA,
    config.AggregationTemporality.CUMULATIVE: otel_metrics_export.AggregationTemporality.CUMULATIVE,
}


def build_preferred_temporality(
    temporality: config.MetricsTemporalityConfig,
) -> dict[type[otel_metrics.Instrument], otel_metrics_export.AggregationTemporality]:
    return {
        otel_sdk_metrics.Counter: _TEMPORALITY_MAP[temporality.counter],
        otel_sdk_metrics.ObservableCounter: _TEMPORALITY_MAP[
            temporality.observable_counter
        ],
        otel_sdk_metrics.UpDownCounter: _TEMPORALITY_MAP[temporality.up_down_counter],
        otel_sdk_metrics.ObservableUpDownCounter: _TEMPORALITY_MAP[
            temporality.observable_up_down_counter
        ],
        otel_sdk_metrics.Histogram: _TEMPORALITY_MAP[temporality.histogram],
    }
