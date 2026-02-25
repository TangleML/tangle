"""
OpenTelemetry metrics configuration.

This module sets up the global meter provider with an OTLP exporter.
"""

import logging

from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc import (
    metric_exporter as otel_grpc_metric_exporter,
)
from opentelemetry.exporter.otlp.proto.http import (
    metric_exporter as otel_http_metric_exporter,
)
from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk import resources as otel_resources
from opentelemetry.sdk.metrics import export as otel_metrics_export

from cloud_pipelines_backend.instrumentation.opentelemetry._internal import config
from cloud_pipelines_backend.instrumentation.opentelemetry._internal import (
    temporality as temporality_mod,
)

_logger = logging.getLogger(__name__)


def setup(
    endpoint: str,
    protocol: str,
    service_name: str,
    service_version: str | None = None,
    temporality: config.MetricsTemporalityConfig | None = None,
) -> None:
    """
    Configure the global OpenTelemetry meter provider.

    Args:
        endpoint: The OTLP collector endpoint URL.
        protocol: The exporter protocol ("grpc" or "http").
        service_name: The service name reported to the collector.
        service_version: The service version (e.g. git revision) reported to the collector.
        temporality: Per-instrument aggregation temporality preferences.
    """
    try:
        _logger.info(
            f"Configuring OpenTelemetry metrics, endpoint={endpoint}, "
            f"protocol={protocol}, service_name={service_name}, "
            f"service_version={service_version}"
        )

        preferred_temporality = (
            temporality_mod.build_preferred_temporality(temporality)
            if temporality
            else None
        )

        if protocol == config.ExporterProtocol.GRPC:
            otel_exporter = otel_grpc_metric_exporter.OTLPMetricExporter(
                endpoint=endpoint,
                preferred_temporality=preferred_temporality,
            )
        else:
            otel_exporter = otel_http_metric_exporter.OTLPMetricExporter(
                endpoint=endpoint,
                preferred_temporality=preferred_temporality,
            )

        attributes = {otel_resources.SERVICE_NAME: service_name}
        if service_version:
            attributes[otel_resources.SERVICE_VERSION] = service_version
        resource = otel_resources.Resource.create(attributes)

        reader = otel_metrics_export.PeriodicExportingMetricReader(otel_exporter)
        meter_provider = otel_sdk_metrics.MeterProvider(
            resource=resource,
            metric_readers=[reader],
        )
        otel_metrics.set_meter_provider(meter_provider)

        _logger.info("OpenTelemetry metrics configured successfully.")
    except Exception as e:
        _logger.exception("Failed to configure OpenTelemetry metrics")
