"""
Shared OpenTelemetry configuration resolution.

Reads and validates OTel settings from environment variables.
"""

import dataclasses
import enum
import os


class EnvVar(str, enum.Enum):
    """Environment variables that control OpenTelemetry configuration.

    Trace exporter:
        TRACE_EXPORTER_ENDPOINT: OTLP collector URL for trace data.
        TRACE_EXPORTER_PROTOCOL: Transport protocol ("grpc" or "http").

    Metric exporter:
        METRIC_EXPORTER_ENDPOINT: OTLP collector URL for metric data.
        METRIC_EXPORTER_PROTOCOL: Transport protocol ("grpc" or "http").

    Metric temporality (per-instrument overrides):
        Derived dynamically from MetricsTemporalityConfig field names using
        the prefix TANGLE_OTEL_METRICS_TEMPORALITY_ (e.g.
        TANGLE_OTEL_METRICS_TEMPORALITY_COUNTER). See MetricsTemporalityConfig
        for available fields and their defaults.

    Service identity:
        ENV: Application environment, used to build the default service name
            (e.g. "tangle-production").
        SERVICE_VERSION: Deployed revision or version tag.
    """

    TRACE_EXPORTER_ENDPOINT = "TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT"
    TRACE_EXPORTER_PROTOCOL = "TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL"
    METRIC_EXPORTER_ENDPOINT = "TANGLE_OTEL_METRIC_EXPORTER_ENDPOINT"
    METRIC_EXPORTER_PROTOCOL = "TANGLE_OTEL_METRIC_EXPORTER_PROTOCOL"
    ENV = "TANGLE_ENV"
    SERVICE_VERSION = "TANGLE_SERVICE_VERSION"


class ExporterProtocol(str, enum.Enum):
    GRPC = "grpc"
    HTTP = "http"


class AggregationTemporality(str, enum.Enum):
    DELTA = "delta"
    CUMULATIVE = "cumulative"


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExporterConfig:
    endpoint: str
    protocol: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class MetricsTemporalityConfig:
    counter: str = AggregationTemporality.DELTA
    observable_counter: str = AggregationTemporality.DELTA
    up_down_counter: str = AggregationTemporality.CUMULATIVE
    observable_up_down_counter: str = AggregationTemporality.CUMULATIVE
    histogram: str = AggregationTemporality.DELTA


@dataclasses.dataclass(frozen=True, kw_only=True)
class MetricsConfig:
    exporter: ExporterConfig
    temporality: MetricsTemporalityConfig


@dataclasses.dataclass(frozen=True, kw_only=True)
class OtelConfig:
    service_name: str
    service_version: str
    trace_exporter: ExporterConfig | None = None
    metrics: MetricsConfig | None = None


def _resolve_exporter(
    endpoint_var: str,
    protocol_var: str,
) -> ExporterConfig | None:
    endpoint = os.environ.get(endpoint_var)
    if not endpoint:
        return None

    protocol = os.environ.get(protocol_var, ExporterProtocol.GRPC)

    if not endpoint.startswith(("http://", "https://")):
        raise ValueError(
            f"Invalid OTel endpoint format: {endpoint}. "
            f"Expected format: http://<host>:<port> or https://<host>:<port>"
        )
    try:
        ExporterProtocol(protocol)
    except ValueError:
        raise ValueError(
            f"Invalid OTel protocol: {protocol}. "
            f"Expected values: {', '.join(ExporterProtocol)}"
        )

    return ExporterConfig(endpoint=endpoint, protocol=protocol)


def _resolve_temporality(env_var: str, default: str) -> str:
    value = os.environ.get(env_var, default).lower()
    try:
        AggregationTemporality(value)
    except ValueError:
        raise ValueError(
            f"Invalid OTel metrics temporality: {value} (from {env_var}). "
            f"Expected values: {', '.join(AggregationTemporality)}"
        )
    return value


_TEMPORALITY_ENV_PREFIX = "TANGLE_OTEL_METRICS_TEMPORALITY_"


def _resolve_metrics_temporality() -> MetricsTemporalityConfig:
    resolved = {
        f.name: _resolve_temporality(
            f"{_TEMPORALITY_ENV_PREFIX}{f.name.upper()}",
            f.default,
        )
        for f in dataclasses.fields(MetricsTemporalityConfig)
    }
    return MetricsTemporalityConfig(**resolved)


def resolve(
    service_name: str | None = None,
    service_version: str | None = None,
) -> OtelConfig | None:
    """Read and validate OTel configuration from environment variables.

    Returns None if no signals are configured (no exporter endpoints set).
    Raises ValueError if any configured exporter has invalid settings.
    """
    trace_exporter = _resolve_exporter(
        endpoint_var=EnvVar.TRACE_EXPORTER_ENDPOINT,
        protocol_var=EnvVar.TRACE_EXPORTER_PROTOCOL,
    )

    metrics_exporter = _resolve_exporter(
        endpoint_var=EnvVar.METRIC_EXPORTER_ENDPOINT,
        protocol_var=EnvVar.METRIC_EXPORTER_PROTOCOL,
    )

    if trace_exporter is None and metrics_exporter is None:
        return None

    if service_name is None:
        app_env = os.environ.get(EnvVar.ENV, "unknown")
        service_name = f"tangle-{app_env}"

    if service_version is None:
        service_version = os.environ.get(EnvVar.SERVICE_VERSION, "unknown")

    metrics = None
    if metrics_exporter is not None:
        metrics = MetricsConfig(
            exporter=metrics_exporter,
            temporality=_resolve_metrics_temporality(),
        )

    return OtelConfig(
        service_name=service_name,
        service_version=service_version,
        trace_exporter=trace_exporter,
        metrics=metrics,
    )
