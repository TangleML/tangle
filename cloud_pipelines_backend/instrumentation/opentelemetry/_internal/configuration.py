"""
Shared OpenTelemetry configuration resolution.

Reads and validates OTel settings from environment variables.
"""

import dataclasses
import enum
import os


class ExporterProtocol(str, enum.Enum):
    GRPC = "grpc"
    HTTP = "http"


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExporterConfig:
    endpoint: str
    protocol: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class OtelConfig:
    service_name: str
    service_version: str
    trace_exporter: ExporterConfig | None = None


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
            f"Expected values: {', '.join(e.value for e in ExporterProtocol)}"
        )

    return ExporterConfig(endpoint=endpoint, protocol=protocol)


def resolve(
    service_name: str | None = None,
    service_version: str | None = None,
) -> OtelConfig | None:
    """Read and validate OTel configuration from environment variables.

    Returns None if no signals are configured (no exporter endpoints set).
    Raises ValueError if any configured exporter has invalid settings.
    """
    trace_exporter = _resolve_exporter(
        endpoint_var="TANGLE_OTEL_TRACE_EXPORTER_ENDPOINT",
        protocol_var="TANGLE_OTEL_TRACE_EXPORTER_PROTOCOL",
    )

    if trace_exporter is None:
        return None

    if service_name is None:
        app_env = os.environ.get("TANGLE_ENV", "unknown")
        service_name = f"tangle-{app_env}"

    if service_version is None:
        service_version = os.environ.get("TANGLE_SERVICE_VERSION", "unknown")

    return OtelConfig(
        service_name=service_name,
        service_version=service_version,
        trace_exporter=trace_exporter,
    )
