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
class OtelConfig:
    endpoint: str
    protocol: str
    service_name: str
    service_version: str


def resolve(
    service_name: str | None = None,
    service_version: str | None = None,
) -> OtelConfig | None:
    """Read and validate shared OTel configuration from environment variables.

    Returns None if OTel is not configured (no exporter endpoint set).
    Raises ValueError if the configuration is invalid.
    """
    otel_endpoint = os.environ.get("TANGLE_OTEL_EXPORTER_ENDPOINT")
    if not otel_endpoint:
        return None

    otel_protocol = os.environ.get(
        "TANGLE_OTEL_EXPORTER_PROTOCOL", ExporterProtocol.GRPC
    )

    if service_name is None:
        app_env = os.environ.get("TANGLE_ENV", "unknown")
        service_name = f"tangle-{app_env}"

    if service_version is None:
        service_version = os.environ.get("TANGLE_SERVICE_VERSION", "unknown")

    if not otel_endpoint.startswith(("http://", "https://")):
        raise ValueError(
            f"Invalid OTel endpoint format: {otel_endpoint}. "
            f"Expected format: http://<host>:<port> or https://<host>:<port>"
        )
    try:
        ExporterProtocol(otel_protocol)
    except ValueError:
        raise ValueError(
            f"Invalid OTel protocol: {otel_protocol}. "
            f"Expected values: {', '.join(e.value for e in ExporterProtocol)}"
        )

    return OtelConfig(
        endpoint=otel_endpoint,
        protocol=otel_protocol,
        service_name=service_name,
        service_version=service_version,
    )
