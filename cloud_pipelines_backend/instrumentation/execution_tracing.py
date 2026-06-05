"""Retroactive OTel trace emission for execution lifecycle.

When an ExecutionNode reaches a terminal status, emits a root ``execution``
span covering the full lifetime plus one ``execution.status`` child span per
status entry recorded in the status history.  All timestamps are derived from
the history so span durations reflect actual time spent, not when this code
ran.
"""

import datetime
import logging

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from .. import backend_types_sql as bts

_logger = logging.getLogger(__name__)
_tracer = trace.get_tracer("tangle.orchestrator")

_CLOUD_PROVIDER_ANNOTATION_KEY = "cloud-pipelines.net/orchestration/cloud_provider"

_HISTORY_KEY = bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY
_TERMINAL_STATUSES = frozenset(s.value for s in bts.CONTAINER_STATUSES_ENDED)
_ERROR_TERMINAL_STATUSES = frozenset(
    {
        bts.ContainerExecutionStatus.FAILED,
        bts.ContainerExecutionStatus.SYSTEM_ERROR,
    }
)


def _error_attrs(*, execution: bts.ExecutionNode, status: str) -> dict[str, object]:
    """Extra attributes for terminal error/success status spans."""
    extra = execution.extra_data or {}
    attrs: dict[str, object] = {}

    def _set_exit_code() -> None:
        if (ce := execution.container_execution) and ce.exit_code is not None:
            attrs["execution.exit_code"] = ce.exit_code

    if status == bts.ContainerExecutionStatus.FAILED:
        msg = extra.get(bts.EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY)
        if msg is not None:
            attrs["exception.message"] = msg
        _set_exit_code()
    elif status == bts.ContainerExecutionStatus.SYSTEM_ERROR:
        msg = extra.get(
            bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY
        )
        if msg is not None:
            attrs["exception.message"] = msg
        tb = extra.get(bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_FULL_KEY)
        if tb is not None:
            attrs["exception.stacktrace"] = tb
    elif status == bts.ContainerExecutionStatus.SUCCEEDED:
        _set_exit_code()

    return attrs


def _launcher_pod_attrs(
    *, execution: bts.ExecutionNode, status: str
) -> dict[str, object]:
    """k8s pod/cluster attributes for the PENDING span."""
    if status != bts.ContainerExecutionStatus.PENDING:
        return {}
    if execution.container_execution_id is None:
        return {}
    ce = execution.container_execution
    if ce is None or ce.launcher_data is None:
        return {}
    k8s = (
        ce.launcher_data.get("kubernetes")
        or ce.launcher_data.get("kubernetes_job")
        or {}
    )
    attrs: dict[str, object] = {}
    if pod_name := k8s.get("pod_name") or k8s.get("job_name"):
        attrs["k8s.pod.name"] = pod_name
    if namespace := k8s.get("namespace"):
        attrs["k8s.namespace.name"] = namespace
    if cluster_url := k8s.get("cluster_server"):
        attrs["k8s.cluster.url"] = cluster_url
    return attrs


def _launcher_type_attrs(*, execution: bts.ExecutionNode) -> dict[str, object]:
    """Launcher type and cluster identity for the root execution span.

    Uses the top-level key of launcher_data as the launcher type — forward-compatible
    with any launcher implementation.  For k8s-family launchers adds cluster_server
    so GKE and Nebius clusters (which share the same launcher class in oasis-backend)
    can be distinguished by URL pattern.
    """
    if execution.container_execution_id is None:
        return {}
    ce = execution.container_execution
    if ce is None or not ce.launcher_data:
        return {}
    launcher_key = next(iter(ce.launcher_data))
    attrs: dict[str, object] = {"execution.launcher": launcher_key}
    inner = ce.launcher_data[launcher_key]
    if isinstance(inner, dict) and (cluster_url := inner.get("cluster_server")):
        attrs["k8s.cluster.url"] = cluster_url
    return attrs


def _cloud_provider_attrs(*, execution: bts.ExecutionNode) -> dict[str, object]:
    """Cloud provider for the root execution span, read from task_spec annotations.

    Returns ``{"execution.cloud_provider": value}`` when the
    ``cloud-pipelines.net/orchestration/cloud_provider`` annotation is present,
    otherwise an empty dict.  Callers (e.g. oasis-backend's MultiLauncherContainerLauncher)
    set this annotation at routing time; tangle launchers with a fixed cloud affinity
    can set it too.
    """
    provider = (
        (execution.task_spec or {})
        .get("annotations", {})
        .get(_CLOUD_PROVIDER_ANNOTATION_KEY)
    )
    if provider is None:
        return {}
    return {"execution.cloud_provider": provider}


def _cache_attrs(*, execution: bts.ExecutionNode) -> dict[str, object]:
    """Cache hit/miss attributes for the root execution span."""
    attrs: dict[str, object] = {}
    if execution.container_execution_cache_key is not None:
        attrs["execution.cache_key"] = execution.container_execution_cache_key
    reused_from = (execution.extra_data or {}).get("reused_from_execution_node_id")
    attrs["execution.cache.hit"] = reused_from is not None
    if reused_from is not None:
        attrs["execution.cache.reused_from_id"] = reused_from
    return attrs


def _pipeline_attrs(*, execution: bts.ExecutionNode) -> dict[str, object]:
    """Parent execution context for the root execution span."""
    if execution.parent_execution_id is None:
        return {}
    return {"execution.parent_id": execution.parent_execution_id}


def _ns(*, dt: datetime.datetime) -> int:
    """Return *dt* as nanoseconds since the Unix epoch (required by OTel SDK)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def emit_execution_trace(*, execution: bts.ExecutionNode) -> None:
    """Emit a complete execution trace when *execution* reaches a terminal status.

    No-op for non-terminal executions.  All exceptions are caught and logged so
    that tracing failures never affect the surrounding SQLAlchemy commit.
    """
    history: list = (execution.extra_data or {}).get(_HISTORY_KEY, [])
    if not history or history[-1]["status"] not in _TERMINAL_STATUSES:
        return
    try:
        first_time = datetime.datetime.fromisoformat(history[0]["first_observed_at"])
        last_time = datetime.datetime.fromisoformat(history[-1]["first_observed_at"])

        root = _tracer.start_span(
            "execution",
            attributes={
                "execution.id": execution.id,
                **_launcher_type_attrs(execution=execution),
                **_cloud_provider_attrs(execution=execution),
                **_cache_attrs(execution=execution),
                **_pipeline_attrs(execution=execution),
            },
            start_time=_ns(dt=first_time),
        )
        root_ctx = trace.set_span_in_context(root)

        for i, entry in enumerate(history):
            t_start = datetime.datetime.fromisoformat(entry["first_observed_at"])
            t_end = (
                datetime.datetime.fromisoformat(history[i + 1]["first_observed_at"])
                if i + 1 < len(history)
                else last_time
            )
            attrs: dict[str, object] = {
                "execution.id": execution.id,
                "execution.status": entry["status"],
                **_error_attrs(execution=execution, status=entry["status"]),
                **_launcher_pod_attrs(execution=execution, status=entry["status"]),
            }
            _tracer.start_span(
                f"execution.status {entry['status']}",
                context=root_ctx,
                attributes=attrs,
                start_time=_ns(dt=t_start),
            ).end(end_time=_ns(dt=t_end))

        if history[-1]["status"] in _ERROR_TERMINAL_STATUSES:
            root.set_status(status=StatusCode.ERROR)
        root.end(end_time=_ns(dt=last_time))
    except Exception:
        _logger.exception(f"Failed to emit execution trace for {execution.id!r}")
