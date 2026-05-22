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

from .. import backend_types_sql as bts

_logger = logging.getLogger(__name__)
_tracer = trace.get_tracer("tangle.orchestrator")

_HISTORY_KEY = bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY
_TERMINAL_STATUSES = frozenset(s.value for s in bts.CONTAINER_STATUSES_ENDED)


def _ns(*, dt: datetime.datetime) -> int:
    """Return *dt* as nanoseconds since the Unix epoch (required by OTel SDK)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def try_emit_execution_trace(*, execution: bts.ExecutionNode) -> None:
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
            attributes={"execution.id": execution.id},
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
            }
            _tracer.start_span(
                f"execution.status {entry['status']}",
                context=root_ctx,
                attributes=attrs,
                start_time=_ns(dt=t_start),
            ).end(end_time=_ns(dt=t_end))

        root.end(end_time=_ns(dt=last_time))
    except Exception:
        _logger.exception(f"Failed to emit execution trace for {execution.id!r}")
