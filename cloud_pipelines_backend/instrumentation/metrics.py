"""
Application-level meters and instruments.

Meters should be named after the software component they represent.
They should not change over time (avoid using __name__).

Instruments should be named after the metric they represent.
First and foremost, they should follow the semantic conventions
(https://opentelemetry.io/docs/specs/semconv/general/metrics/)
of OTel if the metric is common (e.g. http.server.duration).

For custom, application-specific measurements, choose a name after
what is being measured, and not after the software component that
measures it.

Good example:
- Meter: tangle.orchestrator
- Instrument: execution.system_errors

Bad example:
- Meter: tangle.orchestrator
- Instrument: orchestrator_execution_system_errors
"""

import datetime
import enum
import logging

from opentelemetry import metrics as otel_metrics
from sqlalchemy import event as sql_event
from sqlalchemy import orm

from .. import backend_types_sql

_logger = logging.getLogger(__name__)


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    SECONDS = "s"
    ERRORS = "{error}"
    EXECUTIONS = "{execution}"


# ---------------------------------------------------------------------------
# tangle.orchestrator
# ---------------------------------------------------------------------------
orchestrator_meter = otel_metrics.get_meter("tangle.orchestrator")

execution_system_errors = orchestrator_meter.create_counter(
    name="execution.system_errors",
    description="Number of execution nodes that ended in SYSTEM_ERROR status",
    unit=MetricUnit.ERRORS,
)

execution_status_transition_duration = orchestrator_meter.create_histogram(
    name="execution.status_transition.duration",
    description="Duration an execution spent in a status before transitioning to the next status",
    unit=MetricUnit.SECONDS,
)

execution_status_count = orchestrator_meter.create_observable_gauge(
    name="execution.status.count",
    callbacks=[],
    description="Number of execution nodes in each active (non-terminal) status",
    unit=MetricUnit.EXECUTIONS,
)


# ---------------------------------------------------------------------------
# SQLAlchemy event listeners
# ---------------------------------------------------------------------------

_HISTORY_KEY = backend_types_sql.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY


@sql_event.listens_for(orm.Session, "before_commit")
def _handle_before_commit(session: orm.Session) -> None:
    for obj in list(session.new) + list(session.dirty):
        if not isinstance(obj, backend_types_sql.ExecutionNode):
            continue
        if not obj._status_changed:
            continue
        history: list = (obj.extra_data or {}).get(_HISTORY_KEY, [])
        if len(history) >= 2:
            prev = history[-2]
            curr = history[-1]
            prev_time = datetime.datetime.fromisoformat(prev["first_observed_at"])
            curr_time = datetime.datetime.fromisoformat(curr["first_observed_at"])
            try:
                execution_status_transition_duration.record(
                    (curr_time - prev_time).total_seconds(),
                    attributes={
                        "execution.status.from": prev["status"],
                        "execution.status.to": curr["status"],
                    },
                )
            except Exception:
                _logger.warning(
                    f"Failed to record status transition metric for execution {obj.id!r}",
                    exc_info=True,
                )
        obj._status_changed = False
