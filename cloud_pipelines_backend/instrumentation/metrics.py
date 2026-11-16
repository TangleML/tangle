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
from . import execution_tracing

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

execution_launch_rejections = orchestrator_meter.create_counter(
    name="execution.launch_rejections",
    description=(
        "Number of execution nodes that ended in FAILED status because their"
        " launch was rejected (e.g. an admission webhook denied the workload)"
    ),
    unit=MetricUnit.ERRORS,
)

container_execution_missing_workloads = orchestrator_meter.create_counter(
    name="container_execution.missing_workloads",
    description=(
        "Number of container executions that failed because their workload"
        " disappeared before it completed (e.g. the pod was deleted)"
    ),
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

container_execution_refresh_duration = orchestrator_meter.create_histogram(
    name="container_execution.refresh.duration",
    description=(
        "Duration of one orchestrator refresh of a pending or running container"
        " execution, split into its queries and its processing"
    ),
    unit=MetricUnit.SECONDS,
    explicit_bucket_boundaries_advisory=[
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
    ],
)

container_execution_completion_lag_duration = orchestrator_meter.create_histogram(
    name="container_execution.completion_lag.duration",
    description=(
        "Time from a container ending, as reported by its launcher, to the"
        " orchestrator committing its container execution's SUCCEEDED or FAILED status"
    ),
    unit=MetricUnit.SECONDS,
    explicit_bucket_boundaries_advisory=[
        0.5,
        1.0,
        2.0,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
        600.0,
        1800.0,
    ],
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
        execution_tracing.emit_execution_trace(execution=obj)


def record_refresh_duration(
    *, queries_duration_ms: float, processing_duration_ms: float
) -> None:
    """Records one running container execution refresh's queries and processing durations."""
    for phase, duration_ms in (
        ("queries", queries_duration_ms),
        ("processing", processing_duration_ms),
    ):
        container_execution_refresh_duration.record(
            duration_ms / 1000,
            attributes={"container_execution.refresh.phase": phase},
        )


def record_completion_lag(
    *,
    ended_at: datetime.datetime | None,
    status: backend_types_sql.ContainerExecutionStatus,
) -> None:
    """Records how long after its container ended a container execution's SUCCEEDED or FAILED status was committed."""
    if ended_at is None:
        return
    if ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=datetime.timezone.utc)
    lag_seconds = (
        datetime.datetime.now(tz=datetime.timezone.utc) - ended_at
    ).total_seconds()
    container_execution_completion_lag_duration.record(
        max(lag_seconds, 0.0),
        attributes={"container_execution.status": status.value},
    )
