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

import enum

from opentelemetry import metrics as otel_metrics


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    SECONDS = "s"
    ERRORS = "{error}"


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


def register_listener() -> None:
    """Subscribe a status-transition callback to StatusTransitionEvent.

    Called by the OTel metrics setup (opentelemetry/metrics.py::setup) so the
    subscriber is only active when a real MeterProvider is configured.  Environments
    without an OTel metrics exporter never call this, so no transition callbacks fire.
    """
    from .. import event_listeners

    def _on_status_transition(event: event_listeners.StatusTransitionEvent) -> None:
        execution_status_transition_duration.record(
            event.duration_seconds,
            attributes={
                "execution.status.from": event.from_status,
                "execution.status.to": event.to_status,
            },
        )

    event_listeners.subscribe(
        event_type=event_listeners.StatusTransitionEvent,
        callback=_on_status_transition,
        asynchronous=False,
    )
