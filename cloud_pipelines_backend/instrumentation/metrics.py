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

from opentelemetry import metrics as otel_metrics

# ---------------------------------------------------------------------------
# tangle.orchestrator
# ---------------------------------------------------------------------------
orchestrator_meter = otel_metrics.get_meter("tangle.orchestrator")

execution_system_errors = orchestrator_meter.create_counter(
    name="execution.system_errors",
    description="Number of execution nodes that ended in SYSTEM_ERROR status",
    unit="{error}",
)
