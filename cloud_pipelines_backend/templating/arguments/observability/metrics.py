"""The templating meter and its instruments.

Declaration only: this module names what is measured, and `render_observer.py`
records into it. Names follow `template.<name>`, the convention
`emissions/docs/EMISSION_OBSERVABILITY.md` sets -- snake_case, named for what is
measured rather than for who measured it. The Prometheus export rewrites the dots
and adds its own suffixes, so `template.render_failures` arrives as
`template_render_failures_total`.

Counters only. Rendering is pure CPU at roughly 155 microseconds per key with no
queue, no I/O and no fan-out, so there is no duration worth a histogram and no
depth worth a gauge.

`increment` is a copy of the trigger and quota ones rather than an import: every
package that measures owns its own, because importing another feature's
observability to count your own inverts the dependency.
"""

import enum
import logging
import typing

from opentelemetry import metrics as otel_metrics

logger = logging.getLogger(__name__)

# Carried by every instrument: which kind of run was rendering. Three values --
# cron, subscription, manual -- and the kind follows the clock, so a hand-fired
# cron schedule is counted as manual.
KIND_LABEL: typing.Final[str] = "kind"
# Carried by `keys_rendered`: the verdict for one key. Two mutually exclusive
# values, so the series sum is the number of keys rendered.
OUTCOME_LABEL: typing.Final[str] = "outcome"
# Carried by `render_failures`: `TemplateError.code`, the builder's own name. A
# closed set; the message is deliberately not used, being unbounded.
REASON_LABEL: typing.Final[str] = "reason"


class Outcome(str, enum.Enum):
    """The two verdicts `keys_rendered` splits on.

    There is no `overridden`. Displacing a stored argument is only knowable inside the locked
    read that selects the pipeline version, which happens after rendering, so no caller can
    report it -- see `rendering.Rendered.overridden`, which stays because `render` computes it
    correctly for any caller that one day passes a non-empty map.
    """

    RENDERED = "rendered"
    FAILED = "failed"


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    KEYS = "{key}"
    RUNS = "{run}"


template_meter = otel_metrics.get_meter("tangle.templating")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

keys_rendered = template_meter.create_counter(
    name="template.keys_rendered",
    description=(
        "Number of template keys the renderer reached a verdict on, by kind and"
        " outcome. The denominator for the failure rate: a key either rendered or"
        " failed, never both, so their sum is every key rendered"
    ),
    unit=MetricUnit.KEYS,
)

render_failures = template_meter.create_counter(
    name="template.render_failures",
    description=(
        "Number of keys that failed to render, by kind and reason. The alertable"
        " one: a failed key keeps whatever the spec already had, so the run is"
        " submitted and reports SUCCESS while executing against a stale value"
    ),
    unit=MetricUnit.KEYS,
)

runs_with_a_failed_key = template_meter.create_counter(
    name="template.runs_with_a_failed_key",
    description=(
        "Number of runs submitted with at least one key that failed to render, by"
        " kind. Counted per run rather than per key, because one broken template"
        " on a six-key schedule is one bad run and not six"
    ),
    unit=MetricUnit.RUNS,
)

submission_rejected = template_meter.create_counter(
    name="template.submission_rejected",
    description=(
        "Number of firings whose run submission was refused outright, by kind --"
        " in practice a rendered key the pipeline does not declare. For a"
        " subscription this strands the row with no run and no cycle spent;"
        " `trigger.run_not_started` also sees that case, and nothing sees the cron"
        " one, which fires again on the next tick and fails identically"
    ),
    unit=MetricUnit.RUNS,
)


def increment(
    *,
    counter: otel_metrics.Counter,
    attributes: dict[str, str],
) -> None:
    """Add one to a counter, logging instead of raising if that fails.

    Measuring a render must never change whether the run happens, so a broken
    instrument costs a log line and nothing else.
    """
    try:
        counter.add(1, attributes=attributes)
    except Exception:
        logger.warning(
            f"Failed to increment template counter {attributes}", exc_info=True
        )
