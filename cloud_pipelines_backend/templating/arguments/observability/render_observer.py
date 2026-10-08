"""What the renderer reports about one firing's failed keys.

`rendering.render` stays free of instrumentation: it returns what it produced and
names no carrier. The call sites own their identity -- a schedule has an id and a
name, a subscription has an id and a cycle, and neither has the other's -- so each
passes its own, and the fields common to both are formatted here.

One line per failed key, not one per render: the reason differs per key, and a
summary line can only carry one of them.
"""

import logging
from collections.abc import Mapping

from cloud_pipelines_backend.templating.arguments import rendering, sources
from cloud_pipelines_backend.templating.arguments.observability import metrics

logger = logging.getLogger(__name__)

_NONE: str = "none"


def _clock_fields(clock: sources.Clock) -> str:
    """Format the clock as key=value, naming a source that is absent.

    a cron clock     -> "trigger_time=... schedule_time=..."
    a manual clock   -> "trigger_time=... schedule_time=none"
    """
    schedule_time = (
        clock.schedule_time.isoformat() if clock.schedule_time is not None else _NONE
    )
    return (
        f"trigger_time={clock.trigger_time.isoformat()} schedule_time={schedule_time}"
    )


def _count_outcomes(*, rendered: rendering.Rendered, kind: str) -> None:
    """Record one verdict per key, then one per run that lost any key.

    The two buckets are kept disjoint here rather than assumed to be: a key a caller
    pre-seeded in `arguments` keeps that prior value when its template fails, so it appears in
    both maps. Counting RENDERED over the keys absent from `failures` makes
    `keys_rendered = rendered + failed` hold for every input, not just the empty-`arguments`
    callers that exist today.
    """
    for key in rendered.arguments:
        if key in rendered.failures:
            continue
        metrics.increment(
            counter=metrics.keys_rendered,
            attributes={
                metrics.KIND_LABEL: kind,
                metrics.OUTCOME_LABEL: metrics.Outcome.RENDERED.value,
            },
        )
    for key in rendered.failures:
        metrics.increment(
            counter=metrics.keys_rendered,
            attributes={
                metrics.KIND_LABEL: kind,
                metrics.OUTCOME_LABEL: metrics.Outcome.FAILED.value,
            },
        )
        metrics.increment(
            counter=metrics.render_failures,
            attributes={
                metrics.KIND_LABEL: kind,
                metrics.REASON_LABEL: rendered.failure_codes.get(key, _NONE),
            },
        )
    if rendered.failures:
        metrics.increment(
            counter=metrics.runs_with_a_failed_key,
            attributes={metrics.KIND_LABEL: kind},
        )


def report(
    *,
    rendered: rendering.Rendered,
    templates: Mapping[str, str],
    clock: sources.Clock,
    identity: Mapping[str, object],
) -> None:
    """Count every key's verdict, and log one warning per failed key.

    Nothing is logged when every key rendered; the counters still move.

    identity={"schedule_id": "sched_ab", "schedule_name": "nightly-fx"}
        -> "Template render failed key=as_of_date kind=cron schedule_id=sched_ab
            schedule_name=nightly-fx template=... trigger_time=... reason=..."
    """
    kind = clock.kind.value
    _count_outcomes(rendered=rendered, kind=kind)
    if not rendered.failures:
        return
    carrier = " ".join(f"{name}={value}" for name, value in identity.items())
    clock_fields = _clock_fields(clock)
    for key in sorted(rendered.failures):
        logger.warning(
            f"Template render failed key={key} kind={kind} "
            f"{carrier} template={templates.get(key, _NONE)!r} "
            f"{clock_fields} reason={rendered.failures[key]}"
        )


def submission_rejected(*, kind: str) -> None:
    """Count a firing whose run submission was refused outright."""
    metrics.increment(
        counter=metrics.submission_rejected,
        attributes={metrics.KIND_LABEL: kind},
    )
