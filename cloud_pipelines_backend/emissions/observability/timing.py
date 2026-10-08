"""Per-stage timing for one emission.

Timing an emission stage produces three things at once: a duration on a histogram, a child
span, and a number that ends up on the `emission_event` row. `stage_timer` does all three
from a single `with` at the call site, so instrumenting a stage never spreads across the code
it measures.

The row part needs a detour. A stage can be timed several call frames away from the code that
writes the row — the consumer writes the row, but the handler and its sink are what time
`handle` and `sink`, and a handler only ever hands back an `Outcome`. Rather than widen that
contract to carry timings, the consumer opens a bag for the cycle and every stage timer in
that cycle writes its measurement into it. A stage timed with no bag open (the producer's
side, or a handler under test) simply records its metric and span and skips the bag.

`merged_row_timings` is the other half of that: it turns a bag into the `extra_data` the row
is written with. Both the producer and the consumer put timings on the same row from
different processes, so it is the one piece of this module they share.
"""

import contextlib
import contextvars
import logging
import time
import typing

from opentelemetry import metrics as otel_metrics

from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.emissions.observability import tracing as emission_tracing

logger = logging.getLogger(__name__)

# The two sides of the pipeline that write timings onto a row, used as the key that keeps
# their measurements apart in `extra_data.timings`.
PRODUCER_ROLE: typing.Final[str] = "producer"
CONSUMER_ROLE: typing.Final[str] = "consumer"

# The bag of stage durations for the emission currently being handled, keyed `<stage>_s`.
# None outside a timing_bag() block.
_current_bag: contextvars.ContextVar[dict[str, float] | None] = contextvars.ContextVar(
    "emission_timing_bag",
    default=None,
)


@contextlib.contextmanager
def timing_bag() -> typing.Iterator[dict[str, float]]:
    """Collect every stage duration measured inside this block.

    Yields:
        The bag the stage timers write into, keyed `<stage>_s` (for example `handle_s`). It
        is the same object throughout the block, so the caller can read it after the block
        as well as during.
    """
    bag: dict[str, float] = {}
    token = _current_bag.set(bag)
    try:
        yield bag
    finally:
        _current_bag.reset(token)


def current_timings() -> dict[str, float] | None:
    """Return the bag stage timers are currently writing into, or None if no block is open."""
    return _current_bag.get()


def merged_row_timings(
    *,
    extra_data: dict | None,
    role: str,
    timings: dict[str, float],
) -> dict:
    """Return the row's `extra_data` with one role's stage timings added under `timings`.

    A new top-level dict, never a reach into the nested one: change tracking on that column
    sees an assignment, not a mutation buried inside it, so writing the timings any other way
    leaves them unflushed. Whatever the other role already wrote is carried over, which is
    what keeps both halves of an emission's story on the same row.

    Args:
        extra_data: The row's current extra_data, or None on a row being built.
        role: Which side measured these, PRODUCER_ROLE or CONSUMER_ROLE.
        timings: That role's stage durations, in seconds keyed `<stage>_s`.

    Returns:
        A new extra_data dict to assign to the row.
    """
    merged = dict(extra_data or {})
    merged["timings"] = {**(merged.get("timings") or {}), role: dict(timings)}
    return merged


@contextlib.contextmanager
def stage_timer(
    *,
    histogram: otel_metrics.Histogram,
    stage: str,
    emission_type: str,
    attributes: dict[str, str] | None = None,
) -> typing.Iterator[None]:
    """Time one stage of the emission path, recording it everywhere it belongs.

    The stage is measured whether or not the block succeeds: a stage that raised still took
    the time it took, and the exception continues on to the caller unchanged. The histogram
    is recorded before the span closes so the exemplar picks up the trace id.

    Args:
        histogram: The duration histogram for this stage.
        stage: The stage name, used for the span name and the `<stage>_s` bag key.
        emission_type: The kind of emission being handled.
        attributes: Labels for the histogram and the span, beside the emission type. A stage
            that runs several times per emission uses them to say which run this was.

    Yields:
        None. The block runs as it would without the timer.
    """
    start = time.monotonic()
    with emission_tracing.stage_span(
        stage=stage,
        emission_type=emission_type,
        attributes=attributes,
    ):
        try:
            yield
        finally:
            elapsed = time.monotonic() - start
            emission_metrics.record(
                histogram=histogram,
                seconds=elapsed,
                emission_type=emission_type,
                attributes=attributes,
            )
            _write_to_bag(stage=stage, seconds=elapsed)


def _write_to_bag(
    *,
    stage: str,
    seconds: float,
) -> None:
    """Record a stage's duration in the open bag, if there is one.

    One key per stage, so a stage that runs more than once per emission leaves the most recent
    measurement there. That is the shape `take_delivery_timings` reads: it takes the key back
    out as soon as the delivery it belongs to is recorded, before the next one overwrites it.

    Args:
        stage: The stage name; stored as the key `<stage>_s`.
        seconds: The measured duration.
    """
    bag = _current_bag.get()
    if bag is None:
        return
    try:
        bag[f"{stage}_s"] = seconds
    except Exception:
        logger.warning(f"Failed to record {stage} timing on the row", exc_info=True)
