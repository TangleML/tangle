"""What the consumer reports about draining the queue.

Two things live here, on two different clocks. The backlog gauge belongs to the process: it is
registered once, sampled on a timer, and read by the exporter's thread. Everything else
belongs to one cycle — the span covering a single drained row, the stages timed inside it, and
the counter that closes it out. Two counters sit outside a cycle: the claim counter, because an
attempt this consumer loses ends its cycle before the span could open, and the collision
counter, which the ledger's writer reports as it loses a delivery row to one already there.

A cycle is measured from before the poll, because the poll is part of the cycle even though it
is what reveals there is one. That ordering is the awkward part of instrumenting this loop, and
`CycleObserver` exists so the consumer states it once (`start_cycle` then `polled`) instead of
carrying two clocks and a pair of timestamps through its own code.
"""

import contextlib
import enum
import logging
import time
import typing

import sqlalchemy as sql
from opentelemetry import metrics as otel_metrics
from sqlalchemy import orm

from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions.observability import environment
from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.emissions.observability import timing as emission_timing
from cloud_pipelines_backend.emissions.observability import tracing as emission_tracing

logger = logging.getLogger(__name__)

# How stale the reported backlog depth is allowed to get. The loop itself runs far faster
# than this; counting the queue on every cycle would cost a query per 0.5s to answer a
# question nobody asks that often.
_PENDING_SAMPLE_INTERVAL_SECONDS: typing.Final[float] = 30.0


# The bag keys one delivery is timed into, and the keys the event's own timings leave out
# because they belong to a delivery rather than to the emission. A sink that makes a network
# call times that call too, nested inside its own stage, so the pair travels together.
_SINK_TIMING_KEY: typing.Final[str] = "sink_s"
_COLLECTOR_POST_TIMING_KEY: typing.Final[str] = "collector_post_s"
_DELIVERY_TIMING_KEYS: typing.Final[frozenset[str]] = frozenset(
    {_SINK_TIMING_KEY, _COLLECTOR_POST_TIMING_KEY}
)


class ClaimOutcome(str, enum.Enum):
    """How one attempt to claim a row ended.

    The three are mutually exclusive, so they partition every attempt: the ratio of LOST to
    the total is contention between consumers, and RECLAIMED on its own is how often a lease
    ran out from under the consumer holding it.
    """

    # Took a row no consumer held.
    CLAIMED = "claimed"
    # Took a row another consumer held past its lease.
    RECLAIMED = "reclaimed"
    # Another consumer took the candidate first.
    LOST = "lost"


def row_timings(
    *,
    extra_data: dict | None,
    timings: dict[str, float],
) -> dict:
    """Return the `extra_data` to write a settled row back with, carrying its timings.

    Args:
        extra_data: The row's current extra_data, holding the producer's half.
        timings: This cycle's stage durations, in seconds keyed `<stage>_s`.

    Returns:
        A new extra_data dict holding both roles' timings, namespaced by the role that
        measured them so the producer's `total_s` and the consumer's never collide::

            {
                "timings": {
                    "producer": {"total_s": 0.0123},
                    "consumer": {
                        "poll_db_s": 0.0021,
                        "claim_s": 0.0008,
                        "dispatch_s": 0.9032,
                        "handle_s": 0.9010,
                        "total_s": 0.9210,
                    },
                },
            }

        The consumer keys present are whichever stages actually ran: a row that was routed
        nowhere never reaches a handler, so it carries no `handle_s`. Anything else already on
        the row, the producer's half included, is carried over untouched.

        The event grain stops at `handle_s`. One emission can be delivered several times, so a
        single `sink_s` beside it would describe whichever delivery happened to be last;
        `take_delivery_timings` puts each one on its own delivery row instead.
    """
    try:
        return emission_timing.merged_row_timings(
            extra_data=extra_data,
            role=emission_timing.CONSUMER_ROLE,
            timings={
                stage: seconds
                for stage, seconds in timings.items()
                if stage not in _DELIVERY_TIMING_KEYS
            },
        )
    except Exception:
        # Measurement must never cost a delivery. This runs inline on the settle path, so a
        # fault here would propagate into _mark_terminal and leave the row claimed until its
        # lease ran out — the producer wraps its own instrumentation for exactly this reason.
        # The row is written back with what the producer put on it: returning None instead
        # would erase that half rather than merely losing this one.
        logger.warning(
            "Failed to merge consumer timings onto the emission row",
            exc_info=True,
        )
        return extra_data


def take_delivery_timings() -> dict | None:
    """Take the just-finished delivery's own timing, for the row recording that delivery.

    Called by the recorder as it writes a delivery's row, which is the first moment a writer
    holds both the measurement and somewhere to put it. Taking rather than reading is what
    keeps deliveries apart: the key is off the cycle's bag before the next sink overwrites it,
    so a declared sink that reached no implementation records no timing at all.

    Returns:
        The `extra_data` for the delivery row, or None when nothing timed a sink — a fan-out
        running outside a cycle, or a row for a delivery that never happened.
    """
    try:
        bag = emission_timing.current_timings()
        if bag is None:
            return None
        seconds = bag.pop(_SINK_TIMING_KEY, None)
        # Taken whether or not the sink was timed, so it cannot outlive its delivery and be
        # read as the next one's.
        post_seconds = bag.pop(_COLLECTOR_POST_TIMING_KEY, None)
        if seconds is None:
            return None
        timings = {_SINK_TIMING_KEY: seconds}
        if post_seconds is not None:
            timings[_COLLECTOR_POST_TIMING_KEY] = post_seconds
        return emission_timing.merged_row_timings(
            extra_data=None,
            role=emission_timing.CONSUMER_ROLE,
            timings=timings,
        )
    except Exception:
        # Same rule as above, on the per-delivery path: the recorder calls this while writing
        # the outcome row, so a fault here would lose the delivery record itself.
        logger.warning("Failed to take the delivery's timing", exc_info=True)
        return None


def _count_handled(
    *,
    emission_type: str,
    status: str,
) -> None:
    """Count one emission the consumer settled, under how far its fan-out got.

    One increment per emission, whatever it was delivered to: what each of those deliveries
    reported is counted on the delivery counter instead. Called only for a settle that landed, so
    the emission is counted once, by the consumer that closed it.

    Args:
        emission_type: The kind of emission the row carried.
        status: The HandleStatus value the row was settled with.
    """
    emission_metrics.increment(
        counter=emission_metrics.consumer_handled,
        attributes={
            emission_metrics.EMISSION_TYPE_LABEL: emission_type,
            emission_metrics.HANDLE_STATUS_LABEL: status,
        },
    )


def _count_recorder_unavailable(
    *,
    emission_type: str,
) -> None:
    """Count one delivery that happened and could not be recorded.

    The only counter for this failure, and the reason it needs one: the row is left claimed with
    no verdict, so `count_handled` never fires for it and every other signal looks like an
    ordinary cycle that has not finished yet. A rising rate here is a delivery being repeated
    once per lease against a ledger that cannot take it.

    Args:
        emission_type: The kind of emission whose delivery could not be recorded.
    """
    emission_metrics.increment(
        counter=emission_metrics.consumer_recorder_unavailable,
        attributes={emission_metrics.EMISSION_TYPE_LABEL: emission_type},
    )


def _count_delivery_incomplete(
    *,
    emission_type: str,
) -> None:
    """Count one emission a handler stopped part-way through and handed back.

    The only counter for a pass that ended early, and the reason it needs one: the row is left
    claimed with no verdict, so `count_handled` never fires for it, and this is the one failure
    the consumer does not re-raise — it returns and polls straight on, so it does not even show
    up as a failed cycle. A low rate is fan-outs taking a second pass; a sustained one is a
    fan-out that never finishes, which reads on `emission.oldest_in_progress_age` as an age
    climbing past a multiple of the lease.

    Args:
        emission_type: The kind of emission whose delivery stopped part-way.
    """
    emission_metrics.increment(
        counter=emission_metrics.consumer_delivery_incomplete,
        attributes={emission_metrics.EMISSION_TYPE_LABEL: emission_type},
    )


def count_outcome_collision(
    *,
    emission_type: str,
    sink_key: str,
) -> None:
    """Count one delivery row that lost to a row already on the ledger.

    Nonzero means a consumer overran its lease and a delivery was made twice, which nothing
    else reports: the consumer that won the row counts its own delivery and looks ordinary.

    Args:
        emission_type: The kind of emission the delivery belonged to.
        sink_key: The annotation key of the sink whose row collided.
    """
    emission_metrics.increment(
        counter=emission_metrics.consumer_outcome_collisions,
        attributes={
            emission_metrics.EMISSION_TYPE_LABEL: emission_type,
            emission_metrics.SINK_LABEL: sink_key,
        },
    )


def _count_claim(
    *,
    emission_type: str,
    outcome: ClaimOutcome,
) -> None:
    """Count one attempt to claim a row, under the way it ended.

    Args:
        emission_type: The kind of emission the candidate row carried.
        outcome: How the attempt ended.
    """
    emission_metrics.increment(
        counter=emission_metrics.consumer_claims,
        attributes={
            emission_metrics.EMISSION_TYPE_LABEL: emission_type,
            emission_metrics.CLAIM_OUTCOME_LABEL: outcome.value,
        },
    )


class CycleObserver:
    """One consumer cycle, from before the poll to the row being counted."""

    def __init__(
        self,
    ) -> None:
        """Start the cycle on both clocks.

        A monotonic one for the durations, immune to the wall clock being adjusted, and a
        wall-clock one so the spans can be placed on a real timeline afterwards.
        """
        self._started_at = time.monotonic()
        self._started_ns = time.time_ns()
        self._poll_elapsed: float | None = None
        self._poll_ended_ns: int | None = None
        self._claim_elapsed: float | None = None
        self._claim_started_ns: int | None = None
        self._claim_ended_ns: int | None = None
        self._emission_type: str | None = None
        self._timings: dict[str, float] | None = None

    @contextlib.contextmanager
    def claim(
        self,
    ) -> typing.Iterator[None]:
        """Time the conditional UPDATE that takes a row, freezing what it cost.

        Held rather than recorded, for the same reason the poll is: the claim runs before the
        span exists that would carry it. A claim this consumer loses is therefore never
        recorded here at all — the cycle ends before `handling` opens — and is counted on the
        claim counter instead.

        Yields:
            None. The block runs as it would without the timer.
        """
        started_at = time.monotonic()
        started_ns = time.time_ns()
        try:
            yield
        finally:
            self._claim_elapsed = time.monotonic() - started_at
            self._claim_started_ns = started_ns
            self._claim_ended_ns = time.time_ns()

    def polled(
        self,
    ) -> None:
        """Mark the poll as finished, freezing what it cost.

        Taken before anything else the loop does with the row in hand, so work that follows
        the poll is never read as time spent reading it.
        """
        self._poll_elapsed = time.monotonic() - self._started_at
        self._poll_ended_ns = time.time_ns()

    @contextlib.contextmanager
    def handling(
        self,
        *,
        emission_event_id: str,
        emission_type: str,
    ) -> typing.Iterator[None]:
        """Run the rest of the cycle inside this emission's span, timing it onto the row.

        The span is backdated to when the cycle began, so it covers the poll that revealed
        the row even though only that poll could say which row this is.

        Args:
            emission_event_id: The row being drained.
            emission_type: The kind of emission it carries.

        Yields:
            None. Every stage timed inside the block lands on the row's timings.
        """
        self._emission_type = emission_type
        with (
            emission_tracing.consumer_span(
                start_time_ns=self._started_ns
            ) as span_handle,
            emission_timing.timing_bag() as timings,
        ):
            span_handle.identify(
                emission_event_id=emission_event_id,
                emission_type=emission_type,
            )
            self._timings = timings
            self._account_for_poll()
            self._account_for_claim()
            yield

    def _account_for_poll(
        self,
    ) -> None:
        """Record the poll, which finished before this span could name the emission it was for."""
        if self._poll_elapsed is None or self._poll_ended_ns is None:
            return
        self._timings["poll_db_s"] = self._poll_elapsed
        emission_metrics.record(
            histogram=emission_metrics.consumer_duration_poll_db,
            seconds=self._poll_elapsed,
            emission_type=self._emission_type,
        )
        emission_tracing.record_stage_span(
            stage="poll_db",
            emission_type=self._emission_type,
            start_time_ns=self._started_ns,
            end_time_ns=self._poll_ended_ns,
        )

    def _account_for_claim(
        self,
    ) -> None:
        """Record the claim, which the poll contains and which also predates this span."""
        if self._claim_elapsed is None or self._claim_ended_ns is None:
            return
        self._timings["claim_s"] = self._claim_elapsed
        emission_metrics.record(
            histogram=emission_metrics.consumer_duration_claim,
            seconds=self._claim_elapsed,
            emission_type=self._emission_type,
        )
        emission_tracing.record_stage_span(
            stage="claim",
            emission_type=self._emission_type,
            start_time_ns=self._claim_started_ns,
            end_time_ns=self._claim_ended_ns,
        )

    def dispatch(
        self,
    ) -> typing.ContextManager[None]:
        """Time the dispatch, which is routing plus everything the handler does.

        Returns:
            A context manager wrapping the dispatch call. The handler and its sink time
            themselves into the same bag from inside it.
        """
        return emission_timing.stage_timer(
            histogram=emission_metrics.consumer_duration_dispatch,
            stage="dispatch",
            emission_type=self._emission_type,
        )

    @contextlib.contextmanager
    def writeback(
        self,
    ) -> typing.Iterator[dict[str, float]]:
        """Time the terminal write, and hand out the timings that ride it.

        The total is taken before the block runs, because a row cannot record the cost of its
        own write; the histogram in `finished` is recorded afterwards and does include it.
        Yielding the timings rather than exposing them separately is what keeps that ordering
        out of the caller's hands.

        Yields:
            This cycle's stage durations, keyed `<stage>_s`, to persist on the row.
        """
        self._timings["total_s"] = time.monotonic() - self._started_at
        with emission_tracing.stage_span(
            stage="writeback",
            emission_type=self._emission_type,
        ):
            yield dict(self._timings)

    def finished(
        self,
        *,
        status: str,
    ) -> None:
        """Report the finished cycle: how long the whole thing took, and how it ended.

        Args:
            status: The HandleStatus value the row was settled with.
        """
        self._record_total()
        _count_handled(emission_type=self._emission_type, status=status)

    def abandoned(
        self,
    ) -> None:
        """Report a cycle whose settle the fence rejected: the duration, and no count.

        The event is held by the consumer that reclaimed it, and that consumer's settle is what
        counts it. Counting it here as well would report one emission twice, under two verdicts,
        one of which no row holds. The duration is still real work and is recorded.
        """
        self._record_total()

    def _record_total(
        self,
    ) -> None:
        """Record how long this cycle took, whichever way it ended."""
        emission_metrics.record(
            histogram=emission_metrics.duration_total,
            seconds=time.monotonic() - self._started_at,
            emission_type=self._emission_type,
            attributes={emission_metrics.ROLE_LABEL: emission_metrics.CONSUMER_ROLE},
        )


class ConsumerObserver:
    """Everything the consumer reports, for as long as the consumer lives.

    Holds the backlog gauge's state and hands out a `CycleObserver` per row. One per
    consumer, since the gauge it can register is a per-process side effect.
    """

    def __init__(
        self,
    ) -> None:
        """Start with an unobserved backlog; the gauge is opted into separately."""
        # Backlog depth, refreshed on a timer by the loop and read by the gauge callback on
        # the exporter's thread. Only maintained once observe_backlog() has been called.
        self._pending_count = 0
        self._pending_sampled_at: float | None = None
        self._pending_gauge: otel_metrics.ObservableGauge | None = None

    def observe_backlog(
        self,
    ) -> None:
        """Start reporting how many rows the queue has not settled yet, as a gauge.

        Separate from construction because it registers a callback that the metrics SDK
        holds for the life of the process, which is a side effect a test building a consumer
        should not inherit. Until it is called, the loop skips the backlog query entirely.
        """
        self._pending_gauge = emission_metrics.create_pending_gauge(
            callback=self._observe_pending,
        )

    def _observe_pending(
        self,
        _options: otel_metrics.CallbackOptions,
    ) -> typing.Iterable[otel_metrics.Observation]:
        """Report the most recently sampled backlog depth to the metrics SDK.

        Runs on the SDK's exporter thread, so it only reads the cached count rather than
        querying. Reading an attribute is atomic under the interpreter lock, so the loop
        writing a fresh count concurrently is safe without a lock.

        Args:
            _options: The SDK's collection options; unused.

        Returns:
            One observation of the cached count.
        """
        return [
            otel_metrics.Observation(
                self._pending_count,
                environment.with_environment(attributes={}),
            )
        ]

    def refresh_backlog(
        self,
        *,
        session: orm.Session,
    ) -> None:
        """Re-count the rows the queue has not settled, if the cache is due for a refresh.

        Does nothing unless the backlog is being observed, and at most once per sample
        interval. Call it outside a timed stage so counting the queue never shows up as time
        spent reading a row.

        Args:
            session: The open session to count within.
        """
        if self._pending_gauge is None:
            return
        now = time.monotonic()
        if (
            self._pending_sampled_at is not None
            and now - self._pending_sampled_at < _PENDING_SAMPLE_INTERVAL_SECONDS
        ):
            return
        # Written as the two unsettled states rather than as "not settled" so the
        # (claimed_status, created_at) index seeks each of them: two short seeks over the
        # backlog, instead of a scan of the settled rows that make up nearly the whole table.
        self._pending_count = session.scalar(
            sql.select(sql.func.count())
            .select_from(db_models.EmissionEvent)
            .where(
                db_models.EmissionEvent.claimed_status.in_(
                    (
                        db_models.ClaimStatus.PENDING.value,
                        db_models.ClaimStatus.IN_PROGRESS.value,
                    )
                )
            )
        )
        self._pending_sampled_at = now

    def start_cycle(
        self,
    ) -> CycleObserver:
        """Begin measuring one cycle, before the poll that says whether there is a row.

        Returns:
            The observer for this cycle, which is simply dropped if the poll finds nothing.
        """
        return CycleObserver()

    def count_handled(
        self,
        *,
        emission_type: str,
        status: str,
    ) -> None:
        """Count a row closed outside a cycle span, such as one that could not be mapped.

        Args:
            emission_type: The kind of emission the row carried.
            status: The HandleStatus value the row was settled with.
        """
        _count_handled(emission_type=emission_type, status=status)

    def count_recorder_unavailable(
        self,
        *,
        emission_type: str,
    ) -> None:
        """Count a delivery that was made and could not be recorded.

        Reported from the consumer rather than the recorder, because the emission type is what
        makes the count readable and only the consumer holds it.

        Args:
            emission_type: The kind of emission whose delivery could not be recorded.
        """
        _count_recorder_unavailable(emission_type=emission_type)

    def count_delivery_incomplete(
        self,
        *,
        emission_type: str,
    ) -> None:
        """Count a delivery that stopped part-way and asked for the emission back.

        Reported from the consumer for the same reason as the counter above: the emission type
        is what makes the count readable, and only the consumer holds it.

        Args:
            emission_type: The kind of emission whose delivery stopped part-way.
        """
        _count_delivery_incomplete(emission_type=emission_type)

    def count_claim(
        self,
        *,
        emission_type: str,
        outcome: ClaimOutcome,
    ) -> None:
        """Count one attempt to claim a row.

        On the observer rather than the cycle because a lost attempt ends the cycle before
        its span opens, and a lost attempt is the one this counter exists for: it is
        otherwise indistinguishable from an empty poll.

        Args:
            emission_type: The kind of emission the candidate row carried.
            outcome: How the attempt ended.
        """
        _count_claim(emission_type=emission_type, outcome=outcome)
