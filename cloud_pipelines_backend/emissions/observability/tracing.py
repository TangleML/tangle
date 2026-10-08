"""Spans for the emission paths.

One parent span per emission — `emission.producer` on the write side, `emission.consumer`
per drained row — with a child span per stage underneath, all carrying the emission event's
id. That id is what makes a single slow emission findable: it is far too high-cardinality to
be a metric label, so the trace is where per-emission detail lives.

Every helper here swallows its own failures. Tracing sits inside a database commit on one
side and the consumer's poll loop on the other, and neither should ever break because a span
could not be started.
"""

import contextlib
import logging
import typing

from opentelemetry import trace

logger = logging.getLogger(__name__)

_tracer = trace.get_tracer("tangle.emissions")

# Span attribute naming the emission event a span belongs to.
EMISSION_EVENT_ID_ATTRIBUTE: typing.Final[str] = "emission_event_id"
# Span attribute naming the kind of emission (readiness, metadata, ...).
EMISSION_TYPE_ATTRIBUTE: typing.Final[str] = "emission_type"


@contextlib.contextmanager
def _span(
    *,
    name: str,
    attributes: dict[str, object],
) -> typing.Iterator[None]:
    """Run a block inside a span named `name`, or without one if tracing fails.

    Args:
        name: The span name.
        attributes: Attributes to set on the span, with None values dropped (the SDK
            discards them anyway, and dropping them here keeps the intent explicit).

    Yields:
        None. The block runs either way; only the span is conditional.
    """
    try:
        span_context = _tracer.start_as_current_span(
            name,
            attributes={
                key: value for key, value in attributes.items() if value is not None
            },
        )
    except Exception:
        logger.warning(f"Failed to start emission span {name!r}", exc_info=True)
        yield
        return
    with span_context:
        yield


def producer_span(
    *,
    execution_node_id: str,
) -> typing.ContextManager[None]:
    """The parent span covering the producer's write for one node status change.

    The emission event ids do not exist yet at this point (they are assigned by the insert),
    so this span is keyed on the node instead.

    Args:
        execution_node_id: The node whose status change is being emitted for.

    Returns:
        A context manager wrapping the producer's work in the span.
    """
    return _span(
        name="emission.producer",
        attributes={"execution_node_id": execution_node_id},
    )


class ConsumerSpanHandle:
    """Labels the consumer's parent span once the consumer knows which row it drew.

    The span has to start before the poll for the poll to be inside it, but which emission
    the cycle is for is exactly what the poll returns. So the span opens anonymous and the
    consumer names it a moment later through this handle.
    """

    def __init__(
        self,
        *,
        span: trace.Span | None,
    ) -> None:
        """Hold the span to label, or None when tracing is off or failed to start.

        Args:
            span: The started parent span, or None if there is nothing to label.
        """
        self._span = span

    def identify(
        self,
        *,
        emission_event_id: str,
        emission_type: str,
    ) -> None:
        """Name the emission this cycle turned out to be for.

        Args:
            emission_event_id: The row being drained; the id to search a trace backend by.
            emission_type: The kind of emission, matching the metric label of the same name.
        """
        if self._span is None:
            return
        try:
            self._span.set_attribute(EMISSION_EVENT_ID_ATTRIBUTE, emission_event_id)
            self._span.set_attribute(EMISSION_TYPE_ATTRIBUTE, emission_type)
        except Exception:
            logger.warning(
                f"Failed to label emission span for {emission_event_id}",
                exc_info=True,
            )


@contextlib.contextmanager
def consumer_span(
    *,
    start_time_ns: int,
) -> typing.Iterator[ConsumerSpanHandle]:
    """The parent span covering one consumer cycle, from poll to terminal writeback.

    Args:
        start_time_ns: When the cycle began, in nanoseconds since the epoch. Passed
            explicitly so the span can be opened after the poll it covers — the poll is what
            reveals whether there is a cycle worth tracing at all.

    Yields:
        A handle for naming the emission once the poll has returned it.
    """
    try:
        span_context = _tracer.start_as_current_span(
            "emission.consumer",
            start_time=start_time_ns,
            end_on_exit=True,
        )
    except Exception:
        logger.warning("Failed to start emission consumer span", exc_info=True)
        yield ConsumerSpanHandle(span=None)
        return
    with span_context as span:
        yield ConsumerSpanHandle(span=span)


def record_stage_span(
    *,
    stage: str,
    emission_type: str,
    start_time_ns: int,
    end_time_ns: int,
) -> None:
    """Emit a child span for a stage that has already finished.

    For stages that ran before their parent span could be opened. The timestamps are the
    real ones, so the span sits where the work actually happened.

    Args:
        stage: The stage name, used verbatim as the span name.
        emission_type: The kind of emission.
        start_time_ns: When the stage began, in nanoseconds since the epoch.
        end_time_ns: When it finished, in nanoseconds since the epoch.
    """
    try:
        # A zero-length span is dropped by some backends, so give an instant stage 1ns.
        end = max(end_time_ns, start_time_ns + 1)
        _tracer.start_span(
            stage,
            attributes={EMISSION_TYPE_ATTRIBUTE: emission_type},
            start_time=start_time_ns,
        ).end(end_time=end)
    except Exception:
        logger.warning(f"Failed to emit emission span {stage!r}", exc_info=True)


def stage_span(
    *,
    stage: str,
    emission_type: str | None = None,
    attributes: dict[str, str] | None = None,
) -> typing.ContextManager[None]:
    """A child span for one stage of the emission path.

    Args:
        stage: The stage name (for example `poll_db`, `dispatch`, `handle`, `sink`, or
            `writeback`), used verbatim as the span name.
        emission_type: The kind of emission, when the caller knows it.
        attributes: Anything else that identifies this run of the stage, such as which sink a
            delivery went to when the fan-out makes several.

    Returns:
        A context manager wrapping the stage in the span.
    """
    return _span(
        name=stage,
        attributes={
            EMISSION_TYPE_ATTRIBUTE: emission_type,
            **(attributes or {}),
        },
    )
