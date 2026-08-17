"""OTel spans for the orchestrator processing loop so per-execution processing time is measurable."""

from __future__ import annotations

import collections.abc
import contextlib

from opentelemetry import trace
from opentelemetry.trace import StatusCode

_tracer = trace.get_tracer("tangle.orchestrator")


@contextlib.contextmanager
def operation_span(
    name: str, *, attributes: dict[str, object] | None = None
) -> collections.abc.Iterator[trace.Span]:
    with _tracer.start_as_current_span(name, attributes=attributes) as span:
        try:
            yield span
        except Exception as exception:
            span.set_status(StatusCode.ERROR)
            span.record_exception(exception)
            raise
