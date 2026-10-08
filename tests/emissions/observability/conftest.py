"""Fixtures that point the emission instruments at in-memory OTel readers.

Nothing installs an OTel provider under test, so the module-level meter and tracer in
`emissions/observability/` are no-op proxies that record nowhere. The fixtures here swap in a
real SDK provider backed by an in-memory reader, so a test can assert on what the production
code actually recorded. They patch the module attributes rather than the global provider
because the global one can only be set once per process, which would leak the first test's
reader into every test after it.
"""

import collections.abc
import typing

import pytest
from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk import trace as otel_sdk_trace
from opentelemetry.sdk.metrics import export as metrics_export
from opentelemetry.sdk.trace import export as trace_export
from opentelemetry.sdk.trace.export import in_memory_span_exporter

from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.emissions.observability import tracing as emission_tracing
from tests.emissions.observability import probes

# The instrument attributes on emissions.observability.metrics, mapped to the metric names
# they are declared with. The metrics fixture re-declares each one under the same name so a
# test reads back what production would emit; test_metrics.py checks this map against the
# names the module really declared, so a rename cannot quietly diverge from the tests.
_COUNTER_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "producer_written": "emission.written",
    "consumer_handled": "emission.handled",
    "consumer_delivered": "emission.delivered",
    "consumer_outcome_collisions": "emission.outcome_collisions",
    "consumer_recorder_unavailable": "emission.recorder_unavailable",
    "consumer_delivery_incomplete": "emission.delivery_incomplete",
    "consumer_claims": "emission.claims",
    "producer_transitions_recorded": "emission.transitions_recorded",
    "producer_transitions_drained": "emission.transitions_drained",
    "producer_transitions_rejected_stale": "emission.transitions_rejected_stale",
}
_HISTOGRAM_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "duration_total": "emission.duration.total",
    "consumer_duration_poll_db": "emission.duration.poll_db",
    "consumer_duration_claim": "emission.duration.claim",
    "consumer_duration_dispatch": "emission.duration.dispatch",
    "consumer_duration_handle": "emission.duration.handle",
    "consumer_duration_sink": "emission.duration.sink",
}
# Distributions that are not durations, kept apart only because they are re-declared under
# their own unit; a count recorded onto a seconds instrument would export as seconds.
_COUNT_HISTOGRAM_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "consumer_payloads_per_delivery": "emission.payloads_per_delivery",
}


@pytest.fixture()
def instrument_names() -> dict[str, str]:
    """The instrument attribute names, mapped to the metric name each one declares.

    Returns:
        Every counter and distribution the emission code records through.
    """
    return {
        **_COUNTER_INSTRUMENTS,
        **_HISTOGRAM_INSTRUMENTS,
        **_COUNT_HISTOGRAM_INSTRUMENTS,
    }


@pytest.fixture()
def metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> collections.abc.Generator[probes.MetricsProbe, None, None]:
    """Redeclare every emission instrument on an in-memory meter provider.

    Yields:
        A probe over the instruments the emission code recorded through.
    """
    reader = metrics_export.InMemoryMetricReader()
    provider = otel_sdk_metrics.MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("tangle.emissions")
    # The gauges are built on demand from the module's meter, so patch the meter itself too.
    monkeypatch.setattr(emission_metrics, "emission_meter", meter)
    for attribute, name in _COUNTER_INSTRUMENTS.items():
        monkeypatch.setattr(
            emission_metrics,
            attribute,
            meter.create_counter(name=name, unit=emission_metrics.MetricUnit.EMISSIONS),
        )
    for attribute, name in _HISTOGRAM_INSTRUMENTS.items():
        monkeypatch.setattr(
            emission_metrics,
            attribute,
            meter.create_histogram(name=name, unit=emission_metrics.MetricUnit.SECONDS),
        )
    for attribute, name in _COUNT_HISTOGRAM_INSTRUMENTS.items():
        monkeypatch.setattr(
            emission_metrics,
            attribute,
            meter.create_histogram(
                name=name, unit=emission_metrics.MetricUnit.PAYLOADS
            ),
        )
    yield probes.MetricsProbe(reader=reader)
    provider.shutdown()


@pytest.fixture()
def spans(
    monkeypatch: pytest.MonkeyPatch,
) -> probes.SpanProbe:
    """Point the emission tracer at an in-memory span exporter for one test.

    Returns:
        A probe over the spans the emission code finished.
    """
    exporter = in_memory_span_exporter.InMemorySpanExporter()
    provider = otel_sdk_trace.TracerProvider()
    provider.add_span_processor(trace_export.SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        emission_tracing, "_tracer", provider.get_tracer("tangle.emissions")
    )
    return probes.SpanProbe(exporter=exporter)
