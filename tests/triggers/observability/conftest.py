"""Fixtures that point the trigger instruments at an in-memory OTel reader.

Nothing installs an OTel provider under test, so the module-level meter in
`triggers/observability/metrics.py` is a no-op proxy that records nowhere. The fixture here
swaps in a real SDK provider backed by an in-memory reader, so a test can assert on what the
production code actually recorded. It patches the module attributes rather than the global
provider because the global one can only be set once per process, which would leak the first
test's reader into every test after it.

The read-back probe is the emission one, imported rather than copied: it walks the SDK's
collected shape, which is the same whichever meter produced it.
"""

import collections.abc
import typing

import pytest
import sqlalchemy
from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk.metrics import export as metrics_export
from sqlalchemy import orm

from cloud_pipelines_backend.templating.arguments.observability import (
    metrics as template_metrics,
)
from tests.emissions.observability import probes
from cloud_pipelines_backend.triggers.observability import metrics as trigger_metrics

# The instrument attributes on triggers.observability.metrics, mapped to the metric names they
# are declared with. The fixture re-declares each under the same name so a test reads back
# what production would emit; test_trigger_instruments.py checks this map against the names
# the module really declared, so a rename cannot quietly diverge from the tests.
_COUNTER_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "triggered": "trigger.triggered",
    "event_filled": "trigger.event_filled",
    "event_expired": "trigger.event_expired",
    "run_not_started": "trigger.run_not_started",
    "cycle_collisions": "trigger.cycle_collisions",
}


# The templating instruments, redeclared on the same reader. A trigger firing renders
# arguments, so asserting that a counted trigger did *not* count a rendered key needs both
# meters visible to one probe -- otherwise `template.keys_rendered` records to the no-op
# proxy and an assertion that it stayed empty passes for the wrong reason.
_TEMPLATE_COUNTER_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "keys_rendered": "template.keys_rendered",
    "render_failures": "template.render_failures",
    "runs_with_a_failed_key": "template.runs_with_a_failed_key",
    "submission_rejected": "template.submission_rejected",
}


@pytest.fixture()
def counter_names() -> dict[str, str]:
    """The counter attribute names, mapped to the metric name each one declares."""
    return dict(_COUNTER_INSTRUMENTS)


@pytest.fixture()
def metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> collections.abc.Generator[probes.MetricsProbe, None, None]:
    """Redeclare every trigger instrument on an in-memory meter provider.

    Yields:
        A probe over the instruments the trigger code recorded through.
    """
    reader = metrics_export.InMemoryMetricReader()
    provider = otel_sdk_metrics.MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("tangle.triggers")
    # The gauge is built on demand from the module's meter, so patch the meter itself too.
    monkeypatch.setattr(trigger_metrics, "trigger_meter", meter)
    for attribute, name in _COUNTER_INSTRUMENTS.items():
        monkeypatch.setattr(
            trigger_metrics,
            attribute,
            meter.create_counter(name=name, unit=trigger_metrics.MetricUnit.EVENTS),
        )
    template_meter = provider.get_meter("tangle.templating")
    for attribute, name in _TEMPLATE_COUNTER_INSTRUMENTS.items():
        monkeypatch.setattr(
            template_metrics,
            attribute,
            template_meter.create_counter(
                name=name, unit=template_metrics.MetricUnit.KEYS
            ),
        )
    yield probes.MetricsProbe(reader=reader)
    provider.shutdown()


@pytest.fixture()
def session_factory(db_engine: sqlalchemy.Engine) -> orm.sessionmaker:
    """Sessions on the shared in-memory engine, matching the consumer's settings.

    Autoflush and autocommit are off for the same reason `emissions/consumer_main.py` has them
    off: a fixture that differs from production hides the missing flush rather than catching
    it.
    """
    return orm.sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
