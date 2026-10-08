"""Fixtures that point the quota instruments at an in-memory OTel reader.

Nothing installs an OTel provider under test, so the module-level meter in
`quota/observability/metrics.py` is a no-op proxy that records nowhere. The `metrics` fixture
swaps in a real SDK provider backed by an in-memory reader, so a test can assert on what the
production code actually recorded.

It patches the module's attributes rather than the global provider, for the reason
`tests/emissions/observability/conftest.py` gives: the global provider can only be set once
per process, so installing one would leak the first test's reader into every test after it.

The meter attribute is patched too, not just the instruments. The gauges are built on demand
from `quota_meter`, so a poller constructed during a test would otherwise attach its callbacks
to the proxy meter and report into nothing.
"""

import collections.abc
import typing

import pytest
from opentelemetry.sdk import metrics as otel_sdk_metrics
from opentelemetry.sdk.metrics import export as metrics_export

from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from tests.quota.observability import probes

# The instrument attributes on quota.observability.metrics, mapped to the metric names they
# are declared with. The `metrics` fixture re-declares each one under the same name, so a test
# reads back exactly what production would emit. `test_metrics.py` checks this map against the
# names the module really declared, which is what stops a rename quietly diverging from the
# tests that assert on it — the names are the contract a dashboard is written against.
_COUNTER_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "gate_decisions": "quota.gate_decisions",
    "promotions": "quota.promotions",
    "claims_force_deleted": "quota.claims_force_deleted",
}
_HISTOGRAM_INSTRUMENTS: typing.Final[dict[str, str]] = {
    "duration_gate": "quota.duration.gate",
    "duration_promote": "quota.duration.promote",
    "duration_wait": "quota.duration.wait",
}
# The gauges have no module attribute to patch — they are created by a factory when a poller
# is built — so they are checked by calling the factory rather than by reading an attribute.
_GAUGE_FACTORIES: typing.Final[dict[str, str]] = {
    "create_occupancy_gauge": "quota.occupancy",
    "create_capacity_gauge": "quota.capacity",
    "create_waiters_gauge": "quota.waiters",
    "create_oldest_waiter_age_gauge": "quota.oldest_waiter_age",
    "create_active_claims_gauge": "quota.active_claims",
    "create_oldest_active_age_gauge": "quota.oldest_active_age",
}


@pytest.fixture()
def instrument_names() -> dict[str, str]:
    """The instrument attribute names, mapped to the metric name each one declares."""
    return {**_COUNTER_INSTRUMENTS, **_HISTOGRAM_INSTRUMENTS}


@pytest.fixture()
def gauge_factory_names() -> dict[str, str]:
    """The gauge factory names, mapped to the metric name each one creates."""
    return dict(_GAUGE_FACTORIES)


@pytest.fixture()
def metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> collections.abc.Generator[probes.MetricsProbe, None, None]:
    """Redeclare every quota instrument on an in-memory meter provider.

    Yields:
        A probe over the instruments the quota code recorded through.
    """
    reader = metrics_export.InMemoryMetricReader()
    provider = otel_sdk_metrics.MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("tangle.quota")
    monkeypatch.setattr(quota_metrics, "quota_meter", meter)
    for attribute, name in _COUNTER_INSTRUMENTS.items():
        monkeypatch.setattr(
            quota_metrics,
            attribute,
            meter.create_counter(name=name, unit=quota_metrics.MetricUnit.CLAIMS),
        )
    for attribute, name in _HISTOGRAM_INSTRUMENTS.items():
        monkeypatch.setattr(
            quota_metrics,
            attribute,
            meter.create_histogram(name=name, unit=quota_metrics.MetricUnit.SECONDS),
        )
    yield probes.MetricsProbe(reader=reader)
    provider.shutdown()
