"""Tests for the quota meter: what it declares, and what recording through it does."""

import logging

import pytest
from opentelemetry import metrics as otel_metrics

from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from tests.quota.observability import probes

_METRICS_LOGGER = "cloud_pipelines_backend.quota.observability.metrics"
_GATE_DECISIONS = "quota.gate_decisions"
_PROMOTIONS = "quota.promotions"
_DURATION_GATE = "quota.duration.gate"
_GPU = "gpu"


class _CountingInstrument:
    """An instrument that remembers each amount it was handed, one entry per call.

    A counter's exported value cannot tell `add(4)` from four `add(1)`s — both sum to 4 — so
    the only way to pin "one call, not n" is to count the calls.
    """

    def __init__(
        self,
    ) -> None:
        self.calls: list[int] = []

    def add(
        self,
        amount: int,
        attributes: dict[str, str] | None = None,
    ) -> None:
        self.calls.append(amount)


class _BrokenInstrument:
    """An instrument whose every call fails, standing in for a broken metrics pipeline."""

    def add(
        self,
        amount: int,
        attributes: dict[str, str] | None = None,
    ) -> None:
        raise RuntimeError("boom in the metrics pipeline")

    def record(
        self,
        amount: float,
        attributes: dict[str, str] | None = None,
    ) -> None:
        raise RuntimeError("boom in the metrics pipeline")


class TestDeclaredInstruments:
    """The names are the contract: dashboards and alerts are written against them."""

    def test_every_instrument_is_declared_under_its_documented_name(
        self,
        instrument_names: dict[str, str],
    ) -> None:
        # Read off the module's own instruments rather than the ones the metrics fixture
        # rebuilds, so this fails if a name drifts from what the tests record under. With no
        # provider installed these are proxy instruments, which keep their name privately —
        # there is no public accessor to read it back from.
        declared = {
            attribute: getattr(quota_metrics, attribute)._name
            for attribute in instrument_names
        }

        assert declared == instrument_names

    def test_the_map_knows_about_every_instrument_the_module_declares(
        self,
        instrument_names: dict[str, str],
    ) -> None:
        # The check above only runs one way, and one way is not enough: an instrument added to
        # the module but not to the map is not merely undocumented, it is unpatched by the
        # `metrics` fixture, so any test asserting on it reads an empty in-memory reader and
        # passes for the wrong reason. Found by review: `quota.duration.wait` was added and
        # the map was not, and nothing went red.
        on_the_module = {
            name
            for name in dir(quota_metrics)
            if isinstance(
                getattr(quota_metrics, name),
                (otel_metrics.Counter, otel_metrics.Histogram),
            )
        }

        assert on_the_module == set(instrument_names)

    def test_the_map_knows_about_every_gauge_factory_the_module_declares(
        self,
        gauge_factory_names: dict[str, str],
    ) -> None:
        # The same hole on the gauge side, where it is worse: a poller registers its callbacks
        # in its constructor, so an unpatched factory reports into the proxy meter for the
        # whole test and every assertion about that gauge silently reads zero.
        on_the_module = {
            name
            for name in dir(quota_metrics)
            if name.startswith("create_") and name.endswith("_gauge")
        }

        assert on_the_module == set(gauge_factory_names)

    def test_every_gauge_factory_creates_its_documented_name(
        self,
        gauge_factory_names: dict[str, str],
        metrics: probes.MetricsProbe,
    ) -> None:
        # The gauges have no module attribute to read, so the factory is called and the name
        # taken off what it built. Assigned into a list because the caller holding the
        # instrument is what keeps it alive to be observed through. `.name` rather than the
        # `._name` used above: the `metrics` fixture has installed a real provider, so these
        # are SDK instruments with a public accessor, not the proxies the module declares.
        gauges = [
            getattr(quota_metrics, factory)(callback=lambda _options: [])
            for factory in gauge_factory_names
        ]

        assert [gauge.name for gauge in gauges] == list(gauge_factory_names.values())

    def test_the_meter_is_the_one_the_dashboards_scrape(
        self,
    ) -> None:
        # `tangle.quota`, beside `tangle.emissions`. A rename here silently empties every
        # panel, because the scope name is part of what Prometheus exports.
        assert quota_metrics.quota_meter._name == "tangle.quota"


class TestGateDecisionVocabulary:
    """The verdicts, and the sentinel that keeps an unknown name out of a label."""

    def test_the_decisions_are_exactly_the_ones_the_gate_can_reach(
        self,
    ) -> None:
        # Pinned as a list rather than parametrized over the enum, because a dashboard
        # subtracting `admitted` from the total is relying on this being exhaustive: a member
        # added without a panel is as much a defect as one deleted with a panel still on it.
        assert [decision.value for decision in quota_metrics.GateDecision] == [
            "ADMITTED",
            "PARKED",
            "REPARKED",
            "UNGATED",
            "CONTENDED",
            "ERROR",
        ]

    def test_the_missing_group_sentinel_is_not_a_legal_group_name(
        self,
    ) -> None:
        # Group names come from annotations, and the sentinel has to be a string no operator
        # could create — otherwise a real group could collide with the ungated series.
        assert quota_metrics.MISSING_GROUP == "<missing>"


class TestAdd:
    """`add` is the counter path, and it is deliberately one call per batch."""

    def test_a_batch_is_one_call_rather_than_one_per_node(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        instrument = _CountingInstrument()

        quota_metrics.add(counter=instrument, amount=8, attributes={})

        assert instrument.calls == [8]

    @pytest.mark.parametrize("amount", [0, -1])
    def test_a_pass_that_promoted_nothing_records_nothing(
        self,
        amount: int,
    ) -> None:
        # Zero would buy the SDK's per-instrument lock for no data point, and the SDK rejects
        # a negative delta on a monotonic counter outright. A promotion pass that found no
        # waiters is the common case, so this is the hot path, not an edge case.
        instrument = _CountingInstrument()

        quota_metrics.add(counter=instrument, amount=amount, attributes={})

        assert instrument.calls == []

    def test_a_broken_counter_costs_a_log_line_and_nothing_else(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # The whole reason these helpers exist: measuring the gate must never change what the
        # gate decided, so an exception here would be a metric failing a launch.
        with caplog.at_level(logging.WARNING, logger=_METRICS_LOGGER):
            quota_metrics.add(
                counter=_BrokenInstrument(),
                amount=1,
                attributes={"quota_group": _GPU},
            )

        assert "Failed to add 1 to quota counter" in caplog.text

    def test_a_broken_histogram_costs_a_log_line_and_nothing_else(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_METRICS_LOGGER):
            quota_metrics.record(
                histogram=_BrokenInstrument(), seconds=0.5, quota_group=_GPU
            )

        assert "Failed to record quota duration for gpu" in caplog.text


class TestRecordingThrough:
    """What actually arrives at the reader when production code records."""

    def test_a_counter_carries_the_labels_it_was_given(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        quota_metrics.increment(
            counter=quota_metrics.gate_decisions,
            attributes={"quota_group": _GPU, "decision": "ADMITTED"},
        )

        point = metrics.point(name=_GATE_DECISIONS, attributes={"decision": "ADMITTED"})
        assert point.value == 1
        assert point.attributes["quota_group"] == _GPU

    def test_two_decisions_on_one_group_stay_two_series(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The point of an outcome label: the four verdicts share an instrument but must not
        # share a series, or "how many were turned away" is unanswerable.
        for decision in ("ADMITTED", "PARKED"):
            quota_metrics.increment(
                counter=quota_metrics.gate_decisions,
                attributes={"quota_group": _GPU, "decision": decision},
            )

        assert len(metrics.points(name=_GATE_DECISIONS)) == 2

    def test_a_duration_lands_on_the_histogram_under_its_group(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        quota_metrics.record(
            histogram=quota_metrics.duration_gate,
            seconds=0.25,
            quota_group=_GPU,
        )

        point = metrics.point(name=_DURATION_GATE, attributes={"quota_group": _GPU})
        assert point.count == 1
        assert point.sum == pytest.approx(0.25)

    def test_the_promotions_counter_reports_nodes_not_passes(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        quota_metrics.add(
            counter=quota_metrics.promotions,
            amount=5,
            attributes={"quota_group": _GPU, "trigger": "SINK"},
        )

        assert (
            metrics.point(name=_PROMOTIONS, attributes={"trigger": "SINK"}).value == 5
        )
