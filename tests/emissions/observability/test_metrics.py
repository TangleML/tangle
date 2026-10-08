"""Tests for the emission meter: what it declares, and what recording through it does."""

import logging

import pytest
from opentelemetry import metrics as otel_metrics

from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from tests.emissions.observability import probes

_METRICS_LOGGER = "cloud_pipelines_backend.emissions.observability.metrics"
_PENDING_GAUGE = "emission.pending"
_PENDING_TRANSITIONS_GAUGE = "emission.pending_transitions"
_OLDEST_PENDING_AGE = "emission.oldest_pending_age"
_OLDEST_IN_PROGRESS_AGE = "emission.oldest_in_progress_age"
_WRITTEN = "emission.written"
_HANDLED = "emission.handled"
_DELIVERED = "emission.delivered"
_SINK_DURATION = "emission.duration.sink"
_DURATION_TOTAL = "emission.duration.total"
_TRANSITIONS_DRAINED = "emission.transitions_drained"
_READINESS = "readiness"
_METADATA = "metadata"


class _CountingInstrument:
    """An instrument that remembers each amount it was handed, one entry per call.

    A counter's exported value cannot tell `add(4)` from four `add(1)`s — both sum to 4 — so the
    only way to pin "one call, not N" is to count the calls.
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
            attribute: getattr(emission_metrics, attribute)._name
            for attribute in instrument_names
        }

        assert declared == instrument_names

    def test_every_counter_the_module_declares_is_covered(
        self,
        instrument_names: dict[str, str],
    ) -> None:
        """The test above walks the map, so an instrument absent from it is unchecked.

        Without this direction a counter added to the module is never read back by any test,
        and the map's claim to list what production emits quietly stops being true.
        """
        declared = {
            name
            for name, value in vars(emission_metrics).items()
            if isinstance(value, otel_metrics.Counter)
        }

        assert declared - set(instrument_names) == set()

    def test_the_pending_gauge_is_declared_under_its_documented_name(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Assigned because the caller is what keeps the instrument alive to observe through.
        _gauge = emission_metrics.create_pending_gauge(
            callback=lambda _options: [otel_metrics.Observation(0)],
        )

        assert metrics.point(name=_PENDING_GAUGE) is not None

    def test_the_pending_transitions_gauge_is_declared_under_its_documented_name(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Assigned because the caller is what keeps the instrument alive to observe through.
        _gauge = emission_metrics.create_pending_transitions_gauge(
            callback=lambda _options: [otel_metrics.Observation(3)],
        )

        assert metrics.point(name=_PENDING_TRANSITIONS_GAUGE).value == 3

    @pytest.mark.parametrize(
        ("factory_name", "metric_name"),
        [
            ("create_oldest_pending_age_gauge", _OLDEST_PENDING_AGE),
            ("create_oldest_in_progress_age_gauge", _OLDEST_IN_PROGRESS_AGE),
        ],
    )
    def test_each_queue_age_gauge_is_declared_under_its_documented_name(
        self,
        metrics: probes.MetricsProbe,
        factory_name: str,
        metric_name: str,
    ) -> None:
        factory = getattr(emission_metrics, factory_name)

        # Assigned because the caller is what keeps the instrument alive to observe through.
        _gauge = factory(callback=lambda _options: [otel_metrics.Observation(0.0)])

        assert metrics.point(name=metric_name) is not None


class TestIncrement:
    """increment() adds exactly one, and keeps label combinations apart."""

    def test_counts_one_per_call(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        attributes = {emission_metrics.EMISSION_TYPE_LABEL: _READINESS}

        emission_metrics.increment(
            counter=emission_metrics.producer_written, attributes=attributes
        )
        emission_metrics.increment(
            counter=emission_metrics.producer_written, attributes=attributes
        )

        assert metrics.point(name=_WRITTEN, attributes=attributes).value == 2

    def test_each_label_combination_counts_separately(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        for emission_type, status in [
            (_READINESS, "complete"),
            (_READINESS, "complete"),
            (_READINESS, "incomplete"),
            (_METADATA, "complete"),
        ]:
            emission_metrics.increment(
                counter=emission_metrics.consumer_handled,
                attributes={
                    emission_metrics.EMISSION_TYPE_LABEL: emission_type,
                    emission_metrics.HANDLE_STATUS_LABEL: status,
                },
            )

        counts = {
            (
                point.attributes[emission_metrics.EMISSION_TYPE_LABEL],
                point.attributes[emission_metrics.HANDLE_STATUS_LABEL],
            ): point.value
            for point in metrics.points(name=_HANDLED)
        }
        assert counts == {
            (_READINESS, "complete"): 2,
            (_READINESS, "incomplete"): 1,
            (_METADATA, "complete"): 1,
        }

    def test_the_same_sink_counts_apart_by_what_it_reported(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # One emission can be delivered to several sinks, and each sink reports for itself, so
        # the delivery counter has to keep both apart.
        for sink_key, status in [
            (
                "tangleml.com/emission/readiness/sink/start-pipeline-run",
                "success",
            ),
            ("tangleml.com/emission/readiness/sink/start-pipeline-run", "fail"),
            ("tangleml.com/emission/metadata/sink/example-collector", "success"),
        ]:
            emission_metrics.increment(
                counter=emission_metrics.consumer_delivered,
                attributes={
                    emission_metrics.EMISSION_TYPE_LABEL: _READINESS,
                    emission_metrics.SINK_LABEL: sink_key,
                    emission_metrics.OUTCOME_STATUS_LABEL: status,
                },
            )

        counts = {
            (
                point.attributes[emission_metrics.SINK_LABEL],
                point.attributes[emission_metrics.OUTCOME_STATUS_LABEL],
            ): point.value
            for point in metrics.points(name=_DELIVERED)
        }
        assert counts == {
            (
                "tangleml.com/emission/readiness/sink/start-pipeline-run",
                "success",
            ): 1,
            (
                "tangleml.com/emission/readiness/sink/start-pipeline-run",
                "fail",
            ): 1,
            ("tangleml.com/emission/metadata/sink/example-collector", "success"): 1,
        }

    def test_a_broken_counter_costs_a_log_line_and_nothing_else(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_METRICS_LOGGER):
            emission_metrics.increment(
                counter=_BrokenInstrument(),
                attributes={emission_metrics.EMISSION_TYPE_LABEL: _READINESS},
            )

        # increment() delegates to add(), so the log line names the amount it tried to record.
        assert "Failed to add 1 to emission counter" in caplog.text


class TestCoerceReason:
    """The reason label is forced into the vocabulary before it can become a time series."""

    @pytest.mark.parametrize(
        "reason",
        [member.value for member in emission_metrics.DeliveryReason],
    )
    def test_every_member_of_the_vocabulary_passes_through_unchanged(
        self,
        reason: str,
    ) -> None:
        assert emission_metrics.coerce_reason(reason=reason) == reason

    @pytest.mark.parametrize("reason", [None, "", 0, {}])
    def test_a_sink_that_reported_nothing_is_counted_as_nothing_failed(
        self,
        reason: object,
    ) -> None:
        # A successful delivery reports no reason at all, which is the overwhelming majority
        # of calls: the default has to be `none` rather than `unknown`.
        assert (
            emission_metrics.coerce_reason(reason=reason)
            == emission_metrics.DeliveryReason.NONE.value
        )

    @pytest.mark.parametrize(
        "reason", ["collector_broke", "CUSTOMER_CONFIG", 503, object()]
    )
    def test_anything_outside_the_vocabulary_becomes_unknown(
        self,
        reason: object,
    ) -> None:
        # The label is what alerts are written against. An uncoerced value would mint its own
        # time series, so it would escape every alert *and* grow the counter's cardinality.
        assert (
            emission_metrics.coerce_reason(reason=reason)
            == emission_metrics.DeliveryReason.UNKNOWN.value
        )

    def test_an_unrecognized_reason_is_never_filed_as_nothing_failed(
        self,
    ) -> None:
        # The tempting fallback, and the wrong one: it would hide a failure under the label
        # every success carries.
        assert (
            emission_metrics.coerce_reason(reason="collector_broke")
            != emission_metrics.DeliveryReason.NONE.value
        )

    def test_an_unrecognized_reason_names_itself_in_the_log(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # `unknown` says a sink misreported but not which value it sent; the log closes that.
        with caplog.at_level(logging.WARNING, logger=_METRICS_LOGGER):
            emission_metrics.coerce_reason(reason="collector_broke")

        assert "collector_broke" in caplog.text


class TestStatusClass:
    """status_class() runs outside increment()'s wrapper, so it has to be total itself."""

    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [(200, "2xx"), (204, "2xx"), (404, "4xx"), (422, "4xx"), (503, "5xx")],
    )
    def test_a_status_is_reduced_to_its_class(
        self,
        status_code: int,
        expected: str,
    ) -> None:
        assert emission_metrics.status_class(status_code=status_code) == expected

    def test_no_answer_classifies_as_none(
        self,
    ) -> None:
        assert emission_metrics.status_class(status_code=None) == "none"

    @pytest.mark.parametrize(
        "status_code", ["503", 5.03, [503], {"code": 503}, object()]
    )
    def test_a_status_that_is_not_an_integer_classifies_as_none_instead_of_raising(
        self,
        status_code: object,
    ) -> None:
        # It arrives from a sink's free-form detail mapping, and the floor division would
        # raise on all of these. The call is an argument to increment(), so it is evaluated
        # before increment() is entered and its never-raise wrapper cannot absorb it.
        assert emission_metrics.status_class(status_code=status_code) == "none"

    @pytest.mark.parametrize("status_code", [True, False])
    def test_a_boolean_classifies_as_none_rather_than_zero_xx(
        self,
        status_code: bool,
    ) -> None:
        # `isinstance(True, int)` is True in Python, so an isinstance guard alone would let
        # `True // 100` through and report the class as `0xx`.
        assert emission_metrics.status_class(status_code=status_code) == "none"


class TestAdd:
    """add() records a whole amount in one call, and refuses to record a non-amount."""

    def test_records_the_whole_amount(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        emission_metrics.add(
            counter=emission_metrics.producer_transitions_drained,
            amount=5,
            attributes={},
        )

        assert metrics.point(name=_TRANSITIONS_DRAINED).value == 5

    def test_a_non_positive_amount_records_nothing(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Zero would buy the SDK's lock for no data point; a negative delta is rejected outright
        # by a monotonic counter, and would take the caller down with it.
        for amount in (0, -1):
            emission_metrics.add(
                counter=emission_metrics.producer_transitions_drained,
                amount=amount,
                attributes={},
            )

        assert metrics.points(name=_TRANSITIONS_DRAINED) == []

    def test_one_call_reaches_the_instrument_once(
        self,
    ) -> None:
        """The point of the helper: N transitions cost one `add`, not N of them."""
        counter = _CountingInstrument()

        emission_metrics.add(counter=counter, amount=4, attributes={})

        assert counter.calls == [4]

    def test_a_broken_counter_costs_a_log_line_and_nothing_else(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_METRICS_LOGGER):
            emission_metrics.add(counter=_BrokenInstrument(), amount=3, attributes={})

        assert "Failed to add 3 to emission counter" in caplog.text


class TestRecord:
    """record() puts the duration on the histogram under the emission type it measured."""

    def test_records_the_duration_under_the_emission_type_label(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        emission_metrics.record(
            histogram=emission_metrics.duration_total,
            seconds=0.25,
            emission_type=_READINESS,
        )
        emission_metrics.record(
            histogram=emission_metrics.duration_total,
            seconds=0.75,
            emission_type=_READINESS,
        )

        point = metrics.point(
            name=_DURATION_TOTAL,
            attributes={emission_metrics.EMISSION_TYPE_LABEL: _READINESS},
        )
        assert point.count == 2
        assert point.sum == pytest.approx(1.0)

    def test_extra_labels_are_recorded_beside_the_emission_type(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # What the sink stage needs: the same histogram, one series per sink, so a slow target
        # is attributable to itself.
        for sink_key in ["sink-a", "sink-b"]:
            emission_metrics.record(
                histogram=emission_metrics.consumer_duration_sink,
                seconds=0.5,
                emission_type=_READINESS,
                attributes={emission_metrics.SINK_LABEL: sink_key},
            )

        for sink_key in ["sink-a", "sink-b"]:
            point = metrics.point(
                name=_SINK_DURATION,
                attributes={
                    emission_metrics.EMISSION_TYPE_LABEL: _READINESS,
                    emission_metrics.SINK_LABEL: sink_key,
                },
            )
            assert point.count == 1

    def test_a_broken_histogram_costs_a_log_line_and_nothing_else(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_METRICS_LOGGER):
            emission_metrics.record(
                histogram=_BrokenInstrument(),
                seconds=0.5,
                emission_type=_READINESS,
            )

        assert "Failed to record emission duration" in caplog.text


class TestDurationTotalRole:
    """The one instrument whose role lives in an attribute keeps the two roles apart."""

    def test_producer_and_consumer_totals_are_separate_series(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Dropping `<role>` from the names merged these two, and they measure different things:
        # before-commit work versus a whole consumer cycle. Summing them would be meaningless.
        for role, seconds in (
            (emission_metrics.PRODUCER_ROLE, 0.010),
            (emission_metrics.CONSUMER_ROLE, 0.500),
        ):
            emission_metrics.record(
                histogram=emission_metrics.duration_total,
                seconds=seconds,
                emission_type=_READINESS,
                attributes={emission_metrics.ROLE_LABEL: role},
            )

        totals = {
            point.attributes[emission_metrics.ROLE_LABEL]: point.sum
            for point in metrics.points(name=_DURATION_TOTAL)
        }
        assert sorted(totals) == [
            emission_metrics.CONSUMER_ROLE,
            emission_metrics.PRODUCER_ROLE,
        ]
        assert (
            totals[emission_metrics.PRODUCER_ROLE]
            < totals[emission_metrics.CONSUMER_ROLE]
        )


class TestPendingGauge:
    """The backlog gauge reports whatever its callback says at collection time."""

    def test_reports_the_value_the_callback_observes(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        observed = {"pending": 7}
        # Assigned because the caller is what keeps the instrument alive to observe through.
        _gauge = emission_metrics.create_pending_gauge(
            callback=lambda _options: [
                otel_metrics.Observation(observed["pending"]),
            ],
        )

        assert metrics.point(name=_PENDING_GAUGE).value == 7

        # The callback runs per collection, so a drained backlog reports itself drained
        # rather than staying at the value of the first sample.
        observed["pending"] = 0
        assert metrics.point(name=_PENDING_GAUGE).value == 0
