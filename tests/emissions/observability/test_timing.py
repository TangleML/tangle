"""Tests for stage timing: the histogram, the span, and the bag that reaches the row."""

import time

import pytest

from cloud_pipelines_backend.emissions.observability import consumer_observer
from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.emissions.observability import timing as emission_timing
from tests.emissions.observability import probes

_HANDLE = "emission.duration.handle"
_SINK = "emission.duration.sink"
_READINESS = "readiness"


class TestStageTimer:
    """One `with` produces the histogram sample, the span, and the row's number."""

    def test_records_the_stage_everywhere_it_belongs(
        self,
        metrics: probes.MetricsProbe,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_timing.timing_bag() as bag:
            with emission_timing.stage_timer(
                histogram=emission_metrics.consumer_duration_handle,
                stage="handle",
                emission_type=_READINESS,
            ):
                time.sleep(0.01)

        point = metrics.point(
            name=_HANDLE,
            attributes={emission_metrics.EMISSION_TYPE_LABEL: _READINESS},
        )
        assert point.count == 1
        assert point.sum >= 0.01
        assert bag["handle_s"] >= 0.01
        assert spans.named(name="handle") is not None

    def test_a_stage_timed_with_no_bag_open_still_reports_itself(
        self,
        metrics: probes.MetricsProbe,
        spans: probes.SpanProbe,
    ) -> None:
        # The producer times stages without a bag, as does a handler under test.
        with emission_timing.stage_timer(
            histogram=emission_metrics.consumer_duration_handle,
            stage="handle",
            emission_type=_READINESS,
        ):
            pass

        assert emission_timing.current_timings() is None
        assert metrics.point(name=_HANDLE).count == 1
        assert spans.named(name="handle") is not None

    def test_a_stage_that_raised_is_still_timed_and_the_error_still_raises(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with emission_timing.timing_bag() as bag:
            with pytest.raises(RuntimeError, match="boom in the stage"):
                with emission_timing.stage_timer(
                    histogram=emission_metrics.consumer_duration_handle,
                    stage="handle",
                    emission_type=_READINESS,
                ):
                    raise RuntimeError("boom in the stage")

        assert "handle_s" in bag
        assert metrics.point(name=_HANDLE).count == 1

    def test_nested_stages_report_nested_durations(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with emission_timing.timing_bag() as bag:
            with emission_timing.stage_timer(
                histogram=emission_metrics.consumer_duration_handle,
                stage="handle",
                emission_type=_READINESS,
            ):
                with emission_timing.stage_timer(
                    histogram=emission_metrics.consumer_duration_sink,
                    stage="sink",
                    emission_type=_READINESS,
                ):
                    time.sleep(0.01)

        # The stages contain one another rather than partitioning the cycle, which is why
        # each one is its own instrument instead of a label on a shared one.
        assert bag["handle_s"] >= bag["sink_s"]
        assert metrics.point(name=_HANDLE).sum >= metrics.point(name=_SINK).sum

    def test_a_broken_histogram_does_not_stop_the_stage_reaching_the_row(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        class _BrokenHistogram:
            def record(
                self,
                amount: float,
                attributes: dict[str, str] | None = None,
            ) -> None:
                raise RuntimeError("boom in the metrics pipeline")

        with emission_timing.timing_bag() as bag:
            with emission_timing.stage_timer(
                histogram=_BrokenHistogram(),
                stage="handle",
                emission_type=_READINESS,
            ):
                pass

        assert "handle_s" in bag
        assert spans.named(name="handle") is not None


class TestTimingBag:
    """The bag is scoped to the block that opened it, and nothing outside can see it."""

    def test_is_open_only_for_the_block_that_opened_it(
        self,
    ) -> None:
        assert emission_timing.current_timings() is None

        with emission_timing.timing_bag() as bag:
            assert emission_timing.current_timings() is bag

        assert emission_timing.current_timings() is None

    def test_an_inner_block_collects_on_its_own_and_hands_back_the_outer_one(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with emission_timing.timing_bag() as outer:
            with emission_timing.timing_bag() as inner:
                with emission_timing.stage_timer(
                    histogram=emission_metrics.consumer_duration_sink,
                    stage="sink",
                    emission_type=_READINESS,
                ):
                    pass
            with emission_timing.stage_timer(
                histogram=emission_metrics.consumer_duration_handle,
                stage="handle",
                emission_type=_READINESS,
            ):
                pass

        assert list(inner) == ["sink_s"]
        assert list(outer) == ["handle_s"]


class TestMergedRowTimings:
    """Turning a bag into the `extra_data` a row is written with."""

    def test_keeps_the_half_the_other_side_of_the_pipeline_wrote(
        self,
    ) -> None:
        # The producer's timings are already on the row by the time the consumer drains it,
        # and both halves are what makes the row's own story complete.
        produced = emission_timing.merged_row_timings(
            extra_data=None,
            role=emission_timing.PRODUCER_ROLE,
            timings={"total_s": 0.5},
        )

        consumed = emission_timing.merged_row_timings(
            extra_data=produced,
            role=emission_timing.CONSUMER_ROLE,
            timings={"total_s": 1.5, "sink_s": 1.0},
        )

        assert consumed["timings"]["producer"] == {"total_s": 0.5}
        assert consumed["timings"]["consumer"] == {
            "total_s": 1.5,
            "sink_s": 1.0,
        }

    def test_returns_a_new_dict_rather_than_reaching_into_the_old_one(
        self,
    ) -> None:
        # Change tracking on the column sees a top-level assignment, not a mutation buried
        # inside it, so timings written any other way would never be flushed.
        existing = {
            "timings": {"producer": {"total_s": 0.5}},
            "other": "untouched",
        }

        merged = emission_timing.merged_row_timings(
            extra_data=existing,
            role=emission_timing.CONSUMER_ROLE,
            timings={"total_s": 1.5},
        )

        assert merged is not existing
        assert existing == {
            "timings": {"producer": {"total_s": 0.5}},
            "other": "untouched",
        }
        assert merged["other"] == "untouched"


class TestSplittingTheBagBetweenTheTwoGrains:
    """Which timings ride the emission's row, and which ride one delivery's.

    The functions are `consumer_observer`'s, but what they do is read and consume the bag this
    module defines, so they are pinned against it here.
    """

    def test_a_delivery_takes_its_own_time_out_of_the_bag(
        self,
    ) -> None:
        with emission_timing.timing_bag() as bag:
            bag["handle_s"] = 1.0
            bag["sink_s"] = 0.5

            delivery = consumer_observer.take_delivery_timings()

            # Taken, not read: the next sink in the fan-out writes its own measurement under
            # the same key, and the delivery recorded before it must not pick that one up.
            assert consumer_observer.take_delivery_timings() is None
            assert set(bag) == {"handle_s"}
        assert delivery["timings"]["consumer"] == {"sink_s": 0.5}

    def test_a_delivery_that_timed_nothing_carries_no_timings(
        self,
    ) -> None:
        # A declared sink key with no implementation behind it is recorded without anything
        # being delivered, so there is nothing to attribute to it.
        with emission_timing.timing_bag() as bag:
            bag["handle_s"] = 1.0

            assert consumer_observer.take_delivery_timings() is None

    def test_the_event_keeps_every_stage_except_the_delivery_ones(
        self,
    ) -> None:
        extra_data = consumer_observer.row_timings(
            extra_data={"timings": {"producer": {"total_s": 0.2}}},
            timings={"handle_s": 1.0, "sink_s": 0.5, "total_s": 1.2},
        )

        assert extra_data["timings"]["consumer"] == {
            "handle_s": 1.0,
            "total_s": 1.2,
        }
        assert extra_data["timings"]["producer"] == {"total_s": 0.2}
