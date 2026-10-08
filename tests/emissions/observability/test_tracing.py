"""Tests for the emission spans: what they are named, what they carry, and where they sit."""

import logging
import typing

import pytest

from cloud_pipelines_backend.emissions.observability import tracing as emission_tracing
from tests.emissions.observability import probes

_TRACING_LOGGER = "cloud_pipelines_backend.emissions.observability.tracing"
_READINESS = "readiness"


class _BrokenTracer:
    """A tracer that cannot start a span, standing in for a broken tracing pipeline."""

    def start_as_current_span(
        self,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        raise RuntimeError("boom in the tracing pipeline")

    def start_span(
        self,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        raise RuntimeError("boom in the tracing pipeline")


class TestProducerSpan:
    """The write side is keyed on the node, since no emission event id exists yet."""

    def test_names_the_node_the_status_change_was_for(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_tracing.producer_span(execution_node_id="node-1"):
            pass

        span = spans.named(name="emission.producer")
        assert span.attributes["execution_node_id"] == "node-1"


class TestConsumerSpan:
    """The consumer's parent span opens before it knows what it is for, then gets named."""

    def test_carries_the_emission_it_turned_out_to_be_for(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_tracing.consumer_span(start_time_ns=1_000) as handle:
            handle.identify(emission_event_id="em-1", emission_type=_READINESS)

        span = spans.named(name="emission.consumer")
        assert span.attributes[emission_tracing.EMISSION_EVENT_ID_ATTRIBUTE] == "em-1"
        assert span.attributes[emission_tracing.EMISSION_TYPE_ATTRIBUTE] == _READINESS

    def test_starts_when_the_cycle_did_rather_than_when_the_span_opened(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        # The poll runs before the span can exist, so the cycle's real start is passed in and
        # the span is backdated to cover it.
        with emission_tracing.consumer_span(start_time_ns=1_000) as handle:
            handle.identify(emission_event_id="em-1", emission_type=_READINESS)

        span = spans.named(name="emission.consumer")
        assert span.start_time == 1_000
        assert span.end_time > 1_000

    def test_an_unidentified_cycle_still_produces_its_span(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_tracing.consumer_span(start_time_ns=1_000):
            pass

        span = spans.named(name="emission.consumer")
        assert emission_tracing.EMISSION_EVENT_ID_ATTRIBUTE not in (
            span.attributes or {}
        )

    def test_stages_hang_off_the_cycle_they_ran_in(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_tracing.consumer_span(start_time_ns=1_000) as handle:
            handle.identify(emission_event_id="em-1", emission_type=_READINESS)
            emission_tracing.record_stage_span(
                stage="poll_db",
                emission_type=_READINESS,
                start_time_ns=1_000,
                end_time_ns=2_000,
            )
            with emission_tracing.stage_span(
                stage="dispatch", emission_type=_READINESS
            ):
                pass

        parent = spans.named(name="emission.consumer")
        for stage in ["poll_db", "dispatch"]:
            child = spans.named(name=stage)
            assert child.parent.span_id == parent.context.span_id
            assert child.context.trace_id == parent.context.trace_id


class TestRecordStageSpan:
    """A stage that finished before its parent existed is placed back where it ran."""

    def test_sits_at_the_timestamps_the_stage_really_ran_at(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        emission_tracing.record_stage_span(
            stage="poll_db",
            emission_type=_READINESS,
            start_time_ns=5_000,
            end_time_ns=9_000,
        )

        span = spans.named(name="poll_db")
        assert (span.start_time, span.end_time) == (5_000, 9_000)
        assert span.attributes[emission_tracing.EMISSION_TYPE_ATTRIBUTE] == _READINESS

    def test_an_instant_stage_is_still_given_width(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        # A zero-length span is dropped by some backends, so a stage too fast to measure is
        # widened rather than lost.
        emission_tracing.record_stage_span(
            stage="poll_db",
            emission_type=_READINESS,
            start_time_ns=5_000,
            end_time_ns=5_000,
        )

        span = spans.named(name="poll_db")
        assert span.end_time == 5_001


class TestStageSpan:
    """A live stage names itself, and says what kind of emission it was for when it knows."""

    def test_is_named_for_the_stage_and_labelled_with_the_emission_type(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_tracing.stage_span(stage="sink", emission_type=_READINESS):
            pass

        span = spans.named(name="sink")
        assert span.attributes[emission_tracing.EMISSION_TYPE_ATTRIBUTE] == _READINESS

    def test_an_unknown_emission_type_is_left_off_rather_than_recorded_as_none(
        self,
        spans: probes.SpanProbe,
    ) -> None:
        with emission_tracing.stage_span(stage="sink"):
            pass

        span = spans.named(name="sink")
        assert emission_tracing.EMISSION_TYPE_ATTRIBUTE not in (span.attributes or {})


class TestTracingFailureIsInert:
    """Tracing sits inside a commit on one side and the poll loop on the other."""

    def test_the_block_still_runs_when_no_span_can_be_started(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(emission_tracing, "_tracer", _BrokenTracer())
        ran = []

        with caplog.at_level(logging.WARNING, logger=_TRACING_LOGGER):
            with emission_tracing.producer_span(execution_node_id="node-1"):
                ran.append("producer")
            with emission_tracing.consumer_span(start_time_ns=1_000) as handle:
                handle.identify(emission_event_id="em-1", emission_type=_READINESS)
                ran.append("consumer")
            with emission_tracing.stage_span(stage="sink", emission_type=_READINESS):
                ran.append("sink")
            emission_tracing.record_stage_span(
                stage="poll_db",
                emission_type=_READINESS,
                start_time_ns=1_000,
                end_time_ns=2_000,
            )

        assert ran == ["producer", "consumer", "sink"]
        assert "Failed to start emission span" in caplog.text
        assert "Failed to emit emission span" in caplog.text

    def test_labelling_a_span_that_cannot_be_labelled_is_survivable(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        class _BrokenSpan:
            def set_attribute(
                self,
                key: str,
                value: object,
            ) -> None:
                raise RuntimeError("boom setting an attribute")

        handle = emission_tracing.ConsumerSpanHandle(span=_BrokenSpan())

        with caplog.at_level(logging.WARNING, logger=_TRACING_LOGGER):
            handle.identify(emission_event_id="em-1", emission_type=_READINESS)

        assert "Failed to label emission span for em-1" in caplog.text
