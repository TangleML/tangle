"""Unit tests for the readiness vocabulary — its parser, its serializer, and their agreement.

Named for the module under test rather than matching it exactly: pytest imports test modules by
basename, so a second test_annotations.py would collide with the one covering the central
parser.
"""

import logging

import pytest

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions import annotations as emission_annotations
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness_annotations,
)

CES = bts.ContainerExecutionStatus
RA = readiness_annotations.ReadinessAnnotation
RSA = readiness_annotations.ReadinessSinkAnnotation
CODE = readiness_annotations.ReadinessParseCode
_DECLARED = emission_annotations.SINK_DECLARED_VALUE
_START_RUN = (RSA.START_PIPELINE_RUN,)


class TestParse:
    """One parser, both sides: a node's TaskSpec annotations and an event's stored rows
    carry the same keys."""

    def test_default_on_status(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RSA.START_PIPELINE_RUN: "true",
            },
        )
        assert result.intent == readiness_annotations.ReadinessIntent(
            event_key="orders-ready",
            sinks=_START_RUN,
            on_status=CES.SUCCEEDED,
        )
        assert result.issues == []

    def test_explicit_terminal_succeeded(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RA.ON_STATUS: "SUCCEEDED",
                RSA.START_PIPELINE_RUN: "true",
            },
        )
        assert result.intent is not None
        assert result.intent.on_status is CES.SUCCEEDED
        assert result.issues == []

    def test_other_readiness_keys_without_the_event_key_are_reported(
        self,
    ) -> None:
        # A readiness key is set, so the node meant to signal readiness. The event key is what
        # opts it in, so leaving that one out is a typo rather than an absence of intent, and
        # nothing downstream can report it: no row is written at all.
        for annotations in (
            {RA.ON_STATUS: "SUCCEEDED"},
            {RSA.START_PIPELINE_RUN: "true"},
            {f"{readiness_annotations.SINK_PREFIX}not-a-sink": "true"},
        ):
            result = readiness_annotations.parse_readiness(annotations=annotations)
            assert result.intent is None
            assert len(result.issues) == 1
            issue = result.issues[0]
            assert issue.code is CODE.NO_EVENT_KEY
            assert issue.dropped is True
            # The message names the key to add, so the log line states the fix.
            assert RA.EVENT.value in issue.message

    def test_naming_no_readiness_key_at_all_is_silent(
        self,
    ) -> None:
        # How a node opts out, which is nearly every node in the fleet: a finding here would
        # bury the real ones. Another kind's keys are not readiness's business either.
        for annotations in (
            {},
            {"tangleml.com/scheduling/cron": "0 * * * *"},
            {
                f"{emission_annotations.PREFIX}metadata/example-collector/"
                "report-payloads-output-name": "freshness_report"
            },
        ):
            result = readiness_annotations.parse_readiness(annotations=annotations)
            assert result.intent is None
            assert result.issues == []

    def test_blank_event_dropped(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={RA.EVENT: "   ", RSA.START_PIPELINE_RUN: "true"},
        )
        assert result.intent is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code is CODE.BLANK_EVENT_KEY
        assert issue.dropped is True

    def test_non_terminal_on_status_kept_with_issue(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RA.ON_STATUS: "RUNNING",
                RSA.START_PIPELINE_RUN: "true",
            },
        )
        assert result.intent is not None
        assert result.intent.on_status is CES.RUNNING
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code is CODE.NON_TERMINAL_ON_STATUS
        assert issue.dropped is False

    def test_terminal_not_succeeded_kept_with_issue(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RA.ON_STATUS: "FAILED",
                RSA.START_PIPELINE_RUN: "true",
            },
        )
        assert result.intent is not None
        assert result.intent.on_status is CES.FAILED
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code is CODE.NON_SUCCEEDED_ON_STATUS
        assert issue.dropped is False

    def test_unknown_on_status_dropped(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RA.ON_STATUS: "BOGUS",
                RSA.START_PIPELINE_RUN: "true",
            },
        )
        assert result.intent is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code is CODE.UNKNOWN_ON_STATUS
        assert issue.dropped is True
        assert "BOGUS" in issue.message


class TestSinks:
    """A sink is optional and defaulted, declared by the one token, and reported when nothing can serve it."""

    def test_no_sink_key_takes_the_default_sink(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={RA.EVENT: "orders-ready"},
        )
        assert result.intent is not None
        assert result.intent.sinks == _START_RUN
        assert result.issues == []

    @pytest.mark.parametrize("value", ["1", "yes", "false", "", "  "])
    def test_only_the_declared_token_opts_a_sink_in(
        self,
        value: str,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RSA.START_PIPELINE_RUN: value,
            },
        )
        # Anything other than the token declares no sink, so the default one is used.
        assert result.intent is not None
        assert result.intent.sinks == _START_RUN
        assert result.issues == []

    def test_unknown_sink_key_beside_a_known_one_is_kept_and_reported(
        self,
    ) -> None:
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RSA.START_PIPELINE_RUN: "true",
                f"{readiness_annotations.SINK_PREFIX}from-a-newer-build": "true",
            },
        )
        assert result.intent is not None
        assert result.intent.sinks == _START_RUN
        assert [issue.code for issue in result.issues] == [CODE.UNKNOWN_SINK]
        assert result.issues[0].dropped is False
        # Carried out beside the intent: the handler reports the delivery nothing can make.
        assert result.unknown_sink_keys == (
            f"{readiness_annotations.SINK_PREFIX}from-a-newer-build",
        )

    def test_only_an_unknown_sink_key_falls_back_to_the_default(
        self,
    ) -> None:
        # The unrecognized key is still reported: it named a delivery nothing can make. The
        # intent survives on the default sink rather than being dropped.
        result = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                f"{readiness_annotations.SINK_PREFIX}from-a-newer-build": "true",
            },
        )
        assert result.intent is not None
        assert result.intent.sinks == _START_RUN
        assert [issue.code for issue in result.issues] == [CODE.UNKNOWN_SINK]
        assert result.issues[0].dropped is False
        assert result.unknown_sink_keys == (
            f"{readiness_annotations.SINK_PREFIX}from-a-newer-build",
        )


class TestToAnnotationPairs:
    def test_pairs_are_keyed_as_the_node_declared_them(
        self,
    ) -> None:
        pairs = readiness_annotations.to_annotation_pairs(
            intent=readiness_annotations.ReadinessIntent(
                event_key="orders-ready",
                sinks=_START_RUN,
                on_status=CES.FAILED,
            ),
        )
        assert pairs == [
            (RA.EVENT.value, "orders-ready"),
            (RA.ON_STATUS.value, "FAILED"),
            (RSA.START_PIPELINE_RUN.value, _DECLARED),
        ]

    def test_a_defaulted_sink_is_materialized_as_a_row(
        self,
    ) -> None:
        # The default is resolved at parse time, so the stored rows carry it as if the node had
        # written the key itself — the read side never has to re-apply the default.
        result = readiness_annotations.parse_readiness(
            annotations={RA.EVENT: "orders-ready"},
        )
        assert result.intent is not None
        assert readiness_annotations.to_annotation_pairs(intent=result.intent) == [
            (RA.EVENT.value, "orders-ready"),
            (RA.ON_STATUS.value, "SUCCEEDED"),
            (RSA.START_PIPELINE_RUN.value, _DECLARED),
        ]

    def test_default_on_status_is_written_out(
        self,
    ) -> None:
        # on_status is never None, so the row is always there for the read side to find.
        pairs = readiness_annotations.to_annotation_pairs(
            intent=readiness_annotations.ReadinessIntent(
                event_key="orders-ready", sinks=_START_RUN
            ),
        )
        assert pairs == [
            (RA.EVENT.value, "orders-ready"),
            (RA.ON_STATUS.value, "SUCCEEDED"),
            (RSA.START_PIPELINE_RUN.value, _DECLARED),
        ]


class TestRoundTrip:
    """What the serializer writes, the parser reads back as the same intent."""

    @pytest.mark.parametrize("on_status", ["SUCCEEDED", "FAILED", "RUNNING"])
    def test_stored_rows_parse_back_to_the_intent(
        self,
        on_status: str,
    ) -> None:
        from_node = readiness_annotations.parse_readiness(
            annotations={
                RA.EVENT: "orders-ready",
                RA.ON_STATUS: on_status,
                RSA.START_PIPELINE_RUN: "TRUE",
            },
        )
        assert from_node.intent is not None

        stored = dict(
            readiness_annotations.to_annotation_pairs(intent=from_node.intent)
        )
        from_stored = readiness_annotations.parse_readiness(annotations=stored)

        assert from_stored.intent == from_node.intent
        assert [issue.code for issue in from_stored.issues] == [
            issue.code for issue in from_node.issues
        ]

    def test_parsing_never_logs(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            readiness_annotations.parse_readiness(
                annotations={RA.EVENT: "   ", RSA.START_PIPELINE_RUN: "true"}
            )

        # Issues are returned, not logged: the caller logs them with the node or row id.
        assert caplog.records == []
