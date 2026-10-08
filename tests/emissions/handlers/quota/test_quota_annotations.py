"""Unit tests for the quota vocabulary — both halves of it.

The two halves are tested as what they are. The orchestration half is a pure string read the
admission gate depends on, so its edge cases are about malformed input. The emissions half is
the producer's gate and the handler's round trip, so its cases are about the firing rule and
what survives a trip through the annotation rows.

The firing rule is the interesting one, and it used to live in `quota/sink.py`: a slot is freed
by a node that *ended*, however it ended, so CANCELLED counts and CANCELLING does not.
"""

from typing import Any

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions import annotations as emission_annotations
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)

Status = bts.ContainerExecutionStatus
KEY = quota_annotations.QUOTA_GROUP_KEY


class TestParseQuotaGroup:
    def test_a_declared_group_is_returned(self) -> None:
        assert quota_annotations.parse_quota_group(annotations={KEY: "bq"}) == "bq"

    def test_no_annotation_is_no_group(self) -> None:
        assert quota_annotations.parse_quota_group(annotations={}) is None

    def test_surrounding_whitespace_is_stripped(self) -> None:
        assert quota_annotations.parse_quota_group(annotations={KEY: "  bq \n"}) == "bq"

    def test_a_blank_value_is_absent_rather_than_a_group_named_empty(
        self,
    ) -> None:
        # A group named "" can never be created, so gating a node on one would strand it.
        assert quota_annotations.parse_quota_group(annotations={KEY: "   "}) is None

    def test_a_non_string_value_is_coerced(self) -> None:
        # TaskSpec values are arbitrary JSON, so a YAML author writing a bare number gets a
        # group name rather than a crash on the launch path.
        assert quota_annotations.parse_quota_group(annotations={KEY: 4}) == "4"


class TestParseTaskSpecQuotaGroup:
    def test_it_reads_through_the_task_spec(self) -> None:
        spec = {"annotations": {KEY: "bq"}}
        assert quota_annotations.parse_task_spec_quota_group(task_spec=spec) == "bq"

    def test_a_malformed_task_spec_is_ungated_rather_than_an_error(
        self,
    ) -> None:
        """Every one of these must answer None, because this runs on the launch path.

        Raising here would take down the orchestrator's launch of an unrelated node, so a
        task_spec nobody validated resolves to "no group" — the node launches ungated.
        """
        for spec in (
            None,
            "not-a-dict",
            7,
            {},
            {"annotations": None},
            {"annotations": "no"},
        ):
            assert quota_annotations.parse_task_spec_quota_group(task_spec=spec) is None


class TestTheFiringRule:
    """`QuotaIntent.matches` — when a completion frees a slot."""

    def _intent(self) -> quota_annotations.QuotaIntent:
        return quota_annotations.QuotaIntent(quota_group="bq")

    def test_every_ended_status_frees_the_slot(self) -> None:
        # Read off upstream's own set rather than a list retyped here, so a status added
        # upstream is covered without this test being edited.
        for status in bts.CONTAINER_STATUSES_ENDED:
            assert self._intent().matches(node_status=status), status

    def test_succeeded_frees_the_slot(self) -> None:
        assert self._intent().matches(node_status=Status.SUCCEEDED)

    def test_cancelled_frees_the_slot(self) -> None:
        """A launcher callback never sees this one, which is why the rule is status-based."""
        assert self._intent().matches(node_status=Status.CANCELLED)

    def test_skipped_frees_the_slot(self) -> None:
        """A skipped node never ran, but its claim is real and must not strand the group."""
        assert self._intent().matches(node_status=Status.SKIPPED)

    def test_cancelling_is_not_terminal_and_frees_nothing(self) -> None:
        """CANCELLING is deliberately absent from CONTAINER_STATUSES_ENDED: the container is
        still being torn down and still holds the resource this group protects."""
        assert not self._intent().matches(node_status=Status.CANCELLING)

    def test_running_frees_nothing(self) -> None:
        assert not self._intent().matches(node_status=Status.RUNNING)

    def test_the_intent_declares_no_on_status_at_all(self) -> None:
        # The point of subclassing EmissionIntent rather than SingleStatusIntent: quota's rule
        # is not a user's choice, so there is no field for a user to set.
        assert not hasattr(self._intent(), "on_status")


class TestParseQuota:
    def test_membership_alone_produces_an_intent(self) -> None:
        result = quota_annotations.parse_quota(annotations={KEY: "bq"})
        assert result.intent is not None
        assert result.intent.quota_group == "bq"
        assert result.issues == []

    def test_no_sink_annotation_is_required_to_opt_in(self) -> None:
        """Membership *is* the opt-in. A node never writes a quota sink key."""
        result = quota_annotations.parse_quota(annotations={KEY: "bq"})
        assert result.intent is not None
        assert result.intent.sinks == (
            quota_annotations.QuotaSinkAnnotation.PROMOTE_WAITING_NODES,
        )

    def test_a_node_in_no_group_yields_no_intent(self) -> None:
        result = quota_annotations.parse_quota(annotations={})
        assert result.intent is None
        assert [issue.code for issue in result.issues] == [
            quota_annotations.QuotaParseCode.NO_QUOTA_GROUP
        ]

    def test_a_blank_group_yields_no_intent(self) -> None:
        result = quota_annotations.parse_quota(annotations={KEY: "  "})
        assert result.intent is None

    def test_a_stray_sink_key_alone_does_not_opt_in(self) -> None:
        """Only the group key opts in, so a hand-written sink key promotes nothing."""
        annotations: dict[str, Any] = {
            quota_annotations.QuotaSinkAnnotation.PROMOTE_WAITING_NODES.value: "true"
        }
        assert quota_annotations.parse_quota(annotations=annotations).intent is None


class TestToAnnotationPairs:
    def test_it_writes_the_group_and_the_sink_key(self) -> None:
        intent = quota_annotations.QuotaIntent(quota_group="bq")
        assert quota_annotations.to_annotation_pairs(intent=intent) == [
            (KEY, "bq"),
            (
                quota_annotations.QuotaSinkAnnotation.PROMOTE_WAITING_NODES.value,
                emission_annotations.SINK_DECLARED_VALUE,
            ),
        ]

    def test_it_writes_no_on_status_pair(self) -> None:
        # There is no status to serialize, so there is no round trip for one to break.
        intent = quota_annotations.QuotaIntent(quota_group="bq")
        keys = [key for key, _ in quota_annotations.to_annotation_pairs(intent=intent)]
        assert not any("on-status" in key for key in keys)

    def test_the_rows_parse_back_to_the_same_intent(self) -> None:
        """The producer writes these rows and the handler parses them, so the trip must close."""
        original = quota_annotations.QuotaIntent(quota_group="bq")
        rows = dict(quota_annotations.to_annotation_pairs(intent=original))
        assert quota_annotations.parse_quota(annotations=rows).intent == original
