"""Unit tests for emissions.annotations — the vocabulary every kind of emission shares.

Each kind's own keys and parsing are covered under tests/emissions/handlers/<kind>/.
"""

import enum

import pytest

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import annotations, db_models

CES = bts.ContainerExecutionStatus

# The shared helpers take their codes from the caller, so these stand in for the handler code set
# the calling parser would pass. The strings are the ones a handler's code enum carries.
UNKNOWN_SINK_CODE = "unknown_sink"
NO_EVENT_KEY_CODE = "no_event_key"

# A prefix pair, an event key and a sink enum of the shape the shared helpers take, owned by this
# file. They are parameterized on all of them, so exercising them needs no handler's vocabulary.
FAKE_KIND_PREFIX = f"{annotations.PREFIX}fake/"
FAKE_SINK_PREFIX = f"{FAKE_KIND_PREFIX}sink/"
FAKE_EVENT_KEY = f"{FAKE_KIND_PREFIX}event"


class _FakeSinkAnnotation(str, enum.Enum):
    """Two sinks, for the behaviour a single-member enum cannot show.

    Collecting more than one sink, and the order they come back in, both need two members.
    Nothing checks the enum's identity, so any enum whose members are complete keys works.
    """

    FIRST = f"{FAKE_SINK_PREFIX}zzz-declared-first"
    SECOND = f"{FAKE_SINK_PREFIX}aaa-declared-second"


class TestPrefix:
    def test_is_a_path_namespace(
        self,
    ) -> None:
        # Keys are built by concatenation, so the trailing separator has to be part of it.
        assert annotations.PREFIX == "tangleml.com/emission/"


class TestCleanStr:
    def test_strip_and_blank(
        self,
    ) -> None:
        assert annotations.clean_str(value="  padded  ") == "padded"
        assert annotations.clean_str(value="   ") is None
        assert annotations.clean_str(value=None) is None

    def test_non_string_values_are_stringified(
        self,
    ) -> None:
        # Annotations arrive from parsed YAML, so a value can come through as a number.
        assert annotations.clean_str(value=12000) == "12000"


class TestIsTrue:
    def test_accepted(
        self,
    ) -> None:
        for value in ("true", "TRUE", "True", " true ", " True "):
            assert annotations.is_true(value=value) is True

    def test_rejected(
        self,
    ) -> None:
        # "1" and "yes" mean yes to a person and nothing to this parser: the producer writes
        # "true" and only "true", so a wider vocabulary would only match a hand-edited row.
        for value in (
            "1",
            "yes",
            "y",
            "on",
            "false",
            "FALSE",
            "",
            "   ",
            None,
            1,
            0,
        ):
            assert annotations.is_true(value=value) is False

    def test_yaml_unquoted_true_accepted(
        self,
    ) -> None:
        # An unquoted `true` in YAML arrives as a bool, which stringifies to "True".
        assert annotations.is_true(value=True) is True
        assert annotations.is_true(value=False) is False


class TestParseSinks:
    def test_two_sinks_declared(
        self,
    ) -> None:
        sinks, unknown, issues = annotations.parse_sinks(
            annotations={
                _FakeSinkAnnotation.FIRST: "true",
                _FakeSinkAnnotation.SECOND: "TRUE",
            },
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert sinks == (_FakeSinkAnnotation.FIRST, _FakeSinkAnnotation.SECOND)
        assert unknown == ()
        assert issues == []

    def test_declaration_order_not_annotation_order(
        self,
    ) -> None:
        # The member values sort the other way round, so this pins declaration order rather
        # than either the node's insertion order or an alphabetical accident.
        sinks, _, _ = annotations.parse_sinks(
            annotations={
                _FakeSinkAnnotation.SECOND: "true",
                _FakeSinkAnnotation.FIRST: "true",
            },
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert sinks == (_FakeSinkAnnotation.FIRST, _FakeSinkAnnotation.SECOND)

    def test_one_of_two_declared(
        self,
    ) -> None:
        sinks, _, issues = annotations.parse_sinks(
            annotations={
                _FakeSinkAnnotation.FIRST: "true",
                _FakeSinkAnnotation.SECOND: "false",
            },
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert sinks == (_FakeSinkAnnotation.FIRST,)
        assert issues == []

    def test_nothing_declared_takes_the_default(
        self,
    ) -> None:
        # A node that names no deliverable sink still has one destination, so nothing is
        # dropped and there is no issue to report.
        sinks, unknown, issues = annotations.parse_sinks(
            annotations={_FakeSinkAnnotation.FIRST: "false"},
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert sinks == (_FakeSinkAnnotation.SECOND,)
        assert unknown == ()
        assert issues == []

    def test_unknown_key_beside_a_declared_one_is_kept_and_reported(
        self,
    ) -> None:
        unknown_key = f"{FAKE_SINK_PREFIX}not-a-sink"
        sinks, unknown, issues = annotations.parse_sinks(
            annotations={
                _FakeSinkAnnotation.FIRST: "true",
                unknown_key: "true",
            },
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert sinks == (_FakeSinkAnnotation.FIRST,)
        assert unknown == (unknown_key,)
        assert len(issues) == 1
        assert issues[0].code == UNKNOWN_SINK_CODE
        assert issues[0].dropped is False

    def test_unknown_key_alone_defaults_and_still_names_the_key(
        self,
    ) -> None:
        # The default keeps the emission deliverable, but the key that was meant to be a sink
        # is still what makes the mistake legible in the log line.
        unknown_key = f"{FAKE_SINK_PREFIX}not-a-sink"
        sinks, unknown, issues = annotations.parse_sinks(
            annotations={unknown_key: "true"},
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert sinks == (_FakeSinkAnnotation.SECOND,)
        assert unknown == (unknown_key,)
        assert issues[0].code == UNKNOWN_SINK_CODE
        assert issues[0].dropped is False
        assert unknown_key in issues[0].message

    def test_unknown_keys_sorted_and_prefix_scoped(
        self,
    ) -> None:
        _, unknown, _ = annotations.parse_sinks(
            annotations={
                _FakeSinkAnnotation.FIRST: "true",
                f"{FAKE_SINK_PREFIX}b-unknown": "true",
                f"{FAKE_SINK_PREFIX}a-unknown": "true",
                # Out of scope: another prefix's sink key, and a key that is not a sink at all.
                f"{annotations.PREFIX}other/sink/somewhere": "true",
                f"{annotations.PREFIX}fake/event": "model-freshness",
            },
            prefix=FAKE_SINK_PREFIX,
            sink_annotation=_FakeSinkAnnotation,
            kind=db_models.EmissionType.METADATA,
            default_sink=_FakeSinkAnnotation.SECOND,
            unknown_sink_code=UNKNOWN_SINK_CODE,
        )
        assert unknown == (
            f"{FAKE_SINK_PREFIX}a-unknown",
            f"{FAKE_SINK_PREFIX}b-unknown",
        )


class TestParseMissingEventKey:
    """The gate that separates a typo from opting out, exercised on its own."""

    def _issues(
        self,
        *,
        annotations_dict: dict,
    ) -> list[handler_base.ParseIssue]:
        return annotations.parse_missing_event_key(
            annotations=annotations_dict,
            kind_prefix=FAKE_KIND_PREFIX,
            kind=db_models.EmissionType.METADATA,
            event_key=FAKE_EVENT_KEY,
            no_event_key_code=NO_EVENT_KEY_CODE,
        )

    def test_naming_none_of_the_kind_is_silent(
        self,
    ) -> None:
        # How a node opts out, which is nearly every node in the fleet: a finding here would bury
        # the real ones. Another prefix's keys are not this kind's business either.
        for annotations_dict in (
            {},
            {"tangleml.com/scheduling/cron": "0 * * * *"},
            {f"{annotations.PREFIX}other/event": "orders-ready"},
        ):
            assert self._issues(annotations_dict=annotations_dict) == []

    def test_any_key_of_the_kind_is_enough_to_report(
        self,
    ) -> None:
        # Every key of the kind counts, not only a sink: each one says the node meant to opt in.
        for key in (
            f"{FAKE_KIND_PREFIX}on-status",
            _FakeSinkAnnotation.FIRST.value,
            f"{FAKE_SINK_PREFIX}not-a-sink",
        ):
            issues = self._issues(annotations_dict={key: "true"})
            assert len(issues) == 1
            assert issues[0].code == NO_EVENT_KEY_CODE
            assert issues[0].dropped is True

    def test_the_message_names_the_kind_and_the_key_to_add(
        self,
    ) -> None:
        issues = self._issues(annotations_dict={_FakeSinkAnnotation.FIRST: "true"})
        assert db_models.EmissionType.METADATA.value in issues[0].message
        assert FAKE_EVENT_KEY in issues[0].message

    def test_a_non_string_key_does_not_raise(
        self,
    ) -> None:
        # Annotations arrive from parsed task-spec data, so a key need not be a string.
        assert self._issues(annotations_dict={7: "true"}) == []


class TestParseOnStatus:
    def test_absent_falls_back_to_the_default(
        self,
    ) -> None:
        assert annotations.parse_on_status(raw=None) is annotations.DEFAULT_ON_STATUS
        assert annotations.DEFAULT_ON_STATUS is CES.SUCCEEDED

    def test_known_status_rehydrates_to_the_enum(
        self,
    ) -> None:
        assert annotations.parse_on_status(raw="FAILED") is CES.FAILED

    def test_unknown_status_is_none(
        self,
    ) -> None:
        # None is what tells a caller to drop the intent rather than guess at the status.
        assert annotations.parse_on_status(raw="BOGUS") is None


class TestIsJsonObject:
    def test_object_is_the_only_shape_accepted(
        self,
    ) -> None:
        assert annotations.is_json_object(payload='{"rows": 12000}') is True
        assert annotations.is_json_object(payload="{}") is True
        # An array and a scalar are valid JSON, just not the shape a payload has to be.
        assert annotations.is_json_object(payload="[1,2]") is False
        assert annotations.is_json_object(payload="5") is False

    def test_undecodable_payloads_are_not_objects(
        self,
    ) -> None:
        assert annotations.is_json_object(payload="nope") is False
        assert annotations.is_json_object(payload='{"unclosed":') is False
        assert annotations.is_json_object(payload="") is False

    def test_non_standard_constants_are_rejected(
        self,
    ) -> None:
        # The decoder accepts these by default, so without the parse_constant hook each one
        # would decode to a dict and pass as a payload.
        assert annotations.is_json_object(payload='{"value": NaN}') is False
        assert annotations.is_json_object(payload='{"value": Infinity}') is False
        assert annotations.is_json_object(payload='{"value": -Infinity}') is False

    def test_payload_the_decoder_cannot_recurse_through_is_not_an_object(
        self,
    ) -> None:
        # Deep enough to exhaust the decoder's own stack. The depth that does it is a
        # property of the interpreter build rather than of sys.getrecursionlimit(), so this
        # is sized well past any of them: what it pins is that exhausting the decoder comes
        # back as False rather than as a RecursionError raised into the caller.
        depth = 100_000
        assert (
            annotations.is_json_object(
                payload='{"a":' * depth + "1" + "}" * depth,
            )
            is False
        )


class TestJsonPolicyAgreement:
    """NaN, Infinity and -Infinity are not JSON, and both ends of the pipeline say so.

    An emission crosses two JSON boundaries: a payload decoded off a node's annotations on the
    way in, and an outcome detail encoded onto the row on the way out. Python's encoder and
    decoder both accept the three non-standard constants by default, so each end opts out
    separately — `is_json_object` with `parse_constant`, `ensure_storable` with `allow_nan`.
    Nothing but this test makes the two opt-outs agree.
    """

    @pytest.mark.parametrize(
        ("payload", "value"),
        [
            ('{"value": NaN}', float("nan")),
            ('{"value": Infinity}', float("inf")),
            ('{"value": -Infinity}', float("-inf")),
        ],
    )
    def test_both_ends_reject_the_same_constants(
        self,
        payload: str,
        value: float,
    ) -> None:
        # Coming in: the payload decodes, but not to something this pipeline calls JSON.
        assert annotations.is_json_object(payload=payload) is False

        # Going out: the same constant cannot be recorded on the row.
        with pytest.raises(ValueError):
            handler_base.ensure_storable(detail={"value": value})
