"""Unit tests for templating.arguments.envelopes -- the ladder, without a route.

The ladder's rungs are tested here and the HTTP boundary in test_api_routes, so a change
to the 422 wrapping cannot hide a change to what is actually rejected.

    arguments_in     1-2  shape
    check_templates  3-6  pure: classify, parse, grammar, availability
    validated_arguments   both, in order -- what the two API modules actually call
"""

import datetime
import zoneinfo

import fastapi
import pytest
from starlette import status

from cloud_pipelines_backend.templating.arguments import (
    envelopes,
    errors,
    rendering,
    sources,
)

_TORONTO = zoneinfo.ZoneInfo("America/Toronto")


def _render_all(arguments: dict[str, str]) -> dict[str, str]:
    """Render an accepted map on a cron fire, so a positive test asserts a value rather
    than the absence of an exception. Slot 2026-09-02 09:00 America/Toronto."""
    clock = sources.Clock(
        kind=sources.Kind.CRON,
        trigger_time=datetime.datetime(2026, 9, 2, 9, 0, 3, tzinfo=_TORONTO),
        now=datetime.datetime(2026, 9, 2, 9, 0, 4, tzinfo=_TORONTO),
        schedule_time=datetime.datetime(2026, 9, 2, 9, 0, 0, tzinfo=_TORONTO),
    )
    return rendering.render(templates=arguments, arguments={}, clock=clock).arguments


class TestReadingTheEnvelope:
    @pytest.mark.parametrize(
        "envelope",
        [None, {}, {"arguments": None}, {"arguments": {}}],
        ids=["omitted", "empty envelope", "explicit null", "empty arguments"],
    )
    def test_the_four_ways_of_saying_no_templates_all_read_as_empty(
        self, envelope: dict[str, object] | None
    ) -> None:
        """They collapse once, here, so no caller downstream has to know the difference."""
        assert envelopes.arguments_in(envelope=envelope) == {}

    def test_the_arguments_map_is_returned(self) -> None:
        assert envelopes.arguments_in(envelope={"arguments": {"day": "{{ now }}"}}) == {
            "day": "{{ now }}"
        }

    def test_an_unknown_sibling_key_is_ignored(self) -> None:
        """So a newer client can send a key this server has never heard of and still be
        served, rather than 422ing every request during a rollout."""
        assert envelopes.arguments_in(
            envelope={"arguments": {"day": "x"}, "invented_later": {"a": 1}}
        ) == {"day": "x"}

    def test_the_returned_map_is_a_copy(self) -> None:
        """It is about to be stored; sharing the request's dict would let a later edit of
        one change the other."""
        envelope = {"arguments": {"day": "x"}}

        returned = envelopes.arguments_in(envelope=envelope)
        returned["day"] = "changed"

        assert envelope["arguments"] == {"day": "x"}

    def test_arguments_that_is_not_an_object_is_refused(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            envelopes.arguments_in(envelope={"arguments": [1, 2]})

        assert caught.value.detail == (
            "pipeline_templates.arguments must be a map of string to string; got list"
        )

    @pytest.mark.parametrize(
        ("value", "described"),
        [(3, "int"), (None, "NoneType"), (["a"], "list"), ({"a": 1}, "dict")],
        ids=["int", "null", "list", "object"],
    )
    def test_a_value_that_is_not_a_string_is_refused_and_named(
        self, value: object, described: str
    ) -> None:
        """One request carries many templates, so the message names the offender."""
        with pytest.raises(errors.TemplateError) as caught:
            envelopes.arguments_in(envelope={"arguments": {"retries": value}})

        assert caught.value.detail == (
            "pipeline_templates.arguments must be a map of string to string;"
            " 'retries' is " + described
        )


class TestTheTemplateRungs:
    def test_a_constant_is_stored_and_renders_to_itself(self) -> None:
        """Rung 3 stops there: a plain string has no grammar to walk. Asserted by
        rendering it, so "accepted at save" and "works at fire" are one claim."""
        arguments = {"region": "ca-central-1"}

        envelopes.check_templates(arguments=arguments, kind=sources.Kind.CRON)

        assert _render_all(arguments) == {"region": "ca-central-1"}

    def test_an_accepted_expression_renders_to_the_expected_value(self) -> None:
        """The positive case of the whole ladder: what save accepts, a fire produces."""
        arguments = {"as_of_date": "{{ schedule_time | shift('-1d') | date }}"}

        envelopes.check_templates(arguments=arguments, kind=sources.Kind.CRON)

        assert _render_all(arguments) == {"as_of_date": "2026-09-01"}

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("{{ schedule_time", "unexpected"),
            ("run-{{ now | date }}", "not a mix"),
            ("{{ now | upper }}", "unknown filter 'upper'"),
            ("{{ now | date | shift('-1d') }}", "after formatter"),
            ("{{ now | date | rfc3339 }}", "two formatters"),
            ('{{ "2026-09-01" | date }}', "expected a clock source"),
            ("{{ triger_time }}", "unknown source 'triger_time'"),
            ("{{ now | shift('-2 days') }}", "is not an offset"),
            ("{{ now | timezone('America/Torono') }}", "unknown timezone"),
        ],
        ids=[
            "unparseable",
            "mixed",
            "unknown filter",
            "operator after formatter",
            "two formatters",
            "literal base",
            "unknown source",
            "bad offset",
            "unknown timezone",
        ],
    )
    def test_each_grammar_fault_is_refused_with_its_own_message(
        self, value: str, expected: str
    ) -> None:
        """Section 2.4's table, walked. Each row is a distinct message because a caller
        fixing one fault should not have to guess which of nine it hit."""
        with pytest.raises(errors.TemplateError) as caught:
            envelopes.check_templates(
                arguments={"as_of_date": value}, kind=sources.Kind.CRON
            )

        assert expected in caught.value.detail
        assert "as_of_date" in caught.value.detail

    def test_schedule_time_is_storable_on_a_cron_schedule(self) -> None:
        """The mirror of the subscription case below: the same template, the one kind
        that can have a schedule_time, and it renders that."""
        arguments = {"as_of_date": "{{ schedule_time | date }}"}

        envelopes.check_templates(arguments=arguments, kind=sources.Kind.CRON)

        assert _render_all(arguments) == {"as_of_date": "2026-09-02"}

    def test_schedule_time_is_refused_on_a_subscription(self) -> None:
        """Rung 6. A subscription is never scheduled, so storing this would fail at every
        single fire -- refusing it once at save is the whole point of the rung."""
        with pytest.raises(errors.TemplateError) as caught:
            envelopes.check_templates(
                arguments={"as_of_date": "{{ schedule_time | date }}"},
                kind=sources.Kind.SUBSCRIPTION,
            )

        assert "not available for a subscription" in caught.value.detail

    def test_a_coalesce_arm_is_checked_too(self) -> None:
        """Otherwise the ban is cosmetic: wrapping the banned source in coalesce would
        smuggle it past rung 6."""
        with pytest.raises(errors.TemplateError):
            envelopes.check_templates(
                arguments={"as_of_date": "{{ coalesce(schedule_time, now) | date }}"},
                kind=sources.Kind.SUBSCRIPTION,
            )

    def test_the_first_bad_key_stops_the_climb(self) -> None:
        """One message per request. Reporting all of them would mean inventing a
        multi-error body that no other 422 in these modules uses."""
        with pytest.raises(errors.TemplateError) as caught:
            envelopes.check_templates(
                arguments={"first": "{{ nope }}", "second": "{{ also_nope }}"},
                kind=sources.Kind.CRON,
            )

        assert "first" in caught.value.detail


class TestTheWholeLadderAtOnce:
    """`validated_arguments`, the one door both API modules call.

    It is the only thing in templating/arguments that speaks HTTP: the rungs below it
    raise `TemplateError`, and this turns that into the 422 the endpoints return, so
    neither route module has to repeat the translation.
    """

    def test_a_refusal_arrives_as_a_422_rather_than_a_template_error(
        self,
    ) -> None:
        """The whole reason the try/except moved in here: the caller gets a response, and
        the ladder's own message survives into its detail unchanged."""
        with pytest.raises(fastapi.HTTPException) as caught:
            envelopes.validated_arguments(
                envelope={"arguments": {"d": 3}}, kind=sources.Kind.SUBSCRIPTION
            )
        assert caught.value.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert caught.value.detail == (
            "pipeline_templates.arguments must be a map of string to string; 'd' is int"
        )

    def test_the_shape_rungs_are_climbed_before_the_grammar_ones(self) -> None:
        """A malformed envelope is reported as malformed even when it also holds a
        template that would fail availability -- order decides which fault is named, and
        the cheaper fix is named first."""
        with pytest.raises(fastapi.HTTPException) as caught:
            envelopes.validated_arguments(
                envelope={"arguments": {"d": 3, "e": "{{ schedule_time }}"}},
                kind=sources.Kind.SUBSCRIPTION,
            )
        assert caught.value.detail == (
            "pipeline_templates.arguments must be a map of string to string; 'd' is int"
        )

    def test_the_same_envelope_passes_as_a_schedule_and_fails_as_a_subscription(
        self,
    ) -> None:
        """`kind` is the whole reason this is one function with a parameter rather than
        two functions: nothing else differs between the two callers."""
        envelope = {"arguments": {"d": "{{ schedule_time | date }}"}}
        assert envelopes.validated_arguments(
            envelope=envelope, kind=sources.Kind.CRON
        ) == {"d": "{{ schedule_time | date }}"}
        with pytest.raises(fastapi.HTTPException) as caught:
            envelopes.validated_arguments(
                envelope=envelope, kind=sources.Kind.SUBSCRIPTION
            )
        assert (
            "'schedule_time' is not available for a subscription" in caught.value.detail
        )

    def test_the_rungs_below_still_raise_the_domain_error(self) -> None:
        """`arguments_in` and `check_templates` are unchanged, so a non-HTTP caller -- a
        backfill, or these tests -- can still have the `TemplateError` itself."""
        with pytest.raises(errors.TemplateError):
            envelopes.arguments_in(envelope={"arguments": {"d": 3}})

    def test_a_missing_envelope_is_an_empty_map_rather_than_a_refusal(
        self,
    ) -> None:
        """None reaches here whenever the caller omitted the field."""
        assert (
            envelopes.validated_arguments(envelope=None, kind=sources.Kind.CRON) == {}
        )


class TestAFalsyArgumentsValueIsRefusedRatherThanReadAsEmpty:
    """`or {}` would turn `[]`, `""`, `0` and `False` into "no templates given".

    That is the same value an explicit clear produces, so on a PATCH a malformed body
    would delete the stored templates and answer 200.
    """

    @pytest.mark.parametrize(
        ("value", "type_name"),
        [([], "list"), ("", "str"), (0, "int"), (False, "bool")],
        ids=["empty-list", "empty-string", "zero", "false"],
    )
    def test_it_names_the_type_it_got(self, value: object, type_name: str) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            envelopes.arguments_in(envelope={"arguments": value})

        assert type_name in caught.value.detail

    def test_only_a_null_arguments_reads_as_no_templates(self) -> None:
        """The one falsy value that is genuinely absence, and the one that must stay {}."""
        assert envelopes.arguments_in(envelope={"arguments": None}) == {}
        assert envelopes.arguments_in(envelope={}) == {}
        assert envelopes.arguments_in(envelope=None) == {}


class TestOnlyAnEnvelopeNamingArgumentsAsksForAChange:
    """What separates "clear them" from "a key this version does not read".

    Both used to reduce to an empty map, so an envelope carrying only a newer client's
    field deleted every stored template.
    """

    @pytest.mark.parametrize(
        "envelope",
        [None, {}, {"future_key": 1}],
        ids=["omitted", "empty", "unknown-sibling-only"],
    )
    def test_an_envelope_that_names_no_arguments_is_not_an_edit(
        self, envelope: dict[str, object] | None
    ) -> None:
        assert envelopes.names_arguments(envelope=envelope) is False

    @pytest.mark.parametrize(
        "envelope",
        [
            {"arguments": {}},
            {"arguments": {"region": "ca"}},
            {"arguments": {}, "x": 1},
        ],
        ids=["explicit-clear", "a-template", "clear-beside-an-unknown"],
    )
    def test_naming_arguments_is_an_edit_even_when_it_is_empty(
        self, envelope: dict[str, object]
    ) -> None:
        assert envelopes.names_arguments(envelope=envelope) is True
