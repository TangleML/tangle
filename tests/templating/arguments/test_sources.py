"""Unit tests for templating.arguments.sources.

Two clock fixtures, and the difference between them is the whole subject:

    _with_schedule_time()          a cron schedule fired by the scheduler -- schedule_time exists
    _without_schedule_time(kind)   fired any other way                    -- schedule_time does not

Three fixed times, a few seconds apart so a test can tell which one it got:

    schedule_time  09:00:00.000000   the time the cron said was due
    trigger_time   09:00:03.250000   when the run was created
    now            09:00:04.125000   when the template was rendered

`coalesce` is exercised through a real `SandboxedEnvironment` rather than by calling it
directly, because the whole mechanism rests on Jinja2 passing an unavailable source in as
an `Undefined` instead of raising at the call. Calling it with a hand-made sentinel would
test our idea of Jinja2 rather than Jinja2.
"""

import datetime
import zoneinfo

import jinja2
import pytest
from jinja2.sandbox import SandboxedEnvironment

from cloud_pipelines_backend.templating.arguments import errors, filters, sources

_TORONTO = zoneinfo.ZoneInfo("America/Toronto")
_SCHEDULE_TIME = datetime.datetime(2026, 9, 2, 9, 0, 0, tzinfo=_TORONTO)
_TRIGGER = datetime.datetime(2026, 9, 2, 9, 0, 3, 250000, tzinfo=_TORONTO)
_NOW = datetime.datetime(2026, 9, 2, 9, 0, 4, 125000, tzinfo=_TORONTO)


def _with_schedule_time() -> sources.Clock:
    """A cron schedule fired by the scheduler: all three clocks."""
    return sources.Clock(
        kind=sources.Kind.CRON,
        trigger_time=_TRIGGER,
        now=_NOW,
        schedule_time=_SCHEDULE_TIME,
    )


def _without_schedule_time(kind: sources.Kind) -> sources.Clock:
    """A subscription, or a cron schedule fired by hand: no schedule_time."""
    return sources.Clock(kind=kind, trigger_time=_TRIGGER, now=_NOW)


def _render(value: str, *, clock: sources.Clock, key: str = "as_of_date") -> str:
    """Render one template the way the production engine does, so these tests fail if the
    wiring changes."""
    engine = SandboxedEnvironment(autoescape=False, undefined=jinja2.StrictUndefined)
    engine.filters.update(filters.as_jinja_filters())
    return engine.from_string(value).render(clock.context(key=key))


class TestAvailabilityByKind:
    @pytest.mark.parametrize(
        ("kind", "expected"),
        [
            (sources.Kind.CRON, {"schedule_time", "trigger_time", "now"}),
            (sources.Kind.SUBSCRIPTION, {"trigger_time", "now"}),
            (sources.Kind.MANUAL, {"trigger_time", "now"}),
        ],
        ids=["cron", "subscription", "manual"],
    )
    def test_only_a_cron_fire_has_a_schedule_time(
        self, kind: sources.Kind, expected: set[str]
    ) -> None:
        """The matrix, spelled out per kind. Everything else in this module follows from
        which rows contain schedule_time -- exactly one does."""
        assert {source.value for source in sources.AVAILABLE[kind]} == expected

    def test_every_kind_has_a_row_in_the_matrix(self) -> None:
        """A Kind added without a row would KeyError at render -- and only on the path that
        runs that kind, so it would pass every other test and fail in production."""
        assert set(sources.AVAILABLE) == set(sources.Kind)

    def test_schedule_time_is_the_only_clock_that_can_be_missing(self) -> None:
        """Intersect every row: trigger_time and now survive, schedule_time does not. That
        is the entire justification for coalesce -- there is nothing else to fall back
        from, and nothing else to fall back to."""
        always = set.intersection(
            *({s.value for s in v} for v in sources.AVAILABLE.values())
        )

        assert always == {"trigger_time", "now"}
        assert "schedule_time" not in always

    @pytest.mark.parametrize("kind", list(sources.Kind))
    def test_is_available_answers_exactly_what_the_matrix_says(
        self, kind: sources.Kind
    ) -> None:
        """Every source against every kind, so the helper cannot drift from the table it
        is reading."""
        for source in sources.TimeSource:
            assert sources.is_available(source=source, kind=kind) == (
                source in sources.AVAILABLE[kind]
            )

    def test_a_clock_reports_the_row_for_its_own_kind(self) -> None:
        """Clock.available() is a lookup, not a second copy of the matrix."""
        assert _with_schedule_time().available() == sources.AVAILABLE[sources.Kind.CRON]
        assert (
            _without_schedule_time(sources.Kind.SUBSCRIPTION).available()
            == sources.AVAILABLE[sources.Kind.SUBSCRIPTION]
        )


class TestSourceNames:
    def test_the_three_source_names_are_the_strings_a_user_types(self) -> None:
        """TIME_SOURCE_NAMES is what the grammar checks a template's base against, so it has to
        hold the exact spellings, not the enum members' python names."""
        assert sources.TIME_SOURCE_NAMES == frozenset(
            {"schedule_time", "trigger_time", "now"}
        )

    def test_each_member_name_matches_its_string(self) -> None:
        """SCHEDULE_TIME == "schedule_time", and so on. Only the string ever appears in a
        template, so a renamed member that forgot its value would be invisible here and
        break every stored template."""
        for source in sources.TimeSource:
            assert source.name.lower() == source.value

    def test_coalesce_is_not_itself_a_source(self) -> None:
        """It combines sources rather than being one, so `{{ coalesce }}` on its own has
        to stay an unknown source."""
        assert sources.COALESCE not in sources.TIME_SOURCE_NAMES


class TestTheClockRefusesAmbiguousInput:
    @pytest.mark.parametrize("field", ["trigger_time", "now", "schedule_time"])
    def test_a_naive_datetime_is_refused(self, field: str) -> None:
        """Checked on each of the three fields. `timezone` and `epoch_seconds` on a naive
        value do not raise -- they return a different, plausible time, which then reaches a
        partition path looking correct."""
        times = {
            "trigger_time": _TRIGGER,
            "now": _NOW,
            "schedule_time": _SCHEDULE_TIME,
        }
        times[field] = times[field].replace(tzinfo=None)

        with pytest.raises(ValueError, match=f"{field} must be timezone-aware"):
            sources.Clock(kind=sources.Kind.CRON, **times)

    def test_a_cron_clock_without_a_schedule_time_is_refused(self) -> None:
        """The scheduler always supplies one, so a missing schedule_time means the plumbing broke.
        Rendering trigger_time instead would silently write the wrong partition."""
        with pytest.raises(ValueError, match="cron runs must carry a schedule_time"):
            sources.Clock(kind=sources.Kind.CRON, trigger_time=_TRIGGER, now=_NOW)

    @pytest.mark.parametrize("kind", [sources.Kind.SUBSCRIPTION, sources.Kind.MANUAL])
    def test_a_schedule_time_on_a_kind_that_never_has_one_is_refused(
        self, kind: sources.Kind
    ) -> None:
        """Nothing in production can supply one here, so a fixture that does would make
        `{{ schedule_time }}` pass in a test and fail on every real fire."""
        with pytest.raises(ValueError, match="must not carry a schedule_time"):
            sources.Clock(
                kind=kind,
                trigger_time=_TRIGGER,
                now=_NOW,
                schedule_time=_SCHEDULE_TIME,
            )


class TestTheRenderContext:
    def test_a_scheduler_fire_offers_all_three_clocks_and_coalesce(
        self,
    ) -> None:
        """What a template can name. `coalesce` is in the context as a function, which is
        why it is a call in the grammar and not a filter."""
        assert set(_with_schedule_time().context(key="as_of_date")) == {
            "schedule_time",
            "trigger_time",
            "now",
            "coalesce",
        }

    @pytest.mark.parametrize("kind", [sources.Kind.SUBSCRIPTION, sources.Kind.MANUAL])
    def test_a_missing_clock_is_left_out_rather_than_set_to_none(
        self, kind: sources.Kind
    ) -> None:
        """Left out so StrictUndefined fires on it. Present-and-None would render the
        four characters "None" into whatever the argument feeds -- a partition path, most
        likely -- and the run would report success."""
        context = _without_schedule_time(kind).context(key="as_of_date")

        assert "schedule_time" not in context
        assert context.get("schedule_time", "absent") == "absent"

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (
                "{{ schedule_time | rfc3339 }}",
                "2026-09-02T09:00:00.000000-04:00",
            ),
            (
                "{{ trigger_time | rfc3339 }}",
                "2026-09-02T09:00:03.250000-04:00",
            ),
            ("{{ now | rfc3339 }}", "2026-09-02T09:00:04.125000-04:00"),
        ],
        ids=["schedule_time", "trigger_time", "now"],
    )
    def test_each_clock_renders_its_own_distinct_time(
        self, value: str, expected: str
    ) -> None:
        """The three fixtures are seconds apart precisely so this can tell them apart; a
        context that wired two names to one value would pass otherwise."""
        assert _render(value, clock=_with_schedule_time()) == expected

    def test_microseconds_are_not_dropped_on_the_way_through(self) -> None:
        """schedule_time is whole-second, trigger_time and now are not. Truncating would make
        two runs inside the same second render an identical value."""
        assert _render(
            "{{ trigger_time | rfc3339 }}", clock=_with_schedule_time()
        ).endswith(".250000-04:00")
        assert _render("{{ now | rfc3339 }}", clock=_with_schedule_time()).endswith(
            ".125000-04:00"
        )

    def test_trigger_time_and_now_are_not_the_same_instant(self) -> None:
        """They differ by however long the run took to reach the render. That gap is why a
        template rooted in `now` renders differently on a retry and one rooted in
        trigger_time does not."""
        assert _render(
            "{{ trigger_time | rfc3339 }}", clock=_with_schedule_time()
        ) != _render("{{ now | rfc3339 }}", clock=_with_schedule_time())


class TestCoalesceOrdering:
    def test_a_scheduler_fire_picks_the_schedule_time_because_arm_one_has_a_value(
        self,
    ) -> None:
        """coalesce(schedule_time, trigger_time) on a scheduler fire -> schedule_time.

        Arm 1 has a value, so nothing falls through and arm 2 is never reached.
        """
        assert (
            _render(
                "{{ coalesce(schedule_time, trigger_time) | rfc3339 }}",
                clock=_with_schedule_time(),
            )
            == "2026-09-02T09:00:00.000000-04:00"
        )

    def test_swapping_the_arms_swaps_the_winner(self) -> None:
        """coalesce(trigger_time, schedule_time) on a scheduler fire -> trigger_time.

        Same fire, same two arms, reversed: the answer changes. Written order decides the
        winner, not which clocks happen to exist.
        """
        assert (
            _render(
                "{{ coalesce(trigger_time, schedule_time) | rfc3339 }}",
                clock=_with_schedule_time(),
            )
            == "2026-09-02T09:00:03.250000-04:00"
        )

    def test_a_manual_fire_falls_through_to_trigger_time_for_want_of_a_schedule_time(
        self,
    ) -> None:
        """coalesce(schedule_time, trigger_time) fired by hand -> trigger_time.

        A manual fire never went through APScheduler, so there is no schedule_time and arm 1 is
        Undefined. The template is still legal: it was saved against a cron schedule,
        where schedule_time is allowed. MANUAL is the only kind that reaches this -- a
        subscription is refused the template at save time.
        """
        assert (
            _render(
                "{{ coalesce(schedule_time, trigger_time) | rfc3339 }}",
                clock=_without_schedule_time(sources.Kind.MANUAL),
            )
            == "2026-09-02T09:00:03.250000-04:00"
        )

    def test_it_keeps_falling_through_past_two_missing_arms(self) -> None:
        """coalesce(schedule_time, schedule_time, now) fired by hand -> now.

        Both scheduled arms are missing, so the answer is arm 3. Degenerate but storable: every
        name is available under CRON. A single `if` in place of the loop would stop after
        arm 1 and this is the test that would catch it.
        """
        assert (
            _render(
                "{{ coalesce(schedule_time, schedule_time, now) | rfc3339 }}",
                clock=_without_schedule_time(sources.Kind.MANUAL),
            )
            == "2026-09-02T09:00:04.125000-04:00"
        )

    def test_one_template_gives_the_schedule_time_by_scheduler_and_trigger_time_by_hand(
        self,
    ) -> None:
        """One stored template, one cron schedule, two ways of firing it:

            fired by the scheduler -> schedule_time   09:00:00.000000
            fired by hand          -> trigger_time    09:00:03.250000

        This is the only case in the product that reaches coalesce. Without it the
        hand-fired run would have no value for the argument at all.
        """
        portable = "{{ coalesce(schedule_time, trigger_time) | rfc3339 }}"

        assert (
            _render(portable, clock=_with_schedule_time())
            == "2026-09-02T09:00:00.000000-04:00"
        )
        assert _render(portable, clock=_without_schedule_time(sources.Kind.MANUAL)) == (
            "2026-09-02T09:00:03.250000-04:00"
        )


class TestCoalesceWithNothingAvailable:
    def test_a_lone_missing_arm_raises_and_says_to_add_trigger_time(
        self,
    ) -> None:
        """coalesce(schedule_time) fired by hand -> raises; nothing left to fall through
        to. One arm, so the message can name the fix rather than just the problem."""
        with pytest.raises(errors.TemplateError) as caught:
            _render(
                "{{ coalesce(schedule_time) }}",
                clock=_without_schedule_time(sources.Kind.MANUAL),
            )

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': 'schedule_time' is not available for a "
            "manual run; coalesce(schedule_time, trigger_time) is the portable form"
        )

    def test_every_arm_missing_raises_and_lists_what_was_tried(self) -> None:
        """coalesce(schedule_time, schedule_time) fired by hand -> raises. More than one
        arm, so the message lists them instead of suggesting a fix that repeats them."""
        with pytest.raises(errors.TemplateError) as caught:
            _render(
                "{{ coalesce(schedule_time, schedule_time) }}",
                clock=_without_schedule_time(sources.Kind.MANUAL),
            )

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': no source in "
            "coalesce(schedule_time, schedule_time) is available for a manual run"
        )

    def test_the_failure_names_the_argument_it_came_from(self) -> None:
        """A request carries a map of templates. "schedule_time is not available" without
        `partition_date` in it leaves the caller to guess which argument broke."""
        with pytest.raises(errors.TemplateError) as caught:
            _render(
                "{{ coalesce(schedule_time) }}",
                clock=_without_schedule_time(sources.Kind.MANUAL),
                key="partition_date",
            )

        assert caught.value.key == "partition_date"
        assert "partition_date" in caught.value.detail
