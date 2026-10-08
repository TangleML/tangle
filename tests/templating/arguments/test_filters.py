"""Unit tests for templating.arguments.filters.

The worked examples are implementation.md section 3.5's, kept as literals rather than
recomputed, so a change in behaviour shows up as a diff against the design rather than
quietly agreeing with itself.
"""

import datetime
import zoneinfo

import pytest
from jinja2.sandbox import SandboxedEnvironment

from cloud_pipelines_backend.templating.arguments import errors, filters

_TORONTO = zoneinfo.ZoneInfo("America/Toronto")

#: The design doc's instant, used by every example in its operator and formatter tables.
_INSTANT = datetime.datetime(2026, 9, 2, 9, 41, tzinfo=_TORONTO)


class TestTheDocumentedExamples:
    """One test per row of section 3.5's two tables."""

    @pytest.mark.parametrize(
        ("operator", "argument", "expected"),
        [
            ("shift", "-2d", "2026-08-31 09:41:00-04:00"),
            ("truncate_time", "4h", "2026-09-02 05:00:00-04:00"),
            ("timezone", "UTC", "2026-09-02 13:41:00+00:00"),
        ],
    )
    def test_each_operator_matches_its_documented_example(
        self, operator: str, argument: str, expected: str
    ) -> None:
        assert str(filters.Operator(operator).apply(_INSTANT, argument)) == expected

    @pytest.mark.parametrize(
        ("formatter", "expected"),
        [
            ("date", "2026-09-02"),
            ("rfc3339", "2026-09-02T09:41:00.000000-04:00"),
            ("epoch_seconds", "1788356460"),
        ],
    )
    def test_each_formatter_matches_its_documented_example(
        self, formatter: str, expected: str
    ) -> None:
        assert filters.Formatter(formatter).format(_INSTANT) == expected


class TestAlwaysSixFractionalDigits:
    """Python omits the fraction when it is zero, so "emit whatever precision exists"
    gives a shape that varies by value, not by source."""

    @pytest.mark.parametrize(
        ("microsecond", "expected"),
        [
            (0, "2026-09-02T09:41:00.000000-04:00"),
            (282672, "2026-09-02T09:41:00.282672-04:00"),
            (1, "2026-09-02T09:41:00.000001-04:00"),
        ],
        ids=["no-fraction", "microseconds", "one-microsecond"],
    )
    def test_the_width_does_not_depend_on_the_value(
        self, microsecond: int, expected: str
    ) -> None:
        instant = _INSTANT.replace(microsecond=microsecond)

        assert filters.Formatter.RFC3339.format(instant) == expected
        assert len(filters.Formatter.RFC3339.format(instant)) == 32

    def test_precision_is_kept_not_truncated(self) -> None:
        assert filters.Formatter.RFC3339.format(
            _INSTANT.replace(microsecond=282672)
        ).endswith(".282672-04:00")

    def test_the_formatters_that_discard_sub_seconds_are_unaffected(
        self,
    ) -> None:
        noisy = _INSTANT.replace(microsecond=282672)

        assert filters.Formatter.DATE.format(noisy) == filters.Formatter.DATE.format(
            _INSTANT
        )
        assert filters.Formatter.EPOCH_SECONDS.format(
            noisy
        ) == filters.Formatter.EPOCH_SECONDS.format(_INSTANT)


class TestArgumentsAreCheckedAtSaveTime:
    """`check` runs when the template is written, so a bad offset cannot reach a fire
    path. Every message is section 2.4's, named key and all."""

    @pytest.mark.parametrize("argument", ["-2d", "+2d", "2d", "0s", "90m", "1w"])
    def test_a_well_formed_offset_is_accepted(self, argument: str) -> None:
        """`check` raises or returns None, so None is what acceptance looks like."""
        assert (
            filters.check_operator_argument(
                key="as_of_date", operator="shift", argument=argument
            )
            is None
        )

    @pytest.mark.parametrize(
        "argument",
        ["-2 days", "2mo", "", "2y", "d", "2", "--2d", "1.5h", " 2d"],
    )
    def test_anything_else_is_rejected(self, argument: str) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            filters.check_operator_argument(
                key="as_of_date", operator="shift", argument=argument
            )

        assert caught.value.key == "as_of_date"
        assert "is not an offset" in caught.value.detail

    def test_the_message_is_the_documented_one(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            filters.check_operator_argument(
                key="as_of_date", operator="shift", argument="-2 days"
            )

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': shift('-2 days') is not an offset; "
            "expected e.g. '-2d'"
        )

    @pytest.mark.parametrize(
        "argument",
        ["0h", "-1h", "2mo", ""],
        ids=["zero", "negative", "month", "empty"],
    )
    def test_truncate_time_rejects_what_the_doc_says_it_rejects(
        self, argument: str
    ) -> None:
        with pytest.raises(errors.TemplateError):
            filters.check_operator_argument(
                key="as_of_date", operator="truncate_time", argument=argument
            )

    def test_an_unknown_timezone_is_rejected_by_name(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            filters.check_operator_argument(
                key="as_of_date", operator="timezone", argument="America/Torono"
            )

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': unknown timezone 'America/Torono'"
        )

    @pytest.mark.parametrize("name", ["UTC", "America/Toronto", "Asia/Kolkata"])
    def test_a_real_timezone_is_accepted(self, name: str) -> None:
        assert (
            filters.check_operator_argument(
                key="as_of_date", operator="timezone", argument=name
            )
            is None
        )


class TestShiftIsWallClockAcrossADstChange:
    """⚠️ implementation.md does not say which `shift` should be; this pins what it IS.

    Python's arithmetic on an aware datetime is naive, so `shift` moves the wall clock and
    `'-1d'` and `'-24h'` agree even on a 23-hour day. "Same time yesterday" is the reading
    most people want from `'-1d'`; whether `'-24h'` should instead mean 24 elapsed hours is
    an open question, recorded here rather than left to be discovered.
    """

    #: 2026-03-08 is the 23-hour day. Landing ON it and stepping back is what separates
    #: wall-clock from elapsed time; stepping back from the 9th stays inside EDT at both
    #: ends and shows a plain 24 hours, which is the reading that hides the behaviour.
    _DAY_AFTER_THE_SKIP = datetime.datetime(2026, 3, 8, 9, 41, tzinfo=_TORONTO)

    def test_a_day_and_twenty_four_hours_agree_on_a_twenty_three_hour_day(
        self,
    ) -> None:
        instant = self._DAY_AFTER_THE_SKIP

        assert filters.Operator.SHIFT.apply(
            instant, "-1d"
        ) == filters.Operator.SHIFT.apply(instant, "-24h")

    def test_the_wall_clock_time_is_preserved(self) -> None:
        assert (
            str(filters.Operator.SHIFT.apply(self._DAY_AFTER_THE_SKIP, "-1d"))
            == "2026-03-07 09:41:00-05:00"
        )

    def test_the_real_elapsed_gap_is_twenty_three_hours(self) -> None:
        instant = self._DAY_AFTER_THE_SKIP

        shifted = filters.Operator.SHIFT.apply(instant, "-1d")

        assert instant.astimezone(datetime.timezone.utc) - shifted.astimezone(
            datetime.timezone.utc
        ) == datetime.timedelta(hours=23)


class TestOneRegistryFeedsBothConsumers:
    """The grammar walk reads the names, the renderer reads the callables. A filter
    registered without being classified would be silently unreachable -- or silently
    permitted -- so the two sets are derived, never written twice."""

    def test_the_names_are_derived_from_the_registries(self) -> None:
        assert filters.OPERATOR_NAMES == frozenset(filters.Operator)
        assert filters.FORMATTER_NAMES == frozenset(filters.Formatter)

    def test_a_member_is_its_own_filter_name(self) -> None:
        """The member IS the string, so nothing has to hold a second spelling."""
        assert filters.Operator.TRUNCATE_TIME == "truncate_time"
        assert filters.Formatter.EPOCH_SECONDS == "epoch_seconds"

    def test_an_unknown_name_is_rejected_by_the_lookup_itself(self) -> None:
        """What S4 will use to reject an unknown filter."""
        with pytest.raises(ValueError):
            filters.Operator("upper")

    def test_what_reaches_the_jinja_engine_is_plain_str_keys(self) -> None:
        """Jinja2 puts ordinary strings in `env.filters`; ours should be indistinguishable
        from those, not enum members that merely compare equal to them."""
        assert {type(key) for key in filters.as_jinja_filters()} == {str}

    def test_the_six_filters_are_the_documented_six(self) -> None:
        assert filters.OPERATOR_NAMES == {"shift", "truncate_time", "timezone"}
        assert filters.FORMATTER_NAMES == {"date", "rfc3339", "epoch_seconds"}

    def test_no_name_is_both_an_operator_and_a_formatter(self) -> None:
        assert not filters.OPERATOR_NAMES & filters.FORMATTER_NAMES

    def test_the_flattened_map_is_exactly_the_two_registries(self) -> None:
        assert (
            set(filters.as_jinja_filters())
            == filters.OPERATOR_NAMES | filters.FORMATTER_NAMES
        )

    def test_every_operator_can_check_its_own_argument(self) -> None:
        """A new operator added without a `check` would accept anything at save time and
        blow up on a fire path instead."""
        for name in filters.OPERATOR_NAMES:
            with pytest.raises(errors.TemplateError):
                filters.check_operator_argument(
                    key="k", operator=name, argument="definitely not valid"
                )

    def test_truncate_is_not_registered_because_jinja2_owns_that_name(
        self,
    ) -> None:
        """`truncate` is one of Jinja2's 54 builtins, for strings. Registering ours would
        shadow it; after the rename the collision set is empty."""
        import jinja2.filters

        assert "truncate" in jinja2.filters.FILTERS
        assert "truncate" not in filters.OPERATOR_NAMES | filters.FORMATTER_NAMES
        assert not (filters.OPERATOR_NAMES | filters.FORMATTER_NAMES) & set(
            jinja2.filters.FILTERS
        )


class TestTruncateTimeGoesBackThenFloors:
    """`truncate_time(N<unit>)` is `floor_to_<unit>(t - N<unit>)`.

    Left-closed, and there is no grid of slots: the unit alone decides the floor, so no
    anchor has to be chosen and no multiple has to be forbidden. `'5h'` and `'2d'` are as
    well defined as `'4h'`.
    """

    @pytest.mark.parametrize(
        ("hour", "minute", "spec", "expected"),
        [
            (3, 30, "1h", "09-02 02:00:00"),
            (2, 0, "1h", "09-02 01:00:00"),
            (9, 41, "1h", "09-02 08:00:00"),
            (9, 41, "4h", "09-02 05:00:00"),
            (9, 41, "5h", "09-02 04:00:00"),
            (0, 15, "1h", "09-01 23:00:00"),
            (9, 41, "1d", "09-01 00:00:00"),
            (9, 41, "2d", "08-31 00:00:00"),
            (9, 41, "1w", "08-24 00:00:00"),
            (9, 41, "90m", "09-02 08:11:00"),
            (9, 41, "30s", "09-02 09:40:30"),
        ],
    )
    def test_the_agreed_table(
        self, hour: int, minute: int, spec: str, expected: str
    ) -> None:
        instant = datetime.datetime(2026, 9, 2, hour, minute, tzinfo=_TORONTO)

        assert (
            filters.Operator.TRUNCATE_TIME.apply(instant, spec).strftime(
                "%m-%d %H:%M:%S"
            )
            == expected
        )

    def test_it_is_left_closed_at_a_boundary(self) -> None:
        """02:00 with `'1h'` is 01:00 because a full hour is subtracted first, not because
        a boundary instant is pushed into the previous bucket. The distinction matters:
        03:30 goes to 02:00 for the same reason, not to 03:00."""
        on_the_hour = datetime.datetime(2026, 9, 2, 2, 0, tzinfo=_TORONTO)
        mid_hour = datetime.datetime(2026, 9, 2, 3, 30, tzinfo=_TORONTO)

        assert (
            filters.Operator.TRUNCATE_TIME.apply(on_the_hour, "1h").strftime("%H:%M")
            == "01:00"
        )
        assert (
            filters.Operator.TRUNCATE_TIME.apply(mid_hour, "1h").strftime("%H:%M")
            == "02:00"
        )

    @pytest.mark.parametrize("spec", ["1h", "4h", "5h", "7h", "1d", "2d", "3w", "90m"])
    def test_no_multiple_is_forbidden(self, spec: str) -> None:
        """The grid model had to reject `'2d'` because a 48-hour grid needs a start date.
        Going back and flooring needs neither, so the restriction is gone."""
        assert (
            filters.check_operator_argument(
                key="as_of_date", operator="truncate_time", argument=spec
            )
            is None
        )

    def test_subtract_then_floor_equals_floor_then_subtract(self) -> None:
        """A width is always a whole number of its own unit, so the two readings agree.
        Pinned because the docstring claims it."""
        instant = datetime.datetime(2026, 9, 2, 9, 41, tzinfo=_TORONTO)

        for hours in (1, 4, 5, 7):
            floor_then_subtract = instant.replace(
                minute=0, second=0, microsecond=0
            ) - datetime.timedelta(hours=hours)

            assert (
                filters.Operator.TRUNCATE_TIME.apply(instant, f"{hours}h")
                == floor_then_subtract
            )

    def test_a_week_lands_on_a_monday(self) -> None:
        wednesday = datetime.datetime(2026, 9, 2, 9, 41, tzinfo=_TORONTO)

        floored = filters.Operator.TRUNCATE_TIME.apply(wednesday, "1w")

        assert floored.weekday() == 0
        assert str(floored) == "2026-08-24 00:00:00-04:00"

    def test_it_is_not_idempotent_and_that_is_by_design(self) -> None:
        """Every application goes back another width. A standard `date_trunc` is
        idempotent; this is not one, which is why it is worth stating outright."""
        instant = datetime.datetime(2026, 9, 2, 9, 41, tzinfo=_TORONTO)

        once = filters.Operator.TRUNCATE_TIME.apply(instant, "4h")
        twice = filters.Operator.TRUNCATE_TIME.apply(once, "4h")

        assert once.strftime("%H:%M") == "05:00"
        assert twice.strftime("%H:%M") == "01:00"


class TestTruncateTimeIsWallClockAcrossADstChange:
    """Same rule as `shift`: go back on the wall clock, then floor."""

    def test_a_day_back_keeps_the_wall_time_before_flooring(self) -> None:
        instant = datetime.datetime(2026, 3, 8, 9, 41, tzinfo=_TORONTO)

        assert (
            str(filters.Operator.TRUNCATE_TIME.apply(instant, "1d"))
            == "2026-03-07 00:00:00-05:00"
        )

    def test_an_hour_back_across_the_skip_lands_on_the_local_hour(self) -> None:
        """01:30 EST minus 1h is 00:30, floored to 00:00 -- still EST. The offset is
        re-resolved for the floored wall time, not carried over from the input."""
        instant = datetime.datetime(2026, 3, 8, 1, 30, tzinfo=_TORONTO)

        floored = filters.Operator.TRUNCATE_TIME.apply(instant, "1h")

        assert str(floored) == "2026-03-08 00:00:00-05:00"
        assert floored.utcoffset() == datetime.timedelta(hours=-5)


class TestAHalfBuiltFilterCannotExist:
    """The reason `_Behaviour` and `_Format` are abstract rather than pairs of callables.

    Nothing type-checks this repo -- neither mypy nor pyright is installed -- so a filter
    wired up with a missing or wrong-signature half would otherwise be accepted silently
    and fail with a `TypeError` at render time, on a pipeline. Abstract methods move that
    to import.
    """

    def test_an_operator_missing_apply_cannot_be_instantiated(self) -> None:
        class MissingApply(filters._Behaviour):
            @property
            def filter_name(self) -> str:
                return "missing_apply"

            def check(self, *, key: str, argument: str) -> None: ...

        with pytest.raises(TypeError, match="abstract method 'apply'"):
            MissingApply()

    def test_an_operator_missing_check_cannot_be_instantiated(self) -> None:
        class MissingCheck(filters._Behaviour):
            @property
            def filter_name(self) -> str:
                return "missing_check"

            def apply(
                self, value: datetime.datetime, argument: str
            ) -> datetime.datetime:
                return value

        with pytest.raises(TypeError, match="abstract method 'check'"):
            MissingCheck()

    def test_a_filter_missing_its_name_cannot_be_instantiated(self) -> None:
        """`filter_name` is abstract, so the enum can take its value from the behaviour
        and the name is written once."""

        class Nameless(filters._Behaviour):
            def check(self, *, key: str, argument: str) -> None: ...

            def apply(
                self, value: datetime.datetime, argument: str
            ) -> datetime.datetime:
                return value

        with pytest.raises(TypeError, match="abstract method 'filter_name'"):
            Nameless()

    def test_a_formatter_missing_format_cannot_be_instantiated(self) -> None:
        class MissingFormat(filters._Format):
            @property
            def filter_name(self) -> str:
                return "missing_format"

        with pytest.raises(TypeError, match="abstract method 'format'"):
            MissingFormat()

    def test_every_member_carries_a_real_implementation(self) -> None:
        """A member wired to the base class rather than a concrete one would pass the
        tests above and still be broken."""
        for operator in filters.Operator:
            assert isinstance(operator.behaviour, filters._Behaviour)
            assert type(operator.behaviour) is not filters._Behaviour
        for formatter in filters.Formatter:
            assert isinstance(formatter.formatting, filters._Format)
            assert type(formatter.formatting) is not filters._Format

    def test_the_member_name_and_the_filter_name_agree(self) -> None:
        """The one thing moving `filter_name` into the class costs: the enum body no
        longer shows the string, so `TRUNCATE_TIME = _TruncateTime()` reads fine even if
        the class returns 'truncate'. Then `Operator('truncate_time')` raises and the
        filter silently stops existing. Pinned here instead."""
        for member in (*filters.Operator, *filters.Formatter):
            assert member.name.lower() == member.value


class TestTheFiltersDoNotShadowJinjaBuiltins:
    """`truncate` was renamed to `truncate_time` because Jinja2 ships a string filter of
    that name. This is the guard that would have caught it, and catches the next one."""

    def test_installing_ours_adds_exactly_ours(self) -> None:
        engine = SandboxedEnvironment(autoescape=False)
        builtins = set(engine.filters)

        engine.filters.update(filters.as_jinja_filters())

        added = set(engine.filters) - builtins
        assert added == filters.OPERATOR_NAMES | filters.FORMATTER_NAMES

    def test_no_name_is_already_taken(self) -> None:
        """The same fact stated as the collision itself, so a failure names the culprit."""
        builtins = set(SandboxedEnvironment(autoescape=False).filters)

        collisions = builtins & (filters.OPERATOR_NAMES | filters.FORMATTER_NAMES)

        assert collisions == set()

    def test_the_count_is_what_the_registries_hold(self) -> None:
        """A shadowed name would make the installed map smaller than the two enums."""
        assert len(filters.as_jinja_filters()) == len(filters.Operator) + len(
            filters.Formatter
        )
