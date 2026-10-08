"""The six filters, and the one registry both the grammar walk and the renderer read.

No jinja2 import. The grammar walk needs the *names* at save time and the renderer needs
the *callables* at render time; deriving both from one dict is what stops a filter being
registered without being classified, or classified without being registered.

Every operator carries two jobs -- `check` its argument at save time, `apply` it at render
time -- so they live on one object. A separate validator dict would be the drift this
module exists to prevent.
"""

import abc
import datetime
import enum
import re
import zoneinfo
from collections.abc import Callable
from typing import Final

from cloud_pipelines_backend.templating.arguments import errors


# `(str, enum.Enum)` so a member IS its letter: `Unit("h")` parses, and `"".join(Unit)`
# builds the character class the regexes use.
class Unit(str, enum.Enum):
    """A fixed-length unit, its length, and what flooring to it zeroes.

    One row per unit rather than two dicts keyed by the same letters: a unit added to one
    and forgotten in the other is the drift this shape makes impossible.

    Fixed-length only. A month has no fixed length, so `shift('-1mo')` would need a
    calendar rule this DSL does not have; `'2mo'` is rejected rather than guessed at.
    """

    def __new__(
        cls, letter: str, seconds: int, floor_zeroes: tuple[str, ...]
    ) -> "Unit":
        member = str.__new__(cls, letter)
        member._value_ = letter
        member.seconds = seconds
        member.floor_zeroes = floor_zeroes
        return member

    seconds: int
    #: datetime fields a floor to this unit sets to zero. Empty for WEEK, where flooring
    #: means walking back to Monday and is not expressible as a field assignment.
    floor_zeroes: tuple[str, ...]

    SECOND = ("s", 1, ("microsecond",))
    MINUTE = ("m", 60, ("second", "microsecond"))
    HOUR = ("h", 3600, ("minute", "second", "microsecond"))
    DAY = ("d", 86400, ("hour", "minute", "second", "microsecond"))
    WEEK = ("w", 604800, ())


_UNITS: Final[str] = "".join(Unit)

_SHIFT_RE: Final[re.Pattern[str]] = re.compile(rf"([+-]?)(\d+)([{_UNITS}])")
_WIDTH_RE: Final[re.Pattern[str]] = re.compile(rf"(\d+)([{_UNITS}])")


class _Behaviour(abc.ABC):
    """An operator: what it is called, and its two halves.

    `check` runs when a template is saved, `apply` when it is rendered. All three are
    abstract because nothing type-checks this repo -- neither mypy nor pyright is
    installed -- so a half-wired operator would otherwise surface as a `TypeError` on a
    render path. Here it cannot be instantiated at all.
    """

    @property
    @abc.abstractmethod
    def filter_name(self) -> str:
        """What a user writes in a template, and what the enum takes as its value."""

    @abc.abstractmethod
    def check(self, *, key: str, argument: str) -> None:
        """Reject a bad literal at save time. Raises, or returns None."""

    @abc.abstractmethod
    def apply(self, value: datetime.datetime, argument: str) -> datetime.datetime:
        """Apply the operator at render time. The argument has already passed `check`."""


class _Shift(_Behaviour):
    @property
    def filter_name(self) -> str:
        return "shift"

    def check(self, *, key: str, argument: str) -> None:
        if _SHIFT_RE.fullmatch(argument) is None:
            raise errors.bad_operator_argument(
                key=key,
                operator=self.filter_name,
                argument=argument,
                description="an offset",
                example="-2d",
            )

    def apply(self, value: datetime.datetime, argument: str) -> datetime.datetime:
        match = _SHIFT_RE.fullmatch(argument)
        assert match is not None, f"unchecked shift argument {argument!r}"
        sign, count, letter = match.groups()
        seconds = int(count) * Unit(letter).seconds
        return value + datetime.timedelta(seconds=-seconds if sign == "-" else seconds)


class _TruncateTime(_Behaviour):
    """Go back one width, then floor to that width's unit. Left-closed, no slot grid.

    `truncate_time('4h')` on 09:41 is 05:00: four hours back is 05:41, floored to the hour.
    There is no grid of 4-hour slots, so no anchor to choose and no restriction on the
    multiple -- `'5h'` and `'2d'` are as well defined as `'4h'`.

    Equivalently `floor(t) - width`; the two agree because a width is always a whole number
    of its own unit. Written as floor-after-subtract to match how it reads: go back, then
    round down.

    Wall-clock throughout, like `shift`: `'1d'` back from 09:41 is 09:41 yesterday even on a
    23-hour day, and only then is it floored.
    """

    @property
    def filter_name(self) -> str:
        return "truncate_time"

    def check(self, *, key: str, argument: str) -> None:
        match = _WIDTH_RE.fullmatch(argument)
        if match is None or int(match.group(1)) == 0:
            raise errors.bad_operator_argument(
                key=key,
                operator=self.filter_name,
                argument=argument,
                description="a width",
                example="4h",
            )

    def apply(self, value: datetime.datetime, argument: str) -> datetime.datetime:
        match = _WIDTH_RE.fullmatch(argument)
        assert match is not None, f"unchecked truncate_time argument {argument!r}"
        count, unit = int(match.group(1)), Unit(match.group(2))

        back = value - datetime.timedelta(seconds=count * unit.seconds)
        naive = back.replace(tzinfo=None)
        if unit is Unit.WEEK:
            floored = (naive - datetime.timedelta(days=naive.weekday())).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            floored = naive.replace(**dict.fromkeys(unit.floor_zeroes, 0))
        # `replace` on a ZoneInfo re-resolves the offset for the new wall time, so a result
        # lands on the local hour even when a DST change sits between the two.
        return floored.replace(tzinfo=value.tzinfo)


class _Timezone(_Behaviour):
    """The same instant, read in another zone. Nothing about the instant changes."""

    @property
    def filter_name(self) -> str:
        return "timezone"

    def check(self, *, key: str, argument: str) -> None:
        try:
            zoneinfo.ZoneInfo(argument)
        except (zoneinfo.ZoneInfoNotFoundError, ValueError):
            raise errors.unknown_timezone(key=key, name=argument) from None

    def apply(self, value: datetime.datetime, argument: str) -> datetime.datetime:
        return value.astimezone(zoneinfo.ZoneInfo(argument))


class _Format(abc.ABC):
    """A formatter: what it is called, and the one thing it does.

    Abstract for the same reason as `_Behaviour`, even with a single method -- a formatter
    that forgets it fails at import rather than when a pipeline first renders one.
    """

    @property
    @abc.abstractmethod
    def filter_name(self) -> str:
        """What a user writes in a template, and what the enum takes as its value."""

    @abc.abstractmethod
    def format(self, value: datetime.datetime) -> str:
        """Render. Takes no argument, so there is nothing to check at save time."""


class _Date(_Format):
    @property
    def filter_name(self) -> str:
        return "date"

    def format(self, value: datetime.datetime) -> str:
        return value.strftime("%Y-%m-%d")


class _Rfc3339(_Format):
    """Always six fractional digits, so the shape does not depend on the value.

    Python omits the fraction when it is zero, which would give `schedule_time` (never has
    microseconds) and a `trigger_time` that happened to land on `.000000` different widths
    from the same formatter. A partition key cannot have that.
    """

    @property
    def filter_name(self) -> str:
        return "rfc3339"

    def format(self, value: datetime.datetime) -> str:
        return value.isoformat(sep="T", timespec="microseconds")


class _EpochSeconds(_Format):
    @property
    def filter_name(self) -> str:
        return "epoch_seconds"

    def format(self, value: datetime.datetime) -> str:
        return str(int(value.timestamp()))


class Operator(str, enum.Enum):
    """datetime in, datetime out, so operators chain.

    A member IS its Jinja2 filter name -- taken from the behaviour, so the name is written
    once -- and the same object therefore indexes the engine, the grammar walk and
    this table. `Operator(name)` raises `ValueError` on a name that is not one, which is
    the unknown-filter rejection for free.
    """

    #: Declared, not assigned: a value assigned in an Enum body would become a member.
    behaviour: _Behaviour

    def __new__(cls, behaviour: _Behaviour) -> "Operator":
        member = str.__new__(cls, behaviour.filter_name)
        member._value_ = behaviour.filter_name
        member.behaviour = behaviour
        return member

    SHIFT = _Shift()
    TRUNCATE_TIME = _TruncateTime()
    TIMEZONE = _Timezone()

    def check(self, *, key: str, argument: str) -> None:
        self.behaviour.check(key=key, argument=argument)

    def apply(self, value: datetime.datetime, argument: str) -> datetime.datetime:
        return self.behaviour.apply(value, argument)


class Formatter(str, enum.Enum):
    """datetime in, string out, so formatters terminate a chain."""

    formatting: _Format

    def __new__(cls, formatting: _Format) -> "Formatter":
        member = str.__new__(cls, formatting.filter_name)
        member._value_ = formatting.filter_name
        member.formatting = formatting
        return member

    DATE = _Date()
    RFC3339 = _Rfc3339()
    EPOCH_SECONDS = _EpochSeconds()

    def format(self, value: datetime.datetime) -> str:
        return self.formatting.format(value)


OPERATOR_NAMES: Final[frozenset[str]] = frozenset(Operator)
FORMATTER_NAMES: Final[frozenset[str]] = frozenset(Formatter)


def check_operator_argument(*, key: str, operator: str, argument: str) -> None:
    """Save-time check for one operator's literal argument. Raises, or returns None.

    `operator` arrives as a `str` from the Jinja2 AST, so the lookup is by value.
    """
    Operator(operator).check(key=key, argument=argument)


def as_jinja_filters() -> dict[str, Callable[..., datetime.datetime | str]]:
    """Both registries flattened to the `name -> callable` shape a Jinja2 engine wants.

    Returned rather than installed, so this module never imports jinja2 and can be tested
    without building an engine.
    """
    # `.value`, not the member: what reaches `env.filters` should be an ordinary `str`,
    # indistinguishable from the keys Jinja2 puts there itself.
    return {op.value: op.apply for op in Operator} | {
        f.value: f.format for f in Formatter
    }
