"""The clocks a template may read, and which of them exist for a given kind of run.

A template's base is one of three clocks, or a `coalesce` over several. Which clocks exist
is not a property of the template -- it is a property of how the run was started:

    cron          the scheduler fired a scheduled time, so all three exist
    subscription  an event arrived; there is no cron expression, so no schedule_time
    manual        someone pressed the button; likewise no schedule_time

`schedule_time` is therefore the only one that can be missing, and
`coalesce(schedule_time, trigger_time)` is the form that works everywhere.

Pure: no database, no APScheduler, and no implicit clock -- every time is passed in, so a
test fixes them rather than tolerating them.
"""

import dataclasses
import datetime
import enum
from collections.abc import Callable
from typing import Any, Final

import jinja2

from cloud_pipelines_backend.templating.arguments import errors


class TimeSource(str, enum.Enum):
    """A clock a chain may start from.

    Membership is tested through `TIME_SOURCE_NAMES` rather than `TimeSource(name)`, so an unknown
    name becomes our 422 rather than a `ValueError` to translate.

    {{ schedule_time }}   the time a cron fire was due at
    {{ trigger_time }}    when the run was actually created
    {{ now }}             when the template is being rendered
    """

    SCHEDULE_TIME = "schedule_time"
    TRIGGER_TIME = "trigger_time"
    NOW = "now"


TIME_SOURCE_NAMES: Final[frozenset[str]] = frozenset(TimeSource)

# Not a `TimeSource` member: it combines sources rather than being one, so a bare
# `{{ coalesce }}` has to stay invalid.
COALESCE: Final[str] = "coalesce"


class Kind(str, enum.Enum):
    """How a run was started. The only thing it decides here is whether a schedule_time exists.

    CRON         a schedule fired         -> schedule_time, trigger_time, now
    SUBSCRIPTION an event matched         -> trigger_time, now
    MANUAL       someone pressed run      -> trigger_time, now
    """

    CRON = "cron"
    SUBSCRIPTION = "subscription"
    MANUAL = "manual"


# Written out rather than derived from "everything except schedule_time", so that adding a
# source later is a visible edit to every row instead of a silent inheritance.
AVAILABLE: Final[dict[Kind, frozenset[TimeSource]]] = {
    Kind.CRON: frozenset(
        {TimeSource.SCHEDULE_TIME, TimeSource.TRIGGER_TIME, TimeSource.NOW}
    ),
    Kind.SUBSCRIPTION: frozenset({TimeSource.TRIGGER_TIME, TimeSource.NOW}),
    Kind.MANUAL: frozenset({TimeSource.TRIGGER_TIME, TimeSource.NOW}),
}


# MANUAL is a render-time kind only: a cron schedule is *saved* as CRON and *rendered* as
# CRON or MANUAL depending on how it fires. Validating against MANUAL would ban
# schedule_time on every cron schedule, so the validator refuses it rather than obeying it.
SAVE_KINDS: Final[frozenset[Kind]] = frozenset({Kind.CRON, Kind.SUBSCRIPTION})


@dataclasses.dataclass(frozen=True)
class Clock:
    """The times one render reads, fixed before rendering starts.

    `trigger_time` is taken once at the top of the fire path and `now` at render, so they
    differ by however long the run took to reach here. Both are passed in rather than read
    from `datetime.now()` inside, which is what lets a test assert an exact string.

    `schedule_time` is None for every kind but CRON, and that absence is what makes a
    template rooted in it fail rather than quietly render something else.

    Clock(kind=Kind.CRON, trigger_time=t, now=t, schedule_time=due_at)
    Clock(kind=Kind.SUBSCRIPTION, trigger_time=t, now=t)     -> schedule_time is None
    """

    kind: Kind
    trigger_time: datetime.datetime
    now: datetime.datetime
    schedule_time: datetime.datetime | None = None

    def __post_init__(self) -> None:
        # A naive datetime would make `timezone` and `epoch_seconds` silently wrong rather
        # than raise, so it is rejected at construction. This is a caller bug, not user
        # input, so it is a ValueError rather than a TemplateError.
        for name, value in self._times().items():
            if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                raise ValueError(f"{name} must be timezone-aware, got {value!r}")
        if (self.schedule_time is None) == (self.kind is Kind.CRON):
            raise ValueError(
                f"{self.kind.value} runs must{'' if self.kind is Kind.CRON else ' not'} carry a schedule_time"
            )

    def _times(self) -> dict[str, datetime.datetime]:
        """Only the clocks that have a value, keyed by the name a template uses."""
        times = {
            TimeSource.TRIGGER_TIME.value: self.trigger_time,
            TimeSource.NOW.value: self.now,
        }
        if self.schedule_time is not None:
            times[TimeSource.SCHEDULE_TIME.value] = self.schedule_time
        return times

    def available(self) -> frozenset[TimeSource]:
        """Which sources a template may name for this kind of run.

        Clock(kind=Kind.CRON, ...).available()          -> {schedule_time, trigger_time, now}
        Clock(kind=Kind.SUBSCRIPTION, ...).available()  -> {trigger_time, now}
        """
        return AVAILABLE[self.kind]

    def context(self, *, key: str) -> dict[str, Any]:
        """The render context: every available clock by name, plus `coalesce`.

        An unavailable clock is *left out* rather than set to None, so `StrictUndefined`
        turns `{{ schedule_time }}` on a subscription into a failure instead of the string
        "None" reaching a partition path. `coalesce` is bound to `key` here because it is
        the one thing that can raise while rendering, and a message without its key is a
        guess at which of a map's templates broke.

        Clock(kind=Kind.CRON, ...).context(key="as_of_date")
            -> {"trigger_time": ..., "now": ..., "schedule_time": ..., "coalesce": <fn>}
        Clock(kind=Kind.SUBSCRIPTION, ...).context(key="as_of_date")
            -> {"trigger_time": ..., "now": ..., "coalesce": <fn>}
        """
        return {**self._times(), COALESCE: self._coalesce(key=key)}

    def _coalesce(self, *, key: str) -> Callable[..., datetime.datetime]:
        """`coalesce` as the template sees it: the first argument that has a value.

        Jinja2 passes an unavailable source through as an `Undefined` rather than raising
        at the call, which is exactly what makes falling through possible -- touching it in
        any other way would raise first.

        on a subscription:
            coalesce(schedule_time, trigger_time) -> trigger_time
            coalesce(schedule_time)               -> raises, no source available
        on a cron run:
            coalesce(schedule_time, trigger_time) -> schedule_time
        """

        def coalesce(*values: Any) -> datetime.datetime:
            for value in values:
                if not isinstance(value, jinja2.Undefined):
                    return value
            raise errors.source_unavailable(
                key=key,
                sources=[v._undefined_name or "?" for v in values],
                kind=self.kind.value,
            )

        return coalesce


def is_available(*, source: TimeSource, kind: Kind) -> bool:
    """If the source is availble for clock kind.

    is_available(source=TimeSource.SCHEDULE_TIME, kind=Kind.CRON)         -> True
    is_available(source=TimeSource.SCHEDULE_TIME, kind=Kind.SUBSCRIPTION) -> False
    """
    return source in AVAILABLE[kind]
