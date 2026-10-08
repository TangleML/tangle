"""The trigger meter and its instruments.

Declaration only: this module names what is measured, and `triggers/service.py` and the
staleness poller record into it. Names follow `trigger.<name>`, matching the convention
`emissions/docs/EMISSION_OBSERVABILITY.md` sets — snake_case attributes, named for what is
measured rather than for who measured it. The Prometheus export rewrites the dots and adds
its own suffixes, so `trigger.triggered` arrives as `trigger_triggered_total`.

The recording helper is a copy of the emission one rather than an import of it, because the
dependency runs the other way: `emissions/handlers/readiness/sinks/start_pipeline_run.py`
imports `triggers`, so importing `emissions` from here would close the cycle.

What these six answer that a log line cannot: the failures this stage exists for raise
nothing. A subscription whose `expire_seconds` is shorter than the real gap between its
upstreams fills one event, lets it lapse, fills the next, and never triggers — every delivery
succeeds, every log line is unremarkable, and `trigger.event_expired` is the only place it
shows. A subscription whose target pipeline was deleted is quieter still: it stops for good,
and `trigger.run_not_started` is the only signal that says so before someone notices the run
never ran.
"""

import enum
import logging
import typing

import sqlalchemy
from opentelemetry import metrics as otel_metrics
from sqlalchemy import orm

logger = logging.getLogger(__name__)

# Carried by every trigger instrument: which subscription was measured. Cardinality is the
# number of rows in `trigger_subscription`, a human-authored config table of hundreds — the
# same bound the `record_event_and_maybe_start_runs` fan-out relies on, and it stops holding at
# the same moment, when subscriptions start being created programmatically.
SUBSCRIPTION_ID_LABEL: typing.Final[str] = "subscription_id"
# Carried by the two per-event counters: which of the subscription's events this was about.
# Named after the column, so a metric and a query about it cannot disagree.
EVENT_NAME_LABEL: typing.Final[str] = "event_name"
# Carried by `run_not_started`: why the run did not start. The values are the reason constants
# declared at the top of `triggers/service.py`, a closed set of a handful of strings, so this
# adds a bounded factor to that counter's cardinality rather than an open one. It exists so a
# second permanent reason can join the same series instead of needing its own counter.
REASON_LABEL: typing.Final[str] = "reason"


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    SECONDS = "s"
    EVENTS = "{event}"
    TRIGGERS = "{trigger}"


trigger_meter = otel_metrics.get_meter("tangle.triggers")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

triggered = trigger_meter.create_counter(
    name="trigger.triggered",
    description=(
        "Number of cycles a subscription claimed and cleared its events for, by subscription."
        " Counted after the fence insert wins, so it counts runs decided on, not conditions"
        " that looked satisfied"
    ),
    unit=MetricUnit.TRIGGERS,
)

event_filled = trigger_meter.create_counter(
    name="trigger.event_filled",
    description=(
        "Number of arrivals written onto an event state, by subscription and event name. A"
        " redelivery already recorded against the event is not counted — nothing was written"
    ),
    unit=MetricUnit.EVENTS,
)

event_expired = trigger_meter.create_counter(
    name="trigger.event_expired",
    description=(
        "Number of times an evaluation found this event filled but lapsed, by subscription and"
        " event name. Incremented per evaluation that noticed, not per lapse: expiry is a"
        " WHERE clause on the read and nothing sweeps the row, so the lapse itself is not an"
        " event anything could count"
    ),
    unit=MetricUnit.EVENTS,
)

run_not_started = trigger_meter.create_counter(
    name="trigger.run_not_started",
    description=(
        "Number of times a condition held and the run could not be started anyway, by"
        " subscription and reason. Permanent failures only: the arrival is committed, the"
        " emission is settled FAIL with no redelivery, and nothing moves again until a human"
        " repoints the subscription — so unlike every other non-trigger outcome, a non-zero"
        " rate here is a subscription that has stopped and will stay stopped"
    ),
    unit=MetricUnit.TRIGGERS,
)

cycle_collisions = trigger_meter.create_counter(
    name="trigger.cycle_collisions",
    description=(
        "Number of writers that saw the condition satisfied and lost the fence, by"
        " subscription. The only place a would-be double trigger is visible: the loser writes"
        " nothing and reports a successful delivery, so no error surfaces anywhere else"
    ),
    unit=MetricUnit.TRIGGERS,
)


# ---------------------------------------------------------------------------
# Recording helper
# ---------------------------------------------------------------------------


def increment(
    *,
    counter: otel_metrics.Counter,
    attributes: dict[str, str],
) -> None:
    """Add one to a counter, logging instead of raising if that fails.

    Measuring a trigger must never change whether it happened, so a broken instrument costs a
    log line and nothing else. This is called from inside the transaction that holds the
    subscription's `FOR UPDATE` lock, which is the other reason it cannot be allowed to raise.

    Args:
        counter: The counter to add to.
        attributes: The labels to record the increment under.
    """
    try:
        counter.add(1, attributes=attributes)
    except Exception:
        logger.warning(
            f"Failed to increment trigger counter {attributes}", exc_info=True
        )


# Where a session parks the counts it has queued but not yet earned. Keyed on `Session.info`,
# which is per-session scratch space SQLAlchemy carries for exactly this, so the queue travels
# with the transaction rather than with a global the next request would inherit.
_PENDING_COUNTS: typing.Final[str] = "tangle.triggers.pending_counts"

#: Same queue, same guards, for a caller that reports through an observer rather than a
#: bare counter. Separate key so a drain of one cannot swallow the other.
_PENDING_REPORTS: typing.Final[str] = "tangle.triggers.pending_reports"


def record_after_commit(
    *,
    session: orm.Session,
    counter: otel_metrics.Counter,
    attributes: dict[str, str],
) -> None:
    """Queue one count, to be recorded only if the session's transaction commits.

    A counter cannot be rolled back, so counting inside the transaction reports attempted work
    rather than durable work. Two callers make that a real divergence and not a nicety: the
    sink retries a deadlock victim, so one eventual trigger would be counted once per attempt,
    and the `PATCH` route commits well after `update_subscription` returns, so a request that
    fails afterwards would leave a trigger counted that never happened.

    Queueing is not a delivery guarantee. A process that dies between the commit and the
    listener loses the count — which is the right way round, since the alternative loses the
    trigger's credibility instead.

    Args:
        session: The session whose commit the count is waiting on.
        counter: The counter to add to once it does.
        attributes: The labels to record the increment under.
    """
    pending: list[tuple[otel_metrics.Counter, dict[str, str]]] = (
        session.info.setdefault(_PENDING_COUNTS, [])
    )
    pending.append((counter, dict(attributes)))


def report_after_commit(
    *, session: orm.Session, report: typing.Callable[[], None]
) -> None:
    """Queue one whole report, on the same terms as `record_after_commit`.

    For an observer that owns several counters and their labels: the caller cannot queue
    those one at a time without knowing them, and reimplementing the wait would mean a
    second copy of the SAVEPOINT guard below -- the part that is easy to get wrong.

    Args:
        session: The session whose commit the report is waiting on.
        report: Called with no arguments once it commits, and dropped if it rolls back.
    """
    pending: list[typing.Callable[[], None]] = session.info.setdefault(
        _PENDING_REPORTS, []
    )
    pending.append(report)


@sqlalchemy.event.listens_for(orm.Session, "after_commit")
def _record_queued_counts(session: orm.Session) -> None:
    """Record what the committed transaction earned.

    Registered against the `Session` class, so it covers every session in the process; it is
    inert for the ones that never queued anything, which is all of them outside `triggers`.

    A released SAVEPOINT dispatches this event too, and draining there would count a trigger
    the enclosing transaction can still roll back. The guard is what makes the queue wait for
    the outermost commit: the session is still inside its nested transaction when the event
    fires for a release, and out of it when the event fires for the real commit.
    """
    if session.in_nested_transaction():
        return
    for counter, attributes in session.info.pop(_PENDING_COUNTS, []):
        increment(counter=counter, attributes=attributes)
    for report in session.info.pop(_PENDING_REPORTS, []):
        report()


@sqlalchemy.event.listens_for(orm.Session, "after_rollback")
def _discard_queued_counts(session: orm.Session) -> None:
    """Throw away what the rolled-back transaction did not earn.

    Only an outermost rollback empties the queue. This event fires for a SAVEPOINT rollback
    too, and the fence depends on the difference: `_trigger` rolls back a nested SAVEPOINT when
    it loses the race, and the arrival around it — already queued — still commits.

    `in_nested_transaction()` is the discriminator and `in_transaction()` is not: both events
    are dispatched before the transaction is deassociated, so `in_transaction()` reads True on
    an outermost rollback as well and would keep every queue alive to be drained by whatever
    committed next on the same session.
    """
    if session.in_nested_transaction():
        return
    session.info.pop(_PENDING_COUNTS, None)
    session.info.pop(_PENDING_REPORTS, None)


def create_oldest_waiting_arrival_age_gauge(
    *,
    callback: typing.Callable[
        [otel_metrics.CallbackOptions],
        typing.Iterable[otel_metrics.Observation],
    ],
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how long a subscription has held an unconsumed arrival.

    Built on demand rather than at import, like the emission queue's age gauges and for the
    same reason: only a process that polls the table can say what the ages are, and the
    callback that answers belongs to the caller. It is deliberately not polled by the sink —
    a gauge the sink emits cannot report the sink's own absence, and a series that goes stale
    is harder to alert on than a number that climbs.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per
            subscription holding a live arrival it has not triggered off. It runs on the
            exporter's thread, so it should return cached values rather than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return trigger_meter.create_observable_gauge(
        name="trigger.oldest_waiting_arrival_age",
        callbacks=[callback],
        description=(
            "Age of the oldest live arrival an enabled subscription has"
            " recorded and not triggered off"
        ),
        unit=MetricUnit.SECONDS,
    )


def create_staleness_poll_last_success_age_gauge(
    *,
    callback: typing.Callable[
        [otel_metrics.CallbackOptions],
        typing.Iterable[otel_metrics.Observation],
    ],
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how long ago the staleness poller last finished a poll.

    The waiting-age gauge above cannot report its own poller stopping. A poll that fails
    leaves the last good ages in place deliberately, and the loop logs and carries on, so a
    permanently broken poller exports a frozen number rather than nothing: the series stays
    healthy-looking and the process stays up, which leaves restart alerting silent too. This
    is the series that moves in that case, and alerting belongs on it rather than on the ages.

    The emission backlog poller has the same loop and no equivalent gauge yet; giving it one
    is a follow-up rather than part of this change.

    Args:
        callback: Called by the SDK on each collection cycle; yields one unlabelled
            observation. It runs on the exporter's thread, so it should read cached state
            rather than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return trigger_meter.create_observable_gauge(
        name="trigger.staleness_poll_last_success_age",
        callbacks=[callback],
        description=(
            "Seconds since the trigger staleness poller last completed a poll, counted from"
            " process start until the first one succeeds"
        ),
        unit=MetricUnit.SECONDS,
    )
