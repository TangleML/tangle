"""The quota meter and its instruments.

Declaration only: this module names what is measured, and the interceptor, the sink, the API
and the poller record into it. It is the second metered package in Tangle after
`emissions/observability/`, and it follows that package's rules rather than inventing its own.

Instrument names are `quota.<name>` and `quota.duration.<stage>`, matching the shape of
`emission.written` and `emission.duration.total`. The Prometheus export rewrites the dots and
adds its own suffixes, so `quota.gate_decisions` is scraped as `quota_gate_decisions_total`.

`gate_decisions` is one counter with an outcome label because its four values are mutually
exclusive verdicts on the same event — the shape `emission.claims` uses for `claim_outcome`.
`promotions` is a separate instrument rather than a fifth decision, because it is a different
event: it moves *n* nodes at once rather than deciding about one.

Unlike the emission duration histograms, the two here do not nest — a gate call is not inside a
promotion pass — so they need no shared stage label and carry no double-counting risk.
"""

import collections.abc
import enum
import logging
import types
import typing

from opentelemetry import metrics as otel_metrics

logger = logging.getLogger(__name__)

# Carried by every quota instrument: which group was measured. Cardinality is the number of
# quota_group rows, which is operator-created and small — and deliberately never the name from
# a misspelled annotation, which is typo- and attacker-controlled. See MISSING_GROUP below.
QUOTA_GROUP_LABEL: typing.Final[str] = "quota_group"
# Carried by the gate counter only: how the gate call ended. See GateDecision -- no count
# written down here, because it has been wrong at every count so far.
DECISION_LABEL: typing.Final[str] = "decision"
# Carried by the promotion counter only: what woke the pass. See PromotionTrigger.
TRIGGER_LABEL: typing.Final[str] = "trigger"

# The group label recorded when a node names a group that does not exist. The real string is
# unbounded, so it stays out of the metric entirely: it is written to
# `execution.extra_data["quota_group_missing"]` and to the log line instead. The counter says
# that it happened; the marker and the log say to which node, with which name.
MISSING_GROUP: typing.Final[str] = "<missing>"


class GateDecision(str, enum.Enum):
    """How one gate call ended, for the counter these outcomes share.

    Exclusive by construction: the interceptor takes exactly one of these paths per node.

    Exhaustive as well as exclusive, which is what lets a dashboard subtract. `CONTENDED` and
    `ERROR` are the two that used to be missing, and they are kept apart on purpose: an
    exhausted compare-and-set budget is a normal outcome under load, an exception is not, and
    a panel that cannot tell "the group is busy" from "the gate is broken" is worse than no
    panel.
    """

    # The node got a slot and may run.
    ADMITTED = "ADMITTED"
    # The group was full and the node was parked for the first time — no prior claim row.
    PARKED = "PARKED"
    # The node had already been promoted once and went back to waiting. Split out from PARKED
    # because it means something different: a high re-park rate says promotion is
    # over-optimistic and nodes are thrashing, which asks for a different fix from "the group
    # is simply full". The interceptor can tell them apart — a re-park already has a WAITING
    # claim row carrying its original created_at, a first park does not.
    REPARKED = "REPARKED"
    # The node named a group that does not exist, so nothing gated it and it ran. Above zero
    # means a misspelled annotation is in production.
    UNGATED = "UNGATED"
    # Every attempt saw room and lost the compare-and-set anyway, so the gate gave up and left
    # the node QUEUED for a later pass. Deliberately not PARKED: nothing established the group
    # was full. This is the only member that records no change to the node at all. Sustained
    # above zero means DEFAULT_MAX_CAS_ATTEMPTS is too small for the group's arrival rate.
    CONTENDED = "CONTENDED"
    # The gate raised. Not a decision the gate made so much as one it failed to make, and the
    # node is left exactly as the sweep found it. Counted because the alternative is that an
    # exception is silently indistinguishable from contention: both used to record a duration
    # and no decision.
    ERROR = "ERROR"


class PromotionTrigger(str, enum.Enum):
    """What ran a promotion pass, for the promotions counter.

    They all do the same work; they differ in what woke them, and that is what an operator
    needs when promotions stop happening. No count in this sentence on purpose -- it said
    "all three" while there were four, which is how a docstring falls behind its own enum.

    Four are call sites. `RECONCILE` is the odd one, and reading it as a fifth call site is
    the mistake to avoid: it fires precisely because a call site did *not*.
    """

    # A claim was released, so the sink swept for waiters.
    SINK = "SINK"
    # An operator raised the group's capacity via PATCH.
    PATCH = "PATCH"
    # An operator forced a pass via POST .../promote.
    PROMOTE_API = "PROMOTE_API"
    # An operator deleted a claim by hand, which frees a slot and sweeps like a completion
    # would. Not in the original three: the design counted the call sites that exist to
    # promote, and missed that DELETE .../claims/{node} promotes as a side effect. It is kept
    # apart from PROMOTE_API because it answers a different question -- PROMOTE_API is somebody
    # asking why nothing is moving, this is somebody prising a stuck claim loose.
    CLAIM_RELEASE_API = "CLAIM_RELEASE_API"
    # The level-triggered reconciler found a group holding a parked node beside a free slot
    # for longer than any in-flight edge could explain -- `quota/reconciler.py:35` derives
    # that threshold as one consumer lease plus one tick, not a round number. Unlike the four
    # above, this one is not a call site so much as an absence of one: a non-zero rate here
    # means an edge was lost. It is the alert, not the all-clear.
    RECONCILE = "RECONCILE"


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    SECONDS = "s"
    CLAIMS = "{claim}"


quota_meter = otel_metrics.get_meter("tangle.quota")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

gate_decisions = quota_meter.create_counter(
    name="quota.gate_decisions",
    description="Number of gate calls, by quota group and how the call ended",
    unit=MetricUnit.CLAIMS,
)

promotions = quota_meter.create_counter(
    name="quota.promotions",
    description=(
        "Number of waiting nodes un-parked, by quota group and which call site ran the pass"
    ),
    unit=MetricUnit.CLAIMS,
)

claims_force_deleted = quota_meter.create_counter(
    name="quota.claims_force_deleted",
    description=(
        "Number of claims an operator deleted by hand through the API, by quota group. Every"
        " increment is a human working around the system, so this is worth a dashboard panel"
        " even though it should sit at zero"
    ),
    unit=MetricUnit.CLAIMS,
)


# ---------------------------------------------------------------------------
# Duration histograms (independent: a gate call is not inside a promotion pass)
# ---------------------------------------------------------------------------

duration_gate = quota_meter.create_histogram(
    name="quota.duration.gate",
    description=(
        "Time one gate call took, occupancy SELECT, compare-and-set and commit included. This"
        " runs inside the single-threaded sweep, so its p99 is the sweep's throughput ceiling"
    ),
    unit=MetricUnit.SECONDS,
)

duration_promote = quota_meter.create_histogram(
    name="quota.duration.promote",
    description="Time one promotion pass took, whichever call site triggered it",
    unit=MetricUnit.SECONDS,
)

duration_wait = quota_meter.create_histogram(
    name="quota.duration.wait",
    description=(
        "How long an admitted node had been parked, from the claim's created_at to the"
        " admission. The number that says whether a capacity is set correctly, which no gauge"
        " can: a p50 of 200ms and a p50 of 40 minutes are both a waiters gauge reading 1"
    ),
    unit=MetricUnit.SECONDS,
)


# ---------------------------------------------------------------------------
# Recording helpers
# ---------------------------------------------------------------------------
#
# All three swallow their own failures. Measuring the gate must never change what the gate
# decided, so a broken instrument costs a log line and nothing else.


def increment(
    *,
    counter: otel_metrics.Counter,
    attributes: dict[str, str],
) -> None:
    """Add one to a counter, logging instead of raising if that fails.

    Args:
        counter: The counter to add to.
        attributes: The labels to record the increment under.
    """
    add(counter=counter, amount=1, attributes=attributes)


def add(
    *,
    counter: otel_metrics.Counter,
    amount: int,
    attributes: dict[str, str],
) -> None:
    """Add `amount` to a counter in one call, logging instead of raising if that fails.

    One call rather than `amount` calls: each `add` takes the SDK's per-instrument lock and
    hashes the attribute set, and a promotion pass un-parks a whole batch at once, so a loop
    would pay that cost per node for a series that would be identical either way.

    An amount below one records nothing — the SDK rejects a negative delta on a monotonic
    counter, and a promotion pass that found no waiters is the common case.

    Args:
        counter: The counter to add to.
        amount: How much to add.
        attributes: The labels to record it under.
    """
    if amount < 1:
        return
    try:
        counter.add(amount, attributes=attributes)
    except Exception:
        logger.warning(
            f"Failed to add {amount} to quota counter {attributes}",
            exc_info=True,
        )


def record(
    *,
    histogram: otel_metrics.Histogram,
    seconds: float,
    quota_group: str,
    attributes: collections.abc.Mapping[str, str] = types.MappingProxyType({}),
) -> None:
    """Record a duration on a histogram, logging instead of raising if that fails.

    Args:
        histogram: The duration histogram to record on.
        seconds: The measured duration.
        quota_group: The group the time was spent on, recorded on every duration.
        attributes: Any further dimensions the caller wants on this observation. Without one
            of these, a promote duration cannot carry its trigger, and "the sink stopped
            emitting" and "the group is genuinely full" are the same point on a dashboard.
    """
    try:
        # The group label goes on last because it is not the caller's to override: it is the
        # identity of the series, and a caller passing its own would silently split one group's
        # timings across two.
        histogram.record(
            seconds, attributes={**attributes, QUOTA_GROUP_LABEL: quota_group}
        )
    except Exception:
        logger.warning(
            f"Failed to record quota duration for {quota_group}", exc_info=True
        )


# ---------------------------------------------------------------------------
# Gauges
# ---------------------------------------------------------------------------
#
# Built on demand rather than at import, like the emission backlog gauges: only a process that
# actually polls for these numbers has anything to report, and the callback that answers
# belongs to that caller. Every one of them is observed from the metrics poller and never from
# the orchestrator — see `quota/observability/poller.py` for why.

GaugeCallback = typing.Callable[
    [otel_metrics.CallbackOptions], typing.Iterable[otel_metrics.Observation]
]


def create_occupancy_gauge(
    *,
    callback: GaugeCallback,
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how many slots each group currently has taken.

    Computed by the same `quota/occupancy.py` query the gate admits with, so the dashboard and
    the gate cannot disagree about how full a group is.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per known
            group. It runs on the exporter's thread, so it must return cached values rather
            than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return quota_meter.create_observable_gauge(
        name="quota.occupancy",
        callbacks=[callback],
        description="Number of slots currently occupied in each quota group",
        unit=MetricUnit.CLAIMS,
    )


def create_capacity_gauge(
    *,
    callback: GaugeCallback,
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting each group's configured capacity.

    Exported even though it is a setting rather than a measurement, so that `occupancy` and
    `waiters` can be read against it on one dashboard without a second data source. It is also
    what distinguishes a stuck group from a paused one: waiters climbing against `capacity = 0`
    means somebody left it paused.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per known
            group. It runs on the exporter's thread, so it must return cached values rather
            than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return quota_meter.create_observable_gauge(
        name="quota.capacity",
        callbacks=[callback],
        description="Configured number of slots in each quota group",
        unit=MetricUnit.CLAIMS,
    )


def create_waiters_gauge(
    *,
    callback: GaugeCallback,
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how many nodes are parked waiting on each group.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per known
            group, including zero for the groups nobody is waiting on — a gauge that vanishes
            when the condition clears makes "healthy" and "not being measured" the same
            picture. It runs on the exporter's thread, so it must return cached values.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return quota_meter.create_observable_gauge(
        name="quota.waiters",
        callbacks=[callback],
        description="Number of nodes parked waiting on each quota group",
        unit=MetricUnit.CLAIMS,
    )


def create_oldest_waiter_age_gauge(
    *,
    callback: GaugeCallback,
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how long each group's oldest waiter has been parked.

    The most alertable number quota groups produce, and the reason the gauges are polled from
    outside the orchestrator: a dead orchestrator and a merely stuck group look identical here,
    both climbing, so one threshold catches both. Read together with `occupancy` and
    `capacity` it also separates the causes — high age with `occupancy < capacity` means
    promotion itself is broken.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per known
            group, zero for the groups with no waiters. It runs on the exporter's thread, so
            it must return cached values rather than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return quota_meter.create_observable_gauge(
        name="quota.oldest_waiter_age",
        callbacks=[callback],
        description="Age of the oldest parked claim in each quota group",
        unit=MetricUnit.SECONDS,
    )


def create_active_claims_gauge(
    *,
    callback: GaugeCallback,
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how many claims each group has in `ACTIVE`.

    Deliberately not a second name for `quota.occupancy`, and the pair is the point. Occupancy
    is counted off the *node's* container status (`quota/occupancy.py:40`); this is counted off
    the *claim's* state. In health the two agree, and a gap between them is the signature of a
    settlement outage -- the gate is writing ACTIVE claims and nothing is flipping them DONE.
    One number cannot show that; two can.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per known
            group, zero included, for the same reason `create_waiters_gauge` gives. It runs on
            the exporter's thread, so it must return cached values.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return quota_meter.create_observable_gauge(
        name="quota.active_claims",
        callbacks=[callback],
        description="Number of ACTIVE claims on each quota group",
        unit=MetricUnit.CLAIMS,
    )


def create_oldest_active_age_gauge(
    *,
    callback: GaugeCallback,
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how long each group's oldest ACTIVE claim has been running.

    An input to a judgement, not a verdict. This number cannot tell a wedged slot from a job
    that is simply long: the poller sees a claim's timestamp and has no expected duration,
    heartbeat or progress signal to compare it against, so a six-hour reading is either a
    fault or the reason the group exists. An operator who knows their workload can threshold
    it per group; nothing here should assert "wedged" on their behalf.

    Args:
        callback: Called by the SDK on each collection cycle; yields one observation per known
            group, zero for the groups with nothing running. It runs on the exporter's thread,
            so it must return cached values.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return quota_meter.create_observable_gauge(
        name="quota.oldest_active_age",
        callbacks=[callback],
        description="Age of the oldest ACTIVE claim in each quota group",
        unit=MetricUnit.SECONDS,
    )
