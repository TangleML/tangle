"""The emission meter and its instruments.

Declaration only: this module names what is measured, and the producer, consumer, and
handlers record into it. Instrument names follow `emission.<role>.<family>.<name>`, where
role is where the measurement is made from (`producer`, `consumer`, or `queue` for the two
gauges read by neither end of the pipeline) and family is the instrument kind (`count`,
`gauge`, or `duration`). The export path is statsd_exporter, which rewrites the dots to
underscores and appends nothing, so `emission.written` is queried as `emission_written` --
verified against production, not inferred from Prometheus' native-OTLP naming convention, which
would have added a `_total` suffix that is not there.

The duration histograms are deliberately separate instruments rather than one instrument with
a stage label: the stages nest (total contains dispatch contains handle contains sink, and
poll_db contains claim), so summing across a stage label would count the same time several
times over.

The two `count` instruments the consumer writes are separate for the same kind of reason. One
counts deliveries and one counts settled emissions, and an emission declares as many sinks as
it likes: summing the deliveries across the sink label answers "how much was delivered", not
"how many emissions were handled", so neither number is derivable from the other.
"""

import enum
import logging
import typing

from opentelemetry import metrics as otel_metrics

from cloud_pipelines_backend.emissions.observability import environment

logger = logging.getLogger(__name__)

# The label carried by every emission instrument: which kind of emission was measured
# (readiness, metadata, ...).
EMISSION_TYPE_LABEL: typing.Final[str] = "emission_type"
# Carried by the settled counter only: how far the fan-out got. Named after the column that
# stores the same value, so a metric and a query about it cannot disagree.
HANDLE_STATUS_LABEL: typing.Final[str] = "handle_status"
# Carried by the per-delivery instruments: which sink the delivery went to, by the annotation
# key the node declared it with. Cardinality is the number of implemented sinks.
SINK_LABEL: typing.Final[str] = "sink"
# Carried by the delivery counter only: what that one sink reported.
OUTCOME_STATUS_LABEL: typing.Final[str] = "outcome_status"
# Carried by the delivery counter only: who a failed delivery belongs to. `fail` on its own
# does not say — a rejected report is the reporting team's configuration and a collector that
# is down is ours — and paging on their sum is paging on noise.
DELIVERY_REASON_LABEL: typing.Final[str] = "delivery_reason"
# Carried by the delivery counter only: the class of the HTTP status a network sink got back,
# as `2xx`/`4xx`/`5xx`. The class, not the status, so the label stays three values wide.
STATUS_CLASS_LABEL: typing.Final[str] = "status_class"
# Carried by the delivery counter only: the ContainerExecutionStatus the originating node
# emitted on. Independent of `outcome_status`, which is the delivery's verdict: a report from a
# FAILED node can be accepted and one from a SUCCEEDED node refused. Bounded by the enum.
NODE_STATUS_LABEL: typing.Final[str] = "node_status"
# Carried by the claim counter only: how the attempt to take a row ended.
CLAIM_OUTCOME_LABEL: typing.Final[str] = "claim_outcome"
# Carried by `emission.duration.total` only: which end of the pipeline the time was spent at.
ROLE_LABEL: typing.Final[str] = "role"
PRODUCER_ROLE: typing.Final[str] = "producer"
CONSUMER_ROLE: typing.Final[str] = "consumer"


class DeliveryReason(str, enum.Enum):
    """Who a delivery's result belongs to, for the counter that has to be alertable.

    A sink maps its own codes onto these. The vocabulary is deliberately tiny and about
    ownership rather than mechanism: the mechanism is already on the outcome row, and a label
    that grows a member per failure mode is a label nobody can write an alert against.
    """

    # Nothing failed.
    NONE = "none"
    # The reporting node or its target is misconfigured. Unactionable by Tangle, and must
    # never page: a 422 from the collector means the payload named something it does not know.
    CUSTOMER_CONFIG = "customer_config"
    # This workload could not prove who it was. Ours, and usually a deploy or IAM change.
    AUTH = "auth"
    # The far side answered badly or not at all. Ours to escalate, and the one worth paging on.
    UPSTREAM = "upstream"
    # The delivery never left this process — the report could not be read.
    INTERNAL = "internal"
    # A sink reported a reason outside this vocabulary. Never written by a sink directly:
    # `coerce_reason` produces it when it is handed something it does not recognize, so a
    # misbehaving sink shows up as one extra label value instead of a new time series.
    UNKNOWN = "unknown"


def coerce_reason(
    *,
    reason: object,
) -> str:
    """Force whatever a sink reported into the vocabulary an alert can be written against.

    The reason arrives from a sink's free-form detail mapping, and it becomes a metric label.
    An uncoerced value is not merely mislabelled: every distinct string mints its own time
    series, so one sink's typo both escapes the alert written against these five members and
    grows the counter's cardinality without bound. Coercing keeps this layer ignorant of any
    sink's own code vocabulary — DeliveryReason is this module's type, not a sink's.

    Args:
        reason: The `reason` a sink put on its outcome detail, or None when it put none.

    Returns:
        A DeliveryReason value: `none` when nothing was reported, the member's own value when
        it is one, and `unknown` for anything else — never NONE for an unrecognized value,
        which would file a failure under "nothing failed".
    """
    if not reason:
        return DeliveryReason.NONE.value
    try:
        return DeliveryReason(reason).value
    except ValueError:
        logger.warning("Unrecognized delivery reason %r on an outcome detail", reason)
        return DeliveryReason.UNKNOWN.value


def status_class(
    *,
    status_code: object,
) -> str:
    """Reduce an HTTP status to its class, or say there was not one.

    Takes `object` rather than `int | None` because the only caller reads it out of a sink's
    free-form detail mapping, where nothing enforces the annotation. The floor division would
    raise on anything else, and it is evaluated as an argument to `increment` — outside the
    never-raise wrapper that would otherwise absorb it — so the guard has to live here.

    Args:
        status_code: The status the sink got back, or None when it never got an answer.

    Returns:
        `2xx`, `4xx`, `5xx` and so on, or `none` when there was no answer to classify.
    """
    # `isinstance(True, int)` is True in Python, and `True // 100` would quietly report `0xx`.
    if not isinstance(status_code, int) or isinstance(status_code, bool):
        return "none"
    return f"{status_code // 100}xx"


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    SECONDS = "s"
    EMISSIONS = "{emission}"
    PAYLOADS = "{payload}"


emission_meter = otel_metrics.get_meter("tangle.emissions")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

producer_written = emission_meter.create_counter(
    name="emission.written",
    description="Number of emission_event rows the producer appended, by emission type",
    unit=MetricUnit.EMISSIONS,
)

consumer_handled = emission_meter.create_counter(
    name="emission.handled",
    description=(
        "Number of emission_event rows the consumer settled, by emission type"
        " and how far the fan-out got"
    ),
    unit=MetricUnit.EMISSIONS,
)

consumer_delivered = emission_meter.create_counter(
    name="emission.delivered",
    description=(
        "Number of deliveries made, by emission type, the sink they went to, what that sink"
        " reported, and the status the originating node emitted on. One point per payload for"
        " a sink that delivers several in one call, so this counts reports placed rather than"
        " sink calls made; emission.payloads_per_delivery is what relates the two"
    ),
    unit=MetricUnit.EMISSIONS,
)

consumer_outcome_collisions = emission_meter.create_counter(
    name="emission.outcome_collisions",
    description=(
        "Number of delivery rows that lost to a row already on the ledger,"
        " by emission type and sink"
    ),
    unit=MetricUnit.EMISSIONS,
)

consumer_recorder_unavailable = emission_meter.create_counter(
    name="emission.recorder_unavailable",
    description=(
        "Number of deliveries that were made and could not be recorded, by emission type. The"
        " event keeps its claim and is delivered again once the lease runs out, so a nonzero"
        " rate is the only signal for a class of failure that leaves no verdict on the row"
    ),
    unit=MetricUnit.EMISSIONS,
)

consumer_delivery_incomplete = emission_meter.create_counter(
    name="emission.delivery_incomplete",
    description=(
        "Number of emissions a handler stopped part-way through and handed back, by emission"
        " type. The row keeps its claim and is delivered again once the lease runs out, so a"
        " low rate is a fan-out that needed a second pass and a sustained one is a fan-out"
        " that is not converging. Unlike every other failure the consumer sees, this one never"
        " reaches the backoff ladder, so nothing else reports it"
    ),
    unit=MetricUnit.EMISSIONS,
)

consumer_claims = emission_meter.create_counter(
    name="emission.claims",
    description=(
        "Number of attempts to claim an emission_event row, by emission type"
        " and how the attempt ended"
    ),
    unit=MetricUnit.EMISSIONS,
)

# The producer's transition ledger, counted at both ends. A transition is recorded when a
# node's status is assigned and drained when that node's commit emits, so in a healthy process
# the two run level with each other, a commit apart. Recorded minus drained is what is
# currently in flight; a floor that grows across restarts is a leak, and the size gauge below
# is what shows it. Rejected-stale is the third outcome: an assignment whose transaction rolled
# back, which is normal and should stay rare.
producer_transitions_recorded = emission_meter.create_counter(
    name="emission.transitions_recorded",
    description="Number of node status assignments the producer recorded for the next commit",
    unit=MetricUnit.EMISSIONS,
)

producer_transitions_drained = emission_meter.create_counter(
    name="emission.transitions_drained",
    description="Number of recorded transitions the producer drained into a commit",
    unit=MetricUnit.EMISSIONS,
)

producer_transitions_rejected_stale = emission_meter.create_counter(
    name="emission.transitions_rejected_stale",
    description=(
        "Number of recorded transitions dropped at drain time because the node's attribute no"
        " longer agreed with them — the assignment was rolled back"
    ),
    unit=MetricUnit.EMISSIONS,
)


# ---------------------------------------------------------------------------
# Duration histograms (nested: total >= dispatch >= handle >= sink, and
# total >= poll_db >= claim)
# ---------------------------------------------------------------------------

# The one place a role survives in an attribute rather than a name. Dropping `<role>` from the
# instrument names merged exactly these two — the producer's before-commit work and a whole
# consumer cycle — and they measure different things, so they stay one series apart. This does
# not reopen the argument against a stage label: stages nest (total contains dispatch contains
# handle), and summing across them double-counts; producer and consumer time do not nest.
duration_total = emission_meter.create_histogram(
    name="emission.duration.total",
    description=(
        "Time one role spent on one emission: the producer preparing and writing the row, or"
        " the consumer's full cycle of poll, dispatch and terminal writeback"
    ),
    unit=MetricUnit.SECONDS,
)

consumer_duration_poll_db = emission_meter.create_histogram(
    name="emission.duration.poll_db",
    description="Time to claim one row, read it with its annotations, and build the message",
    unit=MetricUnit.SECONDS,
)

consumer_duration_claim = emission_meter.create_histogram(
    name="emission.duration.claim",
    description="Time for the conditional UPDATE that takes one row, and its commit",
    unit=MetricUnit.SECONDS,
)

consumer_duration_dispatch = emission_meter.create_histogram(
    name="emission.duration.dispatch",
    description="Time to route one message and run its handler's parse and handle steps",
    unit=MetricUnit.SECONDS,
)

consumer_duration_handle = emission_meter.create_histogram(
    name="emission.duration.handle",
    description=(
        "Time a handler spent handling one message, every sink call it made included"
    ),
    unit=MetricUnit.SECONDS,
)

consumer_duration_sink = emission_meter.create_histogram(
    name="emission.duration.sink",
    description="Time one sink spent performing the side effect for one message",
    unit=MetricUnit.SECONDS,
)

consumer_duration_collector_post = emission_meter.create_histogram(
    name="emission.duration.collector_post",
    description=(
        "Time one HTTP request to a collector took, from inside the sink that made it. Nested"
        " within the sink stage rather than beside it: the sink also resolves the payload, so"
        " this separates the network hop Tangle does not control from the work it does. One"
        " request, not one delivery: a delivery carrying several payloads records one"
        " observation per attempt, so the distribution stays the latency of a round trip"
    ),
    unit=MetricUnit.SECONDS,
)


# ---------------------------------------------------------------------------
# Distributions that are not durations
# ---------------------------------------------------------------------------

consumer_payloads_per_delivery = emission_meter.create_histogram(
    name="emission.payloads_per_delivery",
    description=(
        "How many payloads one delivery carried, by emission type and sink. A distribution"
        " rather than a counter because the delivery counter already sums the payloads: what"
        " this answers is the shape of a batch, which is what says whether a rise in payloads"
        " is more runs reporting or the same runs reporting more, and what bounds how long one"
        " delivery can hold the consumer's thread"
    ),
    unit=MetricUnit.PAYLOADS,
)


# ---------------------------------------------------------------------------
# Recording helpers
# ---------------------------------------------------------------------------
#
# Both swallow their own failures. Measuring an emission must never change what the emission
# did, so a broken instrument costs a log line and nothing else.


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
    hashes the attribute set, and the producer's caller is inside a commit, so a loop over a
    multi-node commit pays that cost per node for a series that would be identical either way.

    An amount below one records nothing. The SDK rejects a negative delta on a monotonic
    counter, and zero would buy a lock for no data point.

    Args:
        counter: The counter to add to.
        amount: How much to add.
        attributes: The labels to record it under.
    """
    if amount < 1:
        return
    try:
        counter.add(
            amount,
            attributes=environment.with_environment(attributes=attributes),
        )
    except Exception:
        logger.warning(
            f"Failed to add {amount} to emission counter {attributes}",
            exc_info=True,
        )


def record(
    *,
    histogram: otel_metrics.Histogram,
    seconds: float,
    emission_type: str,
    attributes: dict[str, str] | None = None,
) -> None:
    """Record a duration on a histogram, logging instead of raising if that fails.

    Call this inside the active span so the SDK can attach the current trace id to the
    bucket as an exemplar, which is what links a slow bucket back to one emission's trace.

    Args:
        histogram: The duration histogram to record on.
        seconds: The measured duration.
        emission_type: The kind of emission, recorded on every duration.
        attributes: Labels to record beside the emission type, for a stage that happens more
            than once per emission and has to say which one this was.
    """
    try:
        histogram.record(
            seconds,
            attributes=environment.with_environment(
                attributes={
                    EMISSION_TYPE_LABEL: emission_type,
                    **(attributes or {}),
                }
            ),
        )
    except Exception:
        logger.warning(
            f"Failed to record emission duration for {emission_type}",
            exc_info=True,
        )


def record_count(
    *,
    histogram: otel_metrics.Histogram,
    count: int,
    emission_type: str,
    attributes: dict[str, str] | None = None,
) -> None:
    """Record a count on a histogram, logging instead of raising if that fails.

    Separate from `record` only in what it means: that one is seconds and this one is a
    number of things, and a helper named for the unit is what stops a count being recorded
    onto a duration histogram by mistake.

    Args:
        histogram: The distribution to record on.
        count: The measured count.
        emission_type: The kind of emission, recorded on every distribution.
        attributes: Labels to record beside the emission type.
    """
    try:
        histogram.record(
            count,
            attributes=environment.with_environment(
                attributes={
                    EMISSION_TYPE_LABEL: emission_type,
                    **(attributes or {}),
                }
            ),
        )
    except Exception:
        logger.warning(
            f"Failed to record emission payload count for {emission_type}",
            exc_info=True,
        )


def create_pending_gauge(
    *,
    callback: typing.Callable[
        [otel_metrics.CallbackOptions],
        typing.Iterable[otel_metrics.Observation],
    ],
) -> otel_metrics.ObservableGauge:
    """Create the pending-backlog gauge, observed through `callback`.

    Unlike the instruments above, this one is built on demand rather than at import: only a
    process that actually drains the queue can say how deep the backlog is, and the callback
    that answers that belongs to the caller. Building it here with its callback already
    attached keeps the observation wired up at creation.

    Args:
        callback: Called by the SDK on each collection cycle; yields the current count of
            rows the queue has not settled. It runs on the exporter's thread, so it should
            return a cached value rather than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return emission_meter.create_observable_gauge(
        name="emission.pending",
        callbacks=[callback],
        description="Number of emission_event rows the queue has not settled yet",
        unit=MetricUnit.EMISSIONS,
    )


def create_pending_transitions_gauge(
    *,
    callback: typing.Callable[
        [otel_metrics.CallbackOptions],
        typing.Iterable[otel_metrics.Observation],
    ],
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how many recorded transitions are waiting to be drained.

    This is the leak detector for the producer's ledger, and the only one that works. Counting
    recorded against drained cannot do the job: a record whose session is garbage-collected
    before it commits disappears without ever being drained, so the two counters part company
    permanently and the gap says nothing about now. The size of the ledger does — it should sit
    at or near zero between commits, and a floor that climbs is a leak.

    Built on demand like the backlog gauge, because only a process that runs the producer has a
    ledger to report on.

    Args:
        callback: Called by the SDK on each collection cycle; yields the current size of the
            ledger. It runs on the exporter's thread, so it must read the size and nothing
            more — never iterate the mapping, whose entries the emitting thread is free to add
            and remove while the callback runs.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return emission_meter.create_observable_gauge(
        name="emission.pending_transitions",
        callbacks=[callback],
        description=(
            "Number of node status assignments recorded and not yet drained into a commit"
        ),
        unit=MetricUnit.EMISSIONS,
    )


def create_oldest_pending_age_gauge(
    *,
    callback: typing.Callable[
        [otel_metrics.CallbackOptions],
        typing.Iterable[otel_metrics.Observation],
    ],
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how long the oldest unclaimed row has been waiting.

    Built on demand for the same reason as the backlog gauge: the callback belongs to
    whichever process polls for it. Unlike the backlog gauge, that process is deliberately
    not the consumer — see `emissions/observability/backlog_poller.py`.

    Args:
        callback: Called by the SDK on each collection cycle; yields the age in seconds of
            the oldest row no consumer has claimed. It runs on the exporter's thread, so it
            should return a cached value rather than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return emission_meter.create_observable_gauge(
        name="emission.oldest_pending_age",
        callbacks=[callback],
        description="Age of the oldest emission_event row no consumer has claimed",
        unit=MetricUnit.SECONDS,
    )


def create_oldest_in_progress_age_gauge(
    *,
    callback: typing.Callable[
        [otel_metrics.CallbackOptions],
        typing.Iterable[otel_metrics.Observation],
    ],
) -> otel_metrics.ObservableGauge:
    """Create the gauge reporting how long the oldest claimed row has gone unsettled.

    Args:
        callback: Called by the SDK on each collection cycle; yields the age in seconds of
            the oldest row a consumer holds but has not settled. It runs on the exporter's
            thread, so it should return a cached value rather than query anything.

    Returns:
        The gauge instrument, which the caller keeps a reference to for as long as it should
        be observed.
    """
    return emission_meter.create_observable_gauge(
        name="emission.oldest_in_progress_age",
        callbacks=[callback],
        description="Age of the oldest emission_event row a consumer holds but has not settled",
        unit=MetricUnit.SECONDS,
    )
