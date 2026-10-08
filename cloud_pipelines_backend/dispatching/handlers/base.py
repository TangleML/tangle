"""Generic dispatching contracts shared by every message handler."""

import abc
import dataclasses
import enum
import json
import typing

IntentT = typing.TypeVar("IntentT")

# One value as it survives a round trip through a JSON column.
JsonValue = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
# The shape of an Outcome's detail: a JSON object the caller can store as-is.
JsonDetail = dict[str, JsonValue]


class DispatchableMessage(typing.Protocol):
    """The minimal shape the dispatcher needs from any message it routes.

    Structural: any object exposing these two attributes satisfies the contract, so this
    layer never depends on a concrete message type.
    """

    # The routing key the dispatcher looks up to find this message's handler.
    routing_key: str
    # A stable id used to correlate logs and outcomes back to the source message.
    message_id: str


MessageT = typing.TypeVar("MessageT", bound=DispatchableMessage)


class OutcomeStatus(str, enum.Enum):
    """The disposition of one delivery: what one sink reported.

    Per sink, not per message. A message whose intent declares several sinks produces one of
    these per sink, each recorded on its own; `HandleStatus` is the message-grain verdict.
    """

    # The sink completed its work.
    SUCCESS = "success"
    # The sink reported a failure rather than raising. Terminal — there is no retry.
    FAIL = "fail"
    # The sink ran and found nothing actionable, or the delivery was never attempted because
    # the sink key named no implementation.
    IGNORE = "ignore"


class HandleStatus(str, enum.Enum):
    """What became of one message's whole fan-out.

    Per message, so it sorts on whether every declared delivery was reached rather than on
    whether those deliveries succeeded: a sink that reported `fail` still ran, so its message
    is COMPLETE and the failure sits on that delivery's own record. The three other values
    all mean somebody has to change something — a message that parsed into no work, a sink
    key with no implementation behind it, or a raise.
    """

    # Every sink the intent declared was reached. Their verdicts are on the outcome records.
    COMPLETE = "complete"
    # Parsing produced no intent, so there was nothing to deliver.
    NOTHING_TO_DO = "nothing_to_do"
    # A declared sink never reached an implementation, so one delivery was never attempted.
    INCOMPLETE = "incomplete"
    # Nobody reported: no handler for the routing key, or parse or handle raised.
    FAILED = "failed"


def ensure_storable(
    *,
    detail: JsonDetail,
) -> None:
    """Raise unless the detail can be written to a JSON column and read back unchanged.

    `allow_nan=False` rejects NaN, Infinity and -Infinity, which the encoder emits by default
    but which are not JSON. Decoding the encoded form and comparing it back rejects the values
    the encoder rewrites on the way out — non-string dict keys become strings, tuples become
    lists — since the column would return something other than what the caller put in.

    Args:
        detail: The outcome detail to check.

    Raises:
        TypeError: If detail holds a value the encoder cannot serialize.
        ValueError: If detail holds a non-JSON constant or does not survive the round trip.
        RecursionError: If detail is nested too deeply to encode.
    """
    encoded = json.dumps(detail, allow_nan=False)
    if json.loads(encoded) != detail:
        raise ValueError("detail does not survive a JSON round trip unchanged")


def safe_detail(
    *,
    detail: JsonDetail | None,
) -> JsonDetail | None:
    """Return the detail when it is storable, otherwise a stand-in that always is.

    The caller records the detail it is handed and commits. One it cannot store would leave
    the message unfinished, so it is swapped for a value naming what happened. Returns the
    same object when nothing is wrong, so a caller can tell the two apart by identity.

    Args:
        detail: The outcome detail to check.

    Returns:
        detail itself when it is storable, otherwise a stand-in recording the reason and the
        encoder's complaint.
    """
    if detail is None:
        return None
    try:
        ensure_storable(detail=detail)
    except (TypeError, ValueError, RecursionError) as exc:
        return {"reason": "detail_unstorable", "error": repr(exc)}
    return detail


@dataclasses.dataclass(frozen=True, kw_only=True)
class Outcome:
    """The result of one delivery, handed by a sink to the handler that called it.

    One of these is recorded per sink, through the OutcomeRecorder below.
    """

    # The disposition recorded for this delivery.
    status: OutcomeStatus
    # Detail for the caller to record (sink response, error, or reason); None when there is
    # nothing to record. The caller writes it to a JSON column, so it must be storable there.
    detail: JsonDetail | None = None

    def __post_init__(self) -> None:
        """Reject a detail the caller would not be able to record.

        Raises:
            ValueError: If detail cannot be written to a JSON column and read back unchanged.
        """
        if self.detail is None:
            return
        try:
            ensure_storable(detail=self.detail)
        except (TypeError, ValueError, RecursionError) as exc:
            raise ValueError(
                f"Outcome.detail is not storable as JSON: {exc!r}"
            ) from exc


@dataclasses.dataclass(frozen=True, kw_only=True)
class HandleResult:
    """What handling one message produced: the fan-out's verdict, returned to the caller.

    It carries no per-sink verdicts. Each delivery's Outcome is already recorded through the
    OutcomeRecorder by the time this returns, so the caller records only the message-grain
    verdict and never has to reconcile two accounts of the same delivery.
    """

    # What became of the fan-out as a whole.
    status: HandleStatus
    # Every sink key whose delivery was never attempted, which is what makes a fan-out
    # INCOMPLETE. Plain strings: each handler owns its own sink key set and this layer never
    # names them.
    unresolved_sinks: tuple[str, ...] = ()
    # Detail for the caller to record; None when there is nothing to record. The caller writes
    # it to a JSON column, so it must be storable there — the router checks that on the way
    # out, see safe_detail above.
    detail: JsonDetail | None = None


class RecorderUnavailable(Exception):
    """A delivery was made and could not be recorded, by a failure a retry may not hit.

    Not a verdict on the message. The router re-raises it instead of reporting `failed`, so
    the caller leaves the message unsettled rather than closing one whose ledger is missing a
    delivery already made.
    """


class DeliveryIncomplete(Exception):
    """A sink stopped part-way through work it can finish later, and wants the message back.

    Not a verdict either, and for the mirror-image reason to `RecorderUnavailable`: that one
    says a delivery happened and could not be recorded, this one says some of the delivery
    has not happened yet. Both must leave the message unsettled -- a `failed` verdict would
    close it and there is no third party to notice the remainder.

    The sink raises this only when stopping is safe: whatever it did commit must be recognised
    as already done when the message comes back, so the redelivery resumes rather than repeats.
    """


class OutcomeRecorder(abc.ABC):
    """The port a handler records one delivery's Outcome through.

    Abstract here so this layer keeps its own storage-free property: the implementation lives
    with the caller that owns the records. Keyed by sink key, a plain string for the same
    reason `HandleResult.unresolved_sinks` is.

    A delivery is recorded as soon as its sink returns rather than at the end of the fan-out,
    so a crash part-way through leaves the deliveries already made recorded — which is what
    `is_done` reads when the message is handled again.
    """

    @abc.abstractmethod
    def is_done(
        self,
        *,
        sink_key: str,
    ) -> bool:
        """Whether this delivery is already recorded.

        Args:
            sink_key: The key of the sink about to be called.

        Returns:
            True when a record for it already exists, so the delivery must not be repeated.
        """

    @abc.abstractmethod
    def record(
        self,
        *,
        sink_key: str,
        outcome: Outcome,
    ) -> None:
        """Record what one sink reported.

        Args:
            sink_key: The key of the sink that was called.
            outcome: What it reported.

        Raises:
            RecorderUnavailable: If the record could not be written by a failure a retry may
                not hit. The delivery is left unrecorded, so the caller must not treat the
                message as finished.
        """

    @abc.abstractmethod
    def record_unresolved(
        self,
        *,
        sink_key: str,
    ) -> None:
        """Record that a declared sink key reached no implementation.

        Separate from `record` so the reason code for a misconfigured sink is written in one
        place instead of being spelled out by each handler.

        Args:
            sink_key: The declared key nothing could deliver.
        """


@dataclasses.dataclass(frozen=True, kw_only=True)
class ParseIssue:
    """One validation finding produced by a handler's parse step."""

    # A short, stable identifier for the kind of problem, from the handler's own code set.
    code: str
    # Human-readable detail for the log line.
    message: str
    # True when the whole intent was discarded; False when the intent survived (an
    # informational warning, or an optional part was dropped while the intent was kept).
    dropped: bool


@dataclasses.dataclass(frozen=True, kw_only=True)
class ParseResult(typing.Generic[IntentT]):
    """What a handler's parse step returns: the typed intent plus any validation issues.

    `issues` can be non-empty even when `intent` is set, meaning the intent was kept but
    degraded.
    """

    # The parsed intent, or None when parsing found nothing actionable.
    intent: IntentT | None
    # Every validation finding from parsing; may be empty.
    issues: list[ParseIssue] = dataclasses.field(default_factory=list)
    # Sink keys the message declared that this build has no implementation behind — a newer
    # writer naming a sink this process does not know. Carried out of parsing so the handler
    # can report them: nothing can deliver them, so the fan-out is INCOMPLETE.
    unknown_sink_keys: tuple[str, ...] = ()


class Handler(abc.ABC, typing.Generic[MessageT, IntentT]):
    """The contract for handling one kind of message.

    A handler owns its parsing and validation: `parse` turns the raw message into a typed
    intent, and `handle` delivers that intent to each sink the intent declares. The
    dispatcher routes to a handler by its `routing_key`.
    """

    def __init__(
        self,
        *,
        routing_key: str,
    ) -> None:
        """Store the routing key this handler owns.

        Args:
            routing_key: The key the dispatcher routes on to reach this handler.
        """
        self._routing_key = routing_key

    @property
    def routing_key(self) -> str:
        """The routing key this handler owns; keys the dispatcher's routing table."""
        return self._routing_key

    @abc.abstractmethod
    def parse(
        self,
        *,
        event: MessageT,
    ) -> ParseResult[IntentT]:
        """Validate the message and build this handler's typed intent.

        Pure: it returns issues rather than logging them, so the caller can log them with
        the message id. A None intent means there is nothing actionable and the message is
        marked terminal without calling `handle`.

        Args:
            event: The message to validate.

        Returns:
            A ParseResult carrying the typed intent (or None) and any validation issues.
        """

    @abc.abstractmethod
    def handle(
        self,
        *,
        event: MessageT,
        intent: IntentT,
        unknown_sink_keys: tuple[str, ...],
        recorder: OutcomeRecorder,
    ) -> HandleResult:
        """Deliver a parsed intent to every sink it declares and report the fan-out.

        Called only when `parse` produced a non-None intent. Each delivery's Outcome goes to
        the recorder as that sink returns, so the return value describes only how far the
        fan-out got. Must be safe to call more than once for the same message: after a crash
        between a sink side effect and the caller's own write, the message is handled again,
        and `recorder.is_done` is what keeps a delivery from being repeated.

        Args:
            event: The message being handled.
            intent: The typed intent produced by `parse`.
            unknown_sink_keys: What `parse` found declared but unimplementable, to report
                alongside any sink key this handler has no implementation wired for.
            recorder: Where each delivery's Outcome is recorded.

        Returns:
            The HandleResult for this message: whether every declared sink was reached.
        """
