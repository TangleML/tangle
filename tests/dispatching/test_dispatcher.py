"""Unit tests for the generic DispatcherService, using in-test fake handlers."""

import dataclasses
import datetime
import enum
import logging

import pytest

from cloud_pipelines_backend.dispatching import service as dispatching_service
from cloud_pipelines_backend.dispatching.handlers import base as handler_base


@dataclasses.dataclass(frozen=True, kw_only=True)
class _FakeMessage:
    """A minimal message satisfying the DispatchableMessage contract.

    The router only reads routing_key and message_id, so the test's message needs nothing
    more — keeping the dispatching tests free of any concrete domain type.
    """

    routing_key: str
    message_id: str


@dataclasses.dataclass
class _Intent:
    """A trivial parsed intent used to exercise the handle path."""

    value: str


class _FakeRecorder(handler_base.OutcomeRecorder):
    """An OutcomeRecorder holding its records in a dict instead of a database.

    Seeding `recorded` at construction is how a test plays a message being handled a second
    time: those sink keys report done, so the fan-out skips them.
    """

    def __init__(
        self,
        *,
        recorded: dict[str, handler_base.Outcome] | None = None,
    ) -> None:
        self.recorded: dict[str, handler_base.Outcome] = dict(recorded or {})
        self.unresolved: list[str] = []

    def is_done(
        self,
        *,
        sink_key: str,
    ) -> bool:
        return sink_key in self.recorded

    def record(
        self,
        *,
        sink_key: str,
        outcome: handler_base.Outcome,
    ) -> None:
        self.recorded[sink_key] = outcome

    def record_unresolved(
        self,
        *,
        sink_key: str,
    ) -> None:
        self.unresolved.append(sink_key)


class _FakeHandler(handler_base.Handler[_FakeMessage, _Intent]):
    """A handler whose parse and handle results are scripted per test.

    It records the order of parse/handle calls into a shared list so a test can assert
    the dispatcher ran them in sequence. `sink_outcomes` scripts the fan-out: one entry per
    sink key the intent is taken to declare, delivered in order and skipped when the recorder
    says that delivery is already done.
    """

    def __init__(
        self,
        *,
        routing_key: str,
        parse_result: handler_base.ParseResult[_Intent],
        handle_result: handler_base.HandleResult,
        calls: list[str],
        sink_outcomes: dict[str, handler_base.Outcome] | None = None,
        raise_in_parse: bool = False,
        raise_in_handle: bool = False,
        incomplete_in_handle: bool = False,
    ) -> None:
        super().__init__(routing_key=routing_key)
        self._parse_result = parse_result
        self._handle_result = handle_result
        self._calls = calls
        self._sink_outcomes = sink_outcomes or {}
        self._raise_in_parse = raise_in_parse
        self._raise_in_handle = raise_in_handle
        self._incomplete_in_handle = incomplete_in_handle
        # What the router passed in from the parse step, for a test to assert against.
        self.seen_unknown_sink_keys: tuple[str, ...] | None = None

    def parse(
        self,
        *,
        event: _FakeMessage,
    ) -> handler_base.ParseResult[_Intent]:
        self._calls.append(f"parse:{self.routing_key}")
        if self._raise_in_parse:
            raise RuntimeError("boom in parse")
        return self._parse_result

    def handle(
        self,
        *,
        event: _FakeMessage,
        intent: _Intent,
        unknown_sink_keys: tuple[str, ...],
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        self._calls.append(f"handle:{self.routing_key}:{intent.value}")
        self.seen_unknown_sink_keys = unknown_sink_keys
        for sink_key, outcome in self._sink_outcomes.items():
            if recorder.is_done(sink_key=sink_key):
                self._calls.append(f"skip:{sink_key}")
                continue
            recorder.record(sink_key=sink_key, outcome=outcome)
            self._calls.append(f"deliver:{sink_key}")
        # After the deliveries, so a test can prove what was already recorded survives.
        if self._raise_in_handle:
            raise RuntimeError("boom in handle")
        if self._incomplete_in_handle:
            raise handler_base.DeliveryIncomplete("out of budget")
        return self._handle_result


def _make_event(
    *,
    routing_key: str,
) -> _FakeMessage:
    """Build a minimal message routed to the given routing_key."""
    return _FakeMessage(routing_key=routing_key, message_id=f"msg-{routing_key}")


def _parsed(
    *,
    value: str = "ok",
    issues: list[handler_base.ParseIssue] | None = None,
    unknown_sink_keys: tuple[str, ...] = (),
) -> handler_base.ParseResult[_Intent]:
    """A ParseResult carrying a non-None intent (optionally with issues)."""
    return handler_base.ParseResult(
        intent=_Intent(value=value),
        issues=issues or [],
        unknown_sink_keys=unknown_sink_keys,
    )


def _complete() -> handler_base.HandleResult:
    """A complete HandleResult for a fake handler's handle step."""
    return handler_base.HandleResult(
        status=handler_base.HandleStatus.COMPLETE, detail={"ok": True}
    )


def _success() -> handler_base.Outcome:
    """A success Outcome for one delivery."""
    return handler_base.Outcome(
        status=handler_base.OutcomeStatus.SUCCESS, detail={"ok": True}
    )


def test_routes_to_handler_and_runs_parse_then_handle() -> None:
    calls: list[str] = []
    readiness = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(value="r"),
        handle_result=_complete(),
        calls=calls,
    )
    metadata = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=handler_base.HandleResult(
            status=handler_base.HandleStatus.COMPLETE
        ),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[readiness, metadata])

    result = dispatcher.dispatch(
        event=_make_event(routing_key="readiness"), recorder=_FakeRecorder()
    )

    assert result is readiness._handle_result
    # parse ran before handle, and only the readiness handler was touched.
    assert calls == ["parse:readiness", "handle:readiness:r"]


def test_routes_each_type_to_its_own_handler() -> None:
    calls: list[str] = []
    readiness = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(value="r"),
        handle_result=_complete(),
        calls=calls,
    )
    metadata = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=_complete(),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[readiness, metadata])

    dispatcher.dispatch(
        event=_make_event(routing_key="metadata"), recorder=_FakeRecorder()
    )

    assert calls == ["parse:metadata", "handle:metadata:m"]


def test_parse_returns_no_intent_short_circuits_to_nothing_to_do() -> None:
    calls: list[str] = []
    handler = _FakeHandler(
        routing_key="readiness",
        parse_result=handler_base.ParseResult(intent=None, issues=[]),
        handle_result=_complete(),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])
    recorder = _FakeRecorder()

    result = dispatcher.dispatch(
        event=_make_event(routing_key="readiness"), recorder=recorder
    )

    assert result.status is handler_base.HandleStatus.NOTHING_TO_DO
    assert result.detail == {"reason": "parse_returned_none", "issues": []}
    # handle must not run when there is no intent, so nothing is delivered.
    assert calls == ["parse:readiness"]
    assert recorder.recorded == {}


def test_parse_issues_are_logged_with_message_id_and_handle_still_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []
    issue = handler_base.ParseIssue(
        code="BAD_PAYLOAD", message="payload not an object", dropped=False
    )
    handler = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m", issues=[issue]),
        handle_result=_complete(),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    with caplog.at_level(
        logging.WARNING, logger="cloud_pipelines_backend.dispatching.service"
    ):
        result = dispatcher.dispatch(
            event=_make_event(routing_key="metadata"), recorder=_FakeRecorder()
        )

    # kept-but-degraded: the intent survived, so handle still runs and its result returns.
    assert result.status is handler_base.HandleStatus.COMPLETE
    assert calls == ["parse:metadata", "handle:metadata:m"]
    logged = caplog.text
    assert "msg-metadata" in logged
    assert "BAD_PAYLOAD" in logged
    assert "dropped=False" in logged
    assert "payload not an object" in logged


def test_unknown_routing_key_fails_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dispatcher = dispatching_service.DispatcherService(handlers=[])

    with caplog.at_level(
        logging.WARNING, logger="cloud_pipelines_backend.dispatching.service"
    ):
        result = dispatcher.dispatch(
            event=_make_event(routing_key="unknown"), recorder=_FakeRecorder()
        )

    # Nothing is registered to deliver this message, which is a wiring gap rather than a
    # message with nothing to do.
    assert result.status is handler_base.HandleStatus.FAILED
    assert result.detail == {"reason": "no_handler"}
    assert "No handler registered for routing_key=unknown" in caplog.text


def test_exception_in_parse_maps_to_failed_and_dispatcher_stays_usable() -> None:
    calls: list[str] = []
    raising = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=_complete(),
        calls=calls,
        raise_in_parse=True,
    )
    healthy = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=_complete(),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[raising, healthy])

    result = dispatcher.dispatch(
        event=_make_event(routing_key="readiness"), recorder=_FakeRecorder()
    )

    assert result.status is handler_base.HandleStatus.FAILED
    assert "boom in parse" in result.detail["error"]

    # A later dispatch to a healthy handler still succeeds — the failure did not wedge the router.
    second = dispatcher.dispatch(
        event=_make_event(routing_key="metadata"), recorder=_FakeRecorder()
    )
    assert second.status is handler_base.HandleStatus.COMPLETE
    assert calls == ["parse:readiness", "parse:metadata", "handle:metadata:m"]


def test_exception_in_handle_maps_to_failed_and_keeps_the_deliveries_made() -> None:
    calls: list[str] = []
    raising = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=_complete(),
        calls=calls,
        sink_outcomes={"sink/example-collector": _success()},
        raise_in_handle=True,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[raising])
    recorder = _FakeRecorder()

    result = dispatcher.dispatch(
        event=_make_event(routing_key="metadata"), recorder=recorder
    )

    assert result.status is handler_base.HandleStatus.FAILED
    assert "boom in handle" in result.detail["error"]
    assert calls == [
        "parse:metadata",
        "handle:metadata:m",
        "deliver:sink/example-collector",
    ]
    # The delivery that happened before the raise keeps its record; the router's verdict says
    # only that the fan-out did not report for itself.
    assert set(recorder.recorded) == {"sink/example-collector"}


class _UnavailableRecorder(_FakeRecorder):
    """A recorder whose write fails the way a database blip fails it.

    Records nothing and raises, which is what a real recorder does when the INSERT could not be
    committed for a reason a later attempt may not hit.
    """

    def record(
        self,
        *,
        sink_key: str,
        outcome: handler_base.Outcome,
    ) -> None:
        raise handler_base.RecorderUnavailable(f"could not record {sink_key}")


def test_recorder_unavailable_leaves_the_router_instead_of_failing_the_message() -> (
    None
):
    """A delivery that happened and could not be recorded is not the message's verdict.

    Everything else out of `handle` becomes `failed`, which closes the message. This one has to
    reach the caller, because only the caller can leave the message for another attempt — and a
    message closed here would be closed with a delivery missing from the ledger.
    """
    calls: list[str] = []
    handler = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=_complete(),
        calls=calls,
        sink_outcomes={"sink/example-collector": _success()},
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    with pytest.raises(handler_base.RecorderUnavailable):
        dispatcher.dispatch(
            event=_make_event(routing_key="metadata"),
            recorder=_UnavailableRecorder(),
        )

    # The sink ran before the record failed: that side effect is the reason the message must
    # not be settled, so the test pins that it happened.
    assert calls == ["parse:metadata", "handle:metadata:m"]


def test_recorder_unavailable_is_logged_before_it_is_re_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The escape is a drop the caller has to know about, so it does not pass silently."""
    calls: list[str] = []
    handler = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=_complete(),
        calls=calls,
        sink_outcomes={"sink/example-collector": _success()},
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    with caplog.at_level(logging.ERROR):
        with pytest.raises(handler_base.RecorderUnavailable):
            dispatcher.dispatch(
                event=_make_event(routing_key="metadata"),
                recorder=_UnavailableRecorder(),
            )

    assert "Recorder unavailable" in caplog.text
    assert "msg-metadata" in caplog.text


@pytest.mark.parametrize(
    "status",
    [
        handler_base.HandleStatus.COMPLETE,
        handler_base.HandleStatus.INCOMPLETE,
    ],
)
def test_handler_result_is_passed_through_verbatim(
    status: handler_base.HandleStatus,
) -> None:
    calls: list[str] = []
    result_in = handler_base.HandleResult(
        status=status,
        unresolved_sinks=("sink/nothing-wired",),
        detail={"marker": status.value},
    )
    handler = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=result_in,
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    result_out = dispatcher.dispatch(
        event=_make_event(routing_key="readiness"), recorder=_FakeRecorder()
    )

    assert result_out is result_in


def test_unknown_sink_keys_from_parse_reach_the_handler() -> None:
    calls: list[str] = []
    handler = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m", unknown_sink_keys=("sink/from-a-newer-build",)),
        handle_result=handler_base.HandleResult(
            status=handler_base.HandleStatus.INCOMPLETE,
            unresolved_sinks=("sink/from-a-newer-build",),
        ),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    result = dispatcher.dispatch(
        event=_make_event(routing_key="metadata"), recorder=_FakeRecorder()
    )

    # The router carries them from parse to handle; reporting them is the handler's job.
    assert handler.seen_unknown_sink_keys == ("sink/from-a-newer-build",)
    assert result.status is handler_base.HandleStatus.INCOMPLETE


def test_each_delivery_is_recorded_as_its_sink_returns() -> None:
    calls: list[str] = []
    first = _success()
    second = handler_base.Outcome(
        status=handler_base.OutcomeStatus.FAIL, detail={"error": "sink said no"}
    )
    handler = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=_complete(),
        calls=calls,
        sink_outcomes={"sink/first": first, "sink/second": second},
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])
    recorder = _FakeRecorder()

    result = dispatcher.dispatch(
        event=_make_event(routing_key="readiness"), recorder=recorder
    )

    assert recorder.recorded == {"sink/first": first, "sink/second": second}
    # One sink failing does not change the message's verdict: both were reached.
    assert result.status is handler_base.HandleStatus.COMPLETE


def test_a_delivery_already_recorded_is_not_repeated() -> None:
    calls: list[str] = []
    done = _success()
    fresh = _success()
    handler = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=_complete(),
        calls=calls,
        sink_outcomes={"sink/already-done": _success(), "sink/fresh": fresh},
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])
    recorder = _FakeRecorder(recorded={"sink/already-done": done})

    dispatcher.dispatch(event=_make_event(routing_key="readiness"), recorder=recorder)

    assert calls == [
        "parse:readiness",
        "handle:readiness:ok",
        "skip:sink/already-done",
        "deliver:sink/fresh",
    ]
    # The record already there is left as it was, not overwritten by a second delivery.
    assert recorder.recorded["sink/already-done"] is done
    assert recorder.recorded["sink/fresh"] is fresh


def test_duplicate_routing_key_raises_at_construction() -> None:
    calls: list[str] = []
    one = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=_complete(),
        calls=calls,
    )
    two = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=_complete(),
        calls=calls,
    )

    with pytest.raises(ValueError, match="duplicate handler for routing_key=readiness"):
        dispatching_service.DispatcherService(handlers=[one, two])


class _Colour(str, enum.Enum):
    """An enum a handler might reach for when filling in a detail."""

    RED = "red"


@pytest.mark.parametrize(
    ("case", "detail"),
    [
        ("datetime value", {"at": datetime.datetime(2026, 1, 1)}),
        ("object value", {"sink": object()}),
        ("non-JSON constant", {"ratio": float("inf")}),
        ("non-string key", {1: "keys are rewritten to strings"}),
        ("tuple value", {"pair": (1, 2)}),
    ],
)
def test_outcome_rejects_a_detail_the_caller_could_not_store(
    case: str,
    detail: dict,
) -> None:
    with pytest.raises(ValueError, match="not storable as JSON"):
        handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS, detail=detail)


@pytest.mark.parametrize(
    ("case", "detail"),
    [
        ("nested containers", {"items": [{"n": 1}, {"n": 2}], "ok": True}),
        # An enum whose members are str encodes as its value and reads back equal to it.
        ("str enum value", {"colour": _Colour.RED}),
        ("null value", {"error": None}),
    ],
)
def test_outcome_accepts_a_storable_detail(
    case: str,
    detail: dict,
) -> None:
    outcome = handler_base.Outcome(
        status=handler_base.OutcomeStatus.SUCCESS, detail=detail
    )

    assert outcome.detail is detail


def test_dispatch_replaces_an_unstorable_detail_from_a_handler(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []
    result_in = handler_base.HandleResult(
        status=handler_base.HandleStatus.COMPLETE,
        detail={"at": datetime.datetime(2026, 1, 1)},
    )
    handler = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=result_in,
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    with caplog.at_level(
        logging.ERROR, logger="cloud_pipelines_backend.dispatching.service"
    ):
        result_out = dispatcher.dispatch(
            event=_make_event(routing_key="readiness"), recorder=_FakeRecorder()
        )

    # The status the handler reported survives; only the unstorable detail is swapped.
    assert result_out.status is handler_base.HandleStatus.COMPLETE
    assert result_out.detail["reason"] == "detail_unstorable"
    assert "datetime" in result_out.detail["error"]
    handler_base.ensure_storable(detail=result_out.detail)
    assert "msg-readiness" in caplog.text


def test_unresolved_sinks_survive_the_detail_replacement() -> None:
    calls: list[str] = []
    handler = _FakeHandler(
        routing_key="readiness",
        parse_result=_parsed(),
        handle_result=handler_base.HandleResult(
            status=handler_base.HandleStatus.INCOMPLETE,
            unresolved_sinks=("sink/nothing-wired",),
            detail={"at": datetime.datetime(2026, 1, 1)},
        ),
        calls=calls,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    result = dispatcher.dispatch(
        event=_make_event(routing_key="readiness"), recorder=_FakeRecorder()
    )

    # The replacement rebuilds the result, so every field beside detail has to come through.
    assert result.status is handler_base.HandleStatus.INCOMPLETE
    assert result.unresolved_sinks == ("sink/nothing-wired",)


@pytest.mark.parametrize(
    "detail",
    [
        {"reason": "no_handler"},
        {"reason": "parse_returned_none", "issues": []},
        {"error": "RuntimeError('boom in handle')"},
    ],
)
def test_dispatcher_own_details_are_storable(detail: dict) -> None:
    """Every detail the router builds itself must satisfy the contract it enforces."""
    handler_base.ensure_storable(detail=detail)


def test_delivery_incomplete_leaves_the_router_instead_of_failing_the_message() -> None:
    """A handler that stopped part-way is asking for the message back, not reporting a failure.

    The same escape as `RecorderUnavailable` and for the mirror-image reason: that one is a
    delivery made and not recorded, this one is a delivery not yet made. Either way a `failed`
    verdict would close a message whose work is unfinished.
    """
    calls: list[str] = []
    handler = _FakeHandler(
        routing_key="metadata",
        parse_result=_parsed(value="m"),
        handle_result=_complete(),
        calls=calls,
        sink_outcomes={"sink/example-collector": _success()},
        incomplete_in_handle=True,
    )
    dispatcher = dispatching_service.DispatcherService(handlers=[handler])

    with pytest.raises(handler_base.DeliveryIncomplete):
        dispatcher.dispatch(
            event=_make_event(routing_key="metadata"),
            recorder=_FakeRecorder(),
        )

    # What the handler did deliver before it stopped keeps its record; the redelivery skips it.
    assert calls == [
        "parse:metadata",
        "handle:metadata:m",
        "deliver:sink/example-collector",
    ]
