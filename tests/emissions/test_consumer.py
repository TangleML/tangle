"""Tests for the emission consumer: single-row poll loop, mapping, and terminal writeback."""

import collections.abc
import contextlib
import dataclasses
import datetime
import logging
import threading
import time

import pytest
import sqlalchemy
from sqlalchemy import exc as sql_exc
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching import service as dispatching_service
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import consumer as emissions_consumer
from cloud_pipelines_backend.emissions import db_models, outcome_recorder
from cloud_pipelines_backend.emissions import messages as emission_messages

_READINESS = db_models.EmissionType.READINESS.value
_SINK = "tangleml.com/emission/readiness/sink/start-pipeline-run"


@contextlib.contextmanager
def _outcome_writes_failing(
    exc: type[Exception],
) -> collections.abc.Iterator[None]:
    """Fail every flush that carries an outcome row, with the given DB-API error class.

    The fault has to land where a real one does — inside the recorder's own commit, after the
    sink has returned — so it is injected at the session level rather than by stubbing the
    recorder. A test that replaced the recorder would never execute the clause under test.
    """

    def fail(session: orm.Session, flush_context: object, instances: object) -> None:
        if any(isinstance(obj, db_models.EmissionEventOutcome) for obj in session.new):
            raise exc("INSERT emission_event_outcome", {}, Exception("injected"))

    sqlalchemy.event.listen(orm.Session, "before_flush", fail)
    try:
        yield
    finally:
        sqlalchemy.event.remove(orm.Session, "before_flush", fail)


class _RecordingHandler(
    handler_base.Handler[emission_messages.EmissionEventMessage, object]
):
    """A scriptable handler that records the messages it parses.

    Lets a test assert what the consumer routed to it and control the result it returns
    (or raise from parse/handle to exercise the dispatcher's error isolation). `sink_outcomes`
    scripts a fan-out: one entry per sink key, delivered through the recorder the consumer
    built and skipped when that recorder reports the pair already done.
    """

    def __init__(
        self,
        *,
        routing_key: str,
        parse_result: handler_base.ParseResult[object] | None = None,
        handle_result: handler_base.HandleResult | None = None,
        sink_outcomes: dict[str, handler_base.Outcome] | None = None,
        raise_in_parse: bool = False,
        raise_in_handle: bool = False,
        incomplete_in_handle: bool = False,
    ) -> None:
        super().__init__(routing_key=routing_key)
        self._parse_result = parse_result or handler_base.ParseResult(
            intent=object(), issues=[]
        )
        self._handle_result = handle_result or handler_base.HandleResult(
            status=handler_base.HandleStatus.COMPLETE, detail={"ok": True}
        )
        self._sink_outcomes = sink_outcomes or {}
        self._raise_in_parse = raise_in_parse
        self._raise_in_handle = raise_in_handle
        self._incomplete_in_handle = incomplete_in_handle
        self.parsed_events: list[emission_messages.EmissionEventMessage] = []
        self.called: list[str] = []
        self.delivered: list[str] = []
        self.skipped: list[str] = []

    def parse(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
    ) -> handler_base.ParseResult[object]:
        self.parsed_events.append(event)
        if self._raise_in_parse:
            raise RuntimeError("boom in parse")
        return self._parse_result

    def handle(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
        intent: object,
        unknown_sink_keys: tuple[str, ...],
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        for sink_key, outcome in self._sink_outcomes.items():
            if recorder.is_done(sink_key=sink_key):
                self.skipped.append(sink_key)
                continue
            # `called` is the sink's side effect having happened; `delivered` is that side
            # effect having been recorded. They are the same list right up until the write
            # fails, which is the case worth being able to tell apart.
            self.called.append(sink_key)
            recorder.record(sink_key=sink_key, outcome=outcome)
            self.delivered.append(sink_key)
        if self._raise_in_handle:
            raise RuntimeError("boom in handle")
        if self._incomplete_in_handle:
            raise handler_base.DeliveryIncomplete("out of budget")
        return self._handle_result


def _insert_event(
    *,
    session_factory: orm.sessionmaker,
    emission_type: str = _READINESS,
    execution_node_id: str = "node-1",
    container_execution_id: str = "ce-1",
    container_execution_status: str = "SUCCEEDED",
    annotations: dict[str, str] | None = None,
    created_at: datetime.datetime | None = None,
    claimed_status: str | None = None,
    claimed_at: datetime.datetime | None = None,
    claimed_by: str | None = None,
) -> str:
    """Insert one emission_event (plus its annotations) and return its id.

    The dedupe unique index spans (execution_node_id, emission_type,
    container_execution_status), so callers inserting several rows of the same type and status
    must vary the node id.

    created_at and the claim columns are assigned after construction: the model fills all four
    itself, so none of them is a constructor argument.
    """
    with session_factory() as session:
        row = db_models.EmissionEvent(
            execution_node_id=execution_node_id,
            container_execution_id=container_execution_id,
            container_execution_status=container_execution_status,
            pipeline_run_id="run-1",
            emission_type=emission_type,
        )
        if created_at is not None:
            row.created_at = created_at
        if claimed_status is not None:
            row.claimed_status = claimed_status
        if claimed_at is not None:
            row.claimed_at = claimed_at
        if claimed_by is not None:
            row.claimed_by = claimed_by
        session.add(row)
        session.flush()
        event_id = row.id
        for key, value in (annotations or {}).items():
            session.add(
                db_models.EmissionEventAnnotation(
                    emission_event_id=event_id, key=key, value=value
                )
            )
        session.commit()
    return event_id


def _get_event(
    *,
    session_factory: orm.sessionmaker,
    event_id: str,
) -> db_models.EmissionEvent:
    """Fetch one emission_event row by id (in a fresh session)."""
    with session_factory() as session:
        row = session.get(db_models.EmissionEvent, event_id)
        if row is None:
            raise AssertionError(f"emission_event {event_id} not found")
        return row


def _insert_outcome(
    *,
    session_factory: orm.sessionmaker,
    event_id: str,
    sink_key: str,
    status: str = "success",
    detail: dict | None = None,
) -> None:
    """Seed one emission_event_outcome row, standing in for a delivery already made."""
    with session_factory() as session:
        session.add(
            db_models.EmissionEventOutcome(
                emission_event_id=event_id,
                annotation_key=sink_key,
                status=status,
                detail=detail,
            )
        )
        session.commit()


def _outcomes_for(
    *,
    session_factory: orm.sessionmaker,
    event_id: str,
) -> dict[str, db_models.EmissionEventOutcome]:
    """Fetch one event's outcome rows, keyed by the sink key each records."""
    with session_factory() as session:
        rows = session.scalars(
            sqlalchemy.select(db_models.EmissionEventOutcome).where(
                db_models.EmissionEventOutcome.emission_event_id == event_id
            )
        ).all()
        return {row.annotation_key: row for row in rows}


def _consumer_with(
    *,
    session_factory: orm.sessionmaker,
    handlers: list[
        handler_base.Handler[emission_messages.EmissionEventMessage, object]
    ],
    instance_id: str | None = None,
) -> emissions_consumer.ConsumerService:
    """Build a ConsumerService driving a real DispatcherService over the given handlers."""
    dispatcher = dispatching_service.DispatcherService(handlers=handlers)
    return emissions_consumer.ConsumerService(
        session_factory=session_factory,
        dispatcher=dispatcher,
        instance_id=instance_id,
    )


def test_processes_oldest_pending_row_and_writes_terminal(
    session_factory: orm.sessionmaker,
) -> None:
    event_id = _insert_event(
        session_factory=session_factory,
        annotations={
            "tangleml.com/emission/readiness/event": "orders-ready",
            "tangleml.com/emission/readiness/on-status": "SUCCEEDED",
        },
    )
    handler = _RecordingHandler(
        routing_key=_READINESS,
        handle_result=handler_base.HandleResult(
            status=handler_base.HandleStatus.COMPLETE, detail={"pushed": True}
        ),
        sink_outcomes={
            _SINK: handler_base.Outcome(
                status=handler_base.OutcomeStatus.SUCCESS,
                detail={"pushed": True},
            )
        },
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    # The row was routed to its handler with the joined annotations.
    assert len(handler.parsed_events) == 1
    routed = handler.parsed_events[0]
    assert routed.emission_event_id == event_id
    assert routed.annotations == {
        "tangleml.com/emission/readiness/event": "orders-ready",
        "tangleml.com/emission/readiness/on-status": "SUCCEEDED",
    }

    # The verdict was written back and the claim closed with it.
    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.handle_status == "complete"
    assert row.handle_detail == {"pushed": True}
    assert row.claimed_status == "settled"

    # The delivery itself is on its own row, keyed by the sink that made it.
    outcomes = _outcomes_for(session_factory=session_factory, event_id=event_id)
    assert set(outcomes) == {_SINK}
    assert outcomes[_SINK].status == "success"
    assert outcomes[_SINK].detail == {"pushed": True}


@pytest.mark.parametrize(
    ("status", "detail"),
    [
        (handler_base.HandleStatus.COMPLETE, {"sinks": 1}),
        (handler_base.HandleStatus.INCOMPLETE, {"unresolved": ["sink/x"]}),
        (handler_base.HandleStatus.FAILED, {"error": "boom"}),
    ],
)
def test_terminal_writeback_per_handle_status(
    session_factory: orm.sessionmaker,
    status: handler_base.HandleStatus,
    detail: dict,
) -> None:
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(
        routing_key=_READINESS,
        handle_result=handler_base.HandleResult(status=status, detail=detail),
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.handle_status == status.value
    assert row.handle_detail == detail
    # Every verdict closes the claim, including the ones that report a problem.
    assert row.claimed_status == "settled"

    # A settled row no longer matches the claim poll.
    assert consumer._process_one() is False


def test_parse_returning_no_intent_marks_nothing_to_do(
    session_factory: orm.sessionmaker,
) -> None:
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(
        routing_key=_READINESS,
        parse_result=handler_base.ParseResult(intent=None, issues=[]),
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.handle_status == "nothing_to_do"
    assert row.handle_detail == {"reason": "parse_returned_none", "issues": []}
    assert row.claimed_status == "settled"
    # Nothing was delivered, so the ledger stays empty.
    assert _outcomes_for(session_factory=session_factory, event_id=event_id) == {}


def test_terminal_rows_are_never_repolled(
    session_factory: orm.sessionmaker,
) -> None:
    _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True
    # Only the one pending row existed; the second poll finds nothing.
    assert consumer._process_one() is False
    assert len(handler.parsed_events) == 1


def test_handler_exception_is_terminal_failed_and_loop_survives(
    session_factory: orm.sessionmaker,
) -> None:
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS)
        },
        raise_in_handle=True,
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    # The exception is caught, reported as failed, and _process_one still reports a handled row.
    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.handle_status == "failed"
    assert "boom in handle" in row.handle_detail["error"]
    assert row.claimed_status == "settled"
    # The delivery made before the raise keeps its row: each one commits as its sink returns,
    # so the raise costs the verdict, not the record of what was already sent.
    outcomes = _outcomes_for(session_factory=session_factory, event_id=event_id)
    assert set(outcomes) == {_SINK}
    assert outcomes[_SINK].status == "success"
    # The failed row is terminal, not retried.
    assert consumer._process_one() is False


def test_unmappable_row_is_marked_failed_and_advances(
    session_factory: orm.sessionmaker,
) -> None:
    # A stored status value with no ContainerExecutionStatus member makes _map_row_to_event
    # raise ValueError. The row is poison: it can never map, so the consumer must settle it
    # failed and move on rather than re-poll it forever (head-of-line blocking every newer row).
    event_id = _insert_event(
        session_factory=session_factory,
        container_execution_status="BOGUS",
    )
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.handle_status == "failed"
    assert row.handle_detail["reason"] == "map_failed"
    assert row.claimed_status == "settled"
    # The handler was never reached — mapping failed before dispatch.
    assert handler.parsed_events == []
    # The poison row is terminal: the next poll advances past it.
    assert consumer._process_one() is False


def test_transient_error_recovers_and_next_poll_succeeds(
    session_factory: orm.sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_id = _insert_event(
        session_factory=session_factory,
        annotations={"tangleml.com/emission/readiness/event": "orders-ready"},
    )
    handler = _RecordingHandler(
        routing_key=_READINESS,
        handle_result=handler_base.HandleResult(
            status=handler_base.HandleStatus.COMPLETE, detail={"pushed": True}
        ),
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])
    # Drop the error backoff so the retry after the caught error is immediate.
    monkeypatch.setattr(emissions_consumer, "_ERROR_BACKOFF_LADDER", (0.0,))

    stop = threading.Event()
    calls: list[int] = []
    real_process_one = consumer._process_one

    def flaky_process_one() -> bool:
        calls.append(1)
        if len(calls) == 1:
            # A transient DB/infra blip on the first cycle.
            raise RuntimeError("transient db blip")
        # Second cycle runs the real poll, which handles the pending row; then we stop.
        result = real_process_one()
        stop.set()
        return result

    monkeypatch.setattr(consumer, "_process_one", flaky_process_one)

    # run() must catch the error, back off, and continue — the second poll then succeeds.
    consumer.run(stop=stop)

    assert len(calls) == 2
    # The recovered cycle routed the row to the handler and wrote a success outcome.
    assert len(handler.parsed_events) == 1
    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.handle_status == "complete"
    assert row.handle_detail == {"pushed": True}


class _ScriptedLoop(threading.Event):
    """Drives run() through a fixed sequence of cycles, recording every wait it makes.

    Each entry in the script is what `_process_one` does for that cycle: an exception to
    raise, or the bool to return. The event is set once the script runs out, so `run()`
    returns instead of polling forever, and waiting is instant so a 300s rung costs nothing.
    """

    def __init__(
        self,
        *,
        script: list[object],
    ) -> None:
        super().__init__()
        self._script = list(script)
        self.waits: list[float] = []

    def process_one(
        self,
    ) -> bool:
        step = self._script.pop(0)
        if not self._script:
            self.set()
        if isinstance(step, Exception):
            raise step
        return step

    def wait(
        self,
        timeout: float | None = None,
    ) -> bool:
        self.waits.append(timeout)
        return super().wait(0)


def _waits_for(
    *,
    session_factory: orm.sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
    script: list[object],
) -> list[float]:
    """Run the loop over a scripted sequence of cycles and return what it waited for."""
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[_RecordingHandler(routing_key=_READINESS)],
    )
    loop = _ScriptedLoop(script=script)
    monkeypatch.setattr(consumer, "_process_one", loop.process_one)

    consumer.run(stop=loop)

    return loop.waits


@pytest.mark.parametrize(
    ("consecutive_failures", "expected"),
    [(1, 5.0), (2, 30.0), (3, 60.0), (4, 300.0), (5, 300.0), (50, 300.0)],
)
def test_the_backoff_climbs_a_rung_per_consecutive_failure_and_then_holds(
    consecutive_failures: int,
    expected: float,
) -> None:
    """A blip clears on the first rung; an outage settles at the ceiling and stays there."""
    assert (
        emissions_consumer._error_backoff(
            consecutive_failures=consecutive_failures,
            has_completed_a_cycle=True,
        )
        == expected
    )


@pytest.mark.parametrize("consecutive_failures", [1, 2, 3, 4, 50])
def test_the_backoff_stays_on_the_first_rung_until_a_cycle_has_come_back_clean(
    consecutive_failures: int,
) -> None:
    """The cold start: the API server owns create_all, so the table is missing for a while.

    Escalating through that window would sleep the consumer for minutes exactly as the tables
    appear, which reads as a wedged consumer rather than a slow start.
    """
    assert (
        emissions_consumer._error_backoff(
            consecutive_failures=consecutive_failures,
            has_completed_a_cycle=False,
        )
        == 5.0
    )


def test_a_clean_cycle_puts_the_next_failure_back_on_the_first_rung(
    session_factory: orm.sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waits = _waits_for(
        session_factory=session_factory,
        monkeypatch=monkeypatch,
        script=[
            True,
            RuntimeError("blip"),
            RuntimeError("blip"),
            True,
            RuntimeError("blip"),
        ],
    )

    # A drained row waits for nothing, so every entry here is a backoff: two rungs climbed,
    # then the clean cycle in the middle drops the last one back to the bottom.
    assert waits == [5.0, 30.0, 5.0]


def test_an_empty_queue_counts_as_a_clean_cycle(
    session_factory: orm.sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding nothing to do proves the database answered, which is all the latch asks."""
    waits = _waits_for(
        session_factory=session_factory,
        monkeypatch=monkeypatch,
        script=[False, RuntimeError("blip"), RuntimeError("blip")],
    )

    # The idle wait first, then the two rungs. Reaching 30.0 is the point: without the empty
    # poll latching the loop as working, both failures would have held at 5.0.
    assert waits == [0.5, 5.0, 30.0]


def test_rows_are_processed_oldest_first(
    session_factory: orm.sessionmaker,
) -> None:
    base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    # Insert newest first to prove ordering is by created_at, not insertion order. Each row
    # needs a distinct node id to clear the (node, container_execution, type) dedupe index.
    _insert_event(
        session_factory=session_factory,
        execution_node_id="node-newest",
        annotations={"tangleml.com/emission/readiness/event": "newest"},
        created_at=base + datetime.timedelta(seconds=20),
    )
    _insert_event(
        session_factory=session_factory,
        execution_node_id="node-oldest",
        annotations={"tangleml.com/emission/readiness/event": "oldest"},
        created_at=base,
    )
    _insert_event(
        session_factory=session_factory,
        execution_node_id="node-middle",
        annotations={"tangleml.com/emission/readiness/event": "middle"},
        created_at=base + datetime.timedelta(seconds=10),
    )
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    while consumer._process_one():
        pass

    seen = [
        event.annotations["tangleml.com/emission/readiness/event"]
        for event in handler.parsed_events
    ]
    assert seen == ["oldest", "middle", "newest"]


def test_map_row_to_event_is_a_faithful_snapshot(
    session_factory: orm.sessionmaker,
) -> None:
    event_id = _insert_event(
        session_factory=session_factory,
        container_execution_status="FAILED",
        annotations={
            "tangleml.com/emission/readiness/event": "k",
            "tangleml.com/emission/readiness/on-status": "FAILED",
        },
    )
    row = _get_event(session_factory=session_factory, event_id=event_id)

    event = emissions_consumer._map_row_to_event(
        row=row,
        annotations={
            "tangleml.com/emission/readiness/event": "k",
            "tangleml.com/emission/readiness/on-status": "FAILED",
        },
    )

    assert event.emission_event_id == event_id
    assert event.emission_type == _READINESS
    assert event.execution_node_id == "node-1"
    assert event.container_execution_id == "ce-1"
    assert event.pipeline_run_id == "run-1"
    # Stored TEXT is rehydrated to the canonical enum.
    assert event.container_execution_status is bts.ContainerExecutionStatus.FAILED
    assert event.annotations == {
        "tangleml.com/emission/readiness/event": "k",
        "tangleml.com/emission/readiness/on-status": "FAILED",
    }


def _expired_claim_at() -> datetime.datetime:
    """A claim timestamp far enough back that the lease over it has run out."""
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=emissions_consumer.CLAIM_EXPIRES_AFTER_SECONDS + 60
    )


def test_claim_stamps_the_row_with_this_consumers_id(
    session_factory: orm.sessionmaker,
) -> None:
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[handler],
        instance_id="consumer-a",
    )

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    # The claim outlives the handling, so a settled row still names who drained it.
    assert row.claimed_by == "consumer-a"
    assert row.claimed_at is not None
    assert row.claimed_status == "settled"


def test_a_row_another_consumer_holds_is_left_alone(
    session_factory: orm.sessionmaker,
) -> None:
    """The overlap case: two consumers running at once must not both handle one row."""
    _insert_event(
        session_factory=session_factory,
        claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
        claimed_at=datetime.datetime.now(datetime.timezone.utc),
        claimed_by="consumer-b",
    )
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[handler],
        instance_id="consumer-a",
    )

    assert consumer._process_one() is False
    assert handler.parsed_events == []


def test_a_claim_past_its_lease_is_taken_over(
    session_factory: orm.sessionmaker,
) -> None:
    """Crash recovery: a claim nobody is renewing must not hold a row forever."""
    event_id = _insert_event(
        session_factory=session_factory,
        claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
        claimed_at=_expired_claim_at(),
        claimed_by="consumer-that-died",
    )
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[handler],
        instance_id="consumer-a",
    )

    assert consumer._process_one() is True

    assert len(handler.parsed_events) == 1
    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_by == "consumer-a"
    assert row.claimed_status == "settled"
    assert row.handle_status == "complete"


def test_a_settled_row_is_never_claimed(
    session_factory: orm.sessionmaker,
) -> None:
    """The claim column alone decides what the poll sees; handle_status is left out of it."""
    _insert_event(
        session_factory=session_factory,
        claimed_status=db_models.ClaimStatus.SETTLED.value,
    )
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is False
    assert handler.parsed_events == []


def test_a_held_row_does_not_block_the_rest_of_the_queue(
    session_factory: orm.sessionmaker,
) -> None:
    """A row someone else holds is skipped, not waited on, however old it is."""
    base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    _insert_event(
        session_factory=session_factory,
        execution_node_id="node-held",
        annotations={"tangleml.com/emission/readiness/event": "held"},
        created_at=base,
        claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
        claimed_at=datetime.datetime.now(datetime.timezone.utc),
        claimed_by="consumer-b",
    )
    newer_id = _insert_event(
        session_factory=session_factory,
        execution_node_id="node-newer",
        annotations={"tangleml.com/emission/readiness/event": "newer"},
        created_at=base + datetime.timedelta(seconds=10),
    )
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[handler],
        instance_id="consumer-a",
    )

    assert consumer._process_one() is True
    assert consumer._process_one() is False

    seen = [
        event.annotations["tangleml.com/emission/readiness/event"]
        for event in handler.parsed_events
    ]
    assert seen == ["newer"]
    newer = _get_event(session_factory=session_factory, event_id=newer_id)
    assert newer.claimed_status == "settled"


class _StealTheCandidateSession:
    """A session that hands the candidate to another consumer between the poll and the claim.

    In production the race is a window of microseconds: one consumer picks a candidate, and
    another's claim lands before its own UPDATE does. The consumer's first UPDATE on this
    session is that claim — the candidate has already been chosen by then — so running the
    competing claim just ahead of it puts the loser in exactly the state the conditional
    UPDATE exists to catch.
    """

    def __init__(
        self,
        *,
        session: orm.Session,
    ) -> None:
        self._session = session
        self._stolen = False

    def __enter__(self) -> "_StealTheCandidateSession":
        self._session.__enter__()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._session.__exit__(*exc_info)

    def __getattr__(self, name: str) -> object:
        # Everything the consumer does other than execute() runs on the real session, so the
        # candidate poll this class depends on is the genuine one.
        return getattr(self._session, name)

    def execute(self, statement: object, *args: object, **kwargs: object) -> object:
        if not self._stolen and isinstance(statement, sqlalchemy.Update):
            self._stolen = True
            self._session.execute(
                sqlalchemy.update(db_models.EmissionEvent).values(
                    claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
                    claimed_at=datetime.datetime.now(datetime.timezone.utc),
                    claimed_by="consumer-b",
                )
            )
            self._session.commit()
        return self._session.execute(statement, *args, **kwargs)


def test_losing_the_claim_race_yields_without_dispatching(
    session_factory: orm.sessionmaker,
) -> None:
    """The conditional UPDATE is the guard: the loser must hand the row over untouched."""
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[handler],
        instance_id="consumer-a",
    )
    consumer._session_factory = lambda: _StealTheCandidateSession(
        session=session_factory()
    )

    # True, not False: the queue may hold other rows, so the loop retries rather than idling.
    assert consumer._process_one() is True

    assert handler.parsed_events == []
    row = _get_event(session_factory=session_factory, event_id=event_id)
    # The winner's claim stands, and the loser wrote nothing.
    assert row.claimed_by == "consumer-b"
    assert row.claimed_status == "in_progress"
    assert row.handle_status is None


def test_the_claim_alone_bumps_updated_at(
    session_factory: orm.sessionmaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The claim is a Core UPDATE, not an ORM flush, and onupdate has to fire for it too."""
    event_id = _insert_event(session_factory=session_factory)
    at_insert = _get_event(
        session_factory=session_factory, event_id=event_id
    ).updated_at
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    # Fail immediately after the claim commits, which is the only way to observe a row
    # mid-cycle: claimed by this consumer, not yet settled.
    def blow_up(**_kwargs: object) -> dict[str, str]:
        raise RuntimeError("annotation load blew up")

    monkeypatch.setattr(consumer, "_annotations_for", blow_up)

    with pytest.raises(RuntimeError):
        consumer._process_one()

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_status == "in_progress"
    assert row.handle_status is None
    assert row.claimed_at is not None
    assert row.updated_at > at_insert
    assert row.updated_at >= row.claimed_at


def test_updated_at_records_when_the_claim_settled(
    session_factory: orm.sessionmaker,
) -> None:
    """No column stores how long a row took to handle, because two existing ones already do.

    _mark_terminal is the last write to the row, so updated_at is when the claim closed, and
    updated_at - claimed_at is the handling duration. The day something writes emission_event
    after the settle, that stops being true and this test is where it shows up.
    """
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_status == "settled"
    # The settle is a Core UPDATE, so onupdate has to fire for it: updated_at moved off the
    # value the claim left, which is what makes the difference a duration.
    assert row.updated_at > row.claimed_at


# Columns the message deliberately leaves behind, each for a stated reason. A column added to
# EmissionEvent must be mapped onto the message or added here, so the choice is never implicit.
_ROW_ONLY_COLUMNS = frozenset(
    {
        "id",  # surfaced under the message's own name, emission_event_id
        "claimed_status",  # queue state, owned by the consumer's claim
        "claimed_at",
        "claimed_by",
        "handle_status",  # consumer state, written after the fan-out has already run
        "handle_detail",
        "extra_data",
        "created_at",  # row bookkeeping the handler has no use for
        "updated_at",
    }
)


def test_every_row_column_reaches_a_handler_or_is_named_row_only() -> None:
    """A column added to the row must be surfaced on the message or declared row-only.

    The snapshot test above checks the fields it lists; this one checks the list itself.
    Without it a new column is simply never mapped, and nothing about that is visible until a
    handler turns out not to have data it should have had.
    """
    # Read off the table rather than through a module-level import, so the assertion cannot be
    # broken by a later branch dropping an import this is the only remaining user of.
    columns = {column.key for column in db_models.EmissionEvent.__table__.columns}
    fields = {
        field.name
        for field in dataclasses.fields(emission_messages.EmissionEventMessage)
    }

    unaccounted = columns - fields - _ROW_ONLY_COLUMNS
    assert unaccounted == set(), (
        f"emission_event columns {sorted(unaccounted)} reach no handler. Map them in "
        "_map_row_to_event, or add them to _ROW_ONLY_COLUMNS with why."
    )


def _unstorable_result() -> handler_base.HandleResult:
    """A HandleResult carrying a detail no JSON column can hold."""
    return handler_base.HandleResult(
        status=handler_base.HandleStatus.COMPLETE,
        detail={"at": datetime.datetime(2026, 1, 1)},
    )


class _UnsanitizedDispatcher:
    """A dispatcher that hands a result straight through without checking its detail.

    The real router replaces an unstorable detail before the consumer ever sees one, so this
    stands in for anything that could put one in front of the settle: a caller that is not the
    router, or a router whose own check was bypassed.
    """

    def __init__(
        self,
        *,
        result: handler_base.HandleResult,
    ) -> None:
        self._result = result

    def dispatch(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        return self._result


def test_dispatcher_replacement_reaches_the_row(
    session_factory: orm.sessionmaker,
) -> None:
    """The router's stand-in detail is what the row records when a handler returns a bad one."""
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(
        routing_key=_READINESS, handle_result=_unstorable_result()
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    # The verdict the handler reported survives; only the detail was given up.
    assert row.handle_status == "complete"
    assert row.handle_detail["reason"] == "detail_unstorable"
    assert "datetime" in row.handle_detail["error"]
    assert row.claimed_status == "settled"


def test_unstorable_detail_at_the_write_still_clears_the_queue(
    session_factory: orm.sessionmaker,
) -> None:
    """A detail the database cannot hold must not park the oldest row in front of the rest."""
    base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    blocker_id = _insert_event(
        session_factory=session_factory,
        execution_node_id="node-blocker",
        created_at=base,
    )
    newer_id = _insert_event(
        session_factory=session_factory,
        execution_node_id="node-newer",
        created_at=base + datetime.timedelta(seconds=10),
    )
    consumer = emissions_consumer.ConsumerService(
        session_factory=session_factory,
        dispatcher=_UnsanitizedDispatcher(result=_unstorable_result()),
    )

    assert consumer._process_one() is True
    assert consumer._process_one() is True

    # The oldest row reached its verdict on the retry, keeping the status and losing only the
    # detail.
    blocker = _get_event(session_factory=session_factory, event_id=blocker_id)
    assert blocker.handle_status == "complete"
    assert blocker.handle_detail == {"reason": "detail_unstorable"}
    # The retry settles the claim too. A row left in progress here would come back at the end
    # of its lease and be handled a second time, for a detail that can never be stored.
    assert blocker.claimed_status == "settled"
    # The newer row was reached, which is what a stalled queue would have prevented, and the
    # guard settled it the same way: the retry works on every row, not only the first.
    newer = _get_event(session_factory=session_factory, event_id=newer_id)
    assert newer.handle_status == "complete"
    assert newer.handle_detail == {"reason": "detail_unstorable"}
    assert newer.claimed_status == "settled"
    assert consumer._process_one() is False


class _StealTheClaimMidHandle:
    """A dispatcher that hands the event to another consumer while it is being handled.

    That is the state the settle's fence exists for: a consumer stalled past its own lease
    finishes handling an event that somebody else has since claimed and may still be delivering.
    """

    def __init__(
        self,
        *,
        session_factory: orm.sessionmaker,
        event_id: str,
    ) -> None:
        self._session_factory = session_factory
        self._event_id = event_id

    def dispatch(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        with self._session_factory() as session:
            session.execute(
                sqlalchemy.update(db_models.EmissionEvent)
                .where(db_models.EmissionEvent.id == self._event_id)
                .values(
                    claimed_at=datetime.datetime.now(datetime.timezone.utc),
                    claimed_by="consumer-b",
                )
            )
            session.commit()
        return handler_base.HandleResult(status=handler_base.HandleStatus.COMPLETE)


def test_a_settle_after_the_claim_moved_writes_nothing(
    session_factory: orm.sessionmaker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    event_id = _insert_event(session_factory=session_factory)
    consumer = emissions_consumer.ConsumerService(
        session_factory=session_factory,
        dispatcher=_StealTheClaimMidHandle(
            session_factory=session_factory, event_id=event_id
        ),
        instance_id="consumer-a",
    )

    with caplog.at_level(
        logging.ERROR, logger="cloud_pipelines_backend.emissions.consumer"
    ):
        assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    # The row is left exactly as the new holder has it: still in progress, still theirs, with no
    # verdict written. The consumer that holds it settles it.
    assert row.claimed_by == "consumer-b"
    assert row.claimed_status == "in_progress"
    assert row.handle_status is None
    assert row.handle_detail is None
    assert "was no longer claimed by consumer-a" in caplog.text


def test_the_unstorable_detail_retry_is_fenced_too(
    session_factory: orm.sessionmaker,
) -> None:
    """The retry writes to the row a second time, so it needs the same predicate as the first."""
    event_id = _insert_event(session_factory=session_factory)

    class _StealThenReturnAnUnstorableDetail(_StealTheClaimMidHandle):
        def dispatch(
            self,
            *,
            event: emission_messages.EmissionEventMessage,
            recorder: handler_base.OutcomeRecorder,
        ) -> handler_base.HandleResult:
            super().dispatch(event=event, recorder=recorder)
            return _unstorable_result()

    consumer = emissions_consumer.ConsumerService(
        session_factory=session_factory,
        dispatcher=_StealThenReturnAnUnstorableDetail(
            session_factory=session_factory, event_id=event_id
        ),
        instance_id="consumer-a",
    )

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    # Neither write landed: the first was rejected by the database, the second by the fence.
    assert row.claimed_by == "consumer-b"
    assert row.claimed_status == "in_progress"
    assert row.handle_status is None


def test_a_delivery_already_on_the_ledger_is_not_repeated(
    session_factory: orm.sessionmaker,
) -> None:
    """The reclaim path: a consumer taking over an event delivers only what is left."""
    event_id = _insert_event(
        session_factory=session_factory,
        claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
        claimed_at=_expired_claim_at(),
        claimed_by="consumer-that-died",
    )
    other_sink = "tangleml.com/emission/readiness/sink/second"
    _insert_outcome(
        session_factory=session_factory,
        event_id=event_id,
        sink_key=_SINK,
        detail={"pushed": "by the consumer that died"},
    )
    handler = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS),
            other_sink: handler_base.Outcome(
                status=handler_base.OutcomeStatus.SUCCESS,
                detail={"pushed": "now"},
            ),
        },
    )
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    assert consumer._process_one() is True

    assert handler.skipped == [_SINK]
    assert handler.delivered == [other_sink]
    outcomes = _outcomes_for(session_factory=session_factory, event_id=event_id)
    # The row from the first attempt is untouched, and the one that was missing is now there.
    assert outcomes[_SINK].detail == {"pushed": "by the consumer that died"}
    assert outcomes[other_sink].detail == {"pushed": "now"}


def test_a_collision_keeps_the_recorded_delivery_and_notes_the_second(
    session_factory: orm.sessionmaker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two consumers delivering one pair: the row already there wins, and the loser leaves a note.

    The recorder's done set is read at construction, so a pair written after that — by the
    consumer that overran its lease — is one this fan-out still tries to deliver. The composite
    primary key is what stops it from overwriting the record.
    """
    event_id = _insert_event(session_factory=session_factory)
    recorder = outcome_recorder.EmissionOutcomeRecorder(
        session_factory=session_factory,
        emission_event_id=event_id,
        emission_type=_READINESS,
        claimed_by="consumer-a",
    )
    # The winner lands after the done set was read, so is_done still reports False.
    _insert_outcome(
        session_factory=session_factory,
        event_id=event_id,
        sink_key=_SINK,
        status="success",
        detail={"pushed": "first"},
    )
    assert recorder.is_done(sink_key=_SINK) is False

    with caplog.at_level(
        logging.ERROR, logger="cloud_pipelines_backend.emissions.outcome_recorder"
    ):
        recorder.record(
            sink_key=_SINK,
            outcome=handler_base.Outcome(
                status=handler_base.OutcomeStatus.FAIL,
                detail={"pushed": "second"},
            ),
        )

    outcomes = _outcomes_for(session_factory=session_factory, event_id=event_id)
    winner = outcomes[_SINK]
    # The first delivery's verdict and detail stand.
    assert winner.status == "success"
    assert winner.detail == {"pushed": "first"}
    # The second is visible only as a note, which is the whole point of writing one.
    collisions = winner.extra_data["collisions"]
    assert len(collisions) == 1
    assert collisions[0]["claimed_by"] == "consumer-a"
    assert collisions[0]["status"] == "fail"
    assert collisions[0]["at"]
    assert "already exists" in caplog.text
    # The pair counts as done afterwards, whoever recorded it.
    assert recorder.is_done(sink_key=_SINK) is True


def test_a_recorder_failure_leaves_the_event_reclaimable_and_a_retry_completes_it(
    session_factory: orm.sessionmaker,
) -> None:
    """The regression: a sink that ran and a ledger that could not be written keeps the claim.

    Through the real door — a real DispatcherService, a real EmissionOutcomeRecorder and a real
    handler — because the fix is the *absence* of a catch between the recorder and `run()`. A
    test that called the recorder directly would never execute it.

    Two sinks, so the second one proves what the first one's failure costs: before this fix the
    event was settled `failed` with zero outcome rows, and the delivery to the second sink was
    never attempted again.
    """
    second_sink = "tangleml.com/emission/readiness/sink/second"
    event_id = _insert_event(session_factory=session_factory)
    first = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS),
            second_sink: handler_base.Outcome(
                status=handler_base.OutcomeStatus.SUCCESS
            ),
        },
    )
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[first],
        instance_id="consumer-a",
    )

    with _outcome_writes_failing(sql_exc.OperationalError):
        with pytest.raises(handler_base.RecorderUnavailable):
            consumer._process_one()

    # The sink ran, so something happened in the world; the ledger holds nothing, so a verdict
    # here would be a lie. The claim standing is what makes the retry possible.
    assert first.called == [_SINK]
    assert first.delivered == []
    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_status == db_models.ClaimStatus.IN_PROGRESS.value
    assert row.claimed_by == "consumer-a"
    assert row.handle_status is None
    assert _outcomes_for(session_factory=session_factory, event_id=event_id) == {}

    # The lease is the only thing that hands the event on: nothing released the claim.
    with session_factory() as session:
        session.execute(
            sqlalchemy.update(db_models.EmissionEvent)
            .where(db_models.EmissionEvent.id == event_id)
            .values(claimed_at=_expired_claim_at())
        )
        session.commit()

    second = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS),
            second_sink: handler_base.Outcome(
                status=handler_base.OutcomeStatus.SUCCESS
            ),
        },
    )
    retry = _consumer_with(
        session_factory=session_factory,
        handlers=[second],
        instance_id="consumer-b",
    )

    assert retry._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_status == db_models.ClaimStatus.SETTLED.value
    assert row.claimed_by == "consumer-b"
    assert row.handle_status == handler_base.HandleStatus.COMPLETE.value
    # Both deliveries are on the ledger. Set comparison, so sink iteration order cannot flake it.
    assert set(_outcomes_for(session_factory=session_factory, event_id=event_id)) == {
        _SINK,
        second_sink,
    }
    # Nothing was skipped on the retry: the first attempt recorded nothing, so both were
    # delivered again — at-least-once, which is why a real sink has to be idempotent.
    assert second.skipped == []
    assert set(second.delivered) == {_SINK, second_sink}


def test_a_transient_write_failure_raises_and_records_nothing(
    session_factory: orm.sessionmaker,
) -> None:
    """A database that could not take the write leaves the pair unrecorded, and says so.

    The delivery happened — the sink returned before this — so the caller must not settle the
    event. `RecorderUnavailable` is how the recorder says that, and `_done` must not name the
    pair afterwards, or the retry would skip the delivery it never recorded.
    """
    event_id = _insert_event(session_factory=session_factory)
    recorder = outcome_recorder.EmissionOutcomeRecorder(
        session_factory=session_factory,
        emission_event_id=event_id,
        emission_type=_READINESS,
        claimed_by="consumer-a",
    )

    with _outcome_writes_failing(sql_exc.OperationalError):
        with pytest.raises(handler_base.RecorderUnavailable):
            recorder.record(
                sink_key=_SINK,
                outcome=handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS),
            )

    assert recorder.is_done(sink_key=_SINK) is False
    assert _outcomes_for(session_factory=session_factory, event_id=event_id) == {}
    # And the pair is still deliverable: a later attempt records it normally.
    recorder.record(
        sink_key=_SINK,
        outcome=handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS),
    )
    assert recorder.is_done(sink_key=_SINK) is True


def test_a_write_the_database_would_always_reject_stays_terminal(
    session_factory: orm.sessionmaker,
) -> None:
    """A failure a retry cannot clear keeps today's behaviour: it is not RecorderUnavailable.

    Reclaiming an event whose write fails identically every time would re-run the side effect
    once per lease, for ever — nothing counts attempts. So only the failures above are
    retryable; this one bubbles as itself and the router settles the event `failed`.
    """
    event_id = _insert_event(session_factory=session_factory)
    recorder = outcome_recorder.EmissionOutcomeRecorder(
        session_factory=session_factory,
        emission_event_id=event_id,
        emission_type=_READINESS,
        claimed_by="consumer-a",
    )

    with _outcome_writes_failing(sql_exc.DataError):
        with pytest.raises(sql_exc.DataError):
            recorder.record(
                sink_key=_SINK,
                outcome=handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS),
            )

    assert recorder.is_done(sink_key=_SINK) is False
    assert _outcomes_for(session_factory=session_factory, event_id=event_id) == {}


def test_an_unresolved_sink_is_recorded_as_ignored(
    session_factory: orm.sessionmaker,
) -> None:
    """A declared sink nothing implements still gets a row, so the pair is accounted for."""
    event_id = _insert_event(session_factory=session_factory)
    recorder = outcome_recorder.EmissionOutcomeRecorder(
        session_factory=session_factory,
        emission_event_id=event_id,
        emission_type=_READINESS,
        claimed_by="consumer-a",
    )

    recorder.record_unresolved(sink_key=_SINK)

    outcomes = _outcomes_for(session_factory=session_factory, event_id=event_id)
    assert outcomes[_SINK].status == "ignore"
    assert outcomes[_SINK].detail == {"reason": "sink_unresolved"}
    assert recorder.is_done(sink_key=_SINK) is True


def test_run_stops_promptly_when_stop_is_set(
    session_factory: orm.sessionmaker,
) -> None:
    handler = _RecordingHandler(routing_key=_READINESS)
    consumer = _consumer_with(session_factory=session_factory, handlers=[handler])

    stop = threading.Event()
    thread = threading.Thread(target=consumer.run, kwargs={"stop": stop})
    thread.start()
    # Let the loop spin a couple of idle cycles, then request shutdown.
    time.sleep(0.05)
    stop.set()
    thread.join(timeout=2)

    assert not thread.is_alive()


def test_an_incomplete_delivery_leaves_the_row_claimed_and_keeps_polling(
    session_factory: orm.sessionmaker,
) -> None:
    """A handler out of budget is not an error, so the row is left for the lease to hand on.

    Two things have to be true at once and they pull in opposite directions: the row must not
    be settled, and the poll loop must not be slowed down. Raising would get the first and lose
    the second — it reaches `run()`'s backoff ladder, which punishes the whole queue for a
    handler behaving correctly. So this returns True: nothing settled, poll again at once.
    """
    event_id = _insert_event(session_factory=session_factory)
    handler = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS)
        },
        incomplete_in_handle=True,
    )
    consumer = _consumer_with(
        session_factory=session_factory,
        handlers=[handler],
        instance_id="consumer-a",
    )

    assert consumer._process_one() is True

    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_status == db_models.ClaimStatus.IN_PROGRESS.value
    assert row.claimed_by == "consumer-a"
    assert row.handle_status is None
    # What the handler did deliver before it stopped is on the ledger, which is what makes the
    # redelivery cheap.
    assert set(_outcomes_for(session_factory=session_factory, event_id=event_id)) == {
        _SINK
    }


def test_the_lease_hands_an_incomplete_delivery_to_the_next_consumer(
    session_factory: orm.sessionmaker,
) -> None:
    """The redelivery the raise asked for: the lease expires and a second pass finishes it."""
    event_id = _insert_event(session_factory=session_factory)
    first = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS)
        },
        incomplete_in_handle=True,
    )
    _consumer_with(
        session_factory=session_factory,
        handlers=[first],
        instance_id="consumer-a",
    )._process_one()
    with session_factory() as session:
        session.execute(
            sqlalchemy.update(db_models.EmissionEvent)
            .where(db_models.EmissionEvent.id == event_id)
            .values(claimed_at=_expired_claim_at())
        )
        session.commit()

    second = _RecordingHandler(
        routing_key=_READINESS,
        sink_outcomes={
            _SINK: handler_base.Outcome(status=handler_base.OutcomeStatus.SUCCESS)
        },
    )
    _consumer_with(
        session_factory=session_factory,
        handlers=[second],
        instance_id="consumer-b",
    )._process_one()

    # The pair already on the ledger is skipped, not delivered twice, and the row settles.
    assert second.skipped == [_SINK]
    assert second.called == []
    row = _get_event(session_factory=session_factory, event_id=event_id)
    assert row.claimed_status == db_models.ClaimStatus.SETTLED.value
    assert row.handle_status == handler_base.HandleStatus.COMPLETE.value
