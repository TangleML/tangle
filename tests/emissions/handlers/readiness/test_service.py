"""Tests for the readiness handler: what it does with a row, its sinks, and what it records.

The parsing itself is covered in test_readiness_annotations.py; here parse is only checked for
delegating to it.
"""

import datetime
import enum
import logging

import pytest
import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching import service as dispatching_service
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.dispatching.handlers.sinks import base as sinks_base
from cloud_pipelines_backend.emissions import consumer as emissions_consumer
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions import messages as emission_messages
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness_annotations,
)
from cloud_pipelines_backend.emissions.handlers.readiness import (
    service as readiness_service,
)
from cloud_pipelines_backend.emissions.handlers.readiness.sinks import (
    start_pipeline_run,
)
from cloud_pipelines_backend.triggers import db_models as trigger_db_models
from cloud_pipelines_backend.triggers import event_state as trigger_event_state
from cloud_pipelines_backend.triggers import service as trigger_service
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models

CES = bts.ContainerExecutionStatus
RA = readiness_annotations.ReadinessAnnotation
RSA = readiness_annotations.ReadinessSinkAnnotation
_READINESS = db_models.EmissionType.READINESS.value
_START_RUN = (RSA.START_PIPELINE_RUN,)


# Readiness implements one sink, so the multi-sink cases need a second key. Nothing validates
# the enum type at runtime — the intent is a frozen dataclass and the mapping a plain dict — so
# a test-local enum stands in for a build with two sinks. Adding an unimplemented member to
# ReadinessSinkAnnotation instead would be a key a node could declare and nothing could deliver.
class _FakeSinkAnnotation(str, enum.Enum):
    FIRST = f"{readiness_annotations.SINK_PREFIX}first"
    SECOND = f"{readiness_annotations.SINK_PREFIX}second"


class _FakeSink(sinks_base.Sink[readiness_annotations.ReadinessIntent]):
    """A stand-in sink whose result each test dictates.

    Records the intents it was handed so a test can assert the handler forwarded the parsed
    intent rather than re-reading the event.
    """

    def __init__(
        self,
        *,
        outcome: handler_base.Outcome | None = None,
        raises: bool = False,
        incomplete: bool = False,
    ) -> None:
        self._outcome = outcome or handler_base.Outcome(
            status=handler_base.OutcomeStatus.SUCCESS, detail={"ok": True}
        )
        self._raises = raises
        # Separate from `raises` because the handler treats them as opposites: an unexpected
        # raise is a failure the router reports, this one is a sink asking for the message back.
        self._incomplete = incomplete
        self.emitted: list[readiness_annotations.ReadinessIntent] = []
        # Recorded so a test can assert the handler passes the event's node id through,
        # not merely that it accepts the parameter.
        self.emitted_node_ids: list[str] = []

    def emit(
        self,
        *,
        intent: readiness_annotations.ReadinessIntent,
        execution_node_id: str,
        emission_event_id: str = "em-test",
    ) -> handler_base.Outcome:
        self.emitted.append(intent)
        self.emitted_node_ids.append(execution_node_id)
        if self._raises:
            raise RuntimeError("boom in sink")
        if self._incomplete:
            raise handler_base.DeliveryIncomplete("out of budget")
        return self._outcome


class _FakeRecorder(handler_base.OutcomeRecorder):
    """Collects what the handler recorded, with no database behind it.

    `done` seeds the deliveries an earlier claim on this event already made, which is what the
    handler skips.
    """

    def __init__(
        self,
        *,
        done: set[str] | None = None,
    ) -> None:
        self._done = done or set()
        self.recorded: list[tuple[str, handler_base.Outcome]] = []
        self.unresolved: list[str] = []

    def is_done(
        self,
        *,
        sink_key: str,
    ) -> bool:
        return sink_key in self._done

    def record(
        self,
        *,
        sink_key: str,
        outcome: handler_base.Outcome,
    ) -> None:
        self.recorded.append((sink_key, outcome))

    def record_unresolved(
        self,
        *,
        sink_key: str,
    ) -> None:
        self.unresolved.append(sink_key)


def _make_event(
    *,
    annotations: dict[str, str],
    emission_event_id: str = "em-1",
    execution_node_id: str = "node-1",
) -> emission_messages.EmissionEventMessage:
    """Build the message a consumer would hand the handler, carrying the given annotations."""
    return emission_messages.EmissionEventMessage(
        emission_event_id=emission_event_id,
        emission_type=_READINESS,
        execution_node_id=execution_node_id,
        container_execution_id="ce-1",
        pipeline_run_id="run-1",
        container_execution_status=CES.SUCCEEDED,
        annotations=annotations,
    )


def _subscribe_to_a_doomed_pipeline(
    *, session_factory: orm.sessionmaker, event_key: str
) -> str:
    """A subscription whose target pipeline is already a tombstone, and its id.

    Deleted after the subscription exists, which is the only way this state is reachable: the
    PATCH route validates the target live-and-owned, so nothing can point at a corpse on
    purpose.
    """
    condition = {"event": event_key}
    with session_factory() as session:
        pipeline = user_pipeline_db_models.UserPipeline(
            user_id="test-owner",
            file_path="pipelines/doomed.py",
        )
        session.add(pipeline)
        session.flush()
        subscription = trigger_db_models.TriggerSubscription(
            name="doomed",
            definition={"name": "doomed", "condition": condition},
            created_by="test-owner",
            pipeline_task_spec_from_user_pipeline_id=pipeline.id,
        )
        session.add(subscription)
        session.flush()
        trigger_event_state.sync(
            session=session,
            subscription_id=subscription.id,
            condition=condition,
        )
        pipeline.deleted_at = datetime.datetime.now(datetime.timezone.utc)
        session.commit()
        return subscription.id


def _insert_event(
    *,
    session_factory: orm.sessionmaker,
    annotations: dict[str, str],
    execution_node_id: str = "node-1",
) -> str:
    """Insert one pending readiness emission_event plus its annotations, returning its id."""
    with session_factory() as session:
        row = db_models.EmissionEvent(
            execution_node_id=execution_node_id,
            container_execution_id="ce-1",
            container_execution_status=CES.SUCCEEDED.value,
            pipeline_run_id="run-1",
            emission_type=_READINESS,
        )
        session.add(row)
        session.flush()
        event_id = row.id
        for key, value in annotations.items():
            session.add(
                db_models.EmissionEventAnnotation(
                    emission_event_id=event_id, key=key, value=value
                )
            )
        session.commit()
    return event_id


def _outcomes(
    *,
    session_factory: orm.sessionmaker,
    emission_event_id: str,
) -> dict[str, db_models.EmissionEventOutcome]:
    """Read an event's delivery rows, keyed by the annotation key that asked for each."""
    with session_factory() as session:
        rows = (
            session.execute(
                sql.select(db_models.EmissionEventOutcome).where(
                    db_models.EmissionEventOutcome.emission_event_id
                    == emission_event_id
                )
            )
            .scalars()
            .all()
        )
        return {row.annotation_key: row for row in rows}


def _readiness_consumer(
    *,
    session_factory: orm.sessionmaker,
) -> emissions_consumer.ConsumerService:
    """Build a ConsumerService driving the real readiness handler and its real sink."""
    dispatcher = dispatching_service.DispatcherService(
        handlers=[
            readiness_service.ReadinessHandler(
                sinks={
                    RSA.START_PIPELINE_RUN: start_pipeline_run.StartPipelineRunSink(
                        session_factory=session_factory
                    )
                },
            )
        ]
    )
    return emissions_consumer.ConsumerService(
        session_factory=session_factory, dispatcher=dispatcher
    )


class TestParseDelegation:
    """parse() adds nothing of its own; it hands the row's annotations to the read parser."""

    def test_rebuilds_the_intent_from_the_stored_annotations(
        self,
    ) -> None:
        handler = readiness_service.ReadinessHandler(
            sinks={RSA.START_PIPELINE_RUN: _FakeSink()}
        )

        result = handler.parse(
            event=_make_event(
                annotations={
                    RA.EVENT: "orders-ready",
                    RA.ON_STATUS: "FAILED",
                    RSA.START_PIPELINE_RUN: "true",
                },
            )
        )

        assert result.intent == readiness_annotations.ReadinessIntent(
            event_key="orders-ready",
            sinks=_START_RUN,
            on_status=CES.FAILED,
        )

    def test_returns_the_issues_the_parser_found(
        self,
    ) -> None:
        handler = readiness_service.ReadinessHandler(
            sinks={RSA.START_PIPELINE_RUN: _FakeSink()}
        )

        result = handler.parse(
            event=_make_event(
                annotations={RA.EVENT: "   ", RSA.START_PIPELINE_RUN: "true"}
            )
        )

        assert result.intent is None
        assert [issue.code for issue in result.issues] == [
            readiness_annotations.ReadinessParseCode.BLANK_EVENT_KEY
        ]


class TestFanOut:
    """One delivery per declared sink, each recorded as its sink returns."""

    def test_every_declared_sink_is_announced_to_and_recorded(
        self,
    ) -> None:
        first = _FakeSink(
            outcome=handler_base.Outcome(
                status=handler_base.OutcomeStatus.SUCCESS,
                detail={"sink": "first"},
            )
        )
        second = _FakeSink(
            outcome=handler_base.Outcome(
                status=handler_base.OutcomeStatus.IGNORE,
                detail={"sink": "second"},
            )
        )
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: first,
                _FakeSinkAnnotation.SECOND: second,
            }
        )
        intent = readiness_annotations.ReadinessIntent(
            event_key="orders-ready",
            sinks=(_FakeSinkAnnotation.FIRST, _FakeSinkAnnotation.SECOND),
        )
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(annotations={RA.EVENT: "orders-ready"}),
            intent=intent,
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert first.emitted == [intent]
        assert second.emitted == [intent]
        assert [key for key, _ in recorder.recorded] == [
            _FakeSinkAnnotation.FIRST.value,
            _FakeSinkAnnotation.SECOND.value,
        ]
        assert [outcome.detail for _, outcome in recorder.recorded] == [
            {"sink": "first"},
            {"sink": "second"},
        ]
        assert result == handler_base.HandleResult(
            status=handler_base.HandleStatus.COMPLETE
        )

    def test_a_failing_sink_is_recorded_and_the_rest_still_deliver(
        self,
    ) -> None:
        failing = _FakeSink(
            outcome=handler_base.Outcome(
                status=handler_base.OutcomeStatus.FAIL,
                detail={"error": "unavailable"},
            )
        )
        working = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: failing,
                _FakeSinkAnnotation.SECOND: working,
            }
        )
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(annotations={RA.EVENT: "orders-ready"}),
            intent=readiness_annotations.ReadinessIntent(
                event_key="orders-ready",
                sinks=(_FakeSinkAnnotation.FIRST, _FakeSinkAnnotation.SECOND),
            ),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        # The failure is that delivery's verdict, on that delivery's own record. The event's own
        # status says every declared sink was reached, which it was.
        assert dict((key, outcome.status) for key, outcome in recorder.recorded) == {
            _FakeSinkAnnotation.FIRST.value: handler_base.OutcomeStatus.FAIL,
            _FakeSinkAnnotation.SECOND.value: handler_base.OutcomeStatus.SUCCESS,
        }
        assert result.status is handler_base.HandleStatus.COMPLETE

    def test_a_sink_with_no_implementation_is_recorded_unresolved_and_skipped(
        self,
    ) -> None:
        working = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={_FakeSinkAnnotation.SECOND: working}
        )
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(annotations={RA.EVENT: "orders-ready"}),
            intent=readiness_annotations.ReadinessIntent(
                event_key="orders-ready",
                sinks=(_FakeSinkAnnotation.FIRST, _FakeSinkAnnotation.SECOND),
            ),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        # The wiring gap does not cost the sink that is wired.
        assert len(working.emitted) == 1
        assert recorder.unresolved == [_FakeSinkAnnotation.FIRST.value]
        assert [key for key, _ in recorder.recorded] == [
            _FakeSinkAnnotation.SECOND.value
        ]
        assert result == handler_base.HandleResult(
            status=handler_base.HandleStatus.INCOMPLETE,
            unresolved_sinks=(_FakeSinkAnnotation.FIRST.value,),
        )

    def test_a_sink_key_the_parser_could_not_resolve_makes_it_incomplete(
        self,
    ) -> None:
        sink = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={RSA.START_PIPELINE_RUN: sink}
        )
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(annotations={RA.EVENT: "orders-ready"}),
            intent=readiness_annotations.ReadinessIntent(
                event_key="orders-ready", sinks=_START_RUN
            ),
            unknown_sink_keys=(
                f"{readiness_annotations.SINK_PREFIX}from-a-newer-build",
            ),
            recorder=recorder,
        )

        # Nothing can deliver a key no member matches, so there is nothing to record for it —
        # the event carries it out instead.
        assert len(sink.emitted) == 1
        assert recorder.unresolved == []
        assert result == handler_base.HandleResult(
            status=handler_base.HandleStatus.INCOMPLETE,
            unresolved_sinks=(
                f"{readiness_annotations.SINK_PREFIX}from-a-newer-build",
            ),
        )

    def test_a_delivery_already_recorded_is_not_repeated(
        self,
    ) -> None:
        done = _FakeSink()
        pending = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: done,
                _FakeSinkAnnotation.SECOND: pending,
            }
        )
        recorder = _FakeRecorder(done={_FakeSinkAnnotation.FIRST.value})

        result = handler.handle(
            event=_make_event(annotations={RA.EVENT: "orders-ready"}),
            intent=readiness_annotations.ReadinessIntent(
                event_key="orders-ready",
                sinks=(_FakeSinkAnnotation.FIRST, _FakeSinkAnnotation.SECOND),
            ),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert done.emitted == []
        assert len(pending.emitted) == 1
        assert [key for key, _ in recorder.recorded] == [
            _FakeSinkAnnotation.SECOND.value
        ]
        # Every declared sink has a delivery on the ledger, whichever claim made it.
        assert result.status is handler_base.HandleStatus.COMPLETE

    def test_a_raising_sink_leaves_the_loop_with_earlier_deliveries_recorded(
        self,
    ) -> None:
        working = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: working,
                _FakeSinkAnnotation.SECOND: _FakeSink(raises=True),
            }
        )
        recorder = _FakeRecorder()

        with pytest.raises(RuntimeError, match="boom in sink"):
            handler.handle(
                event=_make_event(annotations={RA.EVENT: "orders-ready"}),
                intent=readiness_annotations.ReadinessIntent(
                    event_key="orders-ready",
                    sinks=(
                        _FakeSinkAnnotation.FIRST,
                        _FakeSinkAnnotation.SECOND,
                    ),
                ),
                unknown_sink_keys=(),
                recorder=recorder,
            )

        # A sink reports an expected failure as a fail Outcome, so a raise is unexpected and the
        # router turns it into a failed event. What was already delivered keeps its record.
        assert [key for key, _ in recorder.recorded] == [
            _FakeSinkAnnotation.FIRST.value
        ]


class TestASinkThatStoppedPartWayDoesNotStopItsPeers:
    """`DeliveryIncomplete` is caught per sink and re-raised after the loop.

    Caught outside the loop instead, one sink's pause would skip every sink after it in the
    list — and cost each of them a whole claim lease for work they were ready to do now.
    """

    def test_the_sinks_after_it_still_deliver(self) -> None:
        # The paused sink is first on purpose: the peer behind it is the one that would be
        # dropped by a catch outside the loop.
        peer = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: _FakeSink(incomplete=True),
                _FakeSinkAnnotation.SECOND: peer,
            }
        )
        recorder = _FakeRecorder()

        with pytest.raises(handler_base.DeliveryIncomplete):
            handler.handle(
                event=_make_event(annotations={RA.EVENT: "orders-ready"}),
                intent=readiness_annotations.ReadinessIntent(
                    event_key="orders-ready",
                    sinks=(
                        _FakeSinkAnnotation.FIRST,
                        _FakeSinkAnnotation.SECOND,
                    ),
                ),
                unknown_sink_keys=(),
                recorder=recorder,
            )

        assert len(peer.emitted) == 1
        # Only the peer: an unfinished delivery has no verdict to write, and the absent row is
        # what brings that sink back on the redelivery.
        assert [key for key, _ in recorder.recorded] == [
            _FakeSinkAnnotation.SECOND.value
        ]

    def test_the_event_is_still_left_unsettled(self) -> None:
        # The re-raise after the loop. Swallowing it would return a COMPLETE result, the
        # consumer would settle the row, and the paused sink would never be asked again.
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: _FakeSink(),
                _FakeSinkAnnotation.SECOND: _FakeSink(incomplete=True),
            }
        )

        with pytest.raises(handler_base.DeliveryIncomplete, match="out of budget"):
            handler.handle(
                event=_make_event(annotations={RA.EVENT: "orders-ready"}),
                intent=readiness_annotations.ReadinessIntent(
                    event_key="orders-ready",
                    sinks=(
                        _FakeSinkAnnotation.FIRST,
                        _FakeSinkAnnotation.SECOND,
                    ),
                ),
                unknown_sink_keys=(),
                recorder=_FakeRecorder(),
            )

    def test_a_peer_already_recorded_is_still_skipped(self) -> None:
        # The redelivery this asks for: the peer that succeeded last pass is on the ledger, so
        # it is skipped, and the budget goes to the sink that paused.
        peer = _FakeSink()
        handler = readiness_service.ReadinessHandler(
            sinks={
                _FakeSinkAnnotation.FIRST: _FakeSink(incomplete=True),
                _FakeSinkAnnotation.SECOND: peer,
            }
        )

        with pytest.raises(handler_base.DeliveryIncomplete):
            handler.handle(
                event=_make_event(annotations={RA.EVENT: "orders-ready"}),
                intent=readiness_annotations.ReadinessIntent(
                    event_key="orders-ready",
                    sinks=(
                        _FakeSinkAnnotation.FIRST,
                        _FakeSinkAnnotation.SECOND,
                    ),
                ),
                unknown_sink_keys=(),
                recorder=_FakeRecorder(done={_FakeSinkAnnotation.SECOND.value}),
            )

        assert peer.emitted == []


class TestThroughDispatcher:
    """The handler as the dispatcher drives it."""

    def test_routes_on_readiness_key(
        self,
    ) -> None:
        sink = _FakeSink()
        dispatcher = dispatching_service.DispatcherService(
            handlers=[
                readiness_service.ReadinessHandler(sinks={RSA.START_PIPELINE_RUN: sink})
            ]
        )
        recorder = _FakeRecorder()

        result = dispatcher.dispatch(
            event=_make_event(
                annotations={
                    RA.EVENT: "orders-ready",
                    RSA.START_PIPELINE_RUN: "true",
                }
            ),
            recorder=recorder,
        )

        assert result.status is handler_base.HandleStatus.COMPLETE
        assert [intent.event_key for intent in sink.emitted] == ["orders-ready"]
        assert [key for key, _ in recorder.recorded] == [RSA.START_PIPELINE_RUN.value]

    def test_no_intent_short_circuits_without_touching_a_sink(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        sink = _FakeSink()
        dispatcher = dispatching_service.DispatcherService(
            handlers=[
                readiness_service.ReadinessHandler(sinks={RSA.START_PIPELINE_RUN: sink})
            ]
        )
        recorder = _FakeRecorder()

        with caplog.at_level(
            logging.WARNING, logger="cloud_pipelines_backend.dispatching.service"
        ):
            result = dispatcher.dispatch(
                event=_make_event(
                    annotations={
                        RA.EVENT: "   ",
                        RSA.START_PIPELINE_RUN: "true",
                    }
                ),
                recorder=recorder,
            )

        assert result.status is handler_base.HandleStatus.NOTHING_TO_DO
        assert result.detail == {
            "reason": "parse_returned_none",
            "issues": [readiness_annotations.ReadinessParseCode.BLANK_EVENT_KEY],
        }
        assert sink.emitted == []
        assert recorder.recorded == []
        # The dispatcher is where the parse issues finally get an id attached.
        assert "emission_event_id=em-1" in caplog.text
        assert "BLANK_EVENT_KEY" in caplog.text

    def test_a_raising_sink_is_reported_as_a_failed_event(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        dispatcher = dispatching_service.DispatcherService(
            handlers=[
                readiness_service.ReadinessHandler(
                    sinks={RSA.START_PIPELINE_RUN: _FakeSink(raises=True)}
                )
            ]
        )

        with caplog.at_level(
            logging.ERROR, logger="cloud_pipelines_backend.dispatching.service"
        ):
            result = dispatcher.dispatch(
                event=_make_event(
                    annotations={
                        RA.EVENT: "orders-ready",
                        RSA.START_PIPELINE_RUN: "true",
                    }
                ),
                recorder=_FakeRecorder(),
            )

        assert result.status is handler_base.HandleStatus.FAILED
        assert result.detail == {"error": "RuntimeError('boom in sink')"}
        assert "boom in sink" in caplog.text


class TestEndToEnd:
    """A real row through the real consumer, dispatcher, handler, sink, and ledger."""

    def test_pending_row_is_delivered_and_settled(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={
                RA.EVENT: "orders-ready",
                RA.ON_STATUS: "SUCCEEDED",
                RSA.START_PIPELINE_RUN: "true",
            },
        )

        assert (
            _readiness_consumer(session_factory=session_factory)._process_one() is True
        )

        outcomes = _outcomes(
            session_factory=session_factory, emission_event_id=event_id
        )
        # No trigger subscription waits on this event key, so the sink found nothing to
        # record and ignore is the honest verdict — not a failure, just an unwatched event.
        assert outcomes[RSA.START_PIPELINE_RUN.value].status == (
            handler_base.OutcomeStatus.IGNORE.value
        )
        assert outcomes[RSA.START_PIPELINE_RUN.value].detail == {
            "sink": "start_pipeline_run",
            "event_key": "orders-ready",
            "reason": "no_subscription",
        }
        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            # The event's own verdict is about the fan-out: every declared sink was reached.
            assert row.handle_status == handler_base.HandleStatus.COMPLETE.value
            assert row.handle_detail is None
            assert row.claimed_status == db_models.ClaimStatus.SETTLED.value

    def test_a_dead_target_settles_the_event_with_the_error_on_the_ledger(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """The verdict is the only thing that outlives the delivery.

        A recorded FAIL settles the emission and there is no redelivery behind it, so whatever
        lands in `emission_event_outcome` here is all anyone will ever have to explain why the
        run did not start. It survives the trigger's savepoint rollback because the recorder
        writes on its own session.
        """
        subscription_id = _subscribe_to_a_doomed_pipeline(
            session_factory=session_factory, event_key="orders-ready"
        )
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={
                RA.EVENT: "orders-ready",
                RSA.START_PIPELINE_RUN: "true",
            },
        )

        assert (
            _readiness_consumer(session_factory=session_factory)._process_one() is True
        )

        outcome = _outcomes(
            session_factory=session_factory, emission_event_id=event_id
        )[RSA.START_PIPELINE_RUN.value]
        assert outcome.status == handler_base.OutcomeStatus.FAIL.value
        assert outcome.detail["reason"] == "runs_not_started"
        (failure,) = outcome.detail["failed"]
        assert failure["subscription_id"] == subscription_id
        assert failure["reason"] == trigger_service.TriggerReason.USER_PIPELINE_DELETED
        assert failure["error"]
        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            # Settled, not left for another pass: nothing a retry could do would help.
            assert row.claimed_status == db_models.ClaimStatus.SETTLED.value

    def test_reclaimed_row_skips_the_delivery_already_recorded(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={
                RA.EVENT: "orders-ready",
                RSA.START_PIPELINE_RUN: "true",
            },
        )
        consumer = _readiness_consumer(session_factory=session_factory)
        consumer._process_one()

        # Mimic a crash between the delivery and the settle: the row keeps the dead consumer's
        # claim, with claimed_at pushed past the lease so the next poll reclaims it.
        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            row.claimed_status = db_models.ClaimStatus.IN_PROGRESS.value
            row.claimed_at = datetime.datetime.now(
                datetime.timezone.utc
            ) - datetime.timedelta(
                seconds=emissions_consumer.CLAIM_EXPIRES_AFTER_SECONDS + 1
            )
            row.handle_status = None
            row.handle_detail = None
            session.commit()

        assert consumer._process_one() is True

        # The delivery on the ledger is the one from the first pass, and there is still only
        # one: the reclaim announced nothing and settled the event on what it found.
        outcomes = _outcomes(
            session_factory=session_factory, emission_event_id=event_id
        )
        assert list(outcomes) == [RSA.START_PIPELINE_RUN.value]
        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            assert row.handle_status == handler_base.HandleStatus.COMPLETE.value

    def test_row_declaring_no_sink_delivers_to_the_default_sink(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # A stored row that names no sink is not stuck: the parser resolves the default, so the
        # consumer delivers it like any other and settles the event complete.
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={RA.EVENT: "orders-ready"},
        )

        assert (
            _readiness_consumer(session_factory=session_factory)._process_one() is True
        )

        outcomes = _outcomes(
            session_factory=session_factory, emission_event_id=event_id
        )
        assert list(outcomes) == [RSA.START_PIPELINE_RUN.value]
        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            assert row.handle_status == handler_base.HandleStatus.COMPLETE.value

    def test_row_naming_an_unknown_sink_beside_a_known_one_settles_incomplete(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        unknown_key = f"{readiness_annotations.SINK_PREFIX}from-a-newer-build"
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={
                RA.EVENT: "orders-ready",
                RSA.START_PIPELINE_RUN: "true",
                unknown_key: "true",
            },
        )

        assert (
            _readiness_consumer(session_factory=session_factory)._process_one() is True
        )

        # A stored key this build has no member for is a delivery nothing attempted, and the
        # sink that could be delivered to still was.
        outcomes = _outcomes(
            session_factory=session_factory, emission_event_id=event_id
        )
        assert list(outcomes) == [RSA.START_PIPELINE_RUN.value]
        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            assert row.handle_status == handler_base.HandleStatus.INCOMPLETE.value

    def test_dropping_issue_recorded_on_row(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={RA.EVENT: "   ", RSA.START_PIPELINE_RUN: "true"},
        )

        assert (
            _readiness_consumer(session_factory=session_factory)._process_one() is True
        )

        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            assert row.handle_status == handler_base.HandleStatus.NOTHING_TO_DO.value
            assert row.handle_detail == {
                "reason": "parse_returned_none",
                "issues": ["blank_event_key"],
            }

    def test_row_without_event_key_is_nothing_to_do(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        event_id = _insert_event(
            session_factory=session_factory,
            annotations={
                RA.ON_STATUS: "SUCCEEDED",
                RSA.START_PIPELINE_RUN: "true",
            },
        )

        assert (
            _readiness_consumer(session_factory=session_factory)._process_one() is True
        )

        with session_factory() as session:
            row = session.get(db_models.EmissionEvent, event_id)
            assert row.handle_status == handler_base.HandleStatus.NOTHING_TO_DO.value
            # The row carries readiness keys, so the missing event key is a finding and the
            # detail names it: nothing was delivered and this is the only trace of why.
            assert row.handle_detail == {
                "reason": "parse_returned_none",
                "issues": ["no_event_key"],
            }
