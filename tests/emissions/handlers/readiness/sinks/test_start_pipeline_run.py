"""Tests for the readiness sink: the emission path's one write into the trigger tables.

Real SQL against SQLite, not a mocked service. What is being asserted is which rows an arrival
leaves behind, and a mock cannot be wrong about that.
"""

import datetime
import itertools
import json
import logging
import typing
from typing import Any

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness_annotations,
)
from cloud_pipelines_backend.emissions.handlers.readiness.sinks import (
    start_pipeline_run,
)
from cloud_pipelines_backend.triggers import db_models, event_state
from cloud_pipelines_backend.triggers import service as trigger_service
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services

db_models.register_db_tables()

_SINK_LOGGER = (
    "cloud_pipelines_backend.emissions.handlers.readiness.sinks.start_pipeline_run"
)


@pytest.fixture()
def session_factory() -> orm.sessionmaker:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    # autoflush off, matching emissions/consumer_main.py. It matters: with autoflush on, a
    # pending event-state write is flushed by the next SELECT for free, and code that forgets to
    # flush passes here while triggering one delivery late in production.
    return orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)


def _intent(*, event_key: str) -> readiness_annotations.ReadinessIntent:
    return readiness_annotations.ReadinessIntent(
        event_key=event_key,
        sinks=(readiness_annotations.ReadinessSinkAnnotation.START_PIPELINE_RUN,),
    )


# (created_by, name) is a unique key and this helper stamps a single created_by, so the name
# is what has to vary. The fan-out tests subscribe several times to put several subscriptions
# behind one event; they care about ids and ordering, never about what the rows are called.
_SUBSCRIPTION_NAMES = itertools.count(1)

# `pipeline` is unique on (user_id, file_path), so every pipeline this helper mints needs its
# own path; a shared one would collide on the second subscription.
_PIPELINE_PATHS = itertools.count(1)


# A task spec a run can actually be built from. It has to be valid, not merely present: a
# trigger builds a real pipeline run out of whatever the target's current version holds.
_RUNNABLE_TASK: dict[str, Any] = {
    "componentRef": {
        "spec": {
            "name": "triggered-target",
            "implementation": {"graph": {"tasks": {}}},
        }
    }
}


def _pipeline_id(session: orm.Session) -> str:
    """A real, runnable pipeline for a subscription to target, and its id.

    Real rather than an invented string: the target column is NOT NULL and a foreign key into
    `pipeline`, and a plausible-looking id would only pass because this engine leaves SQLite's
    foreign keys switched off.

    Runnable because a trigger now starts a run from this pipeline's current version, so a row
    with no version fails the trigger rather than being ignored.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id="test-owner",
        file_path=f"pipelines/auto-{next(_PIPELINE_PATHS)}.py",
    )
    session.add(pipeline)
    session.flush()
    session.add(
        user_pipeline_db_models.UserPipelineVersion(
            pipeline_id=pipeline.id,
            version_key=user_pipeline_db_models.CURRENT_VERSION_KEY,
            content_digest="d" * user_pipeline_db_models.DIGEST_LENGTH,
            root_pipeline_task=_RUNNABLE_TASK,
        )
    )
    session.flush()
    # Pointed at the version only after it exists: `current_version_key` is half of a composite
    # foreign key, so setting it on the insert fails where SQLite's foreign keys are on.
    pipeline.current_version_key = user_pipeline_db_models.CURRENT_VERSION_KEY
    session.flush()
    return pipeline.id


def _subscribe(
    *,
    session_factory: orm.sessionmaker,
    condition: dict[str, Any],
    enabled: bool = True,
    name: str | None = None,
) -> str:
    """Subscribe to a condition and return the new subscription's id.

    `name` defaults to a fresh one per call, so asking for two subscriptions gives two rows
    rather than a collision. Pass it explicitly when the test is about the name itself.
    """
    name = f"nightly-retrain-{next(_SUBSCRIPTION_NAMES)}" if name is None else name
    with session_factory() as session:
        subscription = db_models.TriggerSubscription(
            name=name,
            # The column and the blob agree, the way the service keeps them.
            definition={"name": name, "condition": condition},
            created_by="test-owner",
            enabled=enabled,
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
        )
        session.add(subscription)
        session.flush()
        event_state.sync(
            session=session,
            subscription_id=subscription.id,
            condition=condition,
        )
        session.commit()
        return subscription.id


def _states(
    *, session_factory: orm.sessionmaker, subscription_id: str
) -> dict[str, db_models.TriggerEventState]:
    with session_factory() as session:
        rows = session.scalars(
            sqlalchemy.select(db_models.TriggerEventState).where(
                db_models.TriggerEventState.subscription_id == subscription_id
            )
        ).all()
        # Detached copies would expire on close, so read what the assertions need now.
        return {row.event_name: row for row in rows}


def _set_enabled(
    *,
    session_factory: orm.sessionmaker,
    subscription_id: str,
    enabled: bool,
) -> trigger_service.TriggerResult:
    """Flip `enabled` the way a PATCH does, so the off->on re-evaluation runs with it."""
    with session_factory() as session:
        subscription = session.get(db_models.TriggerSubscription, subscription_id)
        assert subscription is not None
        result = trigger_service.update_subscription(
            session=session,
            subscription=subscription,
            enabled=enabled,
            caller=trigger_service.Caller(name="test-owner"),
        )
        session.commit()
        return result


def _history(*, session_factory: orm.sessionmaker) -> list[db_models.TriggerHistory]:
    with session_factory() as session:
        return list(session.scalars(sqlalchemy.select(db_models.TriggerHistory)).all())


def _set_condition(
    *,
    session_factory: orm.sessionmaker,
    subscription_id: str,
    condition: dict[str, Any],
) -> trigger_service.TriggerResult:
    """Edit the condition the way a PATCH does, so the edit is re-evaluated with it."""
    with session_factory() as session:
        subscription = session.get(db_models.TriggerSubscription, subscription_id)
        assert subscription is not None
        result = trigger_service.update_subscription(
            session=session,
            subscription=subscription,
            condition=condition,
            caller=trigger_service.Caller(name="test-owner"),
        )
        session.commit()
        return result


def _rewind(
    *,
    session_factory: orm.sessionmaker,
    subscription_id: str,
    event_name: str,
    by: datetime.timedelta,
) -> None:
    """Age an arrival without sleeping: the same row, just further into the past."""
    with session_factory() as session:
        state = session.get(db_models.TriggerEventState, (subscription_id, event_name))
        assert state is not None and state.filled_at is not None
        state.filled_at = state.filled_at - by
        if state.expires_at is not None:
            state.expires_at = state.expires_at - by
        session.commit()


class TestNothingIsWatching:
    def test_an_unwatched_event_is_ignored_not_failed(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The ordinary case for most readiness events: nobody has set up a trigger for them.
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.IGNORE
        assert outcome.detail == {
            "sink": "start_pipeline_run",
            "event_key": "orders-ready",
            "reason": "no_subscription",
        }

    def test_the_ignore_log_line_spells_the_reason_the_way_the_records_do(
        self,
        session_factory: orm.sessionmaker,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # The reason is interpolated into this line. A mixin enum without the `__str__` override
        # renders `TriggerReason.NO_SUBSCRIPTION` here, so the logs and the stored outcome detail
        # would disagree about the name of the same fact -- and only the logs would be searched
        # by hand. Pinned as a string on purpose: comparing to the member would pass either way.
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        with caplog.at_level(logging.INFO, logger=_SINK_LOGGER):
            sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        assert "reason=no_subscription" in caplog.text


class TestRecordingAnArrival:
    def test_recording_is_a_success_even_when_nothing_triggers(
        self,
        session_factory: orm.sessionmaker,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # SUCCESS means "the sink did its work", and writing the event state is the work.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        with caplog.at_level(logging.INFO, logger=_SINK_LOGGER):
            outcome = sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail["subscriptions"] == [
            {
                "subscription_id": subscription_id,
                "triggered": False,
                "cycle": None,
                "reason": "awaiting_events",
                "pipeline_run_id": None,
            }
        ]
        assert "orders-ready" in caplog.text

    def test_the_arrival_lands_on_the_event_state(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 600},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-9",
        )

        states = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )
        assert states["orders-ready"].last_emission_event_id == "em-9"
        assert states["orders-ready"].filled_at is not None
        assert states["orders-ready"].expires_at == states[
            "orders-ready"
        ].filled_at + datetime.timedelta(seconds=600)
        # The event that has not arrived is untouched.
        assert states["fx-ready"].filled_at is None

    def test_a_satisfied_condition_triggers_inside_the_same_call(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        (reported,) = outcome.detail["subscriptions"]
        assert reported["subscription_id"] == subscription_id
        assert reported["triggered"] is True
        assert reported["cycle"] == 0
        assert reported["reason"] is None
        history = _history(session_factory=session_factory)
        assert [(row.subscription_id, row.cycle) for row in history] == [
            (subscription_id, 0)
        ]
        # The run the trigger started, reported and joined back from the fence row.
        assert reported["pipeline_run_id"] == history[0].pipeline_run_id
        assert history[0].pipeline_run_id is not None
        # The arrival that completed the condition is named on the history row, which is the
        # only reason the sink bothers to record the emission id at all.
        assert history[0].triggered_by == {"orders-ready": "em-1"}
        # Triggering clears the event states and opens the next cycle.
        assert (
            _states(session_factory=session_factory, subscription_id=subscription_id)[
                "orders-ready"
            ].filled_at
            is None
        )

    def test_a_second_delivery_of_the_same_emission_does_not_trigger_twice(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # A consumer that dies between a sink's side effect and its ledger write leaves the row
        # claimable, so the same emission is announced again. Without the check in fill() the
        # redelivery refills the state the trigger just cleared and starts a second run for one
        # readiness signal — and the fence cannot catch it, because the cycle has moved on.
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        # And it says so. The redelivery started nothing, but the emission it re-announces did:
        # this row is the emission's permanent entry in the outcome ledger, so reporting
        # `triggered: false` here would durably deny a run that is executing. The cycle is the
        # one that fired (0), not the subscription's current one (1) -- that cycle has not
        # happened.
        assert outcome.detail["subscriptions"] == [
            {
                "subscription_id": subscription_id,
                "triggered": True,
                "cycle": 0,
                "reason": "run_already_started",
                "pipeline_run_id": _history(session_factory=session_factory)[
                    0
                ].pipeline_run_id,
            }
        ]
        # One signal, one run.
        assert [
            (row.subscription_id, row.cycle)
            for row in _history(session_factory=session_factory)
        ] == [(subscription_id, 0)]
        # And the row is still empty: the redelivery did not refill what the trigger consumed.
        assert (
            _states(session_factory=session_factory, subscription_id=subscription_id)[
                "orders-ready"
            ].filled_at
            is None
        )

    def test_a_genuinely_new_emission_still_triggers_the_next_cycle(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The dedupe is per emission, not a lock-out: the next real readiness signal for the
        # same event starts the next cycle.
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )

        assert outcome.detail["subscriptions"][0]["triggered"] is True
        assert [
            (row.subscription_id, row.cycle)
            for row in _history(session_factory=session_factory)
        ] == [
            (subscription_id, 0),
            (subscription_id, 1),
        ]

    def test_a_redelivery_answers_per_subscription_not_in_one_breath(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # One emission, two subscriptions, two different truths on the redelivery: the bare
        # leaf fired on delivery 1 and its run must be named, while the `all` is still waiting
        # and genuinely triggered nothing. A verdict that collapsed the two would be wrong for
        # one of them whichever way it fell.
        fired = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        waiting = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        (started,) = _history(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert decisions[fired]["triggered"] is True
        assert decisions[fired]["reason"] == "run_already_started"
        assert decisions[fired]["pipeline_run_id"] == started.pipeline_run_id
        assert decisions[waiting]["triggered"] is False
        assert decisions[waiting]["reason"] == "arrival_already_recorded"
        assert decisions[waiting]["pipeline_run_id"] is None

    def test_a_redelivery_before_the_condition_holds_changes_nothing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # Same emission, condition still incomplete: the second delivery must not move the
        # expiry window either, since it is not a new arrival.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 600},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        first = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        was = (first.filled_at, first.expires_at)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert (
            outcome.detail["subscriptions"][0]["reason"] == "arrival_already_recorded"
        )
        again = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        assert (again.filled_at, again.expires_at) == was

    def test_a_new_emission_before_the_condition_holds_moves_the_window(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The mirror of the redelivery above: a genuinely new emission for an event already
        # filled is an arrival, so it restamps the row and pushes the expiry out from the new
        # arrival — keeping a half-satisfied condition alive while it waits for the rest.
        # The row is rewound first so the advance is a fixed distance, not a wall-clock delta.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 600},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        with session_factory() as session:
            state = session.get(
                db_models.TriggerEventState, (subscription_id, "orders-ready")
            )
            # Still live — expire_seconds is 600, so a minute back leaves the arrival valid.
            state.filled_at = state.filled_at - datetime.timedelta(seconds=60)
            state.expires_at = state.expires_at - datetime.timedelta(seconds=60)
            # Read before the commit expires the instance's attributes.
            was = (state.filled_at, state.expires_at)
            session.commit()

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )

        assert outcome.detail["subscriptions"][0]["reason"] == "awaiting_events"
        assert outcome.detail["subscriptions"][0]["triggered"] is False
        again = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        assert again.last_emission_event_id == "em-2"
        assert again.filled_at > was[0]
        assert again.expires_at > was[1]
        # The window is measured from the new arrival, not extended from the old one.
        assert again.expires_at == again.filled_at + datetime.timedelta(seconds=600)

    def test_a_disabled_subscription_still_records_the_arrival(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # Disabled withholds the run, not the recording. The arrival lands on the event state
        # exactly as it would for a live subscription, so the delivery really did do work and
        # the honest verdict is success — the reason rides per subscription, not on the status.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={"event": "orders-ready"},
            enabled=False,
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail["subscriptions"] == [
            {
                "subscription_id": subscription_id,
                "triggered": False,
                "cycle": 0,
                "reason": "subscription_disabled",
                "pipeline_run_id": None,
            }
        ]
        # No run, but the arrival is on the row: this is the whole of the new behaviour.
        assert _history(session_factory=session_factory) == []
        state = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        assert state.filled_at is not None
        assert state.last_emission_event_id == "em-1"

    def test_disabling_clears_nothing_that_was_already_recorded(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The other half of the rule: switching a subscription off is not a reset. Only a
        # trigger empties event states, so an arrival banked while it was on is still there
        # afterwards — otherwise re-enabling would silently discard work already done.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        banked = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        was = (
            banked.filled_at,
            banked.expires_at,
            banked.last_emission_event_id,
        )

        _set_enabled(
            session_factory=session_factory,
            subscription_id=subscription_id,
            enabled=False,
        )

        after = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        assert (
            after.filled_at,
            after.expires_at,
            after.last_emission_event_id,
        ) == was

    def test_re_enabling_triggers_off_what_arrived_while_it_was_off(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The consequence the two rules above buy, and the reason they are worth having: a
        # subscription switched off collects its events anyway, so the off->on transition finds
        # the condition already satisfied and starts the run there and then. Nothing further
        # will arrive to prompt a re-check once every event is filled, so if the transition did
        # not evaluate, the subscription would sit satisfied and dormant forever.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
            enabled=False,
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        sink.emit(
            intent=_intent(event_key="fx-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )
        assert _history(session_factory=session_factory) == []

        result = _set_enabled(
            session_factory=session_factory,
            subscription_id=subscription_id,
            enabled=True,
        )

        assert result.triggered is True
        assert result.cycle == 0
        history = _history(session_factory=session_factory)
        assert [(row.subscription_id, row.cycle) for row in history] == [
            (subscription_id, 0)
        ]
        assert history[0].triggered_by == {
            "orders-ready": "em-1",
            "fx-ready": "em-2",
        }
        # Triggering is the one thing that does clear, so the next cycle starts empty.
        states = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )
        assert [state.filled_at for state in states.values()] == [None, None]

    def test_one_disabled_subscription_does_not_hide_an_enabled_one(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The verdict is the most that happened: one arrival recorded makes the delivery a
        # success, however many switched-off subscriptions were skipped alongside it.
        _subscribe(
            session_factory=session_factory,
            condition={"event": "orders-ready"},
            enabled=False,
        )
        live = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert decisions[live]["triggered"] is True


class TestAnEditAgainstWhatTheSinkRecorded:
    """A PATCH is judged against the state the edit leaves, arrivals the sink banked included.

    The sink never handles an edit — that arrives over the API. What is asserted here is that
    an arrival it wrote is real enough for a later edit to trigger off, with no new emission.
    """

    def test_shrinking_the_condition_triggers_off_a_recorded_arrival(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # Dropping the event that was still missing leaves a condition the earlier arrival
        # already satisfies, so the edit starts the run on the spot. This is the third way a
        # run begins, alongside an arrival and a re-enable, and the only one with no emission.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        assert _history(session_factory=session_factory) == []

        result = _set_condition(
            session_factory=session_factory,
            subscription_id=subscription_id,
            condition={"event": "orders-ready"},
        )

        assert (result.triggered, result.cycle) == (True, 0)
        history = _history(session_factory=session_factory)
        assert history[0].triggered_by == {"orders-ready": "em-1"}
        # The dropped event takes its state with it; the survivor is cleared by the trigger.
        states = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )
        assert list(states) == ["orders-ready"]
        assert states["orders-ready"].filled_at is None

    def test_lengthening_an_expiry_revives_a_lapsed_arrival(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # A lapsed arrival is still on the row: expiry is a WHERE clause on the read, not a
        # delete. expires_at is recomputed from the arrival's own filled_at, so widening the
        # window puts it back in the future and the banked arrival counts again.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 3600},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        _rewind(
            session_factory=session_factory,
            subscription_id=subscription_id,
            event_name="orders-ready",
            by=datetime.timedelta(hours=3),
        )
        outcome = sink.emit(
            intent=_intent(event_key="fx-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )
        # The second arrival does not complete anything: the first one has gone stale.
        assert outcome.detail["subscriptions"][0]["reason"] == "awaiting_events"

        result = _set_condition(
            session_factory=session_factory,
            subscription_id=subscription_id,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 86400},
                    {"event": "fx-ready"},
                ],
            },
        )

        assert (result.triggered, result.cycle) == (True, 0)
        assert _history(session_factory=session_factory)[0].triggered_by == {
            "orders-ready": "em-1",
            "fx-ready": "em-2",
        }

    def test_shortening_an_expiry_withdraws_a_recorded_arrival(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The mirror image, and the reason the edit rewrites expires_at rather than leaving it:
        # a narrower window pushes the banked arrival's deadline into the past, so the arrival
        # that would have completed the condition arrives to find nothing to complete.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 86400},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        _rewind(
            session_factory=session_factory,
            subscription_id=subscription_id,
            event_name="orders-ready",
            by=datetime.timedelta(hours=3),
        )

        result = _set_condition(
            session_factory=session_factory,
            subscription_id=subscription_id,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 3600},
                    {"event": "fx-ready"},
                ],
            },
        )
        outcome = sink.emit(
            intent=_intent(event_key="fx-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )

        assert result.triggered is False
        assert outcome.detail["subscriptions"][0]["reason"] == "awaiting_events"
        assert _history(session_factory=session_factory) == []
        # Still recorded, just no longer live: the shortened window is what withdrew it.
        stale = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        assert stale.last_emission_event_id == "em-1"
        assert stale.expires_at == stale.filled_at + datetime.timedelta(seconds=3600)


class TestSeveralSubscriptionsOnOneEvent:
    def test_each_subscription_decides_for_itself(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        ready_now = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        still_waiting = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert decisions[ready_now]["triggered"] is True
        assert decisions[still_waiting]["triggered"] is False
        assert decisions[still_waiting]["reason"] == "awaiting_events"
        # Both arrivals were recorded; only one of them completed a condition.
        assert (
            _states(session_factory=session_factory, subscription_id=still_waiting)[
                "orders-ready"
            ].filled_at
            is not None
        )


class TestAContendedWrite:
    """A deadlock victim is retried in process, because nothing above the sink will retry it."""

    @staticmethod
    def _failing_factory(
        *,
        session_factory: orm.sessionmaker,
        failures: int,
    ) -> tuple[orm.sessionmaker, list[int]]:
        """A factory that raises the given number of retryable failures, then works."""
        attempts: list[int] = []

        def _factory() -> orm.Session:
            attempts.append(len(attempts) + 1)
            if len(attempts) <= failures:
                raise sqlalchemy.exc.OperationalError(
                    "SELECT 1", {}, Exception("deadlock found")
                )
            return session_factory()

        return typing.cast(orm.sessionmaker, _factory), attempts

    def test_a_transient_failure_is_retried_and_then_succeeds(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        factory, attempts = self._failing_factory(
            session_factory=session_factory, failures=2
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail["subscriptions"][0]["subscription_id"] == subscription_id
        # Two failures, then the third attempt did the work.
        assert len(attempts) == 3

    def test_a_failure_that_outlasts_the_retries_is_reported_not_raised(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The contract asks a sink to report an expected failure rather than raise, and FAIL is
        # terminal: the emission row keeps the readiness signal either way.
        _subscribe(session_factory=session_factory, condition={"event": "orders-ready"})
        factory, attempts = self._failing_factory(
            session_factory=session_factory, failures=99
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["sink"] == "start_pipeline_run"
        assert "deadlock" in outcome.detail["error"]
        # Bounded: the retries stop rather than holding the consumer's claim indefinitely.
        assert len(attempts) == 3
        assert _history(session_factory=session_factory) == []

    def test_a_failure_that_a_retry_cannot_fix_is_not_retried(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # A value the column rejects fails identically every time, so retrying it would only
        # repeat the work. It leaves this sink as a raise, which the router turns into a
        # failed verdict for the whole event.
        def _factory() -> orm.Session:
            raise sqlalchemy.exc.DataError("INSERT", {}, Exception("value too long"))

        sink = start_pipeline_run.StartPipelineRunSink(
            session_factory=typing.cast(orm.sessionmaker, _factory)
        )

        with pytest.raises(sqlalchemy.exc.DataError):
            sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )


class TestTheFenceOnTheArrivalPath:
    def test_a_cycle_already_triggered_is_a_success_not_a_duplicate_run(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # Another writer got cycle 0 a moment ago — a second consumer, or the same emission
        # redelivered. The insert collides, the SAVEPOINT rolls back that row alone, and the
        # arrival this call recorded still commits.
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        with session_factory() as session:
            session.add(
                db_models.TriggerHistory(subscription_id=subscription_id, cycle=0)
            )
            session.commit()
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert outcome.detail["subscriptions"] == [
            {
                "subscription_id": subscription_id,
                "triggered": False,
                "cycle": 0,
                "reason": "cycle_already_triggered",
                "pipeline_run_id": None,
            }
        ]
        # Exactly one history row for cycle 0 — the one that was already there.
        assert [
            (row.subscription_id, row.cycle)
            for row in _history(session_factory=session_factory)
        ] == [(subscription_id, 0)]

    def test_the_arrival_survives_losing_the_fence(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # Losing the race must not throw the event away: the emission is settled after this,
        # so an arrival dropped here is never replayed.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [{"event": "orders-ready"}, {"event": "fx-ready"}],
            },
        )
        with session_factory() as session:
            session.add(
                db_models.TriggerHistory(subscription_id=subscription_id, cycle=0)
            )
            session.commit()
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert (
            _states(session_factory=session_factory, subscription_id=subscription_id)[
                "orders-ready"
            ].last_emission_event_id
            == "em-1"
        )

    def test_a_failure_after_the_fence_takes_the_fence_with_it(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Stands in for the run creation that lands with TangleML/tangle#344 and the User
        # Pipeline work: it goes after the fence, inside the same transaction, so if it raises
        # the fence has to roll back with it — otherwise cycle 0 is burned with nothing
        # running. The raise is injected at event_state.clear, the step that already sits
        # between the fence and the cycle bump, so the assertion is about the transaction
        # boundary rather than about whichever call is last today.
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )

        def _boom(**_kwargs: object) -> None:
            raise RuntimeError("run creation failed")

        monkeypatch.setattr(trigger_service.event_state, "clear", _boom)
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        with pytest.raises(RuntimeError, match="run creation failed"):
            sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        # No fence row, so the cycle is still claimable, and nothing was recorded as arriving.
        assert _history(session_factory=session_factory) == []
        with session_factory() as session:
            assert (
                session.get(db_models.TriggerSubscription, subscription_id).cycle == 0
            )
        assert (
            _states(session_factory=session_factory, subscription_id=subscription_id)[
                "orders-ready"
            ].filled_at
            is None
        )


class TestExpiryNeedsNoSweeper:
    def test_an_elapsed_expiry_blocks_the_trigger(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The first arrival's window closes on its own: no sweeper has run, no lazy-clear pass
        # exists, and the row is still there with filled_at set. It simply stops being live,
        # because freshness rides in the live-event query's WHERE.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 1},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        # Age the arrival past its one-second window without sleeping.
        with session_factory() as session:
            state = session.get(
                db_models.TriggerEventState, (subscription_id, "orders-ready")
            )
            state.filled_at = state.filled_at - datetime.timedelta(hours=1)
            state.expires_at = state.expires_at - datetime.timedelta(hours=1)
            session.commit()

        outcome = sink.emit(
            intent=_intent(event_key="fx-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )

        assert outcome.detail["subscriptions"][0]["reason"] == "awaiting_events"
        assert _history(session_factory=session_factory) == []
        # The stale row was neither swept nor cleared — it is still filled, just not fresh.
        assert (
            _states(session_factory=session_factory, subscription_id=subscription_id)[
                "orders-ready"
            ].filled_at
            is not None
        )

    def test_a_fresh_arrival_revives_the_expired_event(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # Latest-wins is what makes an expiry recoverable: re-emitting the stale event fills it
        # again with a new window, and the condition holds.
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={
                "op": "all",
                "children": [
                    {"event": "orders-ready", "expire_seconds": 1},
                    {"event": "fx-ready"},
                ],
            },
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        with session_factory() as session:
            state = session.get(
                db_models.TriggerEventState, (subscription_id, "orders-ready")
            )
            state.expires_at = state.expires_at - datetime.timedelta(hours=1)
            session.commit()
        sink.emit(
            intent=_intent(event_key="fx-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-3",
        )

        assert outcome.detail["subscriptions"][0]["triggered"] is True
        assert [
            row.triggered_by for row in _history(session_factory=session_factory)
        ] == [{"orders-ready": "em-3", "fx-ready": "em-2"}]


class TestOneContendedSubscriptionDoesNotStarveTheRest:
    """A deadlock on one subscription must not cost the subscriptions behind it their signal.

    The fan-out commits per subscription, so a failure that propagates strands the tail — and
    since a retry restarts from the top, the same row starves the same tail on every attempt.
    These drive the whole sink, so what they assert is the verdict and the detail a consumer
    actually sees.
    """

    @staticmethod
    def _deadlock_on(
        *,
        monkeypatch: pytest.MonkeyPatch,
        subscription_ids: set[str],
        for_visits: int = 99,
    ) -> dict[str, int]:
        """Deadlock the named subscriptions on their first `for_visits` visits each.

        Patching `maybe_trigger` puts the failure inside the per-subscription transaction,
        which is where a deadlock victim actually surfaces — after the row is locked.
        """
        visits: dict[str, int] = {}
        real = trigger_service.maybe_trigger

        def _maybe_trigger(
            *, session: orm.Session, subscription: Any, now: Any
        ) -> trigger_service.TriggerResult:
            visits[subscription.id] = visits.get(subscription.id, 0) + 1
            if (
                subscription.id in subscription_ids
                and visits[subscription.id] <= for_visits
            ):
                raise sqlalchemy.exc.OperationalError(
                    "SELECT 1", {}, Exception("deadlock found")
                )
            return real(session=session, subscription=subscription, now=now)

        monkeypatch.setattr(trigger_service, "maybe_trigger", _maybe_trigger)
        # The backoff is real time the test does not need to spend.
        monkeypatch.setattr(start_pipeline_run.time, "sleep", lambda _seconds: None)
        return visits

    def test_the_subscriptions_behind_the_jam_still_get_the_signal(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Four subscriptions, the first one permanently contended. Before containment the loop
        # stopped there, so the other three were never visited on any of the three attempts.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(4)
        )
        jammed = ids[0]
        visits = self._deadlock_on(monkeypatch=monkeypatch, subscription_ids={jammed})
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        # Every subscription behind the jam was visited on the first attempt, not starved.
        assert set(visits) == set(ids)
        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["reason"] == "fan_out_incomplete"
        assert outcome.detail["deferred"] == [jammed]
        assert outcome.detail["recorded"] == ids[1:]
        # And they really ran: three runs started, one per subscription that got through.
        assert len(_history(session_factory=session_factory)) == 3

    def test_a_jam_that_clears_lets_the_delivery_succeed_whole(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The ordinary contended case: the deadlock victim's winner commits, and the retry
        # picks up only the subscription that was left behind.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(3)
        )
        jammed = ids[1]
        self._deadlock_on(
            monkeypatch=monkeypatch, subscription_ids={jammed}, for_visits=1
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert sorted(decisions) == ids
        assert all(entry["triggered"] for entry in decisions.values())
        assert len(_history(session_factory=session_factory)) == 3

    def test_the_retry_does_not_trigger_the_survivors_a_second_time(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A retry re-runs the whole fan-out, which is only safe because `fill` recognises an
        # arrival already recorded. If it did not, the subscriptions that landed on attempt 1
        # would start a second run on attempt 2 off a single readiness signal.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(3)
        )
        self._deadlock_on(
            monkeypatch=monkeypatch, subscription_ids={ids[2]}, for_visits=1
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        # Three subscriptions, three runs — not four, and not six.
        history = _history(session_factory=session_factory)
        assert len(history) == 3
        assert sorted(row.subscription_id for row in history) == ids

    def test_a_trigger_on_the_first_attempt_is_not_masked_by_the_retry(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The subtle one. On the retry, a subscription that already triggered comes back as
        # `arrival_already_recorded` — true, but it is not what happened to it. Reporting the
        # later answer would tell the consumer nothing triggered when a run is already going.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(2)
        )
        winner, jammed = ids
        self._deadlock_on(
            monkeypatch=monkeypatch, subscription_ids={jammed}, for_visits=1
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert decisions[winner]["triggered"] is True
        assert decisions[winner]["reason"] is None
        assert decisions[jammed]["triggered"] is True

    def test_every_subscription_contended_is_a_failure_not_an_ignore(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The trap the two lists exist to avoid. Nothing recorded, but somebody *was* listening
        # — reporting no_subscription here would file a lost signal as a routine IGNORE.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(2)
        )
        self._deadlock_on(monkeypatch=monkeypatch, subscription_ids=set(ids))
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["reason"] == "fan_out_incomplete"
        assert outcome.detail["recorded"] == []
        assert outcome.detail["deferred"] == ids
        assert _history(session_factory=session_factory) == []

    def test_an_unwatched_event_is_still_an_ignore(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        # The other side of that distinction, kept adjacent so the two cannot drift together:
        # genuinely nobody listening stays IGNORE, and never reports a deferred subscription.
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="nobody-waits"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.IGNORE
        assert outcome.detail["reason"] == trigger_service.TriggerReason.NO_SUBSCRIPTION
        assert "deferred" not in outcome.detail

    def test_the_attempts_are_bounded(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A jam that never clears must stop rather than hold the consumer's claim: the
        # contended subscription is visited once per attempt and no more.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(2)
        )
        visits = self._deadlock_on(monkeypatch=monkeypatch, subscription_ids={ids[0]})
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert visits[ids[0]] == 3

    def test_a_partial_fan_out_is_reported_even_if_the_next_attempt_cannot_open(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Attempt 1 records two of three; attempts 2 and 3 cannot even open a session. Raising
        # there would report the delivery as "nothing happened" and throw away the record of
        # what did land, so the partial report wins over the raise.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(3)
        )
        self._deadlock_on(monkeypatch=monkeypatch, subscription_ids={ids[0]})
        opened: list[int] = []

        def _factory() -> orm.Session:
            opened.append(len(opened) + 1)
            if len(opened) > 1:
                raise sqlalchemy.exc.OperationalError(
                    "SELECT 1", {}, Exception("connection lost")
                )
            return session_factory()

        sink = start_pipeline_run.StartPipelineRunSink(
            session_factory=typing.cast(orm.sessionmaker, _factory)
        )

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["reason"] == "fan_out_incomplete"
        assert outcome.detail["recorded"] == ids[1:]
        assert outcome.detail["deferred"] == [ids[0]]

    def test_a_deferred_subscription_can_be_recorded_by_a_later_emission(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Deferring is not poisoning. The row was rolled back whole, so it holds no trace of
        # the emission it missed and the next signal lands on it normally.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(2)
        )
        jammed = ids[0]
        monkeypatch_undo = pytest.MonkeyPatch()
        self._deadlock_on(monkeypatch=monkeypatch_undo, subscription_ids={jammed})
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )
        assert (
            _states(session_factory=session_factory, subscription_id=jammed)[
                "orders-ready"
            ].last_emission_event_id
            is None
        )
        monkeypatch_undo.undo()

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-2",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert decisions[jammed]["triggered"] is True


class TestContentionThatMovesBetweenAttempts:
    """`recorded` accumulates across attempts and `deferred` is one attempt's snapshot.

    A retry re-runs the whole fan-out, so the contended row on attempt 2 need not be the
    contended row on attempt 1 -- it can be a subscription that already committed. Taken
    wholesale, that snapshot reports a fan-out which is in fact complete as
    `fan_out_incomplete`, a terminal FAIL that settles the emission and names a subscription
    whose run is already going.
    """

    @staticmethod
    def _deadlock_per_attempt(
        *,
        monkeypatch: pytest.MonkeyPatch,
        attempts_by_subscription: dict[str, set[int]],
    ) -> dict[str, int]:
        """Deadlock each subscription's row lock on the listed attempts, and only those.

        The lock is patched rather than `maybe_trigger` because it is the first statement of
        the per-subscription transaction and runs whether or not the arrival was already
        recorded -- `maybe_trigger` is skipped on the second visit, so it cannot contend a row
        that already committed. The fan-out takes the lock once per subscription per attempt,
        so a subscription's visit count is the attempt number.
        """
        visits: dict[str, int] = {}
        real = trigger_service.lock_subscription_until_commit

        def _lock(*, session: orm.Session, subscription_id: str) -> Any:
            visits[subscription_id] = visits.get(subscription_id, 0) + 1
            if visits[subscription_id] in attempts_by_subscription.get(
                subscription_id, set()
            ):
                raise sqlalchemy.exc.OperationalError(
                    "SELECT 1", {}, Exception("deadlock found")
                )
            return real(session=session, subscription_id=subscription_id)

        monkeypatch.setattr(trigger_service, "lock_subscription_until_commit", _lock)
        monkeypatch.setattr(start_pipeline_run.time, "sleep", lambda _seconds: None)
        return visits

    def test_a_subscription_recorded_earlier_is_not_reported_deferred(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # attempt 1: `early` commits, `late` is contended.
        # attempt 2: the contention hops to `early` -- already recorded -- and `late` commits.
        # Both landed, so the delivery succeeded and nothing is outstanding.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(2)
        )
        early, late = ids
        self._deadlock_per_attempt(
            monkeypatch=monkeypatch,
            attempts_by_subscription={early: {2, 3}, late: {1}},
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert "deferred" not in outcome.detail
        decisions = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert sorted(decisions) == ids
        assert all(entry["triggered"] for entry in decisions.values())
        # Two subscriptions, two runs -- and `early`'s run came from attempt 1.
        assert len(_history(session_factory=session_factory)) == 2

    def test_a_complete_fan_out_stops_before_an_attempt_that_cannot_open(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The other way out of the loop. Same hop as above, but attempt 3 could not even open
        # a session -- the break that skips the assignment and keeps whatever it left behind.
        # Nothing is outstanding after attempt 2, so the loop must stop there and never reach
        # it; a stale snapshot would both burn the attempt and settle a FAIL on the way out.
        ids = sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(2)
        )
        early, late = ids
        self._deadlock_per_attempt(
            monkeypatch=monkeypatch,
            attempts_by_subscription={early: {2, 3}, late: {1}},
        )
        opened: list[int] = []

        def _factory() -> orm.Session:
            opened.append(len(opened) + 1)
            if len(opened) > 2:
                raise sqlalchemy.exc.OperationalError(
                    "SELECT 1", {}, Exception("connection lost")
                )
            return session_factory()

        sink = start_pipeline_run.StartPipelineRunSink(
            session_factory=typing.cast(orm.sessionmaker, _factory)
        )

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        assert opened == [1, 2]
        assert len(_history(session_factory=session_factory)) == 2


def _soft_delete_target(
    *, session_factory: orm.sessionmaker, subscription_id: str
) -> None:
    """Tombstone the pipeline a subscription points at, the way the delete route does."""
    with session_factory() as session:
        subscription = session.get(db_models.TriggerSubscription, subscription_id)
        assert subscription is not None
        pipeline = session.get(
            user_pipeline_db_models.UserPipeline,
            subscription.pipeline_task_spec_from_user_pipeline_id,
        )
        assert pipeline is not None
        pipeline.deleted_at = datetime.datetime.now(datetime.timezone.utc)
        session.commit()


# A stored spec that is present and still not a TaskSpec: `image` must be a string.
_UNBUILDABLE_TASK: dict[str, Any] = {
    "componentRef": {"spec": {"implementation": {"container": {"image": 5}}}}
}


def _break_target_spec(
    *, session_factory: orm.sessionmaker, subscription_id: str
) -> None:
    """Leave the target alive, holding a current version whose spec will not parse."""
    with session_factory() as session:
        subscription = session.get(db_models.TriggerSubscription, subscription_id)
        assert subscription is not None
        version = session.get(
            user_pipeline_db_models.UserPipelineVersion,
            (
                subscription.pipeline_task_spec_from_user_pipeline_id,
                user_pipeline_db_models.CURRENT_VERSION_KEY,
            ),
        )
        assert version is not None
        version.root_pipeline_task = _UNBUILDABLE_TASK
        session.commit()


class TestEveryPermanentReasonReachesTheVerdict:
    """The sink recomputes `failed` after its retries, and it must recompute all of them.

    `failed` is a view of `outcomes` rather than a merge, because the attempt that produced a
    given outcome is not necessarily the last one to run. That recompute is a second place
    where a reason has to be classified as permanent, and a reason known to the fan-out but not
    here reports SUCCESS for a subscription that never ran -- the emission then settles as a
    normal delivery and the missing run is invisible. Both sides read
    `trigger_service.RUN_NOT_STARTED_REASONS` so there is one list, not two.
    """

    def test_an_unbuildable_target_fails_the_delivery(
        self, session_factory: orm.sessionmaker
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        _break_target_spec(
            session_factory=session_factory, subscription_id=subscription_id
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["reason"] == "runs_not_started"
        (failure,) = outcome.detail["failed"]
        assert failure["subscription_id"] == subscription_id
        assert failure["reason"] == trigger_service.TriggerReason.TARGET_UNBUILDABLE
        assert failure["error"].startswith("ValidationError:")

    def test_an_unexpected_run_failure_fails_the_delivery(
        self, session_factory: orm.sessionmaker
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        def fail(self, **kwargs):
            del self, kwargs
            raise RuntimeError("something nobody predicted")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                user_pipeline_services.UserPipelineService,
                "create_from_pipeline_no_commit",
                fail,
            )
            outcome = sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        (failure,) = outcome.detail["failed"]
        assert failure["subscription_id"] == subscription_id
        assert failure["reason"] == trigger_service.TriggerReason.RUN_START_FAILED
        assert failure["error"] == "RuntimeError: something nobody predicted"
        # Recorded, so a fix followed by a repoint can still start the run.
        assert (
            _states(session_factory=session_factory, subscription_id=subscription_id)[
                "orders-ready"
            ].filled_at
            is not None
        )

    def test_a_permanent_failure_is_not_retried(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """Same contract as a dead target: three attempts are for `deferred`, not `failed`."""
        subscription_id = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        _break_target_spec(
            session_factory=session_factory, subscription_id=subscription_id
        )
        attempts: list[int] = []
        real = trigger_service.record_event_and_maybe_start_runs

        def counting(**kwargs):
            attempts.append(1)
            return real(**kwargs)

        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                start_pipeline_run.trigger_service,
                "record_event_and_maybe_start_runs",
                counting,
            )
            outcome = sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert len(attempts) == 1


class TestADeadTargetIsReportedNotSwallowed:
    """The target was deleted after the subscription was written.

    Three verdicts are in play and the difference between them is the whole point. IGNORE means
    nothing was listening. SUCCESS means every arrival landed and each subscription decided for
    itself. FAIL means something went wrong that the operator has to know about — and because
    recording a verdict settles the emission with no redelivery behind it, the detail written
    here is the only trace that survives.

    A dead target is FAIL and not a quiet SUCCESS: "the condition held and no run started" is
    exactly the failure that would otherwise go unnoticed until someone asks why the pipeline
    never ran.
    """

    def test_a_dead_target_fails_the_delivery_and_names_the_error(
        self, session_factory: orm.sessionmaker
    ) -> None:
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={"event": "orders-ready"},
        )
        _soft_delete_target(
            session_factory=session_factory, subscription_id=subscription_id
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["reason"] == "runs_not_started"
        assert outcome.detail["recorded"] == [subscription_id]
        (failure,) = outcome.detail["failed"]
        assert failure["subscription_id"] == subscription_id
        assert failure["reason"] == trigger_service.TriggerReason.USER_PIPELINE_DELETED
        # The error string, so whoever reads the settled outcome learns what was wrong.
        assert failure["error"]

    def test_the_arrival_is_still_recorded(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """FAIL is about the run, not the arrival — and the arrival is the recovery path.

        Its emission will not be redelivered, so this row is the only remaining evidence that
        the condition was ever satisfied. Repointing the subscription re-evaluates against it.
        """
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={"event": "orders-ready"},
        )
        _soft_delete_target(
            session_factory=session_factory, subscription_id=subscription_id
        )
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        state = _states(
            session_factory=session_factory, subscription_id=subscription_id
        )["orders-ready"]
        assert state.last_emission_event_id == "em-1"
        assert state.filled_at is not None
        # Nothing else was written: no fence, so the cycle is still there to be claimed.
        assert _history(session_factory=session_factory) == []
        with session_factory() as session:
            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        bts.PipelineRun
                    )
                )
                == 0
            )

    def test_one_dead_target_does_not_stop_the_live_ones(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """Caught per subscription, so a corpse in the middle of the fan-out is survivable."""
        dead = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        live_one = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        live_two = _subscribe(
            session_factory=session_factory, condition={"event": "orders-ready"}
        )
        _soft_delete_target(session_factory=session_factory, subscription_id=dead)
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert [entry["subscription_id"] for entry in outcome.detail["failed"]] == [
            dead
        ]
        # The two live subscriptions triggered and committed regardless.
        assert sorted(
            row.subscription_id for row in _history(session_factory=session_factory)
        ) == sorted([live_one, live_two])
        with session_factory() as session:
            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        bts.PipelineRun
                    )
                )
                == 2
            )

    def test_a_dead_target_is_not_confused_with_a_contended_one(
        self, session_factory: orm.sessionmaker
    ) -> None:
        """`failed` and `deferred` are different lists because they mean different things.

        A deferred subscription never got the signal and a retry can still deliver it. A failed
        one got it and cannot act on it, and no retry will change that — so reporting a dead
        target under `deferred` would have the sink burn its three attempts on a corpse.
        """
        subscription_id = _subscribe(
            session_factory=session_factory,
            condition={"event": "orders-ready"},
        )
        _soft_delete_target(
            session_factory=session_factory, subscription_id=subscription_id
        )
        attempts: list[int] = []
        real = trigger_service.record_event_and_maybe_start_runs

        def counting(**kwargs):
            attempts.append(1)
            return real(**kwargs)

        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                trigger_service, "record_event_and_maybe_start_runs", counting
            )
            patch.setattr(
                start_pipeline_run.trigger_service,
                "record_event_and_maybe_start_runs",
                counting,
            )
            outcome = sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        assert outcome.status is handler_base.OutcomeStatus.FAIL
        assert outcome.detail["reason"] == "runs_not_started"
        # Once. A permanent failure is not retried.
        assert len(attempts) == 1


class TestADeliveryReasonIsWrittenAsItsWireValue:
    """The counterpart of the per-subscription pinning in the trigger service tests.

    Nothing interpolates these two today -- they only reach the outcome detail, where JSON
    already unwraps a str subclass. The `__str__` override and this test exist so the first
    log line that does interpolate one does not quietly write `_DeliveryReason.RUNS_NOT_STARTED`
    beside details that say `runs_not_started`.
    """

    def test_every_member_renders_and_serialises_as_its_value(self) -> None:
        for reason in start_pipeline_run._DeliveryReason:
            assert f"{reason}" == reason.value
            assert str(reason) == reason.value
            assert json.dumps(reason) == json.dumps(reason.value)


class TestABudgetExhaustedFanOutAsksForTheEmissionBack:
    """Out of time is not a verdict: the sink raises so the emission is never settled."""

    @staticmethod
    def _three(session_factory: orm.sessionmaker) -> list[str]:
        return sorted(
            _subscribe(
                session_factory=session_factory,
                condition={"event": "orders-ready"},
            )
            for _ in range(3)
        )

    def test_the_sink_raises_rather_than_reporting_a_verdict(
        self, session_factory: orm.sessionmaker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Every Outcome this sink can return settles the emission, so reporting "some of it
        # happened" would close a row whose remaining subscriptions never heard the signal.
        # The generic exception, not the trigger one: above the sink nothing knows what a
        # subscription is.
        self._three(session_factory)
        monkeypatch.setattr(start_pipeline_run, "_FAN_OUT_BUDGET_SECONDS", -1.0)
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        with pytest.raises(handler_base.DeliveryIncomplete):
            sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

    def test_the_subscription_it_did_reach_keeps_its_run(
        self, session_factory: orm.sessionmaker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The `index > 0` guard seen from outside: a spent budget still buys one subscription,
        # which is what stops a redelivery loop that never gets anywhere.
        first, _second, _third = self._three(session_factory)
        monkeypatch.setattr(start_pipeline_run, "_FAN_OUT_BUDGET_SECONDS", -1.0)
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)

        with pytest.raises(handler_base.DeliveryIncomplete):
            sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )

        assert [
            row.subscription_id for row in _history(session_factory=session_factory)
        ] == [first]

    def test_the_redelivery_finishes_the_rest(
        self, session_factory: orm.sessionmaker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # What the raise is asking for. The redelivery re-derives the list, the subscription
        # already served answers `run_already_started` off the read-back, and the budget goes
        # to the two still waiting.
        first, second, third = self._three(session_factory)
        monkeypatch.setattr(start_pipeline_run, "_FAN_OUT_BUDGET_SECONDS", -1.0)
        sink = start_pipeline_run.StartPipelineRunSink(session_factory=session_factory)
        with pytest.raises(handler_base.DeliveryIncomplete):
            sink.emit(
                intent=_intent(event_key="orders-ready"),
                execution_node_id="node-1",
                emission_event_id="em-1",
            )
        monkeypatch.setattr(start_pipeline_run, "_FAN_OUT_BUDGET_SECONDS", 3600.0)

        outcome = sink.emit(
            intent=_intent(event_key="orders-ready"),
            execution_node_id="node-1",
            emission_event_id="em-1",
        )

        assert outcome.status is handler_base.OutcomeStatus.SUCCESS
        by_id = {
            entry["subscription_id"]: entry for entry in outcome.detail["subscriptions"]
        }
        assert (
            by_id[first]["reason"] == trigger_service.TriggerReason.RUN_ALREADY_STARTED
        )
        assert by_id[second]["triggered"] is True
        assert by_id[third]["triggered"] is True
        assert {
            row.subscription_id for row in _history(session_factory=session_factory)
        } == {first, second, third}
