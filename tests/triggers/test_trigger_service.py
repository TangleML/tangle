"""Unit tests for triggers.service and triggers.event_state — sync, then maybe trigger.

Named test_trigger_service so the module basename stays unique across the suite: the repo has
no __init__.py in tests and pytest runs in the default prepend import mode, which keys modules
by basename.

Every test drives real SQL against SQLite rather than a mocked session: what is being asserted
is which rows survive an edit, and a mock cannot be wrong about that.
"""

import collections.abc
import copy
import datetime
import itertools
import json
import logging
import os
import time
from typing import Any

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.triggers import db_models, event_state, service
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from cloud_pipelines_backend.utils import db as db_utils

# A fixed clock, so an expiry can be placed either side of "now" without sleeping.
_NOW = datetime.datetime(2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
_HOUR = datetime.timedelta(hours=1)


def _leaf(event: str, **extra: Any) -> dict[str, Any]:
    return {"event": event, **extra}


def _all(*children: Any) -> dict[str, Any]:
    return {"op": "all", "children": list(children)}


def _any(*children: Any) -> dict[str, Any]:
    return {"op": "any", "children": list(children)}


def _definition(
    condition: dict[str, Any],
    *,
    name: str = "nightly-retrain",
    templates: dict[str, str] | None = None,
) -> dict[str, Any]:
    definition = {"name": name, "condition": condition}
    if templates is not None:
        definition["pipeline_templates"] = templates
    return definition


# `pipeline` is unique on (user_id, file_path), so each pipeline these helpers mint needs its
# own path; a shared one would fail a second _subscribe in the same session on the wrong table.
_pipeline_serial = itertools.count()


# A task spec a run can actually be built from. It has to be valid, not merely present: a
# trigger now builds a real pipeline run out of whatever the target's current version holds.
_RUNNABLE_TASK: dict[str, Any] = {
    "componentRef": {
        "spec": {
            "name": "triggered-target",
            "implementation": {"graph": {"tasks": {}}},
        }
    }
}


def _pipeline_id(session: orm.Session, *, declares: tuple[str, ...] = ()) -> str:
    """A real, runnable pipeline for a subscription to target, and its id.

    Real rather than an invented id: the target column is NOT NULL and a foreign key, and a
    plausible-looking string would only pass because this engine leaves SQLite's foreign keys
    switched off.

    Runnable rather than merely present, which is newer: a trigger starts a run from this
    pipeline's current version, so a row with no version at all now fails the trigger instead
    of being ignored.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id="test-owner",
        file_path=f"pipelines/auto-{next(_pipeline_serial)}.py",
    )
    session.add(pipeline)
    session.flush()
    task = copy.deepcopy(_RUNNABLE_TASK)
    if declares:
        # Run submission refuses an argument for an input the pipeline does not declare, and
        # a required one with no value refuses just as hard, so these are optional.
        task["componentRef"]["spec"]["inputs"] = [
            {"name": each, "optional": True} for each in declares
        ]
    session.add(
        user_pipeline_db_models.UserPipelineVersion(
            pipeline_id=pipeline.id,
            version_key=user_pipeline_db_models.CURRENT_VERSION_KEY,
            content_digest="d" * user_pipeline_db_models.DIGEST_LENGTH,
            root_pipeline_task=task,
        )
    )
    session.flush()
    # Pointed at the version only after it exists. `current_version_key` is half of a composite
    # foreign key into `user_pipeline_version`, so setting it on the insert fails wherever
    # SQLite's foreign keys are switched on.
    pipeline.current_version_key = user_pipeline_db_models.CURRENT_VERSION_KEY
    session.flush()
    return pipeline.id


# (created_by, name) is a unique key and this helper stamps a single created_by, so the name
# is what has to vary. A counter rather than a fixed default: the tests that build several
# subscriptions ask for them in a `for _ in range(n)` and care about ids and ordering, never
# about what the rows are called, so making them each invent a name would be noise.
_SUBSCRIPTION_NAMES = itertools.count(1)


def _subscribe(
    session: orm.Session,
    *,
    condition: dict[str, Any],
    name: str | None = None,
    templates: dict[str, str] | None = None,
) -> db_models.TriggerSubscription:
    """Create a subscription and the event-state rows its condition asks for.

    `name` defaults to a fresh one per call, so asking for two subscriptions gives two rows
    rather than a collision. Pass it explicitly when the test is about the name itself.
    """
    name = f"nightly-retrain-{next(_SUBSCRIPTION_NAMES)}" if name is None else name
    subscription = db_models.TriggerSubscription(
        name=name,
        # The column and the blob agree, the way the service keeps them.
        definition=_definition(condition, name=name, templates=templates),
        created_by="test-owner",
        pipeline_task_spec_from_user_pipeline_id=_pipeline_id(
            session, declares=tuple(templates or ())
        ),
    )
    session.add(subscription)
    session.flush()
    event_state.sync(
        session=session, subscription_id=subscription.id, condition=condition
    )
    session.commit()
    return subscription


def _states_of(session: orm.Session, subscription_id: str) -> list[str]:
    """The event names a subscription still has state rows for, sorted."""
    return sorted(
        session.scalars(
            sqlalchemy.select(db_models.TriggerEventState.event_name).where(
                db_models.TriggerEventState.subscription_id == subscription_id
            )
        ).all()
    )


def _fill(
    session: orm.Session,
    *,
    subscription: db_models.TriggerSubscription,
    event: str,
    at: datetime.datetime = _NOW - _HOUR,
    emission_id: str = "em-1",
) -> None:
    """Record an arrival the way the sink does — through the same function it calls."""
    state = session.get(db_models.TriggerEventState, (subscription.id, event))
    assert state is not None
    event_state.fill(state=state, emission_event_id=emission_id, now=at)
    session.commit()


def _states(
    session: orm.Session, *, subscription: db_models.TriggerSubscription
) -> dict[str, db_models.TriggerEventState]:
    rows = session.scalars(
        sqlalchemy.select(db_models.TriggerEventState).where(
            db_models.TriggerEventState.subscription_id == subscription.id
        )
    ).all()
    return {row.event_name: row for row in rows}


def _history(session: orm.Session) -> list[db_models.TriggerHistory]:
    return list(session.scalars(sqlalchemy.select(db_models.TriggerHistory)).all())


# A millisecond epoch to mint test ids in, and its neighbour one millisecond later.
_MS = 1_735_732_800_000
_NEXT_MS = _MS + 1


def _id_at(*, milliseconds: int, tail: str) -> str:
    """An emission id with the millisecond and the random tail both chosen.

    Shaped exactly like `generate_unique_id`: a 12-hex millisecond epoch then an 8-hex tail. The
    tail is what the real generator fills with `os.urandom`, so pinning it is how a
    same-millisecond ordering case becomes a deterministic test instead of a coin flip.
    """
    identifier = ("%012x" % milliseconds) + tail
    assert len(identifier) == db_utils.ID_LENGTH, identifier
    return identifier


_HIGH_TAIL = "ffffffff"
_LOW_TAIL = "00000000"


# The helper stamps created_by="test-owner", so this caller owns every subscription under test.
_CALLER = service.Caller(name="test-owner")


@pytest.fixture()
def no_autoflush_session(
    db_engine: sqlalchemy.Engine,
) -> collections.abc.Generator[orm.Session, None, None]:
    """A session shaped like the one the service actually runs under.

    `app.py` and the route fixtures build sessions with `autoflush=False`; the shared `session`
    fixture takes SQLAlchemy's default, which is on. The difference is not cosmetic — with
    autoflush on, an ORM-enabled SELECT quietly flushes pending writes on your behalf, so code
    that forgot an explicit flush still reads its own edits and every test passes. Anything
    asserting that an explicit flush happened has to run without that safety net, or it is
    asserting nothing.
    """
    with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as sess:
        yield sess


class TestSync:
    def test_a_survivor_keeps_everything_it_had(self, session: orm.Session) -> None:
        # The point of syncing rather than rebuilding: an edit elsewhere in the condition
        # must not throw away an arrival that already happened.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a"), _leaf("c")),
                now=_NOW,
            )

        survivor = _states(session, subscription=subscription)["a"]
        assert survivor.filled_at == _NOW - _HOUR
        assert survivor.last_emission_event_id == "em-1"

    def test_a_removed_event_takes_its_state_with_it(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="b")

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a"), _leaf("c")),
                now=_NOW,
            )

        assert sorted(_states(session, subscription=subscription)) == ["a", "c"]

    def test_an_added_event_starts_empty(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a"), _leaf("c", expire_seconds=60)),
                now=_NOW,
            )

        added = _states(session, subscription=subscription)["c"]
        assert (
            added.filled_at,
            added.expires_at,
            added.last_emission_event_id,
        ) == (
            None,
            None,
            None,
        )
        assert added.expire_seconds == 60

    def test_an_identical_update_writes_nothing(
        self, session: orm.Session, db_engine: sqlalchemy.Engine
    ) -> None:
        # No fingerprint column detects this: the diff is empty on both sides, so there is
        # nothing for a sync to emit.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")
        statements: list[str] = []

        @sqlalchemy.event.listens_for(db_engine, "before_cursor_execute")
        def _record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
            statements.append(statement.split()[0].upper())

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a"), _leaf("b")),
                now=_NOW,
            )

        assert [
            statement for statement in statements if statement in {"INSERT", "DELETE"}
        ] == []

    def test_a_malformed_condition_is_rejected_before_any_row_moves(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))

        with pytest.raises(ValueError, match="unknown condition node"):
            with session.begin():
                service.update_subscription(
                    session=session,
                    caller=_CALLER,
                    subscription=subscription,
                    condition={"op": "nope", "children": []},
                    now=_NOW,
                )

        assert sorted(_states(session, subscription=subscription)) == ["a"]
        assert subscription.definition["condition"] == _all(_leaf("a"))


class TestExpiry:
    def test_an_expired_arrival_does_not_count(self, session: orm.Session) -> None:
        # No sweeper has run and the row still holds its filled_at: freshness is decided by
        # the query, not by a background job.
        subscription = _subscribe(
            session, condition=_all(_leaf("a", expire_seconds=3600))
        )
        _fill(session, subscription=subscription, event="a", at=_NOW - 3 * _HOUR)

        assert (
            event_state.events_emitted(
                session=session, subscription_id=subscription.id, now=_NOW
            )
            == {}
        )
        assert (
            _states(session, subscription=subscription)["a"].filled_at
            == _NOW - 3 * _HOUR
        )

    def test_lengthening_an_expiry_revives_a_lapsed_arrival(
        self, session: orm.Session
    ) -> None:
        # Deliberate: the emission genuinely arrived, and the caller has just declared that
        # arrivals stay valid for a day. So this configuration edit starts a run.
        subscription = _subscribe(
            session, condition=_all(_leaf("a", expire_seconds=3600))
        )
        _fill(session, subscription=subscription, event="a", at=_NOW - 3 * _HOUR)

        with session.begin():
            result = service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a", expire_seconds=86400)),
                now=_NOW,
            )

        assert result.triggered is True

    def test_shortening_an_expiry_moves_expires_at_into_the_past(
        self, session: orm.Session
    ) -> None:
        # An expiry edit that left expires_at alone would look like it had done nothing.
        subscription = _subscribe(
            session, condition=_all(_leaf("a", expire_seconds=86400))
        )
        _fill(session, subscription=subscription, event="a", at=_NOW - 3 * _HOUR)

        with session.begin():
            result = service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a", expire_seconds=3600)),
                now=_NOW,
            )

        assert result.triggered is False
        assert (
            _states(session, subscription=subscription)["a"].expires_at
            == _NOW - 2 * _HOUR
        )

    def test_removing_an_expiry_makes_an_arrival_permanent(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(
            session, condition=_all(_leaf("a", expire_seconds=3600))
        )
        _fill(session, subscription=subscription, event="a", at=_NOW - 3 * _HOUR)

        with session.begin():
            result = service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a")),
                now=_NOW,
            )

        assert result.triggered is True
        # A trigger empties the row rather than deleting it: the event is still subscribed.
        permanent = _states(session, subscription=subscription)["a"]
        assert (
            permanent.expire_seconds,
            permanent.filled_at,
            permanent.expires_at,
        ) == (None, None, None)

    def test_an_expiry_on_an_unfilled_event_stays_pending(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a", expire_seconds=60), _leaf("b")),
                now=_NOW,
            )

        pending = _states(session, subscription=subscription)["a"]
        assert (
            pending.expire_seconds,
            pending.filled_at,
            pending.expires_at,
        ) == (
            60,
            None,
            None,
        )


class TestFilledEvents:
    """Reading the arrived events, split into the fresh ones and the expired ones.

    Both halves used to be separate queries whose WHERE clauses had to stay each other's exact
    negation by hand. They are one seek now, so the tests worth having are that the split
    lands on the right side of `now` and that it really is one seek.
    """

    def test_it_splits_the_arrived_rows_on_freshness(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(
            session,
            condition=_all(
                _leaf("never-expires"),
                _leaf("still-fresh", expire_seconds=86400),
                _leaf("long-gone", expire_seconds=3600),
                _leaf("never-arrived"),
            ),
        )
        _fill(
            session,
            subscription=subscription,
            event="never-expires",
            at=_NOW - _HOUR,
        )
        _fill(
            session,
            subscription=subscription,
            event="still-fresh",
            at=_NOW - _HOUR,
        )
        _fill(
            session,
            subscription=subscription,
            event="long-gone",
            at=_NOW - 3 * _HOUR,
        )

        filled = event_state.filled_events(
            session=session, subscription_id=subscription.id, now=_NOW
        )

        # never-arrived is in neither half: it has no filled_at, so it is not an arrival at
        # all rather than an expired one.
        assert sorted(filled.emitted) == ["never-expires", "still-fresh"]
        assert filled.lapsed == ["long-gone"]

    def test_an_expiry_exactly_now_has_lapsed(self, session: orm.Session) -> None:
        # Freshness is strictly `>`, so the boundary instant belongs to the lapsed half. The
        # two halves being one pass is what stops it landing in both or neither.
        subscription = _subscribe(
            session, condition=_all(_leaf("a", expire_seconds=3600))
        )
        _fill(session, subscription=subscription, event="a", at=_NOW - _HOUR)

        filled = event_state.filled_events(
            session=session, subscription_id=subscription.id, now=_NOW
        )

        assert filled.emitted == {}
        assert filled.lapsed == ["a"]

    def test_the_lapsed_names_are_sorted(self, session: orm.Session) -> None:
        subscription = _subscribe(
            session,
            condition=_all(
                _leaf("c", expire_seconds=3600),
                _leaf("a", expire_seconds=3600),
                _leaf("b", expire_seconds=3600),
            ),
        )
        for event in ("c", "a", "b"):
            _fill(
                session,
                subscription=subscription,
                event=event,
                at=_NOW - 3 * _HOUR,
            )

        filled = event_state.filled_events(
            session=session, subscription_id=subscription.id, now=_NOW
        )

        assert filled.lapsed == ["a", "b", "c"]

    def test_nothing_arrived_is_two_empty_halves(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))

        filled = event_state.filled_events(
            session=session, subscription_id=subscription.id, now=_NOW
        )

        assert (filled.emitted, filled.lapsed) == ({}, [])

    def test_events_emitted_is_the_fresh_half(self, session: orm.Session) -> None:
        # events_emitted is a delegate now, so the contract its own callers rely on is that it
        # still answers exactly what the fresh half holds.
        subscription = _subscribe(
            session,
            condition=_all(
                _leaf("fresh", expire_seconds=86400),
                _leaf("lapsed", expire_seconds=3600),
            ),
        )
        _fill(session, subscription=subscription, event="fresh", at=_NOW - _HOUR)
        _fill(
            session,
            subscription=subscription,
            event="lapsed",
            at=_NOW - 3 * _HOUR,
        )

        assert (
            event_state.events_emitted(
                session=session, subscription_id=subscription.id, now=_NOW
            )
            == event_state.filled_events(
                session=session, subscription_id=subscription.id, now=_NOW
            ).emitted
        )

    def test_it_reads_the_table_once(
        self, session: orm.Session, db_engine: sqlalchemy.Engine
    ) -> None:
        # The point of the change. Asking for the fresh and the expired halves separately put
        # a second SELECT on the waiting path, once per subscription in an arrival's fan-out.
        subscription = _subscribe(
            session,
            condition=_all(
                _leaf("fresh", expire_seconds=86400),
                _leaf("lapsed", expire_seconds=3600),
            ),
        )
        _fill(session, subscription=subscription, event="fresh", at=_NOW - _HOUR)
        _fill(
            session,
            subscription=subscription,
            event="lapsed",
            at=_NOW - 3 * _HOUR,
        )
        statements: list[str] = []

        @sqlalchemy.event.listens_for(db_engine, "before_cursor_execute")
        def _record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
            statements.append(statement)

        try:
            event_state.filled_events(
                session=session, subscription_id=subscription.id, now=_NOW
            )
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        assert (
            sum("trigger_event_state" in statement for statement in statements) == 1
        ), statements


class TestTheExpiredMetricCostsNoExtraQuery:
    """The fan-out path reads each subscription's rows once, however many are waiting.

    `trigger.event_expired` is recorded on the path where the condition did not hold, which is
    the normal path for a subscription still collecting events. Reading the expired names with
    a query of their own meant one extra round trip per subscription per arrival.
    """

    @staticmethod
    def _reads_for_a_fan_out_of(
        *,
        session: orm.Session,
        db_engine: sqlalchemy.Engine,
        subscriptions: int,
    ) -> int:
        """SELECTs against trigger_event_state for one arrival fanning out that wide.

        The awaited event is named after the width so that two measurements in one session
        stay independent: an arrival only reaches the subscriptions built for it.
        """
        awaited = f"a{subscriptions}"
        waiting = [
            _subscribe(
                session,
                condition=_all(_leaf(awaited), _leaf("b", expire_seconds=3600)),
            )
            for _ in range(subscriptions)
        ]
        # Each one holds an arrival that has since lapsed, so every subscription in the
        # fan-out takes the branch that records the metric.
        for subscription in waiting:
            _fill(
                session,
                subscription=subscription,
                event="b",
                at=_NOW - 3 * _HOUR,
            )
        statements: list[str] = []

        @sqlalchemy.event.listens_for(db_engine, "before_cursor_execute")
        def _record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
            statements.append(statement)

        try:
            fan_out = service.record_event_and_maybe_start_runs(
                session=session,
                event_name=awaited,
                emission_event_id=f"em-{awaited}",
                now=_NOW,
            )
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        # Every one of them is still waiting, so this is the path that reads the metric.
        assert [outcome.result.reason for outcome in fan_out.outcomes] == [
            service.TriggerReason.AWAITING_EVENTS
        ] * subscriptions
        return sum(
            statement.upper().lstrip().startswith("SELECT")
            and "trigger_event_state" in statement
            for statement in statements
        )

    def test_each_extra_subscription_costs_one_read(
        self, session: orm.Session, db_engine: sqlalchemy.Engine
    ) -> None:
        # A delta rather than a total, so the fan-out's own constant overhead — the one seek
        # that finds who is waiting — cannot be mistaken for per-subscription cost.
        few = self._reads_for_a_fan_out_of(
            session=session, db_engine=db_engine, subscriptions=3
        )
        many = self._reads_for_a_fan_out_of(
            session=session, db_engine=db_engine, subscriptions=8
        )

        # Two: the row `fill` loads to write the arrival onto, and the one `filled_events`
        # takes to evaluate against. Reading the expired names with a query of their own made
        # it three, which is the round trip this is here to keep out.
        reads_per_subscription = (many - few) / 5
        assert reads_per_subscription == 2, (
            f"3 subscriptions read {few}, 8 read {many}"
            f" — {reads_per_subscription} per subscription"
        )


class TestMissing:
    """What a subscription reports it is waiting for.

    This is the number a person reads off the API to decide what to emit next, so it has to be
    the events that would actually move the condition. It used to be every event the condition
    named minus the ones live, which over-reports the moment an `any` branch is settled.
    """

    _A_OR_B_AND_C = _all(_any(_leaf("a"), _leaf("b")), _leaf("c"))

    @pytest.mark.parametrize(
        ("condition", "emitted", "expected"),
        [
            # A settled `any` asks for nothing more, so `b` is not reported.
            (_A_OR_B_AND_C, {"a": "em-1"}, ["c"]),
            (_A_OR_B_AND_C, {}, ["a", "b", "c"]),
            (_all(_leaf("a"), _leaf("b")), {"a": "em-1"}, ["b"]),
            # Satisfied: a subscription about to fire is not waiting on anything.
            (_any(_leaf("a"), _leaf("b")), {"b": "em-1"}, []),
            (_leaf("a"), {}, ["a"]),
            (_A_OR_B_AND_C, {"a": "em-1", "c": "em-2"}, []),
        ],
    )
    def test_only_what_would_move_the_condition(
        self,
        condition: dict[str, Any],
        emitted: dict[str, str | None],
        expected: list[str],
    ) -> None:
        assert event_state.missing(condition=condition, emitted=emitted) == expected

    def test_it_is_sorted(self) -> None:
        # The API returns this straight to a caller, so the order cannot depend on set hashing.
        condition = _all(_leaf("zebra"), _leaf("alpha"), _leaf("middle"))
        assert event_state.missing(condition=condition, emitted={}) == [
            "alpha",
            "middle",
            "zebra",
        ]

    def test_an_arrival_that_expired_is_missing_again(
        self, session: orm.Session
    ) -> None:
        # End to end against the real event-state rows rather than a hand-built mapping: an
        # expired arrival drops out of `events_emitted`, so the event is outstanding once more.
        subscription = _subscribe(
            session, condition=_all(_leaf("a", expire_seconds=60), _leaf("b"))
        )
        _fill(session, subscription=subscription, event="a", at=_NOW - _HOUR)

        emitted = event_state.events_emitted(
            session=session, subscription_id=subscription.id, now=_NOW
        )
        condition = subscription.definition["condition"]
        assert event_state.missing(condition=condition, emitted=emitted) == [
            "a",
            "b",
        ]

    def test_a_settled_choice_is_not_reported_end_to_end(
        self, session: orm.Session
    ) -> None:
        # The regression, against real rows: `b` has a row in trigger_event_state and no
        # arrival, but the `any` it sits under is already settled by `a`.
        subscription = _subscribe(session, condition=self._A_OR_B_AND_C)
        _fill(session, subscription=subscription, event="a")

        emitted = event_state.events_emitted(
            session=session, subscription_id=subscription.id, now=_NOW
        )
        assert set(emitted) == {"a"}
        assert "b" in _states(session, subscription=subscription)
        assert event_state.missing(condition=self._A_OR_B_AND_C, emitted=emitted) == [
            "c"
        ]

    def test_the_awaiting_result_reports_the_same_set(
        self, session: orm.Session
    ) -> None:
        # `TriggerResult.missing` is the other caller, and it must not disagree with the API.
        subscription = _subscribe(session, condition=self._A_OR_B_AND_C)
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert (result.triggered, result.reason, result.missing) == (
            False,
            service.TriggerReason.AWAITING_EVENTS,
            ("c",),
        )


class TestMissingSeparatesEmptyFromUnasked:
    """`()` means "evaluated, nothing outstanding"; None means "never evaluated".

    The route serializes the difference straight through, so collapsing the two here is what
    made a rename claim a satisfied condition.
    """

    def test_a_disabled_subscription_answers_none(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        subscription.enabled = False
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert (result.reason, result.missing) == (
            service.TriggerReason.SUBSCRIPTION_DISABLED,
            None,
        )

    def test_a_trigger_answers_the_empty_tuple(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert (result.triggered, result.missing) == (True, ())

    def test_an_edit_that_evaluates_nothing_answers_none(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            name="renamed",
            now=_NOW,
        )
        session.commit()

        # The condition is satisfied and the rename still starts nothing, so `()` here would
        # be doubly misleading: it did not look, and looking would not have said "nothing".
        assert (result.triggered, result.reason, result.missing) == (
            False,
            None,
            None,
        )


class TestTriggeringOnUpdate:
    def test_removing_the_last_missing_event_triggers_immediately(
        self, session: orm.Session
    ) -> None:
        # The condition is judged against the state the edit leaves, not the state it found:
        # all(a, b) with only a filled becomes all(a), which is satisfied on the spot.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a")),
                now=_NOW,
            )

        assert (result.triggered, result.cycle, result.reason) == (
            True,
            0,
            None,
        )
        assert subscription.cycle == 1
        history = _history(session)
        assert len(history) == 1
        assert history[0].cycle == 0
        assert history[0].triggered_by == {"a": "em-1"}
        assert history[0].matched_events["branch"] == "all[0]"
        assert history[0].matched_events["definition"] == _definition(
            _all(_leaf("a")), name=subscription.name
        )

    def test_adding_a_missing_event_holds_the_trigger_back(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a"), _leaf("b")),
                now=_NOW,
            )

        assert (result.triggered, result.reason, result.missing) == (
            False,
            service.TriggerReason.AWAITING_EVENTS,
            ("b",),
        )
        assert _history(session) == []
        assert subscription.cycle == 0

    def test_a_trigger_clears_events_that_did_not_contribute(
        self, session: orm.Session
    ) -> None:
        # Whole-condition reset: the unchosen half of an `any` must not carry into the next
        # cycle, or a stale arrival could half-satisfy a condition nobody has seen an event for.
        subscription = _subscribe(session, condition=_any(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")
        _fill(session, subscription=subscription, event="b", emission_id="em-2")

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is True
        assert result.matched_events is not None
        assert result.matched_events["branch_events"] == ["a"]  # only the chosen child
        states = _states(session, subscription=subscription)
        assert [(row.filled_at, row.expires_at) for row in states.values()] == [
            (None, None),
            (None, None),
        ]
        # The arrivals are emptied, but which emission each row last saw survives the clear —
        # that is what lets a redelivery of a consumed emission be recognised instead of
        # refilling the row and triggering the next cycle off the same signal.
        assert sorted(row.last_emission_event_id for row in states.values()) == [
            "em-1",
            "em-2",
        ]

    def test_the_updated_definition_is_what_gets_stored(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                name="renamed",
                condition=_all(_leaf("a"), _leaf("b")),
                now=_NOW,
            )

        assert subscription.definition["condition"] == _all(_leaf("a"), _leaf("b"))
        # The column is kept in step with the blob by the write path; nothing re-derives it.
        assert subscription.name == "renamed"


class TestTheFence:
    def test_a_lost_fence_is_reported_not_raised(self, session: orm.Session) -> None:
        # Someone else triggered this cycle a moment ago. The insert collides, the SAVEPOINT rolls
        # back that row alone, and the caller's transaction is still usable.
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        session.add(db_models.TriggerHistory(subscription_id=subscription.id, cycle=0))
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert (result.triggered, result.reason, result.cycle) == (
            False,
            service.TriggerReason.CYCLE_ALREADY_TRIGGERED,
            0,
        )
        assert len(_history(session)) == 1
        # The winner cleared the event states and bumped the cycle; the loser touches neither.
        assert (
            _states(session, subscription=subscription)["a"].filled_at == _NOW - _HOUR
        )
        assert subscription.cycle == 0

    def test_a_loser_leaves_an_arrival_for_the_next_cycle_alone(
        self, session: orm.Session
    ) -> None:
        # The winner triggers for real, so its history row, its clear and its cycle bump land
        # together. A fresh arrival then fills the next cycle, and only after that does another
        # writer claim that cycle. The loser must not clear: nothing replays an emission, so a
        # clear here would strand the subscription waiting for an event that already happened.
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        with session.begin():
            assert (
                service.maybe_trigger(
                    session=session, subscription=subscription, now=_NOW
                ).triggered
                is True
            )
        assert subscription.cycle == 1

        _fill(session, subscription=subscription, event="a", emission_id="em-2")
        session.add(db_models.TriggerHistory(subscription_id=subscription.id, cycle=1))
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert (result.triggered, result.reason, result.cycle) == (
            False,
            service.TriggerReason.CYCLE_ALREADY_TRIGGERED,
            1,
        )
        state = _states(session, subscription=subscription)["a"]
        assert (state.filled_at, state.last_emission_event_id) == (
            _NOW - _HOUR,
            "em-2",
        )
        assert subscription.cycle == 1

    def test_an_update_that_loses_the_fence_still_commits_the_edit(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")
        session.add(db_models.TriggerHistory(subscription_id=subscription.id, cycle=0))
        session.commit()

        with session.begin():
            result = service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a")),
                now=_NOW,
            )

        assert (result.triggered, result.reason) == (
            False,
            service.TriggerReason.CYCLE_ALREADY_TRIGGERED,
        )
        assert subscription.definition["condition"] == _all(_leaf("a"))
        assert sorted(_states(session, subscription=subscription)) == ["a"]


def _runs(session: orm.Session) -> list[bts.PipelineRun]:
    return list(session.scalars(sqlalchemy.select(bts.PipelineRun)).all())


def _count(session: orm.Session, model: Any) -> int:
    return session.scalar(sqlalchemy.select(sqlalchemy.func.count()).select_from(model))


def _soft_delete_target(
    session: orm.Session, *, subscription: db_models.TriggerSubscription
) -> None:
    """Tombstone the pipeline the subscription points at, the way the delete route does."""
    pipeline = session.get(
        user_pipeline_db_models.UserPipeline,
        subscription.pipeline_task_spec_from_user_pipeline_id,
    )
    assert pipeline is not None
    pipeline.deleted_at = db_utils.utc_now()
    session.commit()


class TestTheFenceAndTheRunAreOneWrite:
    """Atomicity, from both sides: they commit together, or neither exists.

    The fence claims a cycle and the run is what that cycle was claimed *for*. A fence without
    a run burns the cycle on nothing and the subscription can never fire for it again; a run
    without a fence is a run nothing can trace back to why it started. Every test here breaks
    the pair in one direction and checks that both halves went.
    """

    def test_a_trigger_writes_the_fence_the_run_and_the_link(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is True
        (history,) = _history(session)
        (run,) = _runs(session)
        # The link is the join from a run back to the subscription and cycle that started it.
        assert history.pipeline_run_id == run.id
        assert result.pipeline_run_id == run.id
        assert run.created_by == subscription.created_by
        # The rest of the trigger still happened: states cleared, cycle opened for the next.
        assert _states(session, subscription=subscription)["a"].filled_at is None
        assert subscription.cycle == 1

    def test_the_run_carries_the_pipelines_provenance_annotations(
        self, session: orm.Session
    ) -> None:
        """No trigger-specific annotations, and none are needed — see the fence row.

        `create_from_pipeline_no_commit` injects the five server-owned `tangleml.com/...` keys,
        so a triggered run is not anonymous. Which *subscription* started it is not annotated on
        purpose: `trigger_history.pipeline_run_id` already joins the two, and an annotation would
        be a second copy of that fact free to drift from the first.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            service.maybe_trigger(session=session, subscription=subscription, now=_NOW)

        (run,) = _runs(session)
        assert run.annotations["tangleml.com/source/user-pipeline"] == "true"
        assert (
            run.annotations["tangleml.com/user-pipeline/pipeline-id"]
            == subscription.pipeline_task_spec_from_user_pipeline_id
        )
        assert not any(
            key.startswith("tangleml.com/trigger/") for key in run.annotations
        )

    def test_a_failure_building_the_run_takes_the_fence_with_it(
        self, session: orm.Session, monkeypatch
    ) -> None:
        """Raise inside the creator, before it writes anything.

        The same monkeypatched failure as before the backstop clause existed, and the same
        assertions about what survives -- only the exit changed. A `RuntimeError` out of the
        run build is now contained as `run_start_failed` instead of propagating, because a
        raise here aborts the fan-out for every other subscription waiting on the event. What
        the savepoint does is unaffected: the fence and the run still go together.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising("run creation failed"),
        )

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is False
        assert result.reason == service.TriggerReason.RUN_START_FAILED
        assert result.error == "RuntimeError: run creation failed"
        session.expire_all()
        assert _history(session) == []
        assert _runs(session) == []
        # The arrival is untouched, so the next attempt still has a satisfied condition.
        assert (
            _states(session, subscription=subscription)["a"].filled_at == _NOW - _HOUR
        )
        assert subscription.cycle == 0

    def test_a_failure_after_the_run_is_flushed_still_takes_everything(
        self, session: orm.Session, monkeypatch
    ) -> None:
        """The one that proves a flush is not a commit.

        `_create_in_transaction` flushes so the run gets its id, and the id is what the fence
        row links to. If a flush were durable, this would leave an orphan run and its whole
        execution graph behind.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        real = user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit

        def flush_then_fail(self, **kwargs):
            real(self, **kwargs)
            raise RuntimeError("failed after the run was flushed")

        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            flush_then_fail,
        )

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        # Contained rather than propagated (see the sibling test above); the savepoint's job is
        # unchanged, and it is the flushed run and its whole graph that have to be gone.
        assert result.reason == service.TriggerReason.RUN_START_FAILED
        session.expire_all()
        assert _history(session) == []
        assert _count(session, bts.PipelineRun) == 0
        assert _count(session, bts.ExecutionNode) == 0
        assert _count(session, bts.ArtifactNode) == 0
        assert subscription.cycle == 0

    def test_losing_the_fence_starts_no_run(self, session: orm.Session) -> None:
        """The other direction: no fence, therefore no run.

        Without this the loser of a race would start a second run off one readiness signal —
        the duplicate the fence exists to prevent.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        session.add(db_models.TriggerHistory(subscription_id=subscription.id, cycle=0))
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.reason == service.TriggerReason.CYCLE_ALREADY_TRIGGERED
        assert result.pipeline_run_id is None
        assert _runs(session) == []

    def test_a_constraint_the_run_insert_violated_is_not_a_lost_fence(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Both inserts live in one savepoint, so both raise the same error class.

        Only the fence's rejection means another writer started the run. A constraint the run
        insert violated means no run exists anywhere -- and `cycle_already_triggered` is
        deliberately outside `RUN_NOT_STARTED_REASONS`, so reporting it that way settles the
        emission a success and leaves a satisfied condition with nothing behind it. Hence
        `_FenceLost` translating the fence's error at the raise site.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising_with(
                sqlalchemy.exc.IntegrityError(
                    "INSERT INTO pipeline_run",
                    {},
                    Exception("FOREIGN KEY constraint failed"),
                )
            ),
        )

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.reason == service.TriggerReason.RUN_START_FAILED
        assert result.reason in service.RUN_NOT_STARTED_REASONS
        assert result.pipeline_run_id is None
        # The fence went back with the savepoint, and nothing marked the cycle spent -- so a
        # later arrival can still trigger it once whatever broke the run insert is fixed.
        session.expire_all()
        assert _history(session) == []
        assert _runs(session) == []
        assert subscription.cycle == 0

    def test_a_run_insert_failure_reaches_the_sink_as_a_failure(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The reason is only half of it: the fan-out has to put it in `failed`.

        That list is what the sink turns into a `runs_not_started` FAIL. Laundered into
        `cycle_already_triggered` the subscription lands in `outcomes` instead, and the
        emission settles SUCCESS with no run started.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising_with(
                sqlalchemy.exc.IntegrityError(
                    "INSERT INTO pipeline_run",
                    {},
                    Exception("FOREIGN KEY constraint failed"),
                )
            ),
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        assert fan_out.failed == [subscription.id]
        assert fan_out.deferred == []

    def test_a_disabled_subscription_starts_no_run(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        subscription.enabled = False
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.reason == service.TriggerReason.SUBSCRIPTION_DISABLED
        assert (_history(session), _runs(session)) == ([], [])


class TestADeadTargetFailsTheTriggerNotTheArrival:
    """The target was deleted after the subscription was written.

    Six things are pending when the trigger runs, and the savepoint splits them in two. The
    arrival is on the near side and commits; the fence, the run, its graph, the clear and the
    cycle bump are on the far side and vanish. That split is the whole recovery story: a
    subscription that keeps its satisfied condition can be rescued by repointing it, and one
    that loses it is stranded for good.
    """

    def test_a_soft_deleted_target_reports_the_reason_and_writes_nothing(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        _soft_delete_target(session, subscription=subscription)

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is False
        assert result.reason == service.TriggerReason.USER_PIPELINE_DELETED
        assert result.cycle == 0
        # The error travels with the result, because the sink is the only place it is recorded.
        assert result.error is not None
        assert _history(session) == []
        assert _count(session, bts.PipelineRun) == 0
        assert _count(session, bts.ExecutionNode) == 0
        assert subscription.cycle == 0

    def test_the_arrival_survives_a_dead_target(self, session: orm.Session) -> None:
        """The load-bearing assertion. Lose this fill and the recovery below is impossible.

        Driven through `record_event_and_maybe_start_runs` rather than `maybe_trigger`, and that
        is the entire point of the test. The `_fill` helper commits, so an arrival written that
        way is already durable and *no* rollback in the trigger could remove it — asserting on
        it would pass whatever the code did. On the real emission path the arrival is still
        pending in the same transaction when the trigger runs, which is the only arrangement
        where "the savepoint rolls back the fence but not the fill" means anything.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _soft_delete_target(session, subscription=subscription)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-dead",
            now=_NOW,
        )

        (outcome,) = fan_out.outcomes
        assert outcome.result.reason == service.TriggerReason.USER_PIPELINE_DELETED
        assert fan_out.failed == [subscription.id]
        session.expire_all()
        # Committed by `_subscription_transaction` on the way out, outside the savepoint.
        state = _states(session, subscription=subscription)["a"]
        assert (state.filled_at, state.last_emission_event_id) == (
            _NOW,
            "em-dead",
        )
        assert _history(session) == []
        assert _count(session, bts.PipelineRun) == 0

    def test_a_pinned_version_that_no_longer_exists_fails_the_same_way(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        # Set past the guard, the way a version deleted after pinning would leave the row.
        subscription.pipeline_task_spec_from_user_pipeline_version_key = (
            "e" * user_pipeline_db_models.DIGEST_LENGTH
        )
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.reason == service.TriggerReason.USER_PIPELINE_DELETED
        assert _history(session) == []
        assert (
            _states(session, subscription=subscription)["a"].filled_at == _NOW - _HOUR
        )

    def test_repointing_at_a_live_pipeline_recovers_the_stranded_run(
        self, session: orm.Session
    ) -> None:
        """The whole loop, end to end: strand it, then rescue it.

        This is what the target-change reversal exists for. The emission behind the arrival was
        settled FAIL and will not be redelivered, so if this PATCH did not re-evaluate there
        would be nothing left that could ever start the run.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _soft_delete_target(session, subscription=subscription)
        # Through the emission path, so the arrival has to survive on its own merits.
        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-dead",
            now=_NOW,
        )
        assert fan_out.failed == [subscription.id]

        replacement = _pipeline_id(session)
        session.commit()
        with session.begin():
            recovered = service.update_subscription(
                session=session,
                subscription=subscription,
                caller=_CALLER,
                pipeline_task_spec_from_user_pipeline_id=replacement,
                now=_NOW,
            )

        assert recovered.triggered is True
        assert recovered.pipeline_run_id is not None
        # Exactly one run: the stranded attempt left nothing behind to double up with.
        (run,) = _runs(session)
        assert run.id == recovered.pipeline_run_id
        (history,) = _history(session)
        assert (history.cycle, history.pipeline_run_id) == (0, run.id)
        assert subscription.cycle == 1

    def test_a_recovered_trigger_cannot_fire_a_second_time(
        self, session: orm.Session
    ) -> None:
        """Why the reversal is safe: a successful trigger consumes the condition.

        Repointing again finds cleared event states, so there is nothing left to fire on. This
        is the guard against a target edit becoming a way to start runs on demand.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        with session.begin():
            assert service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            ).triggered

        second_target = _pipeline_id(session)
        session.commit()
        with session.begin():
            again = service.update_subscription(
                session=session,
                subscription=subscription,
                caller=_CALLER,
                pipeline_task_spec_from_user_pipeline_id=second_target,
                now=_NOW,
            )

        assert again.triggered is False
        assert again.reason == service.TriggerReason.AWAITING_EVENTS
        assert len(_runs(session)) == 1


class TestOneDeadTargetDoesNotBlockTheOthers:
    """Fan-out: a dead target is one subscription's problem, not the emission's."""

    def test_the_live_subscriptions_still_trigger(self, session: orm.Session) -> None:
        dead = _subscribe(session, condition=_all(_leaf("a")))
        live = _subscribe(session, condition=_all(_leaf("a")))
        _soft_delete_target(session, subscription=dead)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-9", now=_NOW
        )

        by_id = {o.subscription_id: o.result for o in fan_out.outcomes}
        assert by_id[dead.id].reason == service.TriggerReason.USER_PIPELINE_DELETED
        assert by_id[live.id].triggered is True
        # Separately reported: `failed` is permanent, `deferred` is worth retrying.
        assert fan_out.failed == [dead.id]
        assert fan_out.deferred == []
        # One run, for the live one.
        assert len(_runs(session)) == 1


def _raising(message: str):
    """A stand-in for the run creator that fails before writing anything."""

    def fail(self, **kwargs):
        del self, kwargs
        raise RuntimeError(message)

    return fail


def _raising_with(error: BaseException):
    """A stand-in for the run creator that raises exactly the error it is handed."""

    def fail(self, **kwargs):
        del self, kwargs
        raise error

    return fail


# A stored spec that is present, well-formed JSON, and still not a TaskSpec: `image` is a
# string, so pydantic rejects the integer. What a spec written under an older schema, or
# written straight into the table, looks like from the trigger's side.
_UNBUILDABLE_TASK: dict[str, Any] = {
    "componentRef": {"spec": {"implementation": {"container": {"image": 5}}}}
}


def _break_target_spec(
    session: orm.Session, *, subscription: db_models.TriggerSubscription
) -> None:
    """Leave the target alive, and its current version holding a spec that will not parse.

    Deliberately not a delete: the pipeline row, the version row and the foreign key are all
    intact, so nothing before the build can notice. The failure surfaces where the run is
    actually constructed, which is the case a liveness check cannot cover.
    """
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


class TestATargetThatNoLongerBuildsFailsTheTriggerNotTheArrival:
    """The pipeline is there; what is stored on it is not a runnable spec.

    Same split as a deleted target -- arrival on the near side of the savepoint, fence and run
    on the far side -- but reached by a different exception and reported under a different
    reason, because a different person fixes it. A deleted target is repointed by whoever owns
    the subscription; a spec that no longer parses is re-saved by whoever owns the pipeline.

    Before this was contained the exception left `_trigger`, aborted the fan-out for every
    subscription waiting on the event name, and failed an emission that is settled rather than
    redelivered -- so one unparseable spec dropped the readiness signal for all of them.
    """

    def test_an_unparseable_spec_reports_the_reason_and_writes_nothing(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        _break_target_spec(session, subscription=subscription)

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is False
        assert result.reason == service.TriggerReason.TARGET_UNBUILDABLE
        assert result.cycle == 0
        # Named, because the reason alone does not say which field the spec got wrong.
        assert result.error is not None
        assert result.error.startswith("ValidationError:")
        assert _history(session) == []
        assert _count(session, bts.PipelineRun) == 0
        assert _count(session, bts.ExecutionNode) == 0
        # No cycle spent, so a re-save of the pipeline can still start this run.
        assert subscription.cycle == 0

    def test_the_arrival_survives_an_unbuildable_target(
        self, session: orm.Session
    ) -> None:
        """Driven through the emission path, where the arrival is still pending.

        The recovery story depends on this: the emission behind it settles FAIL and is never
        redelivered, so the banked arrival is the only thing left that a later re-save can
        trigger from.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _break_target_spec(session, subscription=subscription)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-unbuildable",
            now=_NOW,
        )

        (outcome,) = fan_out.outcomes
        assert outcome.result.reason == service.TriggerReason.TARGET_UNBUILDABLE
        assert fan_out.failed == [subscription.id]
        assert fan_out.deferred == []
        session.expire_all()
        state = _states(session, subscription=subscription)["a"]
        assert (state.filled_at, state.last_emission_event_id) == (
            _NOW,
            "em-unbuildable",
        )
        assert _count(session, bts.PipelineRun) == 0

    def test_a_poison_target_does_not_stop_the_subscriptions_behind_it(
        self, session: orm.Session
    ) -> None:
        """The blocker this containment was added for.

        The fan-out walks subscriptions in id order, so the poison one is put *first* on
        purpose: with the exception propagating, the healthy subscription behind it was never
        visited and never ran.
        """
        one = _subscribe(session, condition=_all(_leaf("a")))
        two = _subscribe(session, condition=_all(_leaf("a")))
        poison, healthy = sorted((one, two), key=lambda s: s.id)
        _break_target_spec(session, subscription=poison)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-poison",
            now=_NOW,
        )

        by_id = {o.subscription_id: o.result for o in fan_out.outcomes}
        assert by_id[poison.id].reason == service.TriggerReason.TARGET_UNBUILDABLE
        assert by_id[healthy.id].triggered is True
        assert fan_out.failed == [poison.id]
        assert fan_out.deferred == []
        # One run, for the healthy one -- and the poison one's rollback did not take it.
        assert len(_runs(session)) == 1


class TestAnUnexpectedFailureIsContainedNotPropagated:
    """The backstop clause: anything the run build raises that we did not predict.

    The trade is deliberate. A bug that used to be a loud crash is now a per-subscription
    `run_start_failed`, so the tests here are as much about the noise it must still make --
    a traceback in the log, the exception's class in the reason detail, a place in `failed`,
    which the sink turns into a FAIL outcome -- as about the containment itself.
    """

    def test_an_unexpected_error_reports_run_start_failed(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising("something nobody predicted"),
        )

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is False
        assert result.reason == service.TriggerReason.RUN_START_FAILED
        # The class name is the whole diagnosis for a reason that names no cause.
        assert result.error == "RuntimeError: something nobody predicted"
        assert _history(session) == []
        assert _runs(session) == []
        assert subscription.cycle == 0
        # The arrival is untouched, so a fix can still be followed by a re-evaluation.
        assert (
            _states(session, subscription=subscription)["a"].filled_at == _NOW - _HOUR
        )

    def test_it_is_logged_with_the_traceback(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Contained is not silent. Without the traceback this reason is undiagnosable."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising("something nobody predicted"),
        )

        with caplog.at_level(
            logging.ERROR, logger="cloud_pipelines_backend.triggers.service"
        ):
            with session.begin():
                service.maybe_trigger(
                    session=session, subscription=subscription, now=_NOW
                )

        (record,) = [
            r
            for r in caplog.records
            if r.name == "cloud_pipelines_backend.triggers.service"
        ]
        assert record.levelno == logging.ERROR
        assert subscription.id in record.getMessage()
        assert record.exc_info is not None

    def test_the_others_still_run_and_it_is_reported_failed(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        one = _subscribe(session, condition=_all(_leaf("a")))
        two = _subscribe(session, condition=_all(_leaf("a")))
        broken, healthy = sorted((one, two), key=lambda s: s.id)
        real = user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit

        def fail_for_the_broken_one(self, **kwargs):
            if kwargs["created_by"] == broken.created_by and kwargs["pipeline_id"] == (
                broken.pipeline_task_spec_from_user_pipeline_id
            ):
                raise RuntimeError("something nobody predicted")
            return real(self, **kwargs)

        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            fail_for_the_broken_one,
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-boom",
            now=_NOW,
        )

        by_id = {o.subscription_id: o.result for o in fan_out.outcomes}
        assert by_id[broken.id].reason == service.TriggerReason.RUN_START_FAILED
        assert by_id[healthy.id].triggered is True
        assert fan_out.failed == [broken.id]
        assert fan_out.deferred == []
        assert len(_runs(session)) == 1

    def test_a_retryable_write_failure_is_still_deferred_not_failed(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The regression the catch-all is one clause away from causing.

        A deadlock victim inside the run build says nothing about whether the run can be
        started. It has to leave `_trigger` so the fan-out defers the subscription and the sink
        retries the delivery. Swallowed into `run_start_failed` instead, it would commit the
        arrival, report a permanent failure and settle an emission that a second attempt would
        have handled -- a lock conflict lasting milliseconds becoming a trigger that never
        fires. Hence `except RETRYABLE_WRITE_FAILURES: raise` sitting above the catch-all.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising_with(
                sqlalchemy.exc.OperationalError(
                    "INSERT", {}, Exception("deadlock found")
                )
            ),
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-deadlock",
            now=_NOW,
        )

        assert fan_out.deferred == [subscription.id]
        assert fan_out.failed == []
        # Nothing committed for it either: a deferred subscription is one the sink retries
        # from the top, and an arrival left behind would make the retry a no-op.
        assert fan_out.outcomes == []
        session.expire_all()
        assert _states(session, subscription=subscription)["a"].filled_at is None

    def test_every_retryable_class_still_leaves_the_trigger(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Not just the deadlock: the whole tuple has to keep flying."""
        for index, retryable in enumerate(service.RETRYABLE_WRITE_FAILURES):
            subscription = _subscribe(session, condition=_all(_leaf("a")))
            _fill(session, subscription=subscription, event="a")
            monkeypatch.setattr(
                user_pipeline_services.UserPipelineService,
                "create_from_pipeline_no_commit",
                _raising_with(retryable("INSERT", {}, Exception(f"transient {index}"))),
            )

            with pytest.raises(retryable):
                with session.begin():
                    service.maybe_trigger(
                        session=session, subscription=subscription, now=_NOW
                    )

    def test_a_shutdown_is_not_contained(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`Exception`, never `BaseException`.

        A KeyboardInterrupt or a SystemExit is the process being asked to stop, not a
        subscription failing to trigger. Contained, it would be reported as a permanent
        `run_start_failed` for whichever subscription happened to be mid-build.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising_with(KeyboardInterrupt()),
        )

        with pytest.raises(KeyboardInterrupt):
            with session.begin():
                service.maybe_trigger(
                    session=session, subscription=subscription, now=_NOW
                )


class TestTheErrorDetailFitsInARecord:
    """`TriggerResult.error` is persisted verbatim in the emission's outcome detail.

    A pydantic ValidationError renders every failing field, so an unbuildable spec with many
    of them would otherwise write kilobytes into a row someone reads on an incident.
    """

    def test_a_short_error_is_carried_whole_with_its_class(self) -> None:
        assert service._error_detail(RuntimeError("boom")) == "RuntimeError: boom"

    def test_a_long_error_is_capped_and_marked(self) -> None:
        detail = service._error_detail(RuntimeError("x" * 5_000))

        assert len(detail) == service._ERROR_DETAIL_LIMIT
        assert detail.startswith("RuntimeError: xxx")
        assert detail.endswith("...")


class TestRollback:
    def test_a_raise_after_the_fence_undoes_the_whole_edit(
        self, session: orm.Session
    ) -> None:
        # A raise between the fence and the commit must undo the fence, or cycle 0 is burned
        # with nothing running. TestTheFenceAndTheRunAreOneWrite covers the real run failures;
        # this one is the generic case, from the update path.

        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")

        with pytest.raises(RuntimeError, match="run creation failed"):
            with session.begin():
                result = service.update_subscription(
                    session=session,
                    caller=_CALLER,
                    subscription=subscription,
                    condition=_all(_leaf("a")),
                    now=_NOW,
                )
                assert result.triggered is True
                raise RuntimeError("run creation failed")

        session.expire_all()
        assert _history(session) == []
        assert subscription.cycle == 0
        assert subscription.definition["condition"] == _all(_leaf("a"), _leaf("b"))
        states = _states(session, subscription=subscription)
        assert sorted(states) == ["a", "b"]
        assert states["a"].filled_at == _NOW - _HOUR


_ADMIN = service.Caller(name="someone-else", is_admin=True)
_STRANGER = service.Caller(name="someone-else")


class TestCreateSubscription:
    def test_stamps_created_by_from_the_caller(self, session: orm.Session) -> None:
        subscription = service.create_subscription(
            session=session,
            definition=_definition(_all(_leaf("a"))),
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert subscription.created_by == "test-owner"

    def test_the_payload_cannot_set_created_by(self, session: orm.Session) -> None:
        """Belt to the request model's braces: the blob is stored, but the column is the caller."""
        definition = _definition(_all(_leaf("a")))
        definition["created_by"] = "someone-else"
        subscription = service.create_subscription(
            session=session,
            definition=definition,
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert subscription.created_by == "test-owner"

    def test_name_column_comes_from_the_definition(self, session: orm.Session) -> None:
        subscription = service.create_subscription(
            session=session,
            definition=_definition(_all(_leaf("a")), name="weekly-report"),
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert subscription.name == "weekly-report"
        assert subscription.definition["name"] == "weekly-report"

    def test_event_states_are_opened_empty(self, session: orm.Session) -> None:
        subscription = service.create_subscription(
            session=session,
            definition=_definition(_all(_leaf("a"), _leaf("b"))),
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        states = session.scalars(
            sqlalchemy.select(db_models.TriggerEventState).where(
                db_models.TriggerEventState.subscription_id == subscription.id
            )
        ).all()
        assert {state.event_name for state in states} == {"a", "b"}
        assert all(state.filled_at is None for state in states)

    def test_a_new_subscription_starts_at_cycle_zero_and_has_not_triggered(
        self, session: orm.Session
    ) -> None:
        subscription = service.create_subscription(
            session=session,
            definition=_definition(_all(_leaf("a"))),
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert subscription.cycle == 0
        assert (
            session.scalars(
                sqlalchemy.select(db_models.TriggerHistory).where(
                    db_models.TriggerHistory.subscription_id == subscription.id
                )
            ).all()
            == []
        )

    def test_a_malformed_condition_writes_nothing(self, session: orm.Session) -> None:
        with pytest.raises(ValueError):
            service.create_subscription(
                session=session,
                definition=_definition({"op": "some", "children": [_leaf("a")]}),
                pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
                caller=service.Caller(name="test-owner"),
            )
        session.rollback()
        assert (
            session.scalars(sqlalchemy.select(db_models.TriggerSubscription)).all()
            == []
        )


class TestWriteAuthorization:
    """Admins first, then the creator. Read paths are not guarded and are not tested here."""

    def test_the_creator_may_update(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        result = service.update_subscription(
            session=session,
            caller=service.Caller(name="test-owner"),
            subscription=subscription,
            condition=_all(_leaf("b")),
            now=_NOW,
        )
        session.commit()
        assert result.triggered is False

    def test_an_admin_may_update_someone_elses(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        service.update_subscription(
            session=session,
            caller=_ADMIN,
            subscription=subscription,
            condition=_all(_leaf("b")),
            now=_NOW,
        )
        session.commit()
        assert subscription.definition["condition"] == _all(_leaf("b"))

    def test_a_stranger_may_not_update(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        with pytest.raises(service.NotAuthorized):
            service.update_subscription(
                session=session,
                caller=_STRANGER,
                subscription=subscription,
                condition=_all(_leaf("b")),
                now=_NOW,
            )

    def test_a_refused_update_changes_nothing(self, session: orm.Session) -> None:
        """The guard runs before validation and before any write, so the edit leaves no trace."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        with pytest.raises(service.NotAuthorized):
            service.update_subscription(
                session=session,
                caller=_STRANGER,
                subscription=subscription,
                condition=_all(_leaf("b")),
                now=_NOW,
            )
        session.rollback()
        assert subscription.definition["condition"] == _all(_leaf("a"))
        states = session.scalars(
            sqlalchemy.select(db_models.TriggerEventState.event_name).where(
                db_models.TriggerEventState.subscription_id == subscription.id
            )
        ).all()
        assert set(states) == {"a"}

    def test_the_message_names_the_creator_and_the_caller(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        with pytest.raises(
            service.NotAuthorized,
            match="created by test-owner, not someone-else",
        ):
            service.delete_subscription(
                session=session, subscription=subscription, caller=_STRANGER
            )

    def test_a_stranger_may_not_delete(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        with pytest.raises(service.NotAuthorized):
            service.delete_subscription(
                session=session, subscription=subscription, caller=_STRANGER
            )
        session.rollback()
        assert session.get(db_models.TriggerSubscription, subscription.id) is not None


class TestDeleteSubscription:
    def test_the_creator_may_delete(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        service.delete_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert session.get(db_models.TriggerSubscription, subscription.id) is None

    def test_an_admin_may_delete_someone_elses(self, session: orm.Session) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        service.delete_subscription(
            session=session, subscription=subscription, caller=_ADMIN
        )
        session.commit()
        assert session.get(db_models.TriggerSubscription, subscription.id) is None

    def test_event_states_go_with_it(self, fk_session: orm.Session) -> None:
        """The schema's half of the promise: on an engine that enforces foreign keys, the
        ON DELETE CASCADE is the thing that removes them. fk_session is that engine."""
        session = fk_session
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        subscription_id = subscription.id
        service.delete_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert (
            session.scalars(
                sqlalchemy.select(db_models.TriggerEventState).where(
                    db_models.TriggerEventState.subscription_id == subscription_id
                )
            ).all()
            == []
        )

    def test_event_states_go_with_it_where_the_cascade_is_inert(
        self, session: orm.Session
    ) -> None:
        """The code's half of the promise, and the regression this class exists for.

        SQLite enforces foreign keys only on a connection that has run
        `PRAGMA foreign_keys=ON`, and the engine factory does not set it. This fixture is that
        unenforcing engine — the shape any SQLite-backed deployment actually runs — so if the
        delete ever goes back to relying on the cascade, the states are orphaned and this
        goes red while the fk_session test above stays green.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        subscription_id = subscription.id
        assert _states_of(session, subscription_id) == ["a", "b"]
        service.delete_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert _states_of(session, subscription_id) == []

    def test_only_this_subscription_loses_its_states(
        self, session: orm.Session
    ) -> None:
        """The explicit delete is filtered, not a table sweep."""
        doomed = _subscribe(session, condition=_all(_leaf("a")))
        bystander = db_models.TriggerSubscription(
            name=f"{doomed.name}-bystander",
            definition=_definition(_all(_leaf("a"))),
            created_by=doomed.created_by,
            pipeline_task_spec_from_user_pipeline_id=doomed.pipeline_task_spec_from_user_pipeline_id,
        )
        session.add(bystander)
        session.flush()
        event_state.sync(
            session=session,
            subscription_id=bystander.id,
            condition=_all(_leaf("a")),
        )
        session.commit()
        bystander_id = bystander.id

        service.delete_subscription(
            session=session,
            subscription=doomed,
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        assert _states_of(session, bystander_id) == ["a"]

    def test_history_outlives_the_subscription(self, session: orm.Session) -> None:
        """trigger_history has no foreign key, so the record of what ran survives the delete."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        subscription_id = subscription.id
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription_id,
                cycle=0,
                matched_events={"events": ["a"]},
                triggered_by={"a": "emission-1"},
            )
        )
        session.commit()
        service.delete_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
        )
        session.commit()
        history = session.scalars(
            sqlalchemy.select(db_models.TriggerHistory).where(
                db_models.TriggerHistory.subscription_id == subscription_id
            )
        ).all()
        assert len(history) == 1
        assert history[0].matched_events == {"events": ["a"]}


class TestTheDefinitionKeysSurviveEveryWrite:
    """The blob's two keys, spelled the way they are spelled on disk.

    `DefinitionKey` is what the tree indexes the blob with, so a respelling of a member would
    move every reader and writer together and nothing would notice — until the rows already
    written stopped being readable. These assertions use the literal strings on purpose: they
    are the one place the storage contract is pinned rather than followed.
    """

    def test_the_enum_spells_the_keys_the_blob_is_stored_under(self) -> None:
        assert db_models.DefinitionKey.NAME == "name"
        assert db_models.DefinitionKey.CONDITION == "condition"
        assert {key.value for key in db_models.DefinitionKey} == {
            "name",
            "condition",
        }

    def test_create_stores_those_two_keys_and_no_others(
        self, session: orm.Session
    ) -> None:
        subscription = service.create_subscription(
            session=session,
            definition=_definition(_all(_leaf("a")), name="weekly-report"),
            pipeline_task_spec_from_user_pipeline_id=_pipeline_id(session),
            caller=_CALLER,
        )
        session.commit()

        assert set(subscription.definition) == {"name", "condition"}
        assert subscription.definition["name"] == "weekly-report"
        assert subscription.definition["condition"] == _all(_leaf("a"))

    def test_evaluation_reads_the_condition_out_of_the_blob(
        self, session: orm.Session
    ) -> None:
        """Read side: nothing caches the condition, so the literal key is the whole contract."""
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")
        # Edited under the literal key, behind the service's back — if maybe_trigger read the
        # condition from anywhere else, this would still be waiting on b.
        subscription.definition = {
            **subscription.definition,
            "condition": _all(_leaf("a")),
        }
        session.commit()

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is True

    def test_editing_the_condition_leaves_the_name_key_alone(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(
            session, condition=_all(_leaf("a")), name="left-alone"
        )

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("b")),
                now=_NOW,
            )

        assert set(subscription.definition) == {"name", "condition"}
        assert subscription.definition["condition"] == _all(_leaf("b"))
        assert subscription.definition["name"] == "left-alone"

    def test_renaming_leaves_the_condition_key_alone(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a")))

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                name="renamed",
                now=_NOW,
            )

        assert set(subscription.definition) == {"name", "condition"}
        assert subscription.definition["name"] == "renamed"
        assert subscription.definition["condition"] == _all(_leaf("a"))
        # The column and the blob move together, the way create sets them.
        assert subscription.name == "renamed"

    def test_an_edited_blob_reloads_with_plain_string_keys(
        self, session: orm.Session
    ) -> None:
        """An enum member is a fine dict key in memory; JSON has no enums, so the row cannot."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                name="renamed",
                condition=_all(_leaf("b")),
                now=_NOW,
            )
        session.expire_all()

        assert [type(key) for key in subscription.definition] == [str, str]

    def test_a_history_snapshot_carries_the_same_two_keys(
        self, session: orm.Session
    ) -> None:
        """The snapshot is the blob, so the keys a reader of an old row needs are these two."""
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            service.update_subscription(
                session=session,
                caller=_CALLER,
                subscription=subscription,
                condition=_all(_leaf("a")),
                now=_NOW,
            )

        snapshot = _history(session)[0].matched_events["definition"]
        assert set(snapshot) == {"name", "condition"}
        assert snapshot["condition"] == _all(_leaf("a"))


class TestTheNaturalKeyCollisionCheck:
    """`_is_name_collision` decides 409-or-500, so it has to be right in both directions.

    The route tests cover the true case end to end. What they cannot reach is the false case:
    every other integrity failure this table could ever raise must keep propagating, because
    reporting an unrelated constraint as "you already have a subscription named X" sends the
    caller off renaming things to fix a problem that has nothing to do with the name.

    Constructed by hand rather than provoked, because the point is to test wordings this
    dialect does not produce — the MySQL form never appears in a SQLite run.
    """

    @staticmethod
    def _integrity_error(message: str) -> sqlalchemy.exc.IntegrityError:
        return sqlalchemy.exc.IntegrityError("INSERT ...", {}, Exception(message))

    def test_it_recognizes_the_sqlite_wording(self) -> None:
        error = self._integrity_error(
            "UNIQUE constraint failed: trigger_subscription.created_by, trigger_subscription.name"
        )
        assert service._is_name_collision(error=error) is True

    def test_it_recognizes_the_mysql_wording(self) -> None:
        """Production's wording, which names the key and never lists the columns."""
        error = self._integrity_error(
            "(1062, \"Duplicate entry 'alice-nightly' for key 'uq_trigger_subscription_created_by_name'\")"
        )
        assert service._is_name_collision(error=error) is True

    def test_a_different_unique_constraint_is_not_a_name_collision(
        self,
    ) -> None:
        """The fence. Misreporting it as a duplicate name would be a 409 on a trigger race."""
        error = self._integrity_error(
            "UNIQUE constraint failed: trigger_history.subscription_id, trigger_history.cycle"
        )
        assert service._is_name_collision(error=error) is False

    def test_a_not_null_violation_on_this_table_is_not_a_name_collision(
        self,
    ) -> None:
        """Same table, and it names one of the two columns — but only one."""
        error = self._integrity_error(
            "NOT NULL constraint failed: trigger_subscription.created_by"
        )
        assert service._is_name_collision(error=error) is False

    def test_the_needles_are_derived_from_the_constraint_not_restated(
        self,
    ) -> None:
        """The check reads the constraint object, so renaming it cannot strand the check.

        Asserted against the live table rather than a literal: if someone changes the
        constraint's columns, this fails here rather than silently downgrading every future
        collision to a 500.
        """
        constraint = db_models.TRIGGER_SUBSCRIPTION_USER_NAME_CONSTRAINT
        assert constraint.name == "uq_trigger_subscription_created_by_name"
        assert [column.name for column in constraint.columns] == [
            "created_by",
            "name",
        ]
        assert constraint in db_models.TriggerSubscription.__table__.constraints

    def test_an_unrelated_integrity_error_propagates_rather_than_becoming_a_409(
        self, session: orm.Session
    ) -> None:
        """The `raise` arm of the guard, driven through a real flush of a real violation.

        The fence, `uq_trigger_history_cycle`, is the other unique constraint this schema has,
        so it is the honest stand-in for "some constraint that is not the natural key".
        """
        for _ in range(2):
            session.add(
                db_models.TriggerHistory(
                    subscription_id="sub-1",
                    cycle=0,
                    matched_events={"events": []},
                    triggered_by={},
                )
            )
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            service._flush_or_name_taken(session=session, name="nightly")


class TestTheFlushSitsOnEveryPath:
    """`_flush_or_name_taken` is called once, unconditionally, and both of its jobs need that.

    It is tempting to move it inside `if name is not None`, since the error it raises is about
    a name. That would be wrong. The sessions this service runs under have `autoflush=False`,
    so the same flush is also what puts `event_state.sync`'s writes on the wire before
    `maybe_trigger` reads them back — and a condition-only edit carries no name at all.

    One test per shape an edit can take, so the gating cannot come back unnoticed. All of them
    take `no_autoflush_session` rather than the shared `session` fixture: under autoflush the
    missing flush is supplied for free by the next ORM SELECT and every one of these passes
    against code that never flushes at all.
    """

    def test_a_condition_only_edit_reaches_the_database_before_it_is_evaluated(
        self, no_autoflush_session: orm.Session
    ) -> None:
        """The discriminating case: no name, so a flush gated on `name` never runs.

        The widened window is the tell. `maybe_trigger` decides on what a SELECT returns, so an
        unflushed `sync` leaves it reading the old `expires_at`, declining, and reporting the
        arrival as still missing — the very evaluation the edit existed to prompt.
        """
        subscription = _subscribe(
            no_autoflush_session, condition=_all(_leaf("a", expire_seconds=60))
        )
        _fill(
            no_autoflush_session,
            subscription=subscription,
            event="a",
            at=_NOW - _HOUR,
        )
        assert (
            service.maybe_trigger(
                session=no_autoflush_session,
                subscription=subscription,
                now=_NOW,
            ).triggered
            is False
        ), "precondition: the 60s window lapsed an hour before _NOW"

        result = service.update_subscription(
            session=no_autoflush_session,
            caller=_CALLER,
            subscription=subscription,
            condition=_all(_leaf("a", expire_seconds=86400)),
            now=_NOW,
        )
        no_autoflush_session.commit()

        assert result.triggered is True

    def test_a_name_only_edit_refuses_a_name_this_caller_already_holds(
        self, no_autoflush_session: orm.Session
    ) -> None:
        """The 409 half, on the path with no condition to sync."""
        subscription = _subscribe(no_autoflush_session, condition=_all(_leaf("a")))
        no_autoflush_session.add(
            db_models.TriggerSubscription(
                name="taken",
                definition=_definition(_all(_leaf("z")), name="taken"),
                created_by="test-owner",
                pipeline_task_spec_from_user_pipeline_id=_pipeline_id(
                    no_autoflush_session
                ),
            )
        )
        no_autoflush_session.commit()

        with pytest.raises(service.NameTaken):
            service.update_subscription(
                session=no_autoflush_session,
                caller=_CALLER,
                subscription=subscription,
                name="taken",
                now=_NOW,
            )

    def test_a_condition_edit_carrying_a_rename_refuses_a_taken_name(
        self, no_autoflush_session: orm.Session
    ) -> None:
        """Both halves in one call, which is the shape a PATCH with every field set produces."""
        subscription = _subscribe(no_autoflush_session, condition=_all(_leaf("a")))
        no_autoflush_session.add(
            db_models.TriggerSubscription(
                name="taken",
                definition=_definition(_all(_leaf("z")), name="taken"),
                created_by="test-owner",
                pipeline_task_spec_from_user_pipeline_id=_pipeline_id(
                    no_autoflush_session
                ),
            )
        )
        no_autoflush_session.commit()

        with pytest.raises(service.NameTaken):
            service.update_subscription(
                session=no_autoflush_session,
                caller=_CALLER,
                subscription=subscription,
                name="taken",
                condition=_all(_leaf("a"), _leaf("b")),
                now=_NOW,
            )

    def test_an_enabled_only_edit_is_written_before_the_call_returns(
        self, no_autoflush_session: orm.Session
    ) -> None:
        """Nothing to collide and nothing to sync, and the flush still runs.

        Read back through raw SQL rather than the ORM. With `autoflush=False` a pending change
        is invisible to a SELECT, so seeing the new value here is evidence the flush happened —
        not merely evidence the attribute was assigned.
        """
        subscription = _subscribe(no_autoflush_session, condition=_all(_leaf("a")))

        service.update_subscription(
            session=no_autoflush_session,
            caller=_CALLER,
            subscription=subscription,
            enabled=False,
            now=_NOW,
        )
        stored = no_autoflush_session.execute(
            sqlalchemy.text("SELECT enabled FROM trigger_subscription WHERE id = :id"),
            {"id": subscription.id},
        ).scalar_one()
        no_autoflush_session.rollback()

        assert stored == 0

    def test_an_edit_that_changes_nothing_emits_no_sql(
        self, no_autoflush_session: orm.Session
    ) -> None:
        """Why the flush can afford to be unconditional: with nothing dirty it is not a trip."""
        subscription = _subscribe(no_autoflush_session, condition=_all(_leaf("a")))
        # Zero the meter. `_subscribe` commits and `expire_on_commit` leaves every attribute
        # expired, so the first read inside the call would lazy-load the row and be counted as
        # a statement this edit sent — one refetch the production code never asked for.
        no_autoflush_session.refresh(subscription)

        statements: list[str] = []

        def record(
            conn, cursor, statement, parameters, context, executemany
        ):  # noqa: ANN001
            statements.append(statement)

        engine = no_autoflush_session.get_bind()
        sqlalchemy.event.listen(engine, "before_cursor_execute", record)
        try:
            result = service.update_subscription(
                session=no_autoflush_session,
                caller=_CALLER,
                subscription=subscription,
                now=_NOW,
            )
        finally:
            sqlalchemy.event.remove(engine, "before_cursor_execute", record)

        assert statements == []
        assert result.triggered is False


class TestReEvaluatingOnlyWhenTheAnswerCouldHaveChanged:
    """Which edits reach `maybe_trigger` at all, and which are inert by construction.

    Two things can change whether the condition holds: the condition itself, and an off->on
    toggle that lifts the refusal to trigger. Everything else is inert, and an inert edit that
    started a pipeline run would be a surprising thing for a rename or a re-save to do.

    `reason is None` is the sharp assertion throughout: `maybe_trigger` always names a reason
    when it declines, so a null reason means it was never consulted.
    """

    def test_a_changed_condition_re_evaluates(self, session: orm.Session) -> None:
        """The positive control: dropping the last unfilled event triggers on the spot."""
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            condition=_all(_leaf("a")),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is True

    def test_resending_the_stored_condition_does_not_re_evaluate(
        self, session: orm.Session
    ) -> None:
        """A PATCH that rewrites nothing must not start a run, even when one is available."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            condition=_all(_leaf("a")),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is False
        assert result.reason is None
        assert _history(session) == []

    def test_a_rename_alone_does_not_re_evaluate(self, session: orm.Session) -> None:
        """An unrelated metadata edit cannot change whether the condition holds."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            name="renamed",
            now=_NOW,
        )
        session.commit()

        assert result.triggered is False
        assert result.reason is None
        assert _history(session) == []

    def test_switching_enabled_back_on_re_evaluates(self, session: orm.Session) -> None:
        """The one metadata edit that must evaluate: nothing else will ever prompt it again."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        subscription.enabled = False
        session.commit()
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            enabled=True,
            now=_NOW,
        )
        session.commit()

        assert result.triggered is True

    def test_switching_enabled_back_on_with_nothing_filled_declines(
        self, session: orm.Session
    ) -> None:
        """The same edit against an unsatisfied condition: evaluated, and it says no.

        Together with the test above this pins the off->on branch to *evaluating* rather than
        to triggering. A mutant that fired unconditionally on the toggle would pass that one
        and fail here, and `reason` is what separates them: a named decline means
        `maybe_trigger` was consulted and answered, where the null of an inert edit means it
        was never asked.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        subscription.enabled = False
        session.commit()

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            enabled=True,
            now=_NOW,
        )
        session.commit()

        assert (result.triggered, result.reason, result.missing) == (
            False,
            service.TriggerReason.AWAITING_EVENTS,
            ("a",),
        )
        assert _history(session) == []

    def test_switching_enabled_off_does_not_re_evaluate(
        self, session: orm.Session
    ) -> None:
        """It would only reach `maybe_trigger` to be turned away by its disabled guard."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            enabled=False,
            now=_NOW,
        )
        session.commit()

        assert result.triggered is False
        assert result.reason is None

    def test_disabling_something_already_disabled_does_not_re_evaluate(
        self, session: orm.Session
    ) -> None:
        """The cell that pins the `enabled and` half of the guard.

        Without it the guard could read `not was_enabled` alone and every test still pass:
        on->off stays inert either way, so only an edit that starts *and* ends disabled tells
        the two apart. It would reach `maybe_trigger` just to be turned away as disabled.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        subscription.enabled = False
        session.commit()
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            enabled=False,
            now=_NOW,
        )
        session.commit()

        assert result.triggered is False
        assert result.reason is None

    def test_enabling_something_already_enabled_does_not_re_evaluate(
        self, session: orm.Session
    ) -> None:
        """It is the transition that matters, not the value the edit leaves behind."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            enabled=True,
            now=_NOW,
        )
        session.commit()

        assert result.triggered is False
        assert result.reason is None

    def test_reordered_children_count_as_a_change(self, session: orm.Session) -> None:
        """Blobs are compared, not conditions.

        `all(a, b)` and `all(b, a)` mean the same thing and compare unequal, so this edit
        re-evaluates when it strictly need not have. That is the safe direction to be wrong in:
        the opposite mistake would skip a re-check that was due.
        """
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")
        _fill(session, subscription=subscription, event="b")

        result = service.update_subscription(
            session=session,
            caller=_CALLER,
            subscription=subscription,
            condition=_all(_leaf("b"), _leaf("a")),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is True


class TestATargetOrPinChangeReEvaluates:
    """A reversal. This class used to be `TestAPinChangeDoesNotReEvaluate`.

    The old rule was "what, not whether": a pin decides which version a satisfied condition
    launches and cannot make an unsatisfied condition hold, so re-checking looked like work
    with no possible new answer. The reasoning was right and the conclusion was wrong, because
    of the case it did not cover — a target deleted out from under a subscription whose
    condition has *already* been satisfied. `_trigger` rolls the fence and the run back to the
    savepoint and leaves the arrival committed, the emission is settled FAIL with no
    redelivery, and repointing the subscription is then the only thing left that can start the
    run. Refusing to re-evaluate strands it permanently.

    It cannot fire twice: a successful trigger clears the event states in the transaction that
    claims the cycle, so a target edit only ever finds a satisfied condition that was never
    consumed.
    """

    def _pinnable(self, session: orm.Session) -> tuple[str, str]:
        """A FULL-mode pipeline with one immutable version, and that version."""
        pipeline_id = _pipeline_id(session)
        version = "a" * user_pipeline_db_models.DIGEST_LENGTH
        session.add(
            user_pipeline_db_models.UserPipelineVersion(
                pipeline_id=pipeline_id,
                version_key=version,
                content_digest=version,
                root_pipeline_task=_RUNNABLE_TASK,
            )
        )
        session.flush()
        return pipeline_id, version

    def test_pinning_a_satisfied_subscription_triggers_it(
        self, session: orm.Session
    ) -> None:
        """The condition holds throughout, so re-evaluating starts a run here."""
        subscription = _subscribe(session, condition=_leaf("a"))
        _fill(session, subscription=subscription, event="a")
        pipeline_id, version = self._pinnable(session)

        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            pin_edit=(True, version),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is True
        assert result.pipeline_run_id is not None
        assert len(_history(session)) == 1
        assert subscription.pipeline_task_spec_from_user_pipeline_version_key == version

    def test_unpinning_a_satisfied_subscription_triggers_it(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_leaf("a"))
        pipeline_id, version = self._pinnable(session)
        subscription.pipeline_task_spec_from_user_pipeline_id = pipeline_id
        subscription.pipeline_task_spec_from_user_pipeline_version_key = version
        session.commit()
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            pin_edit=(True, None),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is True
        assert len(_history(session)) == 1
        assert subscription.pipeline_task_spec_from_user_pipeline_version_key is None

    def test_resending_the_stored_target_and_pin_does_not_re_evaluate(
        self, session: orm.Session
    ) -> None:
        """The guard on the reversal: a no-op PATCH must not start a run.

        A client that reads a subscription, edits one unrelated field and PATCHes the whole
        object back resends the target and the pin every time. Gating on "the caller mentioned
        it" rather than "the value changed" would turn every such rename into a trigger.
        """
        subscription = _subscribe(session, condition=_leaf("a"))
        pipeline_id, version = self._pinnable(session)
        subscription.pipeline_task_spec_from_user_pipeline_id = pipeline_id
        subscription.pipeline_task_spec_from_user_pipeline_version_key = version
        session.commit()
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            name="renamed-but-otherwise-identical",
            pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            pin_edit=(True, version),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is False
        assert _history(session) == []

    def test_a_pin_change_alongside_a_condition_change_still_re_evaluates(
        self, session: orm.Session
    ) -> None:
        """Two reasons to re-check are still one re-check, and one run."""
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        _fill(session, subscription=subscription, event="a")
        pipeline_id, version = self._pinnable(session)

        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            condition=_leaf("a"),
            pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            pin_edit=(True, version),
            now=_NOW,
        )
        session.commit()

        assert result.triggered is True
        assert len(_history(session)) == 1


class TestFill:
    """The event-state write: which emission arrived, when, and when it goes stale."""

    def test_an_arrival_stamps_the_emission_the_time_and_the_expiry(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_leaf("a", expire_seconds=600))
        state = _states(session, subscription=subscription)["a"]

        event_state.fill(state=state, emission_event_id="em-7", now=_NOW)

        assert state.last_emission_event_id == "em-7"
        assert state.filled_at == _NOW
        assert state.expires_at == _NOW + datetime.timedelta(seconds=600)

    def test_no_expiry_means_no_expires_at(self, session: orm.Session) -> None:
        # NULL expire_seconds is the default, and an arrival against it never goes stale —
        # so freshness is decided by the column being NULL, not by a sentinel far-future date.
        subscription = _subscribe(session, condition=_leaf("a"))
        state = _states(session, subscription=subscription)["a"]

        event_state.fill(state=state, emission_event_id="em-7", now=_NOW)

        assert state.filled_at == _NOW
        assert state.expires_at is None

    def test_latest_wins_and_the_expiry_window_moves_with_it(
        self, session: orm.Session
    ) -> None:
        # A redelivered or simply newer emission overwrites the older arrival rather than
        # being dropped: an event refreshed at 12:00 is fresh until 12:10 even though the
        # first arrival's window closed at 11:10.
        subscription = _subscribe(session, condition=_leaf("a", expire_seconds=600))
        state = _states(session, subscription=subscription)["a"]
        event_state.fill(state=state, emission_event_id="em-1", now=_NOW - _HOUR)

        event_state.fill(state=state, emission_event_id="em-2", now=_NOW)

        assert state.last_emission_event_id == "em-2"
        assert state.filled_at == _NOW
        assert state.expires_at == _NOW + datetime.timedelta(seconds=600)

    def test_an_arrival_with_no_id_leaves_the_column_null(
        self, session: orm.Session
    ) -> None:
        # The correlation id is optional; a missing one is recorded as missing rather than
        # invented, and the arrival itself still counts.
        subscription = _subscribe(session, condition=_leaf("a"))
        state = _states(session, subscription=subscription)["a"]

        event_state.fill(state=state, emission_event_id=None, now=_NOW)

        assert state.filled_at == _NOW
        assert state.last_emission_event_id is None

    def test_the_write_is_the_callers_to_commit(self, session: orm.Session) -> None:
        # fill() never commits, which is what lets the sink batch the write with the fence
        # and the run creation in one transaction — and roll all three back together.
        subscription = _subscribe(session, condition=_leaf("a"))
        state = _states(session, subscription=subscription)["a"]

        event_state.fill(state=state, emission_event_id="em-1", now=_NOW)
        session.rollback()

        assert _states(session, subscription=subscription)["a"].filled_at is None

    def test_a_filled_event_is_live_and_an_expired_one_is_not(
        self, session: orm.Session
    ) -> None:
        # The write and the read agree: what fill() stamps is what events_emitted() returns.
        subscription = _subscribe(
            session,
            condition=_all(
                _leaf("fresh", expire_seconds=600),
                _leaf("stale", expire_seconds=60),
            ),
        )
        states = _states(session, subscription=subscription)
        event_state.fill(state=states["fresh"], emission_event_id="em-1", now=_NOW)
        event_state.fill(
            state=states["stale"], emission_event_id="em-2", now=_NOW - _HOUR
        )
        session.flush()

        assert event_state.events_emitted(
            session=session, subscription_id=subscription.id, now=_NOW
        ) == {"fresh": "em-1"}


class TestFillRefusesAnArrivalItHasMovedPast:
    """The dedup is monotonic, not an equality check against one remembered id.

    Equality alone is a single-slot memory, and two interleaved emissions defeat it. These pin
    the ordering that closes that, including the delayed-redelivery interleaving that an `==`
    check waves straight through.
    """

    def test_the_same_emission_twice_is_still_a_no_op(
        self, session: orm.Session
    ) -> None:
        # The case `==` already covered, kept so tightening the comparison cannot loosen this.
        subscription = _subscribe(session, condition=_leaf("a", expire_seconds=600))
        state = _states(session, subscription=subscription)["a"]
        event_state.fill(state=state, emission_event_id="em-2", now=_NOW - _HOUR)

        assert (
            event_state.fill(state=state, emission_event_id="em-2", now=_NOW) is False
        )
        assert state.filled_at == _NOW - _HOUR

    def test_an_older_emission_arriving_late_is_refused(
        self, session: orm.Session
    ) -> None:
        # The case `==` missed entirely: not the same emission, but one this row has already
        # moved past, so there is nothing new to evaluate.
        subscription = _subscribe(session, condition=_leaf("a", expire_seconds=600))
        state = _states(session, subscription=subscription)["a"]
        event_state.fill(state=state, emission_event_id="em-2", now=_NOW)

        assert (
            event_state.fill(state=state, emission_event_id="em-1", now=_NOW + _HOUR)
            is False
        )
        assert state.last_emission_event_id == "em-2"

    def test_a_refused_arrival_does_not_drag_the_expiry_window_back(
        self, session: orm.Session
    ) -> None:
        # The second harm, and the quieter one. A straggler refilling the row would move
        # filled_at and expires_at *backwards*, ageing out an arrival that is genuinely live.
        subscription = _subscribe(session, condition=_leaf("a", expire_seconds=600))
        state = _states(session, subscription=subscription)["a"]
        event_state.fill(state=state, emission_event_id="em-2", now=_NOW)

        event_state.fill(state=state, emission_event_id="em-1", now=_NOW - _HOUR)

        assert state.filled_at == _NOW
        assert state.expires_at == _NOW + datetime.timedelta(seconds=600)

    def test_a_delayed_redelivery_after_a_newer_emission_starts_no_second_cycle(
        self, session: orm.Session
    ) -> None:
        # The whole interleaving, end to end and through the service rather than fill alone:
        # em-1 arrives and triggers, the trigger clears the row, em-2 arrives, and only then
        # does em-1 come back. With an equality check em-1 reads as fresh, because the slot
        # now holds em-2 — and the fence cannot help, since the cycle has already moved on.
        subscription = _subscribe(session, condition=_leaf("a"))

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-2",
            now=_NOW + _HOUR,
        )
        cycles_before = len(_history(session))

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-1",
            now=_NOW + 2 * _HOUR,
        )

        # Refused, and named: em-1 is a straggler here, but it is a straggler that already
        # triggered cycle 0, so the report says so rather than claiming nothing happened.
        assert [arrival.result.reason for arrival in fan_out.outcomes] == [
            service.TriggerReason.RUN_ALREADY_STARTED
        ]
        assert len(_history(session)) == cycles_before
        # And the row still points at the newer emission, not the straggler.
        assert (
            _states(session, subscription=subscription)["a"].last_emission_event_id
            == "em-2"
        )

    def test_a_first_arrival_against_an_empty_row_is_accepted(
        self, session: orm.Session
    ) -> None:
        # Nothing to be older than. The guard needs the stored id to be non-NULL before it can
        # compare, and a row that has never been filled must not swallow its first arrival.
        subscription = _subscribe(session, condition=_leaf("a"))
        state = _states(session, subscription=subscription)["a"]

        assert event_state.fill(state=state, emission_event_id="em-1", now=_NOW) is True

    def test_an_arrival_with_no_id_is_never_refused(self, session: orm.Session) -> None:
        # An arrival carrying no id cannot be ordered against anything, so it is taken at face
        # value rather than compared — the same answer the equality check gave.
        subscription = _subscribe(session, condition=_leaf("a"))
        state = _states(session, subscription=subscription)["a"]
        event_state.fill(state=state, emission_event_id="em-9", now=_NOW - _HOUR)

        assert event_state.fill(state=state, emission_event_id=None, now=_NOW) is True
        assert state.filled_at == _NOW

    def test_a_newer_emission_after_a_refusal_still_lands(
        self, session: orm.Session
    ) -> None:
        # Refusing a straggler must not wedge the row: the next genuinely new emission is
        # still taken, so one late redelivery cannot deafen an event permanently.
        subscription = _subscribe(session, condition=_leaf("a"))
        state = _states(session, subscription=subscription)["a"]
        event_state.fill(state=state, emission_event_id="em-2", now=_NOW)
        event_state.fill(state=state, emission_event_id="em-1", now=_NOW)

        assert (
            event_state.fill(state=state, emission_event_id="em-3", now=_NOW + _HOUR)
            is True
        )
        assert state.last_emission_event_id == "em-3"


class TestEmissionIdsSortInTimeOrder:
    """`fill` orders arrivals by the id's timestamp prefix, which is the only ordered part.

    The coupling is deliberate but invisible from `fill` itself, so it is pinned here: if the id
    scheme ever stops being time-ordered, this goes red next to the code that depends on it
    rather than silently letting stragglers back in.

    The last test pins the *limit* of that ordering — the random tail — because that is the fact
    the comparison in `fill` is built around.
    """

    def test_a_later_id_sorts_after_an_earlier_one(self) -> None:
        earlier = bts.generate_unique_id()
        time.sleep(0.002)
        later = bts.generate_unique_id()

        assert earlier < later

    def test_ids_are_fixed_width_so_the_compare_is_not_ragged(self) -> None:
        # Ordering by string only tracks ordering by time while every id is the same length:
        # a shorter id would sort before a longer one whatever their timestamps say.
        ids = [bts.generate_unique_id() for _ in range(50)]

        assert {len(one) for one in ids} == {db_utils.ID_LENGTH}

    def test_the_time_prefix_is_what_orders_them(self) -> None:
        # Not just "these two happened to sort": the leading hex chars are the millisecond
        # epoch, and that prefix is what carries the order.
        prefix = db_utils.ID_MS_PREFIX_LENGTH
        one = bts.generate_unique_id()
        two = bts.generate_unique_id()

        assert one[:prefix] <= two[:prefix]
        assert int(one[:prefix], 16) > 0

    def test_two_ids_from_one_millisecond_can_sort_the_wrong_way(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The half of the id that is *not* ordered, and the reason `fill` never compares whole
        # ids to decide which arrival came first. The tail is os.urandom(4), so two ids minted
        # in one millisecond sort by random bytes -- here the later id sorts lower.
        #
        # The randomness is pinned rather than sampled: the real misordering is a coin flip, and
        # a test that waits for the coin to land wrong is a flaky test.
        prefix = db_utils.ID_MS_PREFIX_LENGTH
        tails = iter([bytes.fromhex(_HIGH_TAIL), bytes.fromhex(_LOW_TAIL)])
        monkeypatch.setattr(os, "urandom", lambda _size: next(tails))
        monkeypatch.setattr(time, "time_ns", lambda: _MS * 1_000_000)

        first = bts.generate_unique_id()
        second = bts.generate_unique_id()

        assert first[:prefix] == second[:prefix]
        assert (
            first > second
        ), "the id minted second sorts lower, on its random tail alone"


class TestArrivalsSharingAMillisecond:
    """Two distinct emissions minted in one millisecond are two signals, not one.

    `fill` asks two separate questions -- "have I seen exactly this?" and "is this from an older
    millisecond?" -- because no single comparison answers both. Identity needs the whole id;
    ordering is only meaningful on the timestamp prefix, since the tail is random.

    Comparing whole ids conflated them. A genuinely new emission that shared a millisecond with
    the stored one was refused whenever the random tails sorted the wrong way, which is about
    half the time, and the refusal is silent: the caller reads it as a redelivery, skips
    `maybe_trigger`, and a run that should have started never does.

    Every id here is built by `_id_at`, so the tails are chosen rather than rolled.
    """

    @pytest.mark.parametrize(
        ("stored_tail", "arriving_tail", "case"),
        [
            # The bug: the new arrival sorts BELOW the stored id on its tail alone.
            (_HIGH_TAIL, _LOW_TAIL, "arriving id sorts lower"),
            # The same situation with the coin the other way up, which always worked.
            (_LOW_TAIL, _HIGH_TAIL, "arriving id sorts higher"),
        ],
    )
    def test_a_distinct_emission_is_recorded_whichever_way_the_tails_sort(
        self, stored_tail: str, arriving_tail: str, case: str
    ) -> None:
        state = db_models.TriggerEventState(
            subscription_id="sub-1",
            event_name="orders-ready",
            expire_seconds=None,
        )
        stored = _id_at(milliseconds=_MS, tail=stored_tail)
        arriving = _id_at(milliseconds=_MS, tail=arriving_tail)
        assert event_state.fill(state=state, emission_event_id=stored, now=_NOW) is True

        wrote = event_state.fill(
            state=state, emission_event_id=arriving, now=_NOW + _HOUR
        )

        assert wrote is True, f"a real arrival was dropped: {case}"
        assert state.last_emission_event_id == arriving
        assert state.filled_at == _NOW + _HOUR

    def test_the_very_same_id_is_still_a_redelivery(self) -> None:
        # Identity is unchanged by the fix: the whole id still has to match to refuse.
        state = db_models.TriggerEventState(
            subscription_id="sub-1",
            event_name="orders-ready",
            expire_seconds=None,
        )
        only = _id_at(milliseconds=_MS, tail=_LOW_TAIL)
        event_state.fill(state=state, emission_event_id=only, now=_NOW)

        wrote = event_state.fill(state=state, emission_event_id=only, now=_NOW + _HOUR)

        assert wrote is False
        assert state.filled_at == _NOW, "a redelivery must not move the expiry window"

    def test_an_older_millisecond_is_still_a_straggler(self) -> None:
        # Ordering across milliseconds is unchanged, which is where it means something. The
        # tails are set as unhelpfully as possible -- the straggler carries the higher one -- and
        # the prefix still decides, because it is fixed width and so dominates the compare.
        state = db_models.TriggerEventState(
            subscription_id="sub-1",
            event_name="orders-ready",
            expire_seconds=None,
        )
        newer = _id_at(milliseconds=_NEXT_MS, tail=_LOW_TAIL)
        older = _id_at(milliseconds=_MS, tail=_HIGH_TAIL)
        event_state.fill(state=state, emission_event_id=newer, now=_NOW + _HOUR)

        wrote = event_state.fill(state=state, emission_event_id=older, now=_NOW)

        assert wrote is False
        assert state.last_emission_event_id == newer
        assert (
            state.filled_at == _NOW + _HOUR
        ), "a straggler must not drag the window back"

    def test_a_newer_millisecond_is_still_recorded(self) -> None:
        state = db_models.TriggerEventState(
            subscription_id="sub-1",
            event_name="orders-ready",
            expire_seconds=None,
        )
        older = _id_at(milliseconds=_MS, tail=_HIGH_TAIL)
        newer = _id_at(milliseconds=_NEXT_MS, tail=_LOW_TAIL)
        event_state.fill(state=state, emission_event_id=older, now=_NOW)

        wrote = event_state.fill(state=state, emission_event_id=newer, now=_NOW + _HOUR)

        assert wrote is True
        assert state.last_emission_event_id == newer

    def test_the_second_signal_still_starts_a_run(self, session: orm.Session) -> None:
        # The whole point, through the sink's own entry point rather than `fill` directly: two
        # readiness emissions from one millisecond must start two cycles. Under the old compare
        # the second came back ARRIVAL_ALREADY_RECORDED and the run was simply lost.
        subscription = _subscribe(session, condition=_leaf("orders-ready"))
        first = _id_at(milliseconds=_MS, tail=_HIGH_TAIL)
        second = _id_at(milliseconds=_MS, tail=_LOW_TAIL)
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id=first,
            now=_NOW,
        )
        cycles_after_first = len(_history(session))

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id=second,
            now=_NOW + _HOUR,
        )

        assert [arrival.result.reason for arrival in fan_out.outcomes] != [
            service.TriggerReason.ARRIVAL_ALREADY_RECORDED
        ]
        assert len(_history(session)) == cycles_after_first + 1
        assert (
            _states(session, subscription=subscription)[
                "orders-ready"
            ].last_emission_event_id
            == second
        )


class TestSubscriptionIdsWaitingOn:
    """Event name -> the subscriptions to lock, the sink's first move on an arrival."""

    def test_it_finds_every_subscription_waiting_on_the_name(
        self, session: orm.Session
    ) -> None:
        first = _subscribe(session, condition=_leaf("shared"))
        second = _subscribe(session, condition=_any(_leaf("shared"), _leaf("other")))

        found = event_state.subscription_ids_waiting_on(
            session=session, event_name="shared"
        )

        assert set(found) == {first.id, second.id}

    def test_an_unsubscribed_name_finds_nothing(self, session: orm.Session) -> None:
        # The sink's no_subscription case: an emission nobody is waiting for.
        _subscribe(session, condition=_leaf("a"))

        assert (
            event_state.subscription_ids_waiting_on(session=session, event_name="b")
            == []
        )

    def test_an_already_filled_event_still_comes_back(
        self, session: orm.Session
    ) -> None:
        # Latest-wins depends on this: a filled row is a candidate for overwriting, not a row
        # to skip.
        subscription = _subscribe(session, condition=_leaf("a"))
        _fill(session, subscription=subscription, event="a")

        assert event_state.subscription_ids_waiting_on(
            session=session, event_name="a"
        ) == [subscription.id]

    def test_ids_come_back_sorted(self, session: orm.Session) -> None:
        # A stable order across a batch of arrivals, so two consumers touching the same two
        # subscriptions take their row locks in the same sequence rather than deadlocking.
        # Sorted in Python rather than by an ORDER BY, which a single-column index cannot cover.
        ids = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(3)
        )

        assert (
            event_state.subscription_ids_waiting_on(
                session=session, event_name="shared"
            )
            == ids
        )


class TestLockOrder:
    """Every writer takes the subscription row first, then event state, then history.

    The lock itself cannot be asserted here — SQLite renders no FOR UPDATE, so these pass
    with or without it. What is asserted is the order the statements go out in, which is the
    part a reader of this code can get wrong: a fill before the subscription read puts the
    arrival path in the opposite order to the update path, and opposite orders on the same two
    rows is the shape a MySQL deadlock needs.
    """

    @staticmethod
    def _recorded(session: orm.Session) -> list[str]:
        statements: list[str] = []

        def _record(
            conn, cursor, statement, parameters, context, executemany
        ) -> None:  # noqa: ANN001
            statements.append(" ".join(statement.split()))

        sqlalchemy.event.listen(session.get_bind(), "before_cursor_execute", _record)
        return statements

    @staticmethod
    def _first_touch(statements: list[str], *, table: str) -> int:
        for index, statement in enumerate(statements):
            if table in statement:
                return index
        raise AssertionError(f"no statement touched {table}: {statements}")

    def test_the_arrival_path_reads_the_subscription_before_filling(
        self, session: orm.Session
    ) -> None:
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        statements = self._recorded(session)

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        subscription_read = self._first_touch(
            statements, table="FROM trigger_subscription"
        )
        event_state_write = self._first_touch(
            statements, table="UPDATE trigger_event_state"
        )
        assert subscription_read < event_state_write

    def test_the_arrival_path_writes_history_last(self, session: orm.Session) -> None:
        _subscribe(session, condition=_leaf("a"))
        statements = self._recorded(session)

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        subscription_read = self._first_touch(
            statements, table="FROM trigger_subscription"
        )
        history_write = self._first_touch(
            statements, table="INSERT INTO trigger_history"
        )
        assert subscription_read < history_write

    def test_the_update_path_reads_the_subscription_before_syncing(
        self, session: orm.Session
    ) -> None:
        # The route loads the row — with the lock — before update_subscription syncs the event
        # set, so both paths queue on the same row rather than on each other's.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        session.commit()
        statements = self._recorded(session)

        locked = service.lock_subscription_until_commit(
            session=session, subscription_id=subscription.id
        )
        assert locked is not None
        service.update_subscription(
            session=session,
            subscription=locked,
            condition=_all(_leaf("a"), _leaf("c")),
            caller=_CALLER,
            now=_NOW,
        )

        subscription_read = self._first_touch(
            statements, table="FROM trigger_subscription"
        )
        event_state_touch = self._first_touch(statements, table="trigger_event_state")
        assert subscription_read < event_state_touch

    def test_a_missing_subscription_is_not_an_error(self, session: orm.Session) -> None:
        assert (
            service.lock_subscription_until_commit(
                session=session, subscription_id="does-not-exist"
            )
            is None
        )


class TestTheSeekDoesNotPinASnapshot:
    """The arrival path's opening seek gets its own transaction, so no lock inherits its view.

    Taking the lock first is only worth anything if the lock is the first statement of its
    transaction. `record_event_and_maybe_start_runs` opens with a plain seek for the waiting
    subscription ids, and a plain read is what pins a REPEATABLE READ view — so without a
    commit between the seek and the loop, the first iteration would lock inside the seek's
    view, and the `session.get` after that lock would answer from before an edit that
    committed in between. Iterations after the first are already covered by the previous
    subscription's commit; the first one is the whole exposure, and the common case, since
    most arrivals wake exactly one subscription.

    As with `TestLockOrder`, SQLite cannot show the consequence — it has neither FOR UPDATE
    nor the read view. What is asserted is the transaction boundary, which is the part that
    can be regressed, by moving the seek back inside the loop's transaction.
    """

    @staticmethod
    def _log(session: orm.Session) -> list[str]:
        """Statements and commits in one list, so a boundary can be placed among the reads."""
        entries: list[str] = []

        def _record(
            conn, cursor, statement, parameters, context, executemany
        ) -> None:  # noqa: ANN001
            entries.append(" ".join(statement.split()))

        def _commit(session: orm.Session) -> None:
            entries.append("COMMIT")

        sqlalchemy.event.listen(session.get_bind(), "before_cursor_execute", _record)
        sqlalchemy.event.listen(session, "after_commit", _commit)
        return entries

    @staticmethod
    def _first_touch(entries: list[str], *, table: str) -> int:
        for index, entry in enumerate(entries):
            if table in entry:
                return index
        raise AssertionError(f"no statement touched {table}: {entries}")

    def test_the_seek_is_committed_before_the_first_lock(
        self, session: orm.Session
    ) -> None:
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        entries = self._log(session)

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        seek = next(
            index
            for index, entry in enumerate(entries)
            if "FROM trigger_event_state" in entry
        )
        lock = self._first_touch(entries, table="FROM trigger_subscription")
        assert seek < entries.index("COMMIT") < lock

    def test_the_lock_opens_its_transaction(self, session: orm.Session) -> None:
        # Nothing may read between the seek's commit and the lock: a read there would pin a
        # fresh view a moment too early and hand the lock back the problem it just solved.
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        entries = self._log(session)

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        after_commit = entries[entries.index("COMMIT") + 1 :]
        assert "FROM trigger_subscription" in after_commit[0], after_commit[:3]

    def test_no_transaction_is_left_open_when_nothing_waits(
        self, session: orm.Session
    ) -> None:
        # The early return is the easiest half of this to regress: it skips the loop, so it
        # skips every commit the loop would have made, and the seek's view would outlive the
        # call and stale every read the caller makes next.
        _subscribe(session, condition=_leaf("a"))

        assert service.record_event_and_maybe_start_runs(
            session=session,
            event_name="nobody-waits",
            emission_event_id="em-1",
            now=_NOW,
        ) == service.FanOut(outcomes=[], deferred=[])
        assert not session.in_transaction()


class TestArrivalAgainstADisabledSubscription:
    def test_the_arrival_is_stored_and_only_the_run_is_withheld(
        self, session: orm.Session
    ) -> None:
        # Disabled means "do not start a run", not "stop listening": the event state is written
        # exactly as it would be for a live subscription, so re-enabling resumes with a current
        # event set instead of waiting for every event to arrive a second time.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        subscription.enabled = False
        session.commit()

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        assert [arrival.result.reason for arrival in fan_out.outcomes] == [
            service.TriggerReason.SUBSCRIPTION_DISABLED
        ]
        assert [arrival.result.triggered for arrival in fan_out.outcomes] == [False]
        state = _states(session, subscription=subscription)["a"]
        assert state.filled_at == _NOW
        assert state.last_emission_event_id == "em-1"

    def test_a_later_arrival_replaces_the_earlier_one(
        self, session: orm.Session
    ) -> None:
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        # Ordered ids, not "em-old"/"em-new": `fill` compares them, and it compares them as
        # strings because real ids sort chronologically by construction. Names that read as
        # ordered but do not sort that way ('n' < 'o') would have this assert the opposite of
        # what it says.
        _fill(session, subscription=subscription, event="a", emission_id="em-1")
        subscription.enabled = False
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-2", now=_NOW
        )

        # Overwritten by the newer arrival — latest wins whether or not the subscription is
        # switched on — and, either way, never cleared by the disabling itself.
        assert (
            _states(session, subscription=subscription)["a"].last_emission_event_id
            == "em-2"
        )


class TestAnArrivalThatRacedAnEdit:
    """The seek runs before the lock, so what it saw may be gone by the time the lock is held.

    Both tests force the race deterministically by making the seek report a subscription whose
    state for that event does not exist — which is exactly what a concurrent PATCH that dropped
    the event, or a DELETE that removed the subscription, leaves behind.
    """

    def test_an_event_dropped_from_the_condition_is_skipped(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subscription = _subscribe(session, condition=_leaf("a"))
        monkeypatch.setattr(
            service.event_state,
            "subscription_ids_waiting_on",
            lambda **_kwargs: [subscription.id],
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="gone",
            emission_event_id="em-1",
            now=_NOW,
        )

        # Nothing recorded, nothing triggered, and no crash on a row that is not there.
        assert fan_out.outcomes == []
        assert fan_out.deferred == []
        assert _history(session) == []
        assert _states(session, subscription=subscription)["a"].filled_at is None

    def test_a_subscription_deleted_under_the_seek_is_skipped(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            service.event_state,
            "subscription_ids_waiting_on",
            lambda **_kwargs: ["01deadbeefdeadbeefff"],
        )

        assert service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        ) == service.FanOut(outcomes=[], deferred=[])

    def test_a_deleted_subscription_still_ends_its_transaction(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The skip commits, so the subscription after it reads on a fresh snapshot.

        SQLite cannot show what this is for. Under MySQL's REPEATABLE READ the plain reads
        the next iteration makes would otherwise run on the snapshot pinned by the seek at
        the top of this call, and the second subscription could evaluate its condition
        against event states that were already stale when it started — a missed run, with no
        error to notice. The locking read on the missing id also leaves a gap lock behind.
        What is asserted is the boundary itself: a COMMIT between the two subscription reads.
        """
        # Read the id out before the listeners go on: the commit inside _subscribe expired the
        # object, so touching it later would emit a refresh SELECT the trace would count as a
        # third subscription read.
        subscription_id = _subscribe(session, condition=_leaf("a")).id
        monkeypatch.setattr(
            service.event_state,
            "subscription_ids_waiting_on",
            lambda **_kwargs: ["01deadbeefdeadbeefff", subscription_id],
        )
        trace: list[str] = []

        def _statement(
            conn, cursor, statement, parameters, context, executemany
        ) -> None:  # noqa: ANN001
            trace.append(" ".join(statement.split()))

        sqlalchemy.event.listen(session.get_bind(), "before_cursor_execute", _statement)
        sqlalchemy.event.listen(
            session, "after_commit", lambda _session: trace.append("COMMIT")
        )

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        reads = [
            index
            for index, entry in enumerate(trace)
            if "FROM trigger_subscription" in entry
        ]
        assert len(reads) == 2, trace
        assert "COMMIT" in trace[reads[0] : reads[1]], trace

    def test_the_expiry_used_is_the_one_read_inside_the_lock(
        self, session: orm.Session
    ) -> None:
        # The state row is fetched by primary key after the subscription is locked, so an edit
        # that changed this event's expiry is reflected rather than overwritten from a copy read
        # before the lock.
        subscription = _subscribe(session, condition=_leaf("a", expire_seconds=60))
        service.update_subscription(
            session=session,
            subscription=subscription,
            condition=_all(_leaf("a", expire_seconds=600), _leaf("b")),
            caller=_CALLER,
            now=_NOW,
        )
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        state = _states(session, subscription=subscription)["a"]
        assert state.expire_seconds == 600
        assert state.expires_at == _NOW + datetime.timedelta(seconds=600)


class TestAnArrivalThatCompletesTheCondition:
    def test_it_triggers_inside_the_same_call(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # Deliberately not the shared `session` fixture: this one has autoflush off, as the
        # emission consumer's factory does (emissions/consumer_main.py). The arrival just written
        # has to be visible to the SELECT that evaluates the condition, and with autoflush on
        # that happens for free — so a missing flush would pass every other test here and then
        # never trigger in production.
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as session:
            subscription = _subscribe(session, condition=_leaf("a"))

            fan_out = service.record_event_and_maybe_start_runs(
                session=session,
                event_name="a",
                emission_event_id="em-1",
                now=_NOW,
            )

            assert [
                (arrival.result.triggered, arrival.result.reason)
                for arrival in fan_out.outcomes
            ] == [(True, None)]
            assert [(row.subscription_id, row.cycle) for row in _history(session)] == [
                (subscription.id, 0)
            ]


class TestARedeliveredArrival:
    def test_the_same_emission_is_reported_not_replayed(
        self, session: orm.Session
    ) -> None:
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        assert [arrival.result.reason for arrival in fan_out.outcomes] == [
            service.TriggerReason.ARRIVAL_ALREADY_RECORDED
        ]

    def test_an_arrival_with_no_id_is_never_deduplicated(
        self, session: orm.Session
    ) -> None:
        # There is nothing to compare, so an id-less arrival is always treated as new rather
        # than silently swallowed.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id=None, now=_NOW
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id=None, now=_NOW
        )

        assert [arrival.result.reason for arrival in fan_out.outcomes] == [
            service.TriggerReason.AWAITING_EVENTS
        ]
        assert _states(session, subscription=subscription)["a"].filled_at == _NOW


class TestARedeliveryReportsTheRunItAlreadyStarted:
    """The ledger must not deny a run that is executing.

    The recorder writes one outcome row per delivery, and a delivery whose recorder write fails
    is left unsettled and redelivered. So the redelivery is the *normal* consequence of the
    ledger write failing -- and its answer is the one that finally commits. Answering
    `arrival_already_recorded` there files the emission as having started nothing while its run
    is in flight.
    """

    def test_the_run_and_its_cycle_come_back_on_the_second_delivery(
        self, session: orm.Session
    ) -> None:
        _subscribe(session, condition=_leaf("a"))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )
        started = _history(session)[0]

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-1",
            now=_NOW + _HOUR,
        )

        (arrival,) = fan_out.outcomes
        assert arrival.result.triggered is True
        assert arrival.result.reason == service.TriggerReason.RUN_ALREADY_STARTED
        assert arrival.result.pipeline_run_id == started.pipeline_run_id
        assert arrival.result.cycle == started.cycle

    def test_the_report_starts_no_second_run(self, session: orm.Session) -> None:
        # The reason the refusal exists in the first place. Reporting the truth must not become
        # a way of acting on the signal twice.
        _subscribe(session, condition=_leaf("a"))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-1",
            now=_NOW + _HOUR,
        )

        assert len(_history(session)) == 1
        assert len(_runs(session)) == 1

    def test_an_emission_that_only_banked_still_reports_nothing_triggered(
        self, session: orm.Session
    ) -> None:
        # The other half, and why the lookup is keyed on the emission rather than on the
        # subscription. `all(a, b)` never fired for em-1, so there is no run to name and the
        # honest answer is the one this path always gave.
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-1",
            now=_NOW + _HOUR,
        )

        (arrival,) = fan_out.outcomes
        assert arrival.result.triggered is False
        assert arrival.result.reason == service.TriggerReason.ARRIVAL_ALREADY_RECORDED
        assert arrival.result.pipeline_run_id is None

    def test_a_later_unrelated_trigger_is_not_reported_as_this_emission_s(
        self, session: orm.Session
    ) -> None:
        # Keyed on the emission, not the subscription. This subscription has fired twice; a
        # redelivery of the *first* emission must name the first run, and a redelivery of an
        # emission that never fired must name none of them.
        _subscribe(session, condition=_leaf("a"))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-2",
            now=_NOW + _HOUR,
        )
        first, second = sorted(_history(session), key=lambda row: row.cycle)
        assert first.pipeline_run_id != second.pipeline_run_id

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-1",
            now=_NOW + 2 * _HOUR,
        )

        (arrival,) = fan_out.outcomes
        assert arrival.result.pipeline_run_id == first.pipeline_run_id
        assert arrival.result.cycle == first.cycle

    def test_a_trigger_older_than_the_scan_window_degrades_to_a_refusal_with_no_run(
        self, session: orm.Session
    ) -> None:
        # The bound is a window, not a guarantee, and falling out of it must be quiet: the
        # answer goes back to what this path said before the lookup existed rather than
        # becoming wrong in a new way. The fixture is derived from the constant so raising it
        # does not silently stop exercising this.
        #
        # Which refusal it is comes from the other question this path asks: the emission being
        # redelivered is the oldest of several, so the event state has moved past it and the
        # honest answer is `arrival_superseded`. What the window costs is the run id, and that
        # is what `triggered is False` pins.
        _subscribe(session, condition=_leaf("a"))
        oldest = self._fire(session, count=service._NUM_PAST_CYCLES_TO_SCAN + 1)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id=oldest, now=_NOW
        )

        (arrival,) = fan_out.outcomes
        assert arrival.result.triggered is False
        assert arrival.result.pipeline_run_id is None
        assert arrival.result.reason == service.TriggerReason.ARRIVAL_SUPERSEDED

    def test_the_newest_trigger_in_range_is_still_found(
        self, session: orm.Session
    ) -> None:
        # One row inside the window rather than one past it -- the pair that pins the boundary
        # to the constant instead of to an off-by-one.
        _subscribe(session, condition=_leaf("a"))
        oldest = self._fire(session, count=service._NUM_PAST_CYCLES_TO_SCAN)
        first = min(_history(session), key=lambda row: row.cycle)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id=oldest, now=_NOW
        )

        (arrival,) = fan_out.outcomes
        assert arrival.result.pipeline_run_id == first.pipeline_run_id

    @staticmethod
    def _fire(session: orm.Session, *, count: int) -> str:
        """Trigger `count` cycles off `count` distinct emissions, and name the first one.

        Zero-padded ids, because `fill` compares the leading `ID_MS_PREFIX_LENGTH` characters
        and these are shorter than that -- so the whole id is compared, lexicographically. With
        "em-9" and "em-10" that ordering inverts, `fill` reads the tenth arrival as a straggler
        and refuses it, and the fixture silently stops building the history it is measuring.
        """
        for index in range(count):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="a",
                emission_event_id=f"em-{index:04d}",
                now=_NOW + index * _HOUR,
            )
        assert len(_history(session)) == count
        return "em-0000"

    def test_an_id_less_arrival_reads_no_history_at_all(
        self, session: orm.Session
    ) -> None:
        # An arrival with no id is never deduplicated, so this path is unreachable for it --
        # and the guard is what keeps a NULL from matching a history row that stored one.
        subscription = _subscribe(session, condition=_leaf("a"))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id=None, now=_NOW
        )

        assert (
            service._run_started_by(
                session=session,
                subscription_id=subscription.id,
                event_name="a",
                emission_event_id=None,
            )
            is None
        )

    def test_the_reason_is_not_read_as_a_run_that_never_started(
        self, session: orm.Session
    ) -> None:
        # A run exists, so this must never reach the tuple that tells the sink to report FAIL.
        assert (
            service.TriggerReason.RUN_ALREADY_STARTED
            not in service.RUN_NOT_STARTED_REASONS
        )
        _subscribe(session, condition=_leaf("a"))
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1", now=_NOW
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-1",
            now=_NOW + _HOUR,
        )

        assert fan_out.failed == []


class TestAStragglerIsNotReportedAsADuplicate:
    """Found by review: `fill` refuses two different things and only one of them is a repeat.

    The same emission arriving twice is a duplicate -- the work is done, nothing was lost. A
    *different*, older emission arriving after the row has moved on is the opposite fact: a
    readiness signal that will never be acted on, and a run somebody expected that is not
    coming. Collapsing both into `arrival_already_recorded` makes the second invisible, in the
    one record anybody reads afterwards.

    Ids are zero-padded and shorter than `ID_MS_PREFIX_LENGTH`, so `fill` compares them whole
    and the numeric order is the order it sees.
    """

    @staticmethod
    def _bank(session: orm.Session, *, emission: str, at: datetime.datetime) -> None:
        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id=emission, now=at
        )

    @staticmethod
    def _reason(fan_out: service.FanOut) -> service.TriggerReason | None:
        (arrival,) = fan_out.outcomes
        return arrival.result.reason

    def test_an_older_emission_arriving_late_says_it_was_superseded(
        self, session: orm.Session
    ) -> None:
        # Two events, so filling one banks the arrival without triggering and the row stays
        # filled for the straggler to land on.
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        self._bank(session, emission="em-0002", at=_NOW)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-0001",
            now=_NOW + _HOUR,
        )

        assert self._reason(fan_out) == service.TriggerReason.ARRIVAL_SUPERSEDED

    def test_the_same_emission_arriving_twice_still_says_already_recorded(
        self, session: orm.Session
    ) -> None:
        """The control: the new reason must replace one answer, not both."""
        _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        self._bank(session, emission="em-0002", at=_NOW)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-0002",
            now=_NOW + _HOUR,
        )

        assert self._reason(fan_out) == service.TriggerReason.ARRIVAL_ALREADY_RECORDED

    def test_neither_writes_anything_to_the_row(self, session: orm.Session) -> None:
        # The reason is a report, not a decision: dragging `filled_at` back to the straggler's
        # arrival is the bug the monotonic guard exists to prevent.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        self._bank(session, emission="em-0002", at=_NOW)

        self._bank(session, emission="em-0001", at=_NOW + _HOUR)

        state = _states(session, subscription=subscription)["a"]
        assert state.last_emission_event_id == "em-0002"
        assert state.filled_at == _NOW

    def test_a_superseded_emission_that_did_start_a_run_still_names_it(
        self, session: orm.Session
    ) -> None:
        # Branch order: "did this emission start a run?" is asked first, and its answer beats
        # both refusals. A straggler whose first delivery fired is still a run in flight, and
        # reporting `arrival_superseded` for it would deny it exactly as the old answer did.
        _subscribe(session, condition=_leaf("a"))
        self._bank(session, emission="em-0001", at=_NOW)
        self._bank(session, emission="em-0002", at=_NOW + _HOUR)
        first, _ = sorted(_history(session), key=lambda row: row.cycle)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="a",
            emission_event_id="em-0001",
            now=_NOW + 2 * _HOUR,
        )

        (arrival,) = fan_out.outcomes
        assert arrival.result.reason == service.TriggerReason.RUN_ALREADY_STARTED
        assert arrival.result.pipeline_run_id == first.pipeline_run_id

    def test_neither_refusal_is_read_as_a_run_that_never_started(self) -> None:
        # Both mean the arrival is accounted for, so neither may reach the set that tells the
        # sink to report FAIL.
        assert not (
            {
                service.TriggerReason.ARRIVAL_SUPERSEDED,
                service.TriggerReason.ARRIVAL_ALREADY_RECORDED,
            }
            & service.RUN_NOT_STARTED_REASONS
        )


class TestTheArrivalClock:
    def test_the_current_time_is_used_when_none_is_given(
        self, session: orm.Session
    ) -> None:
        # The sink calls record_event_and_maybe_start_runs with no `now`, so the default is
        # the production path. Every other test in this file pins the clock, and would
        # pass with the default gone.
        subscription = _subscribe(session, condition=_all(_leaf("a"), _leaf("b")))
        before = db_utils.utc_now()

        service.record_event_and_maybe_start_runs(
            session=session, event_name="a", emission_event_id="em-1"
        )

        filled_at = _states(session, subscription=subscription)["a"].filled_at
        assert filled_at is not None
        assert before <= filled_at <= db_utils.utc_now()


class TestAnArrivalNobodyIsWaitingFor:
    def test_nothing_is_recorded_and_the_list_comes_back_empty(
        self, session: orm.Session
    ) -> None:
        # The sink's no_subscription case. Asserted through record_event_and_maybe_start_runs
        # rather than on the seek alone, because the early return is what keeps an
        # unwatched event name from opening a transaction at all.
        _subscribe(session, condition=_leaf("a"))

        assert service.record_event_and_maybe_start_runs(
            session=session, event_name="b", emission_event_id="em-1", now=_NOW
        ) == service.FanOut(outcomes=[], deferred=[])
        assert _history(session) == []


class TestTheOrderArrivalsComeBackIn:
    def test_one_entry_per_subscription_in_id_order(self, session: orm.Session) -> None:
        # The `Returns:` contract. It holds because the seek sorts, and this is what goes
        # red if that sort is dropped — asserted through record_event_and_maybe_start_runs
        # rather than on the seek alone, because the promise is made in this function's
        # docstring.
        ids = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(3)
        )

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert [arrival.subscription_id for arrival in fan_out.outcomes] == ids


class TestTheFanOutTransactionBoundary:
    """One transaction per subscription, not one for the batch."""

    def test_a_raise_on_the_second_keeps_the_first_subscriptions_arrival(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The docstring's promise: "a failure while triggering the second must not throw away
        # the first's arrival". The arrival is real work, recorded whether or not anything
        # triggered, and a per-batch transaction would roll it back with the failure.
        first_id, second_id = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(2)
        )
        triggered: list[str] = []

        def _fail_on_the_second(
            *, session: orm.Session, subscription: Any, now: Any
        ) -> service.TriggerResult:
            triggered.append(subscription.id)
            if subscription.id == second_id:
                raise RuntimeError("the run could not be started")
            return service.TriggerResult(triggered=True, reason=None, cycle=0)

        monkeypatch.setattr(service, "maybe_trigger", _fail_on_the_second)

        with pytest.raises(RuntimeError):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="shared",
                emission_event_id="em-1",
                now=_NOW,
            )
        session.rollback()

        assert triggered == [first_id, second_id]
        states = {
            row.subscription_id: row
            for row in session.scalars(
                sqlalchemy.select(db_models.TriggerEventState)
            ).all()
        }
        assert states[first_id].filled_at == _NOW
        assert states[second_id].filled_at is None

    def test_a_raise_leaves_the_session_clean_for_the_caller(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The sibling test above rolls back by hand, which hides whether
        # record_event_and_maybe_start_runs did. This one deliberately does not: the failing
        # subscription's arrival was already flushed when the raise happened, so a caller
        # who catches the error and keeps reading would otherwise see that uncommitted
        # write as though it had been recorded.
        first_id, second_id = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(2)
        )

        def _fail_on_the_second(
            *, session: orm.Session, subscription: Any, now: Any
        ) -> service.TriggerResult:
            if subscription.id == second_id:
                raise RuntimeError("the run could not be started")
            return service.TriggerResult(triggered=True, reason=None, cycle=0)

        monkeypatch.setattr(service, "maybe_trigger", _fail_on_the_second)

        with pytest.raises(RuntimeError):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="shared",
                emission_event_id="em-1",
                now=_NOW,
            )

        assert not session.in_transaction()
        states = {
            row.subscription_id: row
            for row in session.scalars(
                sqlalchemy.select(db_models.TriggerEventState)
            ).all()
        }
        assert states[first_id].filled_at == _NOW
        assert states[second_id].filled_at is None


class TestAContendedSubscriptionDoesNotStarveTheRest:
    """A retryable failure is one subscription's problem, not the whole fan-out's.

    The loop commits as it goes, so propagating a deadlock would strand every subscription
    ordered after the contended one — and because the caller retries by re-running the fan-out
    from the top, the same row would starve the same tail on every attempt. These pin the
    containment, and the boundary between what is contained and what still escapes.
    """

    @staticmethod
    def _deadlock_on(
        *, subscription_ids: set[str], monkeypatch: pytest.MonkeyPatch
    ) -> list[str]:
        """Make `maybe_trigger` raise a deadlock for the named subscriptions only."""
        seen: list[str] = []
        real = service.maybe_trigger

        def _maybe_trigger(
            *, session: orm.Session, subscription: Any, now: Any
        ) -> service.TriggerResult:
            seen.append(subscription.id)
            if subscription.id in subscription_ids:
                raise sqlalchemy.exc.OperationalError(
                    "SELECT 1", {}, Exception("deadlock found")
                )
            return real(session=session, subscription=subscription, now=now)

        monkeypatch.setattr(service, "maybe_trigger", _maybe_trigger)
        return seen

    def test_the_subscriptions_after_the_failure_are_still_attempted(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The heart of it. Before containment the loop stopped at the contended subscription,
        # so the two behind it were never visited at all — not tried and failed, never tried.
        first, second, third = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(3)
        )
        seen = self._deadlock_on(subscription_ids={first}, monkeypatch=monkeypatch)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert seen == [first, second, third]
        assert fan_out.deferred == [first]
        assert [arrival.subscription_id for arrival in fan_out.outcomes] == [
            second,
            third,
        ]

    def test_the_survivors_arrivals_are_committed(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Durable, not merely reported: the point of one transaction per subscription is that
        # a neighbour's deadlock cannot take a committed arrival back. The condition needs a
        # second event so the arrival is still *sitting* there to be asserted on — a condition
        # this arrival completed would trigger, and the trigger clears what it consumed.
        first, second = sorted(
            _subscribe(session, condition=_all(_leaf("shared"), _leaf("other"))).id
            for _ in range(2)
        )
        self._deadlock_on(subscription_ids={first}, monkeypatch=monkeypatch)

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        states = {
            (row.subscription_id, row.event_name): row
            for row in session.scalars(
                sqlalchemy.select(db_models.TriggerEventState)
            ).all()
        }
        assert states[(second, "shared")].filled_at == _NOW
        # And the contended one kept nothing, so a retry sees it as never having arrived.
        assert states[(first, "shared")].filled_at is None
        assert states[(first, "shared")].last_emission_event_id is None

    def test_the_session_is_clean_after_a_contained_failure(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The next subscription runs on this same session, so a half-open transaction left by
        # the rollback would poison it rather than just the row that failed.
        first, second = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(2)
        )
        self._deadlock_on(subscription_ids={first}, monkeypatch=monkeypatch)

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert not session.in_transaction()

    def test_every_subscription_contended_reports_deferred_not_empty(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The trap for the caller: no outcomes, but emphatically not "nobody was listening".
        # A caller reading an empty `outcomes` as no_subscription would file a lost signal as
        # a routine IGNORE, which is why the two lists are separate.
        ids = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(2)
        )
        self._deadlock_on(subscription_ids=set(ids), monkeypatch=monkeypatch)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert fan_out.outcomes == []
        assert fan_out.deferred == ids

    def test_nothing_waiting_is_still_an_empty_fan_out_both_ways(
        self, session: orm.Session
    ) -> None:
        # The other side of the same distinction, so the two cases cannot be confused: nobody
        # listening is empty *and* nothing deferred.
        _subscribe(session, condition=_leaf("a"))

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="nobody-waits",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert fan_out == service.FanOut(outcomes=[], deferred=[])

    def test_a_failure_a_retry_cannot_fix_still_escapes(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Containment is only for failures a later attempt can survive. A value the column
        # rejects fails identically every time, so deferring it would retry it three times and
        # then report it as a transient contention — a lie the caller would act on.
        first, _second = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(2)
        )

        def _bad_value(
            *, session: orm.Session, subscription: Any, now: Any
        ) -> service.TriggerResult:
            if subscription.id == first:
                raise sqlalchemy.exc.DataError(
                    "INSERT", {}, Exception("value too long")
                )
            return service.TriggerResult(triggered=True, reason=None, cycle=0)

        monkeypatch.setattr(service, "maybe_trigger", _bad_value)

        with pytest.raises(sqlalchemy.exc.DataError):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="shared",
                emission_event_id="em-1",
                now=_NOW,
            )

    def test_a_deferred_subscription_is_reported_once_not_as_an_outcome(
        self, session: orm.Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A deferred subscription must not also appear in `outcomes` wearing a reason string:
        # the sink counts `outcomes` as recorded arrivals, so one entry in both lists would be
        # reported as delivered and lost at the same time.
        first, second = sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(2)
        )
        self._deadlock_on(subscription_ids={first}, monkeypatch=monkeypatch)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        recorded = {arrival.subscription_id for arrival in fan_out.outcomes}
        assert recorded.isdisjoint(set(fan_out.deferred))
        assert recorded | set(fan_out.deferred) == {first, second}


class TestAReasonIsWrittenAsItsWireValue:
    """The reason names are a stored format, not a debugging convenience.

    They go into an emission's outcome detail, get read back by the end-to-end assertions, and
    are interpolated into log lines. `(str, enum.Enum)` alone stopped rendering as the value in
    Python 3.11, so the class carries `__str__ = str.__str__`; these pin what that buys.
    """

    def test_every_member_renders_as_the_value_it_stores(self) -> None:
        for reason in service.TriggerReason:
            # f-string, %-format and str() all route through __str__/__format__, and the
            # spellings must not diverge: one of them ends up in a record someone greps.
            assert f"{reason}" == reason.value
            assert "%s" % reason == reason.value
            assert str(reason) == reason.value

    def test_every_member_serialises_to_the_value_it_stores(self) -> None:
        # The outcome detail is JSON. A str subclass encodes as its value, which is what makes
        # the enum safe to put straight into the dict rather than unwrapping at every site.
        for reason in service.TriggerReason:
            assert json.dumps(reason) == json.dumps(reason.value)

    def test_a_member_is_equal_to_its_string(self) -> None:
        # Rows written before the enum existed hold bare strings; comparisons still have to work.
        assert service.TriggerReason.AWAITING_EVENTS == "awaiting_events"
        assert "awaiting_events" in {service.TriggerReason.AWAITING_EVENTS}


class TestTheFanOutRespectsItsBudget:
    """A deadline stops the fan-out between subscriptions, and never inside one."""

    @staticmethod
    def _three(session: orm.Session) -> list[str]:
        return sorted(
            _subscribe(session, condition=_leaf("shared")).id for _ in range(3)
        )

    def test_a_spent_budget_still_serves_one_subscription(
        self, session: orm.Session
    ) -> None:
        # The `index > 0` guard. A budget already gone when the fan-out starts must not serve
        # nobody: the redelivery it would ask for arrives to the same spent budget, and the
        # emission is retried for ever without progressing. One subscription per pass is what
        # bounds the number of passes to the length of the list.
        first, second, third = self._three(session)

        with pytest.raises(service.FanOutIncomplete) as caught:
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="shared",
                emission_event_id="em-1",
                now=_NOW,
                deadline=time.monotonic() - 1.0,
            )

        assert caught.value.served == [first]
        assert caught.value.remaining == [second, third]

    def test_the_arrival_the_budget_allowed_is_durable(
        self, session: orm.Session
    ) -> None:
        # The raise happens after the served subscription's own transaction committed, so its
        # arrival survives it. This is what makes stopping safe rather than merely tidy: the
        # work already paid for is not thrown away by the exception that stops the rest.
        first, second, third = self._three(session)

        with pytest.raises(service.FanOutIncomplete):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="shared",
                emission_event_id="em-1",
                now=_NOW,
                deadline=time.monotonic() - 1.0,
            )
        session.rollback()

        states = {
            row.subscription_id: row
            for row in session.scalars(
                sqlalchemy.select(db_models.TriggerEventState)
            ).all()
        }
        # `last_emission_event_id` rather than `filled_at`: this condition is one leaf, so the
        # arrival satisfied it, the trigger fired and `clear` reset the fill. What the clear
        # keeps is the emission id, which is exactly the field the redelivery dedups on.
        assert states[first].last_emission_event_id == "em-1"
        assert states[second].last_emission_event_id is None
        assert states[third].last_emission_event_id is None
        assert [row.subscription_id for row in _history(session)] == [first]

    def test_no_deadline_serves_every_subscription(self, session: orm.Session) -> None:
        # The default. Every existing caller passes nothing, and nothing about their fan-out
        # changes.
        self._three(session)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert len(fan_out.outcomes) == 3

    def test_a_budget_still_in_hand_serves_every_subscription(
        self, session: orm.Session
    ) -> None:
        # The ordinary case with a budget set: three subscriptions do not take an hour, so the
        # deadline is never reached and the presence of one changes nothing.
        self._three(session)

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
            deadline=time.monotonic() + 3600.0,
        )

        assert len(fan_out.outcomes) == 3

    def test_the_next_pass_finishes_what_the_budget_left(
        self, session: orm.Session
    ) -> None:
        # The redelivery, end to end. The list is re-derived rather than replayed, so the
        # subscription served on the first pass is in it again -- and answers in two reads
        # because `fill` recognises the emission, which is what keeps the second pass's budget
        # for the subscriptions still waiting.
        first, second, third = self._three(session)

        with pytest.raises(service.FanOutIncomplete):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="shared",
                emission_event_id="em-1",
                now=_NOW,
                deadline=time.monotonic() - 1.0,
            )
        session.rollback()

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="shared",
            emission_event_id="em-1",
            now=_NOW,
            deadline=time.monotonic() + 3600.0,
        )

        by_id = {
            outcome.subscription_id: outcome.result for outcome in fan_out.outcomes
        }
        assert by_id[first].reason == service.TriggerReason.RUN_ALREADY_STARTED
        assert by_id[second].triggered is True
        assert by_id[third].triggered is True
        assert {row.subscription_id for row in _history(session)} == {
            first,
            second,
            third,
        }


class TestTemplatesReachTheTriggeredRun:
    """A subscription's stored templates render at trigger time and arrive as the run's arguments.

    The whole path, not a mocked service: the claim is that `run_arguments` is threaded from the
    definition blob into the created run's root task.
    """

    @staticmethod
    def _run_arguments(session: orm.Session) -> dict[str, Any]:
        (run,) = _runs(session)
        return dict(run.root_execution.task_spec.get("arguments") or {})

    def test_a_trigger_renders_trigger_time_into_the_runs_arguments(
        self, session: orm.Session
    ) -> None:
        """The evaluation's clock, so a deferred retry renders at the moment it retried."""
        subscription = _subscribe(
            session,
            condition=_all(_leaf("a")),
            templates={"as_of_date": "{{ trigger_time | date }}"},
        )
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            service.maybe_trigger(session=session, subscription=subscription, now=_NOW)

        assert self._run_arguments(session) == {"as_of_date": _NOW.date().isoformat()}

    def test_a_subscription_with_no_templates_delivers_no_arguments(
        self, session: orm.Session
    ) -> None:
        """The pre-feature path: a definition without the key must not invent an argument."""
        subscription = _subscribe(session, condition=_all(_leaf("a")))
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            service.maybe_trigger(session=session, subscription=subscription, now=_NOW)

        assert self._run_arguments(session) == {}

    def test_a_schedule_time_template_fails_the_key_and_still_starts_the_run(
        self, session: orm.Session
    ) -> None:
        """A subscription is never scheduled, so `schedule_time` has nothing to resolve to."""
        subscription = _subscribe(
            session,
            condition=_all(_leaf("a")),
            templates={
                "as_of_date": "{{ schedule_time | date }}",
                "region": "ca",
            },
        )
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is True
        assert self._run_arguments(session) == {"region": "ca"}

    def test_a_broken_template_does_not_abort_the_fan_out(
        self, session: orm.Session
    ) -> None:
        """Rendering is non-raising: an escaped exception would settle the emission unredelivered."""
        subscription = _subscribe(
            session,
            condition=_all(_leaf("a")),
            templates={"as_of_date": "{{ trigger_time | no_such_filter }}"},
        )
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            result = service.maybe_trigger(
                session=session, subscription=subscription, now=_NOW
            )

        assert result.triggered is True
        assert self._run_arguments(session) == {}


class TestARenderFailureNeverBlocksTheTrigger:
    """`cycle` counts runs started, and a render failure does not change that.

    Nothing consumes a cycle without producing a run, and no run is produced without consuming
    one — so a failing template must not become a third case to reason about at the fence.
    """

    @staticmethod
    def _fire(
        session: orm.Session, *, templates: dict[str, str]
    ) -> db_models.TriggerSubscription:
        subscription = _subscribe(
            session, condition=_all(_leaf("a")), templates=templates
        )
        _fill(session, subscription=subscription, event="a")
        with session.begin():
            service.maybe_trigger(session=session, subscription=subscription, now=_NOW)
        return subscription

    def test_the_cycle_advances_despite_the_failure(self, session: orm.Session) -> None:
        subscription = self._fire(
            session, templates={"as_of_date": "{{ schedule_time | date }}"}
        )

        assert subscription.cycle == 1

    def test_the_fence_and_the_run_are_both_written_despite_the_failure(
        self, session: orm.Session
    ) -> None:
        """A cycle consumed with no run is the failure this invariant exists to exclude."""
        self._fire(session, templates={"as_of_date": "{{ schedule_time | date }}"})

        (history,) = _history(session)
        (run,) = _runs(session)
        assert history.pipeline_run_id == run.id

    def test_failure_is_per_key_so_the_good_templates_still_render(
        self, session: orm.Session
    ) -> None:
        subscription = self._fire(
            session,
            templates={
                "as_of_date": "{{ schedule_time | date }}",
                "region": "ca",
            },
        )

        (run,) = _runs(session)
        assert dict(run.root_execution.task_spec.get("arguments") or {}) == {
            "region": "ca"
        }
        assert subscription.cycle == 1

    def test_the_failure_is_logged_with_the_subscription_and_the_keys(
        self, session: orm.Session, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Until the annotation exists, the log line is the only signal a human gets."""
        with caplog.at_level(
            logging.WARNING, logger="cloud_pipelines_backend.triggers.service"
        ):
            subscription = self._fire(
                session, templates={"as_of_date": "{{ schedule_time | date }}"}
            )

        assert any(
            subscription.id in record.message and "as_of_date" in record.message
            for record in caplog.records
        ), caplog.text


class TestEditingTemplatesRescuesASubscriptionStrandedByThem:
    """A template naming an input the pipeline does not declare loses the run but keeps the
    arrival, and editing the templates is the only thing that can start one.

    Run submission refuses the whole run rather than the one argument, so the savepoint takes
    the fence and the half-built run while the filled event stays committed: condition
    satisfied, cycle unspent, nothing running. The emission settled FAIL, so no redelivery is
    behind it and no further emission is owed. Before templates joined the re-evaluation
    conditions the repair saved the new templates and returned, leaving the subscription
    satisfied and dormant for good.
    """

    @staticmethod
    def _stranded(session: orm.Session) -> db_models.TriggerSubscription:
        """A subscription whose only template names an input its target does not declare."""
        subscription = _subscribe(
            session,
            condition=_all(_leaf("a")),
            templates={"as_of_date": "{{ trigger_time | date }}"},
        )
        #: `_subscribe(templates=...)` declares the inputs it templates, so `region` is
        #: attached afterwards to be the one key the target does not declare. One bad key
        #: is enough: submission refuses the run, not the argument.
        subscription.definition = {
            **subscription.definition,
            "pipeline_templates": {
                "as_of_date": "{{ trigger_time | date }}",
                "region": "ca-central-1",
            },
        }
        session.commit()
        _fill(session, subscription=subscription, event="a")

        with session.begin():
            service.maybe_trigger(session=session, subscription=subscription, now=_NOW)
        return subscription

    def test_the_run_is_lost_while_the_arrival_is_kept(
        self, session: orm.Session
    ) -> None:
        """The state the repair has to rescue, pinned so the rescue cannot be read as a
        fix for something that was never broken."""
        subscription = self._stranded(session)
        session.refresh(subscription)

        assert _runs(session) == []
        assert subscription.cycle == 0
        assert _states_of(session, subscription.id) == ["a"]

    def test_clearing_the_bad_template_starts_the_run(
        self, session: orm.Session
    ) -> None:
        """The repair a caller would reach for, and the whole point of the reversal."""
        subscription = self._stranded(session)

        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            templates={},
            now=_NOW,
        )
        session.commit()
        session.refresh(subscription)

        assert result.triggered is True
        assert subscription.cycle == 1
        assert len(_runs(session)) == 1

    def test_correcting_the_template_starts_the_run_with_it(
        self, session: orm.Session
    ) -> None:
        """Repair by fixing rather than deleting: the rescued run carries the new value."""
        subscription = self._stranded(session)

        service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            templates={"as_of_date": "{{ trigger_time | date }}"},
            now=_NOW,
        )
        session.commit()

        (run,) = _runs(session)
        assert dict(run.root_execution.task_spec.get("arguments") or {}) == {
            "as_of_date": _NOW.date().isoformat()
        }

    def test_resending_the_same_templates_does_not_start_a_run(
        self, session: orm.Session
    ) -> None:
        """Compared against what is stored, so a no-op edit stays a no-op: an unrelated
        PATCH that echoes the templates back must not fire the subscription."""
        subscription = _subscribe(
            session,
            condition=_all(_leaf("a")),
            templates={"as_of_date": "{{ trigger_time | date }}"},
        )
        _fill(session, subscription=subscription, event="a")

        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=_CALLER,
            templates={"as_of_date": "{{ trigger_time | date }}"},
            now=_NOW,
        )
        session.commit()
        session.refresh(subscription)

        assert result.triggered is False
        assert subscription.cycle == 0
        assert _runs(session) == []
