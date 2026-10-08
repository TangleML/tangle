"""What the trigger counters record, driven through the real service paths.

Every test calls `service.record_event_and_maybe_start_runs` or `service.update_subscription`
rather than the counter, because what is being pinned is the *placement* of each increment: a
counter incremented one branch too early would still pass a test that called it directly.

Named test_trigger_instruments so the module basename stays unique across the suite — the
repo has no __init__.py in tests and pytest runs in the default prepend import mode, which
keys modules by basename.
"""

import contextlib
import datetime
import itertools
from typing import Any, Final

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend.templating.arguments import sources
from cloud_pipelines_backend.templating.arguments.observability import (
    metrics as template_metrics,
)
from tests.emissions.observability import probes
from cloud_pipelines_backend.triggers import db_models, event_state, service
from cloud_pipelines_backend.triggers.observability import metrics as trigger_metrics
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from cloud_pipelines_backend.utils import db as db_utils

_NOW = datetime.datetime(2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
_HOUR = datetime.timedelta(hours=1)
_SUBSCRIPTION = trigger_metrics.SUBSCRIPTION_ID_LABEL
_EVENT = trigger_metrics.EVENT_NAME_LABEL
_REASON = trigger_metrics.REASON_LABEL
_KIND = template_metrics.KIND_LABEL


def _all(*events: str, expire_seconds: int | None = None) -> dict[str, Any]:
    leaf: list[dict[str, Any]] = []
    for event in events:
        node: dict[str, Any] = {"event": event}
        if expire_seconds is not None:
            node["expire_seconds"] = expire_seconds
        leaf.append(node)
    return {"op": "all", "children": leaf}


# `pipeline` is unique on (user_id, file_path), so every pipeline these helpers mint needs its
# own path; a shared one would fail the second _subscribe in a session on the wrong table.
_pipeline_serial = itertools.count()

# A task spec a run can actually be built from. `None` was enough while the target was inert;
# now that a trigger starts a real run from it, a pipeline with no version fails the trigger.
_RUNNABLE_TASK: dict[str, Any] = {
    "componentRef": {
        "spec": {
            "name": "triggered-target",
            "implementation": {"graph": {"tasks": {}}},
        }
    }
}


#: The same target, declaring the one input `_TEMPLATES` renders. Submission refuses an
#: argument the component does not declare, so the success-path test needs this and the
#: refusal tests deliberately do not use it.
_RUNNABLE_TASK_DECLARING_AS_OF_DATE: dict[str, Any] = {
    "componentRef": {
        "spec": {
            "name": "triggered-target",
            "inputs": [{"name": "as_of_date", "type": "String", "optional": True}],
            "implementation": {"graph": {"tasks": {}}},
        }
    }
}


def _pipeline_id(
    session: orm.Session,
    *,
    runnable: bool = True,
    declares_as_of_date: bool = False,
) -> str:
    """A real pipeline for a subscription to target, and its id.

    Real rather than an invented id: the target column is NOT NULL and a foreign key, and a
    plausible-looking string would only pass because this engine leaves SQLite's foreign keys
    switched off.

    Args:
        session: the session the rows are written on.
        runnable: whether to give the pipeline a current version. False mints a pipeline that
            exists but cannot be run from, which is what `trigger.run_not_started` counts.
        declares_as_of_date: whether the target declares the input `_TEMPLATES` renders, so a
            rendered argument is accepted rather than refused at submission.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id="test-owner",
        file_path=f"pipelines/auto-{next(_pipeline_serial)}.py",
    )
    session.add(pipeline)
    session.flush()
    if not runnable:
        return pipeline.id
    session.add(
        user_pipeline_db_models.UserPipelineVersion(
            pipeline_id=pipeline.id,
            version_key=user_pipeline_db_models.CURRENT_VERSION_KEY,
            content_digest="d" * user_pipeline_db_models.DIGEST_LENGTH,
            root_pipeline_task=(
                _RUNNABLE_TASK_DECLARING_AS_OF_DATE
                if declares_as_of_date
                else _RUNNABLE_TASK
            ),
        )
    )
    session.flush()
    # Pointed at the version only after it exists. `current_version_key` is half of a composite
    # foreign key into `user_pipeline_version`, so setting it on the insert fails wherever
    # SQLite's foreign keys are switched on.
    pipeline.current_version_key = user_pipeline_db_models.CURRENT_VERSION_KEY
    session.flush()
    return pipeline.id


def _subscribe(
    session: orm.Session,
    *,
    condition: dict[str, Any],
    runnable: bool = True,
    name: str = "nightly-retrain",
    templates: dict[str, str] | None = None,
    declares_as_of_date: bool = False,
) -> db_models.TriggerSubscription:
    # (created_by, name) is unique and this helper stamps a single created_by, so a test that
    # wants two subscriptions has to name them apart.
    definition: dict[str, Any] = {"name": name, "condition": condition}
    if templates is not None:
        # Subscription templates live inside the definition blob, not a column of their own.
        definition["pipeline_templates"] = templates
    subscription = db_models.TriggerSubscription(
        name=name,
        definition=definition,
        created_by="test-owner",
        pipeline_task_spec_from_user_pipeline_id=_pipeline_id(
            session, runnable=runnable, declares_as_of_date=declares_as_of_date
        ),
    )
    session.add(subscription)
    session.flush()
    event_state.sync(
        session=session, subscription_id=subscription.id, condition=condition
    )
    session.commit()
    return subscription


def _history_rows(
    session: orm.Session, *, subscription: db_models.TriggerSubscription
) -> int:
    """How many trigger_history rows this subscription actually has, durably."""
    session.expire_all()
    return len(
        session.scalars(
            sqlalchemy.select(db_models.TriggerHistory).where(
                db_models.TriggerHistory.subscription_id == subscription.id
            )
        ).all()
    )


class TestTheNamesTheModuleDeclares:
    """The fixture re-declares instruments by name, so a rename must not slip past it."""

    def test_every_counter_is_declared_under_the_name_the_fixture_uses(
        self, counter_names: dict[str, str]
    ) -> None:
        # Read off the module's own instruments rather than the ones the fixture rebuilds,
        # so this fails if a name drifts from what the tests record under. With no provider
        # installed these are proxy instruments, which keep their name privately — there is
        # no public accessor to read it back from.
        declared = {
            attribute: getattr(trigger_metrics, attribute)._name
            for attribute in counter_names
        }

        assert declared == counter_names


class TestCountingAnArrival:
    def test_a_recorded_arrival_counts_once_for_its_own_event(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.event_filled",
            attributes={_SUBSCRIPTION: subscription.id, _EVENT: "orders-ready"},
        )
        assert point is not None
        assert point.value == 1
        assert (
            metrics.point(
                name="trigger.event_filled",
                attributes={_SUBSCRIPTION: subscription.id, _EVENT: "fx-ready"},
            )
            is None
        )

    def test_a_redelivery_writes_nothing_and_counts_nothing(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        for _ in range(2):
            service.record_event_and_maybe_start_runs(
                session=session,
                event_name="orders-ready",
                emission_event_id="em-1",
                now=_NOW,
            )

        point = metrics.point(
            name="trigger.event_filled",
            attributes={_SUBSCRIPTION: subscription.id, _EVENT: "orders-ready"},
        )
        assert point is not None
        assert point.value == 1

    def test_nothing_is_counted_when_no_subscription_waits(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        _subscribe(session, condition=_all("orders-ready"))

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="unrelated-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert metrics.points(name="trigger.event_filled") == []


class TestCountingATrigger:
    def test_the_arrival_that_completes_the_condition_counts_a_trigger(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        assert metrics.points(name="trigger.triggered") == []

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="fx-ready",
            emission_event_id="em-2",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.triggered",
            attributes={_SUBSCRIPTION: subscription.id},
        )
        assert point is not None
        assert point.value == 1

    def test_an_edit_that_starts_a_run_counts_the_same_trigger(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The edit path reaches the counter through maybe_trigger, not past it."""
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        with session.begin():
            service.update_subscription(
                session=session,
                subscription=subscription,
                caller=service.Caller(name="test-owner"),
                condition={"event": "orders-ready"},
            )

        point = metrics.point(
            name="trigger.triggered",
            attributes={_SUBSCRIPTION: subscription.id},
        )
        assert point is not None
        assert point.value == 1

    def test_a_disabled_subscription_records_the_arrival_and_no_trigger(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        subscription = _subscribe(session, condition=_all("orders-ready"))
        subscription.enabled = False
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert (
            metrics.point(
                name="trigger.event_filled",
                attributes={
                    _SUBSCRIPTION: subscription.id,
                    _EVENT: "orders-ready",
                },
            ).value
            == 1
        )
        assert metrics.points(name="trigger.triggered") == []


class TestCountingTheSilentFailure:
    def test_an_arrival_landing_beside_a_lapsed_one_counts_the_lapse(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The expiry-too-short signature: one event lapses while the next is still coming."""
        subscription = _subscribe(
            session,
            condition=_all("orders-ready", "fx-ready", expire_seconds=1800),
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        assert metrics.points(name="trigger.event_expired") == []

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="fx-ready",
            emission_event_id="em-2",
            now=_NOW + _HOUR,
        )

        point = metrics.point(
            name="trigger.event_expired",
            attributes={_SUBSCRIPTION: subscription.id, _EVENT: "orders-ready"},
        )
        assert point is not None
        assert point.value == 1
        assert metrics.points(name="trigger.triggered") == []

    def test_a_satisfied_condition_counts_no_lapse(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="fx-ready",
            emission_event_id="em-2",
            now=_NOW,
        )

        assert metrics.points(name="trigger.event_expired") == []

    def test_a_lapse_is_counted_again_by_the_next_evaluation_that_sees_it(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """A rate of noticing, not of lapsing: nothing sweeps the row, so it is re-counted."""
        subscription = _subscribe(
            session,
            condition=_all("orders-ready", "fx-ready", expire_seconds=1800),
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="fx-ready",
            emission_event_id="em-2",
            now=_NOW + _HOUR,
        )

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="fx-ready",
            emission_event_id="em-3",
            now=_NOW + 2 * _HOUR,
        )

        point = metrics.point(
            name="trigger.event_expired",
            attributes={_SUBSCRIPTION: subscription.id, _EVENT: "orders-ready"},
        )
        assert point.value == 2


class TestCountingALostFence:
    def test_the_writer_that_loses_the_fence_is_counted(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The loser writes nothing and reports success, so this counter is its only trace."""
        subscription = _subscribe(session, condition=_all("orders-ready"))
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription.id, cycle=subscription.cycle
            )
        )
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.cycle_collisions",
            attributes={_SUBSCRIPTION: subscription.id},
        )
        assert point is not None
        assert point.value == 1
        assert metrics.points(name="trigger.triggered") == []


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


# Present, well-formed JSON, and still not a TaskSpec: `image` must be a string. Nothing before
# the run build can notice, which is why this reaches `_trigger`'s ValidationError clause and
# not the liveness check.
_UNBUILDABLE_TASK: dict[str, Any] = {
    "componentRef": {"spec": {"implementation": {"container": {"image": 5}}}}
}


def _break_target_spec(
    session: orm.Session, *, subscription: db_models.TriggerSubscription
) -> None:
    """Leave the target alive and its current version holding a spec that will not parse."""
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


def _raising(message: str):
    """A stand-in for the run creator that fails before writing anything."""

    def fail(self, **kwargs):
        del self, kwargs
        raise RuntimeError(message)

    return fail


class TestCountingAPermanentFailure:
    """The condition held and the run could not start — the one outcome nothing else reports.

    Every other non-trigger is either expected or self-clearing. This one settles the emission
    `FAIL` with no redelivery, so the subscription is stopped until a human repoints it, and
    the counter is what says so before someone notices the run never ran.
    """

    def test_a_deleted_target_counts_the_failure_under_its_reason(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        subscription = _subscribe(session, condition=_all("orders-ready"))
        _soft_delete_target(session, subscription=subscription)

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.run_not_started",
            attributes={
                _SUBSCRIPTION: subscription.id,
                _REASON: service.TriggerReason.USER_PIPELINE_DELETED,
            },
        )
        assert point is not None
        assert point.value == 1
        # The reason is on the counter and not only in the log line, so an alert can fire on
        # this series without parsing anything.
        assert metrics.points(name="trigger.triggered") == []

    def test_a_target_with_no_runnable_version_counts_the_same_failure(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """A pipeline row that exists but cannot be run from fails the same way a tombstone does."""
        subscription = _subscribe(
            session, condition=_all("orders-ready"), runnable=False
        )

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.run_not_started",
            attributes={
                _SUBSCRIPTION: subscription.id,
                _REASON: service.TriggerReason.USER_PIPELINE_DELETED,
            },
        )
        assert point is not None
        assert point.value == 1

    def test_an_unbuildable_spec_counts_the_failure_under_its_own_reason(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """A live target whose stored spec no longer parses is the second permanent stop.

        Its own `reason` and not the deleted one: both settle the emission FAIL with no
        redelivery, but a different person repairs each -- the pipeline's owner re-saves the
        spec, where a deleted target is repointed by the subscriber -- so an alert that could
        not tell them apart would page the wrong one.
        """
        subscription = _subscribe(session, condition=_all("orders-ready"))
        _break_target_spec(session, subscription=subscription)

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.run_not_started",
            attributes={
                _SUBSCRIPTION: subscription.id,
                _REASON: service.TriggerReason.TARGET_UNBUILDABLE,
            },
        )
        assert point is not None
        assert point.value == 1
        assert (
            metrics.point(
                name="trigger.run_not_started",
                attributes={
                    _SUBSCRIPTION: subscription.id,
                    _REASON: service.TriggerReason.USER_PIPELINE_DELETED,
                },
            )
            is None
        )
        assert metrics.points(name="trigger.triggered") == []

    def test_an_unexpected_run_build_failure_counts_the_failure_under_its_own_reason(
        self,
        session: orm.Session,
        metrics: probes.MetricsProbe,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The backstop clause is the third permanent stop, and the one with no other trace.

        Its cause has no name, so without this the only evidence is a log line: the emission is
        settled FAIL, nothing redelivers, and the subscription is stopped silently.
        """
        subscription = _subscribe(session, condition=_all("orders-ready"))
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising("something nobody predicted"),
        )

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        point = metrics.point(
            name="trigger.run_not_started",
            attributes={
                _SUBSCRIPTION: subscription.id,
                _REASON: service.TriggerReason.RUN_START_FAILED,
            },
        )
        assert point is not None
        assert point.value == 1
        assert metrics.points(name="trigger.triggered") == []

    def test_an_ordinary_wait_counts_nothing(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """Alertable on any non-zero rate only if the ordinary path never touches it."""
        _subscribe(session, condition=_all("orders-ready", "fx-ready"))

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert metrics.points(name="trigger.run_not_started") == []

    def test_a_lost_fence_is_not_counted_as_a_permanent_failure(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The two failure counters answer different questions and must not merge.

        A lost fence means the run already started — someone else's. A dead target means it
        never will. Counting both here would make the series unalertable.
        """
        subscription = _subscribe(session, condition=_all("orders-ready"))
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription.id, cycle=subscription.cycle
            )
        )
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert (
            metrics.point(
                name="trigger.cycle_collisions",
                attributes={_SUBSCRIPTION: subscription.id},
            ).value
            == 1
        )
        assert metrics.points(name="trigger.run_not_started") == []

    def test_an_edit_that_finds_a_dead_target_counts_the_failure(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The edit path reaches the counter through maybe_trigger, not past it.

        A `PATCH` cannot *set* a dead target — the resolver 4xxs first — but a condition edit
        on a subscription whose target died since reaches `_trigger` and fails there.
        """
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        _soft_delete_target(session, subscription=subscription)

        with session.begin():
            service.update_subscription(
                session=session,
                subscription=subscription,
                caller=service.Caller(name="test-owner"),
                condition={"event": "orders-ready"},
            )

        point = metrics.point(
            name="trigger.run_not_started",
            attributes={
                _SUBSCRIPTION: subscription.id,
                _REASON: service.TriggerReason.USER_PIPELINE_DELETED,
            },
        )
        assert point is not None
        assert point.value == 1

    def test_one_dead_target_is_counted_without_touching_the_live_one(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """Both subscriptions wait on the same event; only the stopped one is counted."""
        dead = _subscribe(session, condition=_all("orders-ready"), name="dead")
        _soft_delete_target(session, subscription=dead)
        live = _subscribe(session, condition=_all("orders-ready"), name="live")

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert (
            metrics.point(
                name="trigger.run_not_started",
                attributes={
                    _SUBSCRIPTION: dead.id,
                    _REASON: service.TriggerReason.USER_PIPELINE_DELETED,
                },
            ).value
            == 1
        )
        assert (
            metrics.point(
                name="trigger.triggered", attributes={_SUBSCRIPTION: live.id}
            ).value
            == 1
        )
        assert (
            metrics.point(
                name="trigger.run_not_started",
                attributes={_SUBSCRIPTION: live.id},
            )
            is None
        )


class TestCountsFollowTheTransaction:
    """A counter cannot be rolled back, so nothing is counted until the write is durable."""

    def test_a_trigger_whose_transaction_rolls_back_is_not_counted(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        # The edit path is where this bites hardest: `update_subscription` decides, and the
        # route commits afterwards, so anything that fails in between would leave a trigger
        # counted that no `trigger_history` row backs.
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        session.begin()
        service.update_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
            condition={"event": "orders-ready"},
        )
        session.rollback()

        assert _history_rows(session, subscription=subscription) == 0
        assert metrics.points(name="trigger.triggered") == []

    def test_a_permanent_failure_whose_transaction_rolls_back_is_not_counted(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        # A failure counted before the arrival is durable reports a subscription as stopped
        # that is about to be retried: the emission consumer reclaims a delivery whose
        # transaction never committed, and the next attempt counts it again.
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        _soft_delete_target(session, subscription=subscription)

        session.begin()
        service.update_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
            condition={"event": "orders-ready"},
        )
        session.rollback()

        assert metrics.points(name="trigger.run_not_started") == []

    def test_an_unbuildable_target_whose_transaction_rolls_back_is_not_counted(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        # Each permanent branch queues its own count, so each needs its own proof that it
        # queued rather than recorded: a count taken inside the branch would report a stopped
        # subscription that the next attempt is about to build successfully.
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        _break_target_spec(session, subscription=subscription)

        session.begin()
        service.update_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
            condition={"event": "orders-ready"},
        )
        session.rollback()

        assert metrics.points(name="trigger.run_not_started") == []

    def test_an_unexpected_failure_whose_transaction_rolls_back_is_not_counted(
        self,
        session: orm.Session,
        metrics: probes.MetricsProbe,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The backstop branch, same rule. Its cause is unnamed, which includes "transient and
        # about to succeed", so counting before the write is durable is worst here.
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising("something nobody predicted"),
        )

        session.begin()
        service.update_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
            condition={"event": "orders-ready"},
        )
        session.rollback()

        assert metrics.points(name="trigger.run_not_started") == []

    def test_a_count_survives_the_savepoint_the_fence_rolls_back(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        # `_trigger` wraps its history insert in a SAVEPOINT and rolls that back when it loses
        # the race — while the arrival around it still commits. A queue emptied by any rollback
        # rather than by a real one would lose the fill this recorded on the way in.
        subscription = _subscribe(session, condition=_all("orders-ready"))
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription.id, cycle=subscription.cycle
            )
        )
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        filled = metrics.point(
            name="trigger.event_filled",
            attributes={
                _SUBSCRIPTION: subscription.id,
                trigger_metrics.EVENT_NAME_LABEL: "orders-ready",
            },
        )
        assert filled is not None
        assert filled.value == 1

    def test_a_rolled_back_count_does_not_leak_into_the_next_commit(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        # A rollback that only *keeps* the queue is not a discard: the counts sit on
        # `Session.info` until something else commits on the same session and records them.
        # In the fan-out that something else is the next subscription's transaction, so the
        # trigger this one rolled back gets counted a moment later anyway.
        subscription = _subscribe(session, condition=_all("orders-ready", "fx-ready"))
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        session.begin()
        service.update_subscription(
            session=session,
            subscription=subscription,
            caller=service.Caller(name="test-owner"),
            condition={"event": "orders-ready"},
        )
        session.rollback()

        # The next commit on this session, in the shape the fan-out makes it: somebody else's.
        _subscribe(session, condition=_all("fx-ready"), name="somebody-elses")

        assert _history_rows(session, subscription=subscription) == 0
        assert metrics.points(name="trigger.triggered") == []


class TestTheQueueWaitsForTheOutermostTransaction:
    """The listeners' own behaviour, at the two points no service path reaches today.

    Driven through `record_after_commit` rather than a service call, unlike every other test
    here. `_trigger` holds the only SAVEPOINT in the trigger code and queues its counts after
    the block rather than inside it, so a released SAVEPOINT cannot currently drain anything
    real — the guard is what keeps that true of the next caller, and this is where it is
    pinned.
    """

    def test_releasing_a_savepoint_does_not_record_early(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        session.begin()
        trigger_metrics.record_after_commit(
            session=session,
            counter=trigger_metrics.triggered,
            attributes={_SUBSCRIPTION: "sub-1"},
        )

        with session.begin_nested():
            session.execute(sqlalchemy.text("SELECT 1"))

        assert metrics.points(name="trigger.triggered") == []
        session.rollback()
        assert metrics.points(name="trigger.triggered") == []

    def test_the_outermost_commit_after_a_release_still_records(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        session.begin()
        trigger_metrics.record_after_commit(
            session=session,
            counter=trigger_metrics.triggered,
            attributes={_SUBSCRIPTION: "sub-1"},
        )

        with session.begin_nested():
            session.execute(sqlalchemy.text("SELECT 1"))
        session.commit()

        point = metrics.point(
            name="trigger.triggered", attributes={_SUBSCRIPTION: "sub-1"}
        )
        assert point is not None
        assert point.value == 1

    def test_a_savepoint_rollback_leaves_the_queue_for_the_outer_commit(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        session.begin()
        trigger_metrics.record_after_commit(
            session=session,
            counter=trigger_metrics.triggered,
            attributes={_SUBSCRIPTION: "sub-1"},
        )

        with contextlib.suppress(RuntimeError):
            with session.begin_nested():
                session.execute(sqlalchemy.text("SELECT 1"))
                raise RuntimeError("the fence lost its race")
        assert metrics.points(name="trigger.triggered") == []

        session.commit()

        point = metrics.point(
            name="trigger.triggered", attributes={_SUBSCRIPTION: "sub-1"}
        )
        assert point is not None
        assert point.value == 1


class TestABrokenInstrument:
    def test_a_counter_that_raises_does_not_stop_the_trigger(
        self,
        session: orm.Session,
        metrics: probes.MetricsProbe,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Measuring a trigger must never change whether it happened."""

        class _Broken:
            def add(self, _amount: int, attributes: dict[str, str]) -> None:
                raise RuntimeError("exporter is down")

        monkeypatch.setattr(trigger_metrics, "triggered", _Broken())
        subscription = _subscribe(session, condition=_all("orders-ready"))

        fan_out = service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert fan_out.outcomes[0].result.triggered is True
        assert session.get(db_models.TriggerSubscription, subscription.id).cycle == 1


#: One key, valid, and cheap to render. `trigger_time` is the subscription clock's own source;
#: `schedule_time` is unavailable here by design, so a template naming it would be refused at
#: save rather than reaching the renderer.
_TEMPLATES: Final[dict[str, str]] = {"as_of_date": "{{ trigger_time | date }}"}


class TestRenderedKeysCountOnlyWhenARunStarts:
    """`template.keys_rendered` is a count of keys that reached a run, not of renders attempted.

    Every early exit in `_trigger` renders first and starts nothing. Counting at the render
    site inflated all of them, and the lost fence doubled: the winner renders the same
    templates and counts them in its own session, so one real run was counted twice.
    """

    def test_a_firing_that_starts_a_run_counts_its_rendered_keys(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The control. Without this, the two tests below pass if rendering never happens."""
        subscription = _subscribe(
            session,
            condition=_all("orders-ready"),
            templates=_TEMPLATES,
            declares_as_of_date=True,
        )

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        triggered = metrics.point(
            name="trigger.triggered",
            attributes={_SUBSCRIPTION: subscription.id},
        )
        assert triggered is not None and triggered.value == 1
        assert sum(
            point.value for point in metrics.points(name="template.keys_rendered")
        ) == len(_TEMPLATES)

    def test_a_lost_fence_counts_the_collision_and_no_rendered_key(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The double-count: the winner already counted these keys in its own session."""
        subscription = _subscribe(
            session, condition=_all("orders-ready"), templates=_TEMPLATES
        )
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription.id, cycle=subscription.cycle
            )
        )
        session.commit()

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        collision = metrics.point(
            name="trigger.cycle_collisions",
            attributes={_SUBSCRIPTION: subscription.id},
        )
        assert collision is not None and collision.value == 1
        assert metrics.points(name="template.keys_rendered") == []

    def test_a_deleted_target_counts_the_failure_and_no_rendered_key(
        self, session: orm.Session, metrics: probes.MetricsProbe
    ) -> None:
        """The render happened; nothing consumed it, so it is not a key that reached a run."""
        subscription = _subscribe(
            session, condition=_all("orders-ready"), templates=_TEMPLATES
        )
        _soft_delete_target(session, subscription=subscription)

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert metrics.points(name="trigger.run_not_started") != []
        assert metrics.points(name="template.keys_rendered") == []


class TestSubmissionRejectedCountsOnlyTemplating:
    """`template.submission_rejected` answers a question about templating, so a subscription
    that templates nothing must not move it.

    The branch it fires from is the fan-out backstop: it catches anything the run start
    raised, including an IntegrityError from the insert, none of which templating caused.
    The schedule path carries the same guard.
    """

    @staticmethod
    def _fire_with_a_failing_run_start(
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
        *,
        templates: dict[str, str] | None,
    ) -> None:
        subscription = _subscribe(
            session,
            condition=_all("orders-ready"),
            templates=templates,
            declares_as_of_date=templates is not None,
        )
        del subscription
        monkeypatch.setattr(
            user_pipeline_services.UserPipelineService,
            "create_from_pipeline_no_commit",
            _raising("the run insert collided"),
        )
        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

    def test_a_template_free_subscription_whose_run_start_fails_counts_nothing(
        self,
        session: orm.Session,
        metrics: probes.MetricsProbe,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The regression: this failure has nothing to do with templating."""
        self._fire_with_a_failing_run_start(session, monkeypatch, templates=None)

        #: The failure really did happen -- without this the assertion below passes
        #: because nothing was tried at all.
        assert metrics.points(name="trigger.run_not_started") != []
        assert metrics.points(name="template.submission_rejected") == []

    def test_a_templated_subscription_whose_run_start_fails_is_still_counted(
        self,
        session: orm.Session,
        metrics: probes.MetricsProbe,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The guard must not silence the case it exists to report."""
        self._fire_with_a_failing_run_start(session, monkeypatch, templates=_TEMPLATES)

        assert metrics.points(name="trigger.run_not_started") != []
        rejected = metrics.point(
            name="template.submission_rejected",
            attributes={_KIND: sources.Kind.SUBSCRIPTION.value},
        )
        assert rejected is not None and rejected.value == 1

    def test_a_firing_that_starts_a_run_rejects_nothing(
        self,
        session: orm.Session,
        metrics: probes.MetricsProbe,
    ) -> None:
        """The control: the counter is silent on the path that works."""
        _subscribe(
            session,
            condition=_all("orders-ready"),
            templates=_TEMPLATES,
            declares_as_of_date=True,
        )

        service.record_event_and_maybe_start_runs(
            session=session,
            event_name="orders-ready",
            emission_event_id="em-1",
            now=_NOW,
        )

        assert metrics.points(name="template.submission_rejected") == []
