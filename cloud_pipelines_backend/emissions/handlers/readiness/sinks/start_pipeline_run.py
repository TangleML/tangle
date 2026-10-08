"""The readiness sink: the port a readiness signal leaves the emission path through."""

import enum
import logging
import time
import typing

from sqlalchemy import orm

from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.dispatching.handlers.sinks import base as sinks_base
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness_annotations,
)
from cloud_pipelines_backend.triggers import service as trigger_service
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services

logger = logging.getLogger(__name__)

_SINK_KEY = "start_pipeline_run"


class _DeliveryReason(str, enum.Enum):
    """Why a whole delivery ended the way it did -- one of these per emission through this sink.

    A different vocabulary from `trigger_service.TriggerReason`, which answers per subscription,
    and the two appear together: a `RUNS_NOT_STARTED` detail carries a `TriggerReason` for every
    subscription listed under `failed`. Keeping them apart is what lets each be exhaustive for
    the key it fills.

    A `(str, enum.Enum)` mixin because the OSS backend still supports Python 3.10, with `__str__`
    restored for the same reason as its counterpart: the values are read back out of stored
    outcome details and interpolated into logs, so they must render as themselves.
    """

    __str__ = str.__str__

    # Some subscriptions were recorded and the rest were given up on.
    FAN_OUT_INCOMPLETE = "fan_out_incomplete"
    # Distinct from `FAN_OUT_INCOMPLETE`: those subscriptions never got the signal and a retry
    # can still deliver it. These got it and could not act on it, and no retry changes that.
    RUNS_NOT_STARTED = "runs_not_started"


# Small, bounded, and in-process because nothing outside this call will do it: a raise from a
# sink is converted to a failed verdict and the emission row is settled, so there is no
# redelivery to fall back on. Three attempts with a short backoff covers a deadlock victim,
# which resolves the moment the winner commits; a longer ladder would hold the consumer's
# claim while the queue backs up behind it.
_MAX_ATTEMPTS: typing.Final[int] = 3
_BACKOFF_SECONDS: typing.Final[float] = 0.05

# How long one emission's whole fan-out may keep taking on new subscriptions -- not a per
# subscription limit and not a per attempt one. The retries above share this budget, because
# the thing being bounded is the consumer's claim: one emission with a long enough subscription
# list holds the poll loop, and everything behind it in the queue waits on work that is not
# urgent. Stopping early costs the unserved subscriptions a redelivery, which is cheap; the
# ones already served cost two reads each on the way back round.
#
# Well under the claim lease (`emissions.consumer.CLAIM_EXPIRES_AFTER_SECONDS`, 300s) on
# purpose: the subscription in flight when the budget expires still runs to completion, and the
# settle after it has to land while this consumer still holds the row.
_FAN_OUT_BUDGET_SECONDS: typing.Final[float] = 30.0


class StartPipelineRunSink(sinks_base.Sink[readiness_annotations.ReadinessIntent]):
    """Records a readiness signal against the trigger subscriptions waiting for it.

    The emission path's one write into the trigger tables. Everything it decides is delegated:
    `triggers.service.record_event_and_maybe_start_runs` writes the event state and, per
    subscription, asks `maybe_trigger` whether the condition now holds — the same call a
    `PATCH` makes, so an arrival and an edit cannot disagree about what "satisfied" means.

    Reports SUCCESS whenever the arrival was recorded, even though usually nothing triggers:
    recording it is the work. IGNORE is reserved for having found nothing to record at all.
    """

    def __init__(
        self,
        *,
        session_factory: orm.sessionmaker,
        pipeline_service: user_pipeline_services.UserPipelineService | None = None,
    ) -> None:
        """Hold the factory each announcement opens its own session from.

        The session is per-call rather than per-sink: this runs in a consumer whose poll loop
        is long-lived, and a session held across it would keep one connection checked out and
        one snapshot of the trigger tables alive for the life of the process.

        Args:
            session_factory: Factory for the session each `emit` opens.
        """
        self._session_factory = session_factory
        self._pipeline_service = pipeline_service

    def emit(
        self,
        *,
        intent: readiness_annotations.ReadinessIntent,
        execution_node_id: str,
        emission_event_id: str,
    ) -> handler_base.Outcome:
        """Record the arrival against every subscription waiting on the intent's event key.

        Idempotent, which the sink contract asks of anything doing more than logging, and by
        two different mechanisms because they cover different races. A *redelivery* of the same
        emission is recognised by the event state remembering which arrival it last saw, so it
        is reported rather than replayed — the fence cannot help there, since a trigger has
        already moved the cycle on. A *concurrent* second writer racing for the same cycle is
        what the fence on `(subscription_id, cycle)` catches, reporting
        `cycle_already_triggered`.

        Reported, and reported *truthfully*: a redelivery whose first delivery started a run
        comes back `triggered: true` with that run's id and cycle (`run_already_started`), not
        as a subscription that did nothing. This matters precisely here, because a redelivery
        is what a failed outcome write causes — so the delivery most likely to be answering for
        an already-started run is the one whose answer becomes the permanent ledger row.

        One emission can reach several subscriptions with different answers, and a delivery has
        one verdict, so the verdict is the most that happened:

            nothing waits on the event key            IGNORE  no_subscription
            every arrival was already recorded        SUCCESS arrival_already_recorded
            everything waiting is disabled            SUCCESS subscription_disabled
            at least one arrival was recorded         SUCCESS per-subscription reasons in detail
            a write kept failing after the retries    FAIL    the error
            part of the fan-out never recorded        FAIL    fan_out_incomplete

        IGNORE means "ran and found nothing actionable", and only one case qualifies: nobody is
        listening, so no event state exists to write to. A disabled subscription is still
        listening — the arrival lands on its event state and only the run is withheld — so that
        delivery is a SUCCESS carrying `subscription_disabled` per subscription. Anything
        recorded is SUCCESS even when no condition held, because recording the arrival *is* the
        work; `awaiting_events` and `cycle_already_triggered` are ordinary outcomes, not
        degradations, and they are reported per subscription rather than collapsed into the
        status.

        Args:
            intent: The validated readiness intent to announce.
            execution_node_id: Unused; the readiness record needs nothing beyond the intent.
            emission_event_id: The emission being announced, recorded on each event state it
                fills so a trigger can name the arrivals that completed its condition.

        Returns:
            The verdict above, with the per-subscription decisions in `detail`.
        """
        try:
            fan_out = self._record_event_and_maybe_start_runs_with_retries(
                intent=intent, emission_event_id=emission_event_id
            )
        except trigger_service.FanOutIncomplete as error:
            # Translated at the boundary rather than let through raw: above this line the
            # emission path knows nothing about subscriptions, and the fact it does need is the
            # generic one -- this delivery is unfinished, do not settle it. Raised, not
            # reported, because every Outcome this method can return settles the emission and
            # the subscriptions this pass did not reach would never hear the signal.
            logger.info(
                f"readiness recording paused: event_key={intent.event_key}"
                f" emission={emission_event_id} served={len(error.served)}"
                f" remaining={len(error.remaining)}; awaiting redelivery"
            )
            raise handler_base.DeliveryIncomplete(str(error)) from error
        # The failure set is the fan-out's, not restated here: it is the layer that catches
        # these now, so a sink retrying a different set would leave a gap between them.
        except trigger_service.RETRYABLE_WRITE_FAILURES as error:
            # Reported rather than raised: the contract asks a sink to report an expected
            # failure, and a raise would be converted to the same verdict with less in it.
            # Nothing was recorded, and this is the only path where that is still true: the
            # fan-out contains a per-subscription failure and hands it back in `deferred`, so
            # what reaches here is the seek and the commit that precede the loop, before any
            # subscription has been written. A partial fan-out leaves through the branch below.
            logger.exception(
                f"readiness recording failed: event_key={intent.event_key} emission={emission_event_id}"
            )
            return handler_base.Outcome(
                status=handler_base.OutcomeStatus.FAIL,
                detail={
                    "sink": _SINK_KEY,
                    "event_key": intent.event_key,
                    "error": repr(error),
                },
            )

        subscription_outcomes = fan_out.outcomes
        if fan_out.deferred:
            # Checked before the empty case below, and not folded into it: a fan-out where every
            # subscription was contended also has no outcomes, and reporting that as "nobody was
            # listening" would turn a lost signal into a routine IGNORE.
            #
            # FAIL because it is one, and both lists are named because recording a FAIL settles
            # the emission: there is no redelivery behind this, so this detail is the only trace
            # of which subscriptions missed the signal and which kept it.
            logger.error(
                f"readiness recording incomplete: event_key={intent.event_key}"
                f" emission={emission_event_id} recorded={len(subscription_outcomes)}"
                f" deferred={len(fan_out.deferred)}"
            )
            return handler_base.Outcome(
                status=handler_base.OutcomeStatus.FAIL,
                detail={
                    "sink": _SINK_KEY,
                    "event_key": intent.event_key,
                    "reason": _DeliveryReason.FAN_OUT_INCOMPLETE,
                    "recorded": [
                        outcome.subscription_id for outcome in subscription_outcomes
                    ],
                    "deferred": list(fan_out.deferred),
                },
            )

        if fan_out.failed:
            # The arrivals committed — every one of them, including these — so this is not a
            # lost signal. It is a permanent refusal to start a run, and it is reported FAIL for
            # the same reason as the branch above: recording a verdict settles the emission, so
            # this detail is the only trace anyone gets. Recovery is a human acting on the
            # subscription or its target -- repointing it, re-saving a pipeline whose stored
            # spec no longer builds, fixing whatever a `run_start_failed` turns out to be --
            # which re-evaluates the condition still sitting satisfied in the event state and
            # starts the run then. The per-subscription `reason` says which.
            by_id = {
                outcome.subscription_id: outcome for outcome in subscription_outcomes
            }
            logger.error(
                f"readiness runs not started: event_key={intent.event_key}"
                f" emission={emission_event_id} recorded={len(subscription_outcomes)}"
                f" failed={len(fan_out.failed)}"
            )
            return handler_base.Outcome(
                status=handler_base.OutcomeStatus.FAIL,
                detail={
                    "sink": _SINK_KEY,
                    "event_key": intent.event_key,
                    "reason": _DeliveryReason.RUNS_NOT_STARTED,
                    "recorded": [
                        outcome.subscription_id for outcome in subscription_outcomes
                    ],
                    "failed": [
                        {
                            "subscription_id": subscription_id,
                            "reason": by_id[subscription_id].result.reason,
                            "error": by_id[subscription_id].result.error,
                        }
                        for subscription_id in fan_out.failed
                    ],
                },
            )

        if not subscription_outcomes:
            return self._nothing_recorded(
                intent=intent,
                reason=trigger_service.TriggerReason.NO_SUBSCRIPTION,
            )
        triggered = [
            outcome for outcome in subscription_outcomes if outcome.result.triggered
        ]
        logger.info(
            f"readiness recorded: event_key={intent.event_key} emission={emission_event_id}"
            f" subscriptions={len(subscription_outcomes)} triggered={len(triggered)}"
        )
        return handler_base.Outcome(
            status=handler_base.OutcomeStatus.SUCCESS,
            detail={
                "sink": _SINK_KEY,
                "event_key": intent.event_key,
                "subscriptions": [
                    {
                        "subscription_id": outcome.subscription_id,
                        "triggered": outcome.result.triggered,
                        "cycle": outcome.result.cycle,
                        "reason": outcome.result.reason,
                        "pipeline_run_id": outcome.result.pipeline_run_id,
                    }
                    for outcome in subscription_outcomes
                ],
            },
        )

    def _record_event_and_maybe_start_runs_with_retries(
        self,
        *,
        intent: readiness_annotations.ReadinessIntent,
        emission_event_id: str,
    ) -> trigger_service.FanOut:
        """Run the fan-out, retrying the write failures that a later attempt can survive.

        A pass-through onto `trigger_service.record_event_and_maybe_start_runs` and nothing more
        -- the echoed name is the point. Every decision, including whether a run starts, belongs
        to the fan-out; this method only decides whether to ask it again.

        The retry lives here because nothing above it will do it: a raise out of a sink becomes
        a failed verdict and the emission row is settled, so there is no redelivery to lean on.
        A deadlock victim is the case this exists for, and it clears as soon as the writer that
        won commits.

        There are two failures to retry and they arrive differently. One is a *contended
        subscription*, which the fan-out contains and reports in `deferred` — no exception is
        raised, so the retry has to be driven off that list. The other is a failure *before* the
        fan-out's loop starts, opening the session or running the seek, which still raises
        because nothing has been written yet and there is nothing partial to report.

        Re-running the whole fan-out is what a retry does, and that is safe rather than merely
        tolerable: `event_state.fill` recognises an arrival already recorded, so a subscription
        that landed on an earlier attempt is a no-op on the next one.

        Each attempt opens its own session. Reusing one after a rolled-back transaction would
        retry with a connection whose state is already suspect, and the read the retry starts
        with has to see what the winner committed.

        Returns:
            Every subscription recorded across the attempts, and those still deferred when the
            attempts ran out.

        Raises:
            The last failure, once the attempts are used up — but only when nothing was ever
            recorded, since a partial fan-out is a result, not an error.
            trigger_service.FanOutIncomplete: passed straight through, and deliberately not
                retried. It is the fan-out saying it is out of time, so asking it again on the
                same budget is the one response guaranteed not to help. What this does throw
                away is `recorded` — every arrival in it is already committed, and comes back
                on the redelivery as `arrival_already_recorded` or `run_already_started`.
        """
        # Keyed by subscription and first-write-wins, because a retry re-runs the whole fan-out
        # and `fill` dedups: a subscription that triggered on attempt 1 comes back refused on
        # attempt 2. The refusal now carries the truth -- `run_already_started` with the run's
        # id -- so keeping the later answer would no longer deny the trigger, but attempt 1's
        # answer is still the fuller one: it is the call that evaluated, so it alone carries
        # `matched_events` and `missing`.
        #
        # This dict is also why the refusal had to learn the truth in the first place. It is
        # in-memory, so it dedups the attempts of one delivery and nothing at all across two,
        # and a redelivery starts with it empty.
        recorded: dict[str, trigger_service.SubscriptionOutcome] = {}
        deferred: list[str] = []
        # Once, before attempt 1, and shared by all of them: the budget bounds this delivery's
        # hold on the consumer, and giving each attempt its own would let three of them hold it
        # for three times as long.
        deadline = time.monotonic() + _FAN_OUT_BUDGET_SECONDS
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                with self._session_factory() as session:
                    fan_out = trigger_service.record_event_and_maybe_start_runs(
                        session=session,
                        event_name=intent.event_key,
                        emission_event_id=emission_event_id,
                        deadline=deadline,
                        **(
                            {"pipeline_service": self._pipeline_service}
                            if self._pipeline_service is not None
                            else {}
                        ),
                    )
            except trigger_service.RETRYABLE_WRITE_FAILURES:
                if attempt == _MAX_ATTEMPTS:
                    if recorded:
                        # An earlier attempt committed something, so this is a partial fan-out
                        # and not a delivery that did not happen. Raising would report it as
                        # the latter and throw away the record of what did land; `deferred`
                        # still holds what that attempt could not write.
                        break
                    raise
                logger.warning(
                    f"readiness recording contended: event_key={intent.event_key}"
                    f" emission={emission_event_id} attempt={attempt}/{_MAX_ATTEMPTS}"
                )
                time.sleep(_BACKOFF_SECONDS * attempt)
                continue
            for outcome in fan_out.outcomes:
                recorded.setdefault(outcome.subscription_id, outcome)
            # Filtered, not taken wholesale: a retry re-runs the whole fan-out, so a
            # subscription that committed earlier can be the contended one this time.
            #
            #   attempt 1: outcomes=[S1] deferred=[S2]  ->  recorded={S1}
            #   attempt 2: outcomes=[S2] deferred=[S1]  ->  recorded={S1, S2}
            #   unfiltered, deferred=[S1] settles a complete fan-out as a terminal FAIL
            #
            # On the assignment, because the raising break above keeps this value.
            deferred = [sid for sid in fan_out.deferred if sid not in recorded]
            if not deferred or attempt == _MAX_ATTEMPTS:
                break
            logger.warning(
                f"readiness recording contended: event_key={intent.event_key}"
                f" emission={emission_event_id} deferred={len(deferred)}"
                f" attempt={attempt}/{_MAX_ATTEMPTS}"
            )
            time.sleep(_BACKOFF_SECONDS * attempt)
        # Sorted because the fan-out promises subscription id order and this dict was filled
        # across attempts that may each have covered a different part of it.
        merged = sorted(recorded.values(), key=lambda o: o.subscription_id)
        return trigger_service.FanOut(
            outcomes=merged,
            deferred=deferred,
            # Recomputed rather than merged: `failed` is a view of `outcomes`, and the attempt
            # that produced a given outcome is not necessarily the last one to run.
            failed=[
                outcome.subscription_id
                for outcome in merged
                if outcome.result.reason in trigger_service.RUN_NOT_STARTED_REASONS
            ],
        )

    def _nothing_recorded(
        self,
        *,
        intent: readiness_annotations.ReadinessIntent,
        reason: trigger_service.TriggerReason,
    ) -> handler_base.Outcome:
        """The IGNORE verdict: the sink ran and found no event state to write to.

        Takes a `TriggerReason` rather than a `_DeliveryReason` for the one fact that is true of
        the delivery and of no subscription: nobody was listening.
        """
        logger.info(
            f"readiness not recorded: event_key={intent.event_key} reason={reason}"
        )
        return handler_base.Outcome(
            status=handler_base.OutcomeStatus.IGNORE,
            detail={
                "sink": _SINK_KEY,
                "event_key": intent.event_key,
                "reason": reason,
            },
        )
