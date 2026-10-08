"""The concrete emission consumer: a single synchronous poll loop over emission_event."""

import datetime
import logging
import threading
import typing

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching import service as dispatching_service
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import db_models, outcome_recorder
from cloud_pipelines_backend.emissions import messages as emission_messages
from cloud_pipelines_backend.emissions.observability import consumer_observer
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

_IDLE_BACKOFF_SECONDS: typing.Final[float] = 0.5
# How long the loop waits after a cycle raised, indexed by how many have raised in a row. A
# blip clears on the first rung; a sustained outage settles at the last one rather than logging
# a stack trace every five seconds for as long as it lasts. The ceiling is minutes rather than
# hours because nothing wakes the loop early when the database comes back — stop.wait() returns
# only on shutdown — so the ceiling is also the worst-case lag before the queue drains again.
_ERROR_BACKOFF_LADDER: typing.Final[tuple[float, ...]] = (
    5.0,
    30.0,
    60.0,
    300.0,
)
# How long a claim stands before another consumer may take the row back — the lease a consumer
# holds on a row it is working. This is the delay before a row is retried after the consumer
# holding it dies, so it wants to be short; but a consumer that is merely slow must never have
# its row taken, so it has to stay comfortably above the longest a fan-out is expected to take.
# Nothing computes that bound: no stage of a cycle is capped, so 300s is a chosen number.
#
# Public, not private: `quota/reconciler.py` derives its stall threshold from this lease plus
# one of its own passes, so the lease is this module's interface rather than its business. A
# backstop that fired inside the lease would race a redelivery that was still coming.
CLAIM_EXPIRES_AFTER_SECONDS: typing.Final[float] = 300.0


def _error_backoff(
    *,
    consecutive_failures: int,
    has_completed_a_cycle: bool,
) -> float:
    """How long to wait after a cycle raised.

    Escalating is the right answer for a consumer that was working and started failing, and
    the wrong one for a consumer that has not started yet. The two are told apart by whether
    any cycle has ever come back clean: until one has, every retry stays on the first rung.
    That is the cold start — the API server owns `create_all`, so a consumer launched
    alongside it polls a table that does not exist yet, for as long as the API server takes.
    Escalating through that window would put the consumer to sleep for minutes just as the
    tables appear.

    Args:
        consecutive_failures: How many cycles have raised in a row, this one included.
        has_completed_a_cycle: Whether any cycle has ever returned without raising.

    Returns:
        The number of seconds to wait before polling again.
    """
    if not has_completed_a_cycle:
        return _ERROR_BACKOFF_LADDER[0]
    rung = min(consecutive_failures, len(_ERROR_BACKOFF_LADDER)) - 1
    return _ERROR_BACKOFF_LADDER[rung]


def _map_row_to_event(
    *,
    row: db_models.EmissionEvent,
    annotations: dict[str, str],
) -> emission_messages.EmissionEventMessage:
    """Build the message DTO from an emission_event row and its joined annotations.

    The row's primary key `row.id` becomes the message's `emission_event_id`, and the stored
    TEXT status column is rehydrated back to its canonical enum (mirroring the producer's
    `.value` write).

    Args:
        row: The emission_event row to snapshot.
        annotations: The row's annotations, keyed by annotation key.

    Returns:
        An immutable, database-detached EmissionEventMessage the dispatcher can route.
    """
    return emission_messages.EmissionEventMessage(
        emission_event_id=row.id,
        emission_type=row.emission_type,
        execution_node_id=row.execution_node_id,
        container_execution_id=row.container_execution_id,
        pipeline_run_id=row.pipeline_run_id,
        container_execution_status=bts.ContainerExecutionStatus(
            row.container_execution_status
        ),
        annotations=dict(annotations),
    )


class ConsumerService:
    """Claims emission_event rows one at a time and dispatches each to a handler.

    Owns a session factory and the injected dispatcher. Each cycle claims the oldest claimable
    row, builds its message, dispatches inline, and settles the row with what came back. The
    claim is what makes several consumers safe to run at once: each row is taken by exactly one
    of them, and it covers the whole fan-out rather than one delivery.

    Each delivery records itself, through the recorder built per claimed row; the consumer never
    reads the ledger. What it writes is the row's own verdict, in a statement of its own after
    every delivery has committed.
    """

    def __init__(
        self,
        *,
        session_factory: orm.sessionmaker,
        dispatcher: dispatching_service.DispatcherService,
        instance_id: str | None = None,
    ) -> None:
        """Store the session factory, the dispatcher to route rows through, and the claim id.

        Args:
            session_factory: Factory for the session the poll loop opens each cycle.
            dispatcher: The router built with the handlers this consumer feeds.
            instance_id: The id stamped on every row this consumer claims, so a row names its
                holder. Defaults to a fresh id per consumer.
        """
        self._session_factory = session_factory
        self._dispatcher = dispatcher
        self._instance_id = instance_id or bts.generate_unique_id()
        # Everything this consumer reports about itself; see
        # emissions/observability/consumer_observer.py.
        self._observer = consumer_observer.ConsumerObserver()

    @property
    def instance_id(self) -> str:
        """The id this consumer writes to claimed_by, for logging it at startup."""
        return self._instance_id

    def observe_backlog(
        self,
    ) -> None:
        """Start reporting how many rows the queue has not settled yet, as a gauge.

        Separate from construction because it registers a callback that the metrics SDK
        holds for the life of the process, which is a side effect a test building a consumer
        should not inherit. Until it is called, the loop skips the backlog query entirely.
        """
        self._observer.observe_backlog()

    def run(
        self,
        *,
        stop: threading.Event,
    ) -> None:
        """Run the poll loop until `stop` is set.

        A processed row loops again immediately to drain a backlog fast; an empty poll waits
        briefly, and that wait also returns as soon as `stop` is set. A transient DB/infra
        error is caught here so a blip never kills the process: the loop logs it, backs off,
        and retries, waiting longer the longer the failures go on.

        Args:
            stop: The event a signal handler sets to request shutdown.
        """
        # A clean cycle resets the count and latches the loop as having worked at least once;
        # the latch is what the backoff reads to tell a cold start from a real outage, and it
        # is never cleared afterwards.
        consecutive_failures = 0
        has_completed_a_cycle = False

        while not stop.is_set():
            # Step 1: Handle one row if any is claimable. A transient DB/infra failure (the
            # claim, the annotation load, or a delivery's record) is caught and backed off
            # rather than crashing the process. A row already claimed when the failure hit keeps
            # its claim, so the next poll moves on to another row and comes back to this one
            # once its lease runs out.
            # A terminal commit that cannot be written is settled in _mark_terminal instead, so
            # it never reaches here and never leaves a row for the next poll to pick up again.
            try:
                processed = self._process_one()
            except Exception:
                consecutive_failures += 1
                backoff_seconds = _error_backoff(
                    consecutive_failures=consecutive_failures,
                    has_completed_a_cycle=has_completed_a_cycle,
                )
                # The ordinal and the wait are both in the line, so how far an outage has
                # escalated is readable from the log without correlating timestamps.
                logger.exception(
                    f"Emission consumer: error processing a row "
                    f"(failure {consecutive_failures} in a row); "
                    f"retrying in {backoff_seconds}s"
                )
                stop.wait(backoff_seconds)
                continue

            # Step 2: The cycle returned, so the database answered. An empty queue proves that
            # just as well as a drained row does, and both count as clean.
            consecutive_failures = 0
            has_completed_a_cycle = True

            # Step 3: Back off briefly when the queue is empty; a handled row loops immediately.
            if not processed:
                stop.wait(_IDLE_BACKOFF_SECONDS)

    def _claimable(
        self,
    ) -> sql.ColumnElement[bool]:
        """Build the predicate matching the rows this consumer is allowed to take.

        A row is claimable when no consumer holds it, or when the consumer that held it has
        gone past its lease. The second case is the whole crash-recovery story: a consumer
        that dies mid-handle leaves its row claimed, and the lease is what hands that row to
        somebody else.

        Returns:
            The predicate, used twice per claim — once to find a candidate, and once inside
            the UPDATE that takes it.
        """
        expiry_cutoff = db_utils.utc_now() - datetime.timedelta(
            seconds=CLAIM_EXPIRES_AFTER_SECONDS
        )
        return sql.or_(
            db_models.EmissionEvent.claimed_status
            == db_models.ClaimStatus.PENDING.value,
            sql.and_(
                db_models.EmissionEvent.claimed_status
                == db_models.ClaimStatus.IN_PROGRESS.value,
                db_models.EmissionEvent.claimed_at < expiry_cutoff,
            ),
        )

    def _process_one(
        self,
    ) -> bool:
        """Claim at most one row and handle it.

        Returns:
            True if the poll should run again immediately: either a row was handled, or
            another consumer took the candidate and a different row may still be waiting.
            False if nothing was claimable.
        """
        with self._session_factory() as session:
            # The cycle is measured from here, before the poll that says whether there is a
            # row at all; an empty poll simply drops this observer unused.
            cycle = self._observer.start_cycle()

            # Step 1: Pick the oldest claimable row. A plain SELECT with no locking hint, so
            # the statement means the same thing on every database the app runs on. The claim
            # state and the emission type come back with the id purely to label the metrics
            # the claim reports; neither is trusted afterwards, since the CAS is what decides
            # whether this consumer gets the row.
            claimable = self._claimable()
            candidate = session.execute(
                sql.select(
                    db_models.EmissionEvent.id,
                    db_models.EmissionEvent.claimed_status,
                    db_models.EmissionEvent.emission_type,
                )
                .where(claimable)
                .order_by(db_models.EmissionEvent.created_at.asc())
                .limit(1)
            ).first()

            if candidate is None:
                # An empty poll is the one moment the backlog is known to be zero, and it is
                # also when the loop has time to spare.
                self._observer.refresh_backlog(session=session)
                return False

            candidate_id, candidate_claim, candidate_type = candidate

            # Step 2: Take the candidate with an UPDATE that re-asserts the same predicate.
            # This is what makes the claim atomic: the database serializes concurrent UPDATEs
            # of one row, so the loser re-reads the predicate against the winner's committed
            # claim, matches nothing, and updates no rows. Exactly one consumer proceeds.
            with cycle.claim():
                claimed = session.execute(
                    sql.update(db_models.EmissionEvent)
                    .where(db_models.EmissionEvent.id == candidate_id, claimable)
                    .values(
                        claimed_status=db_models.ClaimStatus.IN_PROGRESS.value,
                        claimed_at=db_utils.utc_now(),
                        claimed_by=self._instance_id,
                    )
                )
                session.commit()

            if claimed.rowcount != 1:
                # Another consumer got there first. Report a cycle worth repeating rather than
                # an empty queue, so the loop retries at once instead of idling on a backlog.
                self._observer.count_claim(
                    emission_type=candidate_type,
                    outcome=consumer_observer.ClaimOutcome.LOST,
                )
                logger.info(
                    f"Emission row {candidate_id} was claimed by another "
                    "consumer; polling again"
                )
                return True

            # A candidate that was already in progress had its lease run out, which says the
            # consumer holding it died or overran. Counted apart from a first claim, since a
            # rising reclaim rate is how that shows up.
            self._observer.count_claim(
                emission_type=candidate_type,
                outcome=(
                    consumer_observer.ClaimOutcome.RECLAIMED
                    if candidate_claim == db_models.ClaimStatus.IN_PROGRESS.value
                    else consumer_observer.ClaimOutcome.CLAIMED
                ),
            )

            row = session.get(db_models.EmissionEvent, candidate_id)
            if row is None:
                # The row was deleted between the claim and the read. Nothing to handle, but
                # the queue may hold more, so treat it like a lost claim.
                return True

            # Step 3: Read the row's own identifiers now, while it is loaded. The settle below
            # is a Core UPDATE followed by a commit, which expires every attached instance, so
            # nothing after it may reach through this row for a value.
            row_id = row.id

            # Step 4: Load the row's annotations. A failure here is transient (DB I/O) and
            # bubbles to run()'s backoff; the row keeps this consumer's claim until the lease
            # runs out.
            annotations = self._annotations_for(
                session=session, emission_event_id=row_id
            )

            # Step 5: Build the message. Mapping coerces stored TEXT status values back to their
            # enums, so an unknown value raises ValueError. That is deterministic — the row can
            # never map — so settle it failed rather than let it bubble and be reclaimed
            # forever, which would burn a lease on the same poison row over and over.
            try:
                event = _map_row_to_event(row=row, annotations=annotations)
            except ValueError as exc:
                logger.exception(f"Unmappable emission row {row_id}; marking failed")
                settled = self._mark_terminal(
                    session=session,
                    row_id=row_id,
                    result=handler_base.HandleResult(
                        status=handler_base.HandleStatus.FAILED,
                        detail={"error": repr(exc), "reason": "map_failed"},
                    ),
                )
                # Counted only when the settle landed, so the counter stays a count of the
                # emissions this consumer closed.
                if settled:
                    self._observer.count_handled(
                        emission_type=candidate_type,
                        status=handler_base.HandleStatus.FAILED.value,
                    )
                return True

            cycle.polled()

            # Sampled here too, not only on the empty poll: a consumer working through a
            # backlog never sees an empty poll, and a backlog is exactly what the gauge is
            # there to report. The row in hand is not settled yet, so it counts.
            self._observer.refresh_backlog(session=session)

            # Step 6: Build this row's recorder. It opens its own short sessions, so the
            # deliveries commit one at a time while this session still holds the claim.
            recorder = outcome_recorder.EmissionOutcomeRecorder(
                session_factory=self._session_factory,
                emission_event_id=row_id,
                emission_type=candidate_type,
                claimed_by=self._instance_id,
            )

            # The row's current extra_data is read here, before anything commits: the settle
            # expires every instance attached to this session, and the timings are merged onto
            # what the producer already wrote.
            produced_extra_data = row.extra_data

            # The rest of the cycle runs inside the emission's span, and every stage timed
            # within it also lands in the timings the settle persists on the row.
            with cycle.handling(
                emission_event_id=row_id,
                emission_type=candidate_type,
            ):
                # Step 7: Dispatch inline — the dispatcher parses, logs any issues, then hands
                # the intent and the recorder to the handler, which delivers to each declared
                # sink. The handler and its sinks time themselves into the same cycle. A
                # recorder that could not write comes back out of here as RecorderUnavailable
                # rather than as a verdict: the settle below is never reached, so the row keeps
                # this consumer's claim and is handled again once the lease runs out — with
                # every pair already on the ledger skipped. Nothing here catches it; run()'s
                # backoff does.
                with cycle.dispatch():
                    try:
                        result = self._dispatcher.dispatch(
                            event=event, recorder=recorder
                        )
                    except handler_base.RecorderUnavailable:
                        # Counted here because this is where the emission type is known, and
                        # nowhere else reports this failure: the row is left with no verdict, so
                        # count_handled never fires for it and the cycle looks unfinished rather
                        # than dropped. The raise continues to run()'s backoff, which is what
                        # leaves the claim standing.
                        self._observer.count_recorder_unavailable(
                            emission_type=candidate_type
                        )
                        raise
                    except handler_base.DeliveryIncomplete as incomplete:
                        # Returning True, not raising: this is not an error and must not reach
                        # run()'s backoff ladder. A sink stopped part-way on purpose and wants
                        # the row back, so the poll should carry straight on to the next one --
                        # slowing the loop down would punish the queue for a handler behaving
                        # correctly.
                        #
                        # The settle below is skipped, which is the whole point: the row keeps
                        # this consumer's claim, its lease runs out, and whoever reclaims it
                        # finds every pair already on the ledger skipped and finishes the rest.
                        #
                        # Counted for the same reason the raise above is, and one more: this
                        # one does not reach run()'s backoff either, so a fan-out that never
                        # finishes leaves no failed cycle, no verdict and no log of an error --
                        # only this counter and the row's own age. Distinguishing a fan-out
                        # taking a second pass from one that is not converging is the age
                        # gauge's job: `emission.oldest_in_progress_age` past a multiple of
                        # the lease is a lower bound on the passes so far, with no schema
                        # change and nothing counted per row.
                        self._observer.count_delivery_incomplete(
                            emission_type=candidate_type
                        )
                        logger.info(
                            f"Emission {row_id} left unsettled by an incomplete delivery"
                            f" ({incomplete}); awaiting reclaim"
                        )
                        return True

                # Step 8: Settle the row with the fan-out's own verdict, carrying the timings.
                with cycle.writeback() as timings:
                    settled = self._mark_terminal(
                        session=session,
                        row_id=row_id,
                        result=result,
                        extra_data=consumer_observer.row_timings(
                            extra_data=produced_extra_data, timings=timings
                        ),
                    )

                # Only a settle that landed reports the verdict: an emission this consumer no
                # longer holds is counted by the consumer that does.
                if settled:
                    cycle.finished(status=result.status.value)
                else:
                    cycle.abandoned()
            return True

    def _mark_terminal(
        self,
        *,
        session: orm.Session,
        row_id: str,
        result: handler_base.HandleResult,
        extra_data: dict | None = None,
    ) -> bool:
        """Close the row's claim and write the fan-out's verdict, fenced on that claim.

        The deliveries have already committed, one row each, so this is a statement of its own
        rather than the commit that carries them. A settle that never lands leaves the event
        claimed until its lease runs out, and the consumer that reclaims it then finds every
        pair already recorded, delivers nothing, and settles it.

        Two paths skip this call rather than fail it, and both leave the row exactly there on
        purpose: `RecorderUnavailable`, where a delivery was made and could not be written, and
        `DeliveryIncomplete`, where a sink stopped part-way with work it means to finish. In
        neither case is the row's state terminal, and the lease is what brings it back.

        Only reached once the fan-out has reported. A delivery whose record could not be written
        raises out of the dispatcher instead, so this is not called at all and the event stays
        claimed with no verdict — which is what lets a later attempt finish the ledger.

        Fenced on `claimed_by` and `in_progress`, because a consumer stalled past its own lease
        would otherwise settle an event that another consumer has since claimed and may still be
        delivering. `rowcount == 1` means the lease held; `rowcount == 0` means it did not, and
        there is nothing to write. Which of the two it was is returned, because the caller only
        reports the verdict for an event it actually settled.

        The timings ride this write rather than costing one of their own, which is also why they
        cover the cycle only up to this point.

        The row reaches a verdict even when its detail cannot be written. Leaving the claim open
        would be worse than losing the detail: the row would come back at the end of its lease
        and be handled all over again.

        Args:
            session: The open session holding the claim.
            row_id: The emission_event id to settle.
            result: What the fan-out reported.
            extra_data: The row's extra_data with this cycle's timings merged in, or None to
                leave what the row already holds alone.

        Returns:
            True when the fence held and the row was settled, False when another consumer holds
            the event and nothing was written.
        """
        try:
            settled = self._settle(
                session=session,
                row_id=row_id,
                status=result.status,
                detail=result.detail,
                extra_data=extra_data,
            )
        except Exception:
            # Roll back before anything else: it is what reopens the session, and it puts the
            # session back to what the database holds so the retry starts from a clean slate.
            # The claim itself survives — it was committed before the handler ran — so the
            # retry is still writing to a row only this consumer holds.
            session.rollback()
            logger.exception(
                f"Could not write handle_detail for emission row {row_id}; "
                "recording the verdict without it or this cycle's timings"
            )
            # The retry writes the same three columns, with a detail that needs no encoder.
            # Anything else the transaction had touched is gone: the rollback discarded it, so
            # the retry cannot resubmit whatever just failed. This cycle's timings are given up
            # with it rather than re-applied.
            settled = self._settle(
                session=session,
                row_id=row_id,
                status=result.status,
                detail={"reason": "detail_unstorable"},
            )

        if settled != 1:
            # The fence rejected the write, so another consumer owns this event now. Its own
            # settle is the one that counts; this one leaves the row exactly as it found it.
            logger.error(
                f"Emission row {row_id} was no longer claimed by {self._instance_id} at the "
                f"settle; leaving it to the consumer that holds it"
            )
            return False
        return True

    def _settle(
        self,
        *,
        session: orm.Session,
        row_id: str,
        status: handler_base.HandleStatus,
        detail: handler_base.JsonDetail | None,
        extra_data: dict | None = None,
    ) -> int:
        """Run the fenced settle statement and commit it.

        Args:
            session: The open session holding the claim.
            row_id: The emission_event id to settle.
            status: The fan-out's verdict.
            detail: The detail to record with it, or None.
            extra_data: The value to write over the row's extra_data, or None to leave the
                column out of the statement.

        Returns:
            How many rows the statement updated: 1 when this consumer still held the claim, 0
            when it did not.
        """
        values: dict[str, typing.Any] = {
            "claimed_status": db_models.ClaimStatus.SETTLED.value,
            "handle_status": status.value,
            "handle_detail": detail,
        }
        if extra_data is not None:
            values["extra_data"] = extra_data
        settled = session.execute(
            sql.update(db_models.EmissionEvent)
            .where(
                db_models.EmissionEvent.id == row_id,
                db_models.EmissionEvent.claimed_by == self._instance_id,
                db_models.EmissionEvent.claimed_status
                == db_models.ClaimStatus.IN_PROGRESS.value,
            )
            .values(**values)
        )
        session.commit()
        return settled.rowcount

    def _annotations_for(
        self,
        *,
        session: orm.Session,
        emission_event_id: str,
    ) -> dict[str, str]:
        """Return one emission_event's annotations as a key -> value dict.

        Scoping by `emission_event_id` alone already returns only this row's annotations, so no
        key filter is needed.

        Args:
            session: The open session to query within.
            emission_event_id: The emission_event id whose annotations to load.

        Returns:
            The row's annotations keyed by annotation key.
        """
        pairs = session.execute(
            sql.select(
                db_models.EmissionEventAnnotation.key,
                db_models.EmissionEventAnnotation.value,
            ).where(
                db_models.EmissionEventAnnotation.emission_event_id == emission_event_id
            )
        ).all()
        return {key: value for key, value in pairs}
