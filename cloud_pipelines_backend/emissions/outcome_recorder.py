"""The delivery ledger's write side: one emission_event_outcome row per delivery."""

import logging
import typing

import sqlalchemy as sql
from sqlalchemy import exc as sql_exc
from sqlalchemy import orm

from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions.observability import consumer_observer
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

# What a row records when its sink key reached no implementation. The delivery was never
# attempted, so the row exists to say the pair was accounted for, not that anything was sent.
_UNRESOLVED_SINK_REASON: typing.Final[str] = "sink_unresolved"

# The failures of the outcome INSERT that say nothing about the row being written: the
# connection, the lock, the pool. A later attempt may well succeed, so the delivery is left
# unrecorded and the event is left claimable rather than being closed without its ledger row.
# Everything else — a value the column rejects, a statement the database will not accept —
# would fail the same way on every attempt, and retrying it would repeat the side effect for
# as long as the failure lasts, so it stays the router's `failed`.
_RETRYABLE_WRITE_FAILURES: typing.Final[tuple[type[Exception], ...]] = (
    sql_exc.OperationalError,
    sql_exc.InterfaceError,
    sql_exc.InternalError,
    sql_exc.TimeoutError,
)


class EmissionOutcomeRecorder(handler_base.OutcomeRecorder):
    """Records the deliveries of one emission_event, each in its own committed transaction.

    Built per claimed event and handed to the dispatcher, which passes it to the handler. One
    short session per write, so a delivery is durable the moment its sink returns rather than
    at the end of the fan-out: a consumer that dies part-way leaves behind exactly the rows for
    the deliveries it made, and the consumer that reclaims the event skips those.

    The done set is read once, at construction. Only the consumer holding the claim writes this
    event's outcome rows, so nothing can add to them behind this instance's back.
    """

    def __init__(
        self,
        *,
        session_factory: orm.sessionmaker,
        emission_event_id: str,
        emission_type: str,
        claimed_by: str,
    ) -> None:
        """Snapshot which of this event's deliveries are already recorded.

        Args:
            session_factory: Factory for the short session each write opens.
            emission_event_id: The event whose deliveries this records.
            emission_type: The kind of emission being delivered, which labels the collision
                counter this reports on.
            claimed_by: The id of the consumer holding the event's claim, noted on a collision
                so a double delivery names the process that made the second one.
        """
        self._session_factory = session_factory
        self._emission_event_id = emission_event_id
        self._emission_type = emission_type
        self._claimed_by = claimed_by
        self._done: set[str] = self._load_done()

    def _load_done(
        self,
    ) -> set[str]:
        """Read the sink keys this event already has outcome rows for.

        One seek on the leading half of the composite primary key, so no secondary index is
        needed for it.

        Returns:
            The sink keys already recorded for this event.
        """
        with self._session_factory() as session:
            keys = session.scalars(
                sql.select(db_models.EmissionEventOutcome.annotation_key).where(
                    db_models.EmissionEventOutcome.emission_event_id
                    == self._emission_event_id
                )
            ).all()
        return set(keys)

    def is_done(
        self,
        *,
        sink_key: str,
    ) -> bool:
        """Whether this delivery is already recorded.

        Args:
            sink_key: The key of the sink about to be called.

        Returns:
            True when a row for the pair already exists, so the delivery must not be repeated.
        """
        return sink_key in self._done

    def record(
        self,
        *,
        sink_key: str,
        outcome: handler_base.Outcome,
    ) -> None:
        """Write one delivery's outcome row and commit it on its own.

        The composite primary key is the dedupe: a second write of the same pair raises
        IntegrityError instead of duplicating. The first writer wins — the row already there
        describes a delivery that happened — so this one rolls back, logs, and notes the
        collision on the winner. A single INSERT is the whole transaction, so the rollback is
        the entire recovery and it is also what reopens the session for the note.

        The row also carries how long its own delivery took, which is why the timing is taken
        before the insert: it belongs to the sink that just returned, and the next one in the
        fan-out is about to measure itself.

        Args:
            sink_key: The key of the sink that was called.
            outcome: What it reported.

        Raises:
            handler_base.RecorderUnavailable: If the INSERT failed for one of the reasons a
                later attempt may not hit. `_done` is left alone, so the pair stays unrecorded
                and the event is delivered again once its claim expires.
        """
        extra_data = consumer_observer.take_delivery_timings()
        with self._session_factory() as session:
            session.add(
                db_models.EmissionEventOutcome(
                    emission_event_id=self._emission_event_id,
                    annotation_key=sink_key,
                    status=outcome.status.value,
                    detail=outcome.detail,
                    extra_data=extra_data,
                )
            )
            try:
                session.commit()
            except sql_exc.IntegrityError:
                session.rollback()
                logger.error(
                    f"Emission outcome for {self._emission_event_id} sink={sink_key} "
                    f"already exists; keeping the recorded one and noting the collision"
                )
                consumer_observer.count_outcome_collision(
                    emission_type=self._emission_type, sink_key=sink_key
                )
                self._note_collision(
                    session=session, sink_key=sink_key, outcome=outcome
                )
            except _RETRYABLE_WRITE_FAILURES as exc:
                # The delivery happened and the ledger could not be told. The rollback is what
                # releases the failed transaction; the raise then leaves as the one exception
                # the router does not convert into the event's verdict, so the event keeps this
                # consumer's claim, is never settled, and is delivered again once the claim
                # expires. `_done` is deliberately not reached: the pair is not recorded.
                session.rollback()
                raise handler_base.RecorderUnavailable(
                    "could not record emission outcome for "
                    f"{self._emission_event_id} sink={sink_key}"
                ) from exc
        # Recorded either way: after a collision the pair is on the ledger, whoever put it
        # there, so nothing in this fan-out should deliver it again.
        self._done.add(sink_key)

    def record_unresolved(
        self,
        *,
        sink_key: str,
    ) -> None:
        """Record that a declared sink key reached no implementation.

        Goes down the same path as a real delivery so the pair is accounted for exactly once,
        and carries the reason code every handler would otherwise have to spell out itself.

        Args:
            sink_key: The declared key nothing could deliver.
        """
        self.record(
            sink_key=sink_key,
            outcome=handler_base.Outcome(
                status=handler_base.OutcomeStatus.IGNORE,
                detail={"reason": _UNRESOLVED_SINK_REASON},
            ),
        )

    def _note_collision(
        self,
        *,
        session: orm.Session,
        sink_key: str,
        outcome: handler_base.Outcome,
    ) -> None:
        """Append this losing write to the winning row's `extra_data["collisions"]`.

        The winner's `status` and `detail` are never touched: they describe the delivery that
        was recorded first. The note is what makes the second delivery visible at all, since
        nothing else in the row shows it happened.

        Deliberately unfenced. The interesting collision is written by a consumer that has
        already overrun its lease, so a predicate on the claim would reject exactly the note
        worth having. Its own failure is swallowed rather than raised, so it cannot mask the
        IntegrityError it describes.

        `extra_data` is a MutableDict, which tracks assignment at the top level only, so this
        assigns a new dict rather than mutating the one already there.

        Args:
            session: The rolled-back session from the failed INSERT, reused for the note.
            sink_key: The key of the sink whose delivery collided.
            outcome: What this consumer's own delivery reported.
        """
        try:
            winner = session.get(
                db_models.EmissionEventOutcome,
                (self._emission_event_id, sink_key),
            )
            if winner is None:
                # The row that caused the IntegrityError is gone, so there is nothing to note.
                return
            extra_data = dict(winner.extra_data or {})
            collisions = list(extra_data.get("collisions") or [])
            collisions.append(
                {
                    "claimed_by": self._claimed_by,
                    "status": outcome.status.value,
                    "at": db_utils.utc_now().isoformat(),
                }
            )
            extra_data["collisions"] = collisions
            winner.extra_data = extra_data
            session.commit()
        except Exception:
            session.rollback()
            logger.exception(
                "Could not note the outcome collision for "
                f"{self._emission_event_id} sink={sink_key}"
            )
