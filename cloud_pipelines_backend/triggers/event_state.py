"""Reading and syncing the event states a subscription waits on."""

import dataclasses
import datetime
import logging
from collections.abc import Mapping

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend.triggers import db_models, evaluation
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class FilledEvents:
    """A subscription's arrived events, split on whether they are still fresh.

    Attributes:
        emitted: event name -> the last emission seen for it, for arrivals that have not
            expired. Membership of the keys is what a condition is evaluated against.
        lapsed: the event names that arrived and have since expired, sorted.
    """

    emitted: dict[str, str | None]
    lapsed: list[str]


def filled_events(
    *, session: orm.Session, subscription_id: str, now: datetime.datetime
) -> FilledEvents:
    """The subscription's arrived events, partitioned into the fresh and the expired.

    One table, no join, no aggregate: a prefix seek on the (subscription_id, event_name)
    primary key. Both halves are the same slice of that seek — every row with a `filled_at` —
    so they are read once and split on `now` in Python rather than asked for separately.

    That is not only a saved round trip. The two halves are complements by construction here,
    where two SQL predicates would have to stay each other's exact negation by hand; a row can
    no longer fall into both or neither because the clock moved between two queries.

    Example:
        trigger_event_state, for one subscription, with now = 12:00

            event_name      filled_at   expires_at   last_emission_event_id
            orders_ready    11:00       NULL         em-1     never expires
            fx_ready        11:30       12:30        em-2     still fresh
            refunds_ready   09:00       11:00        em-3     expired an hour ago
            payouts_ready   NULL        NULL         NULL     never arrived

            -> emitted {"orders_ready": "em-1", "fx_ready": "em-2"}, lapsed ["refunds_ready"]

        An arrival whose `expires_at` is exactly `now` is lapsed: freshness is strictly `>`.
        A row that never arrived is in neither half.

    Args:
        session: read through this session, so a caller inside a write transaction sees its
            own uncommitted sync.
        subscription_id: the subscription whose events to read.
        now: the instant freshness is judged against.

    Returns:
        Both halves of the filled rows. `lapsed` is the diagnostic behind
        `trigger.event_expired`: an arrival that lapsed is indistinguishable from one that
        never came as far as the condition is concerned, and nothing else records it.
    """
    rows = session.execute(
        sql.select(
            db_models.TriggerEventState.event_name,
            db_models.TriggerEventState.last_emission_event_id,
            db_models.TriggerEventState.expires_at,
        ).where(
            db_models.TriggerEventState.subscription_id == subscription_id,
            db_models.TriggerEventState.filled_at.is_not(None),
        )
    ).all()
    return FilledEvents(
        emitted={
            event_name: emission_id
            for event_name, emission_id, expires_at in rows
            if expires_at is None or expires_at > now
        },
        lapsed=sorted(
            event_name
            for event_name, _emission_id, expires_at in rows
            if expires_at is not None and expires_at <= now
        ),
    )


def events_emitted(
    *, session: orm.Session, subscription_id: str, now: datetime.datetime
) -> dict[str, str | None]:
    """The subscription's events whose emission has arrived and not expired, each with its id.

    The fresh half of `filled_events`, for the callers that have no use for the expired one.
    The emission ids ride along because a trigger records which arrivals completed the
    condition, and asking for them afterwards would be a second query against state the
    trigger has already cleared.

    Example:
        trigger_event_state, for one subscription, with now = 12:00

            event_name      filled_at   expires_at   last_emission_event_id
            orders-ready    11:00       NULL         em-1     never expires
            fx-ready        11:30       12:30        em-2     still fresh
            refunds-ready   09:00       11:00        em-3     expired an hour ago
            payouts-ready   NULL        NULL         NULL     never arrived

            -> {"orders-ready": "em-1", "fx-ready": "em-2"}

        An arrival whose `expires_at` is exactly `now` is out: freshness is strictly `>`.

    Args:
        session: read through this session, so a caller inside a write transaction sees its
            own uncommitted sync.
        subscription_id: the subscription whose events to read.
        now: the instant freshness is judged against.

    Returns:
        Event name -> the last emission seen for it, which is NULL for a row filled before
        that column was recorded. Membership of the keys is what a condition is evaluated
        against.
    """
    return filled_events(
        session=session, subscription_id=subscription_id, now=now
    ).emitted


def sync(*, session: orm.Session, subscription_id: str, condition: object) -> None:
    """Make the stored event states match the events `condition` names.

    Four cases, and only the last two touch a row that already exists:

        in old and new   left alone — filled_at, last_emission_event_id and the arrival's
                         history all survive an unrelated edit to the condition
        in old only      deleted, its state going with it
        in new only      inserted empty, never seen
        expiry changed   expire_seconds rewritten, and expires_at recomputed from the
                         surviving filled_at so an expiry edit is not silently ignored

    Example:
        old condition   {"op": "any", "children": [{"event": "a"},
                                                   {"event": "b", "expire_seconds": 60}]}
        new condition   {"op": "any", "children": [{"event": "b", "expire_seconds": 600},
                                                   {"event": "c"}]}

            a   DELETE                       gone from the condition
            b   UPDATE expire_seconds 60 -> 600, expires_at = filled_at + 600s
                                             the arrival itself is untouched, and a window
                                             that had lapsed can come back
            c   INSERT filled_at NULL        waiting on its first emission

    An identical edit is a no-op by construction: both sides of the diff are empty and every
    expiry already matches, so no statement is emitted at all.

    Read-then-write rather than a dialect-specific upsert: the row count is one per event the
    condition names, and `ON CONFLICT` / `ON DUPLICATE KEY` would have to be written twice,
    once per dialect. Two updates racing on the same subscription therefore collide on the
    primary key and the loser's whole transaction rolls back — no half-synced event set.

    Args:
        session: the caller's transaction; nothing is committed here.
        subscription_id: the subscription being synced.
        condition: the new condition, as authored.

    Returns:
        None. Side effect only: rows are deleted from, inserted into and updated in
        trigger_event_state.
    """
    wanted = evaluation.event_expiries(condition=condition)
    existing = {
        state.event_name: state
        for state in session.scalars(
            sql.select(db_models.TriggerEventState).where(
                db_models.TriggerEventState.subscription_id == subscription_id
            )
        ).all()
    }

    # Two diffs, three regions. Renaming a -> z, where b already has its arrival:
    #
    #   existing  |-----------------|                 => {a, b}
    #   wanted              |---------------------|   => {b, z}
    #                  a        b          z
    #               DELETE    KEEP      INSERT
    removed = sorted(set(existing) - set(wanted))
    for event_name in removed:
        session.delete(existing[event_name])
    for event_name in sorted(set(wanted) - set(existing)):
        session.add(
            db_models.TriggerEventState(
                subscription_id=subscription_id,
                event_name=event_name,
                expire_seconds=wanted[event_name],
            )
        )
    for event_name, expire_seconds in sorted(wanted.items()):
        state = existing.get(event_name)
        if state is None or state.expire_seconds == expire_seconds:
            continue
        state.expire_seconds = expire_seconds
        state.expires_at = _expires_at(
            filled_at=state.filled_at, expire_seconds=expire_seconds
        )
    logger.info(
        f"Synced trigger event states subscription={subscription_id} wanted={len(wanted)} removed={len(removed)}"
    )


def subscription_ids_waiting_on(*, session: orm.Session, event_name: str) -> list[str]:
    """Which subscriptions wait on this event name — the sink's first move.

    One indexed seek on ix_event_state_event_name. The composite primary key cannot serve it:
    event_name is the key's second column and a seek needs the leading one.

    Ids rather than rows, deliberately. The caller locks each subscription before writing, and a
    row read before that lock is a row a concurrent edit may already have changed or deleted —
    so the state row is fetched by primary key inside the lock instead. Returning ids also keeps
    the sort out of SQL: `WHERE event_name = ? ORDER BY subscription_id` is not covered by a
    single-column index and can fall back to a filesort in production, while sorting a handful
    of ids in Python costs nothing and gives the same stable lock order.

    Args:
        session: the caller's session.
        event_name: the readiness event name the emission carried. Lowercase alphanumeric
            kebab, per `api_routes._EVENT_NAME_PATTERN`.

    Returns:
        Subscription ids, sorted, so a batch of arrivals takes subscription locks in a stable
        order. Empty when nothing subscribes to the name — the sink's `no_subscription` case.
    """
    ids = session.scalars(
        sql.select(db_models.TriggerEventState.subscription_id).where(
            db_models.TriggerEventState.event_name == event_name
        )
    ).all()
    return sorted(ids)


def fill(
    *,
    state: db_models.TriggerEventState,
    emission_event_id: str | None,
    now: datetime.datetime,
) -> bool:
    """Record that the event arrived: which emission, when, and when that goes stale.

    Latest-wins for a *newer* emission, and a no-op for one this row has already moved past.
    The second half is what makes a redelivery harmless. Emissions are delivered at least once
    — a consumer that dies between a sink's side effect and its ledger write leaves the row
    claimable — so without this check a redelivered emission would refill an event state the
    trigger had just cleared and start a second run for one readiness signal. The fence cannot
    catch that: it keys on `(subscription_id, cycle)`, and by then the cycle has moved on.

    Two different questions are asked, because one comparison cannot answer both. Equality alone
    is a single-slot memory that two interleaved emissions defeat: em-1 fills the row and
    triggers, `clear` empties it, em-2 arrives and overwrites `last_emission_event_id`, and a
    delayed redelivery of em-1 is then no longer recognised as one. It refills the row, starts
    a second cycle off a signal already acted on, and drags `filled_at` and `expires_at`
    *backwards* — so a live arrival can be aged out early by a redelivery of an older one. So the
    row also has to ask "is this older than what I have?", which is an ordering question.

    Ordering is asked of the *timestamp prefix only*, never the whole id.
    `backend_types_sql.generate_unique_id` is a fixed-width millisecond epoch followed by four
    random bytes (`utils/db.ID_MS_PREFIX_LENGTH`). Across milliseconds the prefix carries the
    order; within one, a lexicographic compare on the full id falls through to the random tail
    and decides by coin flip. Comparing whole ids therefore used to refuse a genuinely distinct
    emission roughly half the time it shared a millisecond with the stored one — and a refusal
    here is silent, because the caller reads it as a redelivery and skips `maybe_trigger`
    entirely, losing a run that should have started.

    What this does *not* fully close: the row remembers one id, so a distinct same-millisecond
    emission and a redelivery of a superseded same-millisecond one are indistinguishable, and
    both are now accepted. That trades a silent lost run for a duplicate one in a strictly
    narrower window (a redelivery *and* a same-millisecond sibling *and* that arrival order).
    Closing it needs a totally ordered id scheme, or a column remembering more than one consumed
    id; `filled_at` cannot serve as that high-water mark, because `clear` nulls it.

    `last_emission_event_id` therefore outlives the clear that empties the row (see `clear`):
    the row remembers which arrival it last saw even after that arrival has been consumed.

    `expires_at` is computed once, here, instead of at read time: freshness then costs one SQL
    parameter (`expires_at IS NULL OR expires_at > :now`) rather than per-row interval
    arithmetic, and no sweeper job has to exist to retire stale rows.

    Example:
        One "fx-ready" row with expire_seconds 600, three calls in a row.

        | call                 | returns | last_emission_event_id | filled_at | expires_at |
        | -------------------- | ------- | ---------------------- | --------- | ---------- |
        | (row as created)     |         | NULL                   | NULL      | NULL       |
        | fill(em-7, at 12:00) | True    | em-7                   | 12:00     | 12:10      |
        | fill(em-7, at 12:03) | False   | em-7                   | 12:00     | 12:10      |
        | fill(em-8, at 12:05) | True    | em-8                   | 12:05     | 12:15      |
        | fill(em-7, at 12:09) | False   | em-8                   | 12:05     | 12:15      |
        | fill(em-8b, at 12:06)| True    | em-8b                  | 12:06     | 12:16      |

        Row 3 wrote nothing — same emission, so filled_at kept 12:00.
        Row 5 is the delayed redelivery of an emission the row has moved past: an older
        millisecond than em-8, so it is refused rather than allowed to drag the window back.
        Row 6 is em-8b, a *different* emission minted in the same millisecond as em-8. It is
        recorded, whichever way the two ids happen to sort: they are two readiness signals and
        dropping one loses a run.
        With expire_seconds NULL, expires_at stays NULL throughout: the row never goes stale.

    Args:
        state: the row to write, already in the caller's session.
        emission_event_id: the arriving emission, kept for correlation and compared against
            the stored one to recognise a redelivery or a straggler. None leaves the column
            NULL rather than inventing an id, and cannot be deduplicated — there is nothing to
            compare it against.
        now: the arrival instant, and the base `expires_at` is measured from.

    Returns:
        Whether anything was written. False means one of exactly two things: the very same
        emission redelivered, or one from an older millisecond arriving late — so the caller has
        nothing new to evaluate. A distinct emission is always recorded, including one sharing a
        millisecond with the stored id.
    """
    if emission_event_id is not None and state.last_emission_event_id is not None:
        stored = state.last_emission_event_id
        # Identity is asked of the whole id; order is asked of the timestamp prefix alone, which
        # is the only part of the id that carries any. See the docstring above.
        prefix = db_utils.ID_MS_PREFIX_LENGTH
        if stored == emission_event_id or stored[:prefix] > emission_event_id[:prefix]:
            return False
    state.last_emission_event_id = emission_event_id
    state.filled_at = now
    state.expires_at = _expires_at(filled_at=now, expire_seconds=state.expire_seconds)
    return True


def clear(*, session: orm.Session, subscription_id: str) -> None:
    """Empty every one of the subscription's event states, contributing or not.

    The whole condition resets on a trigger, so an `any` branch's unused arrivals do not count
    toward the next cycle — otherwise a stale arrival from two cycles ago could half-satisfy
    a condition nobody has seen an event for.

    `last_emission_event_id` is deliberately left standing. Emptying it too would leave the row
    unable to tell a redelivery of the arrival it just consumed from a genuinely new one, and
    that redelivery would trigger the next cycle off a signal already acted on. A filled row is
    one with `filled_at` set, so keeping the id changes nothing about what the condition sees.
    """
    session.execute(
        sql.update(db_models.TriggerEventState)
        .where(db_models.TriggerEventState.subscription_id == subscription_id)
        .values(filled_at=None, expires_at=None)
    )


def delete_all(*, session: orm.Session, subscription_id: str) -> int:
    """Remove every event state this subscription owns, and say how many there were.

    Done in the application rather than left to the schema's `ON DELETE CASCADE`: SQLite
    enforces foreign keys only on a connection that has run `PRAGMA foreign_keys=ON`, and the
    engine factory does not set it, so on SQLite the cascade never fires and these rows would
    outlive the subscription with nothing left to read them. The cascade stays in the schema
    as the database-side backstop on engines that do enforce it; this makes the behaviour the
    same on all of them.

    Args:
        session: the caller's transaction. Nothing is committed here.
        subscription_id: whose states to remove.

    Returns:
        How many rows were deleted, for the caller's log line.
    """
    result = session.execute(
        sql.delete(db_models.TriggerEventState).where(
            db_models.TriggerEventState.subscription_id == subscription_id
        )
    )
    return result.rowcount


def missing(*, condition: object, emitted: Mapping[str, str | None]) -> list[str]:
    """The events the condition still needs — what it is actually waiting for.

    Not every event it names and lacks: `all(any(a, b), c)` with `a` live is waiting for `c`
    alone, and naming `b` would ask a caller to emit an event that changes nothing.
    """
    return sorted(evaluation.outstanding(condition=condition, emitted=set(emitted)))


def _expires_at(
    *, filled_at: datetime.datetime | None, expire_seconds: int | None
) -> datetime.datetime | None:
    """When an arrival goes stale, or None when it cannot: unfilled, or no expiry set."""
    if filled_at is None or expire_seconds is None:
        return None
    return filled_at + datetime.timedelta(seconds=expire_seconds)
