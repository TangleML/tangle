import datetime
import enum
import logging
from typing import Final

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

# The ceiling on one emission_event_annotation.value: sql.Text's own capacity on MySQL. The
# producer checks each value against it and writes no row for an emission holding one that is
# over. In UTF-8 bytes, the unit sql.Text bounds — one character encodes to 1-4 bytes under
# utf8mb4, so a character count would not bound the column at all.
MAX_ANNOTATION_VALUE_BYTES: Final[int] = db_utils.MAX_TEXT_BYTES


class EmissionType(str, enum.Enum):
    """The set of values for the emission_event.emission_type column. The dispatcher uses it
    to route each row to the correct handler, so every member maps to one handler type; adding
    a new kind of emission means adding a member here and a handler for it."""

    READINESS = "readiness"
    METADATA = "metadata"
    QUOTA = "quota"
    NOTIFICATION = "notification"


class ClaimStatus(str, enum.Enum):
    """The set of values for the emission_event.claimed_status column. It tracks the queue's
    own view of a row — whether a consumer holds it right now — separately from what the
    handler decided about it, so a consumer can take a row atomically before dispatching.
    """

    # Queued and unclaimed, where the producer's INSERT lands: a poll can claim it into
    # IN_PROGRESS.
    PENDING = "pending"
    # Claimed by the consumer named in claimed_by, which either settles it or loses it back to
    # another consumer's poll once claimed_at falls outside the lease.
    IN_PROGRESS = "in_progress"
    # Terminal, written in its own commit once every delivery has been recorded: no poll
    # considers the row again.
    SETTLED = "settled"


class EmissionEvent(bts._TableBase):
    """The queue of emissions — one row per emission type detected in a node's annotations.
    The producer writes the rows; the consumer polls the table, feeds each to the dispatcher,
    and routes it to the matching handler."""

    __tablename__ = "emission_event"
    IX_NODE_TYPE_STATUS_NEW: Final[str] = "ix_emission_event_node_id_type_status"
    # The 2-column key this replaced, kept only so the migration can drop it by name.
    IX_NODE_TYPE_STATUS_OLD: Final[str] = (
        "ix_emission_event_execution_node_id_emission_type"
    )

    id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        primary_key=True,
        init=False,
        insert_default=bts.generate_unique_id,
    )

    # --- source identity ---
    # The node whose status change produced this emission (ExecutionNode.id). Part of the
    # dedupe index below.
    execution_node_id: orm.Mapped[str] = orm.mapped_column()
    # The node's container execution, or NULL when the status change happened without one
    # (SKIPPED, QUEUED, WAITING_FOR_UPSTREAM, or a CANCELLED that never launched). Carried
    # for correlation.
    container_execution_id: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # The node's terminal container status (e.g. SUCCEEDED).
    container_execution_status: orm.Mapped[str] = orm.mapped_column()
    # The run that owns the node (PipelineRun.id), or NULL when no run owns it. The correlation
    # key a reader resolves the run's own state from.
    pipeline_run_id: orm.Mapped[str | None] = orm.mapped_column(default=None)

    # --- routing ---
    # The emission type (EmissionType `.value`, e.g. readiness | metadata) the dispatcher
    # routes off. The consumer joins emission_event_annotation for the payload — none here.
    # Part of the dedupe index below.
    emission_type: orm.Mapped[str] = orm.mapped_column()

    # --- claim state ---
    # Where the row sits in the queue (ClaimStatus `.value`) — the poll AND crash-recovery
    # predicate:
    #
    #     producer INSERT     consumer claims        terminal write
    #     'pending'       ->  'in_progress'      ->  'settled'
    #                         (+ claimed_at, claimed_by)      (never claimed again)
    #
    # The consumer polls for a row that is 'pending', or 'in_progress' with a claimed_at
    # older than its lease, and takes it with a conditional UPDATE re-asserting that same
    # predicate. Two consumers racing for one row serialize on the row's write lock, so
    # exactly one UPDATE matches and only that one dispatches the row.
    claimed_status: orm.Mapped[str] = orm.mapped_column(
        init=False,
        insert_default=ClaimStatus.PENDING.value,
    )
    # When the current claim was taken — the lease clock. An 'in_progress' row whose
    # claimed_at is older than the consumer's lease duration is claimable again, which is how
    # a row outlives the consumer that was holding it. NULL while unclaimed.
    claimed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    # The id of the consumer process holding the claim: which replica to look at when a row
    # sits 'in_progress'. NULL while unclaimed.
    claimed_by: orm.Mapped[str | None] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH), default=None
    )

    # --- handling state ---
    # How the fan-out ended (HandleStatus `.value`), written when the row settles. It reports
    # whether every declared sink was reached, not whether the deliveries succeeded — a
    # delivery's own verdict lives on its emission_event_outcome row:
    #
    #     producer INSERT           consumer handles             terminal write
    #     handle_status = NULL  ->  (row claimed, dispatched)  ->  complete | nothing_to_do
    #                                                              | incomplete | failed
    #
    #   - 'complete'      — every declared sink ran. A sink that reported failure still lands
    #                       here; its 'fail' is on its own outcome row.
    #   - 'nothing_to_do' — the annotations did not ask for this emission after all.
    #   - 'incomplete'    — a declared sink never reached an implementation, so one delivery
    #                       was never attempted.
    #   - 'failed'        — nobody reported: no handler for the type, a row whose stored status
    #                       cannot be mapped to its enum, or a raise out of parse or handle.
    #
    # Data only: no predicate reads it, which is also why it carries no index. Where the row
    # sits in the queue is claimed_status, and that is what the poll reads.
    handle_status: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # Why the fan-out ended as it did (e.g. {"reason": "no_handler"}), or NULL when it ended
    # cleanly. A sink's own response and errors are not here; they are on its outcome row.
    handle_detail: orm.Mapped[dict | None] = orm.mapped_column(default=None)
    # Free-form future-proofing. Reserved for needs that would otherwise require a migration.
    extra_data: orm.Mapped[dict | None] = orm.mapped_column(default=None)

    # Row creation time (server-populated). Also the poll ORDER BY (oldest claimable first).
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    # Last-modified time (server-populated; bumped via onupdate on the terminal writeback).
    # The settle is the last write a row takes, so this column's final value is when it
    # settled — which is why there is no separate timestamp for that.
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
        onupdate=db_utils.utc_now,
    )

    __table_args__ = (
        # Dedupe only, not a search path: one emission per node, kind and status, rejected at
        # flush with an IntegrityError. All three are NOT NULL, so the key covers every row —
        # a nullable one would leave its NULL rows unconstrained. Old columns first, so the
        # superseded 2-column key is still this index's left prefix.
        sql.Index(
            IX_NODE_TYPE_STATUS_NEW,
            "execution_node_id",
            "emission_type",
            "container_execution_status",
            unique=True,
        ),
        # Serves the consumer's claim poll (checking the DB for claimable events): WHERE
        # claimed_status = 'pending', or 'in_progress' with an expired claimed_at, ORDER BY
        # created_at. The column order matters — claimed_status first to seek the claimable
        # rows, created_at second so the ORDER BY is served straight from the index (no
        # separate sort).
        sql.Index(
            "ix_emission_event_claimed_status_created_at",
            "claimed_status",
            "created_at",
        ),
    )


class EmissionEventAnnotation(bts._TableBase):
    """The annotations for an EmissionEvent, one row per key/value.

    Composite PK (emission_event_id, key) enforces one value per key per event."""

    __tablename__ = "emission_event_annotation"

    # FK to emission_event.id (plain string, matching repo style — no ForeignKey object).
    # First half of the composite PK.
    emission_event_id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    # The annotation key, verbatim as the node declared it — this table stores whatever string
    # arrived, not a known name. Second half of the composite PK.
    key: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    # The annotation value (enums stored as their `.value`), rehydrated into the handler's
    # typed intent on the read side. sql.Text: 64KB on MySQL, unlimited on PostgreSQL and
    # SQLite — wide enough for a JSON object string, which is what a payload value holds.
    # MAX_ANNOTATION_VALUE_BYTES is that 64KB, and the producer holds writes to it.
    value: orm.Mapped[str] = orm.mapped_column(sql.Text)
    # Free-form future-proofing. Reserved for needs that would otherwise require a migration.
    extra_data: orm.Mapped[dict | None] = orm.mapped_column(default=None)

    __table_args__ = (
        # Speeds up annotation search by (key, value) (e.g. metrics, "is X ready" lookups).
        #
        # MySQL rejects an index on a TEXT column with no prefix length (error 1170), and 512
        # is the widest prefix that fits InnoDB's 3072-byte key budget alongside key:
        # 255 * 4 + 512 * 4 = 3068. Other dialects ignore mysql_length and index value in full.
        sql.Index(
            "ix_emission_event_annotation_key_value",
            "key",
            "value",
            mysql_length={"value": 512},
        ),
    )


class EmissionEventOutcome(bts._TableBase):
    """One delivery of an EmissionEvent to one sink, written when that sink returns.

    An emission can name several sinks, so the verdict is per delivery rather than per event:
    one row here per (event, sink) pair. Its presence is the record that the pair is done —
    each row is committed on its own as its sink returns, which is what lets a consumer that
    reclaims a half-delivered event skip the deliveries already made and run only the rest.

    Composite PK (emission_event_id, annotation_key) enforces one outcome per pair."""

    __tablename__ = "emission_event_outcome"

    # FK to emission_event.id (plain string, matching repo style — no ForeignKey object).
    # First half of the composite PK.
    emission_event_id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    # The emission_event_annotation.key that declared this sink (e.g.
    # "tangleml.com/emission/readiness/sink/start-pipeline-run"). It names the sink and points at the
    # row that asked for the delivery, so the pair needs no column of its own. Second half of the
    # composite PK, which is therefore also the dedupe: a second write of the same delivery
    # collides instead of duplicating, and no separate unique constraint is needed.
    annotation_key: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    # How the delivery went (OutcomeStatus `.value`: success | fail | ignore). Never NULL —
    # the row is written only once its sink has returned, so a row that exists is a delivery
    # that finished.
    status: orm.Mapped[str] = orm.mapped_column()
    # What the sink did and what came back: its reason code, the target's status and response,
    # and — for a sink that sends one — the whole request it sent. The request is kept because
    # it is built fresh per run and survives nowhere else, so without it a row saying a report
    # was rejected cannot say what was rejected. NULL when the sink reported nothing.
    detail: orm.Mapped[dict | None] = orm.mapped_column(default=None)
    # This delivery's timings, and free-form future-proofing. Also where a losing writer notes
    # a collision, since the winning row's status and detail are never overwritten.
    extra_data: orm.Mapped[dict | None] = orm.mapped_column(default=None)

    # The row is written when the sink returns, so this is when the delivery finished.
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
        onupdate=db_utils.utc_now,
    )


def register_db_tables() -> None:
    """Force-import so the models register with _TableBase.metadata before
    create_all() runs."""
    logger.info(
        f"Emission tables registered: {EmissionEvent.__tablename__}, "
        f"{EmissionEventAnnotation.__tablename__}, "
        f"{EmissionEventOutcome.__tablename__}"
    )
