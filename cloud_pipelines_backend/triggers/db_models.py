import datetime
import enum
import logging
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)

# The *target* — what a satisfied condition starts — is a user pipeline, named by the same
# two columns scheduled_pipeline_run uses, so one grep finds every reference to a pipeline and
# both tables answer "what points at this pipeline?" the same way. Still to come: the key in
# the API's request model, and a sink that starts the thing.


# `(str, enum.Enum)` so members act as plain strings for dict lookups and JSON writes.
class DefinitionKey(str, enum.Enum):
    """The keys inside `TriggerSubscription.definition`.

    The subscription exactly as the API received it, stored verbatim and never rewritten, so
    an edit round-trips and the payload shape can grow without a migration. This is the only
    representation of the condition: evaluation reads CONDITION, an update diffs it, and a
    history row snapshots the whole blob. Nothing is compiled.

    A stored definition:

        {
          "name": "nightly-fx",
          "condition": {
            "op": "all",
            "children": [
              {"event": "orders-ready"},
              {"event": "fx-ready", "expire_seconds": 3600}
            ]
          }
        }

    A leaf may stand alone in CONDITION — `{"event": "orders-ready"}` is a whole condition.
    See `triggers/evaluation.py` for the node grammar.
    """

    # Duplicates the `name` column, which is the one the unique constraint and the lookup
    # route address. Kept here so a history row's snapshot says what it was called.
    NAME = "name"
    # The condition tree, as authored: `model_dump(mode="json", exclude_unset=True)`, so an
    # omitted `expire_seconds` stays omitted rather than being written back as a null.
    CONDITION = "condition"


class TriggerSubscription(bts._TableBase):
    """One subscription: what to start, whether it is live, and the fence counter.

    Column types inherited from _TableBase.type_annotation_map:
      str -> String(255), datetime -> UtcDateTime, dict -> MutableDict(JSON)
    Explicit types below only where overriding the default."""

    __tablename__ = "trigger_subscription"

    id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        primary_key=True,
        init=False,
        insert_default=bts.generate_unique_id,
    )
    # The caller's handle for this subscription. Mutable, and unique per creator — see
    # uq_trigger_subscription_created_by_name below.
    #
    # Deliberately given no index of its own, now that the query exists to size against: the
    # only predicate the listing route puts on `name` is `name_contains`, a substring
    # LIKE '%x%'. A leading wildcard cannot use a B-tree, so an index here would be written,
    # maintained on every write, and never read. The unique constraint's index does not serve
    # that search either — it is ordered on (created_by, name), and a substring match cannot
    # seek into it. The listing's real access path is the keyset in __table_args__.
    name: orm.Mapped[str] = orm.mapped_column()
    # The fence counter. One trigger per value: the winner bumps it, and the losing writer's
    # trigger_history insert collides on UNIQUE (subscription_id, cycle) instead of starting
    # a second run.
    cycle: orm.Mapped[int] = orm.mapped_column(default=0)
    # False withholds the run, not the recording: arrivals keep landing on the event states
    # while the subscription is off, so switching it back on resumes with a current event set
    # and may trigger immediately off what arrived meanwhile. Nothing is ever cleared here.
    enabled: orm.Mapped[bool] = orm.mapped_column(default=True)
    # Keys and shape: DefinitionKey, above.
    definition: orm.Mapped[dict[str, Any]] = orm.mapped_column()
    # The creator, immutable.
    created_by: orm.Mapped[str] = orm.mapped_column()
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
        onupdate=db_utils.utc_now,
    )
    # Free-form future-proofing. Reserved for needs that would otherwise require a migration.
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # The pipeline a satisfied condition starts. NOT NULL: a subscription with nothing to
    # start is not a state this table admits, so the target is required at insert and the
    # dataclass makes it a required keyword argument rather than something to remember.
    #
    # No length here on purpose: the column takes its VARCHAR from `pipeline.id` through the
    # ForeignKey, so widening the pipeline's key widens this one and cannot leave the two
    # disagreeing. Unlike scheduled_pipeline_run, this table is new, so a validated FK costs
    # nothing to install — there are no rows to copy and no writes to block.
    #
    # No ondelete: pipelines are soft-deleted (`deleted_at`), so a cascade would never fire.
    # The default RESTRICT is the accurate statement — a hard delete of a referenced pipeline
    # is a mistake, and the database should say so rather than quietly orphan a subscription.
    pipeline_task_spec_from_user_pipeline_id: orm.Mapped[str] = orm.mapped_column(
        sql.ForeignKey(
            user_pipeline_db_models.UserPipeline.id,
            name="fk_trigger_subscription_user_pipeline_id",
        ),
    )
    # Non-NULL only for a pinned reference; NULL means "track current". Deriving the
    # distinction from this column is what removes the need for a persisted mode that could
    # disagree with the data.
    #
    # The length comes from DIGEST_LENGTH rather than a ForeignKey, because the version this
    # points at is identified by the *pair* (pipeline, version_key) — see
    # fk_trigger_subscription_user_pipeline_version below. `pipeline_version.version_key` is
    # the second column of a composite primary key, so no single-column FK can target it.
    pipeline_task_spec_from_user_pipeline_version_key: orm.Mapped[str | None] = (
        orm.mapped_column(
            sql.String(user_pipeline_db_models.DIGEST_LENGTH),
            default=None,
        )
    )

    # The caller-owned natural key. `id` stays the PK and the FK target; this is what a CI job
    # addresses a subscription by, so nobody has to persist their own name -> id map. Scoped
    # per creator, mirroring uq_pipeline_user_id_file_path: two teams may both call something
    # "nightly", and neither can squat the other's handle.
    #
    # Rows are hard-deleted here, so a deleted name frees its slot immediately — unlike
    # user_pipelines, where a soft-deleted row keeps holding one.
    #
    # Equality is the database's, not ours, and the three dialects disagree:
    #   - MySQL (utf8mb4_0900_ai_ci) is case- and accent-insensitive
    #   - PostgreSQL (deterministic collation) is case-sensitive
    #   - SQLite (BINARY) is case-sensitive
    # So "Nightly" collides with "nightly" in prod and not in a local run: this constraint
    # admits both spellings locally and rejects the second one on MySQL. No single dialect-
    # agnostic WHERE clause fixes that; the closest thing, COLLATE, is dialect-aware too.
    #
    # Bound to a name rather than written inline below, so the service can recognize a
    # violation of *this* constraint without restating either the constraint name or its
    # columns — see TRIGGER_SUBSCRIPTION_USER_NAME_CONSTRAINT under the class. A plain class
    # attribute, not a mapped one: SQLAlchemy leaves it alone and it is not a dataclass field.
    _user_name_constraint = sql.UniqueConstraint(
        created_by,
        name,
        name="uq_trigger_subscription_created_by_name",
    )

    # Every constraint below names its columns by the objects declared above rather than by
    # string, so a column rename is a rename and not a runtime surprise. That makes the
    # position of this block load-bearing: it must stay below the columns it references.
    __table_args__ = (
        _user_name_constraint,
        # A pin names a version *of that pipeline*, which is the composite primary key of
        # pipeline_version — so the pin is checked as a pair, not per column.
        #
        # This constraint is MATCH SIMPLE, the SQL default: a NULL in either column skips the
        # whole check. That is what makes "track current" (version_key NULL) legal, and it is
        # also why fk_trigger_subscription_user_pipeline_id exists separately — without it a
        # row could name a pipeline that does not exist as long as it left the pin NULL.
        #
        # The one case MATCH SIMPLE leaves open — a pin with no pipeline to pin it to — is
        # closed by the NOT NULL on the pipeline id, so no CHECK constraint is needed. A
        # CHECK here would be a tautology, which reads as protection and is not.
        sql.ForeignKeyConstraint(
            [
                pipeline_task_spec_from_user_pipeline_id,
                pipeline_task_spec_from_user_pipeline_version_key,
            ],
            [
                user_pipeline_db_models.UserPipelineVersion.pipeline_id,
                user_pipeline_db_models.UserPipelineVersion.version_key,
            ],
            name="fk_trigger_subscription_user_pipeline_version",
        ),
        # The listing route's access path: ORDER BY updated_at DESC, id DESC with a keyset
        # `WHERE (updated_at, id) < (:cursor_updated_at, :cursor_id)`. Both columns, in that
        # order, so the seek and the sort are one index read instead of a filesort over the
        # table. `enabled` is left out on purpose — two values over the whole table is not
        # selective enough to be worth an index, and it composes with this one as a filter.
        sql.Index("ix_trigger_subscription_updated_at_id", updated_at, id),
    )


# Re-exported here because the columns it names are class-body locals: declared inside the
# class it can reference them as objects, and read back out here it stays importable by the
# service, which turns a violation into a 409 by matching on `.name` and `.columns`.
TRIGGER_SUBSCRIPTION_USER_NAME_CONSTRAINT: Final[sql.UniqueConstraint] = (
    TriggerSubscription._user_name_constraint
)


class TriggerEventState(bts._TableBase):
    """The state of one subscribed event: has it arrived, and is it still fresh?

    One row per distinct event a subscription waits on, so however many times the condition
    names an event, one arrival updates exactly one row and no two copies of that event's
    state can drift apart. The only mutable table in the design — written when an emission
    arrives, cleared when the subscription triggers."""

    __tablename__ = "trigger_event_state"

    # Halves of the composite PK, in this order: the live-event query is a prefix seek on
    # subscription_id, so the PK serves it with no index of its own.
    subscription_id: orm.Mapped[str] = orm.mapped_column(
        sql.ForeignKey(TriggerSubscription.id, ondelete="CASCADE"),
        primary_key=True,
    )
    # The readiness event's name. Not widenable to TEXT — it is half the PK and an indexed
    # column — so the API caps it to this same width.
    event_name: orm.Mapped[str] = orm.mapped_column(primary_key=True)

    # How long an arrival stays fresh. NULL means it never expires.
    expire_seconds: orm.Mapped[int | None] = orm.mapped_column(default=None)
    # The latest emission seen for this event, for correlation. NULL until one arrives.
    last_emission_event_id: orm.Mapped[str | None] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH), default=None
    )
    # When the event last arrived. NULL means it has not.
    filled_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    # filled_at + expire_seconds, computed once at fill time so freshness is decidable with ONE
    # SQL parameter (`expires_at IS NULL OR expires_at > :now`) rather than per-row interval
    # arithmetic — which is also why no sweeper job is needed. NULL = never expires.
    expires_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    # Free-form future-proofing. Reserved for needs that would otherwise require a migration.
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    __table_args__ = (
        # The sink's first move on an arriving emission: event name -> candidate
        # subscriptions, in one indexed seek. The composite PK cannot serve it — event_name
        # is its second column, and a seek needs the leading one.
        sql.Index("ix_event_state_event_name", event_name),
    )


class TriggerHistory(bts._TableBase):
    """One row per trigger, and the fence that makes triggering idempotent.

    Outlives its subscription: deleting a subscription cascades its event states away and
    keeps the record that runs were started."""

    __tablename__ = "trigger_history"

    id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        primary_key=True,
        init=False,
        insert_default=bts.generate_unique_id,
    )
    # A plain string and deliberately NOT a ForeignKey: the history has to survive its
    # subscription's deletion, and an enforced reference would either block the delete or
    # NULL this column out, losing which subscription triggered.
    subscription_id: orm.Mapped[str] = orm.mapped_column(sql.String(db_utils.ID_LENGTH))
    # The subscription's cycle at trigger time. UNIQUE with subscription_id — the fence.
    cycle: orm.Mapped[int] = orm.mapped_column()
    # The run this trigger started, and the only join from a run back to the subscription and
    # cycle that started it. Set inside the same SAVEPOINT as the fence insert, so a row that
    # commits always has one: NULL is reachable only mid-transaction, before the flush that
    # populates the run's id.
    pipeline_run_id: orm.Mapped[str | None] = orm.mapped_column(
        sql.ForeignKey(bts.PipelineRun.id), default=None
    )
    # WHY it triggered, in a form that stays true after the condition is edited. Three keys:
    #
    #   branch         the path through the authored condition JSON, e.g. "all[0].any[1]";
    #                  "" when the condition is a bare leaf, which has no operator above it
    #                  and so no branch to name
    #   branch_events  the event names that actually satisfied it — the durable part. One
    #                  entry per matched leaf, in tree order, so it lines up with `branch`
    #                  and can name one event twice: all(a, any(a, b)) satisfied by "a"
    #                  records ["a", "a"]. `triggered_by` below is keyed by event name and
    #                  therefore deduped, so the two columns disagree on count by design.
    #   definition     a snapshot of the whole definition at trigger time, so `branch` stays
    #                  readable and the run's target is recorded as it was
    #
    # An integer term index was rejected: recompiling on edit silently changed its meaning,
    # so every past history row became a lie the moment someone retargeted a condition. The
    # snapshot is what makes this row self-contained once the subscription is gone.
    matched_events: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # The emission event ids that were in the event states when the condition completed.
    triggered_by: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # Free-form future-proofing. Reserved for needs that would otherwise require a migration.
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )

    __table_args__ = (
        # THE FENCE. Not an index for reads: it is what makes triggering idempotent. Two writers
        # that both see the condition satisfied insert the same (subscription_id, cycle), so
        # the DB rejects the second with an IntegrityError instead of starting a second run.
        # The winner bumps the subscription's cycle, which opens the next slot.
        #
        # It also serves a prefix seek by subscription, which is why listing a
        # subscription's fires needs no index of its own yet.
        sql.UniqueConstraint(subscription_id, cycle, name="uq_trigger_history_cycle"),
    )


def register_db_tables() -> None:
    """Explicitly import this module so the trigger models are registered with
    _TableBase.metadata before create_all() runs."""
    logger.info(
        f"Trigger tables registered: {TriggerSubscription.__tablename__}, "
        f"{TriggerEventState.__tablename__}, "
        f"{TriggerHistory.__tablename__}"
    )
