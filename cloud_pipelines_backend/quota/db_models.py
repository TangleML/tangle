"""Quota group DB tables.

Two tables. `quota_group` is the cap; `quota_group_claim` is one node's place
in that cap — `WAITING` if it asked and was parked, `ACTIVE` if it won a slot,
`DONE` once its node ended and the slot went back.

Both are registered on the shared `bts._TableBase.metadata`, so
`metadata.create_all()` creates them alongside the upstream tables.

A claim points at both its parents with a real foreign key, and both cascade:
delete the group and its claims go, delete the execution node and its claim
goes. A claim is bookkeeping about a node, so it can never outlive one.
"""

import datetime
import enum
import logging
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.utils import db as db_utils

logger = logging.getLogger(__name__)


class ClaimState(str, enum.Enum):
    """A claim's lifecycle: WAITING -> ACTIVE -> DONE, or straight to ACTIVE.

    `DONE` is terminal and rows are never deleted, so the table is a ledger of
    every node that ever used a group. It is not the source of truth for
    occupancy: that is read live from `execution_node.container_execution_status`,
    because the sink that writes `DONE` is best-effort and can lag. `DONE` is a
    selectivity hint that keeps the ledger out of the hot queries, and a record.
    """

    WAITING = "WAITING"
    ACTIVE = "ACTIVE"
    DONE = "DONE"


# The two states a claim can be in while it still means something about the present. DONE is
# the third, and it is the group's history: unbounded, and nothing an unfiltered read should
# ever pull back. Every endpoint that does not take an explicit state filters to these, and
# the occupancy join narrows to them for the same reason.
#
# It lives here rather than in either caller because both of them must mean the same thing by
# "live" -- a state added to the enum has one place to be added to, not two that can drift.
LIVE_CLAIM_STATES: Final[tuple[ClaimState, ...]] = (
    ClaimState.WAITING,
    ClaimState.ACTIVE,
)


class QuotaGroup(bts._TableBase):
    """A named cap on how many executions may hold a container at once.

    Identity is `id`; `name` is an immutable alias, fixed at creation. `capacity = 0` is legal and
    acts as a kill switch. `version` is the serialization point — every
    admission bumps it under a compare-and-set, so two concurrent admissions
    cannot both read the same occupancy and both win.

    Ownership is `created_by` and nothing else: the creating principal is the
    group's only editor, for the life of the group.
    """

    __tablename__ = "quota_group"

    # Column types inherited from _TableBase.type_annotation_map:
    #   str -> String(255), datetime -> UtcDateTime, dict -> MutableDict(JSON)

    id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        primary_key=True,
        init=False,
        insert_default=bts.generate_unique_id,
    )
    name: orm.Mapped[str] = orm.mapped_column()
    capacity: orm.Mapped[int] = orm.mapped_column()
    created_by: orm.Mapped[str] = orm.mapped_column()
    # The compare-and-set point. Bumped by every admission; see the
    # business-logic doc for the protocol.
    version: orm.Mapped[int] = orm.mapped_column(default=0)
    extra_data: orm.Mapped[dict[str, Any]] = orm.mapped_column(default_factory=dict)
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
        onupdate=db_utils.utc_now,
    )

    __table_args__ = (
        # Names are unique, but a name is released for reuse when its group is
        # deleted -- so it identifies a group only for that group's lifetime.
        # Foreign keys point at `id`, which does not get recycled.
        sql.UniqueConstraint(name, name="uq_quota_group_name"),
        # `capacity = 0` is legal — it is the kill switch. Negative is not.
        sql.CheckConstraint("capacity >= 0", name="ck_quota_group_capacity"),
    )


class QuotaGroupClaim(bts._TableBase):
    """One execution node's place in one quota group.

    `created_at` is the node's *first* admission attempt and is never updated,
    so a node that loses a race and is re-parked keeps its place in line.
    Ordering promotion by `updated_at` instead would starve it.
    """

    __tablename__ = "quota_group_claim"

    # Two constraints on this table, and neither is redundant -- they forbid
    # different things.
    #
    #   PRIMARY KEY (quota_group_id, execution_node_id)   the PAIR is distinct
    #   UNIQUE      (execution_node_id)                   the COLUMN is distinct
    #
    #   grp-A + node-1   ok
    #   grp-A + node-1   rejected by the PK
    #   grp-B + node-1   ACCEPTED by the PK, rejected by the UNIQUE
    #
    # A composite primary key is unique -- that is not the gap. The gap is that it
    # is unique over the pair, and "one group per node" is a claim about one of its
    # columns.
    #
    # Composite rather than a surrogate `id`: the pair already is the identity of a
    # claim, so a generated key would be a second name for a thing that has one. The
    # InnoDB consequence is deliberate -- the PK is the clustered index, so rows sort
    # physically by (quota_group_id, execution_node_id) and a group's claims sit
    # together, which is what every read here asks for. It also means the key is
    # frozen at first create_all() along with the rest of the table.
    quota_group_id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        sql.ForeignKey(
            QuotaGroup.id,
            ondelete="CASCADE",
            name="fk_quota_group_claim_group",
        ),
        primary_key=True,
    )
    # `execution_node` is an upstream Tangle OSS table, but it lives on the same
    # metadata, so the FK is enforceable. CASCADE rather than the default
    # RESTRICT: a claim describes a node, so a purge that removes the node must
    # not be blocked by our bookkeeping.
    execution_node_id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        sql.ForeignKey(
            bts.ExecutionNode.id,
            ondelete="CASCADE",
            name="fk_quota_group_claim_node",
        ),
        primary_key=True,
    )
    state: orm.Mapped[ClaimState] = orm.mapped_column(
        # native_enum=False is the dialect-agnostic option: identical DDL on
        # SQLite and MySQL -- a VARCHAR(16) plus a CHECK we name. The native
        # form would be a different object per backend (MySQL ENUM, Postgres
        # CREATE TYPE), each needing its own ALTER to add a state.
        # values_callable makes the DB store "ACTIVE", not the member name.
        sql.Enum(
            ClaimState,
            native_enum=False,
            length=16,
            # SQLAlchemy 2.0 defaults create_constraint to False, which would
            # leave the column an unconstrained VARCHAR.
            create_constraint=True,
            values_callable=lambda enum_class: [member.value for member in enum_class],
            name="ck_quota_group_claim_state",
        ),
    )
    # When this claim last entered WAITING; NULL whenever it is not parked.
    #
    # Written explicitly at every site that writes `state`, never by `onupdate`, and that is
    # the whole reason the column earns its place rather than reusing `updated_at` below. A
    # re-park assigns WAITING over WAITING and the same group id over itself: the row is not
    # dirty, no UPDATE is emitted, and `updated_at` stands still through a whole park cycle.
    # A fresh timestamp is always a changed value, so it always emits one.
    #
    # Nullable, rather than "whenever it was last parked, whatever it is doing now": NULL is
    # a fact the reconciler can read, and it keeps the column meaning what its name says.
    parked_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(default=None)
    # {"history": [{"state": ..., "at": ...}, ...]}
    extra_data: orm.Mapped[dict[str, Any]] = orm.mapped_column(default_factory=dict)
    # FIFO order for promotion. Never updated.
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
        onupdate=db_utils.utc_now,
    )

    __table_args__ = (
        # One group per node. The composite primary key does NOT give this:
        # it only forbids the same node claiming the same group twice.
        sql.UniqueConstraint(execution_node_id, name="uq_quota_group_claim_node"),
        # Promotion reads
        #   WHERE quota_group_id = ? AND state = 'WAITING'
        #   ORDER BY created_at ASC LIMIT n
        # and the claims endpoint pages with a keyset on (created_at, node id).
        # Column ORDER is load-bearing: the first two columns are equality
        # predicates picking a contiguous slice, and only created_at is sorted.
        # Turns a filesort over every waiter into a range scan that stops at
        # LIMIT. ASC is the default on both dialects and is spelled out so the
        # index and the query above cannot silently disagree.
        #
        # `state` sitting second is what keeps DONE rows -- eventually most of
        # the table -- out of every WAITING/ACTIVE read. A query that wants that
        # must spell state as a top-level equality predicate; one that buries it
        # in an OR gets only the quota_group_id prefix and scans the ledger.
        #
        # execution_node_id is last because created_at alone is not unique. Claims are
        # created in batches and two can land on the same timestamp, so it is a tiebreaker
        # first and an index column second.
        #
        # The claims endpoint pages with a row-value comparison, not an offset:
        #
        #     WHERE (created_at, execution_node_id) > (:last_created_at, :last_node_id)
        #     ORDER BY created_at, execution_node_id
        #     LIMIT 50
        sql.Index(
            "ix_quota_group_claim_state_created",
            quota_group_id,
            state,
            created_at.asc(),
            execution_node_id,
        ),
    )


def register_db_tables() -> None:
    """Explicitly import this module so the quota tables are registered
    with _TableBase.metadata before create_all() runs."""
    logger.info(
        f"Quota group tables registered: {QuotaGroup.__tablename__}, {QuotaGroupClaim.__tablename__}"
    )
