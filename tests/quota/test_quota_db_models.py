"""Unit tests for quota.db_models.

Two things are checked here: that `create_all()` really emits the constraints
the design depends on, and that they bite at runtime — a claim on a deleted
group disappears, a claim on a deleted node disappears, a negative capacity
is refused, and one node cannot hold claims in two groups.
"""

import datetime

import pytest
import sqlalchemy
from sqlalchemy import exc as sql_exc
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models


def _make_group(
    *,
    session: orm.Session,
    name: str = "bq",
    capacity: int = 2,
    created_by: str = "test@example.com",
) -> db_models.QuotaGroup:
    group = db_models.QuotaGroup(name=name, capacity=capacity, created_by=created_by)
    session.add(group)
    session.commit()
    return group


def _make_node(*, session: orm.Session, node_id: str) -> str:
    """A real execution_node row, so a claim's foreign key is satisfiable.

    `id` is init=False upstream, so it is assigned after construction to keep
    the readable ids these tests use.
    """
    node = bts.ExecutionNode(task_spec={})
    node.id = node_id
    session.add(node)
    session.commit()
    return node_id


def _make_claim(
    *,
    session: orm.Session,
    group: db_models.QuotaGroup,
    execution_node_id: str,
    state: db_models.ClaimState = db_models.ClaimState.ACTIVE,
) -> db_models.QuotaGroupClaim:
    claim = db_models.QuotaGroupClaim(
        quota_group_id=group.id,
        execution_node_id=execution_node_id,
        state=state,
    )
    session.add(claim)
    session.commit()
    return claim


class TestSchema:
    """What `create_all()` puts in the database."""

    def test_tables_are_created(self, db_engine: sqlalchemy.Engine) -> None:
        table_names = sqlalchemy.inspect(db_engine).get_table_names()

        assert "quota_group" in table_names
        assert "quota_group_claim" in table_names

    def test_claim_primary_key_is_composite(self, db_engine: sqlalchemy.Engine) -> None:
        primary_key = sqlalchemy.inspect(db_engine).get_pk_constraint(
            "quota_group_claim"
        )

        assert primary_key["constrained_columns"] == [
            "quota_group_id",
            "execution_node_id",
        ]

    def test_one_group_per_node_is_unique(self, db_engine: sqlalchemy.Engine) -> None:
        """The composite PK does not give this; a separate UNIQUE does."""
        unique_constraints = sqlalchemy.inspect(db_engine).get_unique_constraints(
            "quota_group_claim"
        )
        by_name = {c["name"]: c["column_names"] for c in unique_constraints}

        assert by_name["uq_quota_group_claim_node"] == ["execution_node_id"]

    def test_group_name_is_unique(self, db_engine: sqlalchemy.Engine) -> None:
        unique_constraints = sqlalchemy.inspect(db_engine).get_unique_constraints(
            "quota_group"
        )
        by_name = {c["name"]: c["column_names"] for c in unique_constraints}

        assert by_name["uq_quota_group_name"] == ["name"]

    def test_promotion_index_column_order(self, db_engine: sqlalchemy.Engine) -> None:
        """Order is load-bearing: two equality columns, then the sort column."""
        indexes = sqlalchemy.inspect(db_engine).get_indexes("quota_group_claim")
        by_name = {i["name"]: i["column_names"] for i in indexes}

        assert by_name["ix_quota_group_claim_state_created"] == [
            "quota_group_id",
            "state",
            "created_at",
            "execution_node_id",
        ]

    def test_promotion_index_sorts_ascending(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Reflection reports an index's columns but not their direction."""
        index = next(
            ix
            for ix in db_models.QuotaGroupClaim.__table__.indexes
            if ix.name == "ix_quota_group_claim_state_created"
        )
        create_index = str(sqlalchemy.schema.CreateIndex(index).compile(db_engine))

        assert "created_at ASC" in create_index

    def test_claim_foreign_keys_both_cascade(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A claim outlives neither its group nor its node."""
        foreign_keys = sqlalchemy.inspect(db_engine).get_foreign_keys(
            "quota_group_claim"
        )
        by_table = {fk["referred_table"]: fk for fk in foreign_keys}

        assert set(by_table) == {"quota_group", "execution_node"}
        assert by_table["quota_group"]["constrained_columns"] == ["quota_group_id"]
        assert by_table["execution_node"]["constrained_columns"] == [
            "execution_node_id"
        ]
        assert by_table["quota_group"]["options"]["ondelete"] == "CASCADE"
        assert by_table["execution_node"]["options"]["ondelete"] == "CASCADE"


class TestConstraintsBite:
    """The constraints at runtime, not just in the DDL."""

    def test_negative_capacity_is_rejected(self, session: orm.Session) -> None:
        with pytest.raises(sql_exc.IntegrityError):
            _make_group(session=session, capacity=-1)

    def test_zero_capacity_is_allowed(self, session: orm.Session) -> None:
        """capacity = 0 is the kill switch, not an error."""
        group = _make_group(session=session, capacity=0)

        assert group.capacity == 0

    def test_duplicate_group_name_is_rejected(self, session: orm.Session) -> None:
        _make_group(session=session, name="bq")

        with pytest.raises(sql_exc.IntegrityError):
            _make_group(session=session, name="bq")

    def test_node_cannot_be_in_two_groups(self, session: orm.Session) -> None:
        first = _make_group(session=session, name="bq")
        second = _make_group(session=session, name="gpu")
        node = _make_node(session=session, node_id="node-1")
        _make_claim(session=session, group=first, execution_node_id=node)

        with pytest.raises(sql_exc.IntegrityError):
            _make_claim(session=session, group=second, execution_node_id=node)

    def test_claim_for_an_unknown_node_is_rejected(self, session: orm.Session) -> None:
        """No execution_node row, so the foreign key refuses the claim."""
        group = _make_group(session=session)

        with pytest.raises(sql_exc.IntegrityError):
            _make_claim(session=session, group=group, execution_node_id="never-existed")

    def test_unknown_state_is_rejected(self, session: orm.Session) -> None:
        group = _make_group(session=session)
        node = _make_node(session=session, node_id="node-1")

        with pytest.raises(sql_exc.IntegrityError):
            session.execute(
                sqlalchemy.text(
                    "INSERT INTO quota_group_claim "
                    "(quota_group_id, execution_node_id, state, extra_data,"
                    " created_at, updated_at) "
                    "VALUES (:group_id, :node_id, 'RELEASED', '{}',"
                    " :now, :now)"
                ),
                {
                    "group_id": group.id,
                    "node_id": node,
                    "now": datetime.datetime.now(datetime.timezone.utc),
                },
            )


class TestCascade:
    def test_deleting_a_group_deletes_its_claims(self, session: orm.Session) -> None:
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            execution_node_id=_make_node(session=session, node_id="node-1"),
        )
        _make_claim(
            session=session,
            group=group,
            execution_node_id=_make_node(session=session, node_id="node-2"),
            state=db_models.ClaimState.WAITING,
        )
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    db_models.QuotaGroupClaim
                )
            )
            == 2
        )

        session.delete(group)
        session.commit()

        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    db_models.QuotaGroupClaim
                )
            )
            == 0
        )

    def test_deleting_one_group_leaves_another_groups_claims(
        self, session: orm.Session
    ) -> None:
        doomed = _make_group(session=session, name="bq")
        survivor = _make_group(session=session, name="gpu")
        _make_claim(
            session=session,
            group=doomed,
            execution_node_id=_make_node(session=session, node_id="node-1"),
        )
        _make_claim(
            session=session,
            group=survivor,
            execution_node_id=_make_node(session=session, node_id="node-2"),
        )

        session.delete(doomed)
        session.commit()

        remaining = session.scalars(sqlalchemy.select(db_models.QuotaGroupClaim)).all()
        assert [claim.execution_node_id for claim in remaining] == ["node-2"]

    def test_deleting_a_node_deletes_its_claim(self, session: orm.Session) -> None:
        """The other direction: a purged node takes its bookkeeping with it."""
        group = _make_group(session=session)
        doomed = _make_node(session=session, node_id="node-1")
        survivor = _make_node(session=session, node_id="node-2")
        _make_claim(session=session, group=group, execution_node_id=doomed)
        _make_claim(session=session, group=group, execution_node_id=survivor)

        session.execute(
            sqlalchemy.delete(bts.ExecutionNode).where(bts.ExecutionNode.id == doomed)
        )
        session.commit()

        remaining = session.scalars(sqlalchemy.select(db_models.QuotaGroupClaim)).all()
        assert [claim.execution_node_id for claim in remaining] == [survivor]
        assert session.get(db_models.QuotaGroup, group.id) is not None


class TestRoundTrip:
    def test_group_defaults(self, session: orm.Session) -> None:
        group = _make_group(session=session)
        session.refresh(group)

        assert len(group.id) == 20
        assert group.version == 0
        assert group.extra_data == {}
        assert group.created_at.tzinfo == datetime.timezone.utc
        assert group.updated_at.tzinfo == datetime.timezone.utc

    def test_claim_state_round_trips_as_an_enum(self, session: orm.Session) -> None:
        group = _make_group(session=session)
        _make_claim(
            session=session,
            group=group,
            execution_node_id=_make_node(session=session, node_id="node-1"),
            state=db_models.ClaimState.WAITING,
        )
        session.expire_all()

        loaded = session.get(db_models.QuotaGroupClaim, (group.id, "node-1"))
        assert loaded is not None
        assert loaded.state is db_models.ClaimState.WAITING

    def test_claim_created_at_survives_a_state_change(
        self, session: orm.Session
    ) -> None:
        """Re-parking must not push a node to the back of the promotion queue."""
        group = _make_group(session=session)
        claim = _make_claim(
            session=session,
            group=group,
            execution_node_id=_make_node(session=session, node_id="node-1"),
            state=db_models.ClaimState.WAITING,
        )
        first_seen = claim.created_at

        claim.state = db_models.ClaimState.ACTIVE
        session.commit()
        session.refresh(claim)

        assert claim.created_at == first_seen
        assert claim.updated_at >= first_seen
