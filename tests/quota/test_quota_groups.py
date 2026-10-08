"""Unit tests for quota.groups — resolving a declared group name to a row.

The case that matters is the one the design calls out: a node naming a group that does not
exist launches ungated, and says so on itself rather than failing quietly.
"""

from typing import Any

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.handlers.quota import annotations
from cloud_pipelines_backend.quota import db_models, groups

KEY = annotations.QUOTA_GROUP_KEY


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


def _make_node(
    *,
    session: orm.Session,
    node_id: str,
    node_annotations: dict[str, Any] | None = None,
) -> bts.ExecutionNode:
    """An execution_node row, optionally carrying task_spec annotations.

    `id` is init=False upstream, so it is assigned after construction to keep the readable
    ids these tests use.
    """
    task_spec: dict[str, Any] = {}
    if node_annotations is not None:
        task_spec["annotations"] = node_annotations
    node = bts.ExecutionNode(task_spec=task_spec)
    node.id = node_id
    session.add(node)
    session.commit()
    return node


class TestGetGroupByName:
    def test_found(self, session: orm.Session) -> None:
        _make_group(session=session, name="bq")
        found = groups.get_group_by_name(session=session, name="bq")
        assert found is not None
        assert found.name == "bq"

    def test_not_found_returns_none(self, session: orm.Session) -> None:
        assert groups.get_group_by_name(session=session, name="nope") is None

    def test_lookup_is_exact_not_prefix(self, session: orm.Session) -> None:
        _make_group(session=session, name="bigquery-slots")
        assert groups.get_group_by_name(session=session, name="bigquery") is None


class TestResolveGroup:
    def test_node_with_no_annotation_resolves_to_nothing_and_is_not_marked(
        self, session: orm.Session
    ) -> None:
        node = _make_node(session=session, node_id="node-1")
        resolution = groups.resolve_group(session=session, execution=node)

        assert resolution.group is None
        assert resolution.declared_name is None
        # Not a mistake — the overwhelming common case. Marking it would put the key on
        # every node in the fleet.
        assert not resolution.is_missing
        assert groups.MISSING_GROUP_MARKER not in (node.extra_data or {})

    def test_known_group_resolves_to_the_row(self, session: orm.Session) -> None:
        group = _make_group(session=session, name="bq")
        node = _make_node(
            session=session, node_id="node-1", node_annotations={KEY: "bq"}
        )

        resolution = groups.resolve_group(session=session, execution=node)

        assert resolution.group is not None
        assert resolution.group.id == group.id
        assert resolution.declared_name == "bq"
        assert not resolution.is_missing
        assert groups.MISSING_GROUP_MARKER not in (node.extra_data or {})

    def test_unknown_group_launches_ungated_and_marks_the_node(
        self, session: orm.Session
    ) -> None:
        node = _make_node(
            session=session,
            node_id="node-1",
            node_annotations={KEY: "bigquery-slot"},
        )

        resolution = groups.resolve_group(session=session, execution=node)

        assert resolution.group is None
        assert resolution.declared_name == "bigquery-slot"
        assert resolution.is_missing
        # The marker names *which* string was wrong; that is the whole point of it.
        assert node.extra_data[groups.MISSING_GROUP_MARKER] == "bigquery-slot"

    def test_the_marker_survives_a_commit(self, session: orm.Session) -> None:
        # extra_data is mapped through MutableDict; mutating a plain dict in place would be
        # dropped at flush and the miss would be invisible after all.
        node = _make_node(
            session=session, node_id="node-1", node_annotations={KEY: "typo"}
        )
        groups.resolve_group(session=session, execution=node)
        session.commit()
        session.expunge_all()

        reloaded = session.get(bts.ExecutionNode, "node-1")
        assert reloaded is not None
        assert reloaded.extra_data[groups.MISSING_GROUP_MARKER] == "typo"

    def test_a_group_created_later_resolves_and_overwrites_a_stale_marker(
        self, session: orm.Session
    ) -> None:
        # A parked-then-rechecked node whose group has since been created must gate for
        # real. The stale marker is left behind deliberately: it records that the miss
        # happened, and the resolution the caller acts on is the fresh one.
        node = _make_node(
            session=session, node_id="node-1", node_annotations={KEY: "bq"}
        )
        assert groups.resolve_group(session=session, execution=node).is_missing

        _make_group(session=session, name="bq")
        again = groups.resolve_group(session=session, execution=node)

        assert again.group is not None
        assert not again.is_missing

    def test_the_orm_itself_refuses_a_non_dict_task_spec(
        self, session: orm.Session
    ) -> None:
        # Why resolve_group cannot be handed a malformed node through the ORM, and why the
        # isinstance guard in parse_task_spec_quota_group is nonetheless kept: task_spec is
        # mapped through MutableDict, which coerces on assignment *and* on load, so only a
        # row written outside the ORM could ever carry a non-dict -- and that row would
        # raise here, at the attribute, rather than inside the gate.
        node = _make_node(session=session, node_id="node-1")
        with pytest.raises(ValueError, match="does not accept objects of type"):
            node.task_spec = "not a dict"

    def test_a_task_spec_without_annotations_resolves_to_nothing(
        self, session: orm.Session
    ) -> None:
        node = _make_node(session=session, node_id="node-1")
        assert node.task_spec == {}

        resolution = groups.resolve_group(session=session, execution=node)

        assert resolution.group is None
        assert resolution.declared_name is None
