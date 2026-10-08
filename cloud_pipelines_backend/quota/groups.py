"""Resolving a node's declared quota group to a row — and what happens when it does not.

The annotation names a group by string, so it can name one that was never created (a typo) or
one that existed at submission and was deleted before the node reached the gate. Both are
handled the same way: **the node launches ungated and no claim row is written.**

Nothing strands and no capacity leaks, because every later step misses symmetrically — the
promotion sink looks a claim up by `execution_node_id` and finds none. The real risk is not
corruption but silence: an author writes `bigquery-slot`, believes the node is capped at 4,
and gets no cap at all. So a miss leaves a durable marker on the node naming the string that
was wrong, the same mechanism the cache-hit path uses for `reused_from_execution_node_id`
(`orchestrator_sql.py:534`).

There is deliberately no submission-time rejection: the only place to validate is
`api_server_sql.py:1726`, which is upstream, and validating there would mean upstream code
querying `quota_group` — a table that exists only in application.

No metrics and no logging here; both hang off the resolution in quota/observability/.
"""

import dataclasses
from typing import Final

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)
from cloud_pipelines_backend.quota import db_models

# The key written into execution_node.extra_data when a node names a group that does not
# exist. Queryable in the database and visible on the node in the UI, and it names *which*
# string was wrong -- the counter in quota/observability/ says only that a miss happened.
MISSING_GROUP_MARKER: Final[str] = "quota_group_missing"


@dataclasses.dataclass(frozen=True, kw_only=True)
class GroupResolution:
    """What a node's quota-group annotation resolved to.

    `group is None` covers two different nodes and the caller treats them identically, but
    they are not the same event and instrumentation must tell them apart: a node that
    declared nothing is the overwhelming common case, a node that named a group nobody
    created is a mistake someone wants to hear about. `declared_name` separates them.
    """

    # The resolved row, or None when the node declared no group or named an unknown one.
    group: db_models.QuotaGroup | None
    # The name the node asked for, or None when it declared no group at all.
    declared_name: str | None

    @property
    def is_missing(self) -> bool:
        """True only for a node that named a group which does not exist."""
        return self.group is None and self.declared_name is not None


def get_group_by_name(
    *,
    session: orm.Session,
    name: str,
) -> db_models.QuotaGroup | None:
    """Look a quota group up by its name.

    Names are unique (`uq_quota_group_name`) and immutable: the API has no rename, so a
    name resolves to the same group for that group's whole life. Foreign keys still point
    at `id`, because a name is released for reuse when its group is deleted. The annotation
    is the one place a group is addressed by name.

    Args:
        session: The orchestrator's session; the lookup joins its transaction.
        name: The group name to find.

    Returns:
        The matching group, or None when no group carries that name.
    """
    return session.scalars(
        sql.select(db_models.QuotaGroup).where(db_models.QuotaGroup.name == name)
    ).one_or_none()


def resolve_group(
    *,
    session: orm.Session,
    execution: bts.ExecutionNode,
) -> GroupResolution:
    """Resolve the group a node declared, marking the node when the name matches nothing.

    The marker is written onto the passed-in node but not committed: the caller owns the
    transaction, and on the launch path the orchestrator commits shortly afterwards anyway.
    Writing it on every pass through the gate is intentional — a re-checked node whose group
    was created in the meantime resolves normally, and the marker is overwritten by that
    later, correct outcome only if the name changed.

    Args:
        session: The orchestrator's session.
        execution: The node being considered for launch.

    Returns:
        The resolution. A caller that gets `group is None` launches the node ungated and
        writes no claim row, whichever of the two reasons applies.
    """
    declared_name = quota_annotations.parse_task_spec_quota_group(
        task_spec=execution.task_spec
    )
    if declared_name is None:
        return GroupResolution(group=None, declared_name=None)

    group = get_group_by_name(session=session, name=declared_name)
    if group is None:
        _mark_missing_group(execution=execution, declared_name=declared_name)
    return GroupResolution(group=group, declared_name=declared_name)


def _mark_missing_group(
    *,
    execution: bts.ExecutionNode,
    declared_name: str,
) -> None:
    """Record on the node that it asked for a group that does not exist.

    `extra_data` is nullable upstream and mapped through MutableDict, so it is replaced when
    absent and mutated in place otherwise -- the same two-step the cache path performs at
    `orchestrator_sql.py:532`. Assigning into the MutableDict is what flags the attribute
    dirty; mutating a plain dict would be silently dropped at flush.

    Args:
        execution: The node to mark.
        declared_name: The group name that resolved to nothing.
    """
    if not execution.extra_data:
        execution.extra_data = {}
    execution.extra_data[MISSING_GROUP_MARKER] = declared_name
