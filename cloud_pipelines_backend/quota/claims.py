"""Finding a node's claim row.

One query, shared by the two sides that need it and owned by neither. The admission gate
(`quota/interceptor.py`) asks "does this node already hold a slot here?" before letting it
through; the promotion sink (`emissions/handlers/quota/sinks/quota_group.py`) asks "which
group was this node in?" after it ends. Both are the same lookup, and a second copy of it in
the sink would be a copy free to drift from the uniqueness assumption below.

It lives in `quota/` rather than under `emissions/` because it is a fact about the domain, and
because the sink may import `quota/` while the reverse is deliberately impossible.
"""

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend.quota import db_models


def find_claim(
    *,
    session: orm.Session,
    execution_node_id: str,
) -> db_models.QuotaGroupClaim | None:
    """Find a node's claim, whichever group it is in.

    Keyed on the node alone, not on the composite primary key, because `execution_node_id` is
    unique across the table (`uq_quota_group_claim_node`): a node holds at most one claim
    anywhere. `one_or_none()` rather than `first()` so that if that constraint is ever relaxed
    this raises instead of silently picking a row.

    Args:
        session: The caller's session.
        execution_node_id: The node to look up.

    Returns:
        The claim, or None when the node has never asked any group for a slot.
    """
    return session.scalars(
        sql.select(db_models.QuotaGroupClaim).where(
            db_models.QuotaGroupClaim.execution_node_id == execution_node_id
        )
    ).one_or_none()
