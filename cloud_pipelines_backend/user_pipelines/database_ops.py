"""Portable database primitives for concurrent pipeline writes."""

from typing import Any

import sqlalchemy as sql
from sqlalchemy import orm
from sqlalchemy.exc import IntegrityError


def insert_with_integrity_fallback(
    *,
    session: orm.Session,
    table: sql.Table,
    values: dict[str, Any],
) -> IntegrityError | None:
    """Attempt an insert in a savepoint and return a possible integrity error.

    Callers verify that a conflicting row now exists before treating the error as
    an expected concurrent-write race. Returning the original error lets them
    re-raise unrelated integrity failures instead of silently downgrading them.
    """
    try:
        with session.begin_nested():
            session.execute(sql.insert(table).values(**values))
    except IntegrityError as exc:
        return exc
    return None
