"""Record the SQL a block of code actually emits.

A projection is only worth anything if the statement really stops asking for the
column, and "the endpoint still returns 200" cannot tell you that. These helpers
let a test assert the SELECT's shape and count instead of its outcome, so a
future edit that reintroduces a payload read fails here rather than showing up as
a slow query in production.

Column names are matched as whole words against the emitted statement text. That
is deliberately literal: it works on any dialect, needs no compilation step, and
catches the case the ORM would otherwise hide -- a deferred column being loaded
by a *second* statement rather than the one under test, which shows up as an
extra recorded SELECT.
"""

import collections.abc
import contextlib
import re

import sqlalchemy
from sqlalchemy import event


@contextlib.contextmanager
def capture_sql(
    engine: sqlalchemy.Engine,
) -> collections.abc.Iterator[list[str]]:
    """Collect every statement executed on `engine` inside the block."""
    statements: list[str] = []

    def _record(
        conn, cursor, statement, parameters, context, executemany
    ) -> None:  # noqa: ANN001
        del conn, cursor, parameters, context, executemany
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", _record)
    try:
        yield statements
    finally:
        event.remove(engine, "before_cursor_execute", _record)


def selects(statements: collections.abc.Iterable[str]) -> list[str]:
    """Just the SELECTs, in order."""
    return [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith("SELECT")
    ]


def selects_from(statements: collections.abc.Iterable[str], *, table: str) -> list[str]:
    """The SELECTs that read one table."""
    return [
        statement
        for statement in selects(statements)
        if re.search(rf"\bFROM\s+{re.escape(table)}\b", statement)
    ]


def mentions(statement: str, *, column: str) -> bool:
    """Does this statement name the column at all?"""
    return re.search(rf"\b{re.escape(column)}\b", statement) is not None


def asserted_absent(
    statements: collections.abc.Iterable[str],
    *,
    columns: collections.abc.Iterable[str],
) -> None:
    """Fail with the offending statement if any of these columns is selected."""
    for statement in statements:
        for column in columns:
            assert not mentions(
                statement, column=column
            ), f"{column!r} should not be selected here:\n{statement}"
