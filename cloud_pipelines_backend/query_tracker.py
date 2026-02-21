"""SQLAlchemy query tracker for detecting N+1 query patterns.

Usage as a context manager:

    with QueryTracker(engine) as tracker:
        service.list(session=session, ...)

    tracker.assert_no_n_plus_one(threshold=5)

Or with automatic assertion:

    with QueryTracker(engine, n_plus_one_threshold=5):
        service.list(session=session, ...)
"""

from __future__ import annotations

import re
import textwrap
from collections import Counter
from dataclasses import dataclass, field

import sqlalchemy
from sqlalchemy import event


_PARAM_PATTERNS = [
    re.compile(r"'[^']*'"),
    re.compile(r"\b\d+\b"),
    re.compile(r"\?"),
]


def _normalize_sql(sql: str) -> str:
    """Normalize a SQL statement for grouping: collapse params, whitespace."""
    normalized = sql.strip()
    for pattern in _PARAM_PATTERNS:
        normalized = pattern.sub("?", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


@dataclass
class QueryRecord:
    sql: str
    normalized: str
    parameters: object = None


@dataclass
class NPlusOneViolation:
    normalized_sql: str
    count: int
    example_sql: str


@dataclass
class QueryTracker:
    """Track SQL queries on a SQLAlchemy engine and detect N+1 patterns."""

    engine: sqlalchemy.Engine
    n_plus_one_threshold: int | None = None
    queries: list[QueryRecord] = field(default_factory=list, init=False)
    _listening: bool = field(default=False, init=False)

    def _on_before_execute(
        self, conn, cursor, statement, parameters, context, executemany
    ):
        record = QueryRecord(
            sql=statement,
            normalized=_normalize_sql(statement),
            parameters=parameters,
        )
        self.queries.append(record)

    def start(self) -> None:
        if self._listening:
            return
        event.listen(
            self.engine, "before_cursor_execute", self._on_before_execute
        )
        self._listening = True

    def stop(self) -> None:
        if not self._listening:
            return
        event.remove(
            self.engine, "before_cursor_execute", self._on_before_execute
        )
        self._listening = False

    def reset(self) -> None:
        self.queries.clear()

    @property
    def query_count(self) -> int:
        return len(self.queries)

    def get_pattern_counts(self) -> Counter[str]:
        return Counter(q.normalized for q in self.queries)

    def find_n_plus_one(
        self, threshold: int = 5, selects_only: bool = False
    ) -> list[NPlusOneViolation]:
        """Find query patterns that repeat more than `threshold` times.

        Args:
            threshold: A pattern must repeat more than this many times.
            selects_only: When True, only consider SELECT statements. Useful for
                auto-detection where INSERT/UPDATE from test setup would be
                false positives.
        """
        queries = self.queries
        if selects_only:
            queries = [q for q in queries if q.normalized.upper().startswith("SELECT")]

        counts: Counter[str] = Counter(q.normalized for q in queries)
        example_map: dict[str, str] = {}
        for q in queries:
            if q.normalized not in example_map:
                example_map[q.normalized] = q.sql

        return [
            NPlusOneViolation(
                normalized_sql=pattern,
                count=count,
                example_sql=example_map[pattern],
            )
            for pattern, count in counts.most_common()
            if count > threshold
        ]

    def assert_no_n_plus_one(self, threshold: int = 5) -> None:
        """Raise AssertionError if any query pattern exceeds the threshold."""
        violations = self.find_n_plus_one(threshold)
        if violations:
            parts = [
                f"Detected {len(violations)} N+1 query pattern(s) "
                f"(threshold={threshold}):\n"
            ]
            for v in violations:
                parts.append(
                    f"  [{v.count}x] {textwrap.shorten(v.normalized_sql, 120)}\n"
                    f"        example: {textwrap.shorten(v.example_sql, 120)}\n"
                )
            raise AssertionError("".join(parts))

    def summary(self) -> str:
        """Human-readable summary of captured queries."""
        counts = self.get_pattern_counts()
        lines = [f"Total queries: {self.query_count}"]
        for pattern, count in counts.most_common():
            lines.append(f"  [{count:>3}x] {textwrap.shorten(pattern, 100)}")
        return "\n".join(lines)

    def __enter__(self) -> QueryTracker:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        if exc_type is None and self.n_plus_one_threshold is not None:
            self.assert_no_n_plus_one(self.n_plus_one_threshold)
