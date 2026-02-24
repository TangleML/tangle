from __future__ import annotations

from typing import Callable

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.query_tracker import QueryTracker

N_PLUS_ONE_THRESHOLD = 5


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "allow_n_plus_one: disable the requirement to use query_tracker in this test",
    )


@pytest.fixture
def db_engine() -> sqlalchemy.Engine:
    """Create a fresh in-memory SQLite engine with all migrations applied."""
    return database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")


@pytest.fixture
def session_factory(db_engine: sqlalchemy.Engine) -> Callable[[], orm.Session]:
    return lambda: orm.Session(bind=db_engine)


@pytest.fixture
def session(db_engine: sqlalchemy.Engine) -> orm.Session:
    return orm.Session(bind=db_engine)


@pytest.fixture
def query_tracker(db_engine: sqlalchemy.Engine) -> QueryTracker:
    """Provides a QueryTracker bound to the test's db engine.

    Usage:
        def test_something(query_tracker, session):
            with query_tracker:
                service.list(session=session)
            query_tracker.assert_no_n_plus_one(threshold=5)
    """
    return QueryTracker(engine=db_engine)


@pytest.fixture(autouse=True)
def _require_n_plus_one_coverage(request: pytest.FixtureRequest) -> None:
    """Enforce that every DB test either checks for N+1 or explicitly opts out.

    Any test using db_engine / session / session_factory must also request the
    ``query_tracker`` fixture OR be marked with ``@pytest.mark.allow_n_plus_one``.
    This prevents new code paths from silently skipping N+1 coverage.
    """
    yield

    uses_db = any(
        name in request.fixturenames
        for name in ("db_engine", "session", "session_factory")
    )
    if not uses_db:
        return

    if request.node.get_closest_marker("allow_n_plus_one"):
        return

    if "query_tracker" in request.fixturenames:
        return

    pytest.fail(
        "This test uses a database but has no N+1 query protection.\n"
        "Either:\n"
        "  1. Add the 'query_tracker' fixture and wrap your code under test with:\n"
        "       with query_tracker:\n"
        "           service.your_method(session=session)\n"
        "       query_tracker.assert_no_n_plus_one(threshold=5)\n"
        "  2. Add @pytest.mark.allow_n_plus_one to opt out\n"
    )
