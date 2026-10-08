"""Tests for the run-status cache against SQLite + real models.

Each case builds a run out of a root node plus linked children in given statuses, then asks the
cache what the run's ended state is. The children are what the run's per-status counts are taken
from, so the statuses in the list are exactly the run's executions.

The caching cases count SQL statements off the engine, because a cached answer and a queried one
are identical: only the query count tells them apart.
"""

import collections.abc

import pytest
import sqlalchemy as sql
from sqlalchemy import event, orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.utils import run_status

CES = bts.ContainerExecutionStatus


@pytest.fixture()
def statements(
    db_engine: sql.Engine,
) -> collections.abc.Generator[list[str], None, None]:
    """Every SQL statement the engine executes while a test runs.

    Yields:
        The list of statements, appended to as they execute.
    """
    captured: list[str] = []

    def record(
        conn: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        """Append one executed statement. SQLAlchemy calls this positionally."""
        captured.append(statement)

    event.listen(db_engine, "before_cursor_execute", record)
    try:
        yield captured
    finally:
        event.remove(db_engine, "before_cursor_execute", record)


# --- builders ---------------------------------------------------------------


def _make_node(
    *,
    session: orm.Session,
    status: CES | None = None,
    container_execution_id: str | None = "ce-1",
) -> bts.ExecutionNode:
    """Create and flush an ExecutionNode in the given status."""
    node = bts.ExecutionNode(task_spec={}, container_execution_status=status)
    session.add(node)
    session.flush()
    # container_execution_id is a non-init mapped column, so set it after construction.
    node.container_execution_id = container_execution_id
    session.flush()
    return node


def _run_with_children(
    *,
    session: orm.Session,
    child_statuses: list[CES],
) -> bts.PipelineRun:
    """Create a run whose root has one linked child per given status."""
    root = _make_node(session=session, container_execution_id="ce-root")
    run = bts.PipelineRun(root_execution=root)
    session.add(run)
    session.flush()
    for index, status in enumerate(child_statuses):
        child = _make_node(
            session=session, status=status, container_execution_id=f"ce-{index}"
        )
        session.add(
            bts.ExecutionToAncestorExecutionLink(
                ancestor_execution=root, execution=child
            )
        )
    session.commit()
    return run


def _has_ended(
    *,
    session: orm.Session,
    pipeline_run_id: str | None,
) -> run_status.RunEndedState:
    """Ask a fresh cache, so no case can read a verdict another case stored."""
    return run_status.RunStatusCache().has_ended(
        session=session, pipeline_run_id=pipeline_run_id
    )


# --- a run that ended cleanly -----------------------------------------------


class TestEndedSucceeded:
    """A run is SUCCEEDED when every execution is in (SUCCEEDED, SKIPPED) and one succeeded."""

    def test_all_succeeded(
        self,
        session: orm.Session,
    ) -> None:
        run = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.SUCCEEDED]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.SUCCEEDED

    def test_succeeded_and_skipped(
        self,
        session: orm.Session,
    ) -> None:
        """A skipped node is the conditional-execution path, so it keeps the run clean."""
        run = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.SKIPPED]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.SUCCEEDED


# --- a run that ended badly --------------------------------------------------


class TestEndedFailed:
    """One execution outside (SUCCEEDED, SKIPPED), or no succeeded one, makes a run FAILED."""

    def test_all_skipped(
        self,
        session: orm.Session,
    ) -> None:
        """A run that carried no work out has no success to report, clean or not."""
        run = _run_with_children(
            session=session, child_statuses=[CES.SKIPPED, CES.SKIPPED]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.FAILED

    def test_succeeded_and_failed(
        self,
        session: orm.Session,
    ) -> None:
        run = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.FAILED]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.FAILED

    def test_skipped_and_failed(
        self,
        session: orm.Session,
    ) -> None:
        """Skipped keeps a run clean on its own; it does not absorb a real failure."""
        run = _run_with_children(
            session=session, child_statuses=[CES.SKIPPED, CES.FAILED]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.FAILED

    def test_cancelled_and_system_error(
        self,
        session: orm.Session,
    ) -> None:
        """Every ended status other than SUCCEEDED and SKIPPED counts against the run."""
        run = _run_with_children(
            session=session, child_statuses=[CES.CANCELLED, CES.SYSTEM_ERROR]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.FAILED


# --- no answer to give yet ---------------------------------------------------


class TestNotEnded:
    """Cases with no aggregate status: still in flight, or no run to look at."""

    def test_one_execution_in_flight(
        self,
        session: orm.Session,
    ) -> None:
        run = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.RUNNING]
        )

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state == run_status.RunEndedState(has_ended=False, status=None)

    def test_no_run_id(
        self,
        session: orm.Session,
    ) -> None:
        state = _has_ended(session=session, pipeline_run_id=None)

        assert state == run_status.RunEndedState(has_ended=False, status=None)

    def test_run_with_no_executions(
        self,
        session: orm.Session,
    ) -> None:
        """A run points at its root, and the root is not linked to itself, so it counts none."""
        run = _run_with_children(session=session, child_statuses=[])

        state = _has_ended(session=session, pipeline_run_id=run.id)

        assert state == run_status.RunEndedState(has_ended=False, status=None)


# --- caching: only verdicts that cannot change ------------------------------


class TestCaching:
    """What the cache stores, what it refuses to store, and how many queries that costs.

    A cached verdict and a fresh one are indistinguishable by value, so every case here counts
    statements. Two also read the cache directly, to pin *what* was stored rather than only how
    often the database was asked.
    """

    def test_an_ended_run_is_resolved_once(
        self,
        session: orm.Session,
        statements: list[str],
    ) -> None:
        run_id = _run_with_children(session=session, child_statuses=[CES.SUCCEEDED]).id
        cache = run_status.RunStatusCache()

        statements.clear()
        verdicts = [
            cache.has_ended(session=session, pipeline_run_id=run_id) for _ in range(5)
        ]

        assert len(statements) == 2
        assert verdicts == [verdicts[0]] * 5
        assert verdicts[0].has_ended is True

    def test_a_run_in_flight_is_never_cached(
        self,
        session: orm.Session,
        statements: list[str],
    ) -> None:
        """The verdict that goes stale is the one never stored, so every ask re-queries."""
        run_id = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.RUNNING]
        ).id
        cache = run_status.RunStatusCache()

        statements.clear()
        for _ in range(5):
            state = cache.has_ended(session=session, pipeline_run_id=run_id)
            assert state.has_ended is False

        assert len(statements) == 10

    def test_a_run_that_ends_is_seen_as_ended(
        self,
        session: orm.Session,
    ) -> None:
        """The point of storing nothing for a run in flight: the next ask sees the truth."""
        run = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.RUNNING]
        )
        cache = run_status.RunStatusCache()
        assert (
            cache.has_ended(session=session, pipeline_run_id=run.id).has_ended is False
        )

        in_flight = session.scalars(
            sql.select(bts.ExecutionNode).where(
                bts.ExecutionNode.container_execution_status == CES.RUNNING
            )
        ).one()
        in_flight.container_execution_status = CES.SUCCEEDED
        session.commit()

        state = cache.has_ended(session=session, pipeline_run_id=run.id)

        assert state.has_ended is True
        assert state.status is run_status.RunEndedStatus.SUCCEEDED

    def test_an_ended_verdict_is_reused_across_sessions(
        self,
        db_engine: sql.Engine,
        session: orm.Session,
        statements: list[str],
    ) -> None:
        """A terminal verdict outlives the session that read it — the reuse this exists for."""
        run_id = _run_with_children(session=session, child_statuses=[CES.SUCCEEDED]).id
        cache = run_status.RunStatusCache()

        statements.clear()
        with orm.Session(bind=db_engine) as first:
            first_state = cache.has_ended(session=first, pipeline_run_id=run_id)
        with orm.Session(bind=db_engine) as second:
            second_state = cache.has_ended(session=second, pipeline_run_id=run_id)

        assert len(statements) == 2
        assert first_state == second_state

    def test_two_ended_runs_are_kept_apart(
        self,
        session: orm.Session,
        statements: list[str],
    ) -> None:
        succeeded_id = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED]
        ).id
        failed_id = _run_with_children(session=session, child_statuses=[CES.FAILED]).id
        cache = run_status.RunStatusCache()

        statements.clear()
        for _ in range(2):
            succeeded_state = cache.has_ended(
                session=session, pipeline_run_id=succeeded_id
            )
            failed_state = cache.has_ended(session=session, pipeline_run_id=failed_id)

        assert len(statements) == 4
        assert succeeded_state.status is run_status.RunEndedStatus.SUCCEEDED
        assert failed_state.status is run_status.RunEndedStatus.FAILED

    def test_the_cache_holds_only_the_terminal_verdict(
        self,
        session: orm.Session,
    ) -> None:
        ended = _run_with_children(session=session, child_statuses=[CES.SUCCEEDED])
        in_flight = _run_with_children(session=session, child_statuses=[CES.RUNNING])
        cache = run_status.RunStatusCache()

        ended_verdict = cache.has_ended(session=session, pipeline_run_id=ended.id)
        cache.has_ended(session=session, pipeline_run_id=in_flight.id)

        assert cache._ended == {ended.id: ended_verdict}

    def test_no_run_id_never_queries_and_never_caches(
        self,
        session: orm.Session,
        statements: list[str],
    ) -> None:
        cache = run_status.RunStatusCache()

        statements.clear()
        state = cache.has_ended(session=session, pipeline_run_id=None)

        assert statements == []
        assert state == run_status.RunEndedState(has_ended=False, status=None)
        assert cache._ended == {}


# --- what this module needs from the submodule -------------------------------


class TestUpstreamContract:
    """The shape `RunStatusCache` borrows from `backend/`, asserted where a bump will run it.

    The cache answers through `PipelineRunsApiService_Sql._get_execution_stats_and_summary` so
    that "has this run ended" cannot mean two different things in one process. That method is
    private, and `backend/` is a git submodule of TangleML/tangle, so nothing upstream is
    obliged to keep it. This test is the signal: a rename raises at the call below, and a change
    of meaning shows up in the values — either way it fails here, in this repo's own suite,
    rather than in production.
    """

    def test_the_summary_still_carries_what_the_verdict_reads(
        self,
        session: orm.Session,
    ) -> None:
        """Both halves of the contract, in one call: the method is reachable, and it still means
        what the verdict reads.

        Upstream deciding to count NULL-status executions, or to report `has_ended` some other
        way, would leave the signature intact and quietly change every verdict this module
        gives — so the values, not just the names, are pinned.

        The third child has **no status**, which is what a graph (composite) node looks like:
        upstream filters those out of the statistics, and this module's whole reading of a run
        depends on that. Without it here, dropping the filter upstream would change nothing in
        this test and the drift would pass unnoticed.
        """
        run = _run_with_children(
            session=session, child_statuses=[CES.SUCCEEDED, CES.RUNNING, None]
        )

        (
            stats,
            summary,
        ) = api_server_sql.PipelineRunsApiService_Sql()._get_execution_stats_and_summary(
            session=session,
            root_execution_id=run.root_execution_id,
        )

        assert stats == {CES.SUCCEEDED.value: 1, CES.RUNNING.value: 1}
        assert (summary.total_executions, summary.ended_executions) == (2, 1)
        assert summary.has_ended is False
