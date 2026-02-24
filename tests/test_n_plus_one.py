"""N+1 query detection tests.

These tests create realistic data and exercise the main API code paths,
asserting that the number of repeated query patterns stays below a threshold.
A failure here means a code path is issuing the same query once per row
instead of batching -- the classic N+1 problem.
"""

from __future__ import annotations

from typing import Callable

import pytest
import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql, backend_types_sql as bts, component_structures as cs
from cloud_pipelines_backend.query_tracker import QueryTracker

N_PLUS_ONE_THRESHOLD = 5
NUM_PIPELINE_RUNS = 10


def _make_simple_task(name: str) -> cs.TaskSpec:
    """Create a minimal single-container pipeline task."""
    component = cs.ComponentSpec(
        name=name,
        implementation=cs.ContainerImplementation(
            container=cs.ContainerSpec(image="python:3.12"),
        ),
    )
    return cs.TaskSpec(
        component_ref=cs.ComponentReference(spec=component),
    )


def _make_graph_task(name: str, num_children: int = 3) -> cs.TaskSpec:
    """Create a graph pipeline with multiple child tasks."""
    child_tasks = {}
    for i in range(num_children):
        child_component = cs.ComponentSpec(
            name=f"{name}-step-{i}",
            implementation=cs.ContainerImplementation(
                container=cs.ContainerSpec(image="python:3.12"),
            ),
        )
        child_tasks[f"step_{i}"] = cs.TaskSpec(
            component_ref=cs.ComponentReference(spec=child_component),
        )

    graph_component = cs.ComponentSpec(
        name=name,
        implementation=cs.GraphImplementation(
            graph=cs.GraphSpec(tasks=child_tasks),
        ),
    )
    return cs.TaskSpec(
        component_ref=cs.ComponentReference(spec=graph_component),
    )


def _seed_pipeline_runs(
    session_factory: Callable[[], orm.Session],
    service: api_server_sql.PipelineRunsApiService_Sql,
    count: int = NUM_PIPELINE_RUNS,
) -> list[api_server_sql.PipelineRunResponse]:
    """Create multiple pipeline runs with varied data."""
    runs = []
    for i in range(count):
        task = _make_graph_task(f"pipeline-{i}", num_children=3)
        response = service.create(
            session=session_factory(),
            root_task=task,
            created_by=f"user-{i % 3}",
            annotations={"env": "test", "run_index": str(i)},
        )
        runs.append(response)

        service.set_annotation(
            session=session_factory(),
            id=response.id,
            key="team",
            value=f"team-{i % 2}",
            user_name=f"user-{i % 3}",
        )
    return runs


class TestNPlusOneListPipelineRuns:
    """Ensure listing pipeline runs doesn't degrade into per-row queries."""

    def test_list_no_extras(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(session=session_factory())

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_list_with_pipeline_names(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                include_pipeline_names=True,
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_list_with_execution_stats(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                include_execution_stats=True,
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_list_with_all_extras(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                include_pipeline_names=True,
                include_execution_stats=True,
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)


class TestNPlusOneFilters:
    """Ensure filter queries don't cause per-row lookups."""

    def test_filter_by_status(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                filter="status:UNINITIALIZED",
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_filter_by_created_by(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                filter="created_by:user-0",
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_filter_by_pipeline_name(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                filter="pipeline_name:pipeline",
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_filter_by_annotation(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        with query_tracker:
            service.list(
                session=session_factory(),
                filter="annotation:team=team-0",
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)


class TestNPlusOneGetEndpoints:
    """Ensure individual get endpoints don't cause cascading queries."""

    def test_get_pipeline_run(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        runs = _seed_pipeline_runs(session_factory, service, count=1)

        with query_tracker:
            service.get(session=session_factory(), id=runs[0].id)

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)

    def test_get_execution_node(self, session_factory, query_tracker):
        service = api_server_sql.PipelineRunsApiService_Sql()
        runs = _seed_pipeline_runs(session_factory, service, count=1)

        exec_service = api_server_sql.ExecutionNodesApiService_Sql()
        with query_tracker:
            exec_service.get(
                session=session_factory(), id=runs[0].root_execution_id
            )

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)


class TestQueryTrackerDetection:
    """Verify the tracker actually catches N+1 patterns (sanity check)."""

    @pytest.mark.allow_n_plus_one
    def test_deliberate_n_plus_one_is_detected(self, session_factory, query_tracker):
        """A loop that queries per row must trigger an assertion."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service)

        session = session_factory()
        runs = session.execute(sql.select(bts.PipelineRun)).scalars().all()

        with query_tracker:
            for run in runs:
                session.execute(
                    sql.select(bts.ExecutionNode).where(
                        bts.ExecutionNode.id == run.root_execution_id
                    )
                )

        with pytest.raises(AssertionError, match="N\\+1 query pattern"):
            query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)


class TestGuardCatchesMissingTracker:
    """Demonstrate that CI fails when a DB test skips N+1 coverage.

    DELETE THIS CLASS once you've seen it in action.
    """

    # def test_missing_tracker_fails(self, session_factory):
    #     """Uses the DB but has no query_tracker -- the guard will fail this at teardown."""
    #     service = api_server_sql.PipelineRunsApiService_Sql()
    #     _seed_pipeline_runs(session_factory, service, count=1)

    def test_with_tracker_passes(self, session_factory, query_tracker):
        """Same test but with query_tracker -- the guard is satisfied."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        _seed_pipeline_runs(session_factory, service, count=3)

        with query_tracker:
            service.list(session=session_factory())

        query_tracker.assert_no_n_plus_one(threshold=N_PLUS_ONE_THRESHOLD)
