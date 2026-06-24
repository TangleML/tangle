"""Tests for the orchestrator's transient-infra self-heal (re-queue in place).

When a launched container reports a transient infrastructure failure (e.g. a
wedged gcsfuse sidecar), the orchestrator terminates the wedged pod, marks its
ContainerExecution SYSTEM_ERROR, and re-queues the same ExecutionNode within the
same run — up to `_MAX_TRANSIENT_INFRA_RETRIES` attempts, after which the node is
failed and its downstream skipped. The launcher and storage are faked so the
tests run offline.
"""

from __future__ import annotations

from typing import Callable
from unittest import mock

import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces

_USER = "user1"


def _initialize_db_and_get_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _make_single_container_root_task() -> component_structures.TaskSpec:
    container_component = component_structures.ComponentSpec(
        name="wedge-test",
        implementation=component_structures.ContainerImplementation(
            container=component_structures.ContainerSpec(image="python")
        ),
    )
    container_task = component_structures.TaskSpec(
        component_ref=component_structures.ComponentReference(spec=container_component),
    )
    pipeline_spec = component_structures.ComponentSpec(
        name="wedge-test-pipeline",
        implementation=component_structures.GraphImplementation(
            graph=component_structures.GraphSpec(tasks={"task": container_task})
        ),
    )
    return component_structures.TaskSpec(
        component_ref=component_structures.ComponentReference(spec=pipeline_spec),
    )


class _FakeLaunchedContainer:
    """Always PENDING; optionally reports a transient infra failure reason."""

    def __init__(self, *, reason: str | None = None):
        self._reason = reason
        self.terminate_calls = 0

    @property
    def status(self) -> launcher_interfaces.ContainerStatus:
        return launcher_interfaces.ContainerStatus.PENDING

    def to_dict(self) -> dict:
        return {"fake_launcher_data": True}

    def transient_infra_failure_reason(self) -> str | None:
        return self._reason

    def terminate(self) -> None:
        self.terminate_calls += 1

    def upload_log(self) -> None:
        pass


class _FakeLauncher:
    """Launches PENDING containers; refresh reports `self.reason` (if set)."""

    def __init__(self):
        self.reason: str | None = None
        self.launch_count = 0
        self.refreshed_containers: list[_FakeLaunchedContainer] = []

    def launch_container_task(self, **kwargs) -> _FakeLaunchedContainer:
        self.launch_count += 1
        return _FakeLaunchedContainer()

    def deserialize_launched_container_from_dict(self, d) -> _FakeLaunchedContainer:
        return _FakeLaunchedContainer()

    def get_refreshed_launched_container_from_dict(self, d) -> _FakeLaunchedContainer:
        container = _FakeLaunchedContainer(reason=self.reason)
        self.refreshed_containers.append(container)
        return container


def _make_orchestrator(session_factory, launcher):
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )


def _create_run(session_factory):
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=_make_single_container_root_task(),
        created_by=_USER,
    )


def _all_container_executions(session_factory) -> list[bts.ContainerExecution]:
    with session_factory() as session:
        return list(session.execute(sql.select(bts.ContainerExecution)).scalars().all())


def _the_execution_node(session_factory) -> bts.ExecutionNode:
    with session_factory() as session:
        nodes = list(session.execute(sql.select(bts.ExecutionNode)).scalars().all())
        # A single-container run has exactly one leaf ExecutionNode.
        leaf_nodes = [n for n in nodes if n.container_execution_cache_key is not None]
        assert len(leaf_nodes) == 1, [n.id for n in nodes]
        return leaf_nodes[0]


def test_transient_infra_failure_requeues_node():
    session_factory = _initialize_db_and_get_session_factory()
    launcher = _FakeLauncher()
    launcher.reason = "gke-gcsfuse-sidecar bucket-access-check timeout"
    orchestrator = _make_orchestrator(session_factory, launcher)

    _create_run(session_factory)

    # One sweep: queued launches the task (PENDING), running detects the wedge
    # and re-queues the node.
    orchestrator.process_each_queue_once()

    # The wedged attempt is terminal SYSTEM_ERROR (excluded from cache reuse).
    container_executions = _all_container_executions(session_factory)
    assert len(container_executions) == 1
    assert container_executions[0].status == bts.ContainerExecutionStatus.SYSTEM_ERROR
    assert container_executions[0].ended_at is not None

    # The pod was terminated.
    assert launcher.refreshed_containers[-1].terminate_calls == 1

    # The node is re-queued with retry count 1 and the reason recorded.
    node = _the_execution_node(session_factory)
    assert node.container_execution_status == bts.ContainerExecutionStatus.QUEUED
    assert node.extra_data[orchestrator_sql._TRANSIENT_INFRA_RETRY_COUNT_KEY] == 1
    assert (
        launcher.reason
        in node.extra_data[
            bts.EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
        ]
    )


def test_transient_infra_failure_relaunches_until_cap():
    session_factory = _initialize_db_and_get_session_factory()
    launcher = _FakeLauncher()
    launcher.reason = "gke-gcsfuse-sidecar bucket-access-check timeout"
    orchestrator = _make_orchestrator(session_factory, launcher)

    _create_run(session_factory)

    # Each sweep relaunches a fresh pod that wedges again. After
    # _MAX_TRANSIENT_INFRA_RETRIES re-queues, the next wedge fails the node.
    max_retries = orchestrator_sql._MAX_TRANSIENT_INFRA_RETRIES
    for _ in range(max_retries + 1):
        orchestrator.process_each_queue_once()

    # A fresh pod was launched for the initial attempt plus each retry.
    assert launcher.launch_count == max_retries + 1

    # Every wedged attempt is its own terminal SYSTEM_ERROR ContainerExecution.
    container_executions = _all_container_executions(session_factory)
    assert len(container_executions) == max_retries + 1
    assert all(
        ce.status == bts.ContainerExecutionStatus.SYSTEM_ERROR
        for ce in container_executions
    )

    # The node itself is now failed (no further re-queue).
    node = _the_execution_node(session_factory)
    assert node.container_execution_status == bts.ContainerExecutionStatus.SYSTEM_ERROR
    assert (
        node.extra_data[orchestrator_sql._TRANSIENT_INFRA_RETRY_COUNT_KEY]
        == max_retries
    )
