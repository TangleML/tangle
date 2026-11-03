"""Tests for refreshing the container executions the launcher saw change ahead of the round-robin."""

from typing import Any, Callable
from unittest import mock

from sqlalchemy import orm
from sqlalchemy import sql

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


def _make_launched_container(launcher_data: dict[str, Any]) -> mock.MagicMock:
    return mock.MagicMock(
        status=launcher_interfaces.ContainerStatus.PENDING,
        to_dict=lambda: dict(launcher_data),
    )


def _container_task(image: str) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                implementation=structures.ContainerImplementation(
                    container=structures.ContainerSpec(image=image)
                )
            )
        )
    )


def _make_orchestrator(
    session_factory: Callable[[], orm.Session], launcher: mock.MagicMock
) -> orchestrator_sql.OrchestratorService_Sql:
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )


def _create_launched_container_executions(
    task_count: int,
) -> tuple[Callable[[], orm.Session], mock.MagicMock]:
    """A pipeline run with ``task_count`` launched, PENDING container executions, and the launcher that launched them."""
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    session_factory = lambda: orm.Session(bind=db_engine)
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=structures.TaskSpec(
            component_ref=structures.ComponentReference(
                spec=structures.ComponentSpec(
                    implementation=structures.GraphImplementation(
                        graph=structures.GraphSpec(
                            tasks={
                                f"task{i}": _container_task(image=f"python:3.{i}")
                                for i in range(task_count)
                            }
                        )
                    ),
                )
            )
        ),
        created_by="user1",
    )
    launch_count = iter(range(task_count))
    launcher = mock.MagicMock(
        launch_container_task=mock.MagicMock(
            side_effect=lambda *a, **kw: _make_launched_container(
                {"pod": f"pod-{next(launch_count)}"}
            )
        ),
        deserialize_launched_container_from_dict=mock.MagicMock(
            side_effect=_make_launched_container
        ),
        get_refreshed_launched_container_from_dict=mock.MagicMock(
            side_effect=_make_launched_container
        ),
        pop_changed_container_execution_ids=mock.MagicMock(return_value=[]),
    )
    orchestrator = _make_orchestrator(session_factory, launcher)
    session = session_factory()
    while orchestrator.internal_process_queued_executions_queue(session=session):
        pass
    return session_factory, launcher


def _container_execution_ids_by_pod(
    session_factory: Callable[[], orm.Session],
) -> dict[str, str]:
    return {
        container_execution.launcher_data["pod"]: container_execution.id
        for container_execution in session_factory().scalars(
            sql.select(bts.ContainerExecution)
        )
    }


def test_refreshes_a_changed_container_execution_first():
    session_factory, launcher = _create_launched_container_executions(task_count=3)
    changed_id = _container_execution_ids_by_pod(session_factory)["pod-2"]
    launcher.pop_changed_container_execution_ids.return_value = [changed_id]

    _make_orchestrator(
        session_factory, launcher
    ).internal_process_running_executions_queue(session=session_factory())

    launcher.get_refreshed_launched_container_from_dict.assert_called_once_with(
        {"pod": "pod-2"}
    )


def test_leaves_an_ended_container_execution_out_of_the_running_queue():
    session_factory, launcher = _create_launched_container_executions(task_count=1)
    container_execution_id = _container_execution_ids_by_pod(session_factory)["pod-0"]
    with session_factory() as session:
        session.get(bts.ContainerExecution, container_execution_id).status = (
            bts.ContainerExecutionStatus.SUCCEEDED
        )
        session.commit()
    launcher.pop_changed_container_execution_ids.return_value = [container_execution_id]

    _make_orchestrator(
        session_factory, launcher
    ).internal_process_running_executions_queue(session=session_factory())

    last_processed_at = (
        session_factory()
        .get(bts.ContainerExecution, container_execution_id)
        .last_processed_at
    )
    assert last_processed_at.year != 1970
    launcher.get_refreshed_launched_container_from_dict.assert_not_called()


def test_launchers_report_no_changes_by_default():
    class _Launcher(launcher_interfaces.ContainerTaskLauncher):
        def launch_container_task(self, **kwargs):
            raise NotImplementedError()

    assert _Launcher().pop_changed_container_execution_ids() == []
