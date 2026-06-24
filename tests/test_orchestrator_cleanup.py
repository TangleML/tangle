from typing import Callable
from unittest import mock

from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import component_structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


def _initialize_db_and_get_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def test_orchestrator_cleans_up_terminal_launched_container():
    session_factory = _initialize_db_and_get_session_factory()
    container_task = component_structures.TaskSpec(
        component_ref=component_structures.ComponentReference(
            spec=component_structures.ComponentSpec(
                implementation=component_structures.ContainerImplementation(
                    container=component_structures.ContainerSpec(image="python:3.12")
                )
            )
        ),
        arguments={},
    )
    root_task = component_structures.TaskSpec(
        component_ref=component_structures.ComponentReference(
            spec=component_structures.ComponentSpec(
                implementation=component_structures.GraphImplementation(
                    graph=component_structures.GraphSpec(tasks={"task": container_task})
                )
            )
        ),
        arguments={},
    )
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=root_task,
        created_by="user",
    )

    launched_container = mock.MagicMock()
    launched_container.status = launcher_interfaces.ContainerStatus.PENDING
    launched_container.to_dict.return_value = {"launcher": "pending"}

    reloaded_container = mock.MagicMock()
    reloaded_container.status = launcher_interfaces.ContainerStatus.SUCCEEDED
    reloaded_container.to_dict.return_value = {"launcher": "succeeded"}
    reloaded_container.exit_code = 0
    reloaded_container.started_at = None
    reloaded_container.ended_at = None

    launcher = mock.MagicMock()
    launcher.launch_container_task.return_value = launched_container
    launcher.deserialize_launched_container_from_dict.return_value = launched_container
    launcher.get_refreshed_launched_container_from_dict.return_value = (
        reloaded_container
    )

    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )

    with session_factory() as session:
        assert orchestrator.internal_process_queued_executions_queue(session=session)
    with session_factory() as session:
        assert orchestrator.internal_process_running_executions_queue(session=session)

    reloaded_container.cleanup.assert_called_once_with()
    assert reloaded_container.method_calls.index(
        mock.call.cleanup()
    ) < reloaded_container.method_calls.index(mock.call.upload_log())
