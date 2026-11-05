"""Tests for whether the orchestrator starts its next sweep without waiting."""

import datetime
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


def _create_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


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


def _create_pipeline_run(
    session_factory: Callable[[], orm.Session], task_count: int = 1
) -> None:
    """A pipeline run with ``task_count`` container tasks that do not share a cached container execution."""
    pipeline_spec = structures.ComponentSpec(
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(
                tasks={
                    f"task{i}": _container_task(image=f"python:3.{i}")
                    for i in range(task_count)
                }
            )
        ),
    )
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=structures.TaskSpec(
            component_ref=structures.ComponentReference(spec=pipeline_spec)
        ),
        created_by="user1",
    )


def _make_orchestrator(
    session_factory: Callable[[], orm.Session],
    max_queue_batch_size: int = 1,
) -> orchestrator_sql.OrchestratorService_Sql:
    launcher = mock.MagicMock(
        launch_container_task=mock.MagicMock(
            side_effect=lambda *a, **kw: _make_launched_container({"pod": "pod-0"})
        ),
        deserialize_launched_container_from_dict=mock.MagicMock(
            side_effect=_make_launched_container
        ),
        get_refreshed_launched_container_from_dict=mock.MagicMock(
            side_effect=_make_launched_container
        ),
    )
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
        _max_queue_batch_size=max_queue_batch_size,
        _max_queue_batch_duration=datetime.timedelta(minutes=1),
    )


def _set_last_processed_at(
    session_factory: Callable[[], orm.Session], last_processed_at: datetime.datetime
) -> None:
    with session_factory() as session:
        session.execute(
            sql.update(bts.ContainerExecution).values(
                last_processed_at=last_processed_at
            )
        )
        session.commit()


def test_waits_when_there_is_no_work():
    session_factory = _create_session_factory()

    assert _make_orchestrator(session_factory).process_each_queue_once() is False


def test_sweeps_again_without_waiting_after_launching_a_queued_execution():
    session_factory = _create_session_factory()
    _create_pipeline_run(session_factory)

    assert _make_orchestrator(session_factory).process_each_queue_once() is True


def test_waits_while_every_running_container_execution_was_refreshed_recently():
    session_factory = _create_session_factory()
    _create_pipeline_run(session_factory)
    orchestrator = _make_orchestrator(session_factory)
    orchestrator.process_each_queue_once()

    assert orchestrator.process_each_queue_once() is False


def test_sweeps_again_without_waiting_while_a_running_container_execution_is_overdue():
    session_factory = _create_session_factory()
    _create_pipeline_run(session_factory, task_count=2)
    orchestrator = _make_orchestrator(session_factory)
    orchestrator.process_each_queue_once()
    orchestrator.process_each_queue_once()
    _set_last_processed_at(
        session_factory,
        datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(minutes=5),
    )

    assert orchestrator.process_each_queue_once() is True


def test_waits_after_a_queue_handler_raises():
    session_factory = _create_session_factory()
    _create_pipeline_run(session_factory)
    orchestrator = _make_orchestrator(session_factory)
    with mock.patch.object(
        orchestrator,
        "internal_process_queued_executions_queue",
        side_effect=RuntimeError("The database was unavailable"),
    ):
        assert orchestrator.process_each_queue_once() is False


def test_sweeps_again_without_waiting_when_a_batch_raises_after_finding_work():
    session_factory = _create_session_factory()
    orchestrator = _make_orchestrator(session_factory, max_queue_batch_size=3)
    with mock.patch.object(
        orchestrator,
        "internal_process_queued_executions_queue",
        side_effect=[True, True, RuntimeError("The database was unavailable")],
    ):
        assert orchestrator.process_each_queue_once() is True
