"""Tests for refreshing the container executions the launcher saw change ahead of the round-robin."""

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


def _notify(launcher: mock.MagicMock, container_execution_id: str) -> None:
    """Sends a change through the listener the newest orchestrator subscribed with."""
    listener = launcher.subscribe_to_container_execution_changes.call_args.kwargs[
        "listener"
    ]
    listener(
        launcher_interfaces.ContainerExecutionChange(
            container_execution_id=container_execution_id
        )
    )


def _refreshed_pods(launcher: mock.MagicMock) -> list[str]:
    return [
        call.args[0]["pod"]
        for call in launcher.get_refreshed_launched_container_from_dict.call_args_list
    ]


def _set_last_processed_at(
    session_factory: Callable[[], orm.Session],
    container_execution_id: str,
    last_processed_at: datetime.datetime,
) -> None:
    with session_factory() as session:
        session.get(
            bts.ContainerExecution, container_execution_id
        ).last_processed_at = last_processed_at
        session.commit()


def test_subscribes_to_the_launcher_once():
    launcher = mock.MagicMock()

    _make_orchestrator(lambda: None, launcher)

    launcher.subscribe_to_container_execution_changes.assert_called_once()


def test_refreshes_a_changed_container_execution_first():
    session_factory, launcher = _create_launched_container_executions(task_count=3)
    changed_id = _container_execution_ids_by_pod(session_factory)["pod-2"]
    orchestrator = _make_orchestrator(session_factory, launcher)
    launcher.get_refreshed_launched_container_from_dict.reset_mock()
    _notify(launcher, changed_id)

    orchestrator.internal_process_running_executions_queue(session=session_factory())

    assert _refreshed_pods(launcher) == ["pod-2"]


def test_leaves_an_ended_container_execution_out_of_the_running_queue():
    session_factory, launcher = _create_launched_container_executions(task_count=1)
    container_execution_id = _container_execution_ids_by_pod(session_factory)["pod-0"]
    with session_factory() as session:
        container_execution = session.get(
            bts.ContainerExecution, container_execution_id
        )
        container_execution.status = bts.ContainerExecutionStatus.SUCCEEDED
        last_processed_at = container_execution.last_processed_at
        session.commit()
    orchestrator = _make_orchestrator(session_factory, launcher)
    _notify(launcher, container_execution_id)

    orchestrator.internal_process_running_executions_queue(session=session_factory())

    assert (
        session_factory()
        .get(bts.ContainerExecution, container_execution_id)
        .last_processed_at
        == last_processed_at
    )
    launcher.get_refreshed_launched_container_from_dict.assert_not_called()


def test_an_execution_unrefreshed_longer_than_the_priority_goes_before_a_changed_one():
    session_factory, launcher = _create_launched_container_executions(task_count=2)
    ids_by_pod = _container_execution_ids_by_pod(session_factory)
    _set_last_processed_at(
        session_factory,
        ids_by_pod["pod-0"],
        orchestrator_sql._get_current_time() - datetime.timedelta(minutes=10),
    )
    orchestrator = _make_orchestrator(session_factory, launcher)
    _notify(launcher, ids_by_pod["pod-1"])

    orchestrator.internal_process_running_executions_queue(session=session_factory())

    assert _refreshed_pods(launcher) == ["pod-0"]


def test_executions_that_keep_changing_do_not_starve_one_that_changed_once():
    session_factory, launcher = _create_launched_container_executions(task_count=3)
    ids_by_pod = _container_execution_ids_by_pod(session_factory)
    orchestrator = _make_orchestrator(session_factory, launcher)
    _notify(launcher, ids_by_pod["pod-2"])

    for _ in range(3):
        _notify(launcher, ids_by_pod["pod-0"])
        _notify(launcher, ids_by_pod["pod-1"])
        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

    assert sorted(_refreshed_pods(launcher)) == ["pod-0", "pod-1", "pod-2"]


def test_a_change_seen_during_a_refresh_moves_the_execution_forward_again():
    session_factory, launcher = _create_launched_container_executions(task_count=2)
    ids_by_pod = _container_execution_ids_by_pod(session_factory)
    orchestrator = _make_orchestrator(session_factory, launcher)

    def _refresh_and_change_again(launcher_data):
        if launcher_data["pod"] == "pod-1":
            _notify(launcher, ids_by_pod["pod-1"])
        return _make_launched_container(launcher_data)

    launcher.get_refreshed_launched_container_from_dict.side_effect = (
        _refresh_and_change_again
    )
    _notify(launcher, ids_by_pod["pod-1"])

    for _ in range(2):
        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

    assert _refreshed_pods(launcher) == ["pod-1", "pod-1"]


def test_keeps_at_most_the_maximum_number_of_changed_ids(monkeypatch):
    monkeypatch.setattr(orchestrator_sql, "_MAX_CHANGED_CONTAINER_EXECUTION_IDS", 2)
    launcher = mock.MagicMock()
    orchestrator = _make_orchestrator(lambda: None, launcher)

    for container_execution_id in ("first", "second", "third"):
        _notify(launcher, container_execution_id)

    assert orchestrator._changed_container_execution_ids == {"first", "second"}


def test_launchers_report_no_changes_by_default():
    class _Launcher(launcher_interfaces.ContainerTaskLauncher):
        def launch_container_task(self, **kwargs):
            raise NotImplementedError()

    assert (
        _Launcher().subscribe_to_container_execution_changes(listener=mock.MagicMock())
        is None
    )
