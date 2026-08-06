"""Tests for the refresh-failure budget in the running-executions queue.

The behaviour is gated behind ``_retry_container_refresh_failures`` (env var
``TANGLE_RETRY_CONTAINER_REFRESH_FAILURES``); these tests enable it explicitly.
"""

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


def _retriable_error() -> launcher_interfaces.LauncherError:
    return launcher_interfaces.LauncherError(
        "The platform was unavailable", is_retriable=True
    )


def _non_retriable_error() -> launcher_interfaces.LauncherError:
    return launcher_interfaces.LauncherError("Something went definitively wrong")


def _missing_workload_error() -> launcher_interfaces.LaunchedContainerNotFoundError:
    return launcher_interfaces.LaunchedContainerNotFoundError("The workload is gone")


def _create_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _make_launched_container(launcher_data: dict[str, Any]) -> mock.MagicMock:
    return mock.MagicMock(
        status=launcher_interfaces.ContainerStatus.PENDING,
        to_dict=lambda: dict(launcher_data),
    )


def _container_task(image: str = "python") -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                implementation=structures.ContainerImplementation(
                    container=structures.ContainerSpec(image=image)
                )
            )
        )
    )


def _create_launched_container_executions(
    task_count: int = 1,
) -> Callable[[], orm.Session]:
    """A pipeline run with ``task_count`` container tasks, launched and PENDING.

    Each launch gets distinct ``launcher_data`` so tests can tell which
    execution the orchestrator refreshed. The tasks use distinct images so that
    each gets its own ``ContainerExecution`` instead of a cache hit on the
    first one.
    """
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
    session_factory = _create_session_factory()
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=structures.TaskSpec(
            component_ref=structures.ComponentReference(spec=pipeline_spec)
        ),
        created_by="user1",
    )

    launch_count = iter(range(task_count))
    launch_orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=mock.MagicMock(
            launch_container_task=mock.MagicMock(
                side_effect=lambda *a, **kw: _make_launched_container(
                    {"pod": f"pod-{next(launch_count)}"}
                )
            )
        ),
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )
    session = session_factory()
    for _ in range(20 * task_count):
        if not launch_orchestrator.internal_process_queued_executions_queue(
            session=session
        ):
            break
    return session_factory


def _make_orchestrator(
    session_factory: Callable[[], orm.Session],
    get_refreshed: Callable[..., Any],
    max_failures: int = 3,
    retry_refresh_failures: bool = True,
) -> orchestrator_sql.OrchestratorService_Sql:
    launcher = mock.MagicMock(
        deserialize_launched_container_from_dict=mock.MagicMock(
            side_effect=_make_launched_container
        ),
        get_refreshed_launched_container_from_dict=mock.MagicMock(
            side_effect=get_refreshed
        ),
    )
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
        _retry_container_refresh_failures=retry_refresh_failures,
        _max_container_execution_refresh_error_retries=max_failures,
    )


def _statuses(
    session_factory: Callable[[], orm.Session],
) -> list[bts.ContainerExecutionStatus]:
    return list(
        session_factory().scalars(sql.select(bts.ContainerExecution.status)).all()
    )


def _only_status(
    session_factory: Callable[[], orm.Session],
) -> bts.ContainerExecutionStatus:
    statuses = _statuses(session_factory)
    assert len(statuses) == 1
    return statuses[0]


class TestContainerExecutionRefreshRetries:
    def test_non_retriable_error_terminalizes_immediately(self) -> None:
        session_factory = _create_launched_container_executions()
        orchestrator = _make_orchestrator(
            session_factory,
            mock.MagicMock(side_effect=_non_retriable_error()),
            max_failures=3,
        )

        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

        assert _only_status(session_factory) == (
            bts.ContainerExecutionStatus.SYSTEM_ERROR
        )

    def test_disabled_flag_terminalizes_retriable_error_immediately(self) -> None:
        session_factory = _create_launched_container_executions()
        orchestrator = _make_orchestrator(
            session_factory,
            mock.MagicMock(side_effect=_retriable_error()),
            max_failures=3,
            retry_refresh_failures=False,
        )

        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

        assert _only_status(session_factory) == (
            bts.ContainerExecutionStatus.SYSTEM_ERROR
        )

    def test_missing_workload_is_counted_and_terminalizes(self) -> None:
        session_factory = _create_launched_container_executions()
        orchestrator = _make_orchestrator(
            session_factory,
            mock.MagicMock(side_effect=_missing_workload_error()),
            max_failures=3,
        )

        with mock.patch.object(
            orchestrator_sql.app_metrics, "execution_missing_workloads"
        ) as missing_workloads:
            orchestrator.internal_process_running_executions_queue(
                session=session_factory()
            )

        missing_workloads.add.assert_called_once()
        args, kwargs = missing_workloads.add.call_args
        assert args == (1,)
        assert isinstance(kwargs["attributes"]["status"], str)
        assert kwargs["attributes"]["status"]
        assert _only_status(session_factory) == (
            bts.ContainerExecutionStatus.SYSTEM_ERROR
        )

    def test_retriable_errors_below_budget_leave_execution_running(self) -> None:
        session_factory = _create_launched_container_executions()
        orchestrator = _make_orchestrator(
            session_factory,
            mock.MagicMock(side_effect=_retriable_error()),
            max_failures=3,
        )
        session = session_factory()

        for _ in range(2):
            orchestrator.internal_process_running_executions_queue(session=session)
            assert _only_status(session_factory) == (
                bts.ContainerExecutionStatus.PENDING
            )

    def test_retriable_errors_at_budget_terminalize_execution(self) -> None:
        session_factory = _create_launched_container_executions()
        orchestrator = _make_orchestrator(
            session_factory,
            mock.MagicMock(side_effect=_retriable_error()),
            max_failures=3,
        )
        session = session_factory()

        for _ in range(3):
            orchestrator.internal_process_running_executions_queue(session=session)

        assert _only_status(session_factory) == (
            bts.ContainerExecutionStatus.SYSTEM_ERROR
        )

    def test_successful_refresh_resets_the_budget(self) -> None:
        session_factory = _create_launched_container_executions()
        outcomes = [
            _retriable_error(),
            _retriable_error(),
            None,  # refreshed successfully
            _retriable_error(),
            _retriable_error(),
        ]

        def get_refreshed(launcher_data: dict[str, Any]) -> Any:
            outcome = outcomes.pop(0)
            if outcome is not None:
                raise outcome
            return _make_launched_container(launcher_data)

        orchestrator = _make_orchestrator(
            session_factory, get_refreshed, max_failures=3
        )
        session = session_factory()

        for _ in range(5):
            orchestrator.internal_process_running_executions_queue(session=session)

        # Five sweeps, but never three consecutive failures.
        assert _only_status(session_factory) == bts.ContainerExecutionStatus.PENDING

    def test_retry_moves_execution_to_the_back_of_the_queue(self) -> None:
        session_factory = _create_launched_container_executions()
        orchestrator = _make_orchestrator(
            session_factory,
            mock.MagicMock(side_effect=_retriable_error()),
            max_failures=3,
        )
        before = session_factory().scalar(
            sql.select(bts.ContainerExecution.last_processed_at)
        )

        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

        after = session_factory().scalar(
            sql.select(bts.ContainerExecution.last_processed_at)
        )
        assert before is not None and after is not None
        assert after > before

    def test_failing_execution_does_not_hold_up_the_queue(self) -> None:
        """A retriable-failing execution must not be refreshed ahead of others."""
        session_factory = _create_launched_container_executions(task_count=2)
        assert len(_statuses(session_factory)) == 2
        refreshed: list[dict[str, Any]] = []

        def get_refreshed(launcher_data: dict[str, Any]) -> Any:
            refreshed.append(dict(launcher_data))
            if len(refreshed) == 1:
                raise _retriable_error()
            return _make_launched_container(launcher_data)

        orchestrator = _make_orchestrator(
            session_factory, get_refreshed, max_failures=3
        )
        session = session_factory()

        orchestrator.internal_process_running_executions_queue(session=session)
        orchestrator.internal_process_running_executions_queue(session=session)

        assert len(refreshed) == 2
        # The second sweep moved on to the other execution instead of retrying
        # the failed one, so the orchestrator keeps making progress.
        assert refreshed[0] != refreshed[1]
        assert _statuses(session_factory) == [
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.PENDING,
        ]
