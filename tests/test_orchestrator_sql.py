import datetime
from typing import Any
from unittest import mock

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


@pytest.fixture()
def session_factory():
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return orm.sessionmaker(engine)


class _MockLaunchedContainer(launcher_interfaces.LaunchedContainer):
    """A fake launched container that reports a fixed status and tracks calls."""

    def __init__(self, *, status: launcher_interfaces.ContainerStatus):
        self._status = status
        self.terminate_called = False
        self.upload_log_called = False

    @property
    def status(self) -> launcher_interfaces.ContainerStatus:
        return self._status

    @property
    def exit_code(self) -> int | None:
        return None

    @property
    def has_ended(self) -> bool:
        return False

    @property
    def has_succeeded(self) -> bool:
        return False

    @property
    def has_failed(self) -> bool:
        return False

    @property
    def started_at(self) -> datetime.datetime | None:
        return None

    @property
    def ended_at(self) -> datetime.datetime | None:
        return None

    @property
    def launcher_error_message(self) -> str | None:
        return None

    def to_dict(self) -> dict[str, Any]:
        return {"mock": True}

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(status=launcher_interfaces.ContainerStatus.PENDING)

    def get_refreshed(self):
        return self

    def get_log(self) -> str:
        return ""

    def upload_log(self) -> None:
        self.upload_log_called = True

    def stream_log_lines(self):
        yield ""

    def terminate(self) -> None:
        self.terminate_called = True


class _MockLauncher(launcher_interfaces.ContainerTaskLauncher[_MockLaunchedContainer]):
    """Launcher that returns a mock container with configurable status."""

    def __init__(self, *, status: launcher_interfaces.ContainerStatus):
        self._container = _MockLaunchedContainer(status=status)

    @property
    def container(self) -> _MockLaunchedContainer:
        return self._container

    def launch_container_task(self, **kwargs) -> _MockLaunchedContainer:
        return self._container

    def deserialize_launched_container_from_dict(
        self,
        launched_container_dict: dict,
    ) -> _MockLaunchedContainer:
        return self._container

    def get_refreshed_launched_container_from_dict(
        self,
        launched_container_dict: dict,
    ) -> _MockLaunchedContainer:
        return self._container


def _create_pending_container_execution(
    *,
    session: orm.Session,
    created_at: datetime.datetime,
) -> tuple[bts.ContainerExecution, bts.ExecutionNode]:
    """Creates a ContainerExecution in PENDING state with an associated ExecutionNode."""
    now = datetime.datetime.now(datetime.timezone.utc)

    container_execution = bts.ContainerExecution(
        status=bts.ContainerExecutionStatus.PENDING,
        created_at=created_at,
        last_processed_at=now,
        launcher_data={"mock": True},
    )
    session.add(container_execution)
    session.flush()

    execution_node = bts.ExecutionNode(
        task_spec={"component_ref": {"spec": {}}},
        container_execution_status=bts.ContainerExecutionStatus.PENDING,
        container_execution=container_execution,
    )
    session.add(execution_node)
    session.commit()
    return container_execution, execution_node


class TestOrchestratorServiceSql:

    class TestTerminateTimedOutPendingExecution:
        def test_terminates_expired_execution(self, session_factory):
            """PENDING container older than timeout is terminated and marked SYSTEM_ERROR."""
            mock_launcher = _MockLauncher(
                status=launcher_interfaces.ContainerStatus.PENDING
            )
            storage_provider = mock.MagicMock()

            orchestrator = orchestrator_sql.OrchestratorService_Sql(
                session_factory=session_factory,
                launcher=mock_launcher,
                storage_provider=storage_provider,
                data_root_uri="gs://test/data",
                logs_root_uri="gs://test/logs",
                pending_execution_timeout=datetime.timedelta(hours=12),
            )

            # Create execution that's been PENDING for 13 hours
            with session_factory() as session:
                created_at = datetime.datetime.now(
                    datetime.timezone.utc
                ) - datetime.timedelta(hours=13)
                container_exec, exec_node = _create_pending_container_execution(
                    session=session,
                    created_at=created_at,
                )
                container_exec_id = container_exec.id
                exec_node_id = exec_node.id

            # Process the running queue — should trigger timeout termination
            with session_factory() as session:
                orchestrator.internal_process_running_executions_queue(session=session)

            # Verify: container execution is now SYSTEM_ERROR (no longer PENDING in DB)
            with session_factory() as session:
                container_exec = session.get(bts.ContainerExecution, container_exec_id)
                assert (
                    container_exec.status == bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                assert container_exec.ended_at is not None
                assert "orchestration_error_message" in (
                    container_exec.extra_data or {}
                )

                exec_node = session.get(bts.ExecutionNode, exec_node_id)
                assert (
                    exec_node.container_execution_status
                    == bts.ContainerExecutionStatus.SYSTEM_ERROR
                )

            # Verify: pod was deleted
            assert mock_launcher.container.terminate_called is True

        def test_not_expired_stays_pending(self, session_factory):
            """PENDING container younger than timeout stays PENDING — no termination."""
            mock_launcher = _MockLauncher(
                status=launcher_interfaces.ContainerStatus.PENDING
            )
            storage_provider = mock.MagicMock()

            orchestrator = orchestrator_sql.OrchestratorService_Sql(
                session_factory=session_factory,
                launcher=mock_launcher,
                storage_provider=storage_provider,
                data_root_uri="gs://test/data",
                logs_root_uri="gs://test/logs",
                pending_execution_timeout=datetime.timedelta(hours=12),
            )

            # Create execution that's been PENDING for only 2 hours
            with session_factory() as session:
                created_at = datetime.datetime.now(
                    datetime.timezone.utc
                ) - datetime.timedelta(hours=2)
                container_exec, exec_node = _create_pending_container_execution(
                    session=session,
                    created_at=created_at,
                )
                container_exec_id = container_exec.id
                exec_node_id = exec_node.id

            # Process the running queue — should NOT trigger timeout
            with session_factory() as session:
                orchestrator.internal_process_running_executions_queue(session=session)

            # Verify: still PENDING
            with session_factory() as session:
                container_exec = session.get(bts.ContainerExecution, container_exec_id)
                assert container_exec.status == bts.ContainerExecutionStatus.PENDING
                assert container_exec.ended_at is None
                assert not container_exec.extra_data

                exec_node = session.get(bts.ExecutionNode, exec_node_id)
                assert (
                    exec_node.container_execution_status
                    == bts.ContainerExecutionStatus.PENDING
                )

            # Verify: pod NOT deleted
            assert mock_launcher.container.terminate_called is False

        def test_disabled_when_none(self, session_factory):
            """pending_execution_timeout=None means no timeout enforcement."""
            mock_launcher = _MockLauncher(
                status=launcher_interfaces.ContainerStatus.PENDING
            )
            storage_provider = mock.MagicMock()

            orchestrator = orchestrator_sql.OrchestratorService_Sql(
                session_factory=session_factory,
                launcher=mock_launcher,
                storage_provider=storage_provider,
                data_root_uri="gs://test/data",
                logs_root_uri="gs://test/logs",
                pending_execution_timeout=None,
            )

            # Create execution that's been PENDING for 48 hours
            with session_factory() as session:
                created_at = datetime.datetime.now(
                    datetime.timezone.utc
                ) - datetime.timedelta(hours=48)
                container_exec, exec_node = _create_pending_container_execution(
                    session=session,
                    created_at=created_at,
                )
                container_exec_id = container_exec.id
                exec_node_id = exec_node.id

            # Process the running queue — should NOT terminate despite being very old
            with session_factory() as session:
                orchestrator.internal_process_running_executions_queue(session=session)

            # Verify: still PENDING
            with session_factory() as session:
                container_exec = session.get(bts.ContainerExecution, container_exec_id)
                assert container_exec.status == bts.ContainerExecutionStatus.PENDING
                assert container_exec.ended_at is None
                assert not container_exec.extra_data

                exec_node = session.get(bts.ExecutionNode, exec_node_id)
                assert (
                    exec_node.container_execution_status
                    == bts.ContainerExecutionStatus.PENDING
                )

            assert mock_launcher.container.terminate_called is False

        def test_running_not_affected(self, session_factory):
            """RUNNING containers are not terminated by the pending timeout."""
            mock_launcher = _MockLauncher(
                status=launcher_interfaces.ContainerStatus.RUNNING
            )
            storage_provider = mock.MagicMock()

            orchestrator = orchestrator_sql.OrchestratorService_Sql(
                session_factory=session_factory,
                launcher=mock_launcher,
                storage_provider=storage_provider,
                data_root_uri="gs://test/data",
                logs_root_uri="gs://test/logs",
                pending_execution_timeout=datetime.timedelta(hours=12),
            )

            # Create a RUNNING execution that's been alive for 24 hours
            now = datetime.datetime.now(datetime.timezone.utc)
            with session_factory() as session:
                container_execution = bts.ContainerExecution(
                    status=bts.ContainerExecutionStatus.RUNNING,
                    created_at=now - datetime.timedelta(hours=24),
                    last_processed_at=now - datetime.timedelta(minutes=5),
                    launcher_data={"mock": True},
                )
                session.add(container_execution)
                session.flush()

                execution_node = bts.ExecutionNode(
                    task_spec={"component_ref": {"spec": {}}},
                    container_execution_status=bts.ContainerExecutionStatus.RUNNING,
                    container_execution=container_execution,
                )
                session.add(execution_node)
                session.commit()
                container_exec_id = container_execution.id
                exec_node_id = execution_node.id

            # Process — should not terminate a RUNNING container
            with session_factory() as session:
                orchestrator.internal_process_running_executions_queue(session=session)

            with session_factory() as session:
                container_exec = session.get(bts.ContainerExecution, container_exec_id)
                assert container_exec.status == bts.ContainerExecutionStatus.RUNNING

                exec_node = session.get(bts.ExecutionNode, exec_node_id)
                assert (
                    exec_node.container_execution_status
                    == bts.ContainerExecutionStatus.RUNNING
                )

            assert mock_launcher.container.terminate_called is False


class TestFormatDuration:
    def test_hours(self):
        result = orchestrator_sql._format_duration(
            duration=datetime.timedelta(hours=12, minutes=6)
        )
        assert result == "12.1 hours"

    def test_minutes(self):
        result = orchestrator_sql._format_duration(
            duration=datetime.timedelta(minutes=45)
        )
        assert result == "45.0 minutes"

    def test_seconds(self):
        result = orchestrator_sql._format_duration(
            duration=datetime.timedelta(seconds=30)
        )
        assert result == "30.0 seconds"

    def test_boundary_at_60_seconds(self):
        result = orchestrator_sql._format_duration(
            duration=datetime.timedelta(seconds=60)
        )
        assert result == "1.0 minutes"

    def test_boundary_at_3600_seconds(self):
        result = orchestrator_sql._format_duration(
            duration=datetime.timedelta(seconds=3600)
        )
        assert result == "1.0 hours"
