"""Tests for terminal-state branches in the orchestrator.

Each test targets a specific branch in orchestrator_sql.py that calls
_set_terminal_state and verifies that status, ended_at, exit_code, and
started_at are persisted correctly on the ContainerExecution.

Branches tested:
- SYSTEM_ERROR: internal_process_running_executions_queue exception handler
- SUCCEEDED:    internal_process_one_running_execution → SUCCEEDED branch
- FAILED:       internal_process_one_running_execution → FAILED branch
- CANCELLED:    internal_process_one_running_execution → cancellation branch
"""

from __future__ import annotations

import datetime
from unittest import mock

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


def _utc(*, hour: int = 12) -> datetime.datetime:
    return datetime.datetime(2026, 3, 20, hour, 0, tzinfo=datetime.timezone.utc)


def _make_execution_node(
    *,
    desired_state: str | None = None,
) -> mock.MagicMock:
    node = mock.MagicMock(spec=bts.ExecutionNode)
    node.id = "exec-node-1"
    node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
    node.extra_data = {"desired_state": desired_state} if desired_state else {}
    return node


def _make_container_execution(
    *,
    status: bts.ContainerExecutionStatus = bts.ContainerExecutionStatus.RUNNING,
    launcher_data: dict | None = None,
    execution_nodes: list | None = None,
) -> mock.MagicMock:
    ce = mock.MagicMock(spec=bts.ContainerExecution)
    ce.id = "ce-1"
    ce.status = status
    ce.started_at = None
    ce.ended_at = None
    ce.exit_code = None
    ce.extra_data = None
    ce.launcher_data = launcher_data or {"kubernetes_job": {}}
    ce.last_processed_at = None
    ce.updated_at = None
    ce.output_artifact_data_map = {}
    ce.execution_nodes = execution_nodes or [_make_execution_node()]
    return ce


def _make_launched_container(
    *,
    status: launcher_interfaces.ContainerStatus = launcher_interfaces.ContainerStatus.RUNNING,
    exit_code: int | None = None,
    started_at: datetime.datetime | None = None,
    ended_at: datetime.datetime | None = None,
    launcher_error_message: str | None = None,
) -> mock.MagicMock:
    lc = mock.MagicMock(spec=launcher_interfaces.LaunchedContainer)
    lc.status = status
    lc.exit_code = exit_code
    lc.started_at = started_at
    lc.ended_at = ended_at
    lc.launcher_error_message = launcher_error_message
    lc.to_dict.return_value = {"kubernetes_job": {"refreshed": True}}
    return lc


def _make_mock_session() -> mock.MagicMock:
    session = mock.MagicMock()
    session.scalar.return_value = None
    session.scalars.return_value.all.return_value = []
    session.begin.return_value.__enter__ = mock.Mock(return_value=None)
    session.begin.return_value.__exit__ = mock.Mock(return_value=False)
    return session


def _make_orchestrator(
    *,
    launcher: mock.MagicMock | None = None,
) -> orchestrator_sql.OrchestratorService_Sql:
    if launcher is None:
        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
    storage = mock.MagicMock()
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=mock.MagicMock(),
        launcher=launcher,
        storage_provider=storage,
        data_root_uri="gs://bucket/data",
        logs_root_uri="gs://bucket/logs",
    )


class TestSystemErrorBranch:
    """internal_process_running_executions_queue exception handler.

    When internal_process_one_running_execution raises, the outer handler
    sets SYSTEM_ERROR + ended_at on the ContainerExecution.
    """

    def test_sets_system_error_status_and_ended_at(self) -> None:
        ce = _make_container_execution()
        session = _make_mock_session()
        session.scalar.return_value = ce
        session.scalars.return_value.all.return_value = ["node-1"]

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        orch = _make_orchestrator(launcher=launcher)

        frozen_time = _utc(hour=15)
        with (
            mock.patch.object(
                orch,
                "internal_process_one_running_execution",
                side_effect=RuntimeError("boom"),
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._get_current_time",
                return_value=frozen_time,
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql.record_system_error_exception",
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._mark_all_downstream_executions_as_skipped",
            ),
        ):
            orch.internal_process_running_executions_queue(session=session)

        assert ce.status == bts.ContainerExecutionStatus.SYSTEM_ERROR
        assert ce.ended_at == frozen_time


class TestSucceededBranch:
    """internal_process_one_running_execution → SUCCEEDED branch.

    When the refreshed container reports SUCCEEDED, the orchestrator
    persists status, exit_code, started_at, ended_at.
    """

    def test_sets_succeeded_fields(self) -> None:
        start = _utc(hour=10)
        end = _utc(hour=14)
        execution_node = _make_execution_node()
        ce = _make_container_execution(execution_nodes=[execution_node])

        previous_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.RUNNING,
        )
        refreshed_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.SUCCEEDED,
            exit_code=0,
            started_at=start,
            ended_at=end,
        )

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        launcher.deserialize_launched_container_from_dict.return_value = previous_lc
        launcher.get_refreshed_launched_container_from_dict.return_value = refreshed_lc

        storage = mock.MagicMock()
        orch = _make_orchestrator(launcher=launcher)
        orch._storage_provider = storage
        session = _make_mock_session()

        with (
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._get_current_time",
                return_value=_utc(hour=14),
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._retry",
                side_effect=lambda fn, **kwargs: fn(),
            ),
        ):
            storage.make_uri.return_value.get_reader.return_value.exists.return_value = (
                True
            )
            info = mock.MagicMock()
            info.is_dir = False
            info.total_size = 10
            info.hashes = {"md5": "abc123"}
            storage.make_uri.return_value.get_reader.return_value.get_info.return_value = (
                info
            )
            storage.make_uri.return_value.get_reader.return_value.download_as_bytes.return_value = (
                b"val"
            )

            orch.internal_process_one_running_execution(
                session=session,
                container_execution=ce,
            )

        assert ce.status == bts.ContainerExecutionStatus.SUCCEEDED
        assert ce.exit_code == 0
        assert ce.started_at == start
        assert ce.ended_at == end


class TestFailedBranch:
    """internal_process_one_running_execution → FAILED branch.

    When the refreshed container reports FAILED, the orchestrator
    persists status, exit_code, started_at, ended_at.
    """

    def test_sets_failed_fields(self) -> None:
        start = _utc(hour=10)
        end = _utc(hour=13)
        execution_node = _make_execution_node()
        ce = _make_container_execution(execution_nodes=[execution_node])

        previous_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.RUNNING,
        )
        refreshed_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.FAILED,
            exit_code=1,
            started_at=start,
            ended_at=end,
            launcher_error_message="OOM killed",
        )

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        launcher.deserialize_launched_container_from_dict.return_value = previous_lc
        launcher.get_refreshed_launched_container_from_dict.return_value = refreshed_lc

        orch = _make_orchestrator(launcher=launcher)
        session = _make_mock_session()

        with (
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._get_current_time",
                return_value=_utc(hour=13),
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._retry",
                side_effect=lambda fn, **kwargs: fn(),
            ),
        ):
            orch.internal_process_one_running_execution(
                session=session,
                container_execution=ce,
            )

        assert ce.status == bts.ContainerExecutionStatus.FAILED
        assert ce.exit_code == 1
        assert ce.started_at == start
        assert ce.ended_at == end

    def test_records_launcher_error_message(self) -> None:
        execution_node = _make_execution_node()
        ce = _make_container_execution(execution_nodes=[execution_node])

        previous_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.RUNNING,
        )
        refreshed_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.FAILED,
            exit_code=137,
            started_at=_utc(hour=10),
            ended_at=_utc(hour=11),
            launcher_error_message="OOM killed",
        )

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        launcher.deserialize_launched_container_from_dict.return_value = previous_lc
        launcher.get_refreshed_launched_container_from_dict.return_value = refreshed_lc

        orch = _make_orchestrator(launcher=launcher)
        session = _make_mock_session()

        with (
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._get_current_time",
                return_value=_utc(hour=11),
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._retry",
                side_effect=lambda fn, **kwargs: fn(),
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._record_orchestration_error_message",
            ) as mock_record,
        ):
            orch.internal_process_one_running_execution(
                session=session,
                container_execution=ce,
            )

        mock_record.assert_called_once()
        call_kwargs = mock_record.call_args
        assert "OOM killed" in call_kwargs.kwargs.get(
            "message", ""
        ) or "OOM killed" in str(call_kwargs)


class TestCancelledBranch:
    """internal_process_one_running_execution → cancellation branch.

    When all execution nodes have desired_state=TERMINATED, the orchestrator
    terminates the container and sets CANCELLED + ended_at.
    """

    def test_sets_cancelled_fields(self) -> None:
        execution_node = _make_execution_node(desired_state="TERMINATED")
        ce = _make_container_execution(execution_nodes=[execution_node])

        previous_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.RUNNING,
        )

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        launcher.deserialize_launched_container_from_dict.return_value = previous_lc

        orch = _make_orchestrator(launcher=launcher)
        session = _make_mock_session()

        frozen_time = _utc(hour=16)
        with (
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._get_current_time",
                return_value=frozen_time,
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._retry",
                side_effect=lambda fn, **kwargs: fn(),
            ),
            mock.patch(
                "cloud_pipelines_backend.orchestrator_sql._mark_all_downstream_executions_as_skipped",
            ),
        ):
            orch.internal_process_one_running_execution(
                session=session,
                container_execution=ce,
            )

        assert ce.status == bts.ContainerExecutionStatus.CANCELLED
        assert ce.ended_at == frozen_time
        previous_lc.terminate.assert_called_once()
