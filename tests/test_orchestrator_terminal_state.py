"""Tests for terminal-state branches in the orchestrator.

Each test targets a specific branch in orchestrator_sql.py that calls
_record_terminal_state and verifies that status, ended_at, exit_code, and
started_at are persisted correctly on the ContainerExecution.

Branches tested:
- SYSTEM_ERROR: internal_process_running_executions_queue exception handler
- SUCCEEDED:    internal_process_one_running_execution -> SUCCEEDED branch
- FAILED:       internal_process_one_running_execution -> FAILED branch
- CANCELLED:    internal_process_one_running_execution -> cancellation branch
"""

from __future__ import annotations

import datetime
import pathlib
from unittest import mock

import pytest
from sqlalchemy import orm

from cloud_pipelines.orchestration.storage_providers import local_storage

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces
from tests.test_api_server_sql import session_factory  # noqa: F401

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def storage_provider() -> local_storage.LocalStorageProvider:
    return local_storage.LocalStorageProvider()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc(*, hour: int = 12) -> datetime.datetime:
    return datetime.datetime(2026, 3, 20, hour, 0, tzinfo=datetime.timezone.utc)


def _create_container_execution(
    *,
    session: orm.Session,
    status: bts.ContainerExecutionStatus = bts.ContainerExecutionStatus.RUNNING,
    launcher_data: dict | None = None,
    output_artifact_data_map: dict | None = None,
    desired_state: str | None = None,
) -> bts.ContainerExecution:
    """Insert a real ContainerExecution + ExecutionNode into the DB."""
    ce = bts.ContainerExecution(
        status=status,
        launcher_data=launcher_data or {"kubernetes_job": {}},
        output_artifact_data_map=output_artifact_data_map or {},
    )
    node = bts.ExecutionNode(task_spec={"component_ref": {"name": "test-task"}})
    if desired_state:
        node.extra_data = {"desired_state": desired_state}
    node.container_execution = ce
    node.container_execution_status = status
    session.add(ce)
    session.add(node)
    session.flush()
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


def _make_orchestrator(
    *,
    launcher: mock.MagicMock | None = None,
    storage_provider: local_storage.LocalStorageProvider,
    tmp_path: pathlib.Path,
) -> orchestrator_sql.OrchestratorService_Sql:
    if launcher is None:
        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=mock.MagicMock(),
        launcher=launcher,
        storage_provider=storage_provider,
        data_root_uri=str(tmp_path / "data"),
        logs_root_uri=str(tmp_path / "logs"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSystemErrorBranch:
    """internal_process_running_executions_queue exception handler.

    When internal_process_one_running_execution raises, the outer handler
    records SYSTEM_ERROR + ended_at on the ContainerExecution.
    """

    def test_records_system_error_status_and_ended_at(
        self,
        session_factory: orm.sessionmaker,
        storage_provider: local_storage.LocalStorageProvider,
        tmp_path: pathlib.Path,
    ) -> None:
        with session_factory() as session:
            ce = _create_container_execution(session=session)
            ce_id = ce.id
            session.commit()

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        orch = _make_orchestrator(
            launcher=launcher,
            storage_provider=storage_provider,
            tmp_path=tmp_path,
        )

        frozen_time = _utc(hour=15)

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            with (
                mock.patch.object(
                    orch,
                    "internal_process_one_running_execution",
                    side_effect=RuntimeError("boom"),
                ),
                mock.patch.object(
                    orchestrator_sql,
                    "_get_current_time",
                    return_value=frozen_time,
                ),
                mock.patch.object(orchestrator_sql, "record_system_error_exception"),
                mock.patch.object(
                    orchestrator_sql,
                    "_mark_all_downstream_executions_as_skipped",
                ),
            ):
                session.scalar = mock.MagicMock(return_value=ce)
                session.scalars = mock.MagicMock()
                session.scalars.return_value.all.return_value = [
                    node.id for node in ce.execution_nodes
                ]
                orch.internal_process_running_executions_queue(session=session)

            session.expire_on_commit = False
            session.commit()

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            assert ce.status == bts.ContainerExecutionStatus.SYSTEM_ERROR
            assert ce.ended_at == frozen_time


class TestSucceededBranch:
    """internal_process_one_running_execution -> SUCCEEDED branch.

    When the refreshed container reports SUCCEEDED, the orchestrator
    persists status, exit_code, started_at, ended_at.
    """

    def test_records_succeeded_fields(
        self,
        session_factory: orm.sessionmaker,
        storage_provider: local_storage.LocalStorageProvider,
        tmp_path: pathlib.Path,
    ) -> None:
        start = _utc(hour=10)
        end = _utc(hour=14)

        output_file = tmp_path / "output"
        output_file.write_text("hello")
        output_uri = str(output_file)

        with session_factory() as session:
            ce = _create_container_execution(
                session=session,
                output_artifact_data_map={
                    "result": {"uri": output_uri},
                },
            )
            ce_id = ce.id
            session.commit()

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

        orch = _make_orchestrator(
            launcher=launcher,
            storage_provider=storage_provider,
            tmp_path=tmp_path,
        )

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            with (
                mock.patch.object(
                    orchestrator_sql,
                    "_get_current_time",
                    return_value=_utc(hour=14),
                ),
                mock.patch.object(
                    orchestrator_sql,
                    "_retry",
                    side_effect=lambda fn, **kwargs: fn(),
                ),
            ):
                orch.internal_process_one_running_execution(
                    session=session,
                    container_execution=ce,
                )
            session.commit()

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            assert ce.status == bts.ContainerExecutionStatus.SUCCEEDED
            assert ce.exit_code == 0
            assert ce.started_at == start
            assert ce.ended_at == end


class TestFailedBranch:
    """internal_process_one_running_execution -> FAILED branch.

    When the refreshed container reports FAILED, the orchestrator
    persists status, exit_code, started_at, ended_at.
    """

    def test_records_failed_fields(
        self,
        session_factory: orm.sessionmaker,
        storage_provider: local_storage.LocalStorageProvider,
        tmp_path: pathlib.Path,
    ) -> None:
        start = _utc(hour=10)
        end = _utc(hour=13)

        with session_factory() as session:
            ce = _create_container_execution(session=session)
            ce_id = ce.id
            session.commit()

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

        orch = _make_orchestrator(
            launcher=launcher,
            storage_provider=storage_provider,
            tmp_path=tmp_path,
        )

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            with (
                mock.patch.object(
                    orchestrator_sql,
                    "_get_current_time",
                    return_value=_utc(hour=13),
                ),
                mock.patch.object(
                    orchestrator_sql,
                    "_retry",
                    side_effect=lambda fn, **kwargs: fn(),
                ),
            ):
                orch.internal_process_one_running_execution(
                    session=session,
                    container_execution=ce,
                )
            session.commit()

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            assert ce.status == bts.ContainerExecutionStatus.FAILED
            assert ce.exit_code == 1
            assert ce.started_at == start
            assert ce.ended_at == end

    def test_records_launcher_error_message(
        self,
        session_factory: orm.sessionmaker,
        storage_provider: local_storage.LocalStorageProvider,
        tmp_path: pathlib.Path,
    ) -> None:
        with session_factory() as session:
            ce = _create_container_execution(session=session)
            ce_id = ce.id
            session.commit()

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

        orch = _make_orchestrator(
            launcher=launcher,
            storage_provider=storage_provider,
            tmp_path=tmp_path,
        )

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            with (
                mock.patch.object(
                    orchestrator_sql,
                    "_get_current_time",
                    return_value=_utc(hour=11),
                ),
                mock.patch.object(
                    orchestrator_sql,
                    "_retry",
                    side_effect=lambda fn, **kwargs: fn(),
                ),
            ):
                orch.internal_process_one_running_execution(
                    session=session,
                    container_execution=ce,
                )
            session.commit()

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            assert ce.extra_data is not None
            assert "OOM killed" in ce.extra_data.get(
                bts.CONTAINER_EXECUTION_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY, ""
            )


class TestCancelledBranch:
    """internal_process_one_running_execution -> cancellation branch.

    When all execution nodes have desired_state=TERMINATED, the orchestrator
    terminates the container and sets CANCELLED + ended_at.
    """

    def test_records_cancelled_fields(
        self,
        session_factory: orm.sessionmaker,
        storage_provider: local_storage.LocalStorageProvider,
        tmp_path: pathlib.Path,
    ) -> None:
        with session_factory() as session:
            ce = _create_container_execution(
                session=session,
                desired_state="TERMINATED",
            )
            ce_id = ce.id
            session.commit()

        previous_lc = _make_launched_container(
            status=launcher_interfaces.ContainerStatus.RUNNING,
        )

        launcher = mock.MagicMock(spec=launcher_interfaces.ContainerTaskLauncher)
        launcher.deserialize_launched_container_from_dict.return_value = previous_lc

        orch = _make_orchestrator(
            launcher=launcher,
            storage_provider=storage_provider,
            tmp_path=tmp_path,
        )

        frozen_time = _utc(hour=16)
        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            with (
                mock.patch.object(
                    orchestrator_sql,
                    "_get_current_time",
                    return_value=frozen_time,
                ),
                mock.patch.object(
                    orchestrator_sql,
                    "_retry",
                    side_effect=lambda fn, **kwargs: fn(),
                ),
                mock.patch.object(
                    orchestrator_sql,
                    "_mark_all_downstream_executions_as_skipped",
                ),
            ):
                orch.internal_process_one_running_execution(
                    session=session,
                    container_execution=ce,
                )
            session.commit()

        with session_factory() as session:
            ce = session.get(bts.ContainerExecution, ce_id)
            assert ce.status == bts.ContainerExecutionStatus.CANCELLED
            assert ce.ended_at == frozen_time
        previous_lc.terminate.assert_called_once()
