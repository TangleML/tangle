import datetime
from typing import Any

import pytest
import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_provider_interfaces,
)

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces


class FakeLaunchedContainer(launcher_interfaces.LaunchedContainer):
    def __init__(self, *, launcher: "FakeLauncher", container_id: str) -> None:
        self._launcher: "FakeLauncher" = launcher
        self._id: str = container_id
        self._status: launcher_interfaces.ContainerStatus = (
            launcher_interfaces.ContainerStatus.PENDING
        )
        self._exit_code: int | None = None
        self._started_at: datetime.datetime | None = None
        self._ended_at: datetime.datetime | None = None
        self._launcher_error: str | None = None
        self.terminate_calls: int = 0
        self.upload_log_calls: int = 0

    @property
    def status(self) -> launcher_interfaces.ContainerStatus:
        return self._status

    @property
    def exit_code(self) -> int | None:
        return self._exit_code

    @property
    def started_at(self) -> datetime.datetime | None:
        return self._started_at

    @property
    def ended_at(self) -> datetime.datetime | None:
        return self._ended_at

    @property
    def launcher_error_message(self) -> str | None:
        return self._launcher_error

    def to_dict(self) -> dict[str, Any]:
        return {
            "container_id": self._id,
            "status": self._status.value,
            "exit_code": self._exit_code,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "ended_at": self._ended_at.isoformat() if self._ended_at else None,
            "launcher_error": self._launcher_error,
        }

    def upload_log(self) -> None:
        live = self._launcher._containers.get(self._id, self)
        live.upload_log_calls += 1

    def terminate(self) -> None:
        live = self._launcher._containers.get(self._id, self)
        live.terminate_calls += 1

    def set_running(self) -> None:
        self._status = launcher_interfaces.ContainerStatus.RUNNING
        self._started_at = datetime.datetime.now(tz=datetime.timezone.utc)

    def set_succeeded(self) -> None:
        self._status = launcher_interfaces.ContainerStatus.SUCCEEDED
        self._exit_code = 0
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        self._started_at = self._started_at or now
        self._ended_at = now

    def set_failed(self, *, exit_code: int = 1, error: str | None = None) -> None:
        self._status = launcher_interfaces.ContainerStatus.FAILED
        self._exit_code = exit_code
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        self._started_at = self._started_at or now
        self._ended_at = now
        self._launcher_error = error


class FakeLauncher(launcher_interfaces.ContainerTaskLauncher):
    def __init__(self) -> None:
        self._containers: dict[str, FakeLaunchedContainer] = {}
        self.launch_calls: list[dict[str, Any]] = []
        self.launch_exception: Exception | None = None
        self._next_id: int = 0

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        input_arguments: dict[str, launcher_interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, str] | None = None,
    ) -> FakeLaunchedContainer:
        if self.launch_exception is not None:
            raise self.launch_exception
        self._next_id += 1
        cid = f"fake-container-{self._next_id}"
        container = FakeLaunchedContainer(launcher=self, container_id=cid)
        self._containers[cid] = container
        self.launch_calls.append(
            {
                "component_spec": component_spec,
                "input_arguments": dict(input_arguments),
                "output_uris": dict(output_uris),
                "log_uri": log_uri,
                "annotations": dict(annotations or {}),
                "container": container,
            }
        )
        return container

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict[str, Any]
    ) -> FakeLaunchedContainer:
        cid: str = launched_container_dict["container_id"]
        snapshot = FakeLaunchedContainer(launcher=self, container_id=cid)
        snapshot._status = launcher_interfaces.ContainerStatus(
            launched_container_dict["status"]
        )
        snapshot._exit_code = launched_container_dict.get("exit_code")
        started_at = launched_container_dict.get("started_at")
        if started_at:
            snapshot._started_at = datetime.datetime.fromisoformat(started_at)
        ended_at = launched_container_dict.get("ended_at")
        if ended_at:
            snapshot._ended_at = datetime.datetime.fromisoformat(ended_at)
        snapshot._launcher_error = launched_container_dict.get("launcher_error")
        return snapshot

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict[str, Any]
    ) -> FakeLaunchedContainer:
        return self._containers[launched_container_dict["container_id"]]


class _FakeUriReader:
    def __init__(self, provider: "FakeStorageProvider", uri: str) -> None:
        self._provider = provider
        self._uri = uri

    def exists(self) -> bool:
        return self._provider._existence.get(self._uri, self._provider.default_exists)

    def get_info(self) -> storage_provider_interfaces.DataInfo:
        return self._provider._info.get(
            self._uri,
            storage_provider_interfaces.DataInfo(
                total_size=0, is_dir=False, hashes={"md5": "0" * 32}
            ),
        )

    def download_as_bytes(self) -> bytes:
        return b""


class _FakeUriAccessor:
    def __init__(self, provider: "FakeStorageProvider", uri: str) -> None:
        self._provider = provider
        self._uri = uri

    def get_reader(self) -> _FakeUriReader:
        return _FakeUriReader(self._provider, self._uri)


class FakeStorageProvider:
    def __init__(self) -> None:
        self.default_exists: bool = True
        self._existence: dict[str, bool] = {}
        self._info: dict[str, storage_provider_interfaces.DataInfo] = {}

    def configure(
        self,
        uri: str,
        *,
        exists: bool = True,
        info: storage_provider_interfaces.DataInfo | None = None,
    ) -> None:
        self._existence[uri] = exists
        if info is not None:
            self._info[uri] = info

    def make_uri(self, uri: str) -> _FakeUriAccessor:
        return _FakeUriAccessor(self, uri)


@pytest.fixture()
def session_factory() -> orm.sessionmaker:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return orm.sessionmaker(engine)


@pytest.fixture()
def launcher() -> FakeLauncher:
    return FakeLauncher()


@pytest.fixture()
def storage_provider() -> FakeStorageProvider:
    return FakeStorageProvider()


@pytest.fixture()
def orchestrator(
    session_factory: orm.sessionmaker,
    launcher: FakeLauncher,
    storage_provider: FakeStorageProvider,
) -> orchestrator_sql.OrchestratorService_Sql:
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=storage_provider,
        data_root_uri="memory://data",
        logs_root_uri="memory://logs",
    )


def _make_container_component(
    name: str,
    *,
    inputs: tuple[str, ...] = (),
    outputs: tuple[str, ...] = ("out",),
    image: str = "test-image:latest",
) -> structures.ComponentSpec:
    return structures.ComponentSpec(
        name=name,
        inputs=[structures.InputSpec(name=n, type="String") for n in inputs],
        outputs=[structures.OutputSpec(name=n, type="String") for n in outputs],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image=image),
        ),
    )


def _make_container_task(
    component: structures.ComponentSpec,
    arguments: dict[str, Any] | None = None,
    *,
    execution_options: structures.ExecutionOptionsSpec | None = None,
) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(spec=component),
        arguments=arguments or None,
        execution_options=execution_options,
    )


def _wrap_in_single_task_graph(
    container_task: structures.TaskSpec, task_name: str
) -> structures.TaskSpec:
    graph_component = structures.ComponentSpec(
        name="single_task_graph",
        outputs=[structures.OutputSpec(name="out", type="String")],
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(
                tasks={task_name: container_task},
                output_values={
                    "out": structures.TaskOutputArgument(
                        task_output=structures.TaskOutputReference(
                            output_name="out", task_id=task_name
                        )
                    )
                },
            )
        ),
    )
    return _make_container_task(graph_component)


def _create_single_task_run(
    session_factory: orm.sessionmaker,
    *,
    created_by: str = "tester",
    task_name: str = "solo",
    image: str = "test-image:latest",
    execution_options: structures.ExecutionOptionsSpec | None = None,
) -> bts.IdType:
    service = api_server_sql.PipelineRunsApiService_Sql()
    container_component = _make_container_component(task_name, image=image)
    container_task = _make_container_task(
        container_component, execution_options=execution_options
    )
    root_task = _wrap_in_single_task_graph(container_task, task_name)
    with session_factory() as session:
        response = service.create(
            session=session, root_task=root_task, created_by=created_by
        )
        root_execution_id = session.scalar(
            sql.select(bts.PipelineRun.root_execution_id).where(
                bts.PipelineRun.id == response.id
            )
        )
        return session.scalar(
            sql.select(bts.ExecutionNode.id)
            .where(bts.ExecutionNode.parent_execution_id == root_execution_id)
            .where(bts.ExecutionNode.task_id_in_parent_execution == task_name)
        )


def _create_two_task_chain(
    session_factory: orm.sessionmaker, *, created_by: str = "tester"
) -> tuple[bts.IdType, bts.IdType]:
    service = api_server_sql.PipelineRunsApiService_Sql()
    upstream_task = _make_container_task(
        _make_container_component("upstream", outputs=("out",))
    )
    downstream_task = _make_container_task(
        _make_container_component("downstream", inputs=("in",), outputs=("out",)),
        arguments={
            "in": structures.TaskOutputArgument(
                task_output=structures.TaskOutputReference(
                    output_name="out", task_id="upstream"
                )
            )
        },
    )
    graph_component = structures.ComponentSpec(
        name="chain",
        outputs=[structures.OutputSpec(name="out", type="String")],
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(
                tasks={"upstream": upstream_task, "downstream": downstream_task},
                output_values={
                    "out": structures.TaskOutputArgument(
                        task_output=structures.TaskOutputReference(
                            output_name="out", task_id="downstream"
                        )
                    )
                },
            )
        ),
    )
    root_task = _make_container_task(graph_component)
    with session_factory() as session:
        service.create(
            session=session, root_task=root_task, created_by=created_by
        )
        rows = session.execute(
            sql.select(
                bts.ExecutionNode.id, bts.ExecutionNode.task_id_in_parent_execution
            ).where(
                bts.ExecutionNode.task_id_in_parent_execution.in_(
                    ("upstream", "downstream")
                )
            )
        ).all()
        ids_by_task: dict[str, bts.IdType] = {
            task_id: exec_id for exec_id, task_id in rows
        }
    return ids_by_task["upstream"], ids_by_task["downstream"]


def _status(
    session_factory: orm.sessionmaker, execution_id: bts.IdType
) -> bts.ContainerExecutionStatus | None:
    with session_factory() as session:
        return session.scalar(
            sql.select(bts.ExecutionNode.container_execution_status).where(
                bts.ExecutionNode.id == execution_id
            )
        )


def _drive_to_succeeded(
    orchestrator: orchestrator_sql.OrchestratorService_Sql,
    session_factory: orm.sessionmaker,
    launcher: FakeLauncher,
    execution_id: bts.IdType,
) -> None:
    with session_factory() as session:
        orchestrator.internal_process_queued_executions_queue(session)
    launcher.launch_calls[-1]["container"].set_succeeded()
    with session_factory() as session:
        orchestrator.internal_process_running_executions_queue(session)
    assert (
        _status(session_factory, execution_id) == bts.ContainerExecutionStatus.SUCCEEDED
    )


def _launch_one(
    orchestrator: orchestrator_sql.OrchestratorService_Sql,
    session_factory: orm.sessionmaker,
    launcher: FakeLauncher,
) -> FakeLaunchedContainer:
    with session_factory() as session:
        orchestrator.internal_process_queued_executions_queue(session)
    return launcher.launch_calls[-1]["container"]


class TestOrchestratorService_Sql:
    def test_queued_queue_empty_returns_false_and_sets_idle(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
    ) -> None:
        with session_factory() as session:
            handled = orchestrator.internal_process_queued_executions_queue(session)
        assert handled is False
        assert orchestrator._queued_executions_queue_idle is True

    def test_queued_execution_with_no_inputs_is_launched(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        execution_id = _create_single_task_run(session_factory)

        with session_factory() as session:
            handled = orchestrator.internal_process_queued_executions_queue(session)

        assert handled is True
        assert len(launcher.launch_calls) == 1

        with session_factory() as session:
            execution = session.get(bts.ExecutionNode, execution_id)
            assert (
                execution.container_execution_status
                == bts.ContainerExecutionStatus.PENDING
            )
            assert execution.container_execution_id is not None
            assert execution.container_execution_cache_key is not None
            container_execution = session.get(
                bts.ContainerExecution, execution.container_execution_id
            )
            assert container_execution.status == bts.ContainerExecutionStatus.PENDING
            assert container_execution.launcher_data is not None

    def test_missing_input_goes_to_waiting_for_upstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)

        with session_factory() as session:
            session.get(
                bts.ExecutionNode, downstream_id
            ).container_execution_status = bts.ContainerExecutionStatus.QUEUED
            session.get(
                bts.ExecutionNode, upstream_id
            ).container_execution_status = bts.ContainerExecutionStatus.SUCCEEDED
            session.commit()

        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert launcher.launch_calls == []
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
        )

    def test_uninitialized_status_is_also_picked_up(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        execution_id = _create_single_task_run(session_factory)
        with session_factory() as session:
            session.get(
                bts.ExecutionNode, execution_id
            ).container_execution_status = bts.ContainerExecutionStatus.UNINITIALIZED
            session.commit()

        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert len(launcher.launch_calls) == 1

    def test_execution_level_desired_state_terminated(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        with session_factory() as session:
            session.get(bts.ExecutionNode, upstream_id).extra_data = {
                "desired_state": "TERMINATED"
            }
            session.commit()

        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert launcher.launch_calls == []
        assert (
            _status(session_factory, upstream_id)
            == bts.ContainerExecutionStatus.CANCELLED
        )
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_pipeline_run_level_desired_state_terminated(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        with session_factory() as session:
            pipeline_run = session.scalar(
                sql.select(bts.PipelineRun)
                .join(
                    bts.ExecutionToAncestorExecutionLink,
                    bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                    == bts.PipelineRun.root_execution_id,
                )
                .where(
                    bts.ExecutionToAncestorExecutionLink.execution_id == upstream_id
                )
            )
            pipeline_run.extra_data = {"desired_state": "TERMINATED"}
            session.commit()

        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert launcher.launch_calls == []
        assert (
            _status(session_factory, upstream_id)
            == bts.ContainerExecutionStatus.CANCELLED
        )
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_succeeded_cache_hit_reuses_outputs_and_wakes_downstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        exec_a_id = _create_single_task_run(session_factory)
        _drive_to_succeeded(orchestrator, session_factory, launcher, exec_a_id)
        launches_before = len(launcher.launch_calls)

        exec_b_id = _create_single_task_run(session_factory)
        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert len(launcher.launch_calls) == launches_before
        with session_factory() as session:
            exec_a = session.get(bts.ExecutionNode, exec_a_id)
            exec_b = session.get(bts.ExecutionNode, exec_b_id)
            assert (
                exec_b.container_execution_status
                == bts.ContainerExecutionStatus.SUCCEEDED
            )
            assert exec_b.container_execution_id == exec_a.container_execution_id
            assert (exec_b.extra_data or {}).get(
                "reused_from_execution_node_id"
            ) == exec_a_id

            out_data_b = session.execute(
                sql.select(bts.ArtifactNode.artifact_data_id)
                .join(
                    bts.OutputArtifactLink,
                    bts.OutputArtifactLink.artifact_id == bts.ArtifactNode.id,
                )
                .where(bts.OutputArtifactLink.execution_id == exec_b_id)
            ).scalar_one()
            assert out_data_b is not None

    def test_running_cache_hit_links_container_without_copying_outputs(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        exec_a_id = _create_single_task_run(session_factory)
        _launch_one(orchestrator, session_factory, launcher)
        assert (
            _status(session_factory, exec_a_id)
            == bts.ContainerExecutionStatus.PENDING
        )
        launches_before = len(launcher.launch_calls)

        exec_b_id = _create_single_task_run(session_factory)
        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert len(launcher.launch_calls) == launches_before
        with session_factory() as session:
            exec_a = session.get(bts.ExecutionNode, exec_a_id)
            exec_b = session.get(bts.ExecutionNode, exec_b_id)
            assert exec_b.container_execution_id == exec_a.container_execution_id
            assert (
                exec_b.container_execution_status
                == bts.ContainerExecutionStatus.PENDING
            )
            out_data_b = session.execute(
                sql.select(bts.ArtifactNode.artifact_data_id)
                .join(
                    bts.OutputArtifactLink,
                    bts.OutputArtifactLink.artifact_id == bts.ArtifactNode.id,
                )
                .where(bts.OutputArtifactLink.execution_id == exec_b_id)
            ).scalar_one()
            assert out_data_b is None

    def test_max_cache_staleness_p0d_disables_cache_reuse(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        exec_a_id = _create_single_task_run(session_factory)
        _drive_to_succeeded(orchestrator, session_factory, launcher, exec_a_id)
        launches_before = len(launcher.launch_calls)

        exec_b_id = _create_single_task_run(
            session_factory,
            execution_options=structures.ExecutionOptionsSpec(
                caching_strategy=structures.CachingStrategySpec(
                    max_cache_staleness="P0D"
                )
            ),
        )
        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        assert len(launcher.launch_calls) == launches_before + 1
        assert (
            _status(session_factory, exec_b_id)
            == bts.ContainerExecutionStatus.PENDING
        )

    def test_launcher_exception_marks_system_error_and_skips_downstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        launcher.launch_exception = RuntimeError("boom")

        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)

        with session_factory() as session:
            upstream = session.get(bts.ExecutionNode, upstream_id)
            assert (
                upstream.container_execution_status
                == bts.ContainerExecutionStatus.SYSTEM_ERROR
            )
            extra = upstream.extra_data or {}
            message = extra.get(
                bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY
            )
            assert message and "boom" in message
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_running_queue_empty_returns_false_and_sets_idle(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
    ) -> None:
        with session_factory() as session:
            handled = orchestrator.internal_process_running_executions_queue(session)
        assert handled is False
        assert orchestrator._running_executions_queue_idle is True

    def test_same_status_just_bumps_last_processed_at(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        execution_id = _create_single_task_run(session_factory)
        _launch_one(orchestrator, session_factory, launcher)

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        with session_factory() as session:
            execution = session.get(bts.ExecutionNode, execution_id)
            container_execution = session.get(
                bts.ContainerExecution, execution.container_execution_id
            )
            assert container_execution.status == bts.ContainerExecutionStatus.PENDING
            assert (
                execution.container_execution_status
                == bts.ContainerExecutionStatus.PENDING
            )
            assert container_execution.last_processed_at is not None

    def test_pending_to_running_transition(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        execution_id = _create_single_task_run(session_factory)
        container = _launch_one(orchestrator, session_factory, launcher)
        container.set_running()

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        with session_factory() as session:
            execution = session.get(bts.ExecutionNode, execution_id)
            container_execution = session.get(
                bts.ContainerExecution, execution.container_execution_id
            )
            assert container_execution.status == bts.ContainerExecutionStatus.RUNNING
            assert (
                execution.container_execution_status
                == bts.ContainerExecutionStatus.RUNNING
            )
            assert container_execution.started_at is not None

    def test_success_collects_outputs_and_wakes_downstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
        storage_provider: FakeStorageProvider,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        container = _launch_one(orchestrator, session_factory, launcher)
        with session_factory() as session:
            container_execution = session.get(
                bts.ContainerExecution,
                session.get(bts.ExecutionNode, upstream_id).container_execution_id,
            )
            output_uri = container_execution.output_artifact_data_map["out"]["uri"]
        storage_provider.configure(
            output_uri,
            exists=True,
            info=storage_provider_interfaces.DataInfo(
                total_size=5, is_dir=False, hashes={"md5": "b" * 32}
            ),
        )
        container.set_succeeded()

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        with session_factory() as session:
            upstream = session.get(bts.ExecutionNode, upstream_id)
            downstream = session.get(bts.ExecutionNode, downstream_id)
            container_execution = session.get(
                bts.ContainerExecution, upstream.container_execution_id
            )
            assert (
                container_execution.status
                == bts.ContainerExecutionStatus.SUCCEEDED
            )
            assert (
                upstream.container_execution_status
                == bts.ContainerExecutionStatus.SUCCEEDED
            )
            assert (
                downstream.container_execution_status
                == bts.ContainerExecutionStatus.QUEUED
            )
            out_node = session.execute(
                sql.select(bts.ArtifactNode)
                .join(
                    bts.OutputArtifactLink,
                    bts.OutputArtifactLink.artifact_id == bts.ArtifactNode.id,
                )
                .where(bts.OutputArtifactLink.execution_id == upstream_id)
            ).scalar_one()
            assert out_node.artifact_data is not None
            assert out_node.artifact_data.total_size == 5

    def test_success_with_missing_outputs_marks_failed_and_skips_downstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
        storage_provider: FakeStorageProvider,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        container = _launch_one(orchestrator, session_factory, launcher)
        storage_provider.default_exists = False
        container.set_succeeded()

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        with session_factory() as session:
            upstream = session.get(bts.ExecutionNode, upstream_id)
            container_execution = session.get(
                bts.ContainerExecution, upstream.container_execution_id
            )
            assert container_execution.status == bts.ContainerExecutionStatus.FAILED
            assert (
                upstream.container_execution_status
                == bts.ContainerExecutionStatus.FAILED
            )
            error_message = (container_execution.extra_data or {}).get(
                bts.CONTAINER_EXECUTION_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
            )
            assert error_message and "missing outputs" in error_message
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_failure_marks_failed_and_skips_downstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        container = _launch_one(orchestrator, session_factory, launcher)
        container.set_failed(exit_code=42, error="container crashed")

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        with session_factory() as session:
            upstream = session.get(bts.ExecutionNode, upstream_id)
            container_execution = session.get(
                bts.ContainerExecution, upstream.container_execution_id
            )
            assert container_execution.status == bts.ContainerExecutionStatus.FAILED
            assert container_execution.exit_code == 42
            assert (
                upstream.container_execution_status
                == bts.ContainerExecutionStatus.FAILED
            )
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_terminate_when_all_nodes_vote_to_terminate(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        execution_id = _create_single_task_run(session_factory)
        container = _launch_one(orchestrator, session_factory, launcher)
        with session_factory() as session:
            session.get(bts.ExecutionNode, execution_id).extra_data = {
                "desired_state": "TERMINATED"
            }
            session.commit()

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        assert container.terminate_calls == 1
        with session_factory() as session:
            execution = session.get(bts.ExecutionNode, execution_id)
            container_execution = session.get(
                bts.ContainerExecution, execution.container_execution_id
            )
            assert (
                container_execution.status
                == bts.ContainerExecutionStatus.CANCELLED
            )
            assert container_execution.ended_at is not None
            assert (
                execution.container_execution_status
                == bts.ContainerExecutionStatus.CANCELLED
            )

    def test_refresh_exception_marks_system_error_and_skips_downstream(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        upstream_id, downstream_id = _create_two_task_chain(session_factory)
        _launch_one(orchestrator, session_factory, launcher)

        def _boom(
            launched_container_dict: dict[str, Any],
        ) -> FakeLaunchedContainer:
            raise RuntimeError(f"refresh boom: {launched_container_dict}")

        launcher.get_refreshed_launched_container_from_dict = _boom

        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        with session_factory() as session:
            upstream = session.get(bts.ExecutionNode, upstream_id)
            container_execution = session.get(
                bts.ContainerExecution, upstream.container_execution_id
            )
            assert (
                container_execution.status
                == bts.ContainerExecutionStatus.SYSTEM_ERROR
            )
            assert (
                upstream.container_execution_status
                == bts.ContainerExecutionStatus.SYSTEM_ERROR
            )
        assert (
            _status(session_factory, downstream_id)
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_partial_terminate_vote_does_not_terminate_container(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        exec_a_id = _create_single_task_run(session_factory)
        _launch_one(orchestrator, session_factory, launcher)
        exec_b_id = _create_single_task_run(session_factory)
        with session_factory() as session:
            orchestrator.internal_process_queued_executions_queue(session)
        with session_factory() as session:
            exec_a = session.get(bts.ExecutionNode, exec_a_id)
            exec_b = session.get(bts.ExecutionNode, exec_b_id)
            assert exec_b.container_execution_id == exec_a.container_execution_id
            exec_a.extra_data = {"desired_state": "TERMINATED"}
            session.commit()

        container = launcher.launch_calls[-1]["container"]
        with session_factory() as session:
            orchestrator.internal_process_running_executions_queue(session)

        assert container.terminate_calls == 0
        with session_factory() as session:
            exec_a = session.get(bts.ExecutionNode, exec_a_id)
            exec_b = session.get(bts.ExecutionNode, exec_b_id)
            container_execution = session.get(
                bts.ContainerExecution, exec_a.container_execution_id
            )
            assert (
                exec_a.container_execution_status
                == bts.ContainerExecutionStatus.CANCELLED
            )
            assert (
                exec_b.container_execution_status
                == bts.ContainerExecutionStatus.PENDING
            )
            assert (
                container_execution.status
                == bts.ContainerExecutionStatus.PENDING
            )

    def test_processes_both_queues_in_one_sweep(
        self,
        orchestrator: orchestrator_sql.OrchestratorService_Sql,
        session_factory: orm.sessionmaker,
        launcher: FakeLauncher,
    ) -> None:
        exec_a_id = _create_single_task_run(session_factory)
        _launch_one(orchestrator, session_factory, launcher)

        exec_b_id = _create_single_task_run(session_factory)

        orchestrator.process_each_queue_once()

        assert (
            _status(session_factory, exec_b_id) == bts.ContainerExecutionStatus.PENDING
        )
        with session_factory() as session:
            exec_a = session.get(bts.ExecutionNode, exec_a_id)
            container_execution = session.get(
                bts.ContainerExecution, exec_a.container_execution_id
            )
            assert container_execution.last_processed_at is not None
