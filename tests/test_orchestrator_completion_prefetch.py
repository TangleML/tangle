"""Tests for the completion prefetch.

With the prefetch on, a finished container's log upload and output reads run on the prefetch workers, never on the loop
thread, and the container gets the same terminal outcome it gets with the prefetch off.
"""

import concurrent.futures
import datetime
import threading
from unittest import mock

import pytest
from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_interfaces,
)
from sqlalchemy import orm, sql

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces

_TS = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
_LAUNCHER_DATA = {"pod": "pod-0"}


@pytest.fixture(autouse=True)
def _no_retry_sleeps():
    """Patches out `_retry` sleeps, on the loop thread and the prefetch workers alike."""
    with mock.patch("time.sleep"):
        yield


class _FakeStorage:
    """A storage provider whose every output is a small file, recording which thread made each call."""

    def __init__(self, *, exists: bool = True, error: Exception | None = None):
        self.exists_result = exists
        self.error = error
        self.calls: list[tuple[str, int]] = []

    def make_uri(self, uri: str) -> mock.MagicMock:
        """An accessor whose reader answers for `uri`."""
        reader = mock.MagicMock()
        reader.exists.side_effect = lambda: self._call("exists", self.exists_result)
        reader.get_info.side_effect = lambda: self._call(
            "get_info",
            storage_interfaces.DataInfo(
                total_size=5, is_dir=False, hashes={"md5": "abc"}
            ),
        )
        reader.download_as_bytes.side_effect = lambda: self._call(
            "download_as_bytes", b"hello"
        )
        accessor = mock.MagicMock()
        accessor.get_reader.return_value = reader
        return accessor

    def _call(self, name: str, result):
        """Records the call and its thread, then returns `result` or raises the configured error."""
        self.calls.append((name, threading.get_ident()))
        if self.error is not None:
            raise self.error
        return result


def _container_component(
    *, inputs: list[str] | None = None, outputs: list[str] | None = None
) -> structures.ComponentSpec:
    """A container component with the given input and output names."""
    return structures.ComponentSpec(
        inputs=[structures.InputSpec(name=name) for name in (inputs or [])],
        outputs=[structures.OutputSpec(name=name) for name in (outputs or [])],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image="python")
        ),
    )


def _upstream_downstream_pipeline() -> structures.TaskSpec:
    """`upstream` (produces `out`) feeding `downstream` (consumes `in`)."""
    pipeline_spec = structures.ComponentSpec(
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(
                tasks={
                    "upstream": structures.TaskSpec(
                        component_ref=structures.ComponentReference(
                            spec=_container_component(outputs=["out"])
                        ),
                    ),
                    "downstream": structures.TaskSpec(
                        component_ref=structures.ComponentReference(
                            spec=_container_component(inputs=["in"])
                        ),
                        arguments={
                            "in": structures.TaskOutputArgument(
                                task_output=structures.TaskOutputReference(
                                    task_id="upstream", output_name="out"
                                )
                            )
                        },
                    ),
                }
            )
        ),
    )
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(spec=pipeline_spec)
    )


def _launched_container(
    status: launcher_interfaces.ContainerStatus,
) -> mock.MagicMock:
    """A launched container in `status` that records the thread of each log upload."""
    container = mock.MagicMock(
        status=status,
        exit_code=0,
        started_at=_TS,
        ended_at=_TS,
        launcher_error_message=None,
        to_dict=lambda: dict(_LAUNCHER_DATA),
    )
    container.upload_threads = []
    container.upload_log.side_effect = lambda: container.upload_threads.append(
        threading.get_ident()
    )
    return container


class _Scenario:
    """One pipeline whose upstream container finishes as `ended_container`, with the completion prefetch on or off."""

    def __init__(
        self,
        *,
        prefetch: bool,
        ended_container: mock.MagicMock,
        storage: _FakeStorage,
    ):
        db_engine = database_ops.create_db_engine_and_migrate_db(
            database_uri="sqlite://"
        )
        self.session_factory = lambda: orm.Session(bind=db_engine)
        api_server_sql.PipelineRunsApiService_Sql().create(
            session=self.session_factory(),
            root_task=_upstream_downstream_pipeline(),
            created_by="user1",
        )
        launcher = mock.MagicMock()
        launcher.launch_container_task.side_effect = lambda *a, **kw: (
            _launched_container(launcher_interfaces.ContainerStatus.PENDING)
        )
        launcher.deserialize_launched_container_from_dict.side_effect = (
            lambda data: _launched_container(
                launcher_interfaces.ContainerStatus.PENDING
            )
        )
        launcher.get_refreshed_launched_container_from_dict.side_effect = (
            lambda data: ended_container
        )
        self.launcher = launcher
        self.orchestrator = orchestrator_sql.OrchestratorService_Sql(
            session_factory=self.session_factory,
            launcher=launcher,
            storage_provider=storage,
            data_root_uri="file:///tmp/artifacts",
            logs_root_uri="file:///tmp/logs",
            _completion_prefetch_max_workers=1 if prefetch else 0,
        )
        session = self.session_factory()
        while self.orchestrator.internal_process_queued_executions_queue(
            session=session
        ):
            pass

    def visit_running(self) -> None:
        """Processes the running queue once."""
        self.orchestrator.internal_process_running_executions_queue(
            session=self.session_factory()
        )

    def wait_for_prefetches(self) -> None:
        """Blocks until every in-flight prefetch and its done callback have finished, then starts a fresh executor."""
        if self.orchestrator._completion_prefetch_executor is None:
            return
        self.orchestrator._completion_prefetch_executor.shutdown(wait=True)
        self.orchestrator._completion_prefetch_executor = (
            concurrent.futures.ThreadPoolExecutor(max_workers=1)
        )

    def run_to_completion(self) -> None:
        """Visits the ended container until it leaves PENDING, at most three times."""
        for _ in range(3):
            self.visit_running()
            if self.upstream_status() != bts.ContainerExecutionStatus.PENDING:
                return
            self.wait_for_prefetches()

    def node(self, task_id: str) -> bts.ExecutionNode:
        """The execution node of `task_id`."""
        return self.session_factory().scalar(
            sql.select(bts.ExecutionNode).where(
                bts.ExecutionNode.task_id_in_parent_execution == task_id
            )
        )

    def upstream_status(self) -> bts.ContainerExecutionStatus:
        """The upstream node's status."""
        return self.node("upstream").container_execution_status

    def upstream_last_processed_at(self) -> datetime.datetime:
        """When the upstream container execution was last processed."""
        session = self.session_factory()
        return session.get(
            bts.ContainerExecution, self.node("upstream").container_execution_id
        ).last_processed_at

    def set_upstream_last_processed_at(self, value: datetime.datetime) -> None:
        """Sets when the upstream container execution was last processed."""
        with self.session_factory() as session:
            session.get(
                bts.ContainerExecution,
                self.node("upstream").container_execution_id,
            ).last_processed_at = value
            session.commit()

    def outcome(self) -> dict:
        """Everything the finished container's processing decides."""
        session = self.session_factory()
        upstream = self.node("upstream")
        container_execution = session.get(
            bts.ContainerExecution, upstream.container_execution_id
        )
        artifacts = [
            (data.total_size, data.is_dir, data.hash, data.value)
            for data in session.scalars(sql.select(bts.ArtifactData)).all()
            if data.uri
            in [
                info["uri"]
                for info in container_execution.output_artifact_data_map.values()
            ]
        ]
        return {
            "upstream": upstream.container_execution_status,
            "downstream": self.node("downstream").container_execution_status,
            "container": container_execution.status,
            "error": (container_execution.extra_data or {}).get(
                bts.CONTAINER_EXECUTION_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY
            ),
            "artifacts": artifacts,
        }


def _run(*, prefetch: bool, status, storage) -> _Scenario:
    """Runs the scenario until the ended upstream container gets its terminal status."""
    scenario = _Scenario(
        prefetch=prefetch,
        ended_container=_launched_container(status),
        storage=storage,
    )
    scenario.run_to_completion()
    return scenario


class TestPrefetchedCompletion:
    def test_first_visit_starts_the_prefetch_and_keeps_the_status(self):
        storage = _FakeStorage()
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=storage,
        )
        with mock.patch.object(
            scenario.orchestrator, "_completion_prefetch_executor"
        ) as executor:
            scenario.visit_running()

        executor.submit.assert_called_once()
        assert scenario.upstream_status() == bts.ContainerExecutionStatus.PENDING
        assert storage.calls == []

    def test_terminal_status_lands_on_the_visit_after_the_prefetch(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )

        scenario.visit_running()
        assert scenario.upstream_status() == bts.ContainerExecutionStatus.PENDING
        scenario.wait_for_prefetches()
        scenario.visit_running()

        assert scenario.upstream_status() == bts.ContainerExecutionStatus.SUCCEEDED
        assert scenario.orchestrator._completion_prefetches == {}

    def test_finished_prefetch_moves_the_execution_to_the_front_of_the_queue_on_the_loop_thread(
        self,
    ):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )

        scenario.visit_running()
        scenario.wait_for_prefetches()
        assert scenario.upstream_last_processed_at().year != 1970
        scenario.orchestrator._move_finished_completion_prefetches_to_front(
            session=scenario.session_factory()
        )

        assert scenario.upstream_last_processed_at().year == 1970

    def test_output_that_was_missing_during_the_prefetch_is_read_again(self):
        storage = _FakeStorage(exists=False)
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=storage,
        )

        scenario.visit_running()
        scenario.wait_for_prefetches()
        storage.exists_result = True
        scenario.visit_running()

        assert scenario.upstream_status() == bts.ContainerExecutionStatus.SUCCEEDED

    def test_commits_the_ended_container_without_refreshing_it_again(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )
        scenario.visit_running()
        scenario.wait_for_prefetches()
        scenario.launcher.get_refreshed_launched_container_from_dict.side_effect = (
            RuntimeError("The pod is gone")
        )

        scenario.visit_running()

        assert scenario.upstream_status() == bts.ContainerExecutionStatus.SUCCEEDED
        assert scenario.orchestrator._ended_launched_containers == {}

    def test_system_error_forgets_the_prefetch(self):
        ended_container = _launched_container(
            launcher_interfaces.ContainerStatus.SUCCEEDED
        )
        scenario = _Scenario(
            prefetch=True, ended_container=ended_container, storage=_FakeStorage()
        )
        scenario.visit_running()
        scenario.wait_for_prefetches()

        def _fail_to_serialize():
            raise RuntimeError("The launcher data could not be serialized")

        ended_container.to_dict = _fail_to_serialize

        scenario.visit_running()

        assert scenario.upstream_status() == bts.ContainerExecutionStatus.SYSTEM_ERROR
        assert scenario.orchestrator._completion_prefetches == {}
        assert scenario.orchestrator._ended_launched_containers == {}

    def test_pending_prefetch_visit_moves_the_execution_back_in_the_queue(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )
        scenario.orchestrator._completion_prefetches[
            scenario.node("upstream").container_execution_id
        ] = mock.MagicMock(**{"done.return_value": False})
        scenario.set_upstream_last_processed_at(
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        )

        scenario.visit_running()

        assert scenario.upstream_last_processed_at().year != 1970

    def test_shutdown_stops_new_prefetches(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )

        scenario.orchestrator.shutdown_completion_prefetches(timeout_seconds=0)

        with pytest.raises(RuntimeError):
            scenario.orchestrator._completion_prefetch_executor.submit(lambda: None)

    def test_shutdown_reports_a_prefetch_still_running(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )
        release = threading.Event()
        scenario.orchestrator._completion_prefetches["hung"] = (
            scenario.orchestrator._completion_prefetch_executor.submit(release.wait)
        )

        try:
            assert not scenario.orchestrator.shutdown_completion_prefetches(
                timeout_seconds=0.05
            )
        finally:
            release.set()

    def test_shutdown_reports_finished_prefetches(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.SUCCEEDED
            ),
            storage=_FakeStorage(),
        )
        scenario.visit_running()
        assert scenario.orchestrator._completion_prefetches

        assert scenario.orchestrator.shutdown_completion_prefetches(timeout_seconds=5)

    def test_loop_thread_does_no_storage_or_log_io(self):
        storage = _FakeStorage()
        scenario = _run(
            prefetch=True,
            status=launcher_interfaces.ContainerStatus.SUCCEEDED,
            storage=storage,
        )
        ended_container = scenario.launcher.get_refreshed_launched_container_from_dict(
            None
        )
        loop_thread = threading.get_ident()

        assert sorted({name for name, _ in storage.calls}) == [
            "download_as_bytes",
            "exists",
            "get_info",
        ]
        assert all(thread != loop_thread for _, thread in storage.calls)
        assert ended_container.upload_threads
        assert loop_thread not in ended_container.upload_threads

    def test_refreshes_once_per_visit(self):
        scenario = _Scenario(
            prefetch=True,
            ended_container=_launched_container(
                launcher_interfaces.ContainerStatus.RUNNING
            ),
            storage=_FakeStorage(),
        )

        scenario.visit_running()

        assert (
            scenario.launcher.get_refreshed_launched_container_from_dict.call_count == 1
        )
        assert scenario.upstream_status() == bts.ContainerExecutionStatus.RUNNING


@pytest.mark.parametrize(
    ("status", "storage_kwargs"),
    [
        (launcher_interfaces.ContainerStatus.SUCCEEDED, {}),
        (launcher_interfaces.ContainerStatus.SUCCEEDED, {"exists": False}),
        (launcher_interfaces.ContainerStatus.FAILED, {}),
        (
            launcher_interfaces.ContainerStatus.SUCCEEDED,
            {"error": RuntimeError("storage is down")},
        ),
    ],
    ids=["succeeded", "missing-outputs", "failed", "storage-errors"],
)
def test_outcome_matches_the_orchestrator_without_the_prefetch(status, storage_kwargs):
    without_prefetch = _run(
        prefetch=False,
        status=status,
        storage=_FakeStorage(**storage_kwargs),
    )
    with_prefetch = _run(
        prefetch=True,
        status=status,
        storage=_FakeStorage(**storage_kwargs),
    )

    assert with_prefetch.outcome() == without_prefetch.outcome()
    assert with_prefetch.upstream_status() != bts.ContainerExecutionStatus.PENDING


def test_failed_log_upload_is_retried_inline():
    container = _launched_container(launcher_interfaces.ContainerStatus.FAILED)
    container.upload_log.side_effect = RuntimeError("pod log is gone")
    scenario = _Scenario(
        prefetch=True,
        ended_container=container,
        storage=_FakeStorage(),
    )

    scenario.run_to_completion()

    assert scenario.upstream_status() == bts.ContainerExecutionStatus.FAILED
    assert container.upload_log.call_count == 10


def test_cancelling_forgets_the_prefetch():
    scenario = _Scenario(
        prefetch=True,
        ended_container=_launched_container(
            launcher_interfaces.ContainerStatus.SUCCEEDED
        ),
        storage=_FakeStorage(),
    )
    scenario.visit_running()
    scenario.wait_for_prefetches()
    with scenario.session_factory() as session:
        upstream = scenario.node("upstream")
        session.get(bts.ExecutionNode, upstream.id).extra_data = {
            "desired_state": "TERMINATED"
        }
        session.commit()

    scenario.visit_running()

    assert scenario.upstream_status() == bts.ContainerExecutionStatus.CANCELLED
    assert scenario.orchestrator._completion_prefetches == {}


def test_finished_prefetch_does_not_move_an_ended_execution():
    scenario = _Scenario(
        prefetch=True,
        ended_container=_launched_container(
            launcher_interfaces.ContainerStatus.SUCCEEDED
        ),
        storage=_FakeStorage(),
    )
    scenario.visit_running()
    scenario.wait_for_prefetches()
    scenario.visit_running()
    assert scenario.upstream_status() == bts.ContainerExecutionStatus.SUCCEEDED
    scenario.orchestrator._finished_completion_prefetch_ids.add(
        scenario.node("upstream").container_execution_id
    )

    scenario.orchestrator._move_finished_completion_prefetches_to_front(
        session=scenario.session_factory()
    )

    assert scenario.upstream_last_processed_at().year != 1970


def test_without_the_prefetch_the_first_visit_commits_the_status():
    scenario = _Scenario(
        prefetch=False,
        ended_container=_launched_container(
            launcher_interfaces.ContainerStatus.SUCCEEDED
        ),
        storage=_FakeStorage(),
    )

    scenario.visit_running()

    assert scenario.upstream_status() == bts.ContainerExecutionStatus.SUCCEEDED
    assert scenario.orchestrator.shutdown_completion_prefetches(timeout_seconds=0)
