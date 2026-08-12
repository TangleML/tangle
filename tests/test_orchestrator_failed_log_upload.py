"""A log-upload failure on the FAILED path must not escalate to SYSTEM_ERROR.

Production scenario: the cluster-autoscaler deletes a Job's pod mid-run. The Job
still reports Failed, so the orchestrator has everything it needs to record a
clean FAILED. But ``upload_log`` then reads logs for the remembered (now deleted)
pod, the API 404s, and -- if that call is unguarded -- the exception propagates
into the outer handler, which overwrites FAILED with SYSTEM_ERROR and orphans the
downstream subtree. Losing logs must never change a terminal status.
"""

import datetime
from typing import Callable
from unittest import mock

from sqlalchemy import orm
from sqlalchemy import sql

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces

_TS = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
_LAUNCHER_DATA = {"pod": "pod-0"}


def _create_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _container_component(
    *, inputs: list[str] | None = None, outputs: list[str] | None = None
) -> structures.ComponentSpec:
    return structures.ComponentSpec(
        inputs=[structures.InputSpec(name=name) for name in (inputs or [])],
        outputs=[structures.OutputSpec(name=name) for name in (outputs or [])],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image="python")
        ),
    )


def _upstream_downstream_pipeline() -> structures.TaskSpec:
    """``upstream`` (produces ``out``) feeding ``downstream`` (consumes ``in``)."""
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


def _get_execution_node(session: orm.Session, task_id: str) -> bts.ExecutionNode:
    node = session.scalar(
        sql.select(bts.ExecutionNode).where(
            bts.ExecutionNode.task_id_in_parent_execution == task_id
        )
    )
    assert node is not None, f"No execution node found for task_id={task_id!r}"
    return node


def _pending_launched_container() -> mock.MagicMock:
    return mock.MagicMock(
        status=launcher_interfaces.ContainerStatus.PENDING,
        to_dict=lambda: dict(_LAUNCHER_DATA),
    )


def _failed_launched_container_that_loses_logs() -> mock.MagicMock:
    """A Job that Failed cleanly but whose ``upload_log`` 404s on a deleted pod."""
    return mock.MagicMock(
        status=launcher_interfaces.ContainerStatus.FAILED,
        exit_code=1,
        started_at=_TS,
        ended_at=_TS,
        launcher_error_message=None,
        to_dict=lambda: dict(_LAUNCHER_DATA),
        upload_log=mock.MagicMock(
            side_effect=RuntimeError("read_namespaced_pod_log 404: pod deleted")
        ),
    )


def test_failed_execution_survives_log_upload_failure() -> None:
    session_factory = _create_session_factory()
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=_upstream_downstream_pipeline(),
        created_by="user1",
    )

    launcher = mock.MagicMock()
    launcher.launch_container_task.side_effect = (
        lambda *a, **kw: _pending_launched_container()
    )
    launcher.deserialize_launched_container_from_dict.side_effect = (
        lambda data: _pending_launched_container()
    )
    failed_container = _failed_launched_container_that_loses_logs()
    launcher.get_refreshed_launched_container_from_dict.side_effect = (
        lambda data: failed_container
    )

    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )

    # Launch the queued executions: `upstream` becomes PENDING, `downstream` stays
    # WAITING_FOR_UPSTREAM (it depends on upstream's output).
    session = session_factory()
    for _ in range(20):
        if not orchestrator.internal_process_queued_executions_queue(session=session):
            break

    # Refresh the running execution. `upstream` comes back FAILED, and its log
    # upload raises -- `_retry` burns its attempts and re-raises. `time.sleep` is
    # patched out so the retries do not slow the test down.
    with mock.patch.object(orchestrator_sql.time, "sleep"):
        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

    # The unguarded call would have re-raised into the outer handler and marked
    # upstream SYSTEM_ERROR; the guard keeps it FAILED with normal skipping.
    assert failed_container.upload_log.called
    check_session = session_factory()
    upstream = _get_execution_node(check_session, "upstream")
    downstream = _get_execution_node(check_session, "downstream")
    assert (
        upstream.container_execution_status == bts.ContainerExecutionStatus.FAILED
    ), "a lost log must not turn FAILED into SYSTEM_ERROR"
    assert (
        downstream.container_execution_status == bts.ContainerExecutionStatus.SKIPPED
    ), "downstream of a FAILED node must still be skipped"
