"""Tests for ``orchestrator_sql``.
"""

from typing import Callable
from unittest import mock

import pytest
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


def _make_container_component(
    *,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
) -> structures.ComponentSpec:
    return structures.ComponentSpec(
        inputs=[structures.InputSpec(name=name) for name in (inputs or [])],
        outputs=[structures.OutputSpec(name=name) for name in (outputs or [])],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image="python")
        ),
    )


def _make_graph_task_spec(
    *,
    tasks: dict[str, structures.TaskSpec],
    graph_inputs: list[str] | None = None,
    arguments: dict[str, structures.ArgumentType] | None = None,
) -> structures.TaskSpec:
    """A root pipeline (graph component) task wrapping the given child ``tasks``."""
    pipeline_spec = structures.ComponentSpec(
        inputs=[structures.InputSpec(name=name) for name in (graph_inputs or [])],
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(tasks=tasks)
        ),
    )
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(spec=pipeline_spec),
        arguments=arguments,
    )


def _create_pipeline_run(
    session_factory: Callable[[], orm.Session],
    root_task: structures.TaskSpec,
    created_by: str = "user1",
) -> None:
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=root_task,
        created_by=created_by,
    )


def _get_execution_node(session: orm.Session, task_id: str) -> bts.ExecutionNode:
    node = session.scalar(
        sql.select(bts.ExecutionNode).where(
            bts.ExecutionNode.task_id_in_parent_execution == task_id
        )
    )
    assert node is not None, f"No execution node found for task_id={task_id!r}"
    return node


def _make_launched_container_mock() -> mock.MagicMock:
    launched_container_mock = mock.MagicMock(
        status=launcher_interfaces.ContainerStatus.PENDING,
        to_dict=lambda: {"foo": "bar"},
    )
    return mock.MagicMock(return_value=launched_container_mock)


def _process_queued_executions(
    session_factory: Callable[[], orm.Session],
    launched_container_mock: mock.MagicMock,
    max_number_of_executions: int = 20,
) -> None:
    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=mock.MagicMock(launch_container_task=launched_container_mock),
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )
    session = session_factory()
    # Process the queued queue until it is drained. A bound guards against the
    # (buggy) case where a failed execution never leaves the queue.
    for _ in range(max_number_of_executions):
        if not orchestrator.internal_process_queued_executions_queue(session=session):
            break


def _output_argument(task_id: str, output_name: str) -> structures.TaskOutputArgument:
    return structures.TaskOutputArgument(
        task_output=structures.TaskOutputReference(
            task_id=task_id, output_name=output_name
        )
    )


# --------------------------------------------------------------------------- #
# The queued-execution failure handler must skip the downstream subtree.
# --------------------------------------------------------------------------- #


class TestQueuedExecutionSystemErrorSkipsDownstream:
    """Test orphans with SYSTEM_ERROR and WAITING_FOR_UPSTREAM.
    
    Currently covers the queued-execution failure handler
    (``OrchestratorService_Sql.internal_process_queued_executions_queue``): when
    processing a queued execution raises, the execution is marked ``SYSTEM_ERROR``
    *and* its downstream subtree must be marked ``SKIPPED``. Otherwise the downstream
    nodes sit in ``WAITING_FOR_UPSTREAM`` forever -- no queue handler selects that
    status, so nothing ever wakes them.
    """

    def test_invalid_is_enabled_skips_downstream(self) -> None:
        """An ``OrchestratorError`` inside processing must not orphan downstream.

        ``is_enabled="maybe"`` raises out of ``internal_process_one_queued_execution``
        into the generic handler -- the same path a failed secret lookup takes.
        """
        root_task = _make_graph_task_spec(
            tasks={
                "upstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(outputs=["out"])
                    ),
                    # This line is what triggers the failure. `is_enabled` must
                    # resolve to "true" or "false"; "maybe" matches neither, so
                    # the conditional-execution check raises `OrchestratorError`
                    # before the container is launched, landing in the generic
                    # `except Exception` handler under test.
                    is_enabled="maybe",
                ),
                "downstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(inputs=["in"])
                    ),
                    arguments={"in": _output_argument("upstream", "out")},
                ),
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)
        launched_container_mock = _make_launched_container_mock()

        _process_queued_executions(session_factory, launched_container_mock)

        launched_container_mock.assert_not_called()
        session = session_factory()
        upstream = _get_execution_node(session, "upstream")
        downstream = _get_execution_node(session, "downstream")
        assert (
            upstream.container_execution_status
            == bts.ContainerExecutionStatus.SYSTEM_ERROR
        )
        assert (
            downstream.container_execution_status
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_missing_secret_skips_downstream(self) -> None:
        """The reported production trigger: a secret the user does not have"""
        secret_input_name = "auth_secret"
        root_task = _make_graph_task_spec(
            tasks={
                "upstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(
                            inputs=[secret_input_name], outputs=["out"]
                        )
                    ),
                    arguments={
                        secret_input_name: structures.DynamicDataArgument(
                            dynamic_data={"secret": {"name": "MISSING_SECRET"}}
                        )
                    },
                ),
                "downstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(inputs=["in"])
                    ),
                    arguments={"in": _output_argument("upstream", "out")},
                ),
            },
        )
        session_factory = _create_session_factory()
        # Deliberately do not create the secret.
        _create_pipeline_run(session_factory, root_task)
        launched_container_mock = _make_launched_container_mock()

        _process_queued_executions(session_factory, launched_container_mock)

        launched_container_mock.assert_not_called()
        session = session_factory()
        upstream = _get_execution_node(session, "upstream")
        downstream = _get_execution_node(session, "downstream")
        assert (
            upstream.container_execution_status
            == bts.ContainerExecutionStatus.SYSTEM_ERROR
        )
        assert (
            downstream.container_execution_status
            == bts.ContainerExecutionStatus.SKIPPED
        )

    def test_skips_transitive_downstream(self) -> None:
        """The skip must recurse: A -> B -> C, A fails, both B and C are skipped."""
        root_task = _make_graph_task_spec(
            tasks={
                "a": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(outputs=["out"])
                    ),
                    # Makes `a` raise -- see `test_invalid_is_enabled_skips_downstream`.
                    is_enabled="maybe",
                ),
                "b": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(inputs=["in"], outputs=["out"])
                    ),
                    arguments={"in": _output_argument("a", "out")},
                ),
                "c": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(inputs=["in"])
                    ),
                    arguments={"in": _output_argument("b", "out")},
                ),
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)
        launched_container_mock = _make_launched_container_mock()

        _process_queued_executions(session_factory, launched_container_mock)

        launched_container_mock.assert_not_called()
        session = session_factory()
        assert (
            _get_execution_node(session, "a").container_execution_status
            == bts.ContainerExecutionStatus.SYSTEM_ERROR
        )
        for task_id in ("b", "c"):
            assert (
                _get_execution_node(session, task_id).container_execution_status
                == bts.ContainerExecutionStatus.SKIPPED
            ), f"{task_id} was not skipped"

    def test_failing_downstream_skip_still_marks_system_error(self) -> None:
        """A failure while skipping must not leave the node re-queued.

        Same two-task fixture as ``test_invalid_is_enabled_skips_downstream``, but
        the skip itself is made to blow up. That pins the *commit ordering* rather
        than the skip: ``SYSTEM_ERROR`` is committed before
        ``_mark_all_downstream_executions_as_skipped`` is called, so it survives the
        skip raising. Were it committed afterwards, the rollback would revert
        ``upstream`` to ``QUEUED``, the queue would hand it back on the next sweep,
        and it would fail identically forever.

        Note what this deliberately accepts: ``downstream`` is left in
        ``WAITING_FOR_UPSTREAM`` -- the very orphan this class is about. A durable
        terminal upstream plus a repairable orphan beats an infinite retry loop, and
        the orphan is what the maintenance sweep API cleans up after the fact.

        The skip's exception propagates -- ``process_each_queue_once`` logs it and
        reports it to Bugsnag, and the batch loop continues. This test drives
        ``internal_process_queued_executions_queue`` directly, so it sees the raise.
        """
        root_task = _make_graph_task_spec(
            tasks={
                "upstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(outputs=["out"])
                    ),
                    # Makes `upstream` raise -- see
                    # `test_invalid_is_enabled_skips_downstream`.
                    is_enabled="maybe",
                ),
                "downstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(inputs=["in"])
                    ),
                    arguments={"in": _output_argument("upstream", "out")},
                ),
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)
        launched_container_mock = _make_launched_container_mock()

        # Break the skip. `side_effect` replaces the function outright, so nothing
        # downstream is marked -- this simulates the traversal dying partway (a lost
        # DB connection, say), which is the only reason the commit ordering matters.
        with mock.patch.object(
            orchestrator_sql,
            "_mark_all_downstream_executions_as_skipped",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                _process_queued_executions(session_factory, launched_container_mock)

        session = session_factory()
        upstream = _get_execution_node(session, "upstream")
        assert (
            upstream.container_execution_status
            == bts.ContainerExecutionStatus.SYSTEM_ERROR
        ), "the failed node must be terminal even when skipping downstream fails"
        # The accepted cost, asserted so it is a documented outcome rather than an
        # oversight: the skip never ran, so the orphan is still there.
        downstream = _get_execution_node(session, "downstream")
        assert (
            downstream.container_execution_status
            == bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
        )
