"""Tests for conditional execution (``TaskSpec.is_enabled``).

Covers the two halves of the feature:

* ``api_server_sql._recursively_create_all_executions_and_artifacts`` -- wiring
  the special ``is_enabled`` input artifact link when creating a pipeline run.
* ``orchestrator_sql.internal_process_one_queued_execution`` -- evaluating
  ``is_enabled`` and skipping the execution (and its downstream) when disabled.
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

_IS_ENABLED_INPUT_NAME = bts.EXECUTION_NODE_TASK_IS_ENABLED_SPECIAL_INPUT_NAME


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


def _input_links(node: bts.ExecutionNode) -> dict[str, bts.InputArtifactLink]:
    return {link.input_name: link for link in node.input_artifact_links}


# --------------------------------------------------------------------------- #
# api_server_sql: creation-time wiring of the special is_enabled input link
# --------------------------------------------------------------------------- #


class TestConditionalExecutionCreation:
    def test_constant_string_is_enabled_creates_no_special_link(self) -> None:
        root_task = _make_graph_task_spec(
            tasks={
                "child": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component()
                    ),
                    is_enabled="false",
                )
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)

        child = _get_execution_node(session_factory(), "child")
        assert _IS_ENABLED_INPUT_NAME not in _input_links(child)

    def test_graph_input_is_enabled_creates_special_link(self) -> None:
        root_task = _make_graph_task_spec(
            graph_inputs=["enable"],
            arguments={"enable": "false"},
            tasks={
                "child": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component()
                    ),
                    is_enabled=structures.GraphInputArgument(
                        graph_input=structures.GraphInputReference(input_name="enable")
                    ),
                )
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)

        child = _get_execution_node(session_factory(), "child")
        link = _input_links(child).get(_IS_ENABLED_INPUT_NAME)
        assert link is not None
        assert link.artifact.artifact_data is not None
        assert link.artifact.artifact_data.value == "false"

    def test_task_output_is_enabled_creates_special_link(self) -> None:
        root_task = _make_graph_task_spec(
            tasks={
                "producer": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(outputs=["flag"])
                    ),
                ),
                "child": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component()
                    ),
                    is_enabled=structures.TaskOutputArgument(
                        task_output=structures.TaskOutputReference(
                            task_id="producer", output_name="flag"
                        )
                    ),
                ),
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)

        session = session_factory()
        child = _get_execution_node(session, "child")
        producer = _get_execution_node(session, "producer")
        link = _input_links(child).get(_IS_ENABLED_INPUT_NAME)
        assert link is not None
        # The is_enabled artifact must be the producer's output artifact.
        producer_output = producer.output_artifact_links[0].artifact
        assert link.artifact.id == producer_output.id

    def test_is_enabled_on_graph_task_raises(self) -> None:
        # A graph (non-container) child component with is_enabled is rejected.
        inner_pipeline = structures.ComponentSpec(
            implementation=structures.GraphImplementation(
                graph=structures.GraphSpec(
                    tasks={
                        "inner": structures.TaskSpec(
                            component_ref=structures.ComponentReference(
                                spec=_make_container_component()
                            ),
                        )
                    }
                )
            ),
        )
        root_task = _make_graph_task_spec(
            tasks={
                "child": structures.TaskSpec(
                    component_ref=structures.ComponentReference(spec=inner_pipeline),
                    is_enabled="true",
                )
            },
        )
        session_factory = _create_session_factory()
        with pytest.raises(
            api_server_sql.ApiServiceError, match="only supported for container"
        ):
            _create_pipeline_run(session_factory, root_task)


# --------------------------------------------------------------------------- #
# orchestrator_sql: evaluating is_enabled and skipping executions
# --------------------------------------------------------------------------- #


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
    # (buggy) case where a skipped execution never leaves the queue.
    for _ in range(max_number_of_executions):
        if not orchestrator.internal_process_queued_executions_queue(session=session):
            break


def _run_single_conditional_task(
    is_enabled: structures.ArgumentType,
    *,
    graph_inputs: list[str] | None = None,
    arguments: dict[str, structures.ArgumentType] | None = None,
) -> tuple[Callable[[], orm.Session], mock.MagicMock]:
    root_task = _make_graph_task_spec(
        graph_inputs=graph_inputs,
        arguments=arguments,
        tasks={
            "child": structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    spec=_make_container_component()
                ),
                is_enabled=is_enabled,
            )
        },
    )
    session_factory = _create_session_factory()
    _create_pipeline_run(session_factory, root_task)
    launched_container_mock = _make_launched_container_mock()
    _process_queued_executions(session_factory, launched_container_mock)
    return session_factory, launched_container_mock


class TestConditionalExecutionOrchestration:
    def test_constant_true_launches_container(self) -> None:
        session_factory, launched_container_mock = _run_single_conditional_task("TruE\n")

        launched_container_mock.assert_called_once()
        child = _get_execution_node(session_factory(), "child")
        assert child.container_execution_status != bts.ContainerExecutionStatus.SKIPPED

    def test_constant_false_skips_container(self) -> None:
        session_factory, launched_container_mock = _run_single_conditional_task("FalsE\n")

        launched_container_mock.assert_not_called()
        child = _get_execution_node(session_factory(), "child")
        assert child.container_execution_status == bts.ContainerExecutionStatus.SKIPPED

    def test_invalid_is_enabled_value_marks_system_error(self) -> None:
        session_factory, launched_container_mock = _run_single_conditional_task("maybe")

        launched_container_mock.assert_not_called()
        child = _get_execution_node(session_factory(), "child")
        assert (
            child.container_execution_status
            == bts.ContainerExecutionStatus.SYSTEM_ERROR
        )

    def test_graph_input_false_skips_container(self) -> None:
        session_factory, launched_container_mock = _run_single_conditional_task(
            structures.GraphInputArgument(
                graph_input=structures.GraphInputReference(input_name="enable")
            ),
            graph_inputs=["enable"],
            arguments={"enable": "false\n"},
        )

        launched_container_mock.assert_not_called()
        child = _get_execution_node(session_factory(), "child")
        assert child.container_execution_status == bts.ContainerExecutionStatus.SKIPPED

    def test_graph_input_true_launches_container(self) -> None:
        _session_factory_unused, launched_container_mock = _run_single_conditional_task(
            structures.GraphInputArgument(
                graph_input=structures.GraphInputReference(input_name="enable")
            ),
            graph_inputs=["enable"],
            arguments={"enable": "true\n"},
        )

        launched_container_mock.assert_called_once()
        # The special is_enabled artifact must not be forwarded to the container
        # as a regular input (it is popped before cache-keying / launching).
        input_arguments = (
            launched_container_mock.call_args.kwargs.get("input_arguments") or {}
        )
        assert _IS_ENABLED_INPUT_NAME not in input_arguments

    def test_disabled_task_skips_downstream(self) -> None:
        root_task = _make_graph_task_spec(
            tasks={
                "upstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(outputs=["out"])
                    ),
                    is_enabled="false",
                ),
                "downstream": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component(inputs=["in"])
                    ),
                    arguments={
                        "in": structures.TaskOutputArgument(
                            task_output=structures.TaskOutputReference(
                                task_id="upstream", output_name="out"
                            )
                        )
                    },
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
            upstream.container_execution_status == bts.ContainerExecutionStatus.SKIPPED
        )
        assert (
            downstream.container_execution_status
            == bts.ContainerExecutionStatus.SKIPPED
        )
