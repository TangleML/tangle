"""Tests for ``orchestrator_sql``."""

import datetime
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


def _make_orchestrator(
    *,
    session_factory: Callable[[], orm.Session],
    launched_container_mock: mock.MagicMock,
    queued_execution_interceptor: (
        orchestrator_sql.QueuedExecutionInterceptor | None
    ) = None,
) -> orchestrator_sql.OrchestratorService_Sql:
    """An orchestrator wired to mocks, launching through `launched_container_mock`."""
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=mock.MagicMock(launch_container_task=launched_container_mock),
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
        queued_execution_interceptor=queued_execution_interceptor,
    )


def _process_queued_executions(
    session_factory: Callable[[], orm.Session],
    launched_container_mock: mock.MagicMock,
    max_number_of_executions: int = 20,
) -> None:
    orchestrator = _make_orchestrator(
        session_factory=session_factory,
        launched_container_mock=launched_container_mock,
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


# --------------------------------------------------------------------------- #
# The sweep must not select parked (UNINITIALIZED) executions.
# --------------------------------------------------------------------------- #


class TestSweepIgnoresUninitialized:
    """`UNINITIALIZED` is off the launch path, not merely behind it.

    Downstream (Oasis quota groups) parks an execution by setting it back to
    `UNINITIALIZED`. That only hides the node if the sweep stops selecting the
    status: were it still selected, the node would be picked again on the next
    tick, redo everything above the gate, re-park -- and with no `ORDER BY` the
    same low-id node would be chosen every time, spending the whole sweep budget
    on one parked execution.
    """

    def test_uninitialized_execution_is_not_selected(self) -> None:
        root_task = _make_graph_task_spec(
            tasks={
                "parked": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component()
                    ),
                ),
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)
        launched_container_mock = _make_launched_container_mock()

        # Park it, exactly as the downstream interceptor will.
        session = session_factory()
        _get_execution_node(session, "parked").container_execution_status = (
            bts.ContainerExecutionStatus.UNINITIALIZED
        )
        session.commit()

        orchestrator = _make_orchestrator(
            session_factory=session_factory,
            launched_container_mock=launched_container_mock,
        )
        selected = orchestrator.internal_process_queued_executions_queue(
            session=session_factory()
        )

        assert selected is False, "the sweep selected a parked execution"
        launched_container_mock.assert_not_called()
        assert (
            _get_execution_node(session_factory(), "parked").container_execution_status
            == bts.ContainerExecutionStatus.UNINITIALIZED
        ), "a parked execution must be left exactly as it was found"

    def test_queued_execution_is_still_selected(self) -> None:
        """The other half: narrowing the selection set did not break the sweep."""
        root_task = _make_graph_task_spec(
            tasks={
                "runnable": structures.TaskSpec(
                    component_ref=structures.ComponentReference(
                        spec=_make_container_component()
                    ),
                ),
            },
        )
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, root_task)
        launched_container_mock = _make_launched_container_mock()

        orchestrator = _make_orchestrator(
            session_factory=session_factory,
            launched_container_mock=launched_container_mock,
        )
        selected = orchestrator.internal_process_queued_executions_queue(
            session=session_factory()
        )

        assert selected is True
        launched_container_mock.assert_called_once()


# --------------------------------------------------------------------------- #
# The interceptor seam: a downstream implementation can take an execution over.
# --------------------------------------------------------------------------- #


# A row in a table the orchestrator never writes on this path, so finding it on disk means
# an uncommitted write survived when it should not have. Stands in for the claim row the
# real gate inserts before it decides -- `execution.extra_data` cannot serve, because the
# orchestrator rewrites that field itself with the status history.
_SCRIBBLE = "half_written_by_a_broken_gate"


class _StubInterceptor:
    """Records what it was called with and answers with a fixed decision.

    Stands in for the downstream (Oasis) quota gate, and owns its transaction the way the
    protocol requires, so the tests exercise the contract and not just the branch:

    * `intercepted=False` -- writes nothing and lets the launch proceed.
    * `intercepted=True` -- takes the execution over: moves it off QUEUED and commits that
      itself, so something other than the sweep has to bring it back.
    * `intercepted=True, leaves_queued=True` -- declines to launch without reaching a
      decision. It writes a status, then rolls itself back, which is what the real gate
      does when its compare-and-set budget runs out and the row must stay sweepable.

    `raises=True` makes it blow up after writing and before committing -- the one case
    where the orchestrator, not the gate, has to clean the session up.
    """

    def __init__(
        self,
        *,
        intercepted: bool,
        leaves_queued: bool = False,
        raises: bool = False,
    ) -> None:
        self._intercepted = intercepted
        self._leaves_queued = leaves_queued
        self._raises = raises
        self.calls: list[str] = []

    def intercept(self, *, session: orm.Session, execution: bts.ExecutionNode) -> bool:
        self.calls.append(execution.id)
        if self._raises:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            session.add(
                bts.Secret(
                    user_id=_SCRIBBLE,
                    secret_name=_SCRIBBLE,
                    secret_value="",
                    created_at=now,
                    updated_at=now,
                )
            )
            raise RuntimeError("the gate is broken")
        if not self._intercepted:
            return False
        execution.container_execution_status = (
            bts.ContainerExecutionStatus.UNINITIALIZED
        )
        if self._leaves_queued:
            session.rollback()
        else:
            session.commit()
        return True


def _single_task_pipeline() -> structures.TaskSpec:
    return _make_graph_task_spec(
        tasks={
            "task": structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    spec=_make_container_component()
                ),
            ),
        },
    )


class TestQueuedExecutionInterceptor:
    """One test per arm of the seam: intercepted, deferred, launched, no interceptor."""

    def test_true_takes_the_execution_off_the_launch_path(self) -> None:
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, _single_task_pipeline())
        launched_container_mock = _make_launched_container_mock()
        interceptor = _StubInterceptor(intercepted=True)

        orchestrator = _make_orchestrator(
            session_factory=session_factory,
            launched_container_mock=launched_container_mock,
            queued_execution_interceptor=interceptor,
        )
        orchestrator.internal_process_queued_executions_queue(session=session_factory())

        assert len(interceptor.calls) == 1
        launched_container_mock.assert_not_called()
        node = _get_execution_node(session_factory(), "task")
        assert (
            node.container_execution_status
            == bts.ContainerExecutionStatus.UNINITIALIZED
        ), "the interceptor committed this itself and the orchestrator left it alone"
        assert node.container_execution is None, "no container may have been created"

    def test_true_can_decline_and_still_leave_the_row_sweepable(self) -> None:
        """Declining without deciding stops the launch and nothing else."""
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, _single_task_pipeline())
        launched_container_mock = _make_launched_container_mock()
        interceptor = _StubInterceptor(intercepted=True, leaves_queued=True)

        orchestrator = _make_orchestrator(
            session_factory=session_factory,
            launched_container_mock=launched_container_mock,
            queued_execution_interceptor=interceptor,
        )
        orchestrator.internal_process_queued_executions_queue(session=session_factory())

        assert len(interceptor.calls) == 1
        launched_container_mock.assert_not_called()
        node = _get_execution_node(session_factory(), "task")
        assert (
            node.container_execution_status == bts.ContainerExecutionStatus.QUEUED
        ), "the gate rolled its own write back; the row must still be sweepable"
        assert node.container_execution is None, "no container may have been created"

    def test_false_launches_exactly_as_before(self) -> None:
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, _single_task_pipeline())
        launched_container_mock = _make_launched_container_mock()
        interceptor = _StubInterceptor(intercepted=False)

        orchestrator = _make_orchestrator(
            session_factory=session_factory,
            launched_container_mock=launched_container_mock,
            queued_execution_interceptor=interceptor,
        )
        orchestrator.internal_process_queued_executions_queue(session=session_factory())

        assert len(interceptor.calls) == 1
        launched_container_mock.assert_called_once()

    def test_a_raising_interceptor_fails_open_and_its_write_is_rolled_back(
        self,
    ) -> None:
        """A broken gate must cost gating, not launches -- and leave no trace.

        The stub raises *after* writing and *before* committing, which is the only way
        the orchestrator's session can be left dirty: on every ordinary exit the gate has
        already committed or rolled back for itself. What discards that write is the
        `session.rollback()` the launch path runs to open its own transaction -- delete it
        and this test goes red.
        """
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, _single_task_pipeline())
        launched_container_mock = _make_launched_container_mock()
        interceptor = _StubInterceptor(intercepted=False, raises=True)

        orchestrator = _make_orchestrator(
            session_factory=session_factory,
            launched_container_mock=launched_container_mock,
            queued_execution_interceptor=interceptor,
        )
        orchestrator.internal_process_queued_executions_queue(session=session_factory())

        assert len(interceptor.calls) == 1
        launched_container_mock.assert_called_once()
        leftover = session_factory().get(bts.Secret, (_SCRIBBLE, _SCRIBBLE))
        assert leftover is None, "what the broken gate wrote must not have reached disk"
        node = _get_execution_node(session_factory(), "task")
        assert (
            node.container_execution is not None
        ), "the launch must have been recorded"

    def test_no_interceptor_launches_exactly_as_before(self) -> None:
        """The default. Every existing caller passes nothing and must be unaffected."""
        session_factory = _create_session_factory()
        _create_pipeline_run(session_factory, _single_task_pipeline())
        launched_container_mock = _make_launched_container_mock()

        _process_queued_executions(session_factory, launched_container_mock)

        launched_container_mock.assert_called_once()
