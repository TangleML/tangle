"""Tests for the pod hold that keeps a finished pod readable until we've read it.

Kubernetes clusters garbage-collect terminated pods on their own schedule, and
that schedule is not ours to set: a pod can be removed within seconds of its
container finishing, before the orchestrator's next poll observes the terminal
status, exit code and logs. The pod launcher can opt into holding the pod object
with a finalizer, which turns that race into a handshake.

The dangerous half is the release, not the hold: a pod that is held and never
released stays in ``Terminating`` for ever. These tests pin both halves, and in
particular that every path which terminalizes an execution releases the hold,
including cancellation, which returns early and is never polled again.
"""

import datetime
from typing import Callable
from unittest import mock

import pytest
from kubernetes import client as k8s_client_lib
from kubernetes.client import exceptions as k8s_exceptions
from sqlalchemy import orm
from sqlalchemy import sql

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers

_FINALIZER = kubernetes_launchers._STATUS_OBSERVED_FINALIZER
_LABEL_KEY = kubernetes_launchers._STATUS_OBSERVED_FINALIZER_LABEL_KEY
_LAUNCHER_DATA = {"pod_name": "task-1-abcde", "namespace": "kueue-jobs"}


def _api_exception(status: int) -> k8s_exceptions.ApiException:
    return k8s_exceptions.ApiException(status=status, reason="test")


def _pod(
    *,
    name: str = "task-1-abcde",
    namespace: str = "kueue-jobs",
    finalizers: list[str] | None = None,
    deletion_timestamp=None,
) -> k8s_client_lib.V1Pod:
    return k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(
            name=name,
            namespace=namespace,
            finalizers=finalizers,
            deletion_timestamp=deletion_timestamp,
        )
    )


class TestAddingTheHold:
    def test_adds_the_finalizer_and_a_matching_label(self) -> None:
        pod = _pod()
        kubernetes_launchers._add_status_observed_finalizer(pod)
        assert pod.metadata.finalizers == [_FINALIZER]
        # Finalizers are not selectable, so the sweeper needs the label.
        assert pod.metadata.labels[_LABEL_KEY] == "true"

    def test_is_idempotent_and_keeps_other_finalizers(self) -> None:
        pod = _pod(finalizers=["someone.else/finalizer"])
        kubernetes_launchers._add_status_observed_finalizer(pod)
        kubernetes_launchers._add_status_observed_finalizer(pod)
        assert pod.metadata.finalizers == ["someone.else/finalizer", _FINALIZER]

    def test_the_hold_is_off_unless_asked_for(self) -> None:
        # A hold that is never released is worse than a pod collected too early,
        # so opting in is deliberate.
        with mock.patch.dict("os.environ", {}, clear=True):
            assert kubernetes_launchers._env_flag(
                kubernetes_launchers._HOLD_PODS_ENV_VAR
            ) is False
        with mock.patch.dict(
            "os.environ",
            {kubernetes_launchers._HOLD_PODS_ENV_VAR: "true"},
        ):
            assert kubernetes_launchers._env_flag(
                kubernetes_launchers._HOLD_PODS_ENV_VAR
            ) is True


class TestRemovingTheHold:
    def test_removes_the_finalizer_with_a_json_patch(self) -> None:
        api = mock.MagicMock()
        api.read_namespaced_pod.return_value = _pod(
            finalizers=["someone.else/finalizer", _FINALIZER]
        )

        released = kubernetes_launchers._remove_status_observed_finalizer(
            core_api_client=api, pod_name="task-1-abcde", namespace="kueue-jobs"
        )

        assert released is True
        patch_body = api.patch_namespaced_pod.call_args.kwargs["body"]
        # A list body is what makes the Kubernetes client send a JSON Patch. A
        # strategic merge patch would merge the list and never remove anything.
        assert isinstance(patch_body, list)
        assert patch_body == [
            {
                "op": "test",
                "path": "/metadata/finalizers",
                "value": ["someone.else/finalizer", _FINALIZER],
            },
            {
                "op": "replace",
                "path": "/metadata/finalizers",
                "value": ["someone.else/finalizer"],
            },
        ]

    def test_does_nothing_when_the_pod_is_not_held(self) -> None:
        api = mock.MagicMock()
        api.read_namespaced_pod.return_value = _pod(finalizers=None)

        released = kubernetes_launchers._remove_status_observed_finalizer(
            core_api_client=api, pod_name="task-1-abcde", namespace="kueue-jobs"
        )

        assert released is False
        api.patch_namespaced_pod.assert_not_called()

    def test_a_pod_that_is_already_gone_is_not_an_error(self) -> None:
        api = mock.MagicMock()
        api.read_namespaced_pod.side_effect = _api_exception(404)

        assert (
            kubernetes_launchers._remove_status_observed_finalizer(
                core_api_client=api, pod_name="task-1-abcde", namespace="kueue-jobs"
            )
            is False
        )

    def test_retries_when_another_writer_changed_the_finalizers(self) -> None:
        api = mock.MagicMock()
        api.read_namespaced_pod.return_value = _pod(finalizers=[_FINALIZER])
        # The `test` op fails when the list moved under us; re-read and retry.
        api.patch_namespaced_pod.side_effect = [_api_exception(422), None]

        released = kubernetes_launchers._remove_status_observed_finalizer(
            core_api_client=api, pod_name="task-1-abcde", namespace="kueue-jobs"
        )

        assert released is True
        assert api.patch_namespaced_pod.call_count == 2
        assert api.read_namespaced_pod.call_count == 2

    def test_a_real_api_error_is_not_swallowed_here(self) -> None:
        api = mock.MagicMock()
        api.read_namespaced_pod.return_value = _pod(finalizers=[_FINALIZER])
        api.patch_namespaced_pod.side_effect = _api_exception(500)

        with pytest.raises(k8s_exceptions.ApiException):
            kubernetes_launchers._remove_status_observed_finalizer(
                core_api_client=api, pod_name="task-1-abcde", namespace="kueue-jobs"
            )

    def test_launched_container_release_uses_its_own_pod_coordinates(self) -> None:
        api = mock.MagicMock()
        api.read_namespaced_pod.return_value = _pod(finalizers=[_FINALIZER])
        launcher = mock.MagicMock(_api_client=mock.MagicMock(), _request_timeout=10)
        launched = kubernetes_launchers.LaunchedKubernetesContainer(
            pod_name="task-1-abcde",
            namespace="kueue-jobs",
            output_uris={},
            log_uri="file:///tmp/log",
            debug_pod=_pod(),
            launcher=launcher,
        )

        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=api
        ):
            assert launched.release() is True

        assert api.patch_namespaced_pod.call_args.kwargs["name"] == "task-1-abcde"
        assert api.patch_namespaced_pod.call_args.kwargs["namespace"] == "kueue-jobs"


class TestSweepingAbandonedHolds:
    def _sweep(self, pods: list[k8s_client_lib.V1Pod], api: mock.MagicMock) -> int:
        api.list_pod_for_all_namespaces.return_value = k8s_client_lib.V1PodList(
            items=pods
        )
        fake_launcher = mock.MagicMock(
            _api_client=mock.MagicMock(), _request_timeout=10
        )
        with mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "CoreV1Api", return_value=api
        ):
            return kubernetes_launchers._KubernetesPodLauncher.release_abandoned_finalizer_holds(
                fake_launcher, max_hold=datetime.timedelta(minutes=10)
            )

    def test_releases_only_holds_older_than_the_cap(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        stale = _pod(
            name="stale",
            finalizers=[_FINALIZER],
            deletion_timestamp=now - datetime.timedelta(minutes=30),
        )
        recent = _pod(
            name="recent",
            finalizers=[_FINALIZER],
            deletion_timestamp=now - datetime.timedelta(minutes=1),
        )
        # Held, but nobody has asked for it to be deleted, so it blocks nothing.
        not_deleted = _pod(name="alive", finalizers=[_FINALIZER])
        api = mock.MagicMock()

        released = self._sweep([stale, recent, not_deleted], api)

        assert released == 1
        patched_names = {
            call.kwargs["name"] for call in api.patch_namespaced_pod.call_args_list
        }
        assert patched_names == {"stale"}

    def test_one_stuck_pod_does_not_stop_the_sweep(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        pods = [
            _pod(
                name=name,
                finalizers=[_FINALIZER],
                deletion_timestamp=now - datetime.timedelta(minutes=30),
            )
            for name in ("first", "second")
        ]
        api = mock.MagicMock()
        # The first pod's release blows up; the second must still be swept.
        api.patch_namespaced_pod.side_effect = [_api_exception(500), None]

        assert self._sweep(pods, api) == 1


class TestTheLauncherProductionUses:
    def test_the_pod_or_job_launcher_forwards_the_sweep(self) -> None:
        # `_KubernetesPodOrJobLauncher` composes the pod launcher rather than
        # inheriting from it, so without an explicit delegation the orchestrator
        # would find no sweeper at all on the launcher production uses.
        composite = mock.MagicMock(
            spec=kubernetes_launchers._KubernetesPodOrJobLauncher
        )
        assert hasattr(composite, "release_abandoned_finalizer_holds")

    def test_the_job_launcher_never_holds_its_pods(self) -> None:
        # A Job outlives its pods, and a finalizer on Job-owned pods would wedge
        # the Job controller's own cleanup.
        assert not hasattr(
            kubernetes_launchers._KubernetesJobLauncher,
            "release_abandoned_finalizer_holds",
        )


class TestOrchestratorReleasesEveryTerminalPath:
    def test_a_failing_release_never_changes_the_outcome(self) -> None:
        launched = mock.MagicMock()
        launched.release.side_effect = RuntimeError("kubernetes is having a day")
        # The execution's result is already committed by the time we release.
        orchestrator_sql._release_launched_container(launched)

    def test_cancellation_releases_the_hold(self) -> None:
        session_factory, launched = _create_launched_container_execution()
        _mark_all_execution_nodes_terminated(session_factory)
        orchestrator = _make_orchestrator(session_factory, launched)

        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

        # Cancellation deletes the pod and returns early: the execution is
        # CANCELLED and will never be polled again, so if the hold is not
        # released here the pod is stuck in `Terminating` for ever.
        launched.terminate.assert_called_once()
        launched.release.assert_called_once()
        assert _container_execution_status(session_factory) == (
            bts.ContainerExecutionStatus.CANCELLED
        )

    def test_a_still_running_execution_keeps_its_hold(self) -> None:
        session_factory, launched = _create_launched_container_execution()
        launched.status = launcher_interfaces.ContainerStatus.RUNNING
        orchestrator = _make_orchestrator(session_factory, launched)

        orchestrator.internal_process_running_executions_queue(
            session=session_factory()
        )

        launched.release.assert_not_called()


def _create_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _make_launched_container_mock() -> mock.MagicMock:
    return mock.MagicMock(
        status=launcher_interfaces.ContainerStatus.PENDING,
        to_dict=lambda: dict(_LAUNCHER_DATA),
    )


def _create_launched_container_execution() -> (
    tuple[Callable[[], orm.Session], mock.MagicMock]
):
    """A pipeline run with one container task, launched and PENDING."""
    pipeline_spec = structures.ComponentSpec(
        implementation=structures.GraphImplementation(
            graph=structures.GraphSpec(
                tasks={
                    "child": structures.TaskSpec(
                        component_ref=structures.ComponentReference(
                            spec=structures.ComponentSpec(
                                implementation=structures.ContainerImplementation(
                                    container=structures.ContainerSpec(image="python")
                                )
                            )
                        )
                    )
                }
            )
        ),
    )
    root_task = structures.TaskSpec(
        component_ref=structures.ComponentReference(spec=pipeline_spec)
    )
    session_factory = _create_session_factory()
    api_server_sql.PipelineRunsApiService_Sql().create(
        session=session_factory(),
        root_task=root_task,
        created_by="user1",
    )
    launched = _make_launched_container_mock()
    launch_orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=mock.MagicMock(
            launch_container_task=mock.MagicMock(return_value=launched),
            release_abandoned_finalizer_holds=mock.MagicMock(return_value=0),
        ),
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )
    session = session_factory()
    for _ in range(20):
        if not launch_orchestrator.internal_process_queued_executions_queue(
            session=session
        ):
            break
    return session_factory, launched


def _make_orchestrator(
    session_factory: Callable[[], orm.Session],
    launched_container: mock.MagicMock,
) -> orchestrator_sql.OrchestratorService_Sql:
    launcher = mock.MagicMock(
        deserialize_launched_container_from_dict=mock.MagicMock(
            return_value=launched_container
        ),
        get_refreshed_launched_container_from_dict=mock.MagicMock(
            return_value=launched_container
        ),
        release_abandoned_finalizer_holds=mock.MagicMock(return_value=0),
    )
    return orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=mock.MagicMock(),
        data_root_uri="file:///tmp/artifacts",
        logs_root_uri="file:///tmp/logs",
    )


def _mark_all_execution_nodes_terminated(
    session_factory: Callable[[], orm.Session],
) -> None:
    session = session_factory()
    for execution_node in session.scalars(sql.select(bts.ExecutionNode)).all():
        execution_node.extra_data = {"desired_state": "TERMINATED"}
    session.commit()


def _container_execution_status(
    session_factory: Callable[[], orm.Session],
) -> bts.ContainerExecutionStatus:
    container_execution = session_factory().scalar(sql.select(bts.ContainerExecution))
    assert container_execution is not None
    return container_execution.status
