"""Tests for cloud_pipelines_backend.launchers.kubernetes_launchers.

Focused on `LaunchedKubernetesContainer.transient_infra_failure_reason`, which
detects pods wedged by a transient GKE gcsfuse-sidecar bucket-access-check
failure so the orchestrator can relaunch the task in place. Detection is pure
(reads the cached pod status), so these tests run offline with no API client.
"""

from __future__ import annotations

from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend.launchers import kubernetes_launchers
from cloud_pipelines_backend.launchers.kubernetes_launchers import (
    LaunchedKubernetesContainer,
)


def _make_main_container_status(
    *, running: bool = False, terminated: bool = False
) -> k8s_client_lib.V1ContainerStatus:
    state = k8s_client_lib.V1ContainerState()
    if running:
        state.running = k8s_client_lib.V1ContainerStateRunning()
    elif terminated:
        state.terminated = k8s_client_lib.V1ContainerStateTerminated(exit_code=0)
    else:
        state.waiting = k8s_client_lib.V1ContainerStateWaiting(
            reason="CreateContainerConfigError"
        )
    return k8s_client_lib.V1ContainerStatus(
        name="main", ready=False, restart_count=0, image="img", image_id="", state=state
    )


def _make_sidecar_status(
    *,
    exit_code: int = 255,
    reason: str = "Error",
    message: str = "Bucket access check failed for stg-oasis-tmp",
    in_last_state: bool = True,
) -> k8s_client_lib.V1ContainerStatus:
    terminated = k8s_client_lib.V1ContainerStateTerminated(
        exit_code=exit_code, reason=reason, message=message
    )
    state = k8s_client_lib.V1ContainerState()
    last_state = k8s_client_lib.V1ContainerState()
    if in_last_state:
        last_state.terminated = terminated
    else:
        state.terminated = terminated
    return k8s_client_lib.V1ContainerStatus(
        name=kubernetes_launchers._GCSFUSE_SIDECAR_CONTAINER_NAME,
        ready=False,
        restart_count=1,
        image="gcsfuse",
        image_id="",
        state=state,
        last_state=last_state,
    )


def _make_pod(
    *,
    sidecar_status: k8s_client_lib.V1ContainerStatus | None,
    main_status: k8s_client_lib.V1ContainerStatus | None = None,
) -> k8s_client_lib.V1Pod:
    if main_status is None:
        main_status = _make_main_container_status()
    init_statuses = [sidecar_status] if sidecar_status is not None else []
    return k8s_client_lib.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=k8s_client_lib.V1ObjectMeta(
            name="task-abc-orig",
            generate_name="task-abc-",
            namespace="kueue-jobs-staging",
            annotations={"gke-gcsfuse/volumes": "true"},
        ),
        spec=k8s_client_lib.V1PodSpec(
            restart_policy="Never",
            containers=[k8s_client_lib.V1Container(name="main")],
            init_containers=[
                k8s_client_lib.V1Container(
                    name=kubernetes_launchers._GCSFUSE_SIDECAR_CONTAINER_NAME
                )
            ],
        ),
        status=k8s_client_lib.V1PodStatus(
            phase="Pending",
            container_statuses=[main_status],
            init_container_statuses=init_statuses,
        ),
    )


def _make_container(pod: k8s_client_lib.V1Pod) -> LaunchedKubernetesContainer:
    return LaunchedKubernetesContainer(
        pod_name=pod.metadata.name,
        namespace=pod.metadata.namespace,
        output_uris={"out": "gs://bucket/out"},
        log_uri="gs://bucket/log",
        debug_pod=pod,
        launcher=None,
    )


def test_wedged_sidecar_returns_reason():
    pod = _make_pod(sidecar_status=_make_sidecar_status())
    container = _make_container(pod)

    reason = container.transient_infra_failure_reason()

    assert reason is not None
    assert "gke-gcsfuse-sidecar" in reason


def test_wedge_in_current_state_is_detected():
    pod = _make_pod(sidecar_status=_make_sidecar_status(in_last_state=False))
    container = _make_container(pod)
    assert container.transient_infra_failure_reason() is not None


def test_running_main_container_is_not_a_wedge():
    pod = _make_pod(
        sidecar_status=_make_sidecar_status(),
        main_status=_make_main_container_status(running=True),
    )
    container = _make_container(pod)
    assert container.transient_infra_failure_reason() is None


def test_terminated_main_container_is_not_a_wedge():
    pod = _make_pod(
        sidecar_status=_make_sidecar_status(),
        main_status=_make_main_container_status(terminated=True),
    )
    container = _make_container(pod)
    assert container.transient_infra_failure_reason() is None


def test_sidecar_clean_exit_is_not_a_wedge():
    pod = _make_pod(
        sidecar_status=_make_sidecar_status(exit_code=0, reason="Completed")
    )
    container = _make_container(pod)
    assert container.transient_infra_failure_reason() is None


def test_unrelated_failure_message_is_not_a_wedge():
    pod = _make_pod(sidecar_status=_make_sidecar_status(message="some other failure"))
    container = _make_container(pod)
    assert container.transient_infra_failure_reason() is None


def test_no_sidecar_is_not_a_wedge():
    pod = _make_pod(sidecar_status=None)
    container = _make_container(pod)
    assert container.transient_infra_failure_reason() is None


def test_default_interface_reason_is_none():
    # The base interface default reports no transient failure.
    sentinel = kubernetes_launchers.interfaces.LaunchedContainer()
    assert sentinel.transient_infra_failure_reason() is None
