from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace
from unittest import mock

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers as kl


def _component(*, dynamic_inputs: bool = False) -> structures.ComponentSpec:
    input_names = [
        "number_of_nodes",
        "node_index",
        "node_0_address",
        "all_node_addresses",
    ]
    return structures.ComponentSpec(
        name="test-component",
        inputs=(
            [structures.InputSpec(name=name) for name in input_names]
            if dynamic_inputs
            else []
        ),
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(
                image="python:3.12",
                command=["echo"],
                args=(
                    [
                        structures.InputValuePlaceholder(input_name=name)
                        for name in input_names
                    ]
                    if dynamic_inputs
                    else ["ok"]
                ),
            )
        ),
    )


def _dynamic_input_arguments() -> dict[str, interfaces.InputArgument]:
    dynamic_data_by_input = {
        "number_of_nodes": "system/multi_node/number_of_nodes",
        "node_index": "system/multi_node/node_index",
        "node_0_address": "system/multi_node/node_0_address",
        "all_node_addresses": "system/multi_node/all_node_addresses",
    }
    return {
        name: interfaces.InputArgument(
            total_size=0,
            is_dir=False,
            staging_uri="",
            dynamic_data=dynamic_data,
        )
        for name, dynamic_data in dynamic_data_by_input.items()
    }


def _launch_kwargs(*, dynamic_inputs: bool = False) -> dict:
    return {
        "component_spec": _component(dynamic_inputs=dynamic_inputs),
        "input_arguments": (_dynamic_input_arguments() if dynamic_inputs else {}),
        "output_uris": {},
        "log_uri": "file:///tmp/tangle-test.log",
    }


@pytest.fixture
def api_client():
    client = mock.Mock()
    client.configuration.host = "https://kubernetes.example.test"
    with mock.patch.object(kl.k8s_client_lib, "VersionApi") as version_api:
        version_api.return_value.get_code.return_value = object()
        yield client


@pytest.fixture
def kubernetes_apis(monkeypatch):
    core_api = mock.Mock()
    batch_api = mock.Mock()

    def create_pod(*, namespace, body, **_kwargs):
        body.metadata.name = (
            body.metadata.name or f"{body.metadata.generate_name}abc123"
        )
        body.metadata.namespace = body.metadata.namespace or namespace
        return body

    def create_service(*, namespace, body, **_kwargs):
        body.metadata.namespace = body.metadata.namespace or namespace
        return body

    def create_job(*, namespace, body, **_kwargs):
        body.metadata.namespace = body.metadata.namespace or namespace
        return body

    core_api.create_namespaced_pod.side_effect = create_pod
    core_api.create_namespaced_service.side_effect = create_service
    batch_api.create_namespaced_job.side_effect = create_job
    monkeypatch.setattr(
        kl.k8s_client_lib, "CoreV1Api", lambda *args, **kwargs: core_api
    )
    monkeypatch.setattr(
        kl.k8s_client_lib, "BatchV1Api", lambda *args, **kwargs: batch_api
    )
    return SimpleNamespace(core=core_api, batch=batch_api)


def test_unconfigured_single_node_defaults_to_pod(api_client):
    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=api_client
    )
    expected = object()
    launcher._pod_launcher.launch_container_task = mock.Mock(return_value=expected)
    launcher._job_launcher.launch_single_node_container_task = mock.Mock()

    result = launcher.launch_container_task(**_launch_kwargs())

    assert result is expected
    launcher._pod_launcher.launch_container_task.assert_called_once()
    launcher._job_launcher.launch_single_node_container_task.assert_not_called()


@pytest.mark.parametrize(
    ("constructor_mode", "annotation_mode", "expected_mode"),
    [
        ("pod", None, "pod"),
        ("job", None, "job"),
        ("pod", "job", "job"),
        ("job", "pod", "pod"),
    ],
)
def test_single_node_default_and_annotation_route_tasks(
    api_client, constructor_mode, annotation_mode, expected_mode
):
    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=api_client,
        single_node_execution_mode=constructor_mode,
    )
    pod_result = object()
    job_result = object()
    launcher._pod_launcher.launch_container_task = mock.Mock(return_value=pod_result)
    launcher._job_launcher.launch_single_node_container_task = mock.Mock(
        return_value=job_result
    )
    launcher._job_launcher.launch_container_task = mock.Mock()
    annotations = (
        {kl.SINGLE_NODE_EXECUTION_MODE_ANNOTATION_KEY: annotation_mode}
        if annotation_mode
        else None
    )

    result = launcher.launch_container_task(
        **_launch_kwargs(),
        annotations=annotations,
    )

    assert result is (job_result if expected_mode == "job" else pod_result)
    assert launcher._pod_launcher.launch_container_task.call_count == (
        expected_mode == "pod"
    )
    assert launcher._job_launcher.launch_single_node_container_task.call_count == (
        expected_mode == "job"
    )
    launcher._job_launcher.launch_container_task.assert_not_called()


def test_google_hybrid_constructor_threads_single_node_default(api_client, monkeypatch):
    provider_module_name = (
        "cloud_pipelines.orchestration.storage_providers.google_cloud_storage"
    )
    provider_module = types.ModuleType(provider_module_name)
    provider_module.GoogleCloudStorageProvider = mock.Mock(return_value=mock.Mock())
    monkeypatch.setitem(sys.modules, provider_module_name, provider_module)
    launcher_module_name = (
        "cloud_pipelines_backend.launchers.google_kubernetes_launchers"
    )
    sys.modules.pop(launcher_module_name, None)
    try:
        google_launchers = importlib.import_module(launcher_module_name)
        launcher = google_launchers.GoogleKubernetesEngine_UsingGoogleCloudStorage_KubernetesPodOrJobLauncher(
            api_client=api_client,
            gcs_client=mock.Mock(),
            single_node_execution_mode="job",
        )
        expected = object()
        launcher._job_launcher.launch_single_node_container_task = mock.Mock(
            return_value=expected
        )

        result = launcher.launch_container_task(**_launch_kwargs())

        assert result is expected
        launcher._job_launcher.launch_single_node_container_task.assert_called_once()
    finally:
        sys.modules.pop(launcher_module_name, None)


def test_invalid_single_node_modes_fail_before_dispatch(api_client):
    with pytest.raises(
        interfaces.LauncherError, match="Invalid single-node execution mode"
    ):
        kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
            api_client=api_client,
            single_node_execution_mode="invalid",
        )

    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=api_client
    )
    launcher._pod_launcher.launch_container_task = mock.Mock()
    launcher._job_launcher.launch_single_node_container_task = mock.Mock()
    launcher._job_launcher.launch_container_task = mock.Mock()

    with pytest.raises(
        interfaces.LauncherError, match="Invalid single-node execution mode"
    ):
        launcher.launch_container_task(
            **_launch_kwargs(),
            annotations={kl.SINGLE_NODE_EXECUTION_MODE_ANNOTATION_KEY: "invalid"},
        )

    launcher._pod_launcher.launch_container_task.assert_not_called()
    launcher._job_launcher.launch_single_node_container_task.assert_not_called()
    launcher._job_launcher.launch_container_task.assert_not_called()


def test_multi_node_annotation_takes_precedence_over_single_node_mode(api_client):
    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=api_client,
        single_node_execution_mode="job",
    )
    expected = object()
    launcher._pod_launcher.launch_container_task = mock.Mock()
    launcher._job_launcher.launch_single_node_container_task = mock.Mock()
    launcher._job_launcher.launch_container_task = mock.Mock(return_value=expected)

    result = launcher.launch_container_task(
        **_launch_kwargs(),
        annotations={
            kl.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "2",
            kl.SINGLE_NODE_EXECUTION_MODE_ANNOTATION_KEY: "invalid",
        },
    )

    assert result is expected
    launcher._job_launcher.launch_container_task.assert_called_once()
    launcher._job_launcher.launch_single_node_container_task.assert_not_called()
    launcher._pod_launcher.launch_container_task.assert_not_called()


def test_single_node_job_shape_dynamic_values_and_serialization(
    api_client, kubernetes_apis
):
    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=api_client,
        service_account_name="task-runner",
        pod_labels={"example.test/workload": "training"},
        pod_annotations={"example.test/owner": "test"},
        single_node_execution_mode="job",
    )

    launched = launcher.launch_container_task(
        **_launch_kwargs(dynamic_inputs=True),
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "single-node-123"
        },
    )

    job = kubernetes_apis.batch.create_namespaced_job.call_args.kwargs["body"]
    assert job.spec.completion_mode == "NonIndexed"
    assert job.spec.completions == 1
    assert job.spec.parallelism == 1
    assert job.spec.backoff_limit == 0
    assert job.spec.backoff_limit_per_index is None
    assert job.spec.max_failed_indexes is None
    assert job.spec.pod_replacement_policy is None

    pod_template = job.spec.template
    pod_spec = pod_template.spec
    main_container = pod_spec.containers[0]
    assert pod_spec.restart_policy == "Never"
    assert pod_spec.service_account_name == "task-runner"
    assert pod_spec.hostname is None
    assert pod_spec.subdomain is None
    assert pod_template.metadata.labels == {"example.test/workload": "training"}
    assert pod_template.metadata.annotations["example.test/owner"] == "test"
    assert main_container.args == ["1", "0", "localhost", "localhost"]
    assert kl._MULTI_NODE_NODE_INDEX_ENV_VAR_NAME not in {
        env.name for env in main_container.env or []
    }
    kubernetes_apis.core.create_namespaced_service.assert_not_called()

    serialized = launched.to_dict()
    launcher_data = serialized[kl.LaunchedKubernetesJob.SERIALIZATION_ROOT_KEY]
    assert launcher_data["single_node_job"] is True
    restored = kl.LaunchedKubernetesJob.from_dict(serialized)
    assert restored.to_dict() == serialized


def test_multi_node_indexed_job_shape_is_unchanged(api_client, kubernetes_apis):
    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=api_client,
        single_node_execution_mode="job",
    )

    launched = launcher.launch_container_task(
        **_launch_kwargs(dynamic_inputs=True),
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "multi-node-123",
            kl.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "2",
            kl.SINGLE_NODE_EXECUTION_MODE_ANNOTATION_KEY: "pod",
        },
    )

    job = kubernetes_apis.batch.create_namespaced_job.call_args.kwargs["body"]
    assert job.spec.completion_mode == "Indexed"
    assert job.spec.completions == 2
    assert job.spec.parallelism == 2
    assert job.spec.backoff_limit is None
    assert job.spec.backoff_limit_per_index == 0
    assert job.spec.max_failed_indexes == 0

    pod_spec = job.spec.template.spec
    main_container = pod_spec.containers[0]
    assert pod_spec.restart_policy == "Never"
    assert pod_spec.subdomain == "tangle-ce-multi-node-123"
    completion_index_env = main_container.env[0]
    assert completion_index_env.name == kl._MULTI_NODE_NODE_INDEX_ENV_VAR_NAME
    assert (
        completion_index_env.value_from.field_ref.field_path
        == "metadata.annotations['batch.kubernetes.io/job-completion-index']"
    )
    assert main_container.args == [
        "2",
        f"$({kl._MULTI_NODE_NODE_INDEX_ENV_VAR_NAME})",
        "tangle-ce-multi-node-123-0.tangle-ce-multi-node-123",
        "tangle-ce-multi-node-123-0.tangle-ce-multi-node-123,"
        "tangle-ce-multi-node-123-1.tangle-ce-multi-node-123",
    ]

    service = kubernetes_apis.core.create_namespaced_service.call_args.kwargs["body"]
    assert service.spec.cluster_ip == "None"
    assert service.spec.selector == {"job-name": "tangle-ce-multi-node-123"}
    serialized = launched.to_dict()
    launcher_data = serialized[kl.LaunchedKubernetesJob.SERIALIZATION_ROOT_KEY]
    assert "single_node_job" not in launcher_data
    assert kl.LaunchedKubernetesJob.from_dict(serialized).to_dict() == serialized


def test_dedicated_job_launcher_keeps_indexed_service_behavior(
    api_client, kubernetes_apis
):
    launcher = kl.Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher(
        api_client=api_client
    )

    launched = launcher.launch_container_task(
        **_launch_kwargs(),
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "dedicated-123"
        },
    )

    job = kubernetes_apis.batch.create_namespaced_job.call_args.kwargs["body"]
    assert job.spec.completion_mode == "Indexed"
    assert job.spec.completions == 1
    assert job.spec.parallelism == 1
    assert job.spec.backoff_limit_per_index == 0
    assert job.spec.max_failed_indexes == 0
    assert job.spec.template.spec.subdomain == "tangle-ce-dedicated-123"
    kubernetes_apis.core.create_namespaced_service.assert_called_once()
    assert (
        "single_node_job"
        not in launched.to_dict()[kl.LaunchedKubernetesJob.SERIALIZATION_ROOT_KEY]
    )


def test_legacy_pod_serialization_still_round_trips():
    pod = k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="legacy-pod", namespace="default"),
        spec=k8s_client_lib.V1PodSpec(
            containers=[k8s_client_lib.V1Container(name="main", image="python:3.12")]
        ),
        status=k8s_client_lib.V1PodStatus(phase="Pending"),
    )
    launched = kl.LaunchedKubernetesContainer(
        pod_name="legacy-pod",
        namespace="default",
        output_uris={},
        log_uri="file:///tmp/legacy.log",
        debug_pod=pod,
    )

    serialized = launched.to_dict()

    assert kl.LaunchedKubernetesContainer.from_dict(serialized).to_dict() == serialized
    assert (
        kl.LaunchedKubernetesContainer.from_dict(serialized["kubernetes"]).to_dict()
        == serialized
    )
