import json
from unittest import mock

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import kubernetes_launchers
from cloud_pipelines_backend.runtime import multi_node


def test_kubernetes_job_launcher_injects_multi_node_runtime_env_and_service():
    api_client = mock.MagicMock()
    api_client.configuration.host = "https://cluster.example"
    fake_barrier_token = "fake-barrier-token"

    def create_job(*, namespace, body, _request_timeout):
        del namespace, _request_timeout
        return body

    with (
        mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "VersionApi"
        ) as version_api,
        mock.patch.object(kubernetes_launchers.k8s_client_lib, "CoreV1Api") as core_api,
        mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "BatchV1Api"
        ) as batch_api,
        mock.patch.object(
            kubernetes_launchers.secrets,
            "token_hex",
            return_value=fake_barrier_token,
        ),
    ):
        version_api.return_value.get_code.return_value = object()
        batch_api.return_value.create_namespaced_job.side_effect = create_job
        launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher(
            api_client=api_client,
            namespace="default",
        )

        launched_container = launcher.launch_container_task(
            component_spec=component_structures.ComponentSpec(
                implementation=component_structures.ContainerImplementation(
                    container=component_structures.ContainerSpec(
                        image="python:3.12",
                        command=["python"],
                        args=["-c", "print('hello')"],
                        env={
                            multi_node.NUMBER_OF_NODES_ENV_VAR_NAME: "stale",
                            "USER_ENV": "kept",
                        },
                    )
                )
            ),
            input_arguments={},
            output_uris={},
            log_uri="file:///tmp/logs/execution-123",
            annotations={
                common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "execution-123",
                kubernetes_launchers.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "3",
            },
        )
        launched_container.terminate()

    service = core_api.return_value.create_namespaced_service.call_args.kwargs["body"]
    assert service.metadata.name == "tangle-ce-execution-123"
    assert service.spec.cluster_ip == "None"
    assert service.spec.selector == {"job-name": "tangle-ce-execution-123"}
    assert service.spec.ports[0].name == "barrier"
    assert service.spec.ports[0].port == multi_node.DEFAULT_BARRIER_PORT
    assert service.spec.ports[0].target_port == multi_node.DEFAULT_BARRIER_PORT
    assert service.spec.publish_not_ready_addresses is True
    core_api.return_value.delete_namespaced_service.assert_called_once_with(
        name="tangle-ce-execution-123",
        namespace="default",
        _request_timeout=10,
    )

    job = batch_api.return_value.create_namespaced_job.call_args.kwargs["body"]
    pod_spec = job.spec.template.spec
    assert pod_spec.subdomain == "tangle-ce-execution-123"
    env_by_name = {env_var.name: env_var for env_var in pod_spec.containers[0].env}

    assert env_by_name["USER_ENV"].value == "kept"
    assert env_by_name[multi_node.NUMBER_OF_NODES_ENV_VAR_NAME].value == "3"
    assert (
        env_by_name[multi_node.NODE_INDEX_ENV_VAR_NAME].value
        == "$(_TANGLE_MULTI_NODE_NODE_INDEX)"
    )
    assert (
        env_by_name[multi_node.NODE_0_ADDRESS_ENV_VAR_NAME].value
        == "tangle-ce-execution-123-0.tangle-ce-execution-123"
    )
    assert env_by_name[multi_node.ALL_NODE_ADDRESSES_ENV_VAR_NAME].value == (
        "tangle-ce-execution-123-0.tangle-ce-execution-123,"
        "tangle-ce-execution-123-1.tangle-ce-execution-123,"
        "tangle-ce-execution-123-2.tangle-ce-execution-123"
    )
    assert env_by_name[multi_node.BARRIER_PORT_ENV_VAR_NAME].value == str(
        multi_node.DEFAULT_BARRIER_PORT
    )
    assert (
        env_by_name[multi_node.BARRIER_TOKEN_ENV_VAR_NAME].value == fake_barrier_token
    )
    serialized_launcher_data = json.dumps(launched_container.to_dict())
    assert fake_barrier_token not in serialized_launcher_data
    assert '"value": "[REDACTED]"' in serialized_launcher_data
    assert (
        env_by_name["_TANGLE_MULTI_NODE_NODE_INDEX"].value_from.field_ref.field_path
        == "metadata.annotations['batch.kubernetes.io/job-completion-index']"
    )


def test_kubernetes_job_terminate_deletes_service_when_job_is_already_gone():
    launcher = mock.MagicMock()
    launcher._api_client = mock.MagicMock()
    launcher._request_timeout = 10
    launched_container = kubernetes_launchers.LaunchedKubernetesJob(
        job_name="tangle-ce-execution-123",
        namespace="default",
        output_uris={},
        log_uri="file:///tmp/logs/execution-123",
        debug_job=k8s_client_lib.V1Job(),
        launcher=launcher,
    )

    with (
        mock.patch.object(kubernetes_launchers.k8s_client_lib, "CoreV1Api") as core_api,
        mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "BatchV1Api"
        ) as batch_api,
    ):
        batch_api.return_value.delete_namespaced_job.side_effect = (
            k8s_client_lib.exceptions.ApiException(status=404)
        )
        launched_container.terminate()

    core_api.return_value.delete_namespaced_service.assert_called_once_with(
        name="tangle-ce-execution-123",
        namespace="default",
        _request_timeout=10,
    )


def test_kubernetes_job_launcher_deletes_service_when_job_creation_fails():
    api_client = mock.MagicMock()
    fake_barrier_token = "fake-barrier-token"

    with (
        mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "VersionApi"
        ) as version_api,
        mock.patch.object(kubernetes_launchers.k8s_client_lib, "CoreV1Api") as core_api,
        mock.patch.object(
            kubernetes_launchers.k8s_client_lib, "BatchV1Api"
        ) as batch_api,
        mock.patch.object(
            kubernetes_launchers.secrets,
            "token_hex",
            return_value=fake_barrier_token,
        ),
    ):
        version_api.return_value.get_code.return_value = object()
        batch_api.return_value.create_namespaced_job.side_effect = RuntimeError(
            "job create failed"
        )
        launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher(
            api_client=api_client,
            namespace="default",
        )

        with pytest.raises(kubernetes_launchers.interfaces.LauncherError) as exc_info:
            launcher.launch_container_task(
                component_spec=component_structures.ComponentSpec(
                    implementation=component_structures.ContainerImplementation(
                        container=component_structures.ContainerSpec(
                            image="python:3.12"
                        )
                    )
                ),
                input_arguments={},
                output_uris={},
                log_uri="file:///tmp/logs/execution-123",
                annotations={
                    common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "execution-123",
                    kubernetes_launchers.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "2",
                },
            )

    assert fake_barrier_token not in str(exc_info.value)
    assert "[REDACTED]" in str(exc_info.value)
    core_api.return_value.delete_namespaced_service.assert_called_once_with(
        name="tangle-ce-execution-123",
        namespace="default",
        _request_timeout=10,
    )
