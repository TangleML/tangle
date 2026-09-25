from types import SimpleNamespace

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers


def _launcher(monkeypatch, *, service_error=None):
    events = []
    batch = SimpleNamespace(created=[], deleted=[])
    core = SimpleNamespace(created=[], deleted=[])

    def create_job(namespace, body, _request_timeout):
        events.append("job")
        body.metadata.namespace = namespace
        body.metadata.uid = "job-uid"
        batch.created.append((namespace, body, _request_timeout))
        return body

    def delete_job(**kwargs):
        batch.deleted.append(kwargs)

    def create_service(namespace, body, _request_timeout):
        events.append("service")
        core.created.append((namespace, body, _request_timeout))
        if service_error:
            raise service_error
        return body

    def delete_service(**kwargs):
        core.deleted.append(kwargs)

    batch.create_namespaced_job = create_job
    batch.delete_namespaced_job = delete_job
    core.create_namespaced_service = create_service
    core.delete_namespaced_service = delete_service

    monkeypatch.setattr(
        kubernetes_launchers.k8s_client_lib,
        "BatchV1Api",
        lambda api_client: batch,
    )
    monkeypatch.setattr(
        kubernetes_launchers.k8s_client_lib,
        "CoreV1Api",
        lambda api_client: core,
    )

    launcher = object.__new__(kubernetes_launchers._KubernetesJobLauncher)
    launcher._api_client = SimpleNamespace(
        configuration=SimpleNamespace(host="https://cluster.example")
    )
    launcher._request_timeout = (3, 30)
    launcher._namespace = "tangle-jobs"
    launcher._service_account_name = "tangle"
    launcher._pod_name_prefix = "task-"
    launcher._pod_labels = {}
    launcher._pod_annotations = {}
    launcher._pod_postprocessor = None
    launcher._choose_namespace = lambda annotations: "tangle-jobs"
    launcher._prepare_kubernetes_pod = lambda **kwargs: k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(namespace="tangle-jobs"),
        spec=k8s_client_lib.V1PodSpec(
            containers=[k8s_client_lib.V1Container(name="main")]
        ),
    )
    launcher._transform_job_before_launching = lambda job, annotations: job

    return launcher, batch, core, events


def _launch(launcher):
    return launcher.launch_container_task(
        component_spec=object(),
        input_arguments={},
        output_uris={},
        log_uri="memory://log",
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "execution-1",
            kubernetes_launchers.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "2",
        },
    )


def test_job_service_is_owned_by_the_job(monkeypatch):
    launcher, _, core, events = _launcher(monkeypatch)

    launched = _launch(launcher)

    assert events == ["job", "service"]
    namespace, service, timeout = core.created[0]
    assert namespace == "tangle-jobs"
    assert timeout == (3, 30)
    assert service.metadata.name == "tangle-ce-execution-1"
    assert service.spec.cluster_ip == "None"
    assert service.spec.selector == {"job-name": "tangle-ce-execution-1"}
    assert service.metadata.owner_references == [
        k8s_client_lib.V1OwnerReference(
            api_version="batch/v1",
            kind="Job",
            name="tangle-ce-execution-1",
            uid="job-uid",
        )
    ]
    assert launched._debug_job.spec.template.spec.subdomain == ("tangle-ce-execution-1")


def test_service_creation_failure_rolls_back_the_job(monkeypatch):
    launcher, batch, _, _ = _launcher(
        monkeypatch, service_error=RuntimeError("service create failed")
    )

    with pytest.raises(
        interfaces.LauncherError, match="Failed to create Kubernetes Service"
    ):
        _launch(launcher)

    assert batch.deleted == [
        {
            "name": "tangle-ce-execution-1",
            "namespace": "tangle-jobs",
            "grace_period_seconds": 0,
            "propagation_policy": "Background",
            "_request_timeout": (3, 30),
        }
    ]


def test_cleanup_removes_legacy_service_when_job_is_already_gone(monkeypatch):
    batch = SimpleNamespace()
    core = SimpleNamespace(deleted=[])

    def delete_job(**kwargs):
        raise k8s_client_lib.ApiException(status=404)

    def delete_service(**kwargs):
        core.deleted.append(kwargs)
        raise k8s_client_lib.ApiException(status=404)

    batch.delete_namespaced_job = delete_job
    core.delete_namespaced_service = delete_service
    monkeypatch.setattr(
        kubernetes_launchers.k8s_client_lib,
        "BatchV1Api",
        lambda api_client: batch,
    )
    monkeypatch.setattr(
        kubernetes_launchers.k8s_client_lib,
        "CoreV1Api",
        lambda api_client: core,
    )

    launcher = SimpleNamespace(
        _api_client=object(),
        _request_timeout=(3, 30),
    )
    launched = kubernetes_launchers.LaunchedKubernetesJob(
        job_name="tangle-ce-execution-1",
        namespace="tangle-jobs",
        output_uris={},
        log_uri="memory://log",
        debug_job=k8s_client_lib.V1Job(
            metadata=k8s_client_lib.V1ObjectMeta(name="tangle-ce-execution-1"),
            spec=k8s_client_lib.V1JobSpec(
                template=k8s_client_lib.V1PodTemplateSpec(
                    spec=k8s_client_lib.V1PodSpec(
                        containers=[k8s_client_lib.V1Container(name="main")]
                    )
                )
            ),
        ),
        launcher=launcher,
    )

    launched.cleanup()

    assert core.deleted == [
        {
            "name": "tangle-ce-execution-1",
            "namespace": "tangle-jobs",
            "grace_period_seconds": 0,
            "propagation_policy": "Background",
            "_request_timeout": (3, 30),
        }
    ]
