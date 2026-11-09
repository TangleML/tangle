"""Tests for choosing a launch's namespace through the launcher's `namespace_chooser`."""

from __future__ import annotations

import importlib
import pathlib
import sys
import types
from unittest import mock

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import common_annotations, kubernetes_launchers

_RETRIES_KEY = kubernetes_launchers.RETRIES_MAX_RETRIES_ANNOTATION_KEY


class _Cluster:
    """Stands in for the Kubernetes API and records the namespace of each Pod and Job."""

    def __init__(self) -> None:
        self.namespaces: list[tuple[str, str]] = []

    def create_namespaced_pod(
        self, *, namespace: str, body: k8s_client_lib.V1Pod, _request_timeout: int
    ) -> k8s_client_lib.V1Pod:
        self.namespaces.append(("Pod", namespace))
        body.metadata.name = "task-pod"
        body.metadata.namespace = namespace
        return body

    def create_namespaced_job(
        self, *, namespace: str, body: k8s_client_lib.V1Job, _request_timeout: int
    ) -> k8s_client_lib.V1Job:
        self.namespaces.append(("Job", namespace))
        body.metadata.namespace = namespace
        body.metadata.uid = "job-uid"
        return body

    def create_namespaced_service(
        self, *, namespace: str, body: k8s_client_lib.V1Service, _request_timeout: int
    ) -> k8s_client_lib.V1Service:
        return body


@pytest.fixture
def cluster(monkeypatch: pytest.MonkeyPatch) -> _Cluster:
    api = _Cluster()
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", lambda api_client: api)
    monkeypatch.setattr(k8s_client_lib, "BatchV1Api", lambda api_client: api)
    monkeypatch.setattr(k8s_client_lib.VersionApi, "get_code", lambda self, **_: None)

    def _no_informer(**kwargs: object) -> None:
        raise RuntimeError("Informers are disabled in tests.")

    monkeypatch.setattr(kubernetes_launchers, "MultiNamespaceInformer", _no_informer)
    return api


def _choose_team_namespace(*, annotations: dict[str, str] | None = None) -> str | None:
    return "team-namespace" if (annotations or {}).get("team") else None


def _launch(
    *,
    launcher: kubernetes_launchers._KubernetesContainerLauncherBase,
    annotations: dict[str, str],
    log_uri: str,
) -> None:
    launcher.launch_container_task(
        component_spec=structures.ComponentSpec(
            name="test",
            implementation=structures.ContainerImplementation(
                container=structures.ContainerSpec(image="alpine")
            ),
        ),
        input_arguments={},
        output_uris={},
        log_uri=log_uri,
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "123",
            **annotations,
        },
    )


@pytest.mark.parametrize(
    "annotations, namespace",
    [
        pytest.param({"team": "yes"}, "team-namespace", id="chooser picks"),
        pytest.param({}, "default", id="chooser declines"),
    ],
)
def test_job_is_created_in_the_namespace_the_chooser_picks(
    annotations: dict[str, str],
    namespace: str,
    cluster: _Cluster,
    tmp_path: pathlib.Path,
) -> None:
    launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
        namespace_chooser=_choose_team_namespace,
    )

    _launch(launcher=launcher, annotations=annotations, log_uri=str(tmp_path / "log"))

    assert cluster.namespaces == [("Job", namespace)]


@pytest.mark.parametrize(
    "annotations, kind",
    [
        pytest.param({"team": "yes"}, "Pod", id="pod"),
        pytest.param({"team": "yes", _RETRIES_KEY: "1"}, "Job", id="job"),
    ],
)
def test_router_hands_the_chooser_to_both_launchers(
    annotations: dict[str, str],
    kind: str,
    cluster: _Cluster,
    tmp_path: pathlib.Path,
) -> None:
    launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
        namespace_chooser=_choose_team_namespace,
    )

    _launch(launcher=launcher, annotations=annotations, log_uri=str(tmp_path / "log"))

    assert cluster.namespaces == [(kind, "team-namespace")]


@pytest.fixture
def google_kubernetes_launchers(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Imports the GKE launchers with a stand-in for google-cloud-storage, which is not installed."""
    import google.cloud

    storage = mock.MagicMock()
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage)
    monkeypatch.setattr(google.cloud, "storage", storage, raising=False)
    for name in [
        "cloud_pipelines.orchestration.storage_providers.google_cloud_storage",
        "cloud_pipelines_backend.launchers.google_kubernetes_launchers",
    ]:
        monkeypatch.setitem(sys.modules, name, None)
        monkeypatch.delitem(sys.modules, name)
    return importlib.import_module(
        "cloud_pipelines_backend.launchers.google_kubernetes_launchers"
    )


@pytest.mark.parametrize(
    "module_name, class_name",
    [
        ("kubernetes_launchers", "KubernetesWithHostPathContainerLauncher"),
        ("kubernetes_launchers", "GoogleKubernetesEngineLauncher"),
        (
            "google_kubernetes_launchers",
            "GoogleKubernetesEngine_UsingGoogleCloudStorage_KubernetesJobLauncher",
        ),
        (
            "google_kubernetes_launchers",
            "GoogleKubernetesEngine_UsingGoogleCloudStorage_KubernetesPodOrJobLauncher",
        ),
    ],
)
def test_launcher_hands_the_chooser_on(
    module_name: str,
    class_name: str,
    cluster: _Cluster,
    google_kubernetes_launchers: types.ModuleType,
    tmp_path: pathlib.Path,
) -> None:
    module = {
        "kubernetes_launchers": kubernetes_launchers,
        "google_kubernetes_launchers": google_kubernetes_launchers,
    }[module_name]
    launcher = getattr(module, class_name)(
        api_client=k8s_client_lib.ApiClient(),
        namespace_chooser=_choose_team_namespace,
    )

    _launch(
        launcher=launcher, annotations={"team": "yes"}, log_uri=str(tmp_path / "log")
    )

    [(_, namespace)] = cluster.namespaces
    assert namespace == "team-namespace"
