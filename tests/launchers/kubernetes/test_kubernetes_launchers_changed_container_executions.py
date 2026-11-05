"""Tests for the Kubernetes launchers reporting which container executions' pods or jobs changed.

The Kubernetes API calls are faked so that the tests run offline.
"""

from __future__ import annotations

import pathlib

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import kubernetes_launchers

_CONTAINER_EXECUTION_ID_KEY = common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY


class _FakeVersionApi:
    def __init__(self, api_client=None):
        pass

    def get_code(self, **kwargs):
        return None


class _FakeCluster:
    """Records the created pods and jobs."""

    def __init__(self):
        self.created_pods: list[k8s_client_lib.V1Pod] = []
        self.created_jobs: list[k8s_client_lib.V1Job] = []

    def make_core_api(self):
        cluster = self

        class _FakeCoreV1Api:
            def __init__(self, api_client=None):
                pass

            def create_namespaced_pod(self, namespace, body, **kwargs):
                cluster.created_pods.append(body)
                body.metadata.name = (body.metadata.generate_name or "") + "abcde"
                body.metadata.namespace = namespace
                return body

            def create_namespaced_service(self, namespace, body, **kwargs):
                return body

        return _FakeCoreV1Api

    def make_batch_api(self):
        cluster = self

        class _FakeBatchV1Api:
            def __init__(self, api_client=None):
                pass

            def create_namespaced_job(self, namespace, body, **kwargs):
                cluster.created_jobs.append(body)
                body.metadata.uid = "job-uid"
                return body

        return _FakeBatchV1Api


class _FakeSharedInformer:
    """Records the event handlers it is given and whether they were added after the cache was prefilled."""

    def __init__(self, **kwargs):
        self.handlers: list[tuple[str, object, bool]] = []
        self.listed = False

    def _initial_list(self):
        self.listed = True

    def add_event_handler(self, event_type, handler):
        self.handlers.append((event_type, handler, self.listed))

    def start(self):
        pass


@pytest.fixture
def cluster(monkeypatch) -> _FakeCluster:
    cluster = _FakeCluster()
    monkeypatch.setattr(k8s_client_lib, "VersionApi", _FakeVersionApi)
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", cluster.make_core_api())
    monkeypatch.setattr(k8s_client_lib, "BatchV1Api", cluster.make_batch_api())

    def _no_informer(**kwargs):
        raise RuntimeError("Informers are disabled in tests.")

    monkeypatch.setattr(kubernetes_launchers, "MultiNamespaceInformer", _no_informer)
    return cluster


def _make_launcher() -> (
    kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher
):
    return kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )


def _launch(launcher, tmp_path: pathlib.Path, annotations: dict[str, str]) -> None:
    launcher.launch_container_task(
        component_spec=structures.ComponentSpec(
            name="test",
            implementation=structures.ContainerImplementation(
                container=structures.ContainerSpec(image="alpine")
            ),
        ),
        input_arguments={},
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
        annotations={_CONTAINER_EXECUTION_ID_KEY: "123", **annotations},
    )


def test_launched_pod_carries_the_container_execution_id(cluster, tmp_path):
    _launch(_make_launcher(), tmp_path, {})

    [pod] = cluster.created_pods
    assert pod.metadata.annotations[_CONTAINER_EXECUTION_ID_KEY] == "123"


def test_launched_job_pod_template_carries_the_container_execution_id(
    cluster, tmp_path
):
    _launch(
        _make_launcher(),
        tmp_path,
        {kubernetes_launchers.RETRIES_MAX_RETRIES_ANNOTATION_KEY: "1"},
    )

    [job] = cluster.created_jobs
    assert job.spec.template.metadata.annotations[_CONTAINER_EXECUTION_ID_KEY] == "123"


def test_launching_does_not_change_the_launcher_pod_annotations(cluster, tmp_path):
    launcher = _make_launcher()

    _launch(launcher, tmp_path, {})

    assert _CONTAINER_EXECUTION_ID_KEY not in launcher._pod_launcher._pod_annotations


def test_reports_each_changed_pod_and_job_once(cluster):
    launcher = _make_launcher()
    launcher._pod_launcher._mark_container_execution_changed(
        k8s_client_lib.V1Pod(
            metadata=k8s_client_lib.V1ObjectMeta(
                annotations={_CONTAINER_EXECUTION_ID_KEY: "pod-execution"}
            )
        )
    )
    launcher._job_launcher._mark_container_execution_changed(
        k8s_client_lib.V1Job(
            metadata=k8s_client_lib.V1ObjectMeta(),
            spec=k8s_client_lib.V1JobSpec(
                template=k8s_client_lib.V1PodTemplateSpec(
                    metadata=k8s_client_lib.V1ObjectMeta(
                        annotations={_CONTAINER_EXECUTION_ID_KEY: "job-execution"}
                    )
                )
            ),
        )
    )

    assert sorted(launcher.pop_changed_container_execution_ids()) == [
        "job-execution",
        "pod-execution",
    ]
    assert launcher.pop_changed_container_execution_ids() == []


def test_ignores_objects_without_a_container_execution_id(cluster):
    launcher = _make_launcher()

    launcher._pod_launcher._mark_container_execution_changed(
        k8s_client_lib.V1Pod(metadata=k8s_client_lib.V1ObjectMeta())
    )

    assert launcher.pop_changed_container_execution_ids() == []


def test_records_changes_before_the_first_call(cluster):
    launcher = _make_launcher()
    launcher._pod_launcher._mark_container_execution_changed(
        k8s_client_lib.V1Pod(
            metadata=k8s_client_lib.V1ObjectMeta(
                annotations={_CONTAINER_EXECUTION_ID_KEY: "pod-execution"}
            )
        )
    )

    assert launcher.pop_changed_container_execution_ids() == ["pod-execution"]


def test_keeps_at_most_the_maximum_number_of_ids_between_calls(cluster, monkeypatch):
    monkeypatch.setattr(kubernetes_launchers, "_MAX_CHANGED_CONTAINER_EXECUTION_IDS", 2)
    launcher = _make_launcher()
    for container_execution_id in ("first", "second", "third"):
        launcher._pod_launcher._mark_container_execution_changed(
            k8s_client_lib.V1Pod(
                metadata=k8s_client_lib.V1ObjectMeta(
                    annotations={_CONTAINER_EXECUTION_ID_KEY: container_execution_id}
                )
            )
        )

    assert sorted(launcher.pop_changed_container_execution_ids()) == [
        "first",
        "second",
    ]


def test_informer_reports_changes_after_prefilling_its_cache(monkeypatch):
    monkeypatch.setattr(
        kubernetes_launchers.k8s_informer_lib, "SharedInformer", _FakeSharedInformer
    )
    on_change = object()

    informer = kubernetes_launchers.MultiNamespaceInformer(
        list_func=None, initial_namespaces=["default"], on_change=on_change
    )

    assert informer._informers["default"].handlers == [
        (kubernetes_launchers.k8s_informer_lib.ADDED, on_change, True),
        (kubernetes_launchers.k8s_informer_lib.MODIFIED, on_change, True),
        (kubernetes_launchers.k8s_informer_lib.DELETED, on_change, True),
    ]


def test_informer_without_on_change_adds_no_handlers(monkeypatch):
    monkeypatch.setattr(
        kubernetes_launchers.k8s_informer_lib, "SharedInformer", _FakeSharedInformer
    )

    informer = kubernetes_launchers.MultiNamespaceInformer(
        list_func=None, initial_namespaces=["default"]
    )

    assert informer._informers["default"].handlers == []
