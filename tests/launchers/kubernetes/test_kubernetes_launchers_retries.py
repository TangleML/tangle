"""Tests for the native Kubernetes retries (`tangleml.com/launchers/generic/retries.max_retries`).

The Kubernetes API calls are faked so that the tests run offline.
"""

from __future__ import annotations

import datetime
import logging
import pathlib
import typing

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers

_RETRIES_KEY = kubernetes_launchers.RETRIES_MAX_RETRIES_ANNOTATION_KEY
_NUM_NODES_KEY = kubernetes_launchers.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY
_COMPLETION_INDEX_KEY = "batch.kubernetes.io/job-completion-index"


class _FakeVersionApi:
    def __init__(self, api_client=None):
        pass

    def get_code(self, **kwargs):
        return None


class _FakeLogResponse:
    def __init__(self, text: str):
        self.data = text.encode("utf-8")

    def release_conn(self):
        pass


class _FakeCluster:
    """Records created objects and serves pods/logs."""

    def __init__(self):
        self.created_jobs: list[k8s_client_lib.V1Job] = []
        self.created_pods: list[k8s_client_lib.V1Pod] = []
        self.created_services: list[k8s_client_lib.V1Service] = []
        self.pods: list[k8s_client_lib.V1Pod] = []
        self.pod_logs: dict[str, str] = {}

    def make_batch_api(self):
        cluster = self

        class _FakeBatchV1Api:
            def __init__(self, api_client=None):
                pass

            def create_namespaced_job(self, namespace, body, **kwargs):
                cluster.created_jobs.append(body)
                body.metadata.uid = "job-uid"
                return body

            def delete_namespaced_job(self, **kwargs):
                pass

        return _FakeBatchV1Api

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
                cluster.created_services.append(body)
                return body

            def list_namespaced_pod(self, namespace, label_selector=None, **kwargs):
                return k8s_client_lib.V1PodList(items=list(cluster.pods))

            def read_namespaced_pod_log(self, name, **kwargs):
                if name not in cluster.pod_logs:
                    raise k8s_client_lib.exceptions.ApiException(
                        status=404, reason="Not Found"
                    )
                return _FakeLogResponse(cluster.pod_logs[name])

        return _FakeCoreV1Api


@pytest.fixture
def cluster(monkeypatch) -> _FakeCluster:
    cluster = _FakeCluster()
    monkeypatch.setattr(k8s_client_lib, "VersionApi", _FakeVersionApi)
    monkeypatch.setattr(k8s_client_lib, "BatchV1Api", cluster.make_batch_api())
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", cluster.make_core_api())

    def _no_informer(**kwargs):
        raise RuntimeError("Informers are disabled in tests.")

    # Launchers fall back to direct API calls when the informers cannot be started.
    monkeypatch.setattr(kubernetes_launchers, "MultiNamespaceInformer", _no_informer)
    return cluster


def _make_component_spec() -> structures.ComponentSpec:
    return structures.ComponentSpec(
        name="test",
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(
                image="alpine",
                command=["echo", "hello"],
            )
        ),
    )


def _launch(launcher, annotations: dict[str, typing.Any], tmp_path: pathlib.Path):
    return launcher.launch_container_task(
        component_spec=_make_component_spec(),
        input_arguments={},
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "123",
            **annotations,
        },
    )


def _make_job_launcher():
    return kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )


# Validation


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, 0),
        ("0", 0),
        ("1", 1),
        ("5", 5),
        (3, 3),
        (" 2 ", 2),
    ],
)
def test_get_max_retries_valid(value, expected):
    annotations = {} if value is None else {_RETRIES_KEY: value}
    assert common_annotations.get_max_retries(annotations) == expected


def test_get_max_retries_no_annotations():
    assert common_annotations.get_max_retries(None) == 0


@pytest.mark.parametrize(
    "value", ["-1", "6", "100", "abc", "", "1.5", 1.0, True, "true"]
)
def test_get_max_retries_invalid_fails_closed(value):
    with pytest.raises(interfaces.LauncherError, match="retries.max_retries"):
        common_annotations.get_max_retries({_RETRIES_KEY: value})


# Job launcher


def test_job_launcher_without_retries_keeps_previous_job_spec(cluster, tmp_path):
    _launch(_make_job_launcher(), {}, tmp_path)
    [job] = cluster.created_jobs
    assert job.spec.backoff_limit_per_index == 0
    assert job.spec.max_failed_indexes == 0
    assert job.spec.backoff_limit is None
    assert job.spec.completion_mode == "Indexed"


@pytest.mark.parametrize("value,expected", [("0", 0), ("3", 3), (5, 5)])
def test_job_launcher_sets_backoff_limit_per_index(cluster, tmp_path, value, expected):
    _launch(_make_job_launcher(), {_RETRIES_KEY: value}, tmp_path)
    [job] = cluster.created_jobs
    assert job.spec.backoff_limit_per_index == expected
    # A failed index must still fail the whole Job once its retries are exhausted.
    assert job.spec.max_failed_indexes == 0
    # Disruptions are still ignored (not charged against the retry budget).
    [rule] = job.spec.pod_failure_policy.rules
    assert rule.action == "Ignore"
    [condition] = rule.on_pod_conditions
    assert condition.type == "DisruptionTarget"
    assert condition.status == "True"


def test_job_launcher_invalid_retries_fails_before_creating_job(cluster, tmp_path):
    with pytest.raises(interfaces.LauncherError):
        _launch(_make_job_launcher(), {_RETRIES_KEY: "6"}, tmp_path)
    assert cluster.created_jobs == []


def test_job_launcher_rejects_retries_for_multi_node(cluster, tmp_path):
    with pytest.raises(interfaces.LauncherError, match="multi-node"):
        _launch(
            _make_job_launcher(),
            {_RETRIES_KEY: "2", _NUM_NODES_KEY: "2"},
            tmp_path,
        )
    assert cluster.created_jobs == []
    assert cluster.created_services == []


def test_job_launcher_allows_zero_retries_for_multi_node(cluster, tmp_path):
    _launch(
        _make_job_launcher(),
        {_RETRIES_KEY: "0", _NUM_NODES_KEY: "2"},
        tmp_path,
    )
    [job] = cluster.created_jobs
    assert job.spec.backoff_limit_per_index == 0
    assert job.spec.completions == 2


def test_job_launcher_allows_retries_for_single_node_annotation(cluster, tmp_path):
    _launch(
        _make_job_launcher(),
        {_RETRIES_KEY: "2", _NUM_NODES_KEY: "1"},
        tmp_path,
    )
    [job] = cluster.created_jobs
    assert job.spec.backoff_limit_per_index == 2


# Pod-or-Job router and Pod launcher


def _make_pod_or_job_launcher():
    return kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )


def test_router_without_retries_launches_pod(cluster, tmp_path):
    launched = _launch(_make_pod_or_job_launcher(), {}, tmp_path)
    assert isinstance(launched, kubernetes_launchers.LaunchedKubernetesContainer)
    assert len(cluster.created_pods) == 1
    assert cluster.created_jobs == []


def test_router_with_zero_retries_launches_pod(cluster, tmp_path):
    launched = _launch(_make_pod_or_job_launcher(), {_RETRIES_KEY: "0"}, tmp_path)
    assert isinstance(launched, kubernetes_launchers.LaunchedKubernetesContainer)
    assert cluster.created_jobs == []


def test_router_with_retries_forces_job(cluster, tmp_path):
    launched = _launch(_make_pod_or_job_launcher(), {_RETRIES_KEY: "2"}, tmp_path)
    assert isinstance(launched, kubernetes_launchers.LaunchedKubernetesJob)
    assert cluster.created_pods == []
    [job] = cluster.created_jobs
    assert job.spec.backoff_limit_per_index == 2


def test_router_with_invalid_retries_fails_closed(cluster, tmp_path):
    with pytest.raises(interfaces.LauncherError):
        _launch(_make_pod_or_job_launcher(), {_RETRIES_KEY: "nope"}, tmp_path)
    assert cluster.created_pods == []
    assert cluster.created_jobs == []


def test_pod_launcher_warns_and_ignores_retries(cluster, tmp_path, caplog):
    launcher = kubernetes_launchers.KubernetesWithHostPathContainerLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )
    with caplog.at_level(logging.WARNING, logger=kubernetes_launchers.__name__):
        launched = _launch(launcher, {_RETRIES_KEY: "3"}, tmp_path)
    assert isinstance(launched, kubernetes_launchers.LaunchedKubernetesContainer)
    [pod] = cluster.created_pods
    assert pod.spec.restart_policy == "Never"
    assert any(
        _RETRIES_KEY in record.getMessage() and "ignored" in record.getMessage()
        for record in caplog.records
    )


def test_pod_launcher_invalid_retries_fails_closed(cluster, tmp_path):
    launcher = kubernetes_launchers.KubernetesWithHostPathContainerLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )
    with pytest.raises(interfaces.LauncherError):
        _launch(launcher, {_RETRIES_KEY: "-1"}, tmp_path)
    assert cluster.created_pods == []


# Logs of all attempts


_T0 = datetime.datetime(2026, 10, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)


def _make_pod(
    name: str, index: str, created_at: datetime.datetime, exit_code: int
) -> k8s_client_lib.V1Pod:
    return k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(
            name=name,
            annotations={_COMPLETION_INDEX_KEY: index},
            creation_timestamp=created_at,
        ),
        status=k8s_client_lib.V1PodStatus(
            phase="Failed" if exit_code else "Succeeded",
            container_statuses=[
                k8s_client_lib.V1ContainerStatus(
                    name="main",
                    image="alpine",
                    image_id="",
                    ready=False,
                    restart_count=0,
                    state=k8s_client_lib.V1ContainerState(
                        terminated=k8s_client_lib.V1ContainerStateTerminated(
                            exit_code=exit_code
                        )
                    ),
                )
            ],
        ),
    )


def _make_failed_launched_job(
    cluster: _FakeCluster,
    tmp_path: pathlib.Path,
    latest_pods: dict[str, k8s_client_lib.V1Pod],
    completions: int = 1,
    backoff_limit_per_index: int = 2,
) -> kubernetes_launchers.LaunchedKubernetesJob:
    job = k8s_client_lib.V1Job(
        metadata=k8s_client_lib.V1ObjectMeta(name="tangle-ce-123", namespace="default"),
        spec=k8s_client_lib.V1JobSpec(
            template=k8s_client_lib.V1PodTemplateSpec(),
            completion_mode="Indexed",
            completions=completions,
            backoff_limit_per_index=backoff_limit_per_index,
        ),
        status=k8s_client_lib.V1JobStatus(
            failed=3,
            conditions=[
                k8s_client_lib.V1JobCondition(type="Failed", status="True"),
            ],
        ),
    )
    return kubernetes_launchers.LaunchedKubernetesJob(
        job_name="tangle-ce-123",
        namespace="default",
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
        debug_job=job,
        debug_pods=latest_pods,
        launcher=_make_job_launcher(),
    )


def _set_up_three_attempts(cluster: _FakeCluster):
    pods = [
        _make_pod("tangle-ce-123-0-aaaaa", "0", _T0, exit_code=1),
        _make_pod("tangle-ce-123-0-bbbbb", "0", _T0 + datetime.timedelta(minutes=1), 2),
        _make_pod("tangle-ce-123-0-ccccc", "0", _T0 + datetime.timedelta(minutes=2), 3),
    ]
    # Listing order is not guaranteed to be chronological.
    cluster.pods = [pods[1], pods[2], pods[0]]
    cluster.pod_logs = {
        "tangle-ce-123-0-aaaaa": "2026-10-07T12:00:01.000000000Z attempt one\n",
        "tangle-ce-123-0-bbbbb": "2026-10-07T12:01:01.000000000Z attempt two\n",
        "tangle-ce-123-0-ccccc": "2026-10-07T12:02:01.000000000Z attempt three\n",
    }
    return pods


def test_upload_log_keeps_logs_of_all_attempts(cluster, tmp_path):
    pods = _set_up_three_attempts(cluster)
    launched = _make_failed_launched_job(cluster, tmp_path, {"0": pods[2]})

    launched.upload_log()

    expected_log = (
        "2026-10-07T12:00:00.000000000Z ========== Attempt 1 of 3 (Pod tangle-ce-123-0-aaaaa) ==========\n"
        "2026-10-07T12:00:01.000000000Z attempt one\n"
        "2026-10-07T12:01:00.000000000Z ========== Attempt 2 of 3 (Pod tangle-ce-123-0-bbbbb) ==========\n"
        "2026-10-07T12:01:01.000000000Z attempt two\n"
        "2026-10-07T12:02:00.000000000Z ========== Attempt 3 of 3 (Pod tangle-ce-123-0-ccccc) ==========\n"
        "2026-10-07T12:02:01.000000000Z attempt three\n"
    )
    assert (tmp_path / "log.txt").read_text() == expected_log
    assert (tmp_path / "log.txt.0").read_text() == expected_log
    # The exit code reflects the final attempt.
    assert launched.exit_code == 3


def test_upload_log_marks_missing_attempt_logs(cluster, tmp_path):
    pods = _set_up_three_attempts(cluster)
    # The first attempt's Pod log is gone.
    del cluster.pod_logs["tangle-ce-123-0-aaaaa"]
    launched = _make_failed_launched_job(cluster, tmp_path, {"0": pods[2]})

    log = launched.get_log()

    assert log.startswith(
        "2026-10-07T12:00:00.000000000Z ========== Attempt 1 of 3 (Pod tangle-ce-123-0-aaaaa) ========== (log is not available)\n"
        "2026-10-07T12:01:00.000000000Z ========== Attempt 2 of 3 (Pod tangle-ce-123-0-bbbbb) ==========\n"
    )
    assert "attempt three" in log


def test_upload_log_single_attempt_is_unchanged(cluster, tmp_path):
    pod = _make_pod("tangle-ce-123-0-aaaaa", "0", _T0, exit_code=1)
    cluster.pods = [pod]
    cluster.pod_logs = {
        "tangle-ce-123-0-aaaaa": "2026-10-07T12:00:01.000000000Z only attempt\n",
    }
    launched = _make_failed_launched_job(cluster, tmp_path, {"0": pod})

    launched.upload_log()

    assert (
        tmp_path / "log.txt"
    ).read_text() == "2026-10-07T12:00:01.000000000Z only attempt\n"
    assert launched.exit_code == 1


def test_get_log_without_retries_only_collects_latest_attempts(cluster, tmp_path):
    # Multiple pods per index can also appear without retries (e.g. disruptions or suspend/resume).
    # Without retries, only the latest Pods are used (the previous behavior).
    pods = _set_up_three_attempts(cluster)
    launched = _make_failed_launched_job(
        cluster, tmp_path, {"0": pods[2]}, backoff_limit_per_index=0
    )

    assert launched.get_log() == "2026-10-07T12:02:01.000000000Z attempt three\n"


def test_attempt_pods_prefer_fresh_listed_pod_state(cluster, tmp_path):
    pods = _set_up_three_attempts(cluster)
    stale_latest_pod = _make_pod(
        "tangle-ce-123-0-ccccc", "0", _T0 + datetime.timedelta(minutes=2), 0
    )
    launched = _make_failed_launched_job(cluster, tmp_path, {"0": stale_latest_pod})

    attempt_pods = launched._get_all_attempt_pods()

    assert list(attempt_pods) == ["0"]
    assert [pod.metadata.name for pod in attempt_pods["0"]] == [
        "tangle-ce-123-0-aaaaa",
        "tangle-ce-123-0-bbbbb",
        "tangle-ce-123-0-ccccc",
    ]
    # The deduplicated latest Pod is the freshly listed one.
    assert attempt_pods["0"][2] is pods[2]


def test_get_log_falls_back_to_latest_attempts_when_listing_fails(
    cluster, tmp_path, monkeypatch
):
    pods = _set_up_three_attempts(cluster)
    launched = _make_failed_launched_job(cluster, tmp_path, {"0": pods[2]})

    def _fail(*args, **kwargs):
        raise k8s_client_lib.exceptions.ApiException(status=500, reason="Boom")

    monkeypatch.setattr(
        k8s_client_lib.CoreV1Api, "list_namespaced_pod", _fail, raising=True
    )

    assert launched.get_log() == "2026-10-07T12:02:01.000000000Z attempt three\n"


def test_multi_index_merge_keeps_attempt_headers_in_order(cluster, tmp_path):
    index_0_old = _make_pod("tangle-ce-123-0-aaaaa", "0", _T0, exit_code=1)
    index_0_new = _make_pod(
        "tangle-ce-123-0-bbbbb", "0", _T0 + datetime.timedelta(minutes=1), 1
    )
    index_1 = _make_pod("tangle-ce-123-1-ccccc", "1", _T0, exit_code=0)
    cluster.pods = [index_0_old, index_0_new, index_1]
    cluster.pod_logs = {
        "tangle-ce-123-0-aaaaa": "2026-10-07T12:00:01.000000000Z zero old\n",
        "tangle-ce-123-0-bbbbb": "2026-10-07T12:01:01.000000000Z zero new\n",
        "tangle-ce-123-1-ccccc": "2026-10-07T12:00:30.000000000Z one\n",
    }
    launched = _make_failed_launched_job(
        cluster, tmp_path, {"0": index_0_new, "1": index_1}, completions=2
    )

    # Skipping the empty lines (which have no timestamp).
    log_lines = [
        line for line in launched.get_log().split("\n") if line.partition(" ")[0]
    ]

    assert log_lines == [
        "2026-10-07T12:00:00.000000000Z 0 ========== Attempt 1 of 2 (Pod tangle-ce-123-0-aaaaa) ==========",
        "2026-10-07T12:00:01.000000000Z 0 zero old",
        "2026-10-07T12:00:30.000000000Z 1 one",
        "2026-10-07T12:01:00.000000000Z 0 ========== Attempt 2 of 2 (Pod tangle-ce-123-0-bbbbb) ==========",
        "2026-10-07T12:01:01.000000000Z 0 zero new",
    ]
