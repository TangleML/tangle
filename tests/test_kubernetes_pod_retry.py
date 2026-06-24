import datetime
from types import SimpleNamespace

from kubernetes import client as k8s

from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers as k8sL

_OBSERVED_GCSFUSE_SUBPATH_MESSAGE = (
    'failed to prepare subPath for volumeMount "gcsfuse-prd-oasis-tmp" '
    'of container "main"'
)


def _make_pending_pod(
    *,
    waiting_reason: str = "CreateContainerConfigError",
    waiting_message: str = _OBSERVED_GCSFUSE_SUBPATH_MESSAGE,
    creation_timestamp: datetime.datetime | None = None,
    started: bool | None = False,
    restart_count: int = 0,
    last_state: k8s.V1ContainerState | None = None,
    owner_references: list[k8s.V1OwnerReference] | None = None,
) -> k8s.V1Pod:
    creation_timestamp = creation_timestamp or (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=10)
    )
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name="task-abc-old",
            namespace="default",
            uid="old-uid",
            generate_name="task-abc-",
            labels={"app": "oasis"},
            annotations={"cloud-pipelines.net": "true"},
            creation_timestamp=creation_timestamp,
            owner_references=owner_references,
        ),
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="main", image="example/image")],
            node_name="old-node",
        ),
        status=k8s.V1PodStatus(
            phase="Pending",
            container_statuses=[
                k8s.V1ContainerStatus(
                    name="main",
                    image="example/image",
                    image_id="",
                    ready=False,
                    restart_count=restart_count,
                    started=started,
                    last_state=last_state,
                    state=k8s.V1ContainerState(
                        waiting=k8s.V1ContainerStateWaiting(
                            reason=waiting_reason,
                            message=waiting_message,
                        )
                    ),
                )
            ],
        ),
    )


def test_stuck_pod_classifier_matches_observed_gcsfuse_subpath_error():
    classification = k8sL._classify_stuck_unrecoverable_standalone_pod(
        _make_pending_pod()
    )

    assert classification.retryable is True
    assert classification.reason == k8sL._KUBERNETES_STUCK_POD_GCSFUSE_SUBPATH_REASON
    assert "gcsfuse" in (classification.message or "")


def test_stuck_pod_classifier_is_narrowly_allowlisted():
    image_pull = k8sL._classify_stuck_unrecoverable_standalone_pod(
        _make_pending_pod(
            waiting_reason="ImagePullBackOff",
            waiting_message="failed to pull image",
        )
    )
    assert image_pull.retryable is False

    already_started = k8sL._classify_stuck_unrecoverable_standalone_pod(
        _make_pending_pod(started=True)
    )
    assert already_started.retryable is False

    too_young = k8sL._classify_stuck_unrecoverable_standalone_pod(
        _make_pending_pod(
            creation_timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
    )
    assert too_young.retryable is False

    owned_by_job = k8sL._classify_stuck_unrecoverable_standalone_pod(
        _make_pending_pod(
            owner_references=[
                k8s.V1OwnerReference(
                    api_version="batch/v1",
                    kind="Job",
                    name="job",
                    uid="job-uid",
                )
            ]
        )
    )
    assert owned_by_job.retryable is False


def test_get_refreshed_replaces_stuck_standalone_pod_once(monkeypatch):
    old_pod = _make_pending_pod()

    class FakeCoreV1Api:
        def __init__(self):
            self.deleted = []
            self.created_body = None

        def read_namespaced_pod(self, *, name, namespace, _request_timeout):
            assert name == "task-abc-old"
            assert namespace == "default"
            return old_pod

        def delete_namespaced_pod(
            self, *, name, namespace, grace_period_seconds, _request_timeout
        ):
            self.deleted.append(
                dict(
                    name=name,
                    namespace=namespace,
                    grace_period_seconds=grace_period_seconds,
                )
            )

        def create_namespaced_pod(self, *, namespace, body, _request_timeout):
            self.created_body = body
            body.metadata.name = "task-abc-retry"
            body.metadata.namespace = namespace
            body.metadata.uid = "retry-uid"
            body.metadata.creation_timestamp = datetime.datetime.now(
                datetime.timezone.utc
            )
            body.status = k8s.V1PodStatus(phase="Pending")
            return body

    fake_api = FakeCoreV1Api()
    monkeypatch.setattr(k8sL.k8s_client_lib, "CoreV1Api", lambda api_client: fake_api)

    launched = k8sL.LaunchedKubernetesContainer(
        pod_name="task-abc-old",
        namespace="default",
        output_uris={},
        log_uri="gs://logs/log.txt",
        debug_pod=old_pod,
        launcher=SimpleNamespace(_api_client=object(), _request_timeout=10),
    )

    refreshed = launched.get_refreshed()

    assert fake_api.deleted == [
        dict(name="task-abc-old", namespace="default", grace_period_seconds=10)
    ]
    assert fake_api.created_body.metadata.name == "task-abc-retry"
    assert fake_api.created_body.metadata.generate_name == "task-abc-"
    assert fake_api.created_body.spec.node_name is None
    assert fake_api.created_body.status.phase == "Pending"
    assert refreshed.status == interfaces.ContainerStatus.PENDING

    serialized = refreshed.to_dict()["kubernetes"]
    assert serialized["pod_name"] == "task-abc-retry"
    assert serialized[k8sL._KUBERNETES_STUCK_POD_RETRY_METADATA_KEY]["attempts"] == 1
    assert (
        serialized[k8sL._KUBERNETES_STUCK_POD_RETRY_METADATA_KEY]["history"][0][
            "old_pod_name"
        ]
        == "task-abc-old"
    )


def test_retry_metadata_exhaustion_marks_same_stuck_state_error():
    launched = k8sL.LaunchedKubernetesContainer(
        pod_name="task-abc-old",
        namespace="default",
        output_uris={},
        log_uri="gs://logs/log.txt",
        debug_pod=_make_pending_pod(),
        retry_metadata={"attempts": k8sL._KUBERNETES_STUCK_POD_MAX_RETRY_ATTEMPTS},
    )

    assert launched.status == interfaces.ContainerStatus.ERROR
