"""Tests for cloud_pipelines_backend.launchers.kubernetes_launchers.

These stay offline: they exercise the job_postprocessor plumbing on the base
launcher classes directly, without a real Kubernetes API client or the GKE
launchers (which need google.cloud.storage).
"""

from __future__ import annotations

from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend.launchers import kubernetes_launchers


def _job() -> k8s_client_lib.V1Job:
    return k8s_client_lib.V1Job(
        spec=k8s_client_lib.V1JobSpec(template=k8s_client_lib.V1PodTemplateSpec())
    )


def _job_launcher_with_postprocessor(
    job_postprocessor: kubernetes_launchers.JobPostProcessor | None,
) -> kubernetes_launchers._KubernetesJobLauncher:
    # Bypass __init__ (needs an api_client / storage provider); we only exercise
    # the transform hook, which reads self._job_postprocessor.
    launcher = object.__new__(kubernetes_launchers._KubernetesJobLauncher)
    launcher._job_postprocessor = job_postprocessor
    return launcher


def test_transform_job_before_launching_applies_job_postprocessor():
    def set_ttl(*, job: k8s_client_lib.V1Job, annotations=None) -> k8s_client_lib.V1Job:
        job.spec.ttl_seconds_after_finished = 604800
        return job

    launcher = _job_launcher_with_postprocessor(set_ttl)

    result = launcher._transform_job_before_launching(job=_job(), annotations={})

    assert result.spec.ttl_seconds_after_finished == 604800


def test_transform_job_before_launching_is_noop_without_postprocessor():
    launcher = _job_launcher_with_postprocessor(None)
    job = _job()

    result = launcher._transform_job_before_launching(job=job, annotations={})

    assert result is job
    assert result.spec.ttl_seconds_after_finished is None


def test_pod_or_job_launcher_forwards_job_postprocessor(monkeypatch):
    captured: dict = {}

    class _StubPodLauncher:
        def __init__(self, **kwargs):
            pass

    class _StubJobLauncher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(
        kubernetes_launchers, "_KubernetesPodLauncher", _StubPodLauncher
    )
    monkeypatch.setattr(
        kubernetes_launchers, "_KubernetesJobLauncher", _StubJobLauncher
    )

    def job_postprocessor(
        *, job: k8s_client_lib.V1Job, annotations=None
    ) -> k8s_client_lib.V1Job:
        return job

    kubernetes_launchers._KubernetesPodOrJobLauncher(
        api_client=None,
        job_postprocessor=job_postprocessor,
        _storage_provider=None,
        _create_volume_and_volume_mount=None,
    )

    assert captured["job_postprocessor"] is job_postprocessor
