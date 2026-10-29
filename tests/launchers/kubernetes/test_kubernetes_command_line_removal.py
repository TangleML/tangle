"""Tests that serialized (stored) Kubernetes objects carry no container command lines.

A container that consumes a secret as a value receives that value on its command line, so a
stored command line can hold a secret in plaintext.
"""

from __future__ import annotations

import json
from typing import Final

from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend.launchers import kubernetes_launchers

_SECRET_VALUE: Final[str] = "plaintext-secret-value"


def _make_container() -> k8s_client_lib.V1Container:
    return k8s_client_lib.V1Container(
        name="main",
        image="alpine",
        command=["python3", "-c", "print(1)"],
        args=["--token", _SECRET_VALUE],
    )


def _make_pod() -> k8s_client_lib.V1Pod:
    return k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="test-pod", namespace="test"),
        spec=k8s_client_lib.V1PodSpec(containers=[_make_container()]),
    )


def _make_job() -> k8s_client_lib.V1Job:
    return k8s_client_lib.V1Job(
        metadata=k8s_client_lib.V1ObjectMeta(name="test-job", namespace="test"),
        spec=k8s_client_lib.V1JobSpec(
            template=k8s_client_lib.V1PodTemplateSpec(
                spec=k8s_client_lib.V1PodSpec(containers=[_make_container()]),
            ),
        ),
    )


def test_serialized_pod_has_no_command_line() -> None:
    pod_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(
        _make_pod()
    )

    container = pod_dict["spec"]["containers"][0]
    assert "command" not in container
    assert "args" not in container
    assert container["image"] == "alpine"
    assert _SECRET_VALUE not in json.dumps(pod_dict)


def test_serialized_job_has_no_command_line_on_its_pod_template() -> None:
    job_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(
        _make_job()
    )

    container = job_dict["spec"]["template"]["spec"]["containers"][0]
    assert "command" not in container
    assert "args" not in container
    assert _SECRET_VALUE not in json.dumps(job_dict)


def test_serialized_pod_has_no_command_line_anywhere() -> None:
    """The removal is not selective: every command line goes, whatever holds it.

    An init container, a sidecar and a probe are all stored, and any of them can be the one
    carrying a secret, so none is left behind.
    """
    pod = k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="test-pod", namespace="test"),
        spec=k8s_client_lib.V1PodSpec(
            init_containers=[
                k8s_client_lib.V1Container(
                    name="init",
                    image="alpine",
                    command=["sh", "-c", _SECRET_VALUE],
                ),
            ],
            containers=[
                _make_container(),
                k8s_client_lib.V1Container(
                    name="sidecar",
                    image="alpine",
                    args=["--token", _SECRET_VALUE],
                    readiness_probe=k8s_client_lib.V1Probe(
                        _exec=k8s_client_lib.V1ExecAction(
                            command=["check", _SECRET_VALUE],
                        ),
                    ),
                ),
            ],
        ),
    )

    pod_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(pod)

    assert "command" not in pod_dict["spec"]["initContainers"][0]
    sidecar = pod_dict["spec"]["containers"][1]
    assert "args" not in sidecar
    assert "command" not in sidecar["readinessProbe"]["exec"]
    assert _SECRET_VALUE not in json.dumps(pod_dict)


def test_launched_pod_keeps_its_command_line() -> None:
    """The object sent to the cluster must be untouched: it is how the container is started."""
    pod = _make_pod()

    kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(pod)

    assert pod.spec.containers[0].args == ["--token", _SECRET_VALUE]
