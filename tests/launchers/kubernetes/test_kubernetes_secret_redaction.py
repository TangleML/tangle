"""Tests that secret argument values do not reach the serialized (stored) launcher data.

The launched Pod must keep the real value — that is how the container receives it — while
every serialized copy of that Pod or Job replaces it with a placeholder.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any, Final

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers

_SECRET_VALUE: Final[str] = "plaintext-secret-value"
_SECRET_VALUE_A: Final[str] = "plaintext-secret-value-a"
_SECRET_VALUE_B: Final[str] = "plaintext-secret-value-b"
_PLAIN_VALUE: Final[str] = "plain-argument-value"
_REDACTED: Final[str] = "<REDACTED_SECRET_VALUE>"


class _FakeVersionApi:
    def __init__(self, api_client: Any = None) -> None:
        pass

    def get_code(self, **kwargs: Any) -> None:
        return None


@pytest.fixture(autouse=True)
def offline_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lets the launcher be constructed without a cluster; no API call is made by these tests."""
    monkeypatch.setattr(k8s_client_lib, "VersionApi", _FakeVersionApi)

    def _no_informer(**kwargs: Any) -> None:
        raise RuntimeError("Informers are disabled in tests.")

    monkeypatch.setattr(kubernetes_launchers, "MultiNamespaceInformer", _no_informer)


def _make_component_spec() -> structures.ComponentSpec:
    return structures.ComponentSpec(
        name="test",
        inputs=[
            structures.InputSpec(name="plain"),
            structures.InputSpec(name="token"),
        ],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(
                image="alpine",
                command=["echo"],
                args=[
                    structures.InputValuePlaceholder(input_name="plain"),
                    structures.InputValuePlaceholder(input_name="token"),
                ],
            )
        ),
    )


def _make_multi_secret_component_spec() -> structures.ComponentSpec:
    """A component whose `command` and `args` both carry secret values.

    `token_a` is referenced twice, under two input names holding the same value, so the
    by-value detection has to blank both occurrences.
    """
    return structures.ComponentSpec(
        name="test-multi",
        inputs=[
            structures.InputSpec(name="plain"),
            structures.InputSpec(name="token_a"),
            structures.InputSpec(name="token_b"),
            structures.InputSpec(name="token_a_again"),
        ],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(
                image="alpine",
                command=[
                    "sh",
                    "-c",
                    'echo "$@"',
                    structures.InputValuePlaceholder(input_name="token_a"),
                ],
                args=[
                    structures.InputValuePlaceholder(input_name="plain"),
                    structures.InputValuePlaceholder(input_name="token_b"),
                    "--plain-flag",
                    structures.InputValuePlaceholder(input_name="token_a_again"),
                ],
            )
        ),
    )


def _make_input_argument(
    value: str, *, dynamic_data: dict | None = None
) -> interfaces.InputArgument:
    return interfaces.InputArgument(
        total_size=len(value.encode("utf-8")),
        is_dir=False,
        value=value,
        staging_uri="memory://staging",
        dynamic_data=dynamic_data,
    )


def _prepare_pod(tmp_path: pathlib.Path) -> k8s_client_lib.V1Pod:
    launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )
    return launcher._pod_launcher._prepare_kubernetes_pod(
        component_spec=_make_component_spec(),
        input_arguments={
            "plain": _make_input_argument(_PLAIN_VALUE),
            "token": _make_input_argument(
                _SECRET_VALUE, dynamic_data={"secret": {"name": "my-secret"}}
            ),
        },
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
    )


def _prepare_multi_secret_pod(tmp_path: pathlib.Path) -> k8s_client_lib.V1Pod:
    launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )
    return launcher._pod_launcher._prepare_kubernetes_pod(
        component_spec=_make_multi_secret_component_spec(),
        input_arguments={
            "plain": _make_input_argument(_PLAIN_VALUE),
            "token_a": _make_input_argument(
                _SECRET_VALUE_A, dynamic_data={"secret": {"name": "secret-a"}}
            ),
            "token_b": _make_input_argument(
                _SECRET_VALUE_B, dynamic_data={"secret": {"name": "secret-b"}}
            ),
            "token_a_again": _make_input_argument(
                _SECRET_VALUE_A, dynamic_data={"secret": {"name": "secret-a"}}
            ),
        },
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
    )


def _wrap_pod_in_job(pod: k8s_client_lib.V1Pod) -> k8s_client_lib.V1Job:
    return k8s_client_lib.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_client_lib.V1ObjectMeta(name="test-job"),
        spec=k8s_client_lib.V1JobSpec(
            template=k8s_client_lib.V1PodTemplateSpec(
                metadata=pod.metadata, spec=pod.spec
            ),
        ),
    )


def _redaction_annotation(pod: k8s_client_lib.V1Pod) -> dict[str, list[int]]:
    return json.loads(
        pod.metadata.annotations[
            kubernetes_launchers.REDACTED_COMMAND_LINE_PARTS_ANNOTATION_KEY
        ]
    )


def test_launched_pod_keeps_the_secret_value(tmp_path: pathlib.Path) -> None:
    pod = _prepare_pod(tmp_path)

    args = pod.spec.containers[0].args
    assert args == [_PLAIN_VALUE, _SECRET_VALUE]


def test_serialized_pod_redacts_only_the_secret_part(tmp_path: pathlib.Path) -> None:
    pod = _prepare_pod(tmp_path)

    pod_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(pod)

    assert pod_dict["spec"]["containers"][0]["args"] == [
        _PLAIN_VALUE,
        _REDACTED,
    ]
    assert _SECRET_VALUE not in str(pod_dict)


def test_annotation_omits_the_field_that_holds_no_secret(
    tmp_path: pathlib.Path,
) -> None:
    pod = _prepare_pod(tmp_path)

    # `command` is ["echo"], so only `args` is listed.
    assert _redaction_annotation(pod) == {"args": [1]}


def test_multiple_secrets_in_command_and_args_are_all_redacted(
    tmp_path: pathlib.Path,
) -> None:
    pod = _prepare_multi_secret_pod(tmp_path)
    container = pod.spec.containers[0]

    # The launched Pod carries every real value, including the repeated one.
    assert container.command == ["sh", "-c", 'echo "$@"', _SECRET_VALUE_A]
    assert container.args == [
        _PLAIN_VALUE,
        _SECRET_VALUE_B,
        "--plain-flag",
        _SECRET_VALUE_A,
    ]
    assert _redaction_annotation(pod) == {"command": [3], "args": [1, 3]}

    pod_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(pod)

    serialized_container = pod_dict["spec"]["containers"][0]
    assert serialized_container["command"] == ["sh", "-c", 'echo "$@"', _REDACTED]
    assert serialized_container["args"] == [
        _PLAIN_VALUE,
        _REDACTED,
        "--plain-flag",
        _REDACTED,
    ]
    assert _SECRET_VALUE_A not in str(pod_dict)
    assert _SECRET_VALUE_B not in str(pod_dict)


def test_serialized_job_redacts_multiple_secrets_in_the_pod_template(
    tmp_path: pathlib.Path,
) -> None:
    pod = _prepare_multi_secret_pod(tmp_path)

    job_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(
        _wrap_pod_in_job(pod)
    )

    container = job_dict["spec"]["template"]["spec"]["containers"][0]
    assert container["command"] == ["sh", "-c", 'echo "$@"', _REDACTED]
    assert container["args"] == [_PLAIN_VALUE, _REDACTED, "--plain-flag", _REDACTED]
    assert _SECRET_VALUE_A not in str(job_dict)
    assert _SECRET_VALUE_B not in str(job_dict)


def test_serialized_job_redacts_the_pod_template(tmp_path: pathlib.Path) -> None:
    pod = _prepare_pod(tmp_path)
    job = k8s_client_lib.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=k8s_client_lib.V1ObjectMeta(name="test-job"),
        spec=k8s_client_lib.V1JobSpec(
            template=k8s_client_lib.V1PodTemplateSpec(
                metadata=pod.metadata, spec=pod.spec
            ),
        ),
    )

    job_dict = kubernetes_launchers._serialize_kubernetes_object_to_compact_dict(job)

    containers = job_dict["spec"]["template"]["spec"]["containers"]
    assert containers[0]["args"] == [_PLAIN_VALUE, _REDACTED]
    assert _SECRET_VALUE not in str(job_dict)


def test_a_pod_without_secrets_is_not_annotated(tmp_path: pathlib.Path) -> None:
    launcher = kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesPodOrJobLauncher(
        api_client=k8s_client_lib.ApiClient(),
    )
    pod = launcher._pod_launcher._prepare_kubernetes_pod(
        component_spec=_make_component_spec(),
        input_arguments={
            "plain": _make_input_argument(_PLAIN_VALUE),
            "token": _make_input_argument(_PLAIN_VALUE),
        },
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
    )

    annotations = pod.metadata.annotations or {}
    assert (
        kubernetes_launchers.REDACTED_COMMAND_LINE_PARTS_ANNOTATION_KEY
        not in annotations
    )
