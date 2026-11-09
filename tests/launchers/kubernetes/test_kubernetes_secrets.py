"""Tests for delivering secret inputs to a container through a Kubernetes Secret."""

from __future__ import annotations

from typing import Final

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend.launchers import (
    common_annotations,
    interfaces,
    kubernetes_launchers,
)

_SECRET_VALUE: Final[str] = "FAKE-tok-123"
_CREATED_BY_ALICE: Final[dict[str, str]] = {
    common_annotations.PIPELINE_RUN_CREATED_BY_ANNOTATION_KEY: "alice@example.com"
}


class _SecretsApi:
    """Stands in for the Kubernetes API and records the Secrets the launcher writes."""

    def __init__(self) -> None:
        self.created: list[tuple[str, k8s_client_lib.V1Secret]] = []
        self.patched: list[tuple[str, k8s_client_lib.V1Secret]] = []
        self.existing: set[str] = set()

    def create_namespaced_secret(
        self, *, namespace: str, body: k8s_client_lib.V1Secret
    ) -> None:
        if body.metadata.name in self.existing:
            raise k8s_client_lib.ApiException(status=409, reason="Conflict")
        self.created.append((namespace, body))

    def patch_namespaced_secret(
        self, *, name: str, namespace: str, body: k8s_client_lib.V1Secret
    ) -> None:
        del namespace
        self.patched.append((name, body))


@pytest.fixture
def secrets_api(monkeypatch: pytest.MonkeyPatch) -> _SecretsApi:
    api = _SecretsApi()
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", lambda *, api_client: api)
    return api


def _make_launcher() -> kubernetes_launchers._KubernetesPodLauncher:
    # Skips __init__, which contacts the cluster; the secret handling reads only these two.
    launcher = object.__new__(kubernetes_launchers._KubernetesPodLauncher)
    launcher._api_client = None
    launcher._namespace = "default"
    return launcher


def _make_secret_argument() -> interfaces.InputArgument:
    return interfaces.InputArgument(
        total_size=len(_SECRET_VALUE),
        is_dir=False,
        value=_SECRET_VALUE,
        staging_uri="gs://bucket/token",
        dynamic_data={"secret": {"name": "hf-token"}},
    )


def _make_pod(*, args: list[str]) -> k8s_client_lib.V1Pod:
    return k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="test-pod", namespace="tangle-jobs"),
        spec=k8s_client_lib.V1PodSpec(
            containers=[k8s_client_lib.V1Container(name="main", args=args)]
        ),
    )


def _launch(
    *,
    launcher: kubernetes_launchers._KubernetesPodLauncher,
    annotations: dict[str, str] | None = None,
) -> k8s_client_lib.V1Pod:
    """Runs both secret phases around a pod whose only argument is the secret input."""
    argument = _make_secret_argument()
    values, env_names = launcher._handle_secrets_before_preparing_pod(
        input_arguments={"token": argument}
    )
    pod = _make_pod(args=[argument.value])
    launcher._handle_secrets_after_preparing_pod(
        pod=pod,
        secret_name_to_secret_value=values,
        secret_env_name_to_secret_name=env_names,
        annotations=annotations,
    )
    return pod


def test_secret_value_is_replaced_by_a_placeholder() -> None:
    argument = _make_secret_argument()

    values, env_names = _make_launcher()._handle_secrets_before_preparing_pod(
        input_arguments={"token": argument}
    )

    assert _SECRET_VALUE not in argument.value
    assert values == {"hf-token": _SECRET_VALUE}
    assert env_names == {"__TANGLE_SECRET_TOKEN": "hf-token"}


def test_inputs_without_secrets_are_left_alone() -> None:
    argument = interfaces.InputArgument(
        total_size=5, is_dir=False, value="hello", staging_uri="gs://bucket/greeting"
    )

    values, env_names = _make_launcher()._handle_secrets_before_preparing_pod(
        input_arguments={"greeting": argument}
    )

    assert (values, env_names) == ({}, {})
    assert argument.value == "hello"


@pytest.mark.parametrize(
    "value, dynamic_data",
    [
        pytest.param(None, {"secret": {"name": "hf-token"}}, id="no value"),
        pytest.param(_SECRET_VALUE, {"secret": {}}, id="no secret name"),
    ],
)
def test_incomplete_secret_argument_is_a_launcher_error(
    value: str | None, dynamic_data: dict[str, dict[str, str]]
) -> None:
    argument = interfaces.InputArgument(
        total_size=0,
        is_dir=False,
        value=value,
        staging_uri="gs://bucket/token",
        dynamic_data=dynamic_data,
    )

    with pytest.raises(interfaces.LauncherError):
        _make_launcher()._handle_secrets_before_preparing_pod(
            input_arguments={"token": argument}
        )


def test_secret_is_created_in_the_pod_namespace_and_named_after_the_run_creator(
    secrets_api: _SecretsApi,
) -> None:
    _launch(launcher=_make_launcher(), annotations=_CREATED_BY_ALICE)

    [(namespace, secret)] = secrets_api.created
    assert namespace == "tangle-jobs"
    assert secret.metadata.name == "alice-at-example-com"


def test_run_without_a_creator_uses_the_anonymous_secret(
    secrets_api: _SecretsApi,
) -> None:
    _launch(launcher=_make_launcher())

    [(_, secret)] = secrets_api.created
    assert secret.metadata.name == "anonymous"


def test_existing_secret_is_patched(secrets_api: _SecretsApi) -> None:
    secrets_api.existing.add("alice-at-example-com")

    _launch(launcher=_make_launcher(), annotations=_CREATED_BY_ALICE)

    assert secrets_api.created == []
    [(name, _)] = secrets_api.patched
    assert name == "alice-at-example-com"


@pytest.mark.usefixtures("secrets_api")
def test_container_reads_the_value_from_the_secret() -> None:
    pod = _launch(launcher=_make_launcher(), annotations=_CREATED_BY_ALICE)

    assert pod.spec.containers[0].env == [
        k8s_client_lib.V1EnvVar(
            name="__TANGLE_SECRET_TOKEN",
            value_from=k8s_client_lib.V1EnvVarSource(
                secret_key_ref=k8s_client_lib.V1SecretKeySelector(
                    name="alice-at-example-com", key="hf-token"
                )
            ),
        )
    ]
    assert _SECRET_VALUE not in pod.spec.containers[0].args[0]
