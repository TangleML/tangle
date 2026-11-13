"""Tests for delivering secret inputs to a container through a Kubernetes Secret."""

from __future__ import annotations

import pathlib
from typing import Final

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import (
    common_annotations,
    interfaces,
    kubernetes_launchers,
)

_SECRET_VALUE: Final[str] = "FAKE-tok-123"
_EXECUTION_123: Final[dict[str, str]] = {
    common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "123"
}


class _SecretsApi:
    """Stands in for the Kubernetes API and records the Secrets and Pods the launcher writes."""

    def __init__(self) -> None:
        self.created: list[tuple[str, k8s_client_lib.V1Secret]] = []
        self.deleted_pods: list[str] = []
        self.fail_secrets = False

    def create_namespaced_secret(
        self, *, namespace: str, body: k8s_client_lib.V1Secret, _request_timeout: int
    ) -> None:
        if self.fail_secrets:
            raise k8s_client_lib.ApiException(
                status=500, reason="Internal Server Error"
            )
        self.created.append((namespace, body))

    def create_namespaced_pod(
        self, *, namespace: str, body: k8s_client_lib.V1Pod, _request_timeout: int
    ) -> k8s_client_lib.V1Pod:
        body.metadata.name = "task-pod"
        body.metadata.namespace = namespace
        body.metadata.uid = "pod-uid"
        return body

    def delete_namespaced_pod(
        self, *, name: str, namespace: str, grace_period_seconds: int
    ) -> None:
        self.deleted_pods.append(name)

    def create_namespaced_service(
        self, *, namespace: str, body: k8s_client_lib.V1Service, _request_timeout: int
    ) -> k8s_client_lib.V1Service:
        return body


class _JobsApi:
    """Stands in for the Kubernetes batch API and records the Jobs the launcher creates."""

    def __init__(self) -> None:
        self.created: list[k8s_client_lib.V1Job] = []
        self.deleted: list[str] = []

    def create_namespaced_job(
        self, *, namespace: str, body: k8s_client_lib.V1Job, _request_timeout: int
    ) -> k8s_client_lib.V1Job:
        body.metadata.namespace = namespace
        body.metadata.uid = "job-uid"
        self.created.append(body)
        return body

    def delete_namespaced_job(
        self,
        *,
        name: str,
        namespace: str,
        grace_period_seconds: int,
        propagation_policy: str,
    ) -> None:
        self.deleted.append(name)


@pytest.fixture
def secrets_api(monkeypatch: pytest.MonkeyPatch) -> _SecretsApi:
    api = _SecretsApi()
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", lambda api_client: api)
    monkeypatch.setattr(k8s_client_lib.VersionApi, "get_code", lambda self, **_: None)

    def _no_informer(**kwargs: object) -> None:
        raise RuntimeError("Informers are disabled in tests.")

    monkeypatch.setattr(kubernetes_launchers, "MultiNamespaceInformer", _no_informer)
    return api


@pytest.fixture
def jobs_api(monkeypatch: pytest.MonkeyPatch) -> _JobsApi:
    api = _JobsApi()
    monkeypatch.setattr(k8s_client_lib, "BatchV1Api", lambda api_client: api)
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


@pytest.mark.usefixtures("secrets_api")
def test_container_reads_the_value_from_the_secret() -> None:
    pod = _launch(launcher=_make_launcher(), annotations=_EXECUTION_123)

    assert pod.spec.containers[0].env == [
        k8s_client_lib.V1EnvVar(
            name="__TANGLE_SECRET_TOKEN",
            value_from=k8s_client_lib.V1EnvVarSource(
                secret_key_ref=k8s_client_lib.V1SecretKeySelector(
                    name="tangle-ce-123", key="__TANGLE_SECRET_TOKEN"
                )
            ),
        )
    ]
    assert _SECRET_VALUE not in pod.spec.containers[0].args[0]


def test_two_secret_inputs_on_one_env_var_name_are_a_launcher_error() -> None:
    first, second = _make_secret_argument(), _make_secret_argument()
    second.dynamic_data = {"secret": {"name": "anthropic-key"}}

    with pytest.raises(interfaces.LauncherError, match="__TANGLE_SECRET_API_KEY"):
        _make_launcher()._handle_secrets_before_preparing_pod(
            input_arguments={"api-key": first, "api_key": second}
        )


@pytest.mark.usefixtures("secrets_api")
def test_placeholder_names_the_environment_variable_the_pod_defines() -> None:
    pod = _launch(launcher=_make_launcher())

    container = pod.spec.containers[0]
    assert [env.name for env in container.env] == ["__TANGLE_SECRET_TOKEN"]
    assert container.args == ["$(__TANGLE_SECRET_TOKEN)"]
    assert _SECRET_VALUE not in container.args[0]


def test_secret_carries_the_plain_value_in_string_data() -> None:
    # `data` must be base64, so the API server would decode the plain value into other bytes.
    secret = _make_launcher()._handle_secrets_after_preparing_pod(
        pod=_make_pod(args=[]),
        secret_name_to_secret_value={"hf-token": _SECRET_VALUE},
        secret_env_name_to_secret_name={"__TANGLE_SECRET_TOKEN": "hf-token"},
    )

    assert secret.string_data == {"__TANGLE_SECRET_TOKEN": _SECRET_VALUE}
    assert secret.data is None


@pytest.mark.usefixtures("secrets_api")
def test_environment_entry_goes_to_the_main_container_not_the_first() -> None:
    sidecar = k8s_client_lib.V1Container(name="sidecar")
    pod = _make_pod(args=[])
    pod.spec.containers.insert(0, sidecar)

    _make_launcher()._handle_secrets_after_preparing_pod(
        pod=pod,
        secret_name_to_secret_value={"hf-token": _SECRET_VALUE},
        secret_env_name_to_secret_name={"__TANGLE_SECRET_TOKEN": "hf-token"},
        annotations=None,
    )

    [_, main] = pod.spec.containers
    assert sidecar.env is None
    assert [env.name for env in main.env] == ["__TANGLE_SECRET_TOKEN"]


_JOB_LAUNCHER: Final = (
    kubernetes_launchers.Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher
)
_POD_OR_JOB: Final = pytest.mark.parametrize(
    "launcher_class, pod_or_job",
    [
        pytest.param(
            kubernetes_launchers.KubernetesWithHostPathContainerLauncher,
            k8s_client_lib.V1OwnerReference(
                api_version="v1", kind="Pod", name="task-pod", uid="pod-uid"
            ),
            id="pod",
        ),
        pytest.param(
            _JOB_LAUNCHER,
            k8s_client_lib.V1OwnerReference(
                api_version="batch/v1", kind="Job", name="tangle-ce-123", uid="job-uid"
            ),
            id="job",
        ),
    ],
)


def _launch_task(
    *,
    launcher_class: type[kubernetes_launchers._KubernetesContainerLauncherBase],
    annotations: dict[str, str],
    log_uri: str,
) -> None:
    """Launches a task whose only argument is the secret input."""
    launcher = launcher_class(api_client=k8s_client_lib.ApiClient())
    launcher.launch_container_task(
        component_spec=structures.ComponentSpec(
            name="test",
            inputs=[structures.InputSpec(name="token")],
            implementation=structures.ContainerImplementation(
                container=structures.ContainerSpec(
                    image="alpine",
                    args=[structures.InputValuePlaceholder(input_name="token")],
                )
            ),
        ),
        input_arguments={"token": _make_secret_argument()},
        output_uris={},
        log_uri=log_uri,
        annotations={
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY: "123",
            **annotations,
        },
    )


@pytest.mark.parametrize(
    "annotations",
    [
        pytest.param({}, id="single node"),
        pytest.param(
            {kubernetes_launchers.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "2"},
            id="two nodes",
        ),
    ],
)
def test_job_container_reads_the_value_from_the_secret(
    annotations: dict[str, str],
    secrets_api: _SecretsApi,
    jobs_api: _JobsApi,
    tmp_path: pathlib.Path,
) -> None:
    _launch_task(
        launcher_class=_JOB_LAUNCHER,
        annotations=annotations,
        log_uri=str(tmp_path / "log.txt"),
    )

    [job] = jobs_api.created
    [main] = job.spec.template.spec.containers
    assert "__TANGLE_SECRET_TOKEN" in [env.name for env in main.env]
    assert main.args == ["$(__TANGLE_SECRET_TOKEN)"]
    [(_, secret)] = secrets_api.created
    assert secret.string_data == {"__TANGLE_SECRET_TOKEN": _SECRET_VALUE}


@_POD_OR_JOB
@pytest.mark.usefixtures("jobs_api")
def test_secret_is_named_after_the_execution_and_owned_by_its_pod_or_job(
    launcher_class: type[kubernetes_launchers._KubernetesContainerLauncherBase],
    pod_or_job: k8s_client_lib.V1OwnerReference,
    secrets_api: _SecretsApi,
    tmp_path: pathlib.Path,
) -> None:
    _launch_task(
        launcher_class=launcher_class,
        annotations=_EXECUTION_123,
        log_uri=str(tmp_path / "log.txt"),
    )

    [(namespace, secret)] = secrets_api.created
    assert namespace == "default"
    assert secret.metadata.name == "tangle-ce-123"
    assert secret.metadata.owner_references == [pod_or_job]


@_POD_OR_JOB
def test_failed_secret_deletes_its_pod_or_job(
    launcher_class: type[kubernetes_launchers._KubernetesContainerLauncherBase],
    pod_or_job: k8s_client_lib.V1OwnerReference,
    secrets_api: _SecretsApi,
    jobs_api: _JobsApi,
    tmp_path: pathlib.Path,
) -> None:
    secrets_api.fail_secrets = True

    with pytest.raises(interfaces.LauncherError):
        _launch_task(
            launcher_class=launcher_class,
            annotations=_EXECUTION_123,
            log_uri=str(tmp_path / "log.txt"),
        )

    assert secrets_api.deleted_pods + jobs_api.deleted == [pod_or_job.name]


def test_secret_is_keyed_by_the_env_var_name_so_any_secret_name_works() -> None:
    # Kubernetes refuses "hf token" as a Secret key; the input-derived name is always valid.
    argument = _make_secret_argument()
    argument.dynamic_data = {"secret": {"name": "hf token"}}
    launcher = _make_launcher()
    values, env_names = launcher._handle_secrets_before_preparing_pod(
        input_arguments={"token": argument}
    )
    pod = _make_pod(args=[argument.value])

    secret = launcher._handle_secrets_after_preparing_pod(
        pod=pod,
        secret_name_to_secret_value=values,
        secret_env_name_to_secret_name=env_names,
    )

    assert secret.string_data == {"__TANGLE_SECRET_TOKEN": _SECRET_VALUE}
    [env] = pod.spec.containers[0].env
    assert env.value_from.secret_key_ref.key == "__TANGLE_SECRET_TOKEN"
