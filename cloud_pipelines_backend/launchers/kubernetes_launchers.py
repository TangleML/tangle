from __future__ import annotations

import copy
import datetime
import json
import logging
import os
import pathlib
import typing
from typing import Any, Optional

from kubernetes import client as k8s_client_lib
from kubernetes import watch as k8s_watch_lib

from cloud_pipelines.orchestration.launchers import naming_utils
from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_provider_interfaces,
)
from cloud_pipelines.orchestration.storage_providers import local_storage
from .. import component_structures as structures
from . import common_annotations
from . import container_component_utils
from . import interfaces

if typing.TYPE_CHECKING:
    from google.cloud import storage

_logger = logging.getLogger(__name__)

_MAX_INPUT_VALUE_SIZE = 10000
_MAIN_CONTAINER_NAME = "main"


# Kubernetes annotation keys. (Has strict naming policy. Single slash only etc.)
_CLOUD_PIPELINES_KUBERNETES_ANNOTATION_KEY = "cloud-pipelines.net"
_KUBERNETES_LAUNCHER_ANNOTATION_KEY = "cloud-pipelines.net/launchers.kubernetes"
# ComponentSpec annotation keys
RESOURCES_CPU_ANNOTATION_KEY = "cloud-pipelines.net/launchers/generic/resources.cpu"
RESOURCES_MEMORY_ANNOTATION_KEY = (
    "cloud-pipelines.net/launchers/generic/resources.memory"
)
RESOURCES_ACCELERATORS_ANNOTATION_KEY = (
    "cloud-pipelines.net/launchers/generic/resources.accelerators"
)

# Multi-node constants
_MULTI_NODE_MAX_NUMBER_OF_NODES = 16


_T = typing.TypeVar("_T")

_CONTAINER_FILE_NAME = "data"


def _create_volume_and_volume_mount_host_path(
    container_path: str,
    artifact_uri: str,
    suggested_volume_name: str,
    read_only: bool | None = None,
) -> tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount]:
    container_dir, _, container_file = container_path.rpartition("/")
    artifact_dir_uri, _, artifact_file = artifact_uri.rpartition("/")
    if container_file != artifact_file:
        raise interfaces.LauncherError(
            f"Container file name is different from artifact file name. {container_path=}, {artifact_uri=}"
        )
    # artifact_dir_uri = pathlib.PurePosixPath(artifact_uri).parent.as_posix()
    host_dir = artifact_dir_uri
    sub_path = ""
    # host_dir + "/" + sub_path == artifact_dir
    if os.name == "nt":
        host_dir = windows_path_to_docker_path(host_dir)
    return (
        k8s_client_lib.V1Volume(
            name=suggested_volume_name,
            host_path=k8s_client_lib.V1HostPathVolumeSource(
                path=host_dir,
                # type=?
            ),
        ),
        k8s_client_lib.V1VolumeMount(
            name=suggested_volume_name,
            mount_path=container_dir,
            read_only=read_only,
            sub_path=sub_path,
        ),
    )


def _create_volume_and_volume_mount_google_cloud_storage(
    container_path: str,
    artifact_uri: str,
    suggested_volume_name: str,
    read_only: bool | None = None,
) -> tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount]:
    container_dir, _, container_file = container_path.rpartition("/")
    artifact_dir_uri, _, artifact_file = artifact_uri.rpartition("/")
    if container_file != artifact_file:
        raise interfaces.LauncherError(
            "Container file name is different from artifact file name. {container_path=}, {artifact_uri=}"
        )

    bucket_name, _, sub_path = artifact_dir_uri.removeprefix("gs://").partition("/")
    volume_name = f"gcsfuse-{bucket_name}"

    return (
        k8s_client_lib.V1Volume(
            name=volume_name,
            csi=k8s_client_lib.V1CSIVolumeSource(
                driver="gcsfuse.csi.storage.gke.io",
                volume_attributes={
                    "bucketName": bucket_name,
                    "mountOptions": "implicit-dirs",
                },
            ),
        ),
        k8s_client_lib.V1VolumeMount(
            name=volume_name,
            mount_path=container_dir,
            read_only=read_only,
            sub_path=sub_path,
        ),
    )


class PodPostProcessor(typing.Protocol):
    def __call__(
        self, *, pod: k8s_client_lib.V1Pod, annotations: dict[str, str] | None = None
    ) -> k8s_client_lib.V1Pod: ...


class _KubernetesContainerLauncherBase:
    """Launcher that launches container using Kubernetes"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: PodPostProcessor | None = None,
        _storage_provider: storage_provider_interfaces.StorageProvider,
        _create_volume_and_volume_mount: typing.Callable[
            [str, str, str, bool],
            tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount],
        ],
    ):
        self._namespace = namespace
        self._service_account_name = service_account_name
        self._api_client = api_client
        self._storage_provider = _storage_provider
        self._request_timeout = request_timeout
        self._pod_name_prefix = pod_name_prefix
        self._pod_labels = pod_labels
        self._pod_annotations = {
            _CLOUD_PIPELINES_KUBERNETES_ANNOTATION_KEY: "true",
            _KUBERNETES_LAUNCHER_ANNOTATION_KEY: "true",
        } | (pod_annotations or {})
        self._pod_postprocessor = pod_postprocessor
        self._create_volume_and_volume_mount = _create_volume_and_volume_mount

        try:
            k8s_client_lib.VersionApi(self._api_client).get_code(
                _request_timeout=request_timeout
            )
        except Exception as ex:
            raise RuntimeError(
                "Connection to the Kubernetes cluster does not seem to be working."
                " Please make sure that `kubectl cluster-info` executes without errors."
            ) from ex

    def _prepare_kubernetes_pod(
        self,
        *,
        component_spec: structures.ComponentSpec,
        # Input arguments may be updated with new downloaded values and new URIs of uploaded values.
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
        pod_namespace: str | None = None,
        pod_name_prefix: str | None = None,
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_service_account: str | None = None,
    ) -> k8s_client_lib.V1Pod:
        if not isinstance(
            component_spec.implementation, structures.ContainerImplementation
        ):
            raise interfaces.LauncherError(
                f"Component must have container implementation. {component_spec=}"
            )
        container_spec = component_spec.implementation.container

        volume_map: dict[str, k8s_client_lib.V1Volume] = {}
        volume_mounts: list[k8s_client_lib.V1VolumeMount] = []

        container_inputs_root = pathlib.PurePosixPath("/tmp/inputs")
        container_outputs_root = pathlib.PurePosixPath("/tmp/outputs")

        # Callbacks for the command-line resolving
        # Their main purpose is to return input/output path or value.
        # They add volumes and volume mounts when needed.
        # They also upload/download artifact data when needed.
        def get_input_value(input_name: str) -> str:
            input_argument = input_arguments[input_name]
            if input_argument.is_dir:
                raise interfaces.LauncherError(
                    f"Cannot consume directory as value. {input_name=}, {input_argument=}"
                )
            if input_argument.total_size > _MAX_INPUT_VALUE_SIZE:
                raise interfaces.LauncherError(
                    f"Artifact is too big to consume as value. Consume it as file instead. {input_name=}, {input_argument=}"
                )
            value = input_argument.value
            if value is None:
                # Download artifact data
                if not input_argument.uri:
                    raise interfaces.LauncherError(
                        f"Artifact data has no value and no uri. This cannot happen. {input_name=}, {input_argument=}"
                    )
                uri_reader = self._storage_provider.make_uri(
                    input_argument.uri
                ).get_reader()
                try:
                    data = uri_reader.download_as_bytes()
                except Exception as ex:
                    raise interfaces.LauncherError(
                        f"Error downloading artifact data. {input_name=}, {input_argument.uri=}"
                    ) from ex
                try:
                    value = data.decode("utf-8")
                except Exception as ex:
                    raise interfaces.LauncherError(
                        f"Error converting artifact data to text. {input_name=}, {input_argument.uri=}"
                    ) from ex
                # Updating the input_arguments with the downloaded value
                input_argument.value = value
            return value

        def get_input_path(input_name: str) -> str:
            input_argument = input_arguments[input_name]
            uri = input_argument.uri
            if not uri:
                if input_argument.value is None:
                    raise interfaces.LauncherError(
                        f"Artifact data has no value and no uri. This cannot happen. {input_name=}, {input_argument=}"
                    )
                uri_writer = self._storage_provider.make_uri(
                    input_argument.staging_uri
                ).get_writer()
                try:
                    uri_writer.upload_from_text(input_argument.value)
                except Exception as ex:
                    raise interfaces.LauncherError(
                        f"Error uploading argument value. {input_name=}, {input_argument=}"
                    ) from ex
                uri = input_argument.staging_uri
                # Updating the input_arguments with the URI of the uploaded value
                input_argument.uri = uri

            container_path = (
                container_inputs_root
                / naming_utils.sanitize_file_name(input_name)
                / _CONTAINER_FILE_NAME
            ).as_posix()
            volume_name = naming_utils.sanitize_kubernetes_resource_name(
                "inputs-" + input_name
            )
            volume, volume_mount = self._create_volume_and_volume_mount(
                container_path=container_path,
                artifact_uri=uri,
                suggested_volume_name=volume_name,
                read_only=True,
            )
            volume_map[volume.name] = volume
            volume_mounts.append(volume_mount)
            return container_path

        def get_output_path(output_name: str) -> str:
            uri = output_uris[output_name]
            container_path = (
                container_outputs_root
                / naming_utils.sanitize_file_name(output_name)
                / _CONTAINER_FILE_NAME
            ).as_posix()
            volume_name = naming_utils.sanitize_kubernetes_resource_name(
                "outputs-" + output_name
            )
            volume, volume_mount = self._create_volume_and_volume_mount(
                container_path=container_path,
                artifact_uri=uri,
                suggested_volume_name=volume_name,
            )
            volume_map[volume.name] = volume
            volume_mounts.append(volume_mount)
            return container_path

        # Resolving the command line.
        # Also indirectly populates volumes and volume_mounts.
        resolved_cmd = container_component_utils.resolve_container_command_line(
            component_spec=component_spec,
            provided_input_names=set(input_arguments.keys()),
            get_input_value=get_input_value,
            get_input_path=get_input_path,
            get_output_path=get_output_path,
        )

        container_env = [
            k8s_client_lib.V1EnvVar(name=name, value=value)
            for name, value in (container_spec.env or {}).items()
        ]
        main_container_spec = k8s_client_lib.V1Container(
            name=_MAIN_CONTAINER_NAME,
            image=container_spec.image,
            command=resolved_cmd.command,
            args=resolved_cmd.args,
            env=container_env,
            volume_mounts=volume_mounts,
        )

        annotations = annotations or {}

        cpu_resource_request = annotations.get(RESOURCES_CPU_ANNOTATION_KEY)
        memory_resource_request = annotations.get(RESOURCES_MEMORY_ANNOTATION_KEY)
        if cpu_resource_request or memory_resource_request:
            resources: k8s_client_lib.V1ResourceRequirements = (
                main_container_spec.resources or k8s_client_lib.V1ResourceRequirements()
            )
            main_container_spec.resources = resources
            resources.requests = resources.requests or {}
            resources.limits = resources.limits or {}
            if cpu_resource_request:
                resources.requests["cpu"] = cpu_resource_request
            if memory_resource_request:
                resources.requests["memory"] = memory_resource_request
                resources.limits["memory"] = memory_resource_request

        pod_spec = k8s_client_lib.V1PodSpec(
            init_containers=[],
            containers=[
                main_container_spec,
            ],
            volumes=list(volume_map.values()),
            restart_policy="Never",
            service_account_name=pod_service_account,
        )

        pod = k8s_client_lib.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s_client_lib.V1ObjectMeta(
                generate_name=pod_name_prefix,
                namespace=pod_namespace,
                labels=pod_labels,
                annotations=pod_annotations,
            ),
            spec=pod_spec,
        )
        return pod

    def _choose_namespace(
        self,
        *,
        annotations: dict[str, Any] | None = None,
    ) -> str:
        """Chooses the namespace for the pod.

        Override this method to choose namespace dynamically.
        """
        del annotations
        return self._namespace


class _KubernetesPodLauncher(
    _KubernetesContainerLauncherBase,
    interfaces.ContainerTaskLauncher["LaunchedKubernetesContainer"],
):
    """Launcher that launches a container via a Kubernetes Pod."""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: PodPostProcessor | None = None,
        _storage_provider: storage_provider_interfaces.StorageProvider,
        _create_volume_and_volume_mount: typing.Callable[
            [str, str, str, bool],
            tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount],
        ],
    ):
        super().__init__(
            api_client=api_client,
            namespace=namespace,
            service_account_name=service_account_name,
            request_timeout=request_timeout,
            pod_name_prefix=pod_name_prefix,
            pod_labels=pod_labels,
            pod_annotations=pod_annotations,
            pod_postprocessor=pod_postprocessor,
            _storage_provider=_storage_provider,
            _create_volume_and_volume_mount=_create_volume_and_volume_mount,
        )

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        # Input arguments may be updated with new downloaded values and new URIs of uploaded values.
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> "LaunchedKubernetesContainer":
        namespace = self._choose_namespace(annotations=annotations)

        pod = self._prepare_kubernetes_pod(
            component_spec=component_spec,
            input_arguments=input_arguments,
            output_uris=output_uris,
            log_uri=log_uri,
            annotations=annotations,
            pod_name_prefix=self._pod_name_prefix,
            pod_namespace=namespace,
            pod_labels=self._pod_labels,
            pod_annotations=self._pod_annotations,
            pod_service_account=self._service_account_name,
        )

        # Applying the pod post-processor
        if self._pod_postprocessor:
            pod = self._pod_postprocessor(pod=pod, annotations=annotations)

        core_api_client = k8s_client_lib.CoreV1Api(api_client=self._api_client)
        try:
            created_pod: k8s_client_lib.V1Pod = core_api_client.create_namespaced_pod(
                namespace=pod.metadata.namespace or namespace,
                body=pod,
                _request_timeout=self._request_timeout,
            )
        except Exception as ex:
            raise interfaces.LauncherError(
                f"Failed to create pod: {_kubernetes_serialize(pod)}"
            ) from ex

        pod_name: str = created_pod.metadata.name
        pod_namespace: str = created_pod.metadata.namespace
        _logger.info(f"Created pod {pod_name} in namespace {pod_namespace}")

        launched_kubernetes_container = LaunchedKubernetesContainer(
            pod_name=pod_name,
            namespace=pod_namespace,
            output_uris=output_uris,
            log_uri=log_uri,
            debug_pod=created_pod,
            cluster_server=self._api_client.configuration.host,
            launcher=self,
        )
        return launched_kubernetes_container

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedKubernetesContainer":
        launched_container = LaunchedKubernetesContainer.from_dict(
            launched_container_dict, launcher=self
        )
        return launched_container.get_refreshed()

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedKubernetesContainer":
        launched_container = LaunchedKubernetesContainer.from_dict(
            launched_container_dict, launcher=self
        )
        return launched_container


# https://cloud.google.com/kubernetes-engine/docs/concepts/spot-vms
KUBERNETES_GOOGLE_USE_SPOT_VMS_ANNOTATION_KEY = (
    "cloud-pipelines.net/launchers/kubernetes/google/use_spot_vms"
)


def _google_kubernetes_engine_accelerator_pod_postprocessor(
    *, pod: k8s_client_lib.V1Pod, annotations: dict[str, str] | None = None
) -> k8s_client_lib.V1Pod:
    if not annotations:
        return pod

    accelerators_resource_request = annotations.get(
        RESOURCES_ACCELERATORS_ANNOTATION_KEY
    )
    use_spot_vms = annotations.get(KUBERNETES_GOOGLE_USE_SPOT_VMS_ANNOTATION_KEY, False)
    pod = copy.deepcopy(pod)
    pod_spec: k8s_client_lib.V1PodSpec = pod.spec
    pod_spec.node_selector = pod_spec.node_selector or {}
    # TODO: Get main container by name
    main_container_spec: k8s_client_lib.V1Container = pod_spec.containers[0]
    resources: k8s_client_lib.V1ResourceRequirements = (
        main_container_spec.resources or k8s_client_lib.V1ResourceRequirements()
    )
    main_container_spec.resources = resources
    resources.limits = resources.limits or {}

    if accelerators_resource_request:
        accelerators_dict = json.loads(accelerators_resource_request)
        nvidia_gpu_count = 0
        if len(accelerators_dict) > 1:
            raise interfaces.LauncherError(
                f"Multiple accelerator types were specified: {accelerators_dict=}"
            )
        for resource_name, quantity in (accelerators_dict or {}).items():
            pod_spec.node_selector["cloud.google.com/gke-accelerator"] = resource_name
            if resource_name.startswith("nvidia"):
                nvidia_gpu_count += int(quantity)

        if nvidia_gpu_count:
            resources.limits["nvidia.com/gpu"] = nvidia_gpu_count

    if use_spot_vms:
        pod_spec.node_selector["cloud.google.com/gke-spot"] = use_spot_vms

    return pod


def _create_pod_postprocessor_stack(
    pod_postprocessors: list[PodPostProcessor],
) -> PodPostProcessor:
    def _post_processor(
        *, pod: k8s_client_lib.V1Pod, annotations: dict[str, str] | None = None
    ) -> k8s_client_lib.V1Pod:
        for pod_postprocessor in pod_postprocessors:
            pod = pod_postprocessor(pod=pod, annotations=annotations)
        return pod

    return _post_processor


class KubernetesWithHostPathContainerLauncher(_KubernetesPodLauncher):
    """Launcher that uses single-node Kubernetes (uses hostPath for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: PodPostProcessor | None = None,
    ):
        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_name_prefix=pod_name_prefix,
            _storage_provider=local_storage.LocalStorageProvider(),
            pod_labels=pod_labels,
            pod_annotations=pod_annotations,
            pod_postprocessor=pod_postprocessor,
            _create_volume_and_volume_mount=_create_volume_and_volume_mount_host_path,
        )


class GoogleKubernetesEngineLauncher(_KubernetesPodLauncher):
    """Launcher that uses GKE Kubernetes (uses GKE-gcsfuse driver for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        gcs_client: "storage.Client | None" = None,
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: PodPostProcessor | None = None,
    ):
        pod_postprocessors = [_google_kubernetes_engine_accelerator_pod_postprocessor]
        if pod_postprocessor:
            pod_postprocessors.append(pod_postprocessor)
        final_pod_postporocessor = _create_pod_postprocessor_stack(pod_postprocessors)

        from cloud_pipelines.orchestration.storage_providers import google_cloud_storage

        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_name_prefix=pod_name_prefix,
            _storage_provider=google_cloud_storage.GoogleCloudStorageProvider(
                gcs_client
            ),
            pod_labels=pod_labels,
            pod_annotations={"gke-gcsfuse/volumes": "true"} | (pod_annotations or {}),
            pod_postprocessor=final_pod_postporocessor,
            _create_volume_and_volume_mount=_create_volume_and_volume_mount_google_cloud_storage,
        )


# For backwards compatibility
class KubernetesWithGcsFuseContainerLauncher(GoogleKubernetesEngineLauncher):
    pass


class LaunchedKubernetesContainer(interfaces.LaunchedContainer):

    def __init__(
        self,
        pod_name: str,
        namespace: str,
        output_uris: dict[str, str],
        log_uri: str,
        debug_pod: k8s_client_lib.V1Pod,
        cluster_server: str | None = None,
        launcher: _KubernetesPodLauncher | None = None,
    ):
        self._pod_name = pod_name
        self._namespace = namespace
        self._output_uris = output_uris
        self._log_uri = log_uri
        self._debug_pod = debug_pod
        self._cluster_server = cluster_server
        self._launcher = launcher

    def _get_launcher(self):
        if not self._launcher:
            raise interfaces.LauncherError(
                "This action requires a launcher, but LaunchedKubernetesContainer was constructed without one."
            )
        return self._launcher

    def _get_main_container_state(
        self,
    ) -> k8s_client_lib.V1ContainerState | None:
        pod_status: k8s_client_lib.V1PodStatus = self._debug_pod.status
        if not pod_status or not pod_status.container_statuses:
            return None
        container_statuses: list[k8s_client_lib.V1ContainerStatus] = (
            pod_status.container_statuses
        )
        main_container_statuses = [
            container_status
            for container_status in container_statuses
            if container_status.name == _MAIN_CONTAINER_NAME
        ]
        if len(main_container_statuses) != 1:
            raise RuntimeError(
                f"Cannot get the main container status form the pod: {self._debug_pod}"
            )
        main_container_status = main_container_statuses[0]
        main_container_state: k8s_client_lib.V1ContainerState = (
            main_container_status.state
        )
        return main_container_state

    def _get_main_container_terminated_state(
        self,
    ) -> k8s_client_lib.V1ContainerStateTerminated | None:
        state = self._get_main_container_state()
        if not state:
            return None
        return state.terminated

    # @property
    # def id(self) -> str:
    #     return self.pod_name

    @property
    def status(self) -> interfaces.ContainerStatus:
        phase_str = self._debug_pod.status.phase
        if phase_str == "Pending":
            return interfaces.ContainerStatus.PENDING
        elif phase_str == "Running":
            return interfaces.ContainerStatus.RUNNING
        elif phase_str == "Succeeded":
            return interfaces.ContainerStatus.SUCCEEDED
        elif phase_str == "Failed":
            return interfaces.ContainerStatus.FAILED
        else:
            return interfaces.ContainerStatus.ERROR

    @property
    def exit_code(self) -> Optional[int]:
        main_container_terminated_state = self._get_main_container_terminated_state()
        if main_container_terminated_state is None:
            return None
        return main_container_terminated_state.exit_code

    @property
    def has_ended(self) -> bool:
        main_container_terminated_state = self._get_main_container_terminated_state()
        return main_container_terminated_state is not None

    @property
    def has_succeeded(self) -> bool:
        return self.status == interfaces.ContainerStatus.SUCCEEDED

    @property
    def has_failed(self) -> bool:
        return self.status == interfaces.ContainerStatus.FAILED

    @property
    def started_at(self) -> datetime.datetime | None:
        main_container_state = self._get_main_container_state()
        if main_container_state is None:
            return None
        terminated_state: k8s_client_lib.V1ContainerStateTerminated = (
            main_container_state.terminated
        )
        if terminated_state is not None:
            return terminated_state.started_at
        running_state: k8s_client_lib.V1ContainerStateRunning = (
            main_container_state.running
        )
        if running_state is not None:
            return running_state.started_at
        return None

    @property
    def ended_at(self) -> datetime.datetime | None:
        terminated_state = self._get_main_container_terminated_state()
        if terminated_state is None:
            return None
        return terminated_state.finished_at

    @property
    def launcher_error_message(self) -> str | None:
        main_container_terminated_state = self._get_main_container_terminated_state()
        if main_container_terminated_state is None:
            return None
        if (
            main_container_terminated_state.message is None
            and main_container_terminated_state.reason == "Error"
        ):
            # Do not confuse users with message-less error messages
            return None
        launcher_error_message = f"Kubernetes error. Reason: {main_container_terminated_state.reason}, message: {main_container_terminated_state.message}"
        return launcher_error_message

    def to_dict(self) -> dict[str, Any]:
        pod_dict = _serialize_kubernetes_object_to_compact_dict(self._debug_pod)
        result = dict(
            kubernetes=dict(
                # launched_container_class_name=self.__class__.__name__,
                pod_name=self._pod_name,
                namespace=self._namespace,
                cluster_server=self._cluster_server,
                output_uris=self._output_uris,
                log_uri=self._log_uri,
                debug_pod=pod_dict,
            ),
        )
        return result

    @classmethod
    def from_dict(
        cls, d: dict[str, Any], launcher: _KubernetesPodLauncher | None = None
    ) -> LaunchedKubernetesContainer:
        # Backwards compatibility for old container execution records.
        d = d.get("kubernetes", d)
        debug_pod = _kubernetes_deserialize(d["debug_pod"], cls=k8s_client_lib.V1Pod)
        return LaunchedKubernetesContainer(
            pod_name=d["pod_name"],
            namespace=d["namespace"],
            cluster_server=d.get("cluster_server"),
            output_uris=d["output_uris"],
            log_uri=d["log_uri"],
            debug_pod=debug_pod,
            launcher=launcher,
        )

    def get_refreshed(self) -> "LaunchedKubernetesContainer":
        launcher = self._get_launcher()
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        pod: k8s_client_lib.V1Pod = core_api_client.read_namespaced_pod(
            name=self._pod_name,
            namespace=self._namespace,
            _request_timeout=launcher._request_timeout,
        )
        new_launched_container = copy.copy(self)
        new_launched_container._debug_pod = pod
        return new_launched_container

    def get_log(self) -> str:
        launcher = self._get_launcher()
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        return core_api_client.read_namespaced_pod_log(
            name=self._pod_name,
            namespace=self._namespace,
            container=_MAIN_CONTAINER_NAME,
            timestamps=True,
            # Disabled stream="All" due to error in some new GKE clusters
            # HTTP response body: {"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"PodLogOptions \"task-pod-xxxxx\" is invalid: stream: Forbidden: may not be specified","reason":"Invalid","details":{"name":"task-pod-xxxxx","kind":"PodLogOptions","causes":[{"reason":"FieldValueForbidden","message":"Forbidden: may not be specified","field":"stream"}]},"code":422}
            # stream="All",
            _request_timeout=launcher._request_timeout,
        )

    def upload_log(self):
        launcher = self._get_launcher()
        log = self.get_log()
        uri_writer = launcher._storage_provider.make_uri(self._log_uri).get_writer()
        uri_writer.upload_from_text(log)

    def stream_log_lines(self) -> typing.Iterator[str]:
        launcher = self._get_launcher()
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        stream = k8s_watch_lib.Watch().stream(
            core_api_client.read_namespaced_pod_log,
            name=self._pod_name,
            namespace=self._namespace,
            container=_MAIN_CONTAINER_NAME,
            timestamps=True,
            # Disabled stream="All" due to error in some new GKE clusters
            # HTTP response body: {"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"PodLogOptions \"task-pod-xxxxx\" is invalid: stream: Forbidden: may not be specified","reason":"Invalid","details":{"name":"task-pod-xxxxx","kind":"PodLogOptions","causes":[{"reason":"FieldValueForbidden","message":"Forbidden: may not be specified","field":"stream"}]},"code":422}
            # stream="All",
            _request_timeout=launcher._request_timeout,
        )
        for line in stream:
            yield str(line) + "\n"

    def __str__(self) -> str:
        import pprint

        return pprint.pformat(self.to_dict())

    def terminate(self):
        launcher = self._get_launcher()
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        core_api_client.delete_namespaced_pod(
            name=self._pod_name,
            namespace=self._namespace,
            grace_period_seconds=10,
        )
        _logger.info(f"Terminated pod {self._pod_name} in namespace {self._namespace}")


class _KubernetesJobLauncher(
    _KubernetesContainerLauncherBase,
    interfaces.ContainerTaskLauncher["LaunchedKubernetesJob"],
):
    """Launcher that launches Kubernetes Jobs on a single-node Kubernetes (uses hostPath for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: PodPostProcessor | None = None,
        _storage_provider: storage_provider_interfaces.StorageProvider,
        _create_volume_and_volume_mount: typing.Callable[
            [str, str, str, bool],
            tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount],
        ],
    ):
        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_name_prefix="task-",
            _storage_provider=_storage_provider,
            pod_labels=pod_labels,
            pod_annotations=pod_annotations,
            pod_postprocessor=pod_postprocessor,
            _create_volume_and_volume_mount=_create_volume_and_volume_mount,
        )

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        # Input arguments may be updated with new downloaded values and new URIs of uploaded values.
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> "LaunchedKubernetesJob":
        namespace = self._choose_namespace(annotations=annotations)

        # We have 2 options regarding job name:
        # Option 1: We could use randomized job name generated via `metadata.generateName`.
        #    Randomized job names are slightly harder to use:
        #    * We cannot predict the pod names and their DNS addresses which are needed for cross-pod communication.
        #      We can solve this problem by adding environment variables that are sourced from the job name pod label at runtime and use this environment variable to dynamically construct node addresses.
        #      However this adds complexity and requires using environment variables for everything.
        #    * Service requires a static selector which is usually a job-name label, which cannot be used when job name is randomized.
        #      We can solve this problem by using another label for selector (such as container_execution_id), but this adds more complexity.
        # Option 2 (chosen): We could use explicit job name specified via `metadata.name`.
        #    There do not seem to be any downsides for specifying the job name explicitly.
        #    I do not foresee a need to have multiple Jobs associated with a single container execution ID.
        #    If such cases arise in the future, we can always add a random suffix or switch to option 1.

        container_execution_id = (annotations or {}).get(
            common_annotations.CONTAINER_EXECUTION_ID_ANNOTATION_KEY
        )

        resource_name_prefix = "tangle-ce-"
        if container_execution_id:
            explicit_resource_name = resource_name_prefix + container_execution_id
        else:
            _logger.warning(
                f"Should not happen: Container execution ID annotation is required for multi-node execution, but it was not found."
            )
            import uuid

            explicit_resource_name = resource_name_prefix + uuid.uuid4().hex[:8]

        explicit_job_name = explicit_resource_name

        MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY = (
            "tangleml.com/launchers/kubernetes/multi_node/number_of_nodes"
        )
        num_nodes_annotation_str = (annotations or {}).get(
            MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY, 1
        )
        enable_multi_node = num_nodes_annotation_str is not None
        num_nodes = int(num_nodes_annotation_str) if num_nodes_annotation_str else 1
        if not (0 < num_nodes <= _MULTI_NODE_MAX_NUMBER_OF_NODES):
            raise interfaces.LauncherError(
                f"Invalid number of nodes for multi-node execution. Number of nodes must be between 1 and {_MULTI_NODE_MAX_NUMBER_OF_NODES}, but got {num_nodes}."
            )
        explicit_service_name = explicit_resource_name
        if enable_multi_node:
            all_node_addresses = [
                f"{explicit_job_name}-{idx}.{explicit_service_name}"
                for idx in range(num_nodes)
            ]
            node_0_address = all_node_addresses[0]
            # We could join using comma or newline
            all_node_addresses_str = ",".join(all_node_addresses)

        pod = self._prepare_kubernetes_pod(
            component_spec=component_spec,
            input_arguments=input_arguments,
            output_uris=output_uris,
            log_uri=log_uri,
            annotations=annotations,
            pod_name_prefix=self._pod_name_prefix,
            pod_namespace=namespace,
            pod_labels=self._pod_labels,
            pod_annotations=self._pod_annotations,
            pod_service_account=self._service_account_name,
        )

        # Applying the pod post-processor
        if self._pod_postprocessor:
            pod = self._pod_postprocessor(pod=pod, annotations=annotations)
        assert pod.spec
        assert pod.spec.containers

        # Changing the namespace to the final outcome from the pod_postprocessor.
        # TODO: In the future (once consumers have migrated to _choose_namespace)
        # we should prohibit/ignore changing pod namespace in the pod post-processor.
        namespace = pod.metadata.namespace

        if enable_multi_node:
            # Temporary implementation of implicitly passing multi-node information to the component code.
            # After testing, this implementation will be replaced by more explicit way to consume multi-node information (dynamicData arguments passed to component inputs).
            MULTI_NODE_NUMBER_OF_NODES_ENV_VAR_NAME = (
                "_TANGLE_MULTI_NODE_NUMBER_OF_NODES"
            )
            MULTI_NODE_NODE_INDEX_ENV_VAR_NAME = "_TANGLE_MULTI_NODE_NODE_INDEX"

            main_container_spec = pod.spec.containers[0]
            main_container_spec.env = main_container_spec.env or []
            main_container_spec.env.append(
                k8s_client_lib.V1EnvVar(
                    name=MULTI_NODE_NUMBER_OF_NODES_ENV_VAR_NAME,
                    value=str(num_nodes),
                )
            )
            main_container_spec.env.append(
                k8s_client_lib.V1EnvVar(
                    name=MULTI_NODE_NODE_INDEX_ENV_VAR_NAME,
                    # We cannot use "$(JOB_COMPLETION_INDEX)" in env variables since it's added after all other env variables.
                    # value="$(JOB_COMPLETION_INDEX)",
                    # So we just recreate it by reading the pod annotation/label that Kubernetes sets on pods of indexed jobs.
                    value_from=k8s_client_lib.V1EnvVarSource(
                        field_ref=k8s_client_lib.V1ObjectFieldSelector(
                            field_path="metadata.annotations['batch.kubernetes.io/job-completion-index']"
                        )
                    ),
                )
            )
            # Handling cross-pod communication.
            # Creating headless Kubernetes Service to give all pods in the job a stable DNS name to communicate with each other.
            service = k8s_client_lib.V1Service(
                metadata=k8s_client_lib.V1ObjectMeta(
                    name=explicit_service_name,
                    namespace=namespace,
                ),
                spec=k8s_client_lib.V1ServiceSpec(
                    # "Headless" service.
                    cluster_ip="None",
                    selector={
                        "job-name": explicit_job_name,
                    },
                ),
            )
            core_api_client = k8s_client_lib.CoreV1Api(api_client=self._api_client)
            try:
                _: k8s_client_lib.V1Service = core_api_client.create_namespaced_service(
                    namespace=namespace,
                    body=service,
                    _request_timeout=self._request_timeout,
                )
            except Exception as ex:

                raise interfaces.LauncherError(
                    f"Failed to create Kubernetes Service {explicit_service_name}: {_kubernetes_serialize(service)}"
                ) from ex
            # Setting Pod's spec.subdomain to exact name of teh service.
            # This requires the service name to be known.
            pod.spec.subdomain = explicit_service_name

            # Node addresses:
            #
            # Code to handle auto-generated job names Option 1):
            # main_container_spec.env.append(
            #     k8s_client_lib.V1EnvVar(
            #         name="__JOB_NAME",
            #         value_from=k8s_client_lib.V1EnvVarSource(
            #             field_ref=k8s_client_lib.V1ObjectFieldSelector(
            #                 field_path="metadata.labels['batch.kubernetes.io/job-name']"
            #             )
            #         ),
            #     )
            # )
            # all_node_addresses = [
            #     # f"$(__JOB_NAME)-{idx}.{explicit_service_name}"
            #     for idx in range(num_nodes)
            # ]

            _MULTI_NODE_NODE_0_ADDRESS_ENV_VAR_NAME = (
                "_TANGLE_MULTI_NODE_NODE_0_ADDRESS"
            )
            _MULTI_NODE_ALL_NODE_ADDRESSES_ENV_VAR_NAME = (
                "_TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES"
            )
            main_container_spec.env.append(
                k8s_client_lib.V1EnvVar(
                    name=_MULTI_NODE_NODE_0_ADDRESS_ENV_VAR_NAME,
                    value=node_0_address,
                )
            )

            main_container_spec.env.append(
                k8s_client_lib.V1EnvVar(
                    name=_MULTI_NODE_ALL_NODE_ADDRESSES_ENV_VAR_NAME,
                    value=all_node_addresses_str,
                )
            )

        job = k8s_client_lib.V1Job(
            metadata=k8s_client_lib.V1ObjectMeta(
                name=explicit_job_name,
                namespace=namespace,
                # annotations=self._pod_annotations,
                # labels=self._pod_labels,
            ),
            spec=k8s_client_lib.V1JobSpec(
                template=k8s_client_lib.V1PodTemplateSpec(
                    metadata=pod.metadata,
                    spec=pod.spec,
                ),
                # Let's always use Indexed Jobs. There are no downsides.
                completion_mode="Indexed",
                # backoff_limit=0,
                backoff_limit_per_index=0,
                # Without explicit max_failed_indexes=0, the job waits for all pods to end and then succeeds ("Complete") despite pod failures!
                max_failed_indexes=0,
                completions=num_nodes,
                parallelism=num_nodes,
            ),
        )

        job = self._transform_job_before_launching(job=job, annotations=annotations)

        batch_api_client = k8s_client_lib.BatchV1Api(api_client=self._api_client)
        try:
            created_job: k8s_client_lib.V1Job = batch_api_client.create_namespaced_job(
                namespace=job.metadata.namespace or namespace,
                body=job,
                _request_timeout=self._request_timeout,
            )
        except Exception as ex:
            raise interfaces.LauncherError(
                f"Failed to create Kubernetes Job: {_kubernetes_serialize(job)}"
            ) from ex

        job_name: str = created_job.metadata.name
        job_namespace: str = created_job.metadata.namespace
        _logger.info(f"Created Kubernetes Job {job_name} in namespace {job_namespace}")

        launched_container = LaunchedKubernetesJob(
            job_name=job_name,
            namespace=job_namespace,
            output_uris=output_uris,
            log_uri=log_uri,
            debug_job=created_job,
            debug_pods={},
            cluster_server=self._api_client.configuration.host,
            launcher=self,
        )

        return launched_container

    def _transform_job_before_launching(
        self, *, job: k8s_client_lib.V1Job, annotations: dict[str, str] | None = None
    ) -> k8s_client_lib.V1Job:
        del annotations
        return job

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedKubernetesJob":
        launched_container = LaunchedKubernetesJob.from_dict(
            launched_container_dict, launcher=self
        )
        return launched_container.get_refreshed()

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedKubernetesJob":
        launched_container = LaunchedKubernetesJob.from_dict(
            launched_container_dict, launcher=self
        )
        return launched_container


class LaunchedKubernetesJob(interfaces.LaunchedContainer):

    def __init__(
        self,
        job_name: str,
        namespace: str,
        output_uris: dict[str, str],
        log_uri: str,
        debug_job: k8s_client_lib.V1Job,
        debug_pods: dict[str, k8s_client_lib.V1Pod] | None = None,
        cluster_server: str | None = None,
        launcher: _KubernetesJobLauncher | None = None,
    ):
        self._job_name = job_name
        self._namespace = namespace
        self._output_uris = output_uris
        self._log_uri = log_uri
        self._debug_job = debug_job
        self._debug_pods: dict[str, k8s_client_lib.V1Pod] = debug_pods or {}
        self._cluster_server = cluster_server
        self._launcher = launcher

    def _get_launcher(self):
        if not self._launcher:
            raise interfaces.LauncherError(
                "This action requires a launcher, but LaunchedKubernetesJob was constructed without one."
            )
        return self._launcher

    @property
    def status(self) -> interfaces.ContainerStatus:
        job = self._debug_job
        job_status = self._debug_job.status
        if not job_status:
            return interfaces.ContainerStatus.PENDING
        has_succeeded_condition = any(
            condition.type == "Complete" and condition.status == "True"
            for condition in job_status.conditions or []
        )
        has_failed_condition = any(
            condition.type == "Failed" and condition.status == "True"
            for condition in job_status.conditions or []
        )
        if has_failed_condition:
            return interfaces.ContainerStatus.FAILED
        if has_succeeded_condition:
            return interfaces.ContainerStatus.SUCCEEDED
        num_required_completions = job.spec.completions or 1
        num_ended = (job_status.succeeded or 0) + (job_status.failed or 0)
        num_active_or_ended = num_ended + (job_status.active or 0)
        if num_active_or_ended < num_required_completions:
            return interfaces.ContainerStatus.PENDING
        # TODO: ! Discern pods in Pending and Running states
        return interfaces.ContainerStatus.RUNNING

    @property
    def exit_code(self) -> Optional[int]:
        if not self.has_ended:
            return None
        # Shortcut for succeeded jobs
        if not self.has_succeeded:
            return 0
        main_container_states = [
            # TODO: Properly select the main container
            pod.status.container_statuses[0].state
            for pod in self._debug_pods.values()
            if pod.status and pod.status.container_statuses
        ]
        terminated_container_states = [
            state.terminated
            for state in main_container_states
            if state and state.terminated
        ]
        non_zero_exit_codes = [
            state.exit_code for state in terminated_container_states if state.exit_code
        ]
        if len(non_zero_exit_codes) != 1:
            _logger.warning(
                f"LaunchedKubernetesJob.exit_code: Expected exactly 1 non-zero exit code for failed job, but got {non_zero_exit_codes}."
            )
        if non_zero_exit_codes:
            # Returning 1st non-zero exit code.
            # There should only be one.
            return non_zero_exit_codes[0]
        # We should not reach here. Failed jobs should have at least one non-zero exit code.
        # But just in case, we return None instead of 0 to indicate that exit code is unknown.
        return None

    @property
    def has_ended(self) -> bool:
        return self.has_succeeded or self.has_failed

    @property
    def has_succeeded(self) -> bool:
        return self.status == interfaces.ContainerStatus.SUCCEEDED

    @property
    def has_failed(self) -> bool:
        return self.status == interfaces.ContainerStatus.FAILED

    @property
    def started_at(self) -> datetime.datetime | None:
        job_status = self._debug_job.status
        if not job_status:
            return None
        return job_status.start_time

    @property
    def ended_at(self) -> datetime.datetime | None:
        job = self._debug_job
        job_status = self._debug_job.status
        if not job_status:
            return None
        ended_condition_times = [
            condition.last_transition_time
            for condition in job_status.conditions or []
            if condition.type in ("Succeeded", "Failed") and condition.status == "True"
        ]
        if not ended_condition_times:
            return None
        return ended_condition_times[0]

    @property
    def launcher_error_message(self) -> str | None:
        # TODO: Implement: Collect termination messages from pods
        return None

    SERIALIZATION_ROOT_KEY = "kubernetes_job"

    def to_dict(self) -> dict[str, Any]:
        job_dict = _serialize_kubernetes_object_to_compact_dict(self._debug_job)
        pod_dicts = None
        if self._debug_pods is not None:
            pod_dicts = [
                _serialize_kubernetes_object_to_compact_dict(pod) if pod else None
                for pod in self._debug_pods.values()
            ]
        result = {
            self.SERIALIZATION_ROOT_KEY: dict(
                launched_container_class_name=self.__class__.__name__,
                job_name=self._job_name,
                namespace=self._namespace,
                cluster_server=self._cluster_server,
                output_uris=self._output_uris,
                log_uri=self._log_uri,
                debug_job=job_dict,
                debug_pod=pod_dicts,
            ),
        }
        return result

    @classmethod
    def from_dict(
        cls, d: dict[str, Any], launcher: _KubernetesJobLauncher | None = None
    ) -> LaunchedKubernetesJob:
        d = d[cls.SERIALIZATION_ROOT_KEY]
        debug_job = _kubernetes_deserialize(d["debug_job"], cls=k8s_client_lib.V1Job)
        debug_pod_dicts = d.get("debug_pods")
        debug_pods = None
        if debug_pod_dicts is not None:
            debug_pods = {
                pod_key: _kubernetes_deserialize(pod_dict, cls=k8s_client_lib.V1Pod)
                for pod_key, pod_dict in debug_pod_dicts.items()
            }
        return LaunchedKubernetesJob(
            job_name=d["job_name"],
            namespace=d["namespace"],
            cluster_server=d.get("cluster_server"),
            output_uris=d["output_uris"],
            log_uri=d["log_uri"],
            debug_job=debug_job,
            debug_pods=debug_pods,
            launcher=launcher,
        )

    def get_refreshed(self) -> "LaunchedKubernetesJob":
        launcher = self._get_launcher()
        batch_api_client = k8s_client_lib.BatchV1Api(api_client=launcher._api_client)
        job: k8s_client_lib.V1Job = batch_api_client.read_namespaced_job(
            name=self._job_name,
            namespace=self._namespace,
            _request_timeout=launcher._request_timeout,
        )
        # Refreshing the job pods. We do not strictly need them.
        # But this information is useful for debugging and it will also allow slightly better status reporting.
        core_api_client = k8s_client_lib.CoreV1Api(launcher._api_client)
        pod_list_response: k8s_client_lib.V1PodList = (
            core_api_client.list_namespaced_pod(
                namespace=self._namespace,
                label_selector=f"job-name={self._job_name}",
                watch=False,
                _request_timeout=launcher._request_timeout,
            )
        )
        pod_map: dict[str, k8s_client_lib.V1Pod] = {}
        if job.spec.completion_mode == "Indexed":
            for pod in pod_list_response.items:
                index_str = (
                    pod.metadata.annotations.get(
                        "batch.kubernetes.io/job-completion-index"
                    )
                    if pod.metadata and pod.metadata.annotations
                    else None
                )
                if index_str is None:
                    raise ValueError(
                        f"Pod {pod.metadata.name if pod.metadata else None} of job {self._job_name} does not have completion index annotation."
                    )
                pod_map[index_str] = pod
        else:
            for pod in pod_list_response.items:
                index_str: str = pod.metadata.name
                pod_map[index_str] = pod
        new_launched_container = copy.copy(self)
        new_launched_container._debug_job = job
        new_launched_container._debug_pods = pod_map
        return new_launched_container

    def _get_log_by_pod_key(self, pod_name: str) -> str:
        launcher = self._get_launcher()
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        log = core_api_client.read_namespaced_pod_log(
            name=pod_name,
            namespace=self._namespace,
            container=_MAIN_CONTAINER_NAME,
            timestamps=True,
            _request_timeout=launcher._request_timeout,
        )
        return log

    def _get_all_logs(self) -> dict[str, str]:
        logs = {
            pod_key: self._get_log_by_pod_key(pod.metadata.name)
            for pod_key, pod in self._debug_pods.items()
        }
        return logs

    def _merge_logs(self, logs: dict[str, str]) -> str:
        if not logs:
            return ""
        # If there is only one log, return it as is.
        if len(logs) == 1:
            return list(logs.values())[0]
        all_log_lines: list[str] = []
        for pod_key, log in logs.items():
            for line in log.splitlines():
                timestamp, _, line = line.partition(" ")
                all_log_lines.append(f"{timestamp} {pod_key} {line}")
        all_log_lines.sort(
            key=lambda line: line.partition(" ")[0]
        )  # Sorting by timestamp
        return "\n".join(all_log_lines) + "\n"

    def get_log(self) -> str:
        all_logs = self._get_all_logs()
        merged_log = self._merge_logs(all_logs)
        return merged_log

    def upload_log(self):
        all_logs = self._get_all_logs()
        merged_log = self._merge_logs(all_logs)

        # Uploading the merged log
        launcher = self._get_launcher()
        uri_writer = launcher._storage_provider.make_uri(self._log_uri).get_writer()
        uri_writer.upload_from_text(merged_log)

        # Uploading per-pod logs.
        # It's not ideal to construct new URIs ourselves. But Orchestrator only supports single log per container execution.
        for pod_key, log in all_logs.items():
            uri_writer = launcher._storage_provider.make_uri(
                self._log_uri + f".{pod_key}"
            ).get_writer()
            uri_writer.upload_from_text(log)

    def stream_log_lines(self) -> typing.Iterator[str]:
        # TODO: Implement proper streaming of multiple pods logs with merging lines by timestamp (which might require threading).
        # For now, we just stream logs of 1st pod.

        if not self._debug_pods:
            return
        first_pod = self._debug_pods.get("0") or next(iter(self._debug_pods.values()))
        chosen_pod_name: str = first_pod.metadata.name

        launcher = self._get_launcher()
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        stream = k8s_watch_lib.Watch().stream(
            core_api_client.read_namespaced_pod_log,
            name=chosen_pod_name,
            namespace=self._namespace,
            container=_MAIN_CONTAINER_NAME,
            timestamps=True,
            _request_timeout=launcher._request_timeout,
        )
        for line in stream:
            yield str(line) + "\n"

    def __str__(self) -> str:
        import pprint

        return pprint.pformat(self.to_dict())

    def terminate(self):
        launcher = self._get_launcher()
        batch_api_client = k8s_client_lib.BatchV1Api(api_client=launcher._api_client)
        batch_api_client.delete_namespaced_job(
            name=self._job_name,
            namespace=self._namespace,
            grace_period_seconds=10,
        )
        _logger.info(f"Terminated job {self._job_name} in namespace {self._namespace}")


class Local_Kubernetes_UsingHostPathStorage_KubernetesJobLauncher(
    _KubernetesJobLauncher
):
    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        # job_name_prefix: str = "task-",
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: PodPostProcessor | None = None,
    ):
        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_labels=pod_labels,
            pod_annotations=pod_annotations,
            pod_postprocessor=pod_postprocessor,
            _storage_provider=local_storage.LocalStorageProvider(),
            _create_volume_and_volume_mount=_create_volume_and_volume_mount_host_path,
        )


def _serialize_kubernetes_object_to_compact_dict(obj) -> dict[str, Any]:
    obj_dict = _kubernetes_serialize(obj)
    # Removing trash
    _remove_keys_with_none_values(obj_dict)
    obj_metadata: dict | None = obj_dict.get("metadata")
    if obj_metadata:
        obj_metadata.pop("managedFields", None)
    return obj_dict


def windows_path_to_docker_path(path: str) -> str:
    if os.name != "nt":
        return path

    path_obj = pathlib.Path(path)
    if not path_obj.is_absolute():
        path_obj = path_obj.resolve()

    path_parts = list(path_obj.parts)
    # Changing the drive syntax: "C:\" -> "c"
    path_parts[0] = path_parts[0][0].lower()
    # WSL2 Docker path fix. See https://stackoverflow.com/questions/62812948/volume-mounts-not-working-kubernetes-and-wsl-2-and-docker/63524931#63524931
    posix_path = pathlib.PurePosixPath("/run/desktop/mnt/host/", *path_parts)
    return str(posix_path)


def _kubernetes_serialize(obj) -> dict[str, Any]:
    shallow_client = k8s_client_lib.ApiClient.__new__(k8s_client_lib.ApiClient)
    return shallow_client.sanitize_for_serialization(obj)


def _kubernetes_deserialize(obj_dict: dict[str, Any], cls: typing.Type[_T]) -> _T:
    shallow_client = k8s_client_lib.ApiClient.__new__(k8s_client_lib.ApiClient)
    return shallow_client._ApiClient__deserialize(obj_dict, cls)


def _update_dict_recursively(d1: dict, d2: dict):
    for k, v2 in d2.items():
        if k in d1:
            v1 = d1[k]
            if isinstance(v1, dict) and isinstance(v2, dict):
                _update_dict_recursively(v1, v2)
                continue
            # elif isinstance(v1, list) and isinstance(v2, list):
            # # Merging lists is not supported yet
        d1[k] = v2


def _remove_keys_with_none_values(d: dict):
    for k, v in list(d.items()):
        if v is None:
            del d[k]
        if isinstance(v, dict):
            _remove_keys_with_none_values(v)
