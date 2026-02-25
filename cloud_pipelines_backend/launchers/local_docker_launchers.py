import copy
import datetime
import logging
import pathlib
import typing
from typing import Any

import docker
import docker.models.containers
import docker.types
from cloud_pipelines.orchestration.launchers import naming_utils
from cloud_pipelines.orchestration.storage_providers import local_storage

from .. import component_structures as structures
from . import container_component_utils, interfaces

_logger = logging.getLogger(__name__)

_MAX_INPUT_VALUE_SIZE = 10000

_CONTAINER_FILE_NAME = "data"


def _remove_keys_with_none_values(d: dict):
    for k, v in list(d.items()):
        if v is None:
            del d[k]
        if isinstance(v, dict):
            _remove_keys_with_none_values(v)


def _construct_docker_volume_mount(
    container_path: str,
    artifact_uri: str,
    read_only: bool = False,
) -> docker.types.Mount:
    container_path_obj = pathlib.PurePosixPath(container_path)
    host_path_obj = pathlib.Path(artifact_uri)
    if not container_path_obj.is_absolute():
        raise ValueError(
            f"When creating a mount, container path must be absolute, but got {container_path}."
        )
    # Maybe it's OK to allow relative paths. (To enable portable self-contained local DB+data directory.)
    # if not host_path_obj.is_absolute():
    #     raise ValueError(f"When creating a mount, host path must be absolute, but got {host_path_obj}.")
    host_path_obj = host_path_obj.resolve()
    if container_path_obj.name != host_path_obj.name:
        raise interfaces.LauncherError(
            "Container file name is different from artifact file name. {container_path=}, {artifact_uri=}"
        )
    container_path_dir = str(container_path_obj.parent)
    host_path_dir = str(host_path_obj.parent)

    return docker.types.Mount(
        type="bind",
        source=host_path_dir,
        target=container_path_dir,
        read_only=read_only,
    )


class DockerContainerLauncher(
    interfaces.ContainerTaskLauncher["LaunchedDockerContainer"]
):
    """Launcher that uses Docker installed locally"""

    def __init__(self, client: docker.DockerClient | None = None):
        try:
            self._docker_client = client or docker.from_env(timeout=5)
        except Exception as ex:
            raise RuntimeError(
                "Docker does not seem to be working."
                " Please make sure that `docker version` executes without errors."
                " Docker can be installed from https://docs.docker.com/get-docker/."
            ) from ex

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        # Input arguments may be updated with new downloaded values and new URIs of uploaded values.
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> "LaunchedDockerContainer":
        container_spec = component_spec.implementation.container
        input_names = list(input_arguments.keys())
        output_names = list(output_uris.keys())

        # TODO: Validate the output URIs. Don't forget about (`C:\*` and `C:/*` paths)

        container_inputs_root = pathlib.PurePosixPath("/tmp/inputs")
        container_outputs_root = pathlib.PurePosixPath("/tmp/outputs")

        mounts: list[docker.types.Mount] = []

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

            mounts.append(
                _construct_docker_volume_mount(
                    container_path=container_path,
                    artifact_uri=uri,
                    read_only=True,
                )
            )
            return container_path

        def get_output_path(output_name: str) -> str:
            uri = output_uris[output_name]
            container_path = (
                container_outputs_root
                / naming_utils.sanitize_file_name(output_name)
                / _CONTAINER_FILE_NAME
            ).as_posix()
            mounts.append(
                _construct_docker_volume_mount(
                    container_path=container_path,
                    artifact_uri=uri,
                    read_only=False,
                )
            )
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

        # Preparing the output locations
        for output_host_path in output_uris.values():
            pathlib.Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

        container_env = container_spec.env or {}

        container = self._docker_client.containers.run(
            image=container_spec.image,
            entrypoint=resolved_cmd.command,
            command=resolved_cmd.args,
            environment=container_env,
            mounts=mounts,
            detach=True,
        )
        _logger.debug(f"Launched container {container.id=}")
        launched_container = LaunchedDockerContainer(
            id=container.id,
            container=container,
            output_uris=output_uris,
            log_uri=log_uri,
        )
        return launched_container

    @property
    def _storage_provider(self):
        return local_storage.LocalStorageProvider()

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedDockerContainer":
        launched_container = LaunchedDockerContainer.from_dict(
            launched_container_dict, docker_client=self._docker_client
        )
        return launched_container

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedDockerContainer":
        launched_container = LaunchedDockerContainer.from_dict(
            launched_container_dict, docker_client=self._docker_client
        )
        container = self._docker_client.containers.get(
            container_id=launched_container.id
        )
        new_launched_container = copy.copy(launched_container)
        new_launched_container._container = container
        return new_launched_container


class LaunchedDockerContainer(interfaces.LaunchedContainer):
    def __init__(
        self,
        id: str,
        container: docker.models.containers.Container,
        output_uris: dict[str, str],
        log_uri: str,
    ):
        self._id = id
        self._container = container
        self._output_uris = output_uris
        self._log_uri = log_uri

    @property
    def id(self) -> str:
        return self._id

    @property
    def status(self) -> interfaces.ContainerStatus:
        status_str = self._container.attrs["State"]["Status"]
        if status_str == "created":
            return interfaces.ContainerStatus.PENDING
        elif status_str == "running":
            return interfaces.ContainerStatus.RUNNING
        elif status_str == "exited":
            if self._container.attrs["State"]["ExitCode"] == 0:
                return interfaces.ContainerStatus.SUCCEEDED
            else:
                return interfaces.ContainerStatus.FAILED
        else:  # "paused", "restarting", "removing", "dead"
            return interfaces.ContainerStatus.ERROR

    @property
    def exit_code(self) -> int | None:
        if not self.has_ended:
            return None
        return self._container.attrs["State"]["ExitCode"]

    @property
    def has_ended(self) -> bool:
        return self._container.attrs["State"]["Status"] == "exited"

    @property
    def has_succeeded(self) -> bool:
        return self.has_ended and self.exit_code == 0

    @property
    def has_failed(self) -> bool:
        return self.has_ended and self.exit_code != 0

    @property
    def started_at(self) -> datetime.datetime | None:
        started_at = self._container.attrs["State"].get("StartedAt")
        if started_at:
            return _parse_docker_time(started_at)
        return None

    @property
    def ended_at(self) -> datetime.datetime | None:
        finished_at = self._container.attrs["State"].get("FinishedAt")
        if finished_at:
            return _parse_docker_time(finished_at)
        return None

    @property
    def launcher_error_message(self) -> str | None:
        if self.status == interfaces.ContainerStatus.ERROR:
            return "Docker error."
        return None

    def get_log(self) -> str:
        return self._container.logs(stdout=True, stderr=True, timestamps=True).decode(
            "utf-8", errors="replace"
        )

    def upload_log(self):
        log = self.get_log()
        uri_writer = (
            local_storage.LocalStorageProvider().make_uri(self._log_uri).get_writer()
        )
        uri_writer.upload_from_text(log)

    def stream_log_lines(self) -> typing.Iterator[str]:
        for log_bytes in self._container.logs(
            stdout=True, stderr=True, stream=True, follow=True
        ):
            yield log_bytes.decode("utf-8", errors="replace")

    def terminate(self):
        self._container.stop(timeout=10)

    def to_dict(self) -> dict[str, Any]:
        return dict(
            docker=dict(
                id=self.id,
                output_uris=self._output_uris,
                log_uri=self._log_uri,
                # For debugging purposes, not needed otherwise
                debug_container=self._container.attrs,
            )
        )

    @classmethod
    def from_dict(
        cls,
        d: dict[str, Any],
        docker_client: docker.DockerClient | None = None,
    ) -> "LaunchedDockerContainer":
        docker_dict = d["docker"]
        id = docker_dict["id"]
        output_uris = docker_dict["output_uris"]
        log_uri = docker_dict["log_uri"]
        container = docker.models.containers.Container(
            attrs=docker_dict["debug_container"],
            client=docker_client,
        )
        return LaunchedDockerContainer(
            id=id,
            container=container,
            output_uris=output_uris,
            log_uri=log_uri,
        )

def _parse_docker_time(date_string: str) -> datetime.datetime:
    # Workaround for Python <3.11 failing to parse timestamps that include nanoseconds:
    # datetime.datetime.fromisoformat("2025-10-07T04:48:35.585991509+00:00")
    # >>> ValueError: Invalid isoformat string: '2025-10-07T04:48:35.585991509+00:00'
    # datetime.datetime.strptime("2025-10-07T04:48:35.5859915+00:00", "%Y-%m-%dT%H:%M:%S.%f%z")'
    # >>> ValueError: time data '2025-10-07T04:48:35.5859915+00:00' does not match format '%Y-%m-%dT%H:%M:%S.%f%z'

    # The timestamp is considered to be UTC
    # date_string = date_string.replace("Z", "+00:00")
    return datetime.datetime.strptime(date_string[0:26], "%Y-%m-%dT%H:%M:%S.%f")
