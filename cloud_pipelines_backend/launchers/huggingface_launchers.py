import copy
import dataclasses
import datetime
import logging
import pathlib
import typing
from typing import Any, Optional

import huggingface_hub

from cloud_pipelines.orchestration.launchers import naming_utils
from ..storage_providers import huggingface_repo_storage
from .. import component_structures as structures
from . import container_component_utils
from . import interfaces


_logger = logging.getLogger(__name__)

_MAX_INPUT_VALUE_SIZE = 10000

_CONTAINER_FILE_NAME = "data"


class HuggingFaceJobsContainerLauncher(
    interfaces.ContainerTaskLauncher["LaunchedHuggingFaceJobContainer"]
):
    """Launcher that uses HuggingFace Jobs installed locally"""

    def __init__(
        self,
        *,
        client: Optional[huggingface_hub.HfApi] = None,
        namespace: Optional[str] = None,
        hf_token: Optional[str] = None,
        hf_job_token: Optional[str] = None,
        job_timeout: Optional[int | float | str] = None,
    ):
        # The HF Jobs that we launch need token to write the output artifacts and logs
        hf_token = hf_token or huggingface_hub.get_token()
        hf_job_token = hf_job_token or hf_token
        self._api_client = client or huggingface_hub.HfApi(token=hf_token)
        self._namespace: str = namespace or self._api_client.whoami()["name"]
        self._storage_provider = (
            huggingface_repo_storage.HuggingFaceRepoStorageProvider(
                client=self._api_client
            )
        )
        self._job_timeout = job_timeout
        self._hf_job_token = hf_job_token

    def self_check(self):
        _ = self._api_client.list_jobs(namespace=self._namespace)

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        # Input arguments may be updated with new downloaded values and new URIs of uploaded values.
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> "LaunchedHuggingFaceJobContainer":
        if not isinstance(
            component_spec.implementation, structures.ContainerImplementation
        ):
            raise TypeError(
                f"Container launchers only support container implementations. Got {component_spec=}"
            )
        container_spec = component_spec.implementation.container

        # TODO: Validate the input/output URIs.
        container_inputs_root = pathlib.PurePosixPath("/tmp/component/inputs")
        container_outputs_root = pathlib.PurePosixPath("/tmp/component/outputs")

        # download_input_uris: dict[huggingface_repo_storage.HuggingFaceRepoUri, str] = {}
        # upload_output_uris: dict[str, huggingface_repo_storage.HuggingFaceRepoUri] = {}
        download_input_uris: dict[str, str] = {}
        upload_output_uris: dict[str, str] = {}
        # TODO: Derive common prefix for the upload_output_uris (also log_uri) and upload everything at once

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
            # hf_uri = huggingface_repo_storage.HuggingFaceRepoUri.parse(uri)
            download_input_uris[uri] = container_path
            return container_path

        def get_output_path(output_name: str) -> str:
            uri = output_uris[output_name]
            # container_path = (
            #     container_outputs_root
            #     / naming_utils.sanitize_file_name(output_name)
            #     / _CONTAINER_FILE_NAME
            # ).as_posix()
            hf_uri = huggingface_repo_storage.HuggingFaceRepoUri.parse(uri)
            uri_path_in_repo = hf_uri.path
            container_path = str(container_outputs_root / uri_path_in_repo)
            upload_output_uris[container_path] = uri
            return container_path

        def get_log_path() -> str:
            # TODO: Use common URI here
            hf_uri = huggingface_repo_storage.HuggingFaceRepoUri.parse(log_uri)
            uri_path_in_repo = hf_uri.path
            container_path = str(container_outputs_root / uri_path_in_repo)
            return container_path

        def get_exit_code_path() -> str:
            # TODO: Use common URI here
            hf_uri = huggingface_repo_storage.HuggingFaceRepoUri.parse(log_uri)
            uri_path_in_repo = hf_uri.path
            container_path = str(
                (container_outputs_root / uri_path_in_repo).with_name("exit_code.txt")
            )
            return container_path

        container_log_path = get_log_path()
        exit_code_path = get_exit_code_path()

        # Resolving the command line.
        # Also indirectly populates volumes and volume_mounts.
        resolved_cmd = container_component_utils.resolve_container_command_line(
            component_spec=component_spec,
            provided_input_names=set(input_arguments.keys()),
            get_input_value=get_input_value,
            get_input_path=get_input_path,
            get_output_path=get_output_path,
        )

        # Preparing the artifact uploader wrapper
        # TODO: Use common URI here
        # TODO: Add: --commit-message '{path_in_repo}' once path_in_repo becomes non-empty
        path_in_repo = ""
        # commit_message = path_in_repo

        hf_repo_uri = huggingface_repo_storage.HuggingFaceRepoUri.parse(log_uri)

        # It's hard to download data from HuggingFace.
        # First, there is no way to download a directory:
        # 1. The CLI only downloads to cache. So we have to correctly find the data location in the cache sna copy the data out.
        # 2. We cannot specify which directory to download, so we have to use the --include filter.
        # But `hf download --include path` has an issue that it cannot download a directory unless path ends with slash (and for files, there should be no slash).
        # Adding "*" works for both files and directories. It's imperfect, but fine.
        # Another problem: The files in the snapshot are actually symlinks.
        # cp options:
        # -H                           follow command-line symbolic links in SOURCE
        # -l, --link                   hard link files instead of copying
        # -L, --dereference            always follow symbolic links in SOURCE
        input_download_lines = [
            f'mkdir -p "$(dirname "{container_path}")"'
            f' && snapshot_dir=`hf download --repo-type "{hf_uri.repo_type}" "{hf_uri.repo_id}" --include "{hf_uri.path}*"`'
            f' && cp -r -L "$snapshot_dir/{hf_uri.path}" "{container_path}"'
            for hf_uri, container_path in (
                (huggingface_repo_storage.HuggingFaceRepoUri.parse(uri), container_path)
                for uri, container_path in download_input_uris.items()
            )
        ]
        input_download_code = "\n".join(input_download_lines)

        artifact_uploader_script = f"""
set -e -x
# Workaround for Dash and other shells that do not support -o pipefail.
if (set -o pipefail 2>/dev/null); then
    set -o pipefail
fi

# Installing uv
url="https://astral.sh/uv/install.sh"
if command -v curl 2>/dev/null; then
    # -s: silent, -L: follow redirects
    curl -s -L "$url" | sh
elif command -v wget 2>/dev/null; then
    wget -q -O - "$url" | sh
else
    echo "Error: Neither curl nor wget was found. Trying apt-get install" >&2
    apt-get update --quiet && apt-get install -y --no-install-recommends --quiet curl
    curl -s -L "$url" | sh
fi

export PATH="$HOME/.local/bin:$PATH"

uv tool install 'huggingface_hub>=1.0.0' --python '>=3.9'
hf version

# Downloading the input data
{input_download_code}

# Running the program
log_path='{container_log_path}'
exit_code_path='{exit_code_path}'
mkdir -p "$(dirname "$log_path")"
mkdir -p "$(dirname "$exit_code_path")"
# We need to capture the exit code while piping the stderr and stdout to a log file. Not all shells support `${{PIPEFAIL[0]}}`
set +e +x
{{ "$0" "$@"; echo $? >"$exit_code_path";}} 2>&1 | tee "$log_path"
set -e +x

exit_code=`cat "$exit_code_path"`

hf upload --repo-type '{hf_repo_uri.repo_type}' '{hf_repo_uri.repo_id}' '{container_outputs_root}' '{path_in_repo}'
exit "$exit_code"
"""

        container_env = container_spec.env or {}

        # Passing HF token to the Job
        secrets: dict[str, str] = {}
        if self._hf_job_token:
            secrets["HF_TOKEN"] = self._hf_job_token

        command_line = list(resolved_cmd.command or []) + list(resolved_cmd.args or [])
        command_line = ["sh", "-c", artifact_uploader_script] + command_line
        job = self._api_client.run_job(
            image=container_spec.image,
            command=command_line,
            env=dict(container_env),
            timeout=self._job_timeout,
            namespace=self._namespace,
            secrets=secrets,
            # flavor=...,
        )

        _logger.info(f"Launched HF Job {job.id=}, {job.url=}")
        launched_container = LaunchedHuggingFaceJobContainer(
            id=job.id,
            namespace=self._namespace,
            job=job,
            output_uris=output_uris,
            log_uri=log_uri,
        )
        return launched_container

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict[str, Any]
    ) -> "LaunchedHuggingFaceJobContainer":
        launched_container = LaunchedHuggingFaceJobContainer.from_dict(
            launched_container_dict, api_client=self._api_client
        )
        return launched_container

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict[str, Any]
    ) -> "LaunchedHuggingFaceJobContainer":
        launched_container = LaunchedHuggingFaceJobContainer.from_dict(
            launched_container_dict, api_client=self._api_client
        )
        job = self._api_client.inspect_job(
            job_id=launched_container.id,
            namespace=launched_container._namespace,
        )
        new_launched_container = copy.copy(launched_container)
        new_launched_container._job = job
        return new_launched_container


class LaunchedHuggingFaceJobContainer(interfaces.LaunchedContainer):
    def __init__(
        self,
        id: str,
        namespace: str,
        job: huggingface_hub.JobInfo,
        output_uris: dict[str, str],
        log_uri: str,
        api_client: huggingface_hub.HfApi | None = None,
    ):
        self._id: str = id
        self._namespace: str = namespace
        self._job = job
        self._output_uris: dict[str, str] = output_uris
        self._log_uri: str = log_uri
        self._api_client: huggingface_hub.HfApi | None = api_client

    def _get_api_client(self):
        if not self._api_client:
            raise interfaces.LauncherError(
                "This action requires an API client, but this instance was constructed without one."
            )
        return self._api_client

    @property
    def id(self) -> str:
        return self._id

    @property
    def status(self) -> interfaces.ContainerStatus:
        status_str = self._job.status.stage
        # status_message = self._job.status.message
        if status_str == huggingface_hub.JobStage.RUNNING:
            return interfaces.ContainerStatus.RUNNING
        elif status_str == huggingface_hub.JobStage.COMPLETED:
            return interfaces.ContainerStatus.SUCCEEDED
        elif status_str == huggingface_hub.JobStage.ERROR:
            return interfaces.ContainerStatus.FAILED
        elif status_str == huggingface_hub.JobStage.CANCELED:
            return interfaces.ContainerStatus.FAILED
        else:  # "DELETED"
            return interfaces.ContainerStatus.ERROR

    @property
    def exit_code(self) -> Optional[int]:
        # HF Jobs do not provide exit code
        if not self.has_ended:
            return None
        return None

    @property
    def has_ended(self) -> bool:
        return self.status in (
            interfaces.ContainerStatus.SUCCEEDED,
            interfaces.ContainerStatus.FAILED,
            interfaces.ContainerStatus.ERROR,
        )

    @property
    def has_succeeded(self) -> bool:
        return self.status == interfaces.ContainerStatus.SUCCEEDED

    @property
    def has_failed(self) -> bool:
        return self.status == interfaces.ContainerStatus.FAILED

    @property
    def started_at(self) -> datetime.datetime | None:
        # HF Jobs do not provide started_at, so using created_at
        return self._job.created_at

    @property
    def ended_at(self) -> datetime.datetime | None:
        # HF Jobs do not provide ended_at
        # Fudging the value by returning the current time.
        if self.has_ended:
            return datetime.datetime.now(datetime.timezone.utc)
        return None

    @property
    def launcher_error_message(self) -> str | None:
        if self._job.status.message:
            # TODO: Check what kind of messages this returns and when.
            _logger.info(
                f"launcher_error_message: {self._id=}: {self._job.status.message=}"
            )
            return self._job.status.message
        return None

    def get_log(self) -> str:
        if self.has_ended:
            try:
                return (
                    huggingface_repo_storage.HuggingFaceRepoStorageProvider(
                        client=self._get_api_client()
                    )
                    .make_uri(self._log_uri)
                    .get_reader()
                    .download_as_text()
                )
            except Exception as ex:
                _logger.warning(
                    f"get_log: {self._id=}: Error getting log from URI: {self._log_uri}",
                    ex,
                )
        return "\n".join(
            self._get_api_client().fetch_job_logs(
                job_id=self._id,
            )
        )

    def upload_log(self):
        # Logs should be uploaded automatically by the modified command-line wrapper
        pass

    def stream_log_lines(self) -> typing.Iterator[str]:
        return (
            self._get_api_client()
            .fetch_job_logs(
                job_id=self._id,
                namespace=self._namespace,
            )
            .__iter__()
        )

    def terminate(self):
        self._get_api_client().cancel_job(job_id=self._id, namespace=self._namespace)

    def to_dict(self) -> dict[str, Any]:
        debug_job_info = dataclasses.asdict(self._job)
        # Fix JSON serialization of datetime
        del debug_job_info["created_at"]
        return dict(
            huggingface_job=dict(
                id=self.id,
                namespace=self._namespace,
                output_uris=self._output_uris,
                log_uri=self._log_uri,
                # For debugging purposes, not needed otherwise
                debug_job_info=debug_job_info,
            )
        )

    @classmethod
    def from_dict(
        cls,
        d: dict[str, Any],
        api_client: huggingface_hub.HfApi | None = None,
    ) -> "LaunchedHuggingFaceJobContainer":
        container_dict = d["huggingface_job"]
        job_info = huggingface_hub.JobInfo(
            **container_dict["debug_job_info"],
        )
        return LaunchedHuggingFaceJobContainer(
            id=container_dict["id"],
            namespace=container_dict["namespace"],
            job=job_info,
            output_uris=container_dict["output_uris"],
            log_uri=container_dict["log_uri"],
            api_client=api_client,
        )
