"""SkyPilot launcher for Tangle pipelines.

Translates Tangle's ContainerTaskLauncher contract into sky.jobs managed-job
submissions. SkyPilot then handles container scheduling, multi-cloud /
multi-cluster placement, multi-node coordination, preemption recovery, log
streaming, and cancellation.

Layout follows the existing launchers in cloud_pipelines_backend/launchers/
(local_docker_launchers.py, kubernetes_launchers.py).

Storage provider compatibility
==============================

This launcher relies on SkyPilot's ``file_mounts`` for input/output artifact
transfer, which can mount cloud-storage URIs (``gs://``, ``s3://``, ``abfs://``,
``r2://``, ``https://``) directly into the container but cannot represent the
relative-local-path artifact URIs produced by Tangle's
``LocalStorageProvider``.

Practical consequences:

  * Single-component pipelines run end-to-end on any storage provider — the
    container's stdout/stderr is captured by SkyPilot regardless. Components
    with file outputs that target a local URI will simply have those outputs
    discarded with a warning (the container still executes).

  * Multi-component (graph) pipelines require a cloud StorageProvider. With
    ``LocalStorageProvider``, downstream tasks cannot read the upstream
    output (the SkyPilot pod can't write back to the orchestrator's local
    filesystem), so step 2 fails to find its input.

To run multi-step pipelines, configure Tangle with a cloud storage provider,
for example::

    from cloud_pipelines.orchestration.storage_providers.google_cloud_storage \\
        import GoogleCloudStorageProvider

    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        ...,
        launcher=SkyPilotKubernetesLauncher(infra="kubernetes/<context>"),
        storage_provider=GoogleCloudStorageProvider(),
        data_root_uri="gs://my-tangle-bucket/artifacts",
        logs_root_uri="gs://my-tangle-bucket/logs",
    )
"""

from __future__ import annotations

import dataclasses
import datetime
import io
import json
import logging
import shlex
import threading
from typing import Any, Iterator, Optional

from cloud_pipelines.orchestration.launchers import naming_utils
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import (
    container_component_utils,
    interfaces,
    kubernetes_launchers as _k8s_launchers,
)

import sky
from sky import jobs as sky_jobs

_logger = logging.getLogger(__name__)

_MAX_INPUT_VALUE_SIZE = 10000
_CONTAINER_FILE_NAME = "data"
# SkyPilot itself does not impose an upper bound on num_nodes (only the cloud
# quota does). We keep a sanity cap that is significantly higher than Tangle's
# kubernetes_launchers cap of 16; raise it freely if you have larger jobs.
_MULTI_NODE_MAX_NUMBER_OF_NODES = 256

# Re-use the resource annotation keys from kubernetes_launchers so a Tangle
# component spec is portable across the K8s launcher and this one.
RESOURCES_CPU_ANNOTATION_KEY = _k8s_launchers.RESOURCES_CPU_ANNOTATION_KEY
RESOURCES_MEMORY_ANNOTATION_KEY = _k8s_launchers.RESOURCES_MEMORY_ANNOTATION_KEY
RESOURCES_ACCELERATORS_ANNOTATION_KEY = (
    _k8s_launchers.RESOURCES_ACCELERATORS_ANNOTATION_KEY
)
RESOURCES_EPHEMERAL_STORAGE_ANNOTATION_KEY = (
    _k8s_launchers.RESOURCES_EPHEMERAL_STORAGE_ANNOTATION_KEY
)
MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY = (
    _k8s_launchers.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY
)

# SkyPilot-specific annotation keys (opt-in).
PRIORITY_CLASS_ANNOTATION_KEY = "skypilot.co/launchers/skypilot/priority_class"
SPOT_ANNOTATION_KEY = "skypilot.co/launchers/skypilot/use_spot"

# Tangle's multi-node dynamic-data keys (mirror of kubernetes_launchers).
_MULTI_NODE_NUMBER_OF_NODES_DYNAMIC_DATA_KEY = "system/multi_node/number_of_nodes"
_MULTI_NODE_NODE_INDEX_DYNAMIC_DATA_KEY = "system/multi_node/node_index"
_MULTI_NODE_NODE_0_ADDRESS_DYNAMIC_DATA_KEY = "system/multi_node/node_0_address"
_MULTI_NODE_ALL_NODE_ADDRESSES_DYNAMIC_DATA_KEY = "system/multi_node/all_node_addresses"

# ManagedJobStatus (str-valued enum or string) -> Tangle ContainerStatus.
_TERMINAL_STATUSES = frozenset(
    {
        "SUCCEEDED",
        "CANCELLED",
        "FAILED",
        "FAILED_SETUP",
        "FAILED_PRECHECKS",
        "FAILED_NO_RESOURCE",
        "FAILED_CONTROLLER",
    }
)
_STATUS_MAP: dict[str, interfaces.ContainerStatus] = {
    "PENDING": interfaces.ContainerStatus.PENDING,
    "STARTING": interfaces.ContainerStatus.PENDING,
    "RUNNING": interfaces.ContainerStatus.RUNNING,
    "RECOVERING": interfaces.ContainerStatus.RUNNING,
    "WINDING_DOWN": interfaces.ContainerStatus.RUNNING,
    "CANCELLING": interfaces.ContainerStatus.RUNNING,
    "SUCCEEDED": interfaces.ContainerStatus.SUCCEEDED,
    "CANCELLED": interfaces.ContainerStatus.FAILED,
    "FAILED": interfaces.ContainerStatus.FAILED,
    "FAILED_SETUP": interfaces.ContainerStatus.FAILED,
    "FAILED_PRECHECKS": interfaces.ContainerStatus.FAILED,
    "FAILED_NO_RESOURCE": interfaces.ContainerStatus.FAILED,
    "FAILED_CONTROLLER": interfaces.ContainerStatus.ERROR,
}


def _status_to_string(status: Any) -> str:
    if status is None:
        return "PENDING"
    if hasattr(status, "value"):
        return str(status.value)
    return str(status)


def _shell_quote_argv(argv: list[str]) -> str:
    return " ".join(shlex.quote(p) for p in argv)


# Bash prelude that bridges Tangle's multi-node contract to SkyPilot's runtime
# env vars. Lets a component's command line reference $TANGLE_MULTI_NODE_*
# without caring whether SkyPilot or Kubernetes is the launcher.
_MULTI_NODE_ENV_PRELUDE = (
    'export TANGLE_MULTI_NODE_NUMBER_OF_NODES="${SKYPILOT_NUM_NODES:-1}"\n'
    'export TANGLE_MULTI_NODE_NODE_INDEX="${SKYPILOT_NODE_RANK:-0}"\n'
    'export TANGLE_MULTI_NODE_NODE_0_ADDRESS="$(echo "${SKYPILOT_NODE_IPS:-localhost}" | head -n1)"\n'
    'export TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES="${SKYPILOT_NODE_IPS:-localhost}"\n'
)


@dataclasses.dataclass
class _SkyPilotJobHandle:
    job_id: int
    job_name: str
    output_uris: dict[str, str]
    log_uri: str
    cached_status: Optional[str] = None
    cached_failure_reason: Optional[str] = None
    cached_started_at: Optional[float] = None
    cached_ended_at: Optional[float] = None


def _coerce_disk_size_gb(spec: Any) -> int:
    """Best-effort parse of an ephemeral-storage annotation into GiB."""
    if isinstance(spec, (int, float)):
        return max(1, int(spec))
    s = str(spec).strip().lower()
    multipliers = {
        "ti": 1024.0, "gi": 1.0, "mi": 1 / 1024, "ki": 1 / 1024 / 1024,
        "t": 1000.0, "g": 1.0, "m": 1 / 1000, "k": 1 / 1000 / 1000,
    }
    for suffix, mult in sorted(multipliers.items(), key=lambda kv: -len(kv[0])):
        if s.endswith(suffix):
            try:
                return max(1, int(float(s[: -len(suffix)]) * mult))
            except ValueError:
                continue
    try:
        return max(1, int(float(s)))
    except ValueError:
        return 8


class SkyPilotKubernetesLauncher(
    interfaces.ContainerTaskLauncher["SkyPilotLaunchedJob"]
):
    """Launches Tangle container tasks via SkyPilot managed jobs.

    Designed for Kubernetes-only deployments (the Shopify use case) but works
    against any infra SkyPilot supports. Set ``infra="kubernetes"`` (or
    ``"kubernetes/<context>"``) to keep behavior aligned with the existing
    KubernetesWithGcsFuseContainerLauncher; pass ``infra=None`` to let
    SkyPilot's optimizer pick across any clouds the user has configured.
    """

    def __init__(
        self,
        *,
        infra: Optional[str] = "kubernetes",
        pool: Optional[str] = None,
        default_image: Optional[str] = None,
        default_labels: Optional[dict[str, str]] = None,
        default_envs: Optional[dict[str, str]] = None,
        annotation_to_label_keys: Optional[list[str]] = None,
        priority_class: Optional[str] = None,
        use_spot: Optional[bool] = None,
        job_name_prefix: str = "tangle-",
    ):
        """
        Args:
            infra: SkyPilot infra string. ``"kubernetes"`` for any K8s context;
                ``"kubernetes/<context>"`` to pin to one cluster; ``None`` to
                let the optimizer pick across all configured clouds.
            pool: Optional SkyPilot Pool name. Submitting to a warm Pool gives
                much faster cold-start than full provisioning.
            default_image: Fallback container image when ComponentSpec doesn't
                specify one.
            default_labels: Labels applied to every Sky resource (propagated to
                K8s pod labels under the kubernetes infra).
            default_envs: Env vars injected into every container.
            annotation_to_label_keys: Tangle annotation keys whose values are
                copied into Sky labels. Useful for passing through things like
                ``ml.shopify.io/priority-class`` so the K8s pod ends up with the
                same label kueue is configured to read.
            priority_class: Default SkyPilot priority class (Kueue-compatible).
                Can be overridden per-task via the ``PRIORITY_CLASS_ANNOTATION_KEY``
                annotation on a ComponentSpec.
            use_spot: Default spot-instance preference. Per-task override via
                ``SPOT_ANNOTATION_KEY``.
            job_name_prefix: Prefix for SkyPilot managed job names.
        """
        self._infra = infra
        self._pool = pool
        self._default_image = default_image
        self._default_labels = dict(default_labels or {})
        self._default_envs = dict(default_envs or {})
        self._annotation_to_label_keys = list(annotation_to_label_keys or [])
        self._default_priority_class = priority_class
        self._default_use_spot = use_spot
        self._job_name_prefix = job_name_prefix
        # Serialize submissions; sky's SDK is safe to call concurrently but the
        # orchestrator may invoke launch_container_task from many workers, and
        # serializing keeps log_uri / file_mount conflict checks deterministic.
        self._lock = threading.Lock()

    # ----------------- ContainerTaskLauncher contract -----------------

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> "SkyPilotLaunchedJob":
        if not isinstance(
            component_spec.implementation, structures.ContainerImplementation
        ):
            raise interfaces.LauncherError(
                f"Component must have container implementation. {component_spec=}"
            )
        container_spec = component_spec.implementation.container
        annotations = dict(annotations or {})

        task = self._build_task(
            component_spec=component_spec,
            container_spec=container_spec,
            input_arguments=input_arguments,
            output_uris=output_uris,
            annotations=annotations,
        )
        job_name = task.name or self._job_name_prefix + "task"

        # Submit. sky.jobs.launch returns a RequestId; await it via sky.get().
        # Result shape changed across sky versions: older returns (List[int], Handle),
        # newer (>=0.12) returns (Optional[int], Handle). Handle both.
        with self._lock:
            launch_kwargs: dict[str, Any] = {}
            if self._pool is not None:
                launch_kwargs["pool"] = self._pool
            request_id = sky_jobs.launch(task, name=job_name, **launch_kwargs)
            result = sky.get(request_id)

        job_id_or_ids, _handle = result
        if job_id_or_ids is None:
            raise interfaces.LauncherError(
                f"sky.jobs.launch returned no job id for {job_name}"
            )
        if isinstance(job_id_or_ids, (list, tuple)):
            if not job_id_or_ids:
                raise interfaces.LauncherError(
                    f"sky.jobs.launch returned empty job-id list for {job_name}"
                )
            job_id = int(job_id_or_ids[0])
        else:
            job_id = int(job_id_or_ids)
        _logger.info(
            "Submitted SkyPilot managed job %s (job_id=%d)", job_name, job_id
        )

        return SkyPilotLaunchedJob(
            handle=_SkyPilotJobHandle(
                job_id=job_id,
                job_name=job_name,
                output_uris=dict(output_uris),
                log_uri=log_uri,
            )
        )

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "SkyPilotLaunchedJob":
        return SkyPilotLaunchedJob.from_dict(launched_container_dict)

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "SkyPilotLaunchedJob":
        return SkyPilotLaunchedJob.from_dict(launched_container_dict).get_refreshed()

    # ----------------- ComponentSpec -> sky.Task translation -----------------

    def _build_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        container_spec: structures.ContainerSpec,
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        annotations: dict[str, Any],
    ) -> sky.Task:
        # Resources
        cpus = annotations.get(RESOURCES_CPU_ANNOTATION_KEY)
        memory = annotations.get(RESOURCES_MEMORY_ANNOTATION_KEY)
        accelerators = annotations.get(RESOURCES_ACCELERATORS_ANNOTATION_KEY)
        ephemeral_storage = annotations.get(
            RESOURCES_EPHEMERAL_STORAGE_ANNOTATION_KEY
        )

        # Multi-node count
        num_nodes_str = annotations.get(MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY, "1")
        try:
            num_nodes = int(num_nodes_str)
        except ValueError as ex:
            raise interfaces.LauncherError(
                f"Invalid {MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY}={num_nodes_str!r}"
            ) from ex
        if not (1 <= num_nodes <= _MULTI_NODE_MAX_NUMBER_OF_NODES):
            raise interfaces.LauncherError(
                f"num_nodes must be in [1, {_MULTI_NODE_MAX_NUMBER_OF_NODES}], "
                f"got {num_nodes}"
            )

        # Labels: defaults, plus selected annotations propagated as labels.
        labels = dict(self._default_labels)
        for ann_key in self._annotation_to_label_keys:
            if ann_key in annotations:
                sky_label_key = (
                    ann_key.replace("/", "_").replace(".", "_").replace(":", "_")
                )
                labels[sky_label_key] = str(annotations[ann_key])

        # Pre-resolve multi-node dynamic-data inputs to shell expansions of the
        # TANGLE_MULTI_NODE_* env vars set by our run-script prelude. This keeps
        # the rest of resolve_container_command_line oblivious to multi-node.
        for input_argument in input_arguments.values():
            if input_argument.value is not None or input_argument.uri is not None:
                continue
            if not input_argument.dynamic_data:
                continue
            kind, _payload = container_component_utils.parse_dynamic_data_argument(
                input_argument.dynamic_data
            )
            if kind == _MULTI_NODE_NUMBER_OF_NODES_DYNAMIC_DATA_KEY:
                input_argument.value = "${TANGLE_MULTI_NODE_NUMBER_OF_NODES}"
            elif kind == _MULTI_NODE_NODE_INDEX_DYNAMIC_DATA_KEY:
                input_argument.value = "${TANGLE_MULTI_NODE_NODE_INDEX}"
            elif kind == _MULTI_NODE_NODE_0_ADDRESS_DYNAMIC_DATA_KEY:
                input_argument.value = "${TANGLE_MULTI_NODE_NODE_0_ADDRESS}"
            elif kind == _MULTI_NODE_ALL_NODE_ADDRESSES_DYNAMIC_DATA_KEY:
                input_argument.value = "${TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES}"
            else:
                raise interfaces.LauncherError(
                    f"Dynamic data '{kind}' is not supported by the SkyPilot launcher"
                )

        file_mounts: dict[str, str] = {}
        envs: dict[str, str] = {**self._default_envs, **(container_spec.env or {})}

        def get_input_value(input_name: str) -> str:
            ia = input_arguments[input_name]
            if ia.is_dir:
                raise interfaces.LauncherError(
                    f"Cannot consume directory as value. {input_name=}"
                )
            if ia.total_size > _MAX_INPUT_VALUE_SIZE:
                raise interfaces.LauncherError(
                    f"Artifact too big to consume as value. Use a path. {input_name=}"
                )
            if ia.value is None:
                # First cut: require scalar values to be pre-resolved by the
                # orchestrator. Adding a storage_provider hook here is the
                # natural extension for parity with kubernetes_launchers.
                raise interfaces.LauncherError(
                    f"Input '{input_name}' has no inline value. Pre-resolve "
                    "scalar inputs before submission, or extend this launcher "
                    "with a storage_provider for downloads."
                )
            return ia.value

        def _is_cloud_uri(uri: str) -> bool:
            return any(
                uri.startswith(s)
                for s in ("gs://", "s3://", "abfs://", "https://", "http://", "r2://")
            )

        # SkyPilot's MOUNT mode requires the source to be a bucket root (not a
        # sub-path within a bucket — sky/data/storage.py:_validate_source raises
        # StorageModeError otherwise). We mount each unique bucket once at a
        # stable container path and use sub-paths inside.
        bucket_mount_root = "/mnt/skypilot"

        def _bucket_and_subpath(uri: str) -> tuple[str, str]:
            # gs://bucket/sub/path -> ("gs://bucket", "sub/path")
            scheme, _, rest = uri.partition("://")
            bucket, _, sub = rest.partition("/")
            return f"{scheme}://{bucket}", sub

        def _container_path_for(uri: str) -> str:
            bucket_uri, sub_path = _bucket_and_subpath(uri)
            scheme = bucket_uri.split("://", 1)[0]
            bucket_name = bucket_uri.split("://", 1)[1]
            mount_point = f"{bucket_mount_root}/{scheme}/{bucket_name}"
            file_mounts[mount_point] = {"source": bucket_uri, "mode": "MOUNT"}
            return f"{mount_point}/{sub_path}"

        def get_input_path(input_name: str) -> str:
            ia = input_arguments[input_name]
            if ia.uri is None:
                raise interfaces.LauncherError(
                    f"Input '{input_name}' has no URI. Stage values to cloud "
                    "storage (gs://, s3://, ...) before submitting through the "
                    "SkyPilot launcher."
                )
            if not _is_cloud_uri(ia.uri):
                raise interfaces.LauncherError(
                    f"Input '{input_name}' uri={ia.uri!r} is not a cloud storage URI. "
                    "The SkyPilot launcher requires gs://, s3://, abfs://, https://, or "
                    "r2:// for inputs. Configure Tangle with a cloud StorageProvider "
                    "(e.g. GoogleCloudStorageProvider) for cloud-based runs."
                )
            return _container_path_for(ia.uri)

        def get_output_path(output_name: str) -> str:
            uri = output_uris[output_name]
            if _is_cloud_uri(uri):
                return _container_path_for(uri)
            _logger.warning(
                "Output '%s' uri=%r is not a cloud URI; the SkyPilot launcher "
                "will not persist it back to Tangle's storage. Use a cloud "
                "StorageProvider (gs://, s3://, ...) to persist outputs.",
                output_name, uri,
            )
            sanitized = naming_utils.sanitize_file_name(output_name)
            return f"/tmp/outputs/{sanitized}/{_CONTAINER_FILE_NAME}"

        resolved = container_component_utils.resolve_container_command_line(
            component_spec=component_spec,
            provided_input_names=set(input_arguments.keys()),
            get_input_value=get_input_value,
            get_input_path=get_input_path,
            get_output_path=get_output_path,
        )

        cmd_str = _shell_quote_argv(list(resolved.command) + list(resolved.args))
        run_script = "set -euo pipefail\n" + _MULTI_NODE_ENV_PRELUDE + cmd_str + "\n"

        # Name the SkyPilot job after the component for easy filtering.
        component_name = (
            component_spec.name
            or (component_spec.metadata.name if component_spec.metadata else None)
            or "task"
        )
        job_name = self._job_name_prefix + naming_utils.sanitize_file_name(
            component_name
        )

        image = container_spec.image or self._default_image
        if not image:
            raise interfaces.LauncherError(
                f"Component '{component_name}' has no container image and the "
                "launcher was not configured with default_image."
            )

        # Resources kwargs
        resources_kwargs: dict[str, Any] = {
            # SkyPilot accepts container images via image_id="docker:<image>" — this
            # is the canonical way to launch an arbitrary user container in K8s.
            "image_id": f"docker:{image}",
        }
        if self._infra is not None:
            resources_kwargs["infra"] = self._infra
        if cpus is not None:
            resources_kwargs["cpus"] = cpus
        if memory is not None:
            resources_kwargs["memory"] = memory
        if accelerators is not None:
            # Tangle's kubernetes_launchers expects a JSON object like
            # `{"nvidia-tesla-h100": 8}`. SkyPilot accepts either a Sky-format
            # string ("H100:8") or a {name: count} dict — try JSON first so the
            # same component spec works under either launcher.
            parsed_accel: Any = accelerators
            if isinstance(accelerators, str):
                try:
                    parsed_accel = json.loads(accelerators)
                except (ValueError, TypeError):
                    parsed_accel = accelerators
            resources_kwargs["accelerators"] = parsed_accel
        if ephemeral_storage is not None:
            resources_kwargs["disk_size"] = _coerce_disk_size_gb(ephemeral_storage)
        if labels:
            resources_kwargs["labels"] = labels

        priority_class = annotations.get(
            PRIORITY_CLASS_ANNOTATION_KEY, self._default_priority_class
        )
        if priority_class is not None:
            resources_kwargs["priority_class"] = priority_class

        spot_value = annotations.get(SPOT_ANNOTATION_KEY)
        if spot_value is not None:
            resources_kwargs["use_spot"] = str(spot_value).lower() in (
                "1", "true", "yes",
            )
        elif self._default_use_spot is not None:
            resources_kwargs["use_spot"] = self._default_use_spot

        # Build a sky YAML-shaped dict and let Task.from_yaml_config parse it.
        # The YAML parser auto-promotes cloud-URI entries in file_mounts into
        # sky.Storage MOUNT mounts (sky/task.py:660-688), which is the path that
        # works under consolidation mode. Constructing sky.Task() directly with
        # cloud-URI file_mounts goes through translate_local_file_mounts_to_two_hop
        # which rejects them.
        task_config: dict[str, Any] = {
            "name": job_name,
            "run": run_script,
            "envs": envs,
            "num_nodes": num_nodes,
            "resources": resources_kwargs,
        }
        if file_mounts:
            task_config["file_mounts"] = file_mounts
        task = sky.Task.from_yaml_config(task_config)
        return task


class SkyPilotLaunchedJob(interfaces.LaunchedContainer):
    """Tangle-side handle around a SkyPilot managed job.

    Wraps a job_id; status/logs are queried lazily via the sky SDK. The handle
    is serializable via to_dict / from_dict so the orchestrator can persist it
    in the request DB and reload across restarts.
    """

    def __init__(self, handle: _SkyPilotJobHandle):
        self._handle = handle

    @property
    def id(self) -> str:
        return f"sky:{self._handle.job_id}"

    @property
    def job_id(self) -> int:
        return self._handle.job_id

    @property
    def status(self) -> interfaces.ContainerStatus:
        return _STATUS_MAP.get(
            self._handle.cached_status or "PENDING", interfaces.ContainerStatus.ERROR
        )

    @property
    def exit_code(self) -> Optional[int]:
        if not self.has_ended:
            return None
        return 0 if self.has_succeeded else 1

    @property
    def has_ended(self) -> bool:
        return (self._handle.cached_status or "PENDING") in _TERMINAL_STATUSES

    @property
    def has_succeeded(self) -> bool:
        return self._handle.cached_status == "SUCCEEDED"

    @property
    def has_failed(self) -> bool:
        return self.has_ended and not self.has_succeeded

    @property
    def started_at(self) -> Optional[datetime.datetime]:
        if self._handle.cached_started_at is None:
            return None
        return datetime.datetime.fromtimestamp(
            self._handle.cached_started_at, tz=datetime.timezone.utc
        )

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        if self._handle.cached_ended_at is None:
            return None
        return datetime.datetime.fromtimestamp(
            self._handle.cached_ended_at, tz=datetime.timezone.utc
        )

    @property
    def launcher_error_message(self) -> Optional[str]:
        return self._handle.cached_failure_reason

    def get_log(self) -> str:
        buf = io.StringIO()
        sky_jobs.tail_logs(
            job_id=self._handle.job_id, follow=False, output_stream=buf
        )
        return buf.getvalue()

    def upload_log(self) -> None:
        # SkyPilot already persists logs on its controller and exposes them via
        # sky.jobs.tail_logs(). Mirroring them to log_uri requires a Tangle
        # storage_provider, which the first cut leaves to subclasses.
        _logger.debug(
            "upload_log() is a no-op; use sky.jobs.tail_logs(job_id=%d) "
            "or pass a storage_provider to a subclass.",
            self._handle.job_id,
        )

    def stream_log_lines(self) -> Iterator[str]:
        # Bridge sky.jobs.tail_logs(follow=True) into a generator of lines.
        class _LineBuffer(io.TextIOBase):
            def __init__(self) -> None:
                self._buf = ""
                self.lines: list[str] = []
                self.lock = threading.Lock()

            def write(self, s: str) -> int:
                with self.lock:
                    self._buf += s
                    while "\n" in self._buf:
                        line, self._buf = self._buf.split("\n", 1)
                        self.lines.append(line + "\n")
                return len(s)

            def take(self) -> list[str]:
                with self.lock:
                    out, self.lines = self.lines, []
                    return out

        buf = _LineBuffer()
        finished = threading.Event()

        def _run() -> None:
            try:
                sky_jobs.tail_logs(
                    job_id=self._handle.job_id, follow=True, output_stream=buf
                )
            finally:
                finished.set()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        try:
            while not finished.is_set() or buf.lines:
                for line in buf.take():
                    yield line
                if not finished.is_set():
                    finished.wait(timeout=0.5)
            for line in buf.take():
                yield line
        finally:
            thread.join(timeout=1.0)

    def terminate(self) -> None:
        request_id = sky_jobs.cancel(job_ids=[self._handle.job_id])
        sky.get(request_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "skypilot": {
                "job_id": self._handle.job_id,
                "job_name": self._handle.job_name,
                "output_uris": self._handle.output_uris,
                "log_uri": self._handle.log_uri,
                "cached_status": self._handle.cached_status,
                "cached_failure_reason": self._handle.cached_failure_reason,
                "cached_started_at": self._handle.cached_started_at,
                "cached_ended_at": self._handle.cached_ended_at,
            }
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SkyPilotLaunchedJob":
        sk = d["skypilot"]
        return cls(
            handle=_SkyPilotJobHandle(
                job_id=int(sk["job_id"]),
                job_name=sk["job_name"],
                output_uris=dict(sk.get("output_uris") or {}),
                log_uri=sk["log_uri"],
                cached_status=sk.get("cached_status"),
                cached_failure_reason=sk.get("cached_failure_reason"),
                cached_started_at=sk.get("cached_started_at"),
                cached_ended_at=sk.get("cached_ended_at"),
            )
        )

    def get_refreshed(self) -> "SkyPilotLaunchedJob":
        request_id = sky_jobs.queue(refresh=True, job_ids=[self._handle.job_id])
        result = sky.get(request_id)
        # Sky's queue() return shape varied across versions. Newer (nightly):
        # tuple[list[dict], ...]; older: list[dict] directly. Unwrap to the
        # list of records.
        if isinstance(result, tuple):
            records = result[0] if result else []
        else:
            records = result or []
        # Find the record matching our job_id (queue may ignore the filter
        # and return all jobs).
        rec = None
        for r in records:
            jid = r.get("job_id") if isinstance(r, dict) else getattr(r, "job_id", None)
            if jid == self._handle.job_id:
                rec = r
                break
        if rec is None:
            return SkyPilotLaunchedJob(
                handle=dataclasses.replace(
                    self._handle,
                    cached_status="FAILED_CONTROLLER",
                    cached_failure_reason="job not found in sky.jobs.queue",
                )
            )
        get = (lambda k: rec.get(k)) if isinstance(rec, dict) else (
            lambda k: getattr(rec, k, None)
        )
        status_str = _status_to_string(get("status"))
        started_at = get("start_at")
        ended_at = get("end_at")
        return SkyPilotLaunchedJob(
            handle=dataclasses.replace(
                self._handle,
                cached_status=status_str,
                cached_failure_reason=get("failure_reason"),
                cached_started_at=float(started_at) if started_at else None,
                cached_ended_at=float(ended_at) if ended_at else None,
            )
        )
