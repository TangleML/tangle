"""Tests for cloud_pipelines_backend.launchers.skypilot_launchers.

Translation tests; the sky.jobs SDK calls are stubbed so the test runs offline.
"""

from __future__ import annotations

import dataclasses
import sys
import types

import pytest


@pytest.fixture(autouse=True)
def _stub_sky(monkeypatch):
    """Stub the sky module so launcher tests can run without a real SkyPilot install."""
    sky_mod = types.ModuleType("sky")

    class _FakeTask:
        def __init__(self, *, name=None, run=None, envs=None, num_nodes=1,
                     file_mounts=None, **kwargs):
            self.name = name
            self.run = run
            self.envs = envs or {}
            self.num_nodes = num_nodes
            self.file_mounts = file_mounts
            self.resources = None
            self.kwargs = kwargs

        def set_resources(self, r):
            self.resources = r
            return self

        def set_file_mounts(self, fm):
            self.file_mounts = fm
            return self

    class _FakeResources:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    def _fake_get(req):
        return req

    sky_mod.Task = _FakeTask
    sky_mod.Resources = _FakeResources
    sky_mod.get = _fake_get

    sky_jobs_mod = types.ModuleType("sky.jobs")

    def _fake_launch(task, name=None, **kwargs):
        return ([12345], None)

    def _fake_queue(refresh=False, job_ids=None, **kwargs):
        return [{
            "job_id": (job_ids or [12345])[0],
            "status": "RUNNING",
            "start_at": 1700000000.0,
            "end_at": None,
            "failure_reason": None,
        }]

    def _fake_cancel(job_ids=None, **kwargs):
        return {"cancelled": list(job_ids or [])}

    def _fake_tail_logs(job_id=None, follow=False, output_stream=None, **kwargs):
        if output_stream is not None:
            output_stream.write(f"job {job_id} log line\n")
        return 0

    sky_jobs_mod.launch = _fake_launch
    sky_jobs_mod.queue = _fake_queue
    sky_jobs_mod.cancel = _fake_cancel
    sky_jobs_mod.tail_logs = _fake_tail_logs

    sky_mod.jobs = sky_jobs_mod
    monkeypatch.setitem(sys.modules, "sky", sky_mod)
    monkeypatch.setitem(sys.modules, "sky.jobs", sky_jobs_mod)
    sys.modules.pop("cloud_pipelines_backend.launchers.skypilot_launchers", None)
    yield
    sys.modules.pop("cloud_pipelines_backend.launchers.skypilot_launchers", None)


def _make_component(image="python:3.11", command=None, args=None, env=None,
                    inputs=None, name="test"):
    from cloud_pipelines_backend import component_structures as structures
    return structures.ComponentSpec(
        name=name,
        inputs=[structures.InputSpec(n) for n in (inputs or [])],
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(
                image=image, command=command, args=args, env=env,
            )
        ),
    )


def test_minimal_command_translation():
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    launcher = SkyPilotKubernetesLauncher(infra="kubernetes")
    component = _make_component(
        image="ghcr.io/example/trainer:1.0",
        command=["python", "-m", "trainer"],
        args=["--epochs", "3"],
    )
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={},
    )
    assert "python -m trainer --epochs 3" in task.run
    assert "TANGLE_MULTI_NODE_NODE_INDEX" in task.run
    assert task.num_nodes == 1
    assert task.resources.kwargs["image_id"] == "docker:ghcr.io/example/trainer:1.0"
    assert task.resources.kwargs["infra"] == "kubernetes"


def test_resource_annotations_propagate():
    from cloud_pipelines_backend.launchers import kubernetes_launchers as k8sL
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    launcher = SkyPilotKubernetesLauncher(
        infra="kubernetes", priority_class="emergency"
    )
    component = _make_component(command=["bash", "-c", "true"])
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={
            k8sL.RESOURCES_CPU_ANNOTATION_KEY: "4+",
            k8sL.RESOURCES_MEMORY_ANNOTATION_KEY: "32",
            k8sL.RESOURCES_ACCELERATORS_ANNOTATION_KEY: "H100:8",
            k8sL.RESOURCES_EPHEMERAL_STORAGE_ANNOTATION_KEY: "200Gi",
            k8sL.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "4",
        },
    )
    r = task.resources.kwargs
    assert r["cpus"] == "4+"
    assert r["memory"] == "32"
    assert r["accelerators"] == "H100:8"
    assert r["disk_size"] == 200
    assert r["priority_class"] == "emergency"
    assert task.num_nodes == 4


def test_multi_node_dynamic_data():
    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers import interfaces, kubernetes_launchers as k8sL
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(
        command=["echo"],
        args=[
            structures.InputValuePlaceholder("rank"),
            structures.InputValuePlaceholder("nnodes"),
        ],
        inputs=["rank", "nnodes"],
    )
    input_arguments = {
        "rank": interfaces.InputArgument(
            total_size=0, is_dir=False, staging_uri="",
            dynamic_data="system/multi_node/node_index",
        ),
        "nnodes": interfaces.InputArgument(
            total_size=0, is_dir=False, staging_uri="",
            dynamic_data="system/multi_node/number_of_nodes",
        ),
    }
    launcher = SkyPilotKubernetesLauncher(infra="kubernetes")
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments=input_arguments,
        output_uris={},
        annotations={k8sL.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "2"},
    )
    assert "TANGLE_MULTI_NODE_NODE_INDEX" in task.run
    assert "TANGLE_MULTI_NODE_NUMBER_OF_NODES" in task.run
    assert task.num_nodes == 2


def test_input_path_uri_becomes_file_mount():
    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(
        command=["cat"],
        args=[structures.InputPathPlaceholder("dataset")],
        inputs=["dataset"],
    )
    input_arguments = {
        "dataset": interfaces.InputArgument(
            total_size=10**9,
            is_dir=False,
            uri="gs://example-bucket/datasets/foo.parquet",
            staging_uri="gs://example-bucket/staging/foo",
        ),
    }
    launcher = SkyPilotKubernetesLauncher(infra="kubernetes")
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments=input_arguments,
        output_uris={},
        annotations={},
    )
    assert task.file_mounts is not None
    container_path = next(iter(task.file_mounts))
    assert container_path.startswith("/tmp/inputs/dataset/")
    assert task.file_mounts[container_path] == "gs://example-bucket/datasets/foo.parquet"
    assert container_path in task.run


def test_priority_class_annotation_overrides_default():
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
        PRIORITY_CLASS_ANNOTATION_KEY,
    )

    launcher = SkyPilotKubernetesLauncher(priority_class="batch")
    component = _make_component(command=["true"])
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={PRIORITY_CLASS_ANNOTATION_KEY: "interactive"},
    )
    assert task.resources.kwargs["priority_class"] == "interactive"


def test_annotation_to_label_propagation():
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    launcher = SkyPilotKubernetesLauncher(
        annotation_to_label_keys=["ml.shopify.io/priority-class"],
    )
    component = _make_component(command=["true"])
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={"ml.shopify.io/priority-class": "interactive"},
    )
    labels = task.resources.kwargs["labels"]
    assert labels["ml_shopify_io_priority-class"] == "interactive"


def test_serialize_round_trip():
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotLaunchedJob, _SkyPilotJobHandle,
    )
    from cloud_pipelines_backend.launchers import interfaces

    job = SkyPilotLaunchedJob(
        handle=_SkyPilotJobHandle(
            job_id=42,
            job_name="tangle-test",
            output_uris={"out": "gs://x/y"},
            log_uri="gs://x/log",
            cached_status="RUNNING",
            cached_started_at=1700000000.0,
        )
    )
    d = job.to_dict()
    job2 = SkyPilotLaunchedJob.from_dict(d)
    assert job2.job_id == 42
    assert job2.status == interfaces.ContainerStatus.RUNNING
    assert not job2.has_ended


def test_status_mapping_terminal_states():
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotLaunchedJob, _SkyPilotJobHandle,
    )
    from cloud_pipelines_backend.launchers import interfaces

    def make(status):
        return SkyPilotLaunchedJob(
            handle=_SkyPilotJobHandle(
                job_id=1, job_name="x", output_uris={}, log_uri="",
                cached_status=status,
            )
        )

    assert make("SUCCEEDED").has_succeeded
    assert make("FAILED").has_failed
    assert make("CANCELLED").has_failed
    assert make("RUNNING").status == interfaces.ContainerStatus.RUNNING
    assert make("FAILED_CONTROLLER").status == interfaces.ContainerStatus.ERROR


def test_missing_image_raises():
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(image=None, command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    with pytest.raises(interfaces.LauncherError, match="no container image"):
        launcher._build_task(
            component_spec=component,
            container_spec=component.implementation.container,
            input_arguments={},
            output_uris={},
            annotations={},
        )


def test_default_image_fallback():
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(image=None, command=["true"])
    launcher = SkyPilotKubernetesLauncher(default_image="python:3.11")
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={},
    )
    assert task.resources.kwargs["image_id"] == "docker:python:3.11"


def test_num_nodes_out_of_range():
    from cloud_pipelines_backend.launchers import interfaces, kubernetes_launchers as k8sL
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    with pytest.raises(interfaces.LauncherError):
        launcher._build_task(
            component_spec=component,
            container_spec=component.implementation.container,
            input_arguments={},
            output_uris={},
            annotations={k8sL.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "100000"},
        )


# -----------------------------------------------------------------
# SkyPilot-only capabilities — features Tangle's existing
# kubernetes_launchers cannot do today, exercised end-to-end here.
# -----------------------------------------------------------------


def test_skypilot_only_num_nodes_above_tangle_k8s_cap():
    """Tangle's kubernetes_launchers caps num_nodes at 16; SkyPilot scales further."""
    from cloud_pipelines_backend.launchers import kubernetes_launchers as k8sL
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher, _MULTI_NODE_MAX_NUMBER_OF_NODES,
    )

    # Sanity: this launcher's cap exceeds Tangle K8s launcher's hardcoded 16.
    assert k8sL._MULTI_NODE_MAX_NUMBER_OF_NODES == 16
    assert _MULTI_NODE_MAX_NUMBER_OF_NODES > 16

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={
            k8sL.MULTI_NODE_NUMBER_OF_NODES_ANNOTATION_KEY: "32",
        },
    )
    # 32 nodes is unrepresentable in Tangle's K8s launcher; works here.
    assert task.num_nodes == 32


def test_skypilot_only_use_spot_with_recovery():
    """SkyPilot supports cross-cloud spot/preemptible + auto-recovery via managed
    jobs. Tangle's kubernetes_launchers only has GKE-specific spot via node selector
    (KUBERNETES_GOOGLE_USE_SPOT_VMS_ANNOTATION_KEY) and no preemption recovery."""
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher, SPOT_ANNOTATION_KEY,
    )

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={SPOT_ANNOTATION_KEY: "true"},
    )
    assert task.resources.kwargs["use_spot"] is True


def test_skypilot_only_multi_cloud_no_infra():
    """infra=None lets SkyPilot's optimizer pick across all configured clouds and
    K8s contexts. Tangle's kubernetes_launchers takes a single api_client and is
    pinned to one cluster."""
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher(infra=None)
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={},
    )
    # Without infra, SkyPilot's optimizer picks across all configured clouds.
    assert "infra" not in task.resources.kwargs


def test_skypilot_only_pool_dispatch_passes_through_to_launch():
    """SkyPilot Pools provide warm-pool reuse for fast cold-start. No Tangle
    equivalent — every Tangle K8s task creates a fresh Pod from scratch."""
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )
    import sky.jobs

    submissions: list = []
    original_launch = sky.jobs.launch

    def _capture(task, name=None, **kwargs):
        submissions.append({"task": task, "name": name, "kwargs": kwargs})
        return ([42], None)

    sky.jobs.launch = _capture
    try:
        launcher = SkyPilotKubernetesLauncher(pool="ml-training")
        component = _make_component(command=["true"])
        launcher.launch_container_task(
            component_spec=component,
            input_arguments={},
            output_uris={},
            log_uri="local:test.log",
            annotations={},
        )
    finally:
        sky.jobs.launch = original_launch

    assert len(submissions) == 1
    assert submissions[0]["kwargs"].get("pool") == "ml-training"


def test_skypilot_only_s3_file_mount_accepted():
    """SkyPilot file_mounts accept gs://, s3://, https://, abfs://, and more.
    Tangle's kubernetes_launchers ships with HostPath (local) and gcsfuse (GCS)
    only — no S3 / R2 / Azure Blob built in."""
    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(
        command=["cat"],
        args=[structures.InputPathPlaceholder("dataset")],
        inputs=["dataset"],
    )
    input_arguments = {
        "dataset": interfaces.InputArgument(
            total_size=10**9, is_dir=False,
            uri="s3://my-bucket/datasets/foo.parquet",
            staging_uri="",
        ),
    }
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments=input_arguments,
        output_uris={},
        annotations={},
    )
    assert any(uri.startswith("s3://") for uri in task.file_mounts.values())


def test_skypilot_only_first_class_priority_class():
    """SkyPilot has priority_class as a first-class Resources kwarg with built-in
    Kueue integration. Tangle's kubernetes_launchers requires a custom
    pod_postprocessor to set spec.priorityClassName — there's no annotation API
    for it out of the box."""
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher, PRIORITY_CLASS_ANNOTATION_KEY,
    )

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={PRIORITY_CLASS_ANNOTATION_KEY: "interactive"},
    )
    assert task.resources.kwargs["priority_class"] == "interactive"


def test_accelerators_json_dict_format_compat():
    """Tangle's kubernetes_launchers expects accelerators as a JSON object
    ({"nvidia-tesla-h100": 8}). Our launcher accepts that form too so the same
    ComponentSpec is portable across launchers — and forwards it to SkyPilot,
    which accepts {name: count} dicts directly."""
    import json as _json
    from cloud_pipelines_backend.launchers import kubernetes_launchers as k8sL
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={
            k8sL.RESOURCES_ACCELERATORS_ANNOTATION_KEY: _json.dumps(
                {"nvidia-tesla-h100": 8}
            ),
        },
    )
    assert task.resources.kwargs["accelerators"] == {"nvidia-tesla-h100": 8}


def test_accelerators_sky_string_format_still_works():
    """Plain SkyPilot string accelerators ('H100:8') also pass through unchanged."""
    from cloud_pipelines_backend.launchers import kubernetes_launchers as k8sL
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(command=["true"])
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={},
        annotations={k8sL.RESOURCES_ACCELERATORS_ANNOTATION_KEY: "H100:8"},
    )
    assert task.resources.kwargs["accelerators"] == "H100:8"


def test_multistep_with_cloud_uris_passes_through():
    """Two-step pipelines work when the storage provider produces cloud URIs.
    The upstream task's output URI (e.g. gs://bucket/.../output/data) is
    handed to the downstream task as InputArgument.uri, and our launcher
    mounts both via SkyPilot's file_mounts — the same URI on both sides
    means the downstream container reads what the upstream wrote."""
    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    # Step 2's component reads `message_file` and writes `shouted`.
    component = _make_component(
        command=["sh", "-c", 'tr "[:lower:]" "[:upper:]" < "$0" > "$1"'],
        args=[
            structures.InputPathPlaceholder("message_file"),
            structures.OutputPathPlaceholder("shouted"),
        ],
        inputs=["message_file"],
    )
    # Storage provider has put step 1's output at this gs:// URI; Tangle hands
    # it to step 2 verbatim as InputArgument.uri:
    upstream_uri = "gs://tangle-test/by_execution/abc123/outputs/message_file/data"
    downstream_output_uri = (
        "gs://tangle-test/by_execution/def456/outputs/shouted/data"
    )
    input_arguments = {
        "message_file": interfaces.InputArgument(
            total_size=10**6, is_dir=False,
            uri=upstream_uri, staging_uri="",
        ),
    }
    launcher = SkyPilotKubernetesLauncher(infra="kubernetes")
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments=input_arguments,
        output_uris={"shouted": downstream_output_uri},
        annotations={},
    )
    # Both URIs registered as SkyPilot file_mounts so the container reads/writes
    # through cloud storage.
    assert task.file_mounts is not None
    mount_values = list(task.file_mounts.values())
    assert upstream_uri in mount_values
    assert downstream_output_uri in mount_values


def test_input_local_uri_raises_actionable_error():
    """A non-cloud (local) URI for an input is rejected up front with an
    actionable message (vs. letting sky.Task validation fail with a generic
    'file does not exist' error). Surfaced during E2E testing with Tangle's
    LocalStorageProvider."""
    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(
        command=["cat"],
        args=[structures.InputPathPlaceholder("dataset")],
        inputs=["dataset"],
    )
    input_arguments = {
        "dataset": interfaces.InputArgument(
            total_size=10**6, is_dir=False,
            uri="data/artifacts/by_execution/abc/inputs/dataset/data",  # local
            staging_uri="",
        ),
    }
    launcher = SkyPilotKubernetesLauncher()
    with pytest.raises(interfaces.LauncherError, match="cloud storage URI"):
        launcher._build_task(
            component_spec=component,
            container_spec=component.implementation.container,
            input_arguments=input_arguments,
            output_uris={},
            annotations={},
        )


def test_output_local_uri_skipped_no_mount():
    """A non-cloud (local) output URI is skipped with a warning. The container
    can still write to its own /tmp/outputs/ inside the pod; the artifact just
    won't be persisted to Tangle's local storage. Surfaced during E2E testing."""
    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
    )

    component = _make_component(
        command=["sh", "-c", "echo hi"],
        args=[structures.OutputPathPlaceholder("greeting")],
    )
    launcher = SkyPilotKubernetesLauncher()
    task = launcher._build_task(
        component_spec=component,
        container_spec=component.implementation.container,
        input_arguments={},
        output_uris={"greeting": "data/artifacts/by_execution/abc/outputs/greeting/data"},
        annotations={},
    )
    # No file_mounts entry for the local output URI.
    assert task.file_mounts is None or all(
        not v.startswith("data/") for v in (task.file_mounts or {}).values()
    )


def test_end_to_end_lifecycle_through_stubbed_sky():
    """End-to-end exercise: launch -> refresh status -> stream logs -> terminate.

    Uses the stubbed sky.jobs SDK from the test fixture. Verifies the full
    LaunchedJob lifecycle a Tangle orchestrator would drive.
    """
    from cloud_pipelines_backend.launchers import interfaces
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher, SkyPilotLaunchedJob,
    )

    component = _make_component(
        image="ghcr.io/example/trainer:1.0",
        command=["python", "-m", "trainer", "--epochs", "3"],
        name="trainer",
    )
    launcher = SkyPilotKubernetesLauncher(
        infra="kubernetes",
        priority_class="batch",
        annotation_to_label_keys=["ml.shopify.io/priority-class"],
        default_labels={"managed-by": "tangle"},
    )

    # 1. Submit
    handle = launcher.launch_container_task(
        component_spec=component,
        input_arguments={},
        output_uris={"checkpoint": "gs://example/ckpt"},
        log_uri="gs://example/logs/trainer",
        annotations={
            "cloud-pipelines.net/launchers/generic/resources.cpu": "8+",
            "cloud-pipelines.net/launchers/generic/resources.memory": "32",
            "cloud-pipelines.net/launchers/generic/resources.accelerators": "H100:8",
            "tangleml.com/launchers/kubernetes/multi_node/number_of_nodes": "2",
            "ml.shopify.io/priority-class": "interactive",
        },
    )
    assert isinstance(handle, SkyPilotLaunchedJob)
    assert handle.job_id == 12345

    # 2. Refresh — pull current status from sky.jobs.queue
    refreshed = handle.get_refreshed()
    assert refreshed.status == interfaces.ContainerStatus.RUNNING
    assert not refreshed.has_ended

    # 3. Pull logs (one-shot)
    log = refreshed.get_log()
    assert "log line" in log

    # 4. Persist + reload (orchestrator restart simulation)
    serialized = refreshed.to_dict()
    reloaded = SkyPilotLaunchedJob.from_dict(serialized)
    assert reloaded.job_id == refreshed.job_id
    assert reloaded.status == refreshed.status

    # 5. Terminate
    reloaded.terminate()  # No exception => success
