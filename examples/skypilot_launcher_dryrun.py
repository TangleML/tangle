"""End-to-end dry-run of a Tangle pipeline through the SkyPilot launcher.

Exercises the full ContainerTaskLauncher contract — submit, refresh, log
streaming, persistence round-trip, terminate — without requiring a live
Kubernetes cluster or SkyPilot controller. The sky.jobs SDK is stubbed inline
so the script runs anywhere Tangle is installed.

Run:
    /home/sky/.venv/bin/python examples/run_pipeline_dryrun.py

Output sections:
  1. Translation (ComponentSpec -> sky.Task)
  2. Submission (sky.jobs.launch)
  3. Status refresh (sky.jobs.queue)
  4. Log fetch (sky.jobs.tail_logs)
  5. Persistence round-trip (orchestrator restart simulation)
  6. Termination (sky.jobs.cancel)
"""

from __future__ import annotations

import json
import sys
import textwrap
import types


def _stub_sky() -> dict:
    """Stub the sky module before the launcher imports it."""
    submissions: list = []
    cancellations: list = []

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

        def set_resources(self, r):
            self.resources = r
            return self

        def set_file_mounts(self, fm):
            self.file_mounts = fm
            return self

    class _FakeResources:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __repr__(self):
            return f"Resources({self.kwargs})"

    sky_mod.Task = _FakeTask
    sky_mod.Resources = _FakeResources
    sky_mod.get = lambda req: req

    sky_jobs = types.ModuleType("sky.jobs")

    def _launch(task, name=None, **kwargs):
        submissions.append({"task": task, "name": name, "kwargs": kwargs})
        return ([20251029], None)

    def _queue(refresh=False, job_ids=None, **kwargs):
        return [{
            "job_id": (job_ids or [20251029])[0],
            "status": "RUNNING",
            "start_at": 1700000000.0,
            "end_at": None,
            "failure_reason": None,
        }]

    def _cancel(job_ids=None, **kwargs):
        cancellations.append(list(job_ids or []))
        return {"cancelled": list(job_ids or [])}

    def _tail_logs(job_id=None, follow=False, output_stream=None, **kwargs):
        if output_stream is not None:
            output_stream.write(f"job {job_id} step 1/3 ok\n")
            output_stream.write(f"job {job_id} step 2/3 ok\n")
            output_stream.write(f"job {job_id} step 3/3 ok\n")
        return 0

    sky_jobs.launch = _launch
    sky_jobs.queue = _queue
    sky_jobs.cancel = _cancel
    sky_jobs.tail_logs = _tail_logs

    sky_mod.jobs = sky_jobs
    sys.modules["sky"] = sky_mod
    sys.modules["sky.jobs"] = sky_jobs
    return {"submissions": submissions, "cancellations": cancellations}


def _hr(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


def main() -> None:
    captures = _stub_sky()

    from cloud_pipelines_backend import component_structures as structures
    from cloud_pipelines_backend.launchers.skypilot_launchers import (
        SkyPilotKubernetesLauncher,
        SkyPilotLaunchedJob,
        PRIORITY_CLASS_ANNOTATION_KEY,
        SPOT_ANNOTATION_KEY,
    )

    # -----------------------------------------------------------------
    _hr("1. Build a ComponentSpec — multi-node H100 fine-tune")
    # -----------------------------------------------------------------
    component = structures.ComponentSpec(
        name="qwen3_finetune",
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(
                image="ghcr.io/example/finetune:1.0",
                command=["torchrun"],
                args=[
                    structures.ConcatPlaceholder([
                        "--nnodes=",
                        structures.InputValuePlaceholder("nnodes"),
                    ]),
                    structures.ConcatPlaceholder([
                        "--node_rank=",
                        structures.InputValuePlaceholder("rank"),
                    ]),
                    structures.ConcatPlaceholder([
                        "--master_addr=",
                        structures.InputValuePlaceholder("master"),
                    ]),
                    "train.py",
                    "--data",
                    structures.InputPathPlaceholder("dataset"),
                    "--ckpt-out",
                    structures.OutputPathPlaceholder("checkpoint"),
                ],
                env={"WANDB_PROJECT": "tangle-skypilot-demo"},
            )
        ),
        inputs=[
            structures.InputSpec(name="nnodes"),
            structures.InputSpec(name="rank"),
            structures.InputSpec(name="master"),
            structures.InputSpec(name="dataset"),
        ],
    )
    print(f"  Component: {component.name}")
    print(f"  Image:     {component.implementation.container.image}")

    # -----------------------------------------------------------------
    _hr("2. Configure the SkyPilot launcher")
    # -----------------------------------------------------------------
    launcher = SkyPilotKubernetesLauncher(
        infra="kubernetes",
        pool="ml-training",  # warm-pool reuse — no Tangle K8s equivalent
        priority_class="batch",  # first-class Kueue integration
        default_labels={"managed-by": "tangle"},
        annotation_to_label_keys=[
            "ml.shopify.io/priority-class",
        ],
    )
    print("  infra=kubernetes (single-cluster); set None for multi-cloud")
    print("  pool='ml-training'  (warm-pool reuse)")
    print("  priority_class='batch'  (Kueue-compatible)")

    # -----------------------------------------------------------------
    _hr("3. Submit through launch_container_task — full lifecycle")
    # -----------------------------------------------------------------
    from cloud_pipelines_backend.launchers import interfaces as _ifaces

    input_arguments = {
        # Multi-node dynamic data: get bridged to bash env vars set from
        # SKYPILOT_NUM_NODES / SKYPILOT_NODE_RANK / SKYPILOT_NODE_IPS.
        "nnodes": _ifaces.InputArgument(
            total_size=0, is_dir=False, staging_uri="",
            dynamic_data="system/multi_node/number_of_nodes",
        ),
        "rank": _ifaces.InputArgument(
            total_size=0, is_dir=False, staging_uri="",
            dynamic_data="system/multi_node/node_index",
        ),
        "master": _ifaces.InputArgument(
            total_size=0, is_dir=False, staging_uri="",
            dynamic_data="system/multi_node/node_0_address",
        ),
        # SkyPilot accepts s3:// directly via file_mounts. Tangle's K8s
        # launcher only does GCS (gcsfuse) or HostPath today.
        "dataset": _ifaces.InputArgument(
            total_size=10**9, is_dir=False,
            uri="s3://example-datasets/finetune.parquet",
            staging_uri="",
        ),
    }
    output_uris = {
        "checkpoint": "gs://example-ckpts/qwen3-finetune/run-20260426/",
    }

    handle = launcher.launch_container_task(
        component_spec=component,
        input_arguments=input_arguments,
        output_uris=output_uris,
        log_uri="gs://example-logs/qwen3-finetune/run-20260426.log",
        annotations={
            # Resource asks
            "cloud-pipelines.net/launchers/generic/resources.cpu": "16+",
            "cloud-pipelines.net/launchers/generic/resources.memory": "256",
            "cloud-pipelines.net/launchers/generic/resources.accelerators":
                json.dumps({"nvidia-tesla-h100": 8}),  # Tangle's JSON form
            "cloud-pipelines.net/launchers/generic/resources.ephemeral_storage":
                "1Ti",
            # 32 nodes — above Tangle K8s launcher's hardcoded cap of 16.
            "tangleml.com/launchers/kubernetes/multi_node/number_of_nodes": "32",
            # SkyPilot-only: spot instances with auto-recovery via managed jobs.
            SPOT_ANNOTATION_KEY: "true",
            # Per-task priority override (overrides launcher default).
            PRIORITY_CLASS_ANNOTATION_KEY: "interactive",
            # Propagated to K8s pod label by `annotation_to_label_keys`:
            "ml.shopify.io/priority-class": "interactive",
        },
    )
    submission = captures["submissions"][-1]
    task = submission["task"]
    res = task.resources
    print(f"  Submitted job_id = {handle.job_id}")
    print(f"  Job name         = {submission['name']}")
    print(f"  num_nodes        = {task.num_nodes}  "
          f"(Tangle K8s cap is 16)")
    print(f"  resources        = {res.kwargs}")
    print(f"  pool kwarg       = {submission['kwargs'].get('pool')}")
    print(f"  file_mounts      = {task.file_mounts}")
    print("\n  Generated run script (first 6 lines):")
    for line in task.run.splitlines()[:6]:
        print(f"    {line}")

    # -----------------------------------------------------------------
    _hr("4. Refresh status from the controller")
    # -----------------------------------------------------------------
    handle = handle.get_refreshed()
    print(f"  status     = {handle.status.value}")
    print(f"  has_ended  = {handle.has_ended}")
    print(f"  started_at = {handle.started_at}")

    # -----------------------------------------------------------------
    _hr("5. Fetch logs (one-shot)")
    # -----------------------------------------------------------------
    print(textwrap.indent(handle.get_log(), "  "))

    # -----------------------------------------------------------------
    _hr("6. Persistence round-trip — simulate orchestrator restart")
    # -----------------------------------------------------------------
    serialized = handle.to_dict()
    print(f"  serialized: {json.dumps(serialized, indent=2, default=str)[:200]}...")
    reloaded = SkyPilotLaunchedJob.from_dict(serialized)
    print(f"  reloaded.job_id = {reloaded.job_id}")
    print(f"  reloaded.status = {reloaded.status.value}")

    # -----------------------------------------------------------------
    _hr("7. Terminate")
    # -----------------------------------------------------------------
    reloaded.terminate()
    print(f"  cancellations sent: {captures['cancellations']}")

    print("\nAll lifecycle steps completed without errors.")


if __name__ == "__main__":
    main()
