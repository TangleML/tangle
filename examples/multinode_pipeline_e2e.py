"""Real GPU PyTorch DDP multi-node test on CoreWeave H100s.

Two pods, one H100 each, NCCL backend. Trains a small MLP with synthetic
data via DistributedDataParallel, writes a checkpoint and per-epoch
JSONL log to GCS.

The launcher's run-script prelude exports TANGLE_MULTI_NODE_* from
SkyPilot's runtime values; we map those onto torch.distributed's
MASTER_ADDR / RANK / WORLD_SIZE so torch can rendez-vous across pods.

Worker pods authenticate to GCS via a GCP service-account key mounted
by SkyPilot's helm chart (gcpCredentials.enabled=true), so storage
mounts work outside GKE.
"""
from __future__ import annotations
import datetime, json, time, urllib.request, urllib.error

BASE = "http://localhost:9091"


def post(path, body):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def get(path):
    try:
        with urllib.request.urlopen(BASE + path, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        return {"_error": e.code, "_body": e.read().decode()[:200]}


_TRAIN_PY = r"""
import json, os, socket, time
import torch, torch.nn as nn
import torch.distributed as dist
from torch.utils.data import DataLoader, TensorDataset
from torch.utils.data.distributed import DistributedSampler
from torch.nn.parallel import DistributedDataParallel as DDP

rank = int(os.environ["RANK"])
world = int(os.environ["WORLD_SIZE"])
torch.manual_seed(42 + rank)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
gpu_name = torch.cuda.get_device_name(0) if torch.cuda.is_available() else "cpu"
print(f"[rank {rank}/{world}] host={socket.gethostname()} torch={torch.__version__} "
      f"device={device} gpu={gpu_name} master={os.environ['MASTER_ADDR']}:{os.environ['MASTER_PORT']}",
      flush=True)

if device.type == "cuda":
    torch.cuda.set_device(0)

backend = "nccl" if device.type == "cuda" else "gloo"
dist.init_process_group(backend=backend, rank=rank, world_size=world,
                        timeout=__import__("datetime").timedelta(seconds=180))
print(f"[rank {rank}] process group initialized (backend={backend})", flush=True)

# Synthetic regression — a small MLP that gives the GPU something to do.
N, D, H = 32768, 256, 512
torch.manual_seed(0)
X = torch.randn(N, D)
W_true = torch.randn(D, 1) * 0.5
y = X @ W_true + 0.1 + 0.05 * torch.randn(N, 1)
ds = TensorDataset(X, y)

batch = int(os.environ.get("BATCH_SIZE", "128"))
epochs = int(os.environ.get("EPOCHS", "5"))
lr = float(os.environ.get("LR", "0.01"))

sampler = DistributedSampler(ds, num_replicas=world, rank=rank, shuffle=True)
loader = DataLoader(ds, batch_size=batch, sampler=sampler)

model = nn.Sequential(nn.Linear(D, H), nn.ReLU(),
                     nn.Linear(H, H), nn.ReLU(),
                     nn.Linear(H, 1)).to(device)
ddp = DDP(model, device_ids=[0] if device.type == "cuda" else None)
opt = torch.optim.Adam(ddp.parameters(), lr=lr)
loss_fn = nn.MSELoss()

log_lines = []
t0 = time.time()
for epoch in range(epochs):
    sampler.set_epoch(epoch)
    epoch_loss, n_batches = 0.0, 0
    for xb, yb in loader:
        xb, yb = xb.to(device), yb.to(device)
        pred = ddp(xb)
        loss = loss_fn(pred, yb)
        opt.zero_grad()
        loss.backward()
        opt.step()
        epoch_loss += loss.item()
        n_batches += 1
    t = torch.tensor([epoch_loss, float(n_batches)], device=device)
    dist.all_reduce(t, op=dist.ReduceOp.SUM)
    avg = (t[0] / t[1]).item()
    msg = {"epoch": epoch + 1, "rank": rank, "avg_loss": round(avg, 6),
           "elapsed_s": round(time.time() - t0, 2), "device": str(device)}
    print(f"[rank {rank}] {msg}", flush=True)
    log_lines.append(json.dumps(msg))

dist.barrier()
if rank == 0:
    ckpt_path = os.environ["OUTPUT_CKPT"]
    log_path = os.environ["OUTPUT_LOG"]
    os.makedirs(os.path.dirname(ckpt_path), exist_ok=True)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    torch.save({"state_dict": ddp.module.state_dict(),
                "world_size": world, "epochs": epochs,
                "device": str(device), "gpu": gpu_name}, ckpt_path)
    with open(log_path, "w") as f:
        f.write("\n".join(log_lines) + "\n")
    print(f"[rank 0] wrote {ckpt_path} ({os.path.getsize(ckpt_path)} bytes) "
          f"and {log_path}", flush=True)
dist.destroy_process_group()
print(f"[rank {rank}] done", flush=True)
"""

ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
training_spec = {
    # The "skypilot-" prefix surfaces the launcher in Tangle's UI / sky
    # dashboard — both display the component name unchanged.
    "name": f"skypilot-pytorch-ddp-h100-{ts}",
    "outputs": [
        {"name": "checkpoint", "type": "Model"},
        {"name": "training_log", "type": "String"},
    ],
    "implementation": {
        "container": {
            "image": "pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime",
            "env": {"PIPELINE_RUN_TS": ts},
            "command": [
                "bash", "-c",
                'set -euo pipefail; '
                'export MASTER_ADDR="${TANGLE_MULTI_NODE_NODE_0_ADDRESS:-localhost}"; '
                'export MASTER_PORT="29500"; '
                'export RANK="${TANGLE_MULTI_NODE_NODE_INDEX:-0}"; '
                'export WORLD_SIZE="${TANGLE_MULTI_NODE_NUMBER_OF_NODES:-1}"; '
                'export OUTPUT_CKPT="$0"; '
                'export OUTPUT_LOG="$1"; '
                'export EPOCHS="5"; export BATCH_SIZE="128"; export LR="0.01"; '
                'echo "[$(hostname)] rank=$RANK/$WORLD_SIZE master=$MASTER_ADDR:$MASTER_PORT"; '
                'nvidia-smi -L 2>/dev/null || echo "nvidia-smi unavailable"; '
                f'python3 -u <<\'PYEOF\'\n{_TRAIN_PY}\nPYEOF',
                {"outputPath": "checkpoint"},
                {"outputPath": "training_log"},
            ],
        }
    },
}

pipeline_spec = {
    "name": f"skypilot-pytorch-ddp-h100-pipeline-{ts}",
    "outputs": [{"name": "checkpoint", "type": "Model"}],
    "implementation": {
        "graph": {
            "tasks": {
                "train": {
                    "componentRef": {"spec": training_spec},
                    "annotations": {
                        "tangleml.com/launchers/kubernetes/multi_node/number_of_nodes": "2",
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "4",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "16",
                        "cloud-pipelines.net/launchers/generic/resources.accelerators": "H100:1",
                    },
                },
            },
            "outputValues": {
                "checkpoint": {
                    "taskOutput": {"taskId": "train", "outputName": "checkpoint"}
                }
            },
        }
    },
}

print(f"=== submit pytorch-ddp pipeline (ts={ts}) ===")
body = {"root_task": {"componentRef": {"spec": pipeline_spec}, "arguments": {}}}
run = post("/api/pipeline_runs/", body)
print(json.dumps(run, indent=2))
root_exec = run["root_execution_id"]

print(f"\n=== poll graph_execution_state for {root_exec} ===")
deadline = time.time() + 1800
last = None
while time.time() < deadline:
    state = get(f"/api/executions/{root_exec}/graph_execution_state")
    line = json.dumps(state.get("child_execution_status_stats", {})) if state else "<no state>"
    if line != last:
        print(f"  [{time.strftime('%H:%M:%S')}] {line}", flush=True)
        last = line
    stats = (state or {}).get("child_execution_status_stats", {}) or {}
    summary = {}
    for child_id, status_dict in stats.items():
        for status, count in status_dict.items():
            summary[status] = summary.get(status, 0) + count
    if any(summary.get(k, 0) > 0 for k in ("FAILED", "SYSTEM_ERROR", "INVALID", "CANCELLED")):
        break
    if (summary.get("SUCCEEDED", 0) >= 1 and
            not any(summary.get(k, 0) > 0
                    for k in ("PENDING", "QUEUED", "RUNNING", "WAITING_FOR_UPSTREAM",
                              "STARTING"))):
        break
    time.sleep(15)

print(f"\n=== final root state ===")
print(json.dumps(get(f"/api/executions/{root_exec}/graph_execution_state"), indent=2)[:2500])

print(f"\n=== child task statuses ===")
details = get(f"/api/executions/{root_exec}/details")
child_ids = (details or {}).get("child_task_execution_ids", {}) or {}
for task_id, exec_id in child_ids.items():
    cstate = get(f"/api/executions/{exec_id}/container_state")
    print(f"  {task_id}: status={(cstate or {}).get('status')}  "
          f"exit_code={(cstate or {}).get('exit_code')}")
    if cstate and cstate.get("debug_info", {}).get("skypilot"):
        sky = cstate["debug_info"]["skypilot"]
        print(f"    sky job_id={sky.get('job_id')}  name={sky.get('job_name')}")
