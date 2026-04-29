"""Multi-cluster inference example via the SkyPilot launcher.

Two inference tasks in the same Tangle pipeline:
  - 'gpt2_on_h100': asks for an H100 → sky picks the sky-dev cluster
  - 'gpt2_on_h200': asks for an H200 → sky picks the h200cluster

Both run the same gpt2 generation script on a fixed list of prompts.
Outputs land in GCS; a final 'compare' task reads both and prints them
side-by-side. Demonstrates SkyPilot's cross-cluster placement under the
Tangle launcher: one ComponentSpec deployed to two different K8s clusters
purely by accelerator constraint.

Requires SkyPilot's `kubernetes.allowed_contexts` to include both contexts
(`sky-dev` and `h200cluster`) and the launcher to be initialized with
`infra=None` so the optimizer can pick per task.
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


_PROMPTS = [
    "The capital of France is",
    "In the year 2050, robots will",
    "A haiku about distributed computing:",
]


_INFER_PY = r"""
import json, os, socket, time
prompts_in = os.environ['PROMPTS_PATH']
out_path = os.environ['OUTPUT_PATH']
os.makedirs(os.path.dirname(out_path), exist_ok=True)

with open(prompts_in) as f:
    prompts = json.load(f)
print(f'[{socket.gethostname()}] loaded {len(prompts)} prompts', flush=True)

# Importing transformers may take a moment; print before & after.
print('[importing transformers]', flush=True)
import torch
from transformers import GPT2LMHeadModel, GPT2Tokenizer
print(f'[torch={torch.__version__} cuda={torch.cuda.is_available()} '
      f'gpu={torch.cuda.get_device_name(0) if torch.cuda.is_available() else "cpu"}]',
      flush=True)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
tok = GPT2Tokenizer.from_pretrained('gpt2')
model = GPT2LMHeadModel.from_pretrained('gpt2').to(device).eval()

results = []
for p in prompts:
    t0 = time.time()
    ids = tok(p, return_tensors='pt').to(device)
    with torch.no_grad():
        out = model.generate(**ids, max_new_tokens=24, do_sample=False,
                             pad_token_id=tok.eos_token_id)
    text = tok.decode(out[0], skip_special_tokens=True)
    elapsed_ms = round((time.time() - t0) * 1000, 1)
    print(f'[{p!r}] ({elapsed_ms}ms) -> {text!r}', flush=True)
    results.append({'prompt': p, 'completion': text,
                    'elapsed_ms': elapsed_ms,
                    'gpu': torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'cpu',
                    'host': socket.gethostname()})

with open(out_path, 'w') as f:
    json.dump(results, f, indent=2)
print(f'[wrote {out_path}, {os.path.getsize(out_path)} bytes]', flush=True)
"""


# --- Task 1: prepare prompts (CPU only, lands wherever sky picks) -----------
prepare_spec = {
    "name": "skypilot-prepare-prompts",
    "outputs": [{"name": "prompts", "type": "String"}],
    "implementation": {
        "container": {
            "image": "python:3.11-slim",
            "command": [
                "bash", "-c",
                'set -euo pipefail; mkdir -p "$(dirname "$0")"; '
                f"python3 -c 'import json,sys; json.dump({json.dumps(_PROMPTS)}, open(sys.argv[1], \"w\"))' \"$0\"; "
                'echo "wrote prompts to $0"; cat "$0"',
                {"outputPath": "prompts"},
            ],
        }
    },
}


def _make_inference_spec(suffix: str) -> dict:
    return {
        # Using the same component name for both H100 and H200 tasks would
        # hit Tangle's cache and reuse one execution for both — bake the
        # accelerator name into the component name so they're distinct.
        "name": f"skypilot-gpt2-inference-{suffix}",
        "inputs": [{"name": "prompts", "type": "String"}],
        "outputs": [{"name": "completions", "type": "String"}],
        "implementation": {
            "container": {
                "image": "pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime",
                "env": {"COMPONENT_VARIANT": suffix},
                "command": [
                    "bash", "-c",
                    'set -euo pipefail; '
                    'export PROMPTS_PATH="$0"; export OUTPUT_PATH="$1"; '
                    # transformers isn't bundled in pytorch image — pip
                    # install once, ~10s on a cold pod.
                    'pip install -q --no-cache-dir transformers==4.41.1 >/dev/null; '
                    'nvidia-smi -L; '
                    f"python3 -u <<'PYEOF'\n{_INFER_PY}\nPYEOF",
                    {"inputPath": "prompts"},
                    {"outputPath": "completions"},
                ],
            }
        },
    }


# --- Task 4: print results from both clusters side-by-side ------------------
compare_spec = {
    "name": "skypilot-compare-completions",
    "inputs": [
        {"name": "h100_completions", "type": "String"},
        {"name": "h200_completions", "type": "String"},
    ],
    "outputs": [{"name": "report", "type": "String"}],
    "implementation": {
        "container": {
            "image": "python:3.11-slim",
            "command": [
                "bash", "-c",
                'set -euo pipefail; mkdir -p "$(dirname "$2")"; '
                'python3 - "$0" "$1" "$2" <<\'PY\'\n'
                'import json, sys\n'
                'h100 = json.load(open(sys.argv[1]))\n'
                'h200 = json.load(open(sys.argv[2]))\n'
                'lines = ["=== Multi-cluster inference comparison ==="]\n'
                'for a, b in zip(h100, h200):\n'
                '    lines.append(f"prompt: {a[\'prompt\']!r}")\n'
                '    lines.append(f"  H100 ({a[\'gpu\']} on {a[\'host\']}, {a[\'elapsed_ms\']}ms): {a[\'completion\']!r}")\n'
                '    lines.append(f"  H200 ({b[\'gpu\']} on {b[\'host\']}, {b[\'elapsed_ms\']}ms): {b[\'completion\']!r}")\n'
                '    lines.append("")\n'
                'report = "\\n".join(lines)\n'
                'print(report)\n'
                'open(sys.argv[3], "w").write(report + "\\n")\n'
                'PY',
                {"inputPath": "h100_completions"},
                {"inputPath": "h200_completions"},
                {"outputPath": "report"},
            ],
        }
    },
}


ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
pipeline_spec = {
    "name": f"skypilot-multicluster-inference-{ts}",
    "outputs": [{"name": "report", "type": "String"}],
    "implementation": {
        "graph": {
            "tasks": {
                "prepare": {
                    "componentRef": {"spec": prepare_spec},
                    "annotations": {
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "1",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "1",
                    },
                },
                "infer_h100": {
                    "componentRef": {"spec": _make_inference_spec("h100")},
                    "arguments": {
                        "prompts": {"taskOutput": {"taskId": "prepare", "outputName": "prompts"}}
                    },
                    "annotations": {
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "2",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "8",
                        "cloud-pipelines.net/launchers/generic/resources.accelerators": "H100:1",
                    },
                },
                "infer_h200": {
                    "componentRef": {"spec": _make_inference_spec("h200")},
                    "arguments": {
                        "prompts": {"taskOutput": {"taskId": "prepare", "outputName": "prompts"}}
                    },
                    "annotations": {
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "2",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "8",
                        "cloud-pipelines.net/launchers/generic/resources.accelerators": "H200:1",
                    },
                },
                "compare": {
                    "componentRef": {"spec": compare_spec},
                    "arguments": {
                        "h100_completions": {"taskOutput": {"taskId": "infer_h100", "outputName": "completions"}},
                        "h200_completions": {"taskOutput": {"taskId": "infer_h200", "outputName": "completions"}},
                    },
                    "annotations": {
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "1",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "1",
                    },
                },
            },
            "outputValues": {
                "report": {"taskOutput": {"taskId": "compare", "outputName": "report"}}
            },
        }
    },
}

print(f"=== submit multi-cluster inference (ts={ts}) ===")
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
    if (summary.get("SUCCEEDED", 0) >= 4 and
            not any(summary.get(k, 0) > 0
                    for k in ("PENDING", "QUEUED", "RUNNING", "WAITING_FOR_UPSTREAM",
                              "STARTING"))):
        break
    time.sleep(20)

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
