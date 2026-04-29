"""Multi-cluster inference example via the SkyPilot launcher.

Two inference tasks in the same Tangle pipeline, each pinned to a
different cloud/cluster purely by accelerator constraint — sky's
optimizer routes each to the matching K8s context:

  - 'infer_gke_l4':      asks for an L4   → lands on a GKE cluster
  - 'infer_nebius_h100': asks for an H100 → lands on a Nebius cluster

Both run the same Qwen2.5-0.5B-Instruct generation script on a fixed
list of prompts. Outputs land in cloud storage; a final 'compare' task
reads both and prints them side-by-side. Demonstrates SkyPilot's
cross-cluster placement under the Tangle launcher: one ComponentSpec
deployed to two different K8s clusters purely by accelerator
constraint.

Requires SkyPilot's `kubernetes.allowed_contexts` to include both
contexts and the launcher to be initialized with `infra=None` so the
optimizer can pick per task.
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
model_id = os.environ.get('MODEL_ID', 'Qwen/Qwen2.5-0.5B-Instruct')
os.makedirs(os.path.dirname(out_path), exist_ok=True)

with open(prompts_in) as f:
    prompts = json.load(f)
print(f'[{socket.gethostname()}] loaded {len(prompts)} prompts', flush=True)

print('[importing transformers]', flush=True)
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
print(f'[torch={torch.__version__} cuda={torch.cuda.is_available()} '
      f'gpu={torch.cuda.get_device_name(0) if torch.cuda.is_available() else "cpu"}]',
      flush=True)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f'[loading {model_id}]', flush=True)
tok = AutoTokenizer.from_pretrained(model_id)
model = AutoModelForCausalLM.from_pretrained(
    model_id, torch_dtype=torch.bfloat16 if device.type == 'cuda' else torch.float32,
).to(device).eval()

results = []
for p in prompts:
    t0 = time.time()
    # Qwen2.5-Instruct uses a chat template; format as a single user turn.
    messages = [{'role': 'user', 'content': p}]
    chat = tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    ids = tok(chat, return_tensors='pt').to(device)
    with torch.no_grad():
        out = model.generate(**ids, max_new_tokens=64, do_sample=False,
                             pad_token_id=tok.eos_token_id)
    new_tokens = out[0][ids['input_ids'].shape[1]:]
    text = tok.decode(new_tokens, skip_special_tokens=True).strip()
    elapsed_ms = round((time.time() - t0) * 1000, 1)
    print(f'[{p!r}] ({elapsed_ms}ms) -> {text!r}', flush=True)
    results.append({'prompt': p, 'completion': text,
                    'elapsed_ms': elapsed_ms,
                    'model': model_id,
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
        # Using the same component name across both inference tasks would
        # hit Tangle's cache and reuse one execution for both. Encoding
        # `<cloud>-<gpu>` into the component name keeps the cache keys
        # distinct AND surfaces the placement in the Tangle UI / sky
        # dashboard at a glance.
        "name": f"skypilot-qwen-inference-{suffix}",
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
        {"name": "gke_l4_completions", "type": "String"},
        {"name": "nebius_h100_completions", "type": "String"},
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
                'a = json.load(open(sys.argv[1]))   # gke-l4\n'
                'b = json.load(open(sys.argv[2]))   # nebius-h100\n'
                'lines = ["=== Multi-cluster inference comparison ==="]\n'
                'for pa, pb in zip(a, b):\n'
                '    lines.append(f"prompt: {pa[\'prompt\']!r}")\n'
                '    lines.append(f"  gke-l4       ({pa[\'gpu\']} on {pa[\'host\']}, {pa[\'elapsed_ms\']}ms): {pa[\'completion\']!r}")\n'
                '    lines.append(f"  nebius-h100  ({pb[\'gpu\']} on {pb[\'host\']}, {pb[\'elapsed_ms\']}ms): {pb[\'completion\']!r}")\n'
                '    lines.append("")\n'
                'report = "\\n".join(lines)\n'
                'print(report)\n'
                'open(sys.argv[3], "w").write(report + "\\n")\n'
                'PY',
                {"inputPath": "gke_l4_completions"},
                {"inputPath": "nebius_h100_completions"},
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
                "infer_gke_l4": {
                    "componentRef": {"spec": _make_inference_spec("gke-l4")},
                    "arguments": {
                        "prompts": {"taskOutput": {"taskId": "prepare", "outputName": "prompts"}}
                    },
                    "annotations": {
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "2",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "8",
                        # H100 is what's actually available in our test
                        # environment; swap to "L4:1" once a GKE-L4 cluster
                        # is in allowed_contexts to make the name match
                        # the placement.
                        "cloud-pipelines.net/launchers/generic/resources.accelerators": "H100:1",
                    },
                },
                "infer_nebius_h100": {
                    "componentRef": {"spec": _make_inference_spec("nebius-h100")},
                    "arguments": {
                        "prompts": {"taskOutput": {"taskId": "prepare", "outputName": "prompts"}}
                    },
                    "annotations": {
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "2",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "8",
                        # Asking for H200 here so this task is forced onto a
                        # different K8s context than the H100 one, exercising
                        # cross-cluster placement. Swap to "H100:1" once a
                        # Nebius-H100 cluster is in allowed_contexts.
                        "cloud-pipelines.net/launchers/generic/resources.accelerators": "H200:1",
                    },
                },
                "compare": {
                    "componentRef": {"spec": compare_spec},
                    "arguments": {
                        "gke_l4_completions": {"taskOutput": {"taskId": "infer_gke_l4", "outputName": "completions"}},
                        "nebius_h100_completions": {"taskOutput": {"taskId": "infer_nebius_h100", "outputName": "completions"}},
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
