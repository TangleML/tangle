"""End-to-end multi-node test for the SkyPilot launcher.

Demonstrates a capability that Tangle's stock kubernetes_launchers cannot
do straightforwardly: a single Tangle ComponentSpec annotated with
`tangleml.com/launchers/kubernetes/multi_node/number_of_nodes: 2` runs as
a 2-pod SkyPilot job with peer addressing and intra-job networking.

Each node:
  - Prints its rank (TANGLE_MULTI_NODE_NODE_INDEX) and peer-0 address
    (bridged from $SKYPILOT_NODE_RANK / $SKYPILOT_NODE_IPS by the launcher).
  - Confirms the launcher's TANGLE_MULTI_NODE_* env-var prelude actually
    fires inside the pod.
  - Node 1 opens a TCP connection to node 0 to prove peer addressing
    works end-to-end (not just env vars).

Only rank 0 writes the report file (its MOUNT path is shared across pods,
so concurrent writes would race).
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


# Single multi-node component.
#
# `python:3.11-slim` is Debian-based — it has /bin/bash, which SkyPilot's
# setup commands require. Alpine/distroless images don't work here.
#
# The launcher's run-script prelude exports these env vars on every pod
# from $SKYPILOT_NUM_NODES / $SKYPILOT_NODE_RANK / $SKYPILOT_NODE_IPS:
#   TANGLE_MULTI_NODE_NUMBER_OF_NODES
#   TANGLE_MULTI_NODE_NODE_INDEX
#   TANGLE_MULTI_NODE_NODE_0_ADDRESS
#   TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES
multinode_spec = {
    # "skypilot-" prefix makes the launcher visible in the Tangle UI / dashboard
    # without changing any launcher defaults.
    "name": "skypilot-multinode-peer-check",
    "outputs": [{"name": "report", "type": "String"}],
    "implementation": {
        "container": {
            "image": "python:3.11-slim",
            "command": [
                "bash", "-c",
                # `bash -c CMD ARG` makes $0 == ARG. $0 is the output path.
                r'''
set -euo pipefail
RANK="${TANGLE_MULTI_NODE_NODE_INDEX:-?}"
NNODES="${TANGLE_MULTI_NODE_NUMBER_OF_NODES:-?}"
PEER0="${TANGLE_MULTI_NODE_NODE_0_ADDRESS:-?}"
ALL="${TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES:-?}"
HOST="$(hostname)"
TS="$(date -u +%FT%TZ)"
echo "[$HOST rank=$RANK/$NNODES] peer0=$PEER0 all=$ALL ts=$TS"
echo "[$HOST rank=$RANK] SKYPILOT_NODE_RANK=${SKYPILOT_NODE_RANK:-unset} \
SKYPILOT_NUM_NODES=${SKYPILOT_NUM_NODES:-unset} \
SKYPILOT_NODE_IPS=${SKYPILOT_NODE_IPS:-unset}"

if [ "$RANK" = "0" ]; then
  python3 -u <<PYEOF
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 12321))
s.listen(1)
s.settimeout(60)
print("[rank 0] listening on :12321 for peer", flush=True)
try:
    conn, addr = s.accept()
    payload = conn.recv(2048).decode()
    print(f"[rank 0] received from {addr}: {payload!r}", flush=True)
    conn.close()
except socket.timeout:
    print("[rank 0] timed out waiting for peer", flush=True)
PYEOF
  mkdir -p "$(dirname "$0")"
  cat > "$0" <<EOF
multinode SUCCESS  rank0=$HOST  peer0=$PEER0  ts=$TS
all_addrs=$ALL
EOF
  echo "[rank 0] wrote report to $0"
else
  # Worker rank: send a message to rank 0.
  sleep 8  # give rank-0 a moment to start listening
  python3 -u <<PYEOF
import socket, os, time
peer = os.environ["TANGLE_MULTI_NODE_NODE_0_ADDRESS"]
rank = os.environ["TANGLE_MULTI_NODE_NODE_INDEX"]
host = socket.gethostname()
for attempt in range(5):
    try:
        with socket.create_connection((peer, 12321), timeout=10) as s:
            s.sendall(f"hello from rank {rank} ({host})".encode())
            print(f"[rank {rank}] sent message to {peer}:12321", flush=True)
            break
    except Exception as e:
        print(f"[rank {rank}] attempt {attempt+1} failed: {e}", flush=True)
        time.sleep(3)
else:
    raise SystemExit(f"[rank {rank}] could not reach rank 0 after 5 attempts")
PYEOF
fi
                ''',
                {"outputPath": "report"},
            ],
        }
    },
}

ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
pipeline_spec = {
    "name": f"multinode-pipeline-{ts}",
    "outputs": [{"name": "result", "type": "String"}],
    "implementation": {
        "graph": {
            "tasks": {
                # Annotations live on the TaskSpec — orchestrator_sql.py forwards
                # task_spec.annotations to launcher.launch_container_task(), but
                # NOT component_spec.metadata.annotations.
                "multinode": {
                    "componentRef": {"spec": multinode_spec},
                    "annotations": {
                        "tangleml.com/launchers/kubernetes/multi_node/number_of_nodes": "2",
                        "cloud-pipelines.net/launchers/generic/resources.cpu": "1",
                        "cloud-pipelines.net/launchers/generic/resources.memory": "1",
                    },
                },
            },
            "outputValues": {
                "result": {
                    "taskOutput": {"taskId": "multinode", "outputName": "report"}
                }
            },
        }
    },
}

print(f"=== submit multinode pipeline (ts={ts}) ===")
body = {"root_task": {"componentRef": {"spec": pipeline_spec}, "arguments": {}}}
run = post("/api/pipeline_runs/", body)
print(json.dumps(run, indent=2))
root_exec = run["root_execution_id"]

print(f"\n=== poll graph_execution_state for {root_exec} ===")
deadline = time.time() + 1800  # 30 min cap
last = None
final_done = False
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
        final_done = True
        break
    if (summary.get("SUCCEEDED", 0) >= 1 and
            not any(summary.get(k, 0) > 0
                    for k in ("PENDING", "QUEUED", "RUNNING", "WAITING_FOR_UPSTREAM",
                              "STARTING"))):
        final_done = True
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
