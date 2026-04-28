"""Publish SkyPilot-flavored components into Tangle's component library.

Run once after starting Tangle (start_local_skypilot.py) and the components
will appear in the UI's component picker when building pipelines. Each
component name is prefixed with "SkyPilot:" so they're easy to spot.

Usage:
    python examples/publish_skypilot_components.py

Idempotent — already-published components return 409 and are skipped.
"""
from __future__ import annotations
import json
import sys
import urllib.error
import urllib.request

BASE = "http://localhost:9091"


_GPU_SANITY_CHECK = """\
name: 'SkyPilot: GPU Sanity Check'
description: |
  Demonstrates SkyPilot-specific capabilities exposed through the Tangle
  launcher contract:
    * GPU accelerator request via cloud-pipelines.net resource annotations
    * Multi-node coordination via TANGLE_MULTI_NODE_* env vars (bridged from
      SKYPILOT_NODE_RANK / NODE_IPS by the SkyPilot launcher prelude)
    * SkyPilot-only annotations: priority_class (Kueue) and use_spot

inputs:
  - {name: epochs, type: Integer, default: "1"}
outputs:
  - {name: report, type: String, description: "Sanity-check report."}
implementation:
  container:
    image: pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime
    command:
      - sh
      - -c
      - |
        set -euo pipefail
        mkdir -p "$(dirname "$1")"
        python -c "
        import os, socket, datetime
        try:
            import torch
            cuda_ok = torch.cuda.is_available()
            n = torch.cuda.device_count() if cuda_ok else 0
            name = torch.cuda.get_device_name(0) if cuda_ok else 'no-gpu'
        except Exception as e:
            cuda_ok, n, name = False, 0, f'torch-import-failed: {e}'
        rank = os.environ.get('TANGLE_MULTI_NODE_NODE_INDEX', '0')
        nnodes = os.environ.get('TANGLE_MULTI_NODE_NUMBER_OF_NODES', '1')
        peer0 = os.environ.get('TANGLE_MULTI_NODE_NODE_0_ADDRESS', 'localhost')
        msg = (f'host={socket.gethostname()}  rank={rank}/{nnodes}  '
               f'peer0={peer0}  cuda={cuda_ok}  ndev={n}  gpu={name}  '
               f'epochs=$0  ts={datetime.datetime.utcnow().isoformat()}Z')
        print(msg)
        with open('$1', 'w') as f: f.write(msg + chr(10))
        " "$0" "$1"
      - {inputValue: epochs}
      - {outputPath: report}
"""


_MULTINODE_PEER_CHECK = """\
name: 'SkyPilot: Multi-node Peer Check'
description: |
  Two-pod SkyPilot managed job that prints rank/peer info and proves
  intra-job networking by having rank 1 send a TCP message to rank 0.
  Annotate the TaskSpec with
  `tangleml.com/launchers/kubernetes/multi_node/number_of_nodes: "2"`
  to actually launch as multi-node.

outputs:
  - {name: report, type: String}
implementation:
  container:
    image: python:3.11-slim
    command:
      - bash
      - -c
      - |
        set -euo pipefail
        RANK="${TANGLE_MULTI_NODE_NODE_INDEX:-?}"
        NNODES="${TANGLE_MULTI_NODE_NUMBER_OF_NODES:-?}"
        PEER0="${TANGLE_MULTI_NODE_NODE_0_ADDRESS:-?}"
        ALL="${TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES:-?}"
        HOST="$(hostname)"
        TS="$(date -u +%FT%TZ)"
        echo "[$HOST rank=$RANK/$NNODES] peer0=$PEER0 all=$ALL ts=$TS"

        if [ "$RANK" = "0" ]; then
          python3 -u <<'PY'
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", 12321)); s.listen(1); s.settimeout(60)
        print("[rank 0] listening", flush=True)
        try:
            conn, addr = s.accept()
            print(f"[rank 0] received from {addr}: {conn.recv(2048).decode()!r}",
                  flush=True)
            conn.close()
        except socket.timeout:
            print("[rank 0] timed out", flush=True)
        PY
          mkdir -p "$(dirname "$0")"
          printf 'rank0=%s peer0=%s ts=%s\\n' "$HOST" "$PEER0" "$TS" > "$0"
        else
          sleep 8
          python3 -u - <<PY
        import socket, os, time
        peer = os.environ["TANGLE_MULTI_NODE_NODE_0_ADDRESS"]
        rank = os.environ["TANGLE_MULTI_NODE_NODE_INDEX"]
        for i in range(5):
            try:
                with socket.create_connection((peer, 12321), timeout=10) as s:
                    s.sendall(f"hello from rank {rank}".encode())
                    print(f"[rank {rank}] sent to {peer}:12321", flush=True)
                    break
            except Exception as e:
                print(f"[rank {rank}] attempt {i+1} failed: {e}", flush=True)
                time.sleep(3)
        else:
            raise SystemExit("could not reach rank 0")
        PY
        fi
      - {outputPath: report}
"""


_COMPONENTS = [
    ("SkyPilot: GPU Sanity Check", _GPU_SANITY_CHECK),
    ("SkyPilot: Multi-node Peer Check", _MULTINODE_PEER_CHECK),
]


def publish(name: str, text: str) -> None:
    body = {"text": text}
    req = urllib.request.Request(
        BASE + "/api/published_components/",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
        print(f"published: {name!r}  digest={resp.get('digest')}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        if e.code == 409 or "already exists" in body:
            print(f"already exists: {name!r}")
        else:
            print(f"FAILED: {name!r}  HTTP {e.code}: {body}", file=sys.stderr)
            sys.exit(1)


for n, t in _COMPONENTS:
    publish(n, t)
