#!/bin/sh

set -e -o pipefail -o nounset

current_path=`pwd`
cd $(dirname $0)

# ! The app does not work when accessed via http://0.0.0.0:8000 (but http://127.0.0.1:8000 or https://* work).
# We can either use `fastapi run --host 127.0.0.1` or `fastapi dev` to make the correct URL show up in the log.
CLOUD_PIPELINES_BACKEND_DATA_DIR="${current_path}/data" uv run --frozen fastapi dev start_local.py "$@"
