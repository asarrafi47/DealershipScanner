#!/bin/sh
# Railway scanner job (docs/RAILWAY_SCANNING.md): HTTP-only fleet scan + post steps.
#
# Logs and every other workspace/ file (dealer_logs, recipe file cache, pipeline
# run dirs, scan logs) live on the service volume: /app/workspace -> $SCAN_DATA_DIR/workspace
# (default /data, the volume mount). Idle unless SCAN_FLEET=1 or SCAN_DEALERS is set.
set -eu
export PYTHONPATH="${PYTHONPATH:-/app}"
DATA="${SCAN_DATA_DIR:-/data}"
mkdir -p "$DATA/workspace"
if [ ! -L /app/workspace ]; then
  if [ -d /app/workspace ]; then
    # an image built with workspace/ (not ours: .dockerignore drops it) — keep it, copy nothing
    echo "WARN: /app/workspace is a real directory; logs stay in the container" >&2
  else
    ln -s "$DATA/workspace" /app/workspace
  fi
fi
# Railway injects DATABASE_URL-style references; the scanner reads INVENTORY_DATABASE_URL.
if [ -z "${INVENTORY_DATABASE_URL:-}" ] && [ -n "${DATABASE_URL:-}" ]; then
  export INVENTORY_DATABASE_URL="$DATABASE_URL"
fi
# Railway is a datacenter IP that some dealer edges (Cloudflare) refuse while a home
# IP gets through. The tag keeps its 401/403s out of the shared recipe store:
# recipes stay live, scan_hints.recipe_status = blocked:railway:... (recipes.egress_tag).
export SCANNER_EGRESS_TAG="${SCANNER_EGRESS_TAG:-railway}"
cd /app
# Scan loop (docs/scan_loop/BUNDLE.md section 10): when the private data repository is configured, the per-dealer
# logs of this run are written inside its clone on the volume, and fleet_scan publishes them afterwards. A failed
# sync only leaves the logs where they were (workspace/dealer_logs on the volume); it never blocks the scan.
if [ -n "${LOOP_DATA_REPO:-}" ] && [ -n "${LOOP_DATA_TOKEN:-}" ]; then
  export LOOP_DATA_DIR="${LOOP_DATA_DIR:-$DATA/loop-data}"
  if python3 -m backend.scripts.ops_publish sync; then
    export DEALER_LOGS_ROOT="$LOOP_DATA_DIR/workspace/dealer_logs"
  else
    echo "WARN: data repository sync failed; dealer logs stay on the volume" >&2
  fi
fi
exec python3 -m backend.scripts.fleet_scan
