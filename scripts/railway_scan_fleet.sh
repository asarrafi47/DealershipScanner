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
cd /app
exec python3 -m backend.scripts.fleet_scan
