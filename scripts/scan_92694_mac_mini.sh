#!/usr/bin/env bash
# Local 92694 / 25 mi scan for Mac Mini (16 GB RAM). Does not use production scanner.py.
set -euo pipefail
cd "$(dirname "$0")/.."

MANIFEST="${DEALERS_MANIFEST_PATH:-workspace/manifest_92694_25mi.json}"
export DEALERS_MANIFEST_PATH="$MANIFEST"

exec python3 scanner_mac_mini.py --manifest "$MANIFEST" --scan-only
