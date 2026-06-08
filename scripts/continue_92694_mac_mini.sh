#!/usr/bin/env bash
# Resume 92694 / 25 mi scan on Mac Mini — one dealer per subprocess (fresh browser, lower RAM).
set -euo pipefail
cd "$(dirname "$0")/.."

export DEALERS_MANIFEST_PATH="${DEALERS_MANIFEST_PATH:-workspace/manifest_92694_25mi.json}"
LOG="workspace/scan_lab_logs/92694_25mi_macmini_continue_$(date +%Y%m%d_%H%M%S).log"

echo "[continue] logging to $LOG"
exec python3 scanner_mac_mini.py --continue 2>&1 | tee -a "$LOG"
