#!/bin/sh
# Pull the Railway volume's dealer_logs and fold them into the MBP canonical tree (P6A.5, D-IF10).
#
#   scripts/pull_railway_logs.sh [--apply]
#
# Needs the railway CLI linked to the scanner service, and a post-run hold on the job
# (SCAN_POST_RUN_HOLD_SECONDS, P6A.2) so the container is still up. The pull lands in a fresh
# staging directory under workspace/pulled_logs/; the merge is a dry run unless --apply is
# given (backend/scripts/merge_dealer_logs.py: idempotent, never deletes).
# SCANNER_SERVICE names the Railway service (default scanner-nightly); REMOTE_LOGS the path.
set -eu
cd "$(dirname "$0")/.."
svc="${SCANNER_SERVICE:-scanner-nightly}"
remote="${REMOTE_LOGS:-/data/workspace/dealer_logs}"
stage="workspace/pulled_logs/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$stage"
railway ssh --service "$svc" "tar -C $(dirname "$remote") -cf - $(basename "$remote")" | tar -xf - -C "$stage"
PY="${PYTHON:-python3}"
"$PY" -m backend.scripts.merge_dealer_logs --src "$stage/$(basename "$remote")" --dest workspace/dealer_logs "$@"
echo "staged copy kept at $stage" >&2
