#!/usr/bin/env bash
# Persist a cloud session's loop output through the scan loop API (docs/scan_loop/README.md). A reclaimed VM
# loses everything that is not sent, so run this after each dealer or agent finishes. It validates new requests
# locally first, then sends the session's new requests, proposals, findings and its summary.md / location.md notes
# (scripts/cloud/loop_client.py push), five attempts with backoff. The server checks every file again.
#
#   scripts/cloud/persist.sh
#
# Environment: LOOP_DATA_DIR      the hydrated data folder (default /opt/ds-data)
#              LOOP_API_URL       the web service's https base URL
#              LOOP_SUBMIT_TOKEN  the submit token (set in the environment; never printed, never written to a file)
#              PERSIST_SLEEP      seconds before the first retry, doubled each time (default 2)
# Exit: 0 sent or nothing to send, 1 not sent (the files stay in place), 2 usage or setup error.
# Written for bash 3.2 and up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DATA="${LOOP_DATA_DIR:-/opt/ds-data}"
ATTEMPTS=5
SLEEP="${PERSIST_SLEEP:-2}"

die() { printf 'persist.sh: %s\n' "$1" >&2; exit "${2:-2}"; }

case "${1:-}" in -h|--help) sed -n '2,13p' "${BASH_SOURCE[0]}"; exit 0 ;; "") ;; *) die "unknown option: $1" ;; esac
[ -d "$DATA" ] || die "$DATA is not a folder (run scripts/cloud/ops_hydrate.sh, or set LOOP_DATA_DIR)"
[ -n "${LOOP_SUBMIT_TOKEN:-}" ] || die "LOOP_SUBMIT_TOKEN is not set"
[ -n "${LOOP_API_URL:-}" ] || die "LOOP_API_URL is not set"

validator="$ROOT/backend/scripts/ops_requests.py"
files=()
for f in "$DATA"/requests/rq-*.json; do
  [ -e "$f" ] || continue
  case "$f" in *.status.json) continue ;; esac
  [ -e "${f%.json}.status.json" ] || files+=("$f")
done
if [ "${#files[@]}" -gt 0 ]; then
  [ -f "$validator" ] || die "request files exist but $validator is missing: refusing to send unvalidated requests" 1
  if ! (cd "$ROOT" && python "$validator" validate "${files[@]}"); then
    printf 'persist.sh: a request was rejected; nothing was sent\n' >&2
    exit 1
  fi
fi

i=1
delay="$SLEEP"
while [ "$i" -le "$ATTEMPTS" ]; do
  if python3 "$ROOT/scripts/cloud/loop_client.py" push --root "$DATA"; then
    echo "persist.sh: sent (attempt $i)"
    exit 0
  fi
  i=$((i + 1))
  [ "$i" -le "$ATTEMPTS" ] && { sleep "$delay"; delay=$((delay * 2)); }
done
printf 'persist.sh: could not send after %s attempts; the files are still in %s\n' "$ATTEMPTS" "$DATA" >&2
exit 1
