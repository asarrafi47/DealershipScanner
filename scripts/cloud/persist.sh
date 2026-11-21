#!/usr/bin/env bash
# Persist a cloud session's loop output into the private data repository (docs/scan_loop/BUNDLE.md
# section 10). A reclaimed VM loses everything that is not pushed, so run this after each dealer or
# agent finishes. It stages only what a session may write, validates new requests first, commits,
# rebases onto the remote and pushes, five attempts with backoff.
#
#   scripts/cloud/persist.sh [-m MESSAGE]
#
# Environment: LOOP_DATA_DIR  the data repository's clone (default /opt/ds-data)
#              PERSIST_SLEEP  seconds before the first retry, doubled each time (default 2)
# Exit: 0 pushed or nothing to push, 1 not pushed (work stays committed locally), 2 usage or setup error.
# Written for bash 3.2 and up. It never reads or writes a credential: git uses the clone's own remote.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DATA="${LOOP_DATA_DIR:-/opt/ds-data}"
MSG="session: dealer logs, proposals, findings, requests"
ATTEMPTS=5
SLEEP="${PERSIST_SLEEP:-2}"

die() { printf 'persist.sh: %s\n' "$1" >&2; exit "${2:-2}"; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    -m) shift; MSG="${1:-}" ;;
    -h|--help) sed -n '2,12p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done
[ -n "$MSG" ] || die "empty commit message"
git -C "$DATA" rev-parse --git-dir >/dev/null 2>&1 || die "$DATA is not a git clone (set LOOP_DATA_DIR)"

# Only these paths are ever staged. Never *.status.json, fix_cycles/, mirror/, runs/ or evidence/.
STAGE_PATHS="workspace/dealer_logs proposals findings"

new_requests() {  # untracked request files (created once, never edited)
  git -C "$DATA" ls-files --others --exclude-standard -- 'requests/rq-*.json' | grep -v '\.status\.json$' || true
}

reqs="$(new_requests)"
if [ -n "$reqs" ]; then
  validator="$ROOT/backend/scripts/ops_requests.py"
  [ -f "$validator" ] || die "new request files exist but $validator is missing: refusing to push unvalidated requests" 1
  files=()
  while IFS= read -r f; do files+=("$DATA/$f"); done <<<"$reqs"
  if ! (cd "$ROOT" && python "$validator" validate "${files[@]}"); then
    printf 'persist.sh: a request was rejected; nothing was staged\n' >&2
    exit 1
  fi
fi

for p in $STAGE_PATHS; do
  [ -e "$DATA/$p" ] && git -C "$DATA" add -A -- "$p"
done
if [ -n "$reqs" ]; then
  while IFS= read -r f; do git -C "$DATA" add -- "$f"; done <<<"$reqs"
fi

if git -C "$DATA" diff --cached --quiet; then
  # Nothing new, but an earlier attempt may have left an unpushed commit.
  if [ -z "$(git -C "$DATA" log '@{u}..HEAD' --oneline 2>/dev/null || true)" ]; then
    echo "persist.sh: nothing to persist"
    exit 0
  fi
else
  git -C "$DATA" commit -q -m "$MSG"
fi

i=1
delay="$SLEEP"
while [ "$i" -le "$ATTEMPTS" ]; do
  if git -C "$DATA" pull -q --rebase 2>/dev/null; then
    if git -C "$DATA" push -q 2>/dev/null; then
      echo "persist.sh: pushed (attempt $i)"
      exit 0
    fi
  else
    # A conflict: abort the rebase, keep the local commit, and try again after a fresh fetch.
    git -C "$DATA" rebase --abort >/dev/null 2>&1 || true
  fi
  git -C "$DATA" fetch -q 2>/dev/null || true
  i=$((i + 1))
  [ "$i" -le "$ATTEMPTS" ] && { sleep "$delay"; delay=$((delay * 2)); }
done
printf 'persist.sh: could not push after %s attempts; the work is committed in %s\n' "$ATTEMPTS" "$DATA" >&2
exit 1
