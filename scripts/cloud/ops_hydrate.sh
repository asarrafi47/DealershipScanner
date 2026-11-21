#!/usr/bin/env bash
# Hydrate an analysis session (docs/scan_loop/README.md): pull the loop's files from the scan loop API into a data
# folder next to the public code checkout, so ops_gates.py runs on the newest run and every workspace/dealer_logs
# path resolves. Idempotent. Used in the `dealership-analysis` environment setup script.
#
#   scripts/cloud/ops_hydrate.sh [DATA_DIR [CODE_DIR]]
#
# DATA_DIR  where the files land (default: $LOOP_DATA_DIR or /opt/ds-data)
# CODE_DIR  where the public code is cloned read-only, anonymous HTTPS, dev tip (default /opt/ds-code)
# Environment: LOOP_API_URL, LOOP_READ_TOKEN (set in the environment; never printed or written to a file),
#              LOOP_CODE_URL  the code repository URL (default https://github.com/asarrafi47/DealershipScanner.git)
# Output: the export lines to append to $CLAUDE_ENV_FILE are printed, and appended when it is set.
# Written for bash 3.2 and up.
set -euo pipefail

DATA="${1:-${LOOP_DATA_DIR:-/opt/ds-data}}"
CODE="${2:-/opt/ds-code}"
CODE_URL="${LOOP_CODE_URL:-https://github.com/asarrafi47/DealershipScanner.git}"

die() { printf 'ops_hydrate.sh: %s\n' "$1" >&2; exit "${2:-1}"; }

[ -n "${LOOP_READ_TOKEN:-}" ] || die "LOOP_READ_TOKEN is not set" 2
[ -n "${LOOP_API_URL:-}" ] || die "LOOP_API_URL is not set" 2

if [ ! -d "$CODE/.git" ]; then
  git clone -q --depth 1 --single-branch --branch dev "$CODE_URL" "$CODE" || die "cannot clone the code repository"
else
  git -C "$CODE" fetch -q --depth 1 origin dev && git -C "$CODE" checkout -q -f FETCH_HEAD || die "cannot update $CODE"
fi

mkdir -p "$DATA/workspace/dealer_logs"
python3 "$CODE/scripts/cloud/loop_client.py" pull --out "$DATA" || die "cannot pull the loop files"
[ -f "$DATA/CLAUDE.md" ] || cp "$CODE/docs/scan_loop/data_template/CLAUDE.md" "$DATA/CLAUDE.md"

# The code repository ignores workspace/, so the link does not dirty it.
if [ -L "$CODE/workspace" ]; then
  :
elif [ -e "$CODE/workspace" ]; then
  die "$CODE/workspace exists and is not a link" 1
else
  ln -s "$DATA/workspace" "$CODE/workspace"
fi
# .gitignore's "workspace/" does not match a symlink, so keep the link out of `git status` locally.
exclude="$(git -C "$CODE" rev-parse --git-path info/exclude)"
case "$exclude" in /*) ;; *) exclude="$CODE/$exclude" ;; esac
mkdir -p "$(dirname "$exclude")"
grep -qxF 'workspace' "$exclude" 2>/dev/null || printf 'workspace\n' >>"$exclude"

mkdir -p "$DATA/proposals/_scratch"
lines=(
  "export LOOP_DATA_DIR=\"$DATA\""
  "export DEALER_LOGS_ROOT=\"$DATA/workspace/dealer_logs\""
  "export RECIPES_DB_DISABLED=1"
  "export RECIPES_CACHE_DIR=\"$DATA/proposals/_scratch\""
  "export LOOP_RUNS_DIR=\"$DATA/runs\""
)
for l in "${lines[@]}"; do
  printf '%s\n' "$l"
  if [ -n "${CLAUDE_ENV_FILE:-}" ]; then grep -qxF -- "$l" "$CLAUDE_ENV_FILE" 2>/dev/null || printf '%s\n' "$l" >>"$CLAUDE_ENV_FILE"; fi
done
