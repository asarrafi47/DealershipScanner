#!/usr/bin/env bash
# Hydrate an analysis session (docs/scan_loop/BUNDLE.md section 10): make the private data repository
# available next to the public code checkout, so ops_gates.py runs on the newest run and every
# workspace/dealer_logs path resolves. Idempotent. Used in the `dealership-analysis` environment, where the
# data repository is the attached repository (its checkout path is the first argument).
#
#   scripts/cloud/ops_hydrate.sh DATA_CLONE [CODE_DIR]
#
# DATA_CLONE  the attached data repository (default: $LOOP_DATA_DIR or /opt/ds-data)
# CODE_DIR    where the public code is cloned read-only, anonymous HTTPS, dev tip (default /opt/ds-code)
# Environment: LOOP_CODE_URL  the code repository URL (default https://github.com/asarrafi47/DealershipScanner.git)
# Output: the export lines to append to $CLAUDE_ENV_FILE are printed, and appended when it is set.
# Written for bash 3.2 and up. It reads and writes no credential.
set -euo pipefail

DATA="${1:-${LOOP_DATA_DIR:-/opt/ds-data}}"
CODE="${2:-/opt/ds-code}"
CODE_URL="${LOOP_CODE_URL:-https://github.com/asarrafi47/DealershipScanner.git}"

die() { printf 'ops_hydrate.sh: %s\n' "$1" >&2; exit "${2:-1}"; }

[ -d "$DATA/.git" ] || die "$DATA is not a git clone of the data repository" 2

if [ ! -d "$CODE/.git" ]; then
  git clone -q --depth 1 --branch dev "$CODE_URL" "$CODE" || die "cannot clone the code repository"
else
  git -C "$CODE" fetch -q --depth 1 origin dev && git -C "$CODE" checkout -q -f FETCH_HEAD || die "cannot update $CODE"
fi

# The runs branch: newest bundles in a second, detached worktree (Railway rewrites it as an orphan branch, so the
# refspec is forced).
RUNS=""
if git -C "$DATA" fetch -q --depth 1 origin '+runs:refs/remotes/origin/runs' 2>/dev/null; then
  RUNS="$DATA-runs"
  if [ -d "$RUNS" ]; then
    git -C "$RUNS" checkout -q -f --detach origin/runs
  else
    git -C "$DATA" worktree add -q --detach "$RUNS" origin/runs
  fi
else
  printf 'ops_hydrate.sh: no runs branch yet (the first Railway publish creates it)\n' >&2
fi

mkdir -p "$DATA/workspace/dealer_logs"
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

lines=(
  "export LOOP_DATA_DIR=\"$DATA\""
  "export DEALER_LOGS_ROOT=\"$DATA/workspace/dealer_logs\""
  "export RECIPES_DB_DISABLED=1"
  "export RECIPES_CACHE_DIR=\"$DATA/proposals/_scratch\""
)
[ -z "$RUNS" ] || lines+=("export LOOP_RUNS_DIR=\"$RUNS\"")
mkdir -p "$DATA/proposals/_scratch"
for l in "${lines[@]}"; do
  printf '%s\n' "$l"
  if [ -n "${CLAUDE_ENV_FILE:-}" ]; then grep -qxF -- "$l" "$CLAUDE_ENV_FILE" 2>/dev/null || printf '%s\n' "$l" >>"$CLAUDE_ENV_FILE"; fi
done
