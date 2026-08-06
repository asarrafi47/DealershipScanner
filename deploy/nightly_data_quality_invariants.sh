#!/usr/bin/env bash
#
# nightly_data_quality_invariants.sh — run the data-quality invariant suite and
# fail loudly when a spec-pipeline defect gets WORSE than its recorded baseline.
#
# Why this is scheduled rather than run by hand: every defect the suite encodes
# shipped to shoppers and was found months later by a person reading a car page.
# A check that only runs when someone remembers it is the same as no check.
#
# ORDER MATTERS. Schedule this AFTER deploy/nightly_http_refresh.sh has finished,
# so it grades the inventory the refresh just wrote. Running it first grades
# yesterday's data and tells you nothing about last night's scrape.
#
# EXIT CODES (this wrapper propagates the script's, unlike nightly_http_refresh.sh
# which is deliberately best-effort — a data-quality gate that swallows its own
# failure is not a gate):
#   0  every invariant at or below baseline
#   1  at least one invariant regressed, or has no baseline entry
#   2  the run itself failed — treated as a failure, never as "clean"
#
# The suite is READ-ONLY: it opens the inventory in a session where every
# transaction is READ ONLY, so it cannot repair what it finds. It reports; a
# human fixes, and then re-records the baseline explicitly with --write-baseline.
#
# Install as a launchd job — same procedure as deploy/NIGHTLY_REFRESH.md, using
# deploy/com.sarraficars.nightly-data-quality.plist.

set -u

REPO_ROOT="/Users/asarrafi/Projects/DealershipScanner"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to $REPO_ROOT" >&2; exit 2; }

set -a
# shellcheck disable=SC1091
source "$REPO_ROOT/.env" >/dev/null 2>&1 || true
set +a
export PYTHONPATH="$REPO_ROOT"

PYTHON="$REPO_ROOT/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

RUN_DATE="$(date '+%Y%m%d')"
LOG_DIR="$REPO_ROOT/workspace/scanlogs"
REPORT_DIR="$REPO_ROOT/workspace/data_quality"
LOG_FILE="$LOG_DIR/data_quality_${RUN_DATE}.log"
REPORT_JSON="$REPORT_DIR/invariants_${RUN_DATE}.json"
mkdir -p "$LOG_DIR" "$REPORT_DIR"

log() { printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" | tee -a "$LOG_FILE"; }

log "==================================================================="
log "DATA-QUALITY INVARIANTS START  pid=$$  date=${RUN_DATE}"
log "python=${PYTHON}  report=${REPORT_JSON}"
log "==================================================================="

# Any extra arguments are passed straight through, so the same wrapper can be run
# by hand as `nightly_data_quality_invariants.sh --sql-only` for a fast check.
START=$(date +%s)
"$PYTHON" -m backend.scripts.data_quality_invariants --json "$REPORT_JSON" "$@" 2>&1 \
  | while IFS= read -r line; do
      printf '%s   %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$line"
    done | tee -a "$LOG_FILE"
RC=${PIPESTATUS[0]}
END=$(date +%s)

log "-------------------------------------------------------------------"
log "elapsed: $((END - START))s   exit: ${RC}"
case "$RC" in
  0) log "OK: no invariant regressed against its baseline." ;;
  1) log "FAIL: an invariant regressed (or has no baseline). See ${REPORT_JSON}." ;;
  *) log "FAIL: the invariant run itself did not complete. This is NOT a pass." ;;
esac
log "DATA-QUALITY INVARIANTS DONE  pid=$$"
log "==================================================================="

exit "$RC"
