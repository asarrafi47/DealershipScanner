#!/usr/bin/env bash
#
# nightly_rooftop_refusals.sh — run the rooftop-refusal reporting scripts nightly and
# make their three readings visible without anyone remembering to run them by hand.
#
# backend/scripts/report_rooftop_refusals.py and backend/scripts/rooftop_refusals_census.py
# have existed since the rooftop attribution gate (migrations/V009__rooftop_refusals.sql)
# shipped, but neither had a deploy wrapper or a cron/launchd entry — the ledger was
# write-only in practice because reading it was a thing a human had to remember to do.
# This wrapper is that memory, following the exact same pattern as
# deploy/nightly_data_quality_invariants.sh (same log/report directory layout, same
# read-only posture, same "the run itself failing is never mistaken for clean").
#
# ORDER: schedule this AFTER deploy/nightly_data_quality_invariants.sh (06:00, ~11 min),
# same reasoning as that job runs after nightly_http_refresh.sh -- grade the scans that
# already happened, not yesterday's. See deploy/com.sarraficars.nightly-rooftop-refusals.plist
# (06:20 by default).
#
# BOTH SCRIPTS ARE STRICTLY READ-ONLY (SET SESSION CHARACTERISTICS AS TRANSACTION
# READ ONLY before any query): this wrapper reports, it never un-lists or re-attributes
# a car. See backend/scripts/report_rooftop_refusals.py and rooftop_refusals_census.py
# docstrings for why "refuse to WRITE without evidence, never un-list without it" matters
# here specifically.
#
# EXIT CODES (propagated, worst of the two sub-runs -- never swallowed, matching
# nightly_data_quality_invariants.sh's stance that a data-quality gate must not be
# best-effort):
#   0  both scripts ran; no alarm
#   2  either script's run itself failed (DB unreachable, import error, etc.)
#   3  report_rooftop_refusals.py's regression alarm FIRED: zero refusals recorded
#      while scans ran -- the attribution gate (or its ledger) may have gone silent.
#      See that script's docstring before trusting attribution data.
#
# Install as a launchd job exactly as described in deploy/NIGHTLY_REFRESH.md, using
# deploy/com.sarraficars.nightly-rooftop-refusals.plist.

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
REPORT_DIR="$REPO_ROOT/workspace/rooftop_refusals"
LOG_FILE="$LOG_DIR/rooftop_refusals_${RUN_DATE}.log"
CENSUS_JSON="$REPORT_DIR/census_${RUN_DATE}.json"
REPORT_TXT="$REPORT_DIR/report_${RUN_DATE}.txt"
mkdir -p "$LOG_DIR" "$REPORT_DIR"

log() { printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" | tee -a "$LOG_FILE"; }

log "==================================================================="
log "ROOFTOP REFUSALS REPORTING START  pid=$$  date=${RUN_DATE}"
log "python=${PYTHON}  census=${CENSUS_JSON}  report=${REPORT_TXT}"
log "==================================================================="

# --- 1. rooftop_refusals_census.py: dealer x reason, age distribution, inventory
#        cross-reference. --json for a machine-readable snapshot alongside the
#        human-readable log line count.
START=$(date +%s)
"$PYTHON" -m backend.scripts.rooftop_refusals_census --json > "$CENSUS_JSON" \
  2> >(tee -a "$LOG_FILE" >&2)
CENSUS_RC=$?
log "rooftop_refusals_census.py exit=${CENSUS_RC}  wrote ${CENSUS_JSON}"

# --- 2. report_rooftop_refusals.py: worst-first census, unregistered-rooftop gaps,
#        and the zero-refusal regression alarm (exit 3 when it fires). Plain text;
#        this script has no --json mode.
"$PYTHON" -m backend.scripts.report_rooftop_refusals 2>&1 \
  | while IFS= read -r line; do
      printf '%s   %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$line"
    done | tee "$REPORT_TXT" | tee -a "$LOG_FILE" > /dev/null
REPORT_RC=${PIPESTATUS[0]}
END=$(date +%s)

log "report_rooftop_refusals.py exit=${REPORT_RC}  wrote ${REPORT_TXT}"

# Worst-of-two: a run failure (2) on either script outranks the alarm (3) outranks
# clean (0), because "the reports didn't even run" is worse than "the reports ran and
# something is wrong" -- and a failure must never look like an alarm went unfired.
RC=0
if [ "$CENSUS_RC" -eq 2 ] || [ "$REPORT_RC" -eq 2 ]; then
  RC=2
elif [ "$REPORT_RC" -eq 3 ]; then
  RC=3
elif [ "$CENSUS_RC" -ne 0 ]; then
  RC=2
fi

log "-------------------------------------------------------------------"
log "elapsed: $((END - START))s   census_exit=${CENSUS_RC}  report_exit=${REPORT_RC}  wrapper_exit=${RC}"
case "$RC" in
  0) log "OK: reports generated, no alarm." ;;
  3) log "ALARM: the rooftop refusal gate recorded zero refusals while scans ran. See ${REPORT_TXT}." ;;
  *) log "FAIL: a reporting script did not complete. This is NOT a pass." ;;
esac
log "ROOFTOP REFUSALS REPORTING DONE  pid=$$"
log "==================================================================="

exit "$RC"
