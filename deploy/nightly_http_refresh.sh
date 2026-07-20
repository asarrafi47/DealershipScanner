#!/usr/bin/env bash
#
# nightly_http_refresh.sh — browser-free nightly inventory freshness pass.
#
# Runs four HTTP-only steps in order (NO Chrome/Playwright is ever launched):
#   1. HTTP delta refresh   (scanner.py --delta) — replays captured API recipes
#      over plain HTTP: upserts fresh prices/inventory, marks sold cars inactive.
#   2. harvest_carscommerce — bulk field fill from the shared CarsCommerce API.
#   3. heal_from_recipes    — fill remaining gaps from other captured recipes.
#   4. harvest_html_jsonld  — fill gaps on the server-rendered JSON-LD platform
#      (Honda of El Cajon, Pacific VW, ...). Honors SCANNER_HTTP_PROXY when set,
#      which is how the Cloudflare-walled dealers get through.
#   5. rebuild_listings_index — recompute the incomplete-listings index.
#
# Design:
#   * Idempotent and safe to run repeatedly.
#   * A PID lock file guarantees runs never overlap.
#   * Every step is best-effort: a failure is logged, not fatal — freshness is
#     more valuable than atomicity, so we always push through all four steps.
#   * Browser-free is asserted: chrome/chromium process counts are snapshotted
#     around the delta step and any increase attributable to this job is flagged.
#
# Install as a launchd job — see deploy/NIGHTLY_REFRESH.md.

set -u

# ---------------------------------------------------------------------------
# Paths / environment
# ---------------------------------------------------------------------------
REPO_ROOT="/Users/asarrafi/Projects/DealershipScanner"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to $REPO_ROOT" >&2; exit 1; }

# Load .env (KEY=VALUE lines) into the environment, then set PYTHONPATH.
set -a
# shellcheck disable=SC1091
source "$REPO_ROOT/.env" >/dev/null 2>&1 || true
set +a
export PYTHONPATH="$REPO_ROOT"

PYTHON="python3"                       # system homebrew python3 (verified working)
MANIFEST="workspace/manifest_92694_25mi_full.json"

RUN_DATE="$(date '+%Y%m%d')"
LOG_DIR="$REPO_ROOT/workspace/scanlogs"
LOG_FILE="$LOG_DIR/nightly_${RUN_DATE}.log"
LOCK_FILE="$LOG_DIR/.nightly_http_refresh.lock"
mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Logging helpers — timestamp every line, tee to the dated log.
# ---------------------------------------------------------------------------
log() { printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*" | tee -a "$LOG_FILE"; }

# Run a command, prefix each output line with a timestamp, return its real exit
# code (not the pipeline's). Output is teed to the log.
run_step() {
  local label="$1"; shift
  log ">>> STEP START: ${label}"
  local start_ts end_ts rc
  start_ts=$(date +%s)
  {
    "$@" 2>&1 | while IFS= read -r line; do
      printf '%s   %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$line"
    done
    exit "${PIPESTATUS[0]}"
  } | tee -a "$LOG_FILE"
  rc=${PIPESTATUS[0]}
  end_ts=$(date +%s)
  if [ "$rc" -eq 0 ]; then
    log "<<< STEP OK: ${label} (rc=0, $((end_ts - start_ts))s)"
  else
    log "!!! STEP FAILED (continuing): ${label} (rc=${rc}, $((end_ts - start_ts))s)"
  fi
  return "$rc"
}

# Count live *Playwright-launched* browser processes only.
# Playwright runs chromium/headless_shell out of the ms-playwright cache, so its
# argv path contains "ms-playwright" or "headless_shell". This deliberately does
# NOT match the user's desktop Google Chrome (runs from /Applications), which
# would otherwise produce false positives as tabs open and close.
count_browsers() {
  pgrep -f 'ms-playwright|headless_shell' 2>/dev/null | wc -l | tr -d ' '
}

# ---------------------------------------------------------------------------
# Lock — never overlap. PID file; stale lock (dead PID) is reclaimed.
# ---------------------------------------------------------------------------
if [ -f "$LOCK_FILE" ]; then
  OTHER_PID="$(cat "$LOCK_FILE" 2>/dev/null || true)"
  if [ -n "${OTHER_PID:-}" ] && kill -0 "$OTHER_PID" 2>/dev/null; then
    log "ANOTHER RUN IS ACTIVE (pid=${OTHER_PID}); exiting."
    exit 0
  fi
  log "Stale lock found (pid=${OTHER_PID:-?} not running); reclaiming."
fi
echo "$$" > "$LOCK_FILE"
cleanup() { rm -f "$LOCK_FILE"; }
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
OVERALL_START=$(date +%s)
FAILS=0

log "==================================================================="
log "NIGHTLY HTTP REFRESH START  (browser-free)  pid=$$  date=${RUN_DATE}"
log "repo=${REPO_ROOT}  python=$(command -v $PYTHON)  manifest=${MANIFEST}"
log "==================================================================="

# Baseline incomplete-listings count (informational).
INC_BEFORE="$(psql postgresql://localhost/cars -tc "SELECT COUNT(*) FROM incomplete_listings;" 2>/dev/null | tr -d ' ')"
log "incomplete_listings BEFORE: ${INC_BEFORE:-<unavailable>}"

# Browser snapshot before the browser-sensitive step.
BROWSERS_BEFORE="$(count_browsers)"
log "browser processes before delta step: ${BROWSERS_BEFORE}"

# --- Step 1: HTTP delta refresh (HTTP-only by design) ----------------------
# Roster from ACTIVE INVENTORY, not a single regional manifest. The old
# "--manifest $MANIFEST" scoped the nightly delta to one region (92694 SoCal
# 25mi), so every dealer scanned in another region went stale (stale/partial
# inventory + stale prices). DEALERS_FROM_ACTIVE_INVENTORY=1 rosters the delta
# from every dealer with live rows in `cars` (all of which have stored recipes);
# no-recipe dealers skip fast. Per-dealer timeout raised to 900s so the biggest
# lots (2000+ VIN replays) finish instead of timing out at the 300s default.
run_step "1/4 http-delta-refresh" \
  env DEALERS_FROM_ACTIVE_INVENTORY=1 SCANNER_DELTA_DEALER_TIMEOUT=900 SCANNER_DELTA_CONCURRENCY=8 \
  "$PYTHON" scanner.py --delta || FAILS=$((FAILS+1))

# Browser snapshot after — flag any NEW browser processes this job may have spawned.
BROWSERS_AFTER="$(count_browsers)"
log "browser processes after delta step: ${BROWSERS_AFTER}"
if [ "${BROWSERS_AFTER:-0}" -gt "${BROWSERS_BEFORE:-0}" ]; then
  log "WARNING: browser process count rose (${BROWSERS_BEFORE} -> ${BROWSERS_AFTER})."
  log "WARNING: --delta must be browser-free. If no other Playwright scan is running,"
  log "WARNING: this is a BUG — the delta path launched a browser. Investigate."
else
  log "OK: no net new browser processes from the delta step."
fi

# --- Step 2: CarsCommerce bulk field fill ----------------------------------
run_step "2/4 harvest-carscommerce" \
  "$PYTHON" backend/scripts/harvest_carscommerce.py || FAILS=$((FAILS+1))

# --- Step 3: heal from other captured recipes ------------------------------
run_step "3/5 heal-from-recipes" \
  "$PYTHON" backend/scripts/heal_from_recipes.py || FAILS=$((FAILS+1))

# --- Step 4: HTML/JSON-LD platform (server-rendered; no API) ----------------
# Fetches each incomplete car's VDP over plain HTTP and fills from JSON-LD.
# Cloudflare-walled dealers only get through when SCANNER_HTTP_PROXY is set;
# without it, blocked fetches are logged and skipped (best-effort, no crash).
run_step "4/5 harvest-html-jsonld" \
  "$PYTHON" backend/scripts/harvest_html_jsonld.py || FAILS=$((FAILS+1))

# --- Step 5: rebuild incomplete-listings index -----------------------------
run_step "5/5 rebuild-listings-index" \
  "$PYTHON" backend/scripts/rebuild_listings_index.py || FAILS=$((FAILS+1))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
INC_AFTER="$(psql postgresql://localhost/cars -tc "SELECT COUNT(*) FROM incomplete_listings;" 2>/dev/null | tr -d ' ')"
OVERALL_END=$(date +%s)

log "-------------------------------------------------------------------"
log "incomplete_listings AFTER:  ${INC_AFTER:-<unavailable>}  (before: ${INC_BEFORE:-?})"
log "steps failed: ${FAILS}/5"
log "total elapsed: $((OVERALL_END - OVERALL_START))s"
log "NIGHTLY HTTP REFRESH DONE  pid=$$"
log "==================================================================="

# Exit 0 even on step failures (best-effort freshness); reserve nonzero for
# infrastructure problems handled above.
exit 0
