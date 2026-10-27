#!/usr/bin/env bash
#
# run_home_lane.sh — the home-IP egress lane (P15A.2): scans the dealers that answer only
# from a residential IP (deploy/railway/home_lane_dealers.txt) and writes to the PROD
# database, from a dedicated prod-lane clone. See deploy/home_lane/README.md.
#
# Guards (each one refuses with exit 2 before anything is scanned):
#   * runs only from a clone that carries the marker file .prod_lane_clone, never from
#     the main checkout (its workspace/ holds local recipes and dealer logs);
#   * HOME_LANE_DATABASE_URL from the env or ~/.config/sarraficars/home_lane.env, a file
#     outside the repo that is not group- or world-readable;
#   * the target may not be localhost / 127.* / ::1 / port 15432 unless
#     HOME_LANE_ALLOW_LOCAL=1 (a mini .env once sent 668 rows to the wrong DB).
#
# Usage:  run_home_lane.sh [--dry-run]     (--dry-run: guards, then print the roster, no scan)
#
# Credentials are never printed: only the target host and port.

set -u

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SELF_DIR/../.." && pwd)"
ENV_FILE="${HOME_LANE_ENV_FILE:-$HOME/.config/sarraficars/home_lane.env}"
ROSTER_FILE="$REPO_ROOT/deploy/railway/home_lane_dealers.txt"
DNS_FILE="$REPO_ROOT/deploy/railway/do_not_scan.txt"

DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    *) echo "usage: run_home_lane.sh [--dry-run]" >&2; exit 64 ;;
  esac
done

refuse() { echo "REFUSED: $*" >&2; exit 2; }

# --- 1. dedicated prod-lane clone -------------------------------------------------------
[ -f "$REPO_ROOT/.prod_lane_clone" ] \
  || refuse "$REPO_ROOT is not a prod-lane clone (no .prod_lane_clone marker). Run this from a dedicated clone, e.g. ~/sarraficars-prod-lane, and 'touch .prod_lane_clone' there."

# --- 2. DSN source ----------------------------------------------------------------------
if [ -z "${HOME_LANE_DATABASE_URL:-}" ]; then
  [ -f "$ENV_FILE" ] || refuse "HOME_LANE_DATABASE_URL is not set and $ENV_FILE does not exist."
  case "$ENV_FILE" in
    "$REPO_ROOT"/*) refuse "$ENV_FILE is inside the repo; keep the DSN file outside it." ;;
  esac
  perms="$(stat -f '%Lp' "$ENV_FILE" 2>/dev/null || stat -c '%a' "$ENV_FILE" 2>/dev/null || echo 777)"
  [ $(( 8#$perms & 8#077 )) -eq 0 ] \
    || refuse "$ENV_FILE is mode $perms; it must not be group- or world-readable (chmod 600)."
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi
DSN="${HOME_LANE_DATABASE_URL:-}"
[ -n "$DSN" ] || refuse "HOME_LANE_DATABASE_URL is empty."

# --- 3. target host/port (credentials stripped) -----------------------------------------
rest="${DSN#*://}"
rest="${rest%%[/?]*}"
hostport="${rest##*@}"
case "$hostport" in
  \[*\]*) host="${hostport%%\]*}"; host="${host#\[}"; port="${hostport##*\]}"; port="${port#:}" ;;
  *:*)    host="${hostport%%:*}"; port="${hostport##*:}" ;;
  *)      host="$hostport"; port="" ;;
esac
if [ "${HOME_LANE_ALLOW_LOCAL:-0}" != "1" ]; then
  case "$host" in
    ""|localhost|127.*|::1|0.0.0.0) refuse "target host '${host:-<unix socket>}' is local; set HOME_LANE_ALLOW_LOCAL=1 to override." ;;
  esac
  [ "$port" != "15432" ] || refuse "target port 15432 is the local SSH tunnel; set HOME_LANE_ALLOW_LOCAL=1 to override."
fi
echo "home lane target: host=$host port=${port:-5432} (credentials not shown)"

# --- 4. roster --------------------------------------------------------------------------
ids_of() { [ -f "$1" ] && sed 's/#.*//' "$1" | tr -d ' \t\r' | grep -v '^$' | sort -u; }
ROSTER="$(comm -23 <(ids_of "$ROSTER_FILE") <(ids_of "$DNS_FILE"))"
count=0; [ -n "$ROSTER" ] && count="$(printf '%s\n' "$ROSTER" | wc -l | tr -d ' ')"
echo "home lane roster: $count dealer(s)"
[ -z "$ROSTER" ] || printf '  %s\n' $ROSTER

if [ "$DRY_RUN" = "1" ]; then
  echo "dry run: nothing scanned"
  exit 0
fi
[ "$count" -gt 0 ] || { echo "home lane roster is empty; nothing to scan"; exit 0; }

# Active-row count at the target (best effort; a failure is a warning, not a stop).
if command -v psql >/dev/null 2>&1; then
  rows="$(psql "$DSN" -Atc "SELECT COUNT(*) FROM cars WHERE COALESCE(listing_active,1)=1" 2>/dev/null || true)"
  echo "active rows at target: ${rows:-unknown}"
fi

# --- 5. environment ---------------------------------------------------------------------
# Unset variables are refilled from .env (load_project_dotenv override=False), so the
# egress tag, proxy and browser switch are exported as EMPTY strings, not unset.
export PROJECT_DOTENV_DISABLE=1
export INVENTORY_DATABASE_URL="$DSN" DATABASE_URL="$DSN"
export SCANNER_EGRESS_TAG="" SCANNER_HTTP_PROXY="" SCANNER_ALLOW_BROWSER=""
export SCAN_FLEET=1 SCAN_LANE=home SCAN_POST_STEPS=0
export SCAN_SHARDS="${HOME_LANE_SHARDS:-2}"
# runner.py launches Playwright wherever it is installed; discovery stays off.
export SCAN_PIPELINE_ARGS="${SCAN_PIPELINE_ARGS:---no-discover}"
unset SCAN_DEALERS SCAN_FLEET_WINDOW_UTC HOME_LANE_DATABASE_URL
export PYTHONPATH="$REPO_ROOT"

PYTHON="$REPO_ROOT/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

cd "$REPO_ROOT" || refuse "cannot cd to $REPO_ROOT"
LOG_DIR="$REPO_ROOT/workspace/scanlogs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/home_lane_$(date '+%Y%m%d').log"
echo "logging to $LOG_FILE"
"$PYTHON" -m backend.scripts.fleet_scan >>"$LOG_FILE" 2>&1
rc=$?
echo "fleet_scan exit $rc" | tee -a "$LOG_FILE"
exit "$rc"
