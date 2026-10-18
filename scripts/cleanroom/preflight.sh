#!/usr/bin/env bash
# preflight.sh: read-only checks before a clean-room re-run into a scratch DB (N1.3).
# Parameterized copy of workspace/cleanroom_2026_10/preflight.sh, which passes only
# for cars_cleanroom (:19, :43-50). Run it in the SAME shell where run.env.template
# is sourced:
#
#   bash scripts/cleanroom/preflight.sh --db cars_cr_x --art workspace/cleanroom_runs/cars_cr_x \
#        --roster <file> --sha <expected git HEAD> [--python .venv/bin/python] [--egress] [--stamp]
#
# Refuses (prints FAIL, exits 1, never connects) when the write target is cars or
# cars_cleanroom or not a cars_cr_* / cars_rehearsal name; when the env DSN is not
# that DB on host /tmp port 5432 (port 15432, the old SSH tunnel, is named explicitly);
# when --art is not workspace/cleanroom_runs/<db> (scripts/cleanroom/check_run_root.sh:
# an ancestor of workspace/, the frozen workspace/cleanroom_2026_10 and the real
# dealer_logs / recipes dirs are always refused; CR_ALLOW_ART_OUTSIDE_RUNS=1 lets tests
# use a dir outside every workspace/ and is printed); when RECIPES_CACHE_DIR,
# DEALER_LOGS_ROOT, SCAN_OUT_ROOT, SCANNER_LOCK_PATH or the manifests sit outside the
# run root; or when `git rev-parse HEAD` is not --sha.
# Then checks the DB, the app's view of it, the roster, paid keys and auto-heal.
# Prints PREFLIGHT OK or PREFLIGHT FAIL. Writes nothing (unless --stamp: one JSON
# file under <art>/runs/ after an OK). --egress makes one HTTPS request to
# api.ipify.org to print the public IP.
set -uo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
DB= ART= ROSTER= SHA= PY= EGRESS= STAMP=
while [ $# -gt 0 ]; do
  case "$1" in
    --db) DB=${2:-}; shift 2;;
    --art) ART=${2:-}; shift 2;;
    --roster) ROSTER=${2:-}; shift 2;;
    --sha) SHA=${2:-}; shift 2;;
    --python) PY=${2:-}; shift 2;;
    --egress) EGRESS=1; shift;;
    --stamp) STAMP=1; shift;;
    *) echo "  FAIL unknown argument $1"; echo "PREFLIGHT FAIL"; exit 1;;
  esac
done
fail=0; ok() { echo "  ok   $*"; }; bad() { echo "  FAIL $*"; fail=1; }
abs() { case "$1" in /*) printf '%s' "$1";; '') ;; *) printf '%s' "$REPO/$1";; esac; }
real() { python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1"; }
under() {  # under <path> <root>: true when path resolves inside root
  [ -n "$1" ] && [ -n "$2" ] || return 1
  local p r; p=$(real "$1"); r=$(real "$2")
  case "$p" in "$r"/*) return 0;; *) return 1;; esac
}
ART=$(abs "$ART"); ROSTER=$(abs "$ROSTER")

echo "== refusals (no connection is made before these pass)"
case "$DB" in
  '') bad "--db is required";;
  cars|cars_cleanroom) bad "write target $DB is the live DB or the frozen baseline: refused";;
  *) if [[ "$DB" =~ ^(cars_cr_[a-z0-9_]+|cars_rehearsal)$ ]]; then ok "write target $DB is a scratch name"
     else bad "write target $DB is not a scratch name (cars_cr_<name> or cars_rehearsal)"; fi;;
esac
[ -n "$ART" ] || bad "--art is required"
[ -n "$ROSTER" ] && [ -f "$ROSTER" ] && ok "roster $ROSTER exists" || bad "--roster missing or not a file"
[ -n "$SHA" ] || bad "--sha is required (the expected git HEAD: phase HEAD or RC_SHA)"
if [ -n "$ART" ]; then
  # run root must be workspace/cleanroom_runs/<db> (check_run_root.sh; tests opt out
  # with CR_ALLOW_ART_OUTSIDE_RUNS=1, reported as a "note" line)
  if rr=$(bash "$REPO/scripts/cleanroom/check_run_root.sh" --art "$ART" ${DB:+--db "$DB"}); then echo "  $rr"
  else bad "${rr#REFUSED }"; fi
fi
for v in INVENTORY_DATABASE_URL DATABASE_URL; do
  verdict=$(python3 - "${!v:-}" "$DB" <<'PY'
import sys
from urllib.parse import urlsplit, parse_qs
url, want = sys.argv[1], sys.argv[2]
if not url:
    print("unset"); sys.exit()
u = urlsplit(url)
q = {k: v[-1] for k, v in parse_qs(u.query).items()}
db = u.path.lstrip("/")
host = q.get("host") or (u.hostname or "")
port = q.get("port") or (str(u.port) if u.port else "5432")
if u.scheme not in ("postgresql", "postgres"):
    print(f"scheme {u.scheme!r} is not postgresql")
elif db in ("cars", "cars_cleanroom"):
    print(f"points at {db}: refused")
elif db != want:
    print(f"points at {db!r}, not --db {want!r}")
elif host != "/tmp" or u.hostname:
    print(f"host {host or u.hostname!r} is not /tmp")
elif port == "15432":
    print("port 15432 (SSH tunnel) refused")
elif port != "5432":
    print(f"port {port} is not 5432")
else:
    print("ok")
PY
)
  [ "$verdict" = "ok" ] && ok "$v -> $DB on /tmp:5432" || bad "$v: $verdict (source run.env.template)"
done
for v in PGHOST PGHOSTADDR PGPORT PGDATABASE PGSERVICE; do
  [ -z "${!v:-}" ] || bad "$v is set (${!v}): it would redirect libpq"
done
for v in RECIPES_CACHE_DIR DEALER_LOGS_ROOT SCAN_OUT_ROOT SCANNER_LOCK_PATH DEALERS_MANIFEST_PATH; do
  under "${!v:-}" "$ART" && ok "$v under the run root" || bad "$v='${!v:-}' is not under $ART"
done
man=$(echo "${SCAN_PIPELINE_ARGS:-}" | sed -n 's/.*--manifest[ =]\([^ ]*\).*/\1/p')
under "$man" "$ART" && ok "pipeline --manifest under the run root" || bad "pipeline --manifest '$man' is not under $ART"
head=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo "?")
if [ -n "$SHA" ] && [ "${#SHA}" -ge 7 ] && [ "${head#"$SHA"}" != "$head" ]; then ok "git HEAD $head matches --sha"
else bad "git HEAD $head does not match --sha ${SHA:-<none>}"; fi
if [ $fail != 0 ]; then echo "PREFLIGHT FAIL"; exit 1; fi

cd "$REPO" || { echo "PREFLIGHT FAIL"; exit 1; }
PY=${PY:-$REPO/.venv/bin/python}
[ -x "$PY" ] || { bad "python $PY not found (pass --python)"; echo "PREFLIGHT FAIL"; exit 1; }

echo "== power"
pmset -g batt 2>/dev/null | grep -q "AC Power" && ok "on AC power" || echo "  WARN on battery (AC recommended for runs over about 2 h)"

echo "== no other scanner"
pgrep -fl 'backend[./]scripts[./](fleet_scan|dealer_pipeline)|backend/scanner/scanner\.py' >/dev/null \
  && bad "a scanner/pipeline process is running" || ok "no scanner/pipeline process"
[ -e "$REPO/workspace/scanner.lock" ] && bad "workspace/scanner.lock exists" || ok "no workspace/scanner.lock"
[ -e "${SCANNER_LOCK_PATH:-/nonexistent}" ] && bad "run-root lock $SCANNER_LOCK_PATH exists" || ok "no run-root lock"

echo "== env"
[ -z "${PGOPTIONS:-}" ] && ok "PGOPTIONS unset" || bad "PGOPTIONS is set ($PGOPTIONS): scan writes would fail"
[ -z "${SCAN_FLEET:-}" ] && ok "SCAN_FLEET unset" || bad "SCAN_FLEET is set"
roster_ids=$(grep -v '^[[:space:]]*\(#\|$\)' "$ROSTER" | tr -d ' \t\r' | sort -u)
env_ids=$(echo "${SCAN_DEALERS:-}" | tr , '\n' | grep . | sort -u)
nr=$(echo "$roster_ids" | grep -c .); ne=$(echo "$env_ids" | grep -c .)
[ "$roster_ids" = "$env_ids" ] && [ "$nr" -gt 0 ] && ok "SCAN_DEALERS = roster ($nr ids)" || bad "SCAN_DEALERS ($ne ids) differs from the roster ($nr ids)"
for f in "${DEALERS_MANIFEST_PATH:-}" "$man"; do
  if [ -f "$f" ]; then
    mids=$(python3 -c 'import json,sys; print("\n".join(sorted({r["dealer_id"] for r in json.load(open(sys.argv[1]))})))' "$f")
    [ "$mids" = "$roster_ids" ] && ok "manifest $(basename "$f") = roster" || bad "manifest $f differs from the roster (scripts/cleanroom/make_manifest.sh)"
  else
    bad "manifest $f missing (scripts/cleanroom/make_manifest.sh)"
  fi
done
s=${SCAN_SHARDS:-}; { [ -n "$s" ] && [ "$s" -le 8 ]; } && ok "SCAN_SHARDS=$s" || bad "SCAN_SHARDS unset or > 8"
[ "${SCAN_KEEP_DAYS:-0}" -ge 3650 ] 2>/dev/null && ok "SCAN_KEEP_DAYS=$SCAN_KEEP_DAYS" || bad "SCAN_KEEP_DAYS must be large (never 0)"
env | grep -iqE '^(https?|all)_proxy=|^SCANNER_HTTP_PROXY=' && bad "a proxy variable is set" || ok "no proxy variables"
[ -z "${SCANNER_EGRESS_TAG:-}" ] && ok "no egress tag" || bad "SCANNER_EGRESS_TAG is set"
[ -z "${SCANNER_ALLOW_BROWSER:-}" ] && ok "browser not allowed" || bad "SCANNER_ALLOW_BROWSER is set"
echo "${SCAN_PIPELINE_ARGS:-}" | grep -q -- '--no-discover' && ok "--no-discover" || bad "--no-discover missing from SCAN_PIPELINE_ARGS"

echo "== paid steps off (after the app's own dotenv load)"
"$PY" -c '
import os, sys
from backend.utils.project_env import load_project_dotenv as l; l()
keys = ("ANTHROPIC_API_KEY","OPENAI_API_KEY","GOOGLE_MAPS_API_KEY","GOOGLE_CSE_API_KEY","EIA_API_KEY","RESEND_API_KEY")
live = [k for k in keys if os.environ.get(k)]
sys.exit("live paid keys: " + ",".join(live) if live else 0)' && ok "paid keys blank after dotenv" || bad "a paid key is live"
"$PY" -c 'from backend.scanner.post_scan.auto_heal import auto_heal_enabled as f; import sys; sys.exit(1 if f() else 0)' \
  && ok "auto-heal off" || bad "auto-heal is on"

echo "== database"
RO="-c default_transaction_read_only=on -c statement_timeout=60000"
e=$(PGOPTIONS="$RO" psql -X -h /tmp -p 5432 -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='$DB'" 2>/dev/null)
if [ "$e" = "1" ]; then
  ok "$DB exists"
  CR_WANT_DB=$DB "$PY" -c '
import os, sys
from backend.db.inventory_db import get_conn
c = get_conn(); k = c.cursor(); k.execute("select current_database()"); d = k.fetchone()[0]
sys.exit(0 if d == os.environ["CR_WANT_DB"] else "app resolves to " + d)' && ok "app DSN resolves to $DB" || bad "app DSN does not resolve to $DB"
  "$PY" -c 'from backend.db.inventory_db import get_conn; from backend.db.schema_version import ensure_schema_current as e; assert e(get_conn())' >/dev/null 2>&1 \
    && ok "schema check strict passes" || bad "schema check fails on $DB (run migrate --apply)"
  c=$(PGOPTIONS="$RO" psql -X -h /tmp -p 5432 -d "$DB" -Atc "SELECT count(*) FROM cars" 2>/dev/null)
  [ "$c" = "0" ] && ok "cars is empty" || echo "  note cars has ${c:-?} rows (template copy or second pass?)"
else
  bad "$DB does not exist (run scripts/cleanroom/build_db.sh --db $DB)"
fi

if [ -n "$EGRESS" ]; then
  echo "== egress"
  ip=$(curl -fsS -m 10 https://api.ipify.org 2>/dev/null || echo unknown); echo "  public IP: $ip (confirm it is the home IP)"
fi
df -g "$REPO" 2>/dev/null | awk 'NR==2{print "  disk free: "$4" GB"}'

if [ $fail = 0 ]; then
  if [ -n "$STAMP" ]; then
    mkdir -p "$SCAN_OUT_ROOT"
    printf '{"db": "%s", "git_head": "%s", "preflight_at": "%s", "art": "%s"}\n' \
      "$DB" "$head" "$(date -u +%FT%TZ)" "$ART" > "$SCAN_OUT_ROOT/preflight_$(date -u +%Y%m%dT%H%M%SZ).json"
  fi
  echo "PREFLIGHT OK ($DB @ $head)"
else
  echo "PREFLIGHT FAIL"; exit 1
fi
