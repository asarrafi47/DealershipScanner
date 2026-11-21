#!/usr/bin/env bash
# N37.1: ephemeral Postgres 18 cluster for the pg test tier (D-PG12).
#
#   scripts/pg_test_up.sh -- pytest backend/tests/test_pg_harness.py -q
#       starts the cluster, runs the command with TEST_PG_ADMIN_DSN and REQUIRE_PG=1 set,
#       tears the cluster down when the command exits, and exits with its status.
#   scripts/pg_test_up.sh
#       starts the cluster, prints the DSN, waits until Ctrl-C, then tears it down.
#
# The cluster is unix-socket only (no TCP port, nothing else on the network can reach it),
# lives in a fresh temp dir, runs with fsync off and has pgvector. It never touches the
# server that holds `cars`: an existing TEST_PG_ADMIN_DSN / PGHOST / PGPORT / PGDATABASE
# that points at `cars`, `cars_cleanroom`, a non-local host or the 15432 tunnel is refused.
set -euo pipefail

PGBIN="${PG_TEST_BIN:-/opt/homebrew/opt/postgresql@18/bin}"
EXTDIR="${PG_TEST_EXT:-/opt/homebrew/share/postgresql@18/extension}"

die() { echo "pg_test_up: $*" >&2; exit 1; }

# --- refuse anything that points at real data ------------------------------
refuse_if_protected() {
  local what="$1" val="${2:-}"
  [ -z "$val" ] && return 0
  case "$what" in
    dsn)
      case "$val" in
        *dbname=cars*|*/cars|*/cars\?*|*cars_cleanroom*|*:15432*|*port=15432*|*rlwy.net*|*railway*)
          die "refusing: TEST_PG_ADMIN_DSN points at a protected database or the tunnel ($what); this script starts its own cluster" ;;
      esac ;;
    db)  case "$val" in cars|cars_cleanroom|railway) die "refusing: PGDATABASE=$val" ;; esac ;;
    port) [ "$val" = "15432" ] && die "refusing: PGPORT=15432 is the tunnel to the inventory server" ;;
  esac
  return 0
}
refuse_if_protected dsn "${TEST_PG_ADMIN_DSN:-}"
refuse_if_protected db "${PGDATABASE:-}"
refuse_if_protected port "${PGPORT:-}"

# --- prerequisites -----------------------------------------------------------
for b in initdb pg_ctl pg_isready psql; do
  [ -x "$PGBIN/$b" ] || die "$PGBIN/$b not found; brew install postgresql@18 (or set PG_TEST_BIN)"
done
[ -f "$EXTDIR/vector.control" ] || die "pgvector for postgresql@18 not found under $EXTDIR; brew install pgvector (or set PG_TEST_EXT)"

# Short path: unix socket paths are limited to ~100 bytes and $TMPDIR is long on macOS.
ROOT="$(mktemp -d /tmp/pgtu.XXXXXX)"
DATA="$ROOT/data"
cleanup() {
  "$PGBIN/pg_ctl" -D "$DATA" -m immediate stop >/dev/null 2>&1 || true
  case "$ROOT" in /tmp/pgtu.*) rm -rf "$ROOT" ;; esac
}
trap cleanup EXIT
trap 'exit 130' INT TERM

"$PGBIN/initdb" -D "$DATA" --no-sync -A trust -U postgres -E UTF8 >/dev/null
"$PGBIN/pg_ctl" -D "$DATA" -w -s -l "$ROOT/log" \
  -o "-c listen_addresses='' -c unix_socket_directories='$ROOT' -c port=5439 -c fsync=off -c synchronous_commit=off -c full_page_writes=off -c max_connections=200" \
  start || { cat "$ROOT/log" >&2; die "cluster failed to start"; }
"$PGBIN/pg_isready" -h "$ROOT" -p 5439 -q || die "cluster not ready"

DSN="host=$ROOT port=5439 dbname=postgres user=postgres"
"$PGBIN/psql" "$DSN" -qAtc "select 1" >/dev/null || die "cannot connect to the ephemeral cluster"

export TEST_PG_ADMIN_DSN="$DSN"
export REQUIRE_PG=1

if [ "${1:-}" = "--" ]; then
  shift
  [ "$#" -gt 0 ] || die "no command after --"
  set +e
  "$@"
  rc=$?
  set -e
  exit "$rc"
fi

echo "TEST_PG_ADMIN_DSN=\"$DSN\""
echo "pg_test_up: cluster running; Ctrl-C to stop and delete it" >&2
while true; do sleep 3600; done
