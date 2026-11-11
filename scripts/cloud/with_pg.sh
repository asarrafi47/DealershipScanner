#!/usr/bin/env bash
# Run a command against a throwaway Postgres 18 cluster: the pg test tier, in a cloud session.
#
#   scripts/cloud/with_pg.sh -- python -m pytest backend/tests -m pg -q -p no:cacheprovider --timeout=300 -rs
#
# scripts/pg_test_up.sh starts that cluster, but initdb and pg_ctl refuse to run as root and a
# cloud session is root. As root this wrapper runs pg_test_up.sh as the postgres OS user with a
# small holder command, runs your command as you (so the clone and the venv stay readable and
# writable), and then ends the holder, which makes pg_test_up.sh stop and delete the cluster by
# itself. As any other user it calls pg_test_up.sh as is.
#
# The command gets TEST_PG_ADMIN_DSN (a unix-socket DSN) and REQUIRE_PG=1, as the CI pg job
# sets them. pg_test_up.sh keeps its own refusals: nothing here can reach a real database.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
UP="$HERE/../pg_test_up.sh"

die() { printf 'with_pg.sh: %s\n' "$*" >&2; exit 1; }
usage() { printf 'usage: %s -- <command> [args...]\n' "$0" >&2; exit 2; }

[ "${1:-}" = "--" ] || usage
shift
[ "$#" -gt 0 ] || usage

# The Debian paths of the packages scripts/cloud/setup.sh installs. Where they do not exist
# (the Mac), pg_test_up.sh keeps its Homebrew defaults.
if [ -z "${PG_TEST_BIN:-}" ] && [ -x /usr/lib/postgresql/18/bin/initdb ]; then
  export PG_TEST_BIN=/usr/lib/postgresql/18/bin
fi
if [ -z "${PG_TEST_EXT:-}" ] && [ -f /usr/share/postgresql/18/extension/vector.control ]; then
  export PG_TEST_EXT=/usr/share/postgresql/18/extension
fi

if [ "$(id -u)" -ne 0 ]; then
  exec bash "$UP" -- "$@"
fi

command -v runuser >/dev/null 2>&1 || die "runuser not found (util-linux)"
id postgres >/dev/null 2>&1 || die "no postgres OS user: install postgresql-18 first (scripts/cloud/setup.sh)"

# The postgres user must be able to read the script, so it runs from a copy in a shared dir.
STAGE="$(mktemp -d /tmp/ds-pg.XXXXXX)"
chmod 755 "$STAGE"
install -m 0755 "$UP" "$STAGE/pg_test_up.sh"
LOG="$STAGE/cluster.log"
STOP="$STAGE/stop"
: >"$LOG"

pid=""
cmd_pid=""
# Runs from the trap below.
# shellcheck disable=SC2329
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [ -n "$cmd_pid" ] && kill -0 "$cmd_pid" 2>/dev/null; then kill -TERM "$cmd_pid" 2>/dev/null || true; fi
  if [ -n "$pid" ]; then
    : >"$STOP" 2>/dev/null || true  # ends the holder; pg_test_up.sh then stops and deletes the cluster
    for _ in $(seq 1 60); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then  # still up after 30 s: take down the whole process group
      kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
      sleep 1
      kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$STAGE"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# pg_test_up.sh -- <holder>: it exports TEST_PG_ADMIN_DSN to the holder, which prints it and then
# waits for the stop file. (Without `--` pg_test_up.sh ends in a `sleep 3600` loop, and bash would
# defer a SIGTERM until that sleep finished: a kill cannot be how this ends.)
# shellcheck disable=SC2016
holder='echo "DSN=$TEST_PG_ADMIN_DSN"; while [ ! -e "$1" ]; do sleep 0.5; done'
launch=(runuser -u postgres -- env HOME=/var/lib/postgresql bash "$STAGE/pg_test_up.sh" -- bash -c "$holder" holder "$STOP")
# Its own session and process group, so the fallback above can reach everything it started.
if command -v setsid >/dev/null 2>&1; then launch=(setsid "${launch[@]}"); fi
"${launch[@]}" >"$LOG" 2>&1 &
pid=$!

dsn=""
for _ in $(seq 1 240); do
  # sed reads the file itself and quits after the first match: no pipe whose reader could exit early.
  dsn="$(sed -n '/^DSN=/{s/^DSN=//p;q;}' "$LOG")"
  [ -n "$dsn" ] && break
  if ! kill -0 "$pid" 2>/dev/null; then
    cat "$LOG" >&2
    die "pg_test_up.sh exited before the cluster was up"
  fi
  sleep 0.5
done
if [ -z "$dsn" ]; then
  cat "$LOG" >&2
  die "timed out waiting for the throwaway cluster"
fi
# The socket lives in the cluster's directory (private to postgres by default); open that one
# directory so a command run as another user can connect. The data directory below stays 0700.
cluster_dir="$(printf '%s' "$dsn" | sed -n 's/.*host=\([^ ]*\).*/\1/p')"
case "$cluster_dir" in /tmp/pgtu.*) chmod 755 "$cluster_dir" 2>/dev/null || true ;; esac

# The command runs as a background job that this shell `wait`s for: bash runs a trap while it is in
# `wait`, but defers it until a foreground command ends, so a SIGTERM to this wrapper would
# otherwise leave the cluster up until the command finished. <&0 keeps the command's stdin.
TEST_PG_ADMIN_DSN="$dsn" REQUIRE_PG=1 "$@" <&0 &
cmd_pid=$!
set +e
wait "$cmd_pid"
rc=$?
set -e
cmd_pid=""
exit "$rc"
