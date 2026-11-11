#!/usr/bin/env bash
# Run a command against a throwaway Postgres cluster (18 when installed): the pg test tier, in a
# cloud session.
#
#   scripts/cloud/with_pg.sh -- python -m pytest backend/tests -m pg -q -p no:cacheprovider --timeout=300 -rs
#
# scripts/pg_test_up.sh starts that cluster, but initdb and pg_ctl refuse to run as root and a
# cloud session is root. As root this wrapper runs pg_test_up.sh as the postgres OS user with a
# small holder command, runs your command as you (so the clone and the venv stay readable and
# writable), and then ends the holder, which makes pg_test_up.sh stop and delete the cluster by
# itself. As any other user it calls pg_test_up.sh as is.
#
# The command gets TEST_PG_ADMIN_DSN and SCHEMA_PARITY_PG_DSN (the same unix-socket DSN) and
# REQUIRE_PG=1, as the CI pg job sets them. pg_test_up.sh keeps its own refusals: nothing here
# can reach a real database.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
UP="$HERE/../pg_test_up.sh"

die() { printf 'with_pg.sh: %s\n' "$*" >&2; exit 1; }
usage() { printf 'usage: %s -- <command> [args...]\n' "$0" >&2; exit 2; }

[ "${1:-}" = "--" ] || usage
shift
[ "$#" -gt 0 ] || usage

# The Debian paths of the packages scripts/cloud/setup.sh installs: CI's major (18) when it is
# there, else the first of setup.sh's fallback majors (backend/tests/pg_harness.py needs 15 or
# newer). Where none exists (the Mac), pg_test_up.sh keeps its Homebrew defaults. An explicit
# PG_TEST_BIN and PG_TEST_EXT (the SessionStart hook exports them) win.
if [ -z "${PG_TEST_BIN:-}" ] || [ -z "${PG_TEST_EXT:-}" ]; then
  major=""
  for m in 18 17 16 15; do
    if [ -x "/usr/lib/postgresql/$m/bin/initdb" ] && [ -f "/usr/share/postgresql/$m/extension/vector.control" ]; then
      major="$m"
      break
    fi
  done
  if [ -n "$major" ]; then
    : "${PG_TEST_BIN:=/usr/lib/postgresql/$major/bin}"
    : "${PG_TEST_EXT:=/usr/share/postgresql/$major/extension}"
    export PG_TEST_BIN PG_TEST_EXT
    if [ "$major" != 18 ]; then
      printf 'with_pg.sh: using PostgreSQL %s; CI and production run 18, so this is not parity (docs/CLOUD.md)\n' "$major" >&2
    fi
  elif [ "$(uname -s)" = Linux ]; then
    # Without this, pg_test_up.sh would complain about its Homebrew default, which a VM never has.
    die "no PostgreSQL with pgvector under /usr/lib/postgresql; run scripts/cloud/setup.sh (docs/CLOUD.md)"
  fi
fi

if [ "$(id -u)" -ne 0 ]; then
  exec bash "$UP" -- "$@"
fi

command -v runuser >/dev/null 2>&1 || die "runuser not found (util-linux)"
id postgres >/dev/null 2>&1 || die "no postgres OS user: install PostgreSQL first (scripts/cloud/setup.sh)"

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
# The postgres user needs these two spelled out: nothing else of this environment is passed on.
pg_env=(HOME=/var/lib/postgresql)
if [ -n "${PG_TEST_BIN:-}" ]; then pg_env+=("PG_TEST_BIN=$PG_TEST_BIN"); fi
if [ -n "${PG_TEST_EXT:-}" ]; then pg_env+=("PG_TEST_EXT=$PG_TEST_EXT"); fi
launch=(runuser -u postgres -- env "${pg_env[@]}" bash "$STAGE/pg_test_up.sh" -- bash -c "$holder" holder "$STOP")
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
# The socket lives in the cluster's directory, which only the postgres user may enter. The command
# below runs as root (this branch is root only), and root may connect, so nothing is opened up: the
# cluster has no password (pg_test_up.sh uses trust), and that directory is what keeps other users out.

# The command runs as a background job that this shell `wait`s for: bash runs a trap while it is in
# `wait`, but defers it until a foreground command ends, so a SIGTERM to this wrapper would
# otherwise leave the cluster up until the command finished. <&0 keeps the command's stdin.
TEST_PG_ADMIN_DSN="$dsn" SCHEMA_PARITY_PG_DSN="$dsn" REQUIRE_PG=1 "$@" <&0 &
cmd_pid=$!
set +e
wait "$cmd_pid"
rc=$?
set -e
cmd_pid=""
exit "$rc"
