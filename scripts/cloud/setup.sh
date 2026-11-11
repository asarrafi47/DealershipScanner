#!/usr/bin/env bash
# Cloud VM setup for DealershipScanner (Claude Code on the web). Owner guide: docs/CLOUD.md.
#
# Makes a fresh Ubuntu cloud VM match this repo's CI, and keeps it that way: the repo's
# SessionStart hook (.claude/settings.json) runs this on every session start, and a VM that
# is already set up finishes in about a second. Steps (--skip / --only take these names):
#
#   git       core.hooksPath=scripts/git-hooks, so the pre-push VERSION rule runs in a fresh
#             clone, and a local origin/main, which that hook needs for a new branch
#   python    a Python 3.12 venv outside the clone, CPU-only torch FIRST, then
#             requirements.txt + requirements-test.txt (the CI order, D-TC5), pinned ruff
#   pgeocode  the US postal table; backend/db/geo.py loads it at import, and a cold cache
#             downloads from hosts the default cloud network policy blocks
#   node      check only: Node 20 at /opt/node20 (CI's version; the VM puts 22 first on PATH)
#   pg        Postgres 18 + pgvector from the PGDG apt repository, for the pg test tier
#   env       .venv link, and PATH / PG_TEST_BIN / PG_TEST_EXT into $CLAUDE_ENV_FILE
#
# It never connects to a database and never reads or writes a secret. The only cluster this
# directory starts is the throwaway one in scripts/pg_test_up.sh (via with_pg.sh).
#
# Written for bash 3.2 and up: --check and --help also run on the owner's Mac.
set -euo pipefail

# --- constants. backend/tests/test_cloud_setup.py pins the ones that must match CI. ---------
PYTHON_MINOR="3.12"
TORCH_INDEX="https://download.pytorch.org/whl/cpu"
RUFF_PIN="ruff==0.15.22"
PG_MAJOR="18"
PG_BIN="/usr/lib/postgresql/${PG_MAJOR}/bin"
PG_EXT="/usr/share/postgresql/${PG_MAJOR}/extension"
PGDG_URL="https://apt.postgresql.org/pub/repos/apt"
PGDG_KEY_URL="https://www.postgresql.org/media/keys/ACCC4CF8.asc"
PGDG_KEY_FPR="B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8"
PGDG_KEY="/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc"
PGDG_LIST="/etc/apt/sources.list.d/pgdg.list"
NODE_DIR="/opt/node20/bin"
# Tried in this order, one at a time: pgeocode itself falls back only on HTTP errors, and a
# blocked host often fails as a connection error instead.
PGEOCODE_URLS="https://download.geonames.org/export/zip/{country}.zip https://raw.githubusercontent.com/symerio/postal-codes-data/master/data/geonames/{country}.txt"
STEPS="git python pgeocode node pg env"

TMP_ROOT="${TMPDIR:-/tmp}"
TMP_ROOT="${TMP_ROOT%/}"
VENV="${DS_CLOUD_VENV:-/opt/ds-venv}"
LOG_DIR="${DS_CLOUD_LOG_DIR:-$TMP_ROOT/ds-cloud-setup}"
LOCK_FILE="$TMP_ROOT/ds-cloud-setup.lock"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

CHECK_ONLY=0
HOOK=0
FORCE=0
SKIP=""
ONLY=""

# Per-run state. REPORT collects "<sort key>|<level>|<label>|<detail>" lines; it is a file
# because the pg step runs in a background subshell next to the python step.
REPORT=""
STEP_LOG=/dev/null
CUR_IDX=0
SEQ=0
DETAIL=""
FIXERR=""
ENSURED=0
REQS_OK=0

usage() {
  cat <<'EOF'
Usage: scripts/cloud/setup.sh [options]

  (no options)  install or repair whatever is missing (Linux only; root for apt and /opt)
  --check       only report what is missing; change nothing; exit 1 if a required item is
  --hook        SessionStart mode: acts only when CLAUDE_CODE_REMOTE=true, prints a short
                summary for the session, always exits 0
  --force       redo the pip install even when the requirements digest matches
  --skip A,B    leave these steps out        --only A,B    run only these steps
  -h, --help    this text

Steps: git python pgeocode node pg env
Environment: DS_CLOUD_VENV (default /opt/ds-venv), DS_CLOUD_LOG_DIR (default /tmp/ds-cloud-setup)
Guide: docs/CLOUD.md
EOF
}

die() { printf 'setup.sh: %s\n' "$1" >&2; exit "${2:-1}"; }  # die <message> [exit code]
say() { if [ "$HOOK" = 0 ]; then printf '%s\n' "$*"; fi; }
have() { command -v "$1" >/dev/null 2>&1; }

want() {  # is step $1 enabled?
  case ",$SKIP," in *",$1,"*) return 1 ;; esac
  [ -z "$ONLY" ] && return 0
  case ",$ONLY," in *",$1,"*) return 0 ;; esac
  return 1
}

check_step_names() {
  local list="$1" s
  local IFS=,
  for s in $list; do
    case " $STEPS " in *" $s "*) ;; *) die "unknown step '$s' (steps: $STEPS)" 2 ;; esac
  done
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --check) CHECK_ONLY=1 ;;
      --hook) HOOK=1 ;;
      --force) FORCE=1 ;;
      --skip) shift; SKIP="${1:-}" ;;
      --skip=*) SKIP="${1#--skip=}" ;;
      --only) shift; ONLY="${1:-}" ;;
      --only=*) ONLY="${1#--only=}" ;;
      -h|--help) usage; exit 0 ;;
      *) usage >&2; die "unknown option: $1" 2 ;;
    esac
    shift
  done
  check_step_names "$SKIP"
  check_step_names "$ONLY"
}

# --- reporting ----------------------------------------------------------------------------
report() {  # report <ok|fixed|missing|failed|optional> <label> <detail>
  printf '%02d%03d|%s|%s|%s\n' "$CUR_IDX" "$SEQ" "$1" "$2" "$3" >>"$REPORT"
  SEQ=$((SEQ + 1))
}

# ensure <required|optional> <label> <check_fn> <fix_fn>
#   check_fn sets DETAIL and returns 0 when the item is in place. fix_fn runs only without
#   --check; it sets FIXERR when it fails. ENSURED is 1 afterwards when the item is in place.
#   Always returns 0, so a problem is reported instead of tripping `set -e`.
ensure() {
  local need="$1" label="$2" check="$3" fix="$4" why
  ENSURED=0
  DETAIL=""
  FIXERR=""
  if "$check"; then
    ENSURED=1
    report ok "$label" "$DETAIL"
    return 0
  fi
  why="$DETAIL"
  # --check, or an item this script cannot fix (an empty fix_fn): report the gap and go on.
  if [ "$CHECK_ONLY" = 1 ] || [ -z "$fix" ]; then
    if [ "$need" = required ]; then report missing "$label" "$why"; else report optional "$label" "$why"; fi
    return 0
  fi
  say "  $label: $why; fixing"
  if [ "$STEP_LOG" != /dev/null ]; then printf '=== %s: fix started ===\n' "$label" >>"$STEP_LOG"; fi
  if "$fix"; then
    DETAIL=""
    if "$check"; then
      ENSURED=1
      report fixed "$label" "$DETAIL"
      return 0
    fi
    FIXERR="${FIXERR:-still not in place after the fix: $DETAIL}"
  fi
  FIXERR="${FIXERR:-the fix failed}"
  if [ "$HOOK" = 0 ] && [ "$STEP_LOG" != /dev/null ] && [ -s "$STEP_LOG" ]; then
    say "  last lines logged by that fix ($STEP_LOG):"
    awk -v m="=== $label: fix started ===" '
      $0 == m { n = 0; next }
      { buf[n++] = $0 }
      END { for (i = (n > 12 ? n - 12 : 0); i < n; i++) print "    | " buf[i] }' "$STEP_LOG"
  fi
  if [ "$need" = required ]; then report failed "$label" "$FIXERR"; else report optional "$label" "$FIXERR"; fi
  return 0
}

# needs ...: like ensure, but only when the item before it is in place. --check lists every
# gap regardless, so one missing venv does not hide the rest.
needs() {
  if [ "$CHECK_ONLY" = 1 ] || [ "$ENSURED" = 1 ]; then
    ensure "$@"
  else
    if [ "$1" = required ]; then report failed "$2" "skipped: the step before it failed"; else report optional "$2" "skipped: the step before it failed"; fi
  fi
}

# --- helpers --------------------------------------------------------------------------------
sha256_stdin() {
  if have sha256sum; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi
}

tmo() {  # tmo <seconds> <cmd...>: run with a time limit when `timeout` exists
  local secs="$1"
  shift
  if have timeout; then timeout "$secs" "$@"; else "$@"; fi
}

apt_run() {  # apt_run <apt-get args...>: non-interactive, serialized with this script's other apt calls
  local -a pre=()
  if have flock; then pre=(flock /var/lock/ds-cloud-apt.lock); fi
  env DEBIAN_FRONTEND=noninteractive NEEDRESTART_SUSPEND=1 ${pre[@]+"${pre[@]}"} \
    apt-get -o DPkg::Lock::Timeout=300 -o Dpkg::Options::=--force-confold "$@" >>"$STEP_LOG" 2>&1
}

apt_install() {
  apt_run update -qq || true  # a flaky unrelated repo must not stop the install
  apt_run install -y --no-install-recommends "$@"
}

pip_run() {  # pip inside the venv; output goes to the step log
  "$VENV/bin/python" -m pip --disable-pip-version-check --no-input "$@" >>"$STEP_LOG" 2>&1
}

# --- step: git ----------------------------------------------------------------------------
git_hooks_ok() {
  local v hook
  v="$(git -C "$ROOT" config --get core.hooksPath 2>/dev/null || true)"
  case "$v" in
    scripts/git-hooks) hook="$ROOT/$v" ;;
    */scripts/git-hooks) hook="$v" ;;
    *) DETAIL="core.hooksPath is ${v:-unset}; the pre-push VERSION rule will not run"; return 1 ;;
  esac
  if [ ! -x "$hook/pre-push" ]; then
    DETAIL="$hook/pre-push is missing or not executable (git skips a hook that is not)"
    return 1
  fi
  DETAIL="core.hooksPath=$v"
}

git_hooks_fix() {
  git -C "$ROOT" config core.hooksPath scripts/git-hooks || return 1
  chmod +x "$ROOT/scripts/git-hooks/pre-push" 2>/dev/null || true
}

origin_main_ok() {
  local sha
  if sha="$(git -C "$ROOT" rev-parse --verify -q refs/remotes/origin/main 2>/dev/null)"; then
    DETAIL="origin/main at $(printf '%s' "$sha" | cut -c1-9)"
    return 0
  fi
  DETAIL="no refs/remotes/origin/main; the pre-push hook silently skips the VERSION check for a new branch"
  return 1
}

origin_main_fix() {
  local -a args=(--no-tags)
  git -C "$ROOT" remote get-url origin >/dev/null 2>&1 || { FIXERR="no origin remote to fetch main from"; return 1; }
  if [ "$(git -C "$ROOT" rev-parse --is-shallow-repository 2>/dev/null)" = true ]; then args+=(--depth=50); fi
  tmo 120 git -C "$ROOT" fetch "${args[@]}" origin +refs/heads/main:refs/remotes/origin/main >>"$STEP_LOG" 2>&1 \
    || { FIXERR="git fetch origin main failed or timed out"; return 1; }
}

step_git() {
  if ! have git || ! git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    report optional "git" "$ROOT is not a git work tree (or git is missing); hooks and origin/main skipped"
    return 0
  fi
  ensure required "git hooks" git_hooks_ok git_hooks_fix
  ensure optional "origin/main" origin_main_ok origin_main_fix
}

# --- step: python -------------------------------------------------------------------------
find_python312() {  # print an interpreter whose version is 3.12.x
  local c v
  for c in python3.12 /usr/bin/python3.12 python3 python; do
    have "$c" || continue
    v="$("$c" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)" || continue
    if [ "$v" = "$PYTHON_MINOR" ]; then command -v "$c"; return 0; fi
  done
  if have uv; then
    c="$(uv python find "$PYTHON_MINOR" 2>/dev/null)" || c=""
    if [ -n "$c" ]; then printf '%s\n' "$c"; return 0; fi
  fi
  return 1
}

venv_ok() {
  local v
  if [ ! -x "$VENV/bin/python" ]; then DETAIL="no venv at $VENV (CI runs Python $PYTHON_MINOR)"; return 1; fi
  v="$("$VENV/bin/python" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null)" \
    || { DETAIL="$VENV/bin/python does not run"; return 1; }
  case "$v" in "$PYTHON_MINOR".*) ;; *) DETAIL="$VENV is Python $v, CI runs $PYTHON_MINOR"; return 1 ;; esac
  "$VENV/bin/python" -m pip --version >/dev/null 2>&1 || { DETAIL="$VENV has no working pip"; return 1; }
  DETAIL="$VENV, Python $v"
}

venv_make() {
  local py
  if [ -e "$VENV" ] && [ ! -f "$VENV/pyvenv.cfg" ] && [ -n "$(ls -A "$VENV" 2>/dev/null)" ]; then
    FIXERR="$VENV exists and is not a Python venv; refusing to touch it (set DS_CLOUD_VENV to another path)"
    return 1
  fi
  mkdir -p "$(dirname "$VENV")" 2>/dev/null \
    || { FIXERR="cannot create $(dirname "$VENV"): run as root or set DS_CLOUD_VENV to a writable path"; return 1; }
  if [ -f "$VENV/pyvenv.cfg" ]; then rm -rf "$VENV"; fi  # wrong Python or no pip: rebuild
  if py="$(find_python312)" && "$py" -m venv "$VENV" >>"$STEP_LOG" 2>&1; then return 0; fi
  if [ -f "$VENV/pyvenv.cfg" ]; then rm -rf "$VENV"; fi
  if have uv && uv venv --seed --python "$PYTHON_MINOR" "$VENV" >>"$STEP_LOG" 2>&1; then return 0; fi
  if [ -f "$VENV/pyvenv.cfg" ]; then rm -rf "$VENV"; fi
  if have apt-get && [ "$(id -u)" -eq 0 ] \
    && apt_install "python${PYTHON_MINOR}" "python${PYTHON_MINOR}-venv" \
    && "python${PYTHON_MINOR}" -m venv "$VENV" >>"$STEP_LOG" 2>&1; then
    return 0
  fi
  FIXERR="could not create a Python $PYTHON_MINOR venv (tried python${PYTHON_MINOR} -m venv, uv, apt); see $STEP_LOG"
  return 1
}

torch_ok() {
  local v
  v="$("$VENV/bin/python" -c 'import importlib.metadata as m; print(m.version("torch"))' 2>/dev/null)" \
    || { DETAIL="torch is not installed"; return 1; }
  case "$(uname -m)" in
    x86_64|amd64)
      case "$v" in
        *+cpu) ;;
        *) DETAIL="torch $v is not the CPU build; the default wheel is the multi-GB CUDA stack"; return 1 ;;
      esac ;;
  esac
  DETAIL="torch $v"
}

torch_install() {
  if "$VENV/bin/python" -c 'import importlib.metadata as m; m.version("torch")' >/dev/null 2>&1; then
    pip_run uninstall -y torch || true  # a non-CPU build is in the way
  fi
  if pip_run install --retries 2 --timeout 30 --no-cache-dir torch --index-url "$TORCH_INDEX"; then return 0; fi
  FIXERR="CPU torch install failed. download.pytorch.org and download-r2.pytorch.org must be allowed (docs/CLOUD.md, Network access); see $STEP_LOG"
  return 1
}

reqs_digest() {  # what the pip install depends on: every requirements*.txt plus the torch index
  { cat "$ROOT"/requirements*.txt; printf '%s\n' "$TORCH_INDEX"; } | sha256_stdin
}

reqs_ok() {
  local want have_digest
  if [ ! -f "$ROOT/requirements.txt" ] || [ ! -f "$ROOT/requirements-test.txt" ]; then
    DETAIL="requirements.txt or requirements-test.txt not found under $ROOT"
    return 1
  fi
  # CPU torch comes first, and a requirements install must not have replaced it with the CUDA build.
  if ! torch_ok; then DETAIL="needs CPU torch first ($DETAIL)"; return 1; fi
  want="$(reqs_digest)"
  have_digest="$(cat "$VENV/.ds-requirements.sha256" 2>/dev/null || true)"
  if [ "$FORCE" = 0 ] && [ "$want" = "$have_digest" ]; then
    DETAIL="requirements.txt + requirements-test.txt installed (digest ${want:0:8})"
    return 0
  fi
  if [ "$FORCE" = 1 ]; then DETAIL="--force"; else DETAIL="requirements changed since the last install, or none yet"; fi
  return 1
}

reqs_install() {
  # CI order (D-TC5): CPU torch is already in, so pip skips the CUDA build sentence-transformers would pull.
  if ! torch_ok; then
    FIXERR="refusing to install the requirements without CPU torch (the default torch wheel is multi-GB): $DETAIL"
    return 1
  fi
  if ! pip_run install --retries 3 --timeout 60 --no-cache-dir \
    -r "$ROOT/requirements.txt" -r "$ROOT/requirements-test.txt"; then
    FIXERR="pip install -r requirements.txt -r requirements-test.txt failed; see $STEP_LOG"
    return 1
  fi
  reqs_digest >"$VENV/.ds-requirements.sha256"
  FORCE=0
}

ruff_ok() {
  local v
  v="$("$VENV/bin/ruff" --version 2>/dev/null)" || { DETAIL="ruff is not installed (CI lints with $RUFF_PIN)"; return 1; }
  if [ "$v" = "ruff ${RUFF_PIN#ruff==}" ]; then DETAIL="$v"; return 0; fi
  DETAIL="$v is installed, CI pins $RUFF_PIN"
  return 1
}

ruff_install() {
  pip_run install --no-cache-dir "$RUFF_PIN" || { FIXERR="pip install $RUFF_PIN failed; see $STEP_LOG"; return 1; }
}

step_python() {
  ensure required "python venv" venv_ok venv_make
  needs required "cpu torch" torch_ok torch_install
  needs required "requirements" reqs_ok reqs_install
  REQS_OK="$ENSURED"
  needs required "ruff" ruff_ok ruff_install
}

# --- step: pgeocode -----------------------------------------------------------------------
pgeocode_dir() { "$VENV/bin/python" -c 'import pgeocode; print(pgeocode.STORAGE_DIR)' 2>/dev/null; }

pgeocode_ok() {
  local d
  d="$(pgeocode_dir)" || { DETAIL="pgeocode is not installed in $VENV"; return 1; }
  if [ -s "$d/US.txt" ] && [ -s "$d/US-index.txt" ]; then
    DETAIL="US postal table cached in $d"
    return 0
  fi
  DETAIL="US postal table not cached in $d; backend/db/geo.py would download it at import"
  return 1
}

pgeocode_seed() {
  # One source per try, with a socket timeout, so a blocked host fails fast.
  if DS_PGEOCODE_URLS="$PGEOCODE_URLS" "$VENV/bin/python" - >>"$STEP_LOG" 2>&1 <<'PY'
import os
import socket
import sys

import pgeocode

socket.setdefaulttimeout(30)
for url in os.environ["DS_PGEOCODE_URLS"].split():
    pgeocode.DOWNLOAD_URL = [url]
    try:
        pgeocode.Nominatim("us")
        print("pgeocode: US table cached from", url)
        sys.exit(0)
    except Exception as exc:
        print("pgeocode: %s failed: %s: %s" % (url, type(exc).__name__, exc))
sys.exit(1)
PY
  then
    return 0
  fi
  FIXERR="could not download the US postal table: allow download.geonames.org, or raw.githubusercontent.com for the mirror (docs/CLOUD.md); see $STEP_LOG"
  return 1
}

step_pgeocode() {
  if [ "$CHECK_ONLY" = 0 ] && [ "$REQS_OK" != 1 ] && want python; then
    report failed "pgeocode table" "skipped: the requirements are not in place"
    return 0
  fi
  ensure required "pgeocode table" pgeocode_ok pgeocode_seed
}

# --- step: node (check only) ---------------------------------------------------------------
node_ok() {
  local v bin
  if [ -x "$NODE_DIR/node" ]; then bin="$NODE_DIR/node"; elif have node; then bin="$(command -v node)"; else
    DETAIL="node not found (the VM image normally ships $NODE_DIR); the JS unit tests skip without it"
    return 1
  fi
  v="$("$bin" --version 2>/dev/null)" || { DETAIL="$bin does not run"; return 1; }
  case "$v" in
    v20.*) DETAIL="node $v at $bin"; return 0 ;;
  esac
  DETAIL="node $v at $bin; CI runs Node 20 (the VM image normally ships it at $NODE_DIR)"
  return 1
}

# Check only: this script does not install Node, so there is no fix function.
step_node() { ensure optional "node 20" node_ok ""; }

# --- step: pg ---------------------------------------------------------------------------
pg_ok() {
  local v
  if [ ! -x "$PG_BIN/initdb" ] || [ ! -x "$PG_BIN/pg_ctl" ] || [ ! -x "$PG_BIN/psql" ]; then
    DETAIL="no PostgreSQL $PG_MAJOR server binaries in $PG_BIN"
    return 1
  fi
  [ -f "$PG_EXT/vector.control" ] || { DETAIL="pgvector is missing from $PG_EXT"; return 1; }
  [ -f "$PG_EXT/dblink.control" ] || { DETAIL="dblink (contrib) is missing from $PG_EXT"; return 1; }
  v="$("$PG_BIN/postgres" --version 2>/dev/null || true)"
  DETAIL="${v:-PostgreSQL $PG_MAJOR} with pgvector and dblink"
}

# Fingerprint of the first key in an armored key file. awk reads all of gpg's output (no early
# exit), because under `set -o pipefail` a reader that quits first turns the pipeline into a failure.
key_fingerprint() {
  local home out
  home="$(mktemp -d)"
  out="$(GNUPGHOME="$home" gpg --show-keys --with-colons "$1" 2>/dev/null | awk -F: '/^fpr:/ && !seen {print $10; seen = 1}')" || out=""
  rm -rf "$home"
  printf '%s' "$out"
}

pgdg_key_ok() {
  [ -s "$PGDG_KEY" ] || return 1
  have gpg || return 1
  [ "$(key_fingerprint "$PGDG_KEY")" = "$PGDG_KEY_FPR" ]
}

pg_install() {
  local id codename tmp fpr line conf policy
  if [ "$(id -u)" -ne 0 ]; then FIXERR="installing PostgreSQL needs root"; return 1; fi
  if ! have apt-get; then FIXERR="no apt-get here; the pg tier needs a Debian or Ubuntu VM (docs/CLOUD.md)"; return 1; fi
  # shellcheck source=/dev/null
  id="$(. /etc/os-release && printf '%s' "${ID:-}")"
  # shellcheck source=/dev/null
  codename="$(. /etc/os-release && printf '%s' "${VERSION_CODENAME:-}")"
  case "$id" in ubuntu|debian) ;; *) FIXERR="PGDG packages are set up here for Ubuntu and Debian only, not '$id'"; return 1 ;; esac
  [ -n "$codename" ] || { FIXERR="no VERSION_CODENAME in /etc/os-release"; return 1; }
  if ! have gpg || ! have curl; then
    apt_install ca-certificates curl gnupg || { FIXERR="could not install curl and gnupg; see $STEP_LOG"; return 1; }
  fi

  # Fail early, before touching apt's sources, with the fix that matters in a restricted cloud network.
  if ! curl -fsS -o /dev/null --max-time 15 --retry 1 "$PGDG_URL/dists/${codename}-pgdg/Release" 2>>"$STEP_LOG"; then
    FIXERR="apt.postgresql.org is not reachable: add apt.postgresql.org and www.postgresql.org to the environment's allowed hosts (docs/CLOUD.md)"
    return 1
  fi

  if ! pgdg_key_ok; then
    mkdir -p "$(dirname "$PGDG_KEY")"
    tmp="$(mktemp)"
    if ! curl -fsSL --max-time 60 --retry 2 -o "$tmp" "$PGDG_KEY_URL" 2>>"$STEP_LOG"; then
      rm -f "$tmp"
      FIXERR="could not download the PGDG signing key from www.postgresql.org (allow it in the environment's network settings)"
      return 1
    fi
    fpr="$(key_fingerprint "$tmp")"
    if [ "$fpr" != "$PGDG_KEY_FPR" ]; then
      rm -f "$tmp"
      FIXERR="the downloaded PGDG key has fingerprint '${fpr:-none}', expected $PGDG_KEY_FPR; not trusting it"
      return 1
    fi
    install -m 0644 "$tmp" "$PGDG_KEY"
    rm -f "$tmp"
  fi
  line="deb [signed-by=$PGDG_KEY] $PGDG_URL ${codename}-pgdg main"
  if ! grep -qxF "$line" "$PGDG_LIST" 2>/dev/null; then printf '%s\n' "$line" >"$PGDG_LIST"; fi

  # No default cluster: the package would create and start 18/main on a port the tests do not use.
  conf=/etc/postgresql-common/createcluster.conf
  mkdir -p /etc/postgresql-common
  if [ -f "$conf" ] && grep -qE '^[#[:space:]]*create_main_cluster' "$conf"; then
    sed -i -E 's/^[#[:space:]]*create_main_cluster[[:space:]]*=.*/create_main_cluster = false/' "$conf"
  elif [ -f "$conf" ]; then
    printf 'create_main_cluster = false\n' >>"$conf"
  else
    printf 'create_main_cluster = false\n' >"$conf"
  fi

  apt_run update -qq || true
  # Read into a variable and match with `case`: `apt-cache | grep -q` can take SIGPIPE, which
  # `set -o pipefail` turns into a failure even though the candidate is there.
  policy="$(apt-cache policy "postgresql-$PG_MAJOR" 2>/dev/null || true)"
  case "$policy" in
    *"Candidate: "[0-9]*) ;;
    *) FIXERR="postgresql-$PG_MAJOR has no install candidate after apt-get update (apt.postgresql.org blocked?); see $STEP_LOG"; return 1 ;;
  esac
  apt_run install -y --no-install-recommends \
    "postgresql-$PG_MAJOR" "postgresql-client-$PG_MAJOR" "postgresql-$PG_MAJOR-pgvector" \
    || { FIXERR="apt-get install postgresql-$PG_MAJOR failed; see $STEP_LOG"; return 1; }

  # End to end: start the throwaway cluster the way the tests do and load both extensions.
  # The single quotes are deliberate: the inner shell expands $0 (the bin dir) and the DSN.
  # shellcheck disable=SC2016
  if ! "$ROOT/scripts/cloud/with_pg.sh" -- bash -c '"$0/psql" "$TEST_PG_ADMIN_DSN" -qAtX -c "create extension vector; create extension dblink; select 1"' "$PG_BIN" >>"$STEP_LOG" 2>&1; then
    FIXERR="PostgreSQL $PG_MAJOR installed, but a throwaway cluster with pgvector and dblink did not start; see $STEP_LOG"
    return 1
  fi
}

step_pg() { ensure optional "postgres $PG_MAJOR + pgvector" pg_ok pg_install; }

# --- step: env ------------------------------------------------------------------------------
venvlink_ok() {
  if [ -L "$ROOT/.venv" ] && [ "$(readlink "$ROOT/.venv")" = "$VENV" ]; then DETAIL=".venv -> $VENV"; return 0; fi
  if [ -e "$ROOT/.venv" ]; then DETAIL="$ROOT/.venv already exists and is left alone"; return 0; fi
  DETAIL="no .venv link; scripts that call .venv/bin/python will not find the venv"
  return 1
}

venvlink_make() {
  local exclude
  [ -x "$VENV/bin/python" ] || { FIXERR="no venv to link"; return 1; }
  ln -s "$VENV" "$ROOT/.venv" || return 1
  # .gitignore's ".venv/" does not match a symlink, so keep it out of `git status` locally.
  if have git && exclude="$(git -C "$ROOT" rev-parse --git-path info/exclude 2>/dev/null)"; then
    case "$exclude" in /*) ;; *) exclude="$ROOT/$exclude" ;; esac
    mkdir -p "$(dirname "$exclude")"
    grep -qxF '.venv' "$exclude" 2>/dev/null || printf '.venv\n' >>"$exclude"
  fi
}

env_file_write() {
  local mark="# ds-cloud-setup"
  [ -n "${CLAUDE_ENV_FILE:-}" ] || return 0
  if grep -qxF "$mark" "$CLAUDE_ENV_FILE" 2>/dev/null; then return 0; fi
  {
    printf '%s\n' "$mark"
    # $PATH stays literal on purpose: the env file is sourced before each command, and expands it then.
    # shellcheck disable=SC2016
    if [ -d "$NODE_DIR" ]; then
      printf 'export PATH="%s/bin:%s:$PATH"\n' "$VENV" "$NODE_DIR"
    else
      printf 'export PATH="%s/bin:$PATH"\n' "$VENV"
    fi
    if pg_ok; then
      printf 'export PG_TEST_BIN="%s"\nexport PG_TEST_EXT="%s"\n' "$PG_BIN" "$PG_EXT"
    fi
  } >>"$CLAUDE_ENV_FILE"
}

step_env() {
  if [ -x "$VENV/bin/python" ]; then ensure optional ".venv link" venvlink_ok venvlink_make; fi
  if [ -z "${CLAUDE_ENV_FILE:-}" ]; then
    report ok "session env" "CLAUDE_ENV_FILE not set (outside a SessionStart hook): PATH and PG_TEST_* not exported"
  elif [ "$CHECK_ONLY" = 1 ]; then
    report ok "session env" "would append PATH and PG_TEST_* to CLAUDE_ENV_FILE"
  else
    env_file_write
    report ok "session env" "PATH (venv, node 20) and PG_TEST_BIN/PG_TEST_EXT appended to CLAUDE_ENV_FILE"
  fi
}

# --- driver ---------------------------------------------------------------------------------
run_step() {  # run_step <index> <name> <function>
  want "$2" || return 0
  CUR_IDX="$1"
  SEQ=0
  if [ "$CHECK_ONLY" = 0 ]; then
    STEP_LOG="$LOG_DIR/$2.log"
    say "[$2]"
  else
    STEP_LOG=/dev/null
  fi
  "$3"
}

summary_table() {
  sort "$REPORT" | while IFS='|' read -r _key level label detail; do
    printf '  %-9s %-24s %s\n' "$level" "$label" "$detail"
  done
}

count_level() { grep -cE "^[0-9]+\|($1)\|" "$REPORT" || true; }

hook_summary() {
  local facts bad
  bad="$(sort "$REPORT" | awk -F'|' '$2=="missing"||$2=="failed" {printf "%s%s (%s)", sep, $3, $4; sep="; "}')"
  facts="$(sort "$REPORT" | awk -F'|' '($2=="ok"||$2=="fixed") && ($3 ~ /^python venv|^cpu torch|^node|^postgres/) {printf "%s%s", sep, $4; sep="; "}')"
  if [ -n "$bad" ]; then
    printf 'cloud-setup: NOT READY after %ss: %s\n' "$SECONDS" "$bad"
    printf 'cloud-setup: run scripts/cloud/setup.sh in a shell to see why; logs are in %s. Guide: docs/CLOUD.md\n' "$LOG_DIR"
  else
    printf 'cloud-setup: environment ready in %ss. %s\n' "$SECONDS" "$facts"
    printf 'cloud-setup: commands and rules are in docs/CLOUD.md. Never push main; each push after a branch'"'"'s first needs scripts/bump_version.sh patch.\n'
  fi
  sort "$REPORT" | awk -F'|' '$2=="optional" {printf "cloud-setup: not available: %s (%s)\n", $3, $4}'
}

finish() {
  local bad opt
  bad="$(count_level 'missing|failed')"
  opt="$(count_level 'optional')"
  if [ "$HOOK" = 1 ]; then
    hook_summary
    return 0
  fi
  printf '\nsetup.sh report (repo %s, venv %s, %ss)\n' "$ROOT" "$VENV" "$SECONDS"
  summary_table
  if [ "$bad" -gt 0 ]; then
    printf '\n%s required item(s) %s.\n' "$bad" "$([ "$CHECK_ONLY" = 1 ] && echo missing || echo "not in place")"
    return 1
  fi
  if [ "$opt" -gt 0 ]; then
    printf '\nAll required items are in place; %s optional item(s) are not available (see above).\n' "$opt"
  else
    printf '\nAll items are in place.\n'
  fi
}

# One run at a time. The script re-runs itself under flock(1), which holds the lock for the
# child and closes the lock file in it (-o): a process that outlives the script, like a service a
# package started, cannot inherit the lock and block every later run.
acquire_lock() {
  have flock || return 0
  [ -z "${DS_CLOUD_LOCKED:-}" ] || return 0
  DS_CLOUD_LOCKED=1 exec flock -o -w 1800 "$LOCK_FILE" bash "${BASH_SOURCE[0]}" "$@"
}

main() {
  local pg_pid=""
  parse_args "$@"
  if [ "$HOOK" = 1 ] && [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then exit 0; fi  # a local session: nothing to do
  if [ "$CHECK_ONLY" = 0 ] && [ "$(uname -s)" != Linux ]; then
    die "this installs system packages and belongs on the Linux cloud VM; use --check to inspect this machine"
  fi
  if [ "$CHECK_ONLY" = 0 ]; then acquire_lock "$@"; fi  # may re-exec, so before anything that needs cleanup
  REPORT="$(mktemp "$TMP_ROOT/ds-cloud-report.XXXXXX")"
  trap 'rm -f "$REPORT"' EXIT
  if [ "$CHECK_ONLY" = 0 ]; then mkdir -p "$LOG_DIR"; fi

  # The two slow downloads (apt for Postgres, pip for the Python side) overlap.
  if want pg; then
    if [ "$CHECK_ONLY" = 0 ] && want python; then
      ( run_step 5 pg step_pg ) &
      pg_pid=$!
    else
      run_step 5 pg step_pg
    fi
  fi
  run_step 1 git step_git
  run_step 2 python step_python
  run_step 3 pgeocode step_pgeocode
  run_step 4 node step_node
  if [ -n "$pg_pid" ]; then wait "$pg_pid" || true; fi
  run_step 6 env step_env
  finish
}

# Sourcing the file (for tests) defines everything and runs nothing.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
