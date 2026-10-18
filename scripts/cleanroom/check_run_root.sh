#!/usr/bin/env bash
# check_run_root.sh: the one run-root rule every clean-room run script applies (N1.3).
#
#   bash scripts/cleanroom/check_run_root.sh --art <dir> [--db <name>]
#
# A scratch re-run's run root is workspace/cleanroom_runs/<db>/ (plan Section 7.0). The
# rule is checked against this checkout and, from a git worktree, the main checkout too
# (git common dir), both through realpath:
#   1. REFUSED when <art> is, or contains, a checkout root or its workspace/ (so an
#      ancestor such as the repo root, $HOME or / can never be a run root).
#   2. OK when <art> is exactly workspace/cleanroom_runs/<db> (no --db: <db> is the
#      basename of <art>, which must be a cars_cr_<name> / cars_rehearsal name).
#   3. REFUSED when <art> is anywhere else under workspace/: inside the frozen
#      workspace/cleanroom_2026_10, the real workspace/dealer_logs or workspace/recipes,
#      workspace/cleanroom_runs itself or another DB's run root.
#   4. Outside every workspace/: REFUSED, unless CR_ALLOW_ART_OUTSIDE_RUNS=1 (tests and
#      throwaway dirs only), which is reported in the output.
# Prints one line ("ok ..." or "note ...", exit 0; "REFUSED ...", exit 1). Writes nothing,
# never connects to a database. Callers: preflight.sh, build_db.sh, isolation_snapshot.sh,
# make_manifest.sh, run.env.template.
set -uo pipefail
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ART='' DB=''
while [ $# -gt 0 ]; do
  case "$1" in
    --art) ART=${2:-}; shift 2 2>/dev/null || shift;;
    --db) DB=${2:-}; shift 2 2>/dev/null || shift;;
    *) echo "REFUSED unknown argument $1"; exit 1;;
  esac
done
MAIN=$(git -C "$REPO" rev-parse --path-format=absolute --git-common-dir 2>/dev/null) && MAIN=$(dirname "$MAIN") || MAIN=$REPO
python3 - "$REPO" "$MAIN" "$ART" "$DB" "${CR_ALLOW_ART_OUTSIDE_RUNS:-}" <<'PY'
import os
import re
import sys

repo, main, art_arg, db, allow = sys.argv[1:6]
SCRATCH = re.compile(r"^(cars_cr_[a-z0-9_]+|cars_rehearsal)$")


def out(ok: bool, msg: str) -> None:
    print(("" if ok else "REFUSED ") + msg)
    sys.exit(0 if ok else 1)


if not art_arg:
    out(False, "--art is required")
art = os.path.realpath(art_arg if os.path.isabs(art_arg) else os.path.join(repo, art_arg))
if db and not SCRATCH.match(db):
    out(False, f"--db {db!r} is not a scratch name (cars_cr_<name> or cars_rehearsal)")
want_db = db or os.path.basename(art)


def under(p: str, root: str) -> bool:
    return p == root or p.startswith(root.rstrip("/") + "/")


roots = []
for r in (repo, main):
    r = os.path.realpath(r)
    if r not in roots:
        roots.append(r)

# 1. never a checkout root, its workspace/, or anything that contains them
for r in roots:
    for p, what in ((r, "the checkout"), (os.path.join(r, "workspace"), "the real workspace")):
        if under(p, art):
            out(False, f"run root {art} is or contains {what} {p}")

# 2. exactly workspace/cleanroom_runs/<db>
for r in roots:
    want = os.path.realpath(os.path.join(r, "workspace", "cleanroom_runs", want_db))
    if art == want and SCRATCH.match(want_db):
        out(True, f"ok   run root {art} is workspace/cleanroom_runs/{want_db}")

# 3. anywhere else under a workspace/
named = (("cleanroom_2026_10", "the frozen 2026-10 clean-room dir"),
         ("dealer_logs", "the real dealer logs dir"),
         ("recipes", "the real recipe cache"))
for r in roots:
    w = os.path.realpath(os.path.join(r, "workspace"))
    for sub, what in named:
        if under(art, os.path.join(w, sub)):
            out(False, f"run root {art} is inside {what} ({os.path.join(w, sub)})")
    if under(art, w):
        if not SCRATCH.match(want_db):
            out(False, f"run root {art} is under {w} but {want_db!r} is not a scratch DB name")
        out(False, f"run root {art} is not {os.path.join(w, 'cleanroom_runs', want_db)}")

# 4. outside every workspace/
if allow == "1":
    out(True, f"note run root {art} is outside workspace/cleanroom_runs/{want_db}: "
              "allowed by CR_ALLOW_ART_OUTSIDE_RUNS=1 (tests and throwaway dirs only)")
out(False, f"run root {art} is not workspace/cleanroom_runs/{want_db} under {roots[0]}"
           + (f" or {roots[1]}" if len(roots) > 1 else "")
           + " (CR_ALLOW_ART_OUTSIDE_RUNS=1 is for tests only)")
PY
