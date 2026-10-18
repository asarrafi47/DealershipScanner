#!/usr/bin/env bash
# make_manifest.sh: write <art>/manifest/{scanner_dealers.json,pipeline_dealers.json} and
# <art>/roster.txt for a scratch re-run, filtered to --roster from a source manifest dir
# (default: the frozen clean-room manifests, workspace/cleanroom_2026_10/manifest).
#
#   scripts/cleanroom/make_manifest.sh --art workspace/cleanroom_runs/cars_cr_x --roster <file> [--from <dir>] [--db cars_cr_x]
#
# Refuses (exit 2) when a roster id is missing from either source manifest (a new
# rooftop needs its entry in a --from dir first), or, before writing anything, when
# --art is not workspace/cleanroom_runs/<db> (<db> defaults to the basename of --art;
# scripts/cleanroom/check_run_root.sh: the frozen workspace/cleanroom_2026_10 and any
# ancestor of workspace/ always; tests may use a dir outside every workspace/ with
# CR_ALLOW_ART_OUTSIDE_RUNS=1). Writes only under <art>.
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
ART= ROSTER= DB= FROM=$REPO/workspace/cleanroom_2026_10/manifest
while [ $# -gt 0 ]; do
  case "$1" in
    --art) ART=${2:-}; shift 2;;
    --roster) ROSTER=${2:-}; shift 2;;
    --from) FROM=${2:-}; shift 2;;
    --db) DB=${2:-}; shift 2;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
[ -n "$ART" ] && [ -n "$ROSTER" ] || { echo "usage: $0 --art <dir> --roster <file> [--from <dir>] [--db <name>]" >&2; exit 2; }
case "$ART" in /*) ;; *) ART=$REPO/$ART;; esac
rr=$(bash "$REPO/scripts/cleanroom/check_run_root.sh" --art "$ART" ${DB:+--db "$DB"}) || { echo "REFUSED: ${rr#REFUSED }" >&2; exit 2; }
echo "$rr"
mkdir -p "$ART/manifest"
python3 - "$ROSTER" "$FROM" "$ART" <<'PY'
import json, sys
from pathlib import Path
roster_path, src, art = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
ids = [ln.split("#", 1)[0].strip() for ln in roster_path.read_text().splitlines()]
ids = [i for i in ids if i]
if len(ids) != len(set(ids)):
    sys.exit("REFUSED: duplicate ids in roster")
for name in ("scanner_dealers.json", "pipeline_dealers.json"):
    rows = json.loads((src / name).read_text())
    by = {r["dealer_id"]: r for r in rows}
    missing = [i for i in ids if i not in by]
    if missing:
        print(f"REFUSED: {len(missing)} roster ids missing from {src / name}: {', '.join(missing[:20])}", file=sys.stderr)
        sys.exit(2)
    (art / "manifest" / name).write_text(json.dumps([by[i] for i in ids], indent=1) + "\n")
(art / "roster.txt").write_text("\n".join(ids) + "\n")
print(f"manifest: {len(ids)} dealers -> {art / 'manifest'}; roster -> {art / 'roster.txt'}")
PY
