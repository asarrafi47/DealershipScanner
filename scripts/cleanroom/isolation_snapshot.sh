#!/usr/bin/env bash
# isolation_snapshot.sh before|after: prove a scratch-DB re-run wrote nothing outside its
# run root (N1.3). Parameterized copy of workspace/cleanroom_2026_10/isolation_snapshot.sh
# (which hard-codes ART at :6).
#
#   bash scripts/cleanroom/isolation_snapshot.sh before --db cars_cr_x --art workspace/cleanroom_runs/cars_cr_x [--roster <file>]
#   ... run ...
#   bash scripts/cleanroom/isolation_snapshot.sh after  --db cars_cr_x --art workspace/cleanroom_runs/cars_cr_x
#
# Records, through read-only sessions: pg_stat_database tup_inserted / tup_updated /
# tup_deleted for every protected DB (cars, cars_cleanroom, plus --protect <db>...), the
# cars and dealer_recipes row counts of each, file counts of the real workspace dirs,
# whether the real recipe cache got a store tag, and the git tracked-change count.
# "after" diffs against "before" and lists files newer than <art>/START under the real
# dirs. Prints ISOLATION OK, or exits 1. Writes only <art>/snapshots/* and <art>/START.
set -uo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
mode=${1:-}; shift || true
DB= ART= PROTECT="cars cars_cleanroom"
while [ $# -gt 0 ]; do
  case "$1" in
    --db) DB=${2:-}; shift 2;;
    --art) ART=${2:-}; shift 2;;
    --roster) shift 2;;  # accepted for symmetry with the other run scripts
    --protect) PROTECT="$PROTECT ${2:-}"; shift 2;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
case "$mode" in before|after) ;; *) echo "usage: $0 before|after --db <name> --art <dir>" >&2; exit 2;; esac
[ -n "$DB" ] && [ -n "$ART" ] || { echo "--db and --art are required" >&2; exit 2; }
case " $PROTECT " in *" $DB "*) echo "REFUSED: $DB is a protected DB, not a scratch target" >&2; exit 2;; esac
case "$ART" in /*) ;; *) ART=$REPO/$ART;; esac
SNAP=$ART/snapshots; mkdir -p "$SNAP"
RO="-c default_transaction_read_only=on -c statement_timeout=60000"
q() { env -u PGAPPNAME PGOPTIONS="$RO" psql -X -h /tmp -p 5432 -At -F'|' "$@"; }
DIRS="$REPO/workspace/recipes $REPO/workspace/dealer_logs $REPO/workspace/debug $REPO/workspace/pipeline $REPO/workspace/scanlogs $REPO/workspace/cleanroom_2026_10/recipes_cache $REPO/workspace/cleanroom_2026_10/dealer_logs $REPO/workspace/cleanroom_2026_10/runs $REPO/workspace/cleanroom_2026_10/manifest $REPO/workspace/cleanroom_2026_10/workspace /Users/asarrafi/Projects/workspace/scanlogs $REPO/debug $REPO/car_window_stickers $REPO/backend/dictionary/derived"
out=$SNAP/$mode.txt
{
  echo "taken: $(date -u +%FT%TZ)"
  for pdb in $PROTECT; do
    q -d postgres -c "SELECT 'stat $pdb', tup_inserted, tup_updated, tup_deleted FROM pg_stat_database WHERE datname='$pdb'"
    if [ "$(q -d postgres -c "SELECT count(*) FROM pg_database WHERE datname='$pdb'")" = "1" ]; then
      q -d "$pdb" -c "SELECT 'rows $pdb cars', count(*), max(scraped_at) FROM cars" \
                  -c "SELECT 'rows $pdb dealer_recipes', count(*), max(updated_at) FROM dealer_recipes"
    fi
  done
  for d in $DIRS; do [ -d "$d" ] && echo "dir $d files=$(find "$d" -type f | wc -l | tr -d ' ')"; done
  echo "store_tag_in_real_cache=$([ -e "$REPO/workspace/recipes/_store.json" ] && echo PRESENT || echo absent)"
  echo "git_tracked_changes=$(git -C "$REPO" status --porcelain --untracked-files=no | wc -l | tr -d ' ')"
} > "$out"
if [ "$mode" = before ]; then
  touch "$ART/START"; echo "wrote $out and $ART/START"; exit 0
fi
[ -f "$SNAP/before.txt" ] || { echo "no before snapshot in $SNAP" >&2; exit 1; }
rc=0
echo "== diff before -> after (protected DB write counters and rows must match)"
if diff <(grep -v '^taken' "$SNAP/before.txt") <(grep -v '^taken' "$out"); then echo "  identical"; else echo "  DIFFERENCES ABOVE"; rc=1; fi
echo "== files under the real dirs newer than START (must be empty)"
leaks=$(for d in $DIRS; do [ -d "$d" ] && find "$d" -type f -newer "$ART/START"; done)
if [ -z "$leaks" ]; then echo "  none"; else echo "$leaks" | head -50; echo "  LEAKS FOUND"; rc=1; fi
grep -q "store_tag_in_real_cache=absent" "$out" && echo "  real recipe cache still untagged" || { echo "  REAL CACHE WAS TAGGED"; rc=1; }
[ $rc = 0 ] && echo "ISOLATION OK" || { echo "ISOLATION FAIL"; exit 1; }
