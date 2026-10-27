#!/usr/bin/env bash
# prep_conv_db.sh: prepare the convergence DB for N11.5/N11.7 passes C and D (N11.1a).
#
#   scripts/cleanroom/prep_conv_db.sh --db cars_cr_conv [--art workspace/cleanroom_runs/cars_cr_conv]
#                                     [--source cars] [--shift-days 20]
#
# Run after `build_db.sh --from-template cars_cleanroom cars_cr_conv` and `migrate --apply`.
# Two steps, in one transaction each:
#   1. Replace every COPY_TABLES reference table (the list is read from build_db.sh, so the
#      two scripts cannot drift; dealer_recipes is one of them) with the current rows of
#      --source (read only), by the same data-only pg_dump pipe as build_db.sh. The template's
#      10-13 recipes still hold mbbeverlyhills-com's byte copy of mbontario's listing and
#      the 13 VIN-list recipes, which prod will not have after P6B.2.
#   2. UPDATE cars SET scraped_at = scraped_at - interval '<shift-days> days' (default 20),
#      so every row is more than 30 days old and passes C/D take P5A.1 rule 4's 90-day
#      fallback branch, as prod fleet #1 will (09-29 rows).
# Row counts of both steps go to <art>/build/prep_conv_db.json.
#
# Refuses: any target that is not cars_cr_conv (or cars_cr_conv_<x>), so never cars,
# cars_cleanroom or another scratch DB; a source equal to the target; a missing DB; a target
# with no cars rows (not a template copy); a second run (it would shift twice: the finished
# prep is recorded as the database comment 'prep_conv_db: ...'); an --art that is not the DB's
# run root (check_run_root.sh, before any connection). Only talks to the local Homebrew PG
# at /tmp:5432. Never run it with run.env.template sourced.
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
DB= ART= SOURCE=cars SHIFT=20
usage() { sed -n '2,24p' "$0" >&2; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --db) DB=${2:-}; shift 2;;
    --art) ART=${2:-}; shift 2;;
    --source) SOURCE=${2:-}; shift 2;;
    --shift-days) SHIFT=${2:-}; shift 2;;
    -h|--help) usage;;
    *) echo "unknown argument $1" >&2; usage;;
  esac
done
refuse() { echo "REFUSED: $*" >&2; exit 2; }
[[ "$DB" =~ ^cars_cr_conv(_[a-z0-9]+)?$ ]] || refuse "'${DB}' is not a convergence DB (cars_cr_conv); cars, cars_cleanroom and other scratch DBs are never prepared here"
[ "$SOURCE" = "$DB" ] && refuse "source and target are the same DB"
[[ "$SOURCE" =~ ^[a-z0-9_]+$ ]] || refuse "bad source name '$SOURCE'"
[[ "$SHIFT" =~ ^[0-9]+$ ]] && [ "$SHIFT" -ge 1 ] && [ "$SHIFT" -le 365 ] || refuse "--shift-days must be 1..365"
ART=${ART:-$REPO/workspace/cleanroom_runs/$DB}
case "$ART" in /*) ;; *) ART=$REPO/$ART;; esac
rr=$(bash "$REPO/scripts/cleanroom/check_run_root.sh" --art "$ART" --db "$DB") || refuse "${rr#REFUSED }"
echo "$rr"
RO="-c default_transaction_read_only=on"
PSQL=(psql -X -h /tmp -p 5432)
unset PGOPTIONS PGHOST PGPORT PGDATABASE PGSERVICE

# The one reference-table list: the COPY_TABLES block of build_db.sh.
COPY_TABLES=$(awk '/^ *COPY_TABLES="/{f=1; sub(/^ *COPY_TABLES="/,"")} f{ if (sub(/"$/,"")) {print; exit} print }' \
  "$REPO/scripts/cleanroom/build_db.sh" | tr '\n' ' ')
case " $COPY_TABLES " in *" dealer_recipes "*) ;; *) refuse "could not read COPY_TABLES (with dealer_recipes) from build_db.sh";; esac
for t in $COPY_TABLES; do [[ "$t" =~ ^[a-z0-9_]+$ ]] || refuse "bad table name '$t' in COPY_TABLES"; done

exists() { [ "$("${PSQL[@]}" -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='$1'")" = "1" ]; }
exists "$DB" || refuse "$DB does not exist; build it first (build_db.sh --from-template cars_cleanroom $DB)"
exists "$SOURCE" || refuse "source $SOURCE does not exist"
done_note=$("${PSQL[@]}" -d postgres -Atc "SELECT coalesce(shobj_description(oid,'pg_database'),'') FROM pg_database WHERE datname='$DB'")
case "$done_note" in prep_conv_db:*) refuse "$DB was already prepared ($done_note); shifting scraped_at twice would corrupt the gap. Drop and rebuild it";; esac
cars_before=$("${PSQL[@]}" -d "$DB" -Atc "SELECT count(*) FROM cars")
[ "${cars_before:-0}" -gt 0 ] 2>/dev/null || refuse "$DB has no cars rows; it is not a --from-template copy"

mkdir -p "$ART/build"
counts() {  # table|rows for each reference table of a DB
  for t in $COPY_TABLES; do echo "$t|$(PGOPTIONS="$RO" "${PSQL[@]}" -d "$1" -Atc "SELECT count(*) FROM public.$t")"; done
}
counts "$DB" > "$ART/build/prep_conv_db_before.txt"

echo "[1/2] replace reference tables in $DB with the rows of $SOURCE (read only)"
targs=(); for t in $COPY_TABLES; do targs+=(-t "public.$t"); done
{
  echo "SET session_replication_role = replica;"   # FK order is irrelevant for a full replace
  for t in $COPY_TABLES; do echo "DELETE FROM public.$t;"; done
  PGOPTIONS="$RO" pg_dump -h /tmp -p 5432 -d "$SOURCE" --data-only "${targs[@]}"
} | "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -1 -q
counts "$DB" > "$ART/build/prep_conv_db_after.txt"
recipes=$(grep '^dealer_recipes|' "$ART/build/prep_conv_db_after.txt" | cut -d'|' -f2)

echo "[2/2] shift scraped_at back $SHIFT days"
shifted=$("${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -Atc \
  "WITH u AS (UPDATE cars SET scraped_at = scraped_at - interval '$SHIFT days' RETURNING 1) SELECT count(*) FROM u")
stamp="prep_conv_db: shifted $shifted rows by $SHIFT days at $(date -u +%FT%TZ)"
"${PSQL[@]}" -d postgres -q -c "COMMENT ON DATABASE $DB IS '$stamp'"
HEAD_SHA=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)
cat > "$ART/build/prep_conv_db.json" <<JSON
{"db": "$DB", "source": "$SOURCE", "git_head": "$HEAD_SHA",
 "prepared_at": "$(date -u +%FT%TZ)", "reference_tables": "$(echo $COPY_TABLES | tr ' ' ',')",
 "dealer_recipes_rows": ${recipes:-null}, "cars_rows": $cars_before,
 "scraped_at_shift_days": $SHIFT, "scraped_at_rows_shifted": $shifted,
 "counts_before": "$ART/build/prep_conv_db_before.txt", "counts_after": "$ART/build/prep_conv_db_after.txt"}
JSON
echo "dealer_recipes rows: $recipes; scraped_at shifted: $shifted of $cars_before cars rows by $SHIFT days"
echo "logged: $ART/build/prep_conv_db.json"
echo "next: scripts/cleanroom/recipe_identity_check (clean on $DB), isolation_snapshot.sh, preflight.sh"
echo "PREP OK"
