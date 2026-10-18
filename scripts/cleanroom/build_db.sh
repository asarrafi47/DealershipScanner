#!/usr/bin/env bash
# build_db.sh: create a clean-room scratch DB (N1.3). Parameterized copy of
# workspace/cleanroom_2026_10/build_db.sh, which hard-codes cars_cleanroom (:19-26).
#
#   scripts/cleanroom/build_db.sh --db cars_cr_x [--art workspace/cleanroom_runs/cars_cr_x]
#                                 [--source cars] [--from-template <db>]
#
# Default mode: V025-era schema and the reference tables copied from --source (read
# only), car-derived tables empty (same table list as the 10-13 build). With
# --from-template <db> it runs `createdb -T <db>` instead (a full copy; the template
# is only read). Either way it stamps `git rev-parse HEAD` into <art>/build/git_head.txt
# and <art>/BUILD_INFO.json, and prints the next step (migrate --apply on the new DB).
#
# Refuses: an existing DB; a target named cars or cars_cleanroom or anything that is
# not cars_cr_<name> / cars_rehearsal; a template equal to the target; an --art that is
# not workspace/cleanroom_runs/<db> (scripts/cleanroom/check_run_root.sh, checked before
# any connection or mkdir; CR_ALLOW_ART_OUTSIDE_RUNS=1 admits a dir outside every
# workspace/ for tests). Only ever talks to the local Homebrew PG at /tmp:5432. Never
# run it with run.env.template sourced.
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
DB= ART= SOURCE=cars TEMPLATE=
usage() { sed -n '2,19p' "$0" >&2; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --db) DB=${2:-}; shift 2;;
    --art) ART=${2:-}; shift 2;;
    --source) SOURCE=${2:-}; shift 2;;
    --from-template) TEMPLATE=${2:-}; shift 2;;
    -h|--help) usage;;
    -*) echo "unknown option $1" >&2; usage;;
    *) [ -z "$DB" ] && DB=$1 || usage; shift;;
  esac
done
refuse() { echo "REFUSED: $*" >&2; exit 2; }
case "$DB" in
  ''|cars|cars_cleanroom) refuse "write target '${DB}' is the frozen baseline or the live DB";;
esac
[[ "$DB" =~ ^(cars_cr_[a-z0-9_]+|cars_rehearsal)$ ]] || refuse "'$DB' is not a scratch name (cars_cr_<name> or cars_rehearsal)"
[ -n "$TEMPLATE" ] && [ "$TEMPLATE" = "$DB" ] && refuse "template and target are the same DB"
ART=${ART:-$REPO/workspace/cleanroom_runs/$DB}
case "$ART" in /*) ;; *) ART=$REPO/$ART;; esac
# Before any connection or mkdir: the run root must be workspace/cleanroom_runs/<db>, so
# the frozen run's build/, recipes_cache/ and the real workspace are never written.
rr=$(bash "$REPO/scripts/cleanroom/check_run_root.sh" --art "$ART" --db "$DB") || refuse "${rr#REFUSED }"
echo "$rr"
RO="-c default_transaction_read_only=on"
PSQL=(psql -X -h /tmp -p 5432)
unset PGOPTIONS PGHOST PGPORT PGDATABASE PGSERVICE

exists() { [ "$("${PSQL[@]}" -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='$1'")" = "1" ]; }
exists "$DB" && refuse "$DB already exists; drop it deliberately first (dropdb -h /tmp $DB)"
[ -n "$TEMPLATE" ] && ! exists "$TEMPLATE" && refuse "template $TEMPLATE does not exist"
[ -z "$TEMPLATE" ] && ! exists "$SOURCE" && refuse "source $SOURCE does not exist"

mkdir -p "$ART/build" "$ART/recipes_cache" "$ART/dealer_logs" "$ART/runs" "$ART/snapshots" "$ART/manifest"
if [ -f "$REPO/workspace/recipes/_aliases.json" ]; then
  cp "$REPO/workspace/recipes/_aliases.json" "$ART/recipes_cache/"
else
  echo "WARN: $REPO/workspace/recipes/_aliases.json not found (run from the main checkout); recipes_cache has no aliases" >&2
fi
HEAD_SHA=$(git -C "$REPO" rev-parse HEAD)
echo "$HEAD_SHA" > "$ART/build/git_head.txt"

stat_of() {  # write counters of a DB we only read
  PGOPTIONS="$RO" "${PSQL[@]}" -d postgres -Atc \
    "SELECT now(), tup_inserted, tup_updated, tup_deleted FROM pg_stat_database WHERE datname='$1'"
}
WATCH=${TEMPLATE:-$SOURCE}
stat_of "$WATCH" > "$ART/build/${WATCH}_stat_before.txt"

if [ -n "$TEMPLATE" ]; then
  echo "[1/2] createdb -T $TEMPLATE $DB"
  createdb -h /tmp -p 5432 -T "$TEMPLATE" "$DB"
  echo "[2/2] analyze"
  "${PSQL[@]}" -d "$DB" -q -c 'ANALYZE'
else
  # COPY: reference / recipe / registry data the scan or free enrichment reads (FK closure kept).
  # package_values* and trim_msrp_bands are derived from cars/stickers, so they stay EMPTY (cost review B5).
  COPY_TABLES="schema_migrations dealer_recipes dealerships dealer_ratings dealer_geopoints dealer_feed_scope
dealer_scan_profile epa_master epa_extended_specs epa_master_dump_id_map catalog_trims catalog_options
catalog_packages catalog_package_features catalog_exterior_colors catalog_interior_colors model_specs
model_generations ai_model_specs ai_engine_specs dictionary_options brochure_vision_facts
brochure_local_extraction_failures incomplete_listings_meta"
  echo "[1/6] createdb $DB"
  createdb -h /tmp -p 5432 -T template0 -E UTF8 --locale=en_US.UTF-8 "$DB"
  echo "[2/6] schema (pre-data) from $SOURCE, backup tables excluded"
  PGOPTIONS="$RO" pg_dump -h /tmp -p 5432 -d "$SOURCE" --section=pre-data --no-owner --no-privileges \
    -T 'public.cars_backup_*' -T 'public.*_bak_*' -f "$ART/build/schema_pre.sql"
  PGOPTIONS="$RO" pg_dump -h /tmp -p 5432 -d "$SOURCE" --section=post-data --no-owner --no-privileges \
    -T 'public.cars_backup_*' -T 'public.*_bak_*' -f "$ART/build/schema_post.sql"
  "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -1 -q -f "$ART/build/schema_pre.sql"
  echo "[3/6] reference data"
  targs=(); for t in $COPY_TABLES; do targs+=(-t "public.$t"); done
  PGOPTIONS="$RO" pg_dump -h /tmp -p 5432 -d "$SOURCE" --data-only "${targs[@]}" \
    | "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -1 -q
  echo "[4/6] nhtsa_vpic_cache"
  PGOPTIONS="$RO" pg_dump -h /tmp -p 5432 -d "$SOURCE" --data-only -t public.nhtsa_vpic_cache \
    | "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -1 -q
  echo "[5/6] post-data (indexes, constraints) + analyze"
  "${PSQL[@]}" -d "$DB" -v ON_ERROR_STOP=1 -1 -q -f "$ART/build/schema_post.sql"
  "${PSQL[@]}" -d "$DB" -q -c 'VACUUM (ANALYZE)'
  echo "[6/6] verify"
fi

"${PSQL[@]}" -d "$DB" -At -c \
  "SELECT 'cars='||(SELECT count(*) FROM cars)||' recipes='||(SELECT count(*) FROM dealer_recipes)||' vpic='||(SELECT count(*) FROM nhtsa_vpic_cache)||' migrations='||(SELECT count(*) FROM schema_migrations)" \
  | tee "$ART/build/rows_$DB.txt"
stat_of "$WATCH" > "$ART/build/${WATCH}_stat_after.txt"
before=$(cut -d'|' -f2- "$ART/build/${WATCH}_stat_before.txt")
after=$(cut -d'|' -f2- "$ART/build/${WATCH}_stat_after.txt")
MODE=dump; [ -n "$TEMPLATE" ] && MODE=template
cat > "$ART/BUILD_INFO.json" <<EOF
{"db": "$DB", "mode": "$MODE", "source": "${TEMPLATE:-$SOURCE}",
 "git_head": "$HEAD_SHA", "built_at": "$(date -u +%FT%TZ)", "art": "$ART",
 "watched_db_counters_before": "$before", "watched_db_counters_after": "$after"}
EOF
echo "$WATCH write counters before: $before"
echo "$WATCH write counters after:  $after"
[ "$before" = "$after" ] || { echo "WARNING: $WATCH write counters moved during the build (another writer?)" >&2; }
echo "git HEAD stamped: $HEAD_SHA -> $ART/build/git_head.txt"
echo "next: INVENTORY_DATABASE_URL='postgresql:///$DB?host=/tmp&port=5432' .venv/bin/python -m backend.scripts.migrate --apply"
echo "BUILD OK"
