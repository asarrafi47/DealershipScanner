#!/usr/bin/env bash
# P10B.4 helper: backups + rollback SQL for the LOCAL provenance backfill. Owner-gated.
# Default is a PLAN ONLY (prints every step, touches nothing). --run executes steps 1-3
# (quiescence check, pg_dump to files, snapshot table). The backfill itself is never run here:
# see RUNBOOK_P10B4.md for the dry run and the --apply the owner OKs on the counts.
#
#   workspace/phase_runs/N23/p10b4_prepare.sh [--run] [--db cars] [--out DIR]
set -euo pipefail

RUN=0; DB=cars; STAMP=$(date +%Y%m%d)
OUT="workspace/backups/p10b4_${STAMP}"
while [ $# -gt 0 ]; do
  case "$1" in
    --run) RUN=1 ;;
    --db) DB="$2"; shift ;;
    --out) OUT="$2"; shift ;;
    *) echo "unknown arg $1" >&2; exit 64 ;;
  esac
  shift
done
case "$DB" in cars_cleanroom|cars_cr_*) echo "refusing: $DB is a frozen baseline" >&2; exit 64 ;; esac

export PATH=/opt/homebrew/opt/postgresql@18/bin:$PATH
PSQL="psql -h /tmp -d $DB -v ON_ERROR_STOP=1"
SNAP="cars_epa_link_snapshot_${STAMP}"
PY="${PYTHON:-.venv/bin/python}"

step() { echo "== $*"; }
do_or_say() { if [ "$RUN" = 1 ]; then "$@"; else echo "   (plan) $*"; fi; }

step "1. quiescence (no other session active or idle in transaction; mini shards included)"
do_or_say env PYTHONPATH=. INVENTORY_DATABASE_URL="postgresql://localhost/$DB" \
  "$PY" backend/scripts/backfill_epa_master_provenance.py --check-quiescent

step "2. pg_dump the three catalog tables to $OUT"
do_or_say mkdir -p "$OUT"
for t in epa_master epa_extended_specs epa_master_dump_id_map; do
  do_or_say pg_dump -h /tmp -d "$DB" --table="public.$t" --format=custom --file="$OUT/$t.dump"
done

step "3. snapshot of every car link: public.$SNAP"
do_or_say $PSQL -c "CREATE TABLE public.$SNAP AS SELECT id AS car_id, epa_master_id FROM public.cars WHERE epa_master_id IS NOT NULL"

step "4. rollback SQL written to $OUT/rollback.sql (provenance columns only; ids never move)"
ROLLBACK_SQL=$(cat <<SQL
-- Undo the P10B.4 backfill: clear what it stamped, restore links from the snapshot if any moved.
BEGIN;
UPDATE public.epa_master SET source = NULL, natural_key = NULL;
UPDATE public.cars c SET epa_master_id = s.epa_master_id
  FROM public.$SNAP s WHERE s.car_id = c.id AND c.epa_master_id IS DISTINCT FROM s.epa_master_id;
COMMIT;
-- Exact restore of the catalog tables instead: pg_restore --clean --if-exists -d $DB $OUT/<table>.dump
SQL
)
if [ "$RUN" = 1 ]; then printf '%s\n' "$ROLLBACK_SQL" > "$OUT/rollback.sql"; else printf '%s\n' "$ROLLBACK_SQL" | sed 's/^/   (plan) /'; fi

step "done. Next: RUNBOOK_P10B4.md (dry run, owner OK on counts, --apply)"
