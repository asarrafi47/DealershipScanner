#!/bin/sh
# Ownership snapshot (N10.7, CF-14 tooling): who holds each VIN right now, as CSV files.
#
#   scripts/ownership_snapshot.sh <dsn-env> <label>
#
# <dsn-env> is the NAME of the environment variable that holds the Postgres DSN
# (INVENTORY_DATABASE_URL, PROD_DATABASE_URL, ...), never the DSN itself. The session is
# read-only (default_transaction_read_only=on), so nothing here can write to the database.
# Files land in workspace/backups/ (ignored by git):
#   ownership_<label>_<stamp>.csv            id, vin, dealer_id, listing_active, scraped_at of every cars row
#   ownership_<label>_<stamp>_moves.csv      car_move_log (empty file when the table does not exist yet)
#   ownership_<label>_<stamp>_conflicts.csv  vin_owner_conflicts (empty when absent)
#   ownership_<label>_<stamp>_pairs.csv      read-only census: conflicts by (owner, claimant) pair
# Compare two snapshots with: python -m backend.scripts.ownership_diff <before> <after>
set -eu
cd "$(dirname "$0")/.."

if [ $# -ne 2 ]; then
  echo "usage: $0 <dsn-env-var-name> <label>" >&2
  exit 2
fi
envname="$1"
label="$2"
case "$envname" in *[!A-Za-z0-9_]*|"") echo "dsn-env must be a variable name" >&2; exit 2 ;; esac
case "$label" in *[!A-Za-z0-9_.-]*|"") echo "label may use letters, digits, . _ - only" >&2; exit 2 ;; esac
eval "dsn=\${$envname:-}"
if [ -z "$dsn" ]; then
  echo "$envname is not set" >&2
  exit 2
fi

stamp=$(date -u +%Y%m%dT%H%M%SZ)
dir="workspace/backups"
mkdir -p "$dir"
base="$dir/ownership_${label}_${stamp}"
export PGOPTIONS="${PGOPTIONS:-} -c default_transaction_read_only=on"

q() { psql "$dsn" -X -q -v ON_ERROR_STOP=1 "$@"; }

has() { [ "$(q -At -c "SELECT to_regclass('public.$1') IS NOT NULL")" = "t" ]; }

q -c "\\copy (SELECT id, vin, dealer_id, listing_active, scraped_at FROM cars ORDER BY id) TO '$base.csv' WITH (FORMAT csv, HEADER true)"

if has car_move_log; then
  q -c "\\copy (SELECT id, car_id, from_dealer_id, to_dealer_id, evidence, moved_at, reverted_at FROM car_move_log ORDER BY id) TO '${base}_moves.csv' WITH (FORMAT csv, HEADER true)"
else
  echo "id,car_id,from_dealer_id,to_dealer_id,evidence,moved_at,reverted_at" > "${base}_moves.csv"
fi

if has vin_owner_conflicts; then
  q -c "\\copy (SELECT vin, owner_dealer_id, claimant_dealer_id, seen_at FROM vin_owner_conflicts ORDER BY vin, owner_dealer_id, claimant_dealer_id) TO '${base}_conflicts.csv' WITH (FORMAT csv, HEADER true)"
  q -c "\\copy (SELECT owner_dealer_id, claimant_dealer_id, COUNT(*) AS rows, COUNT(DISTINCT vin) AS vins, MIN(seen_at) AS first_seen, MAX(seen_at) AS last_seen FROM vin_owner_conflicts GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2) TO '${base}_pairs.csv' WITH (FORMAT csv, HEADER true)"
else
  echo "vin,owner_dealer_id,claimant_dealer_id,seen_at" > "${base}_conflicts.csv"
  echo "owner_dealer_id,claimant_dealer_id,rows,vins,first_seen,last_seen" > "${base}_pairs.csv"
fi

echo "$base.csv"
wc -l "$base.csv" "${base}_moves.csv" "${base}_conflicts.csv" "${base}_pairs.csv" >&2
