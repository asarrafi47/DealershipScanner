#!/usr/bin/env bash
# Copy inventory data from local Docker Postgres to Railway Postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOCAL_CONTAINER="${LOCAL_PG_CONTAINER:-dealership-scanner-postgres}"
LOCAL_USER="${LOCAL_PG_USER:-dealership}"
LOCAL_DB="${LOCAL_PG_DB:-dealership}"

if ! command -v railway >/dev/null 2>&1; then
  echo "Install Railway CLI" >&2
  exit 1
fi

if [[ -z "${INVENTORY_DATABASE_URL:-}" ]]; then
  INVENTORY_DATABASE_URL="$(railway variable list -s Postgres --kv 2>/dev/null | grep '^DATABASE_PUBLIC_URL=' | cut -d= -f2- || true)"
fi
if [[ -z "${INVENTORY_DATABASE_URL:-}" ]]; then
  echo "Set INVENTORY_DATABASE_URL to Railway Postgres public URL" >&2
  exit 1
fi

TABLES=(
  dealerships
  cars
  dealer_scan_registry
  dealer_jobs
  dealer_scan_profile
  dealer_geopoints
  catalog_trims
  catalog_packages
  catalog_package_features
  catalog_options
  catalog_exterior_colors
  catalog_interior_colors
  epa_master
  model_specs
  scan_runs
  saved_cars
  nhtsa_vpic_cache
)

DUMP="/tmp/dealership-inv-data.sql"
echo "==> Dumping local Postgres (${LOCAL_CONTAINER})"
docker exec "$LOCAL_CONTAINER" pg_dump -U "$LOCAL_USER" -d "$LOCAL_DB" \
  --no-owner --no-acl --data-only --inserts \
  $(printf -- '-t %s ' "${TABLES[@]}") > "$DUMP"

echo "==> Restoring to Railway Postgres"
psql "$INVENTORY_DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null
{
  echo "SET session_replication_role = replica;"
  cat "$DUMP"
  echo "SET session_replication_role = DEFAULT;"
} | psql "$INVENTORY_DATABASE_URL" -v ON_ERROR_STOP=0 2>&1 | tail -30

echo "==> Row counts on Railway"
psql "$INVENTORY_DATABASE_URL" -t -c "SELECT 'cars', COUNT(*) FROM cars UNION ALL SELECT 'dealerships', COUNT(*) FROM dealerships UNION ALL SELECT 'dealer_scan_registry', COUNT(*) FROM dealer_scan_registry UNION ALL SELECT 'dealer_geopoints', COUNT(*) FROM dealer_geopoints;"
