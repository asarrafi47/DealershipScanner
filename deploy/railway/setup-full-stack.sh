#!/usr/bin/env bash
# Provision DealershipScanner on Railway: web + Postgres + scanner-worker + scanner-scheduler.
# Run from repo root after: railway login && railway link -p dealership-scanner
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

REPO="${RAILWAY_REPO:-asarrafi47/DealershipScanner}"
BRANCH="${RAILWAY_BRANCH:-main}"

if ! command -v railway >/dev/null 2>&1; then
  echo "Install Railway CLI: https://docs.railway.com/guides/cli" >&2
  exit 1
fi

_ensure_service() {
  local name="$1"
  if railway service list 2>/dev/null | rg -q "^${name}[[:space:]]"; then
    echo "==> Service exists: ${name}"
    return 0
  fi
  echo "==> Creating service: ${name}"
  railway add --repo "$REPO" --branch "$BRANCH" --service "$name" --json >/dev/null
}

_wire_scanner_service() {
  local name="$1"
  local config_relpath="$2"
  local dockerfile="$3"

  echo "==> Wiring ${name}"
  railway variable set "RAILWAY_DOCKERFILE_PATH=${dockerfile}" -s "$name" --skip-deploys
  railway variable set "INVENTORY_DATABASE_URL=\${{Postgres.DATABASE_URL}}" -s "$name" --skip-deploys
  railway variable set "KMAC_VAULT_AUTO=1" -s "$name" --skip-deploys
  railway variable set "VAULT_ADDR=http://kmac-vault.railway.internal:9999" -s "$name" --skip-deploys
  railway variable set "VAULT_LOAD_RETRIES=8" -s "$name" --skip-deploys
  railway variable set "PYTHONPATH=/app" -s "$name" --skip-deploys
  railway variable set "INVENTORY_DB_PATH=/data/inventory.db" -s "$name" --skip-deploys
  railway variable set "SCANNER_BROWSER_PROFILE_DIR=/data/browser-profiles" -s "$name" --skip-deploys
  railway variable set "SCANNER_VDP_IMAGE_DOWNLOAD_DIR=/data/vdp_images" -s "$name" --skip-deploys
  railway variable set "SCANNER_PARALLEL_UPSERT=1" -s "$name" --skip-deploys
  railway variable set "SCANNER_JOB_TIMEOUT_SEC=3600" -s "$name" --skip-deploys

  if [[ "$name" == "scanner-worker" ]]; then
    railway variable set "SCANNER_WORKER_POLL_SEC=10" -s "$name" --skip-deploys
    railway variable set "SCANNER_MAX_DEALER_CONCURRENCY=1" -s "$name" --skip-deploys
  else
    railway variable set "SCANNER_SCHEDULER_INTERVAL_SEC=60" -s "$name" --skip-deploys
  fi

  if [[ -n "${RAILWAY_VAULT_TOKEN:-}" ]]; then
    railway variable set "VAULT_TOKEN=${RAILWAY_VAULT_TOKEN}" -s "$name" --skip-deploys
  elif [[ -f "${HOME}/railway-vault-token.txt" ]]; then
    railway variable set "VAULT_TOKEN=$(tr -d '[:space:]' < "${HOME}/railway-vault-token.txt")" -s "$name" --skip-deploys
  fi

  echo "    Config file (set in dashboard if not auto-detected): ${config_relpath}"
  echo "    Attach a Volume at /data on ${name} (browser profiles + scanner SQLite scratch)."
  echo "    Recommended memory: 2 GB+ for scanner-worker."
}

echo "==> DealershipScanner Railway full stack"
_ensure_service "scanner-worker"
_ensure_service "scanner-scheduler"

_wire_scanner_service "scanner-worker" "deploy/railway/railway.scanner-worker.toml" "Dockerfile.scanner-worker"
_wire_scanner_service "scanner-scheduler" "deploy/railway/railway.scanner-scheduler.toml" "Dockerfile.scanner-scheduler"

echo ""
echo "==> Web service (if not done):"
echo "    INVENTORY_DATABASE_URL=\${{Postgres.DATABASE_URL}}"
echo "    Config: railway.toml (Dockerfile.web)"
echo "    Volume: /data (users.db, vdp_images)"
echo ""
echo "==> Next:"
echo "    1. Dashboard -> each scanner service -> Settings -> Config file path (see above)"
echo "    2. Dashboard -> scanner-worker -> add Volume mount /data"
echo "    3. Redeploy: railway redeploy -s scanner-worker -y && railway redeploy -s scanner-scheduler -y"
echo "    4. Migrate inventory: run migrate script against Railway Postgres PUBLIC_URL"
