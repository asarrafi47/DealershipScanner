#!/usr/bin/env bash
# Build and start DealershipScanner locally in Docker.
# Usage (from repo root):
#   ./deploy/up.sh              # port 18000
#   WEB_PORT=8000 ./deploy/up.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/deploy"

export WEB_PORT="${WEB_PORT:-18000}"

# shellcheck source=/dev/null
source "$ROOT/deploy/load-vault-env.sh"

if [[ -n "${GOOGLE_MAPS_API_KEY:-}" ]]; then
  echo "==> Loaded GOOGLE_MAPS_API_KEY from kmac vault (Dealer:google_maps_api_key)"
fi
if [[ -n "${ADMIN_PASSWORD:-}" ]]; then
  echo "==> Loaded ADMIN_PASSWORD from kmac vault (Dealer:site_admin_password)"
fi

echo "==> Building dealership-scanner web image"
docker compose -f docker-compose.yml build web

echo "==> Starting dealership-scanner-web on port ${WEB_PORT}"
docker compose -f docker-compose.yml up -d web

echo "==> Waiting for /health"
for i in $(seq 1 30); do
  if curl -sf --max-time 2 "http://127.0.0.1:${WEB_PORT}/health" >/dev/null 2>&1; then
    echo "==> Bootstrapping site admin in data/runtime/users.db"
    (
      cd "$ROOT"
      export PYTHONPATH="$ROOT"
      export USERS_DB_PATH="$ROOT/data/runtime/users.db"
      export DEV_USERS_DB_PATH="$ROOT/data/runtime/dev_users.db"
      export ALLOW_UNENCRYPTED_USER_DB=1
      export APP_ADMIN_USERNAMES APP_ADMIN_EMAILS ADMIN_PASSWORD
      python3 scripts/bootstrap_site_admin.py
    )
    echo "==> Ready: http://127.0.0.1:${WEB_PORT}"
    echo "    Site admin login: /login  (user: ${APP_ADMIN_USERNAMES%%,*})"
    echo "    Dev tools login:  /dev/login  (user: ${ADMIN_USERNAME})"
    echo "    Password: kmac vault key Dealer:site_admin_password"
    docker compose -f docker-compose.yml ps
    exit 0
  fi
  sleep 1
done

echo "ERROR: health check failed on port ${WEB_PORT}" >&2
docker logs dealership-scanner-web 2>&1 | tail -30
exit 1
