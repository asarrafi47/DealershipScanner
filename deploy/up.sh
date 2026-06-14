#!/usr/bin/env bash
# Build and start DealershipScanner locally in Docker (Postgres inventory required).
#
#   ./deploy/up.sh
#   WEB_PORT=8000 ./deploy/up.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/deploy"

export WEB_PORT="${WEB_PORT:-18000}"
export WORKER_REPLICAS="${WORKER_REPLICAS:-2}"
export INVENTORY_DATABASE_URL="postgresql://dealership:dealership@postgres:5432/dealership"
export COMPOSE_PROFILES=full

# shellcheck source=/dev/null
source "$ROOT/deploy/load-vault-env.sh"

if [[ -n "${GOOGLE_MAPS_API_KEY:-}" ]]; then
  echo "==> Loaded GOOGLE_MAPS_API_KEY from kmac vault"
fi
if [[ -n "${STRIPE_SECRET_KEY:-}" ]]; then
  echo "==> Loaded STRIPE_SECRET_KEY from kmac vault"
fi

cat > "$ROOT/deploy/.env" <<EOF
INVENTORY_DATABASE_URL=${INVENTORY_DATABASE_URL}
COMPOSE_PROFILES=full
WEB_PORT=${WEB_PORT}
WORKER_REPLICAS=${WORKER_REPLICAS}
EOF

echo "==> Stack: postgres + web + ${WORKER_REPLICAS} scanner-workers + scheduler"
docker compose -f docker-compose.yml build web scanner-worker scanner-scheduler
docker compose -f docker-compose.yml up -d postgres
echo "==> Waiting for Postgres"
for i in $(seq 1 40); do
  if docker compose -f docker-compose.yml exec -T postgres pg_isready -U dealership -d dealership >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker compose -f docker-compose.yml up -d web scanner-scheduler
docker compose -f docker-compose.yml up -d --scale "scanner-worker=${WORKER_REPLICAS}" scanner-worker

echo "==> Waiting for /health"
for i in $(seq 1 45); do
  if curl -sf --max-time 2 "http://127.0.0.1:${WEB_PORT}/health" >/dev/null 2>&1; then
    echo "==> Initializing Postgres inventory schema"
    docker compose -f docker-compose.yml exec -T web python3 -c \
      "from backend.db.inventory_db import init_inventory_db; from backend.scanner.job_queue import init_job_queue_schema; init_inventory_db(); init_job_queue_schema(); print('ok')"
    echo "==> Ready: http://127.0.0.1:${WEB_PORT}"
    docker compose -f docker-compose.yml ps
    exit 0
  fi
  sleep 1
done

echo "ERROR: health check failed on port ${WEB_PORT}" >&2
docker logs dealership-scanner-web 2>&1 | tail -40
exit 1
