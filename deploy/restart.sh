#!/usr/bin/env bash
# Recreate the web container with vault secrets (no image rebuild).
# Use after docker restarts or when dealer search says "no API key".
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/deploy/load-vault-env.sh"

cd "$ROOT/deploy"
export WEB_PORT="${WEB_PORT:-18000}"

echo "==> Recreating dealership-scanner-web on port ${WEB_PORT}"
docker compose up -d web

echo "==> Ready: http://127.0.0.1:${WEB_PORT}"
