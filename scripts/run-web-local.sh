#!/usr/bin/env bash
# Run gunicorn on the host against Docker Postgres (port 5432).
#
# Prerequisite: ./deploy/up.sh (or at least postgres container healthy).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export PYTHONPATH="$ROOT"
export ALLOW_UNENCRYPTED_USER_DB=1
export USERS_DB_PATH="$ROOT/data/runtime/users.db"
export DEV_USERS_DB_PATH="$ROOT/data/runtime/dev_users.db"
export INVENTORY_DATABASE_URL="${INVENTORY_DATABASE_URL:-postgresql://dealership:dealership@127.0.0.1:5432/dealership}"
export FLASK_ENV=development
PORT="${PORT:-8000}"

if ! pg_isready -h 127.0.0.1 -p 5432 -U dealership -d dealership >/dev/null 2>&1; then
  echo "ERROR: Postgres not ready on 127.0.0.1:5432. Run ./deploy/up.sh first." >&2
  exit 1
fi

exec "$ROOT/.venv/bin/gunicorn" -w 1 --threads 4 -b "0.0.0.0:${PORT}" \
  --timeout 120 backend.main:app
