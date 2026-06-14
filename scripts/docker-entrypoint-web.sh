#!/bin/sh
# Web container entrypoint: optional vault preload, then gunicorn on Railway/Docker PORT.
set -eu

export KMAC_VAULT_AUTO="${KMAC_VAULT_AUTO:-1}"
export PYTHONPATH="${PYTHONPATH:-/app}"

if [ "${KMAC_VAULT_AUTO}" != "0" ]; then
  python3 -c "from backend.utils.kmac_vault import load_kmac_vault_secrets; load_kmac_vault_secrets()" \
    || echo "WARN: kmac vault preload skipped (vault unreachable or token missing)" >&2
fi

python3 scripts/bootstrap_site_admin.py \
  || echo "WARN: site admin bootstrap skipped" >&2

PORT="${PORT:-8000}"
GUNICORN_RELOAD=""
if [ "${FLASK_ENV:-}" = "development" ]; then
  # poll: reliable reload when /app is a Docker bind mount (inotify misses host edits on macOS)
  GUNICORN_RELOAD="--reload --reload-engine poll"
fi
exec gunicorn -w 1 --threads 4 -b "0.0.0.0:${PORT}" \
  --timeout 120 --graceful-timeout 30 \
  ${GUNICORN_RELOAD} \
  backend.main:app
