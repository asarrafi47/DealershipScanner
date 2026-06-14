#!/bin/sh
set -eu
export KMAC_VAULT_AUTO="${KMAC_VAULT_AUTO:-1}"
export PYTHONPATH="${PYTHONPATH:-/app}"

if [ "${KMAC_VAULT_AUTO}" != "0" ]; then
  python3 -c "from backend.utils.kmac_vault import load_kmac_vault_secrets; load_kmac_vault_secrets()" \
    || echo "WARN: kmac vault preload skipped" >&2
fi

exec python3 scripts/scanner_scheduler_loop.py
