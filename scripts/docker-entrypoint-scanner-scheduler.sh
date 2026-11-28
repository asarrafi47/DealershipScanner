#!/bin/sh
set -eu
export KMAC_VAULT_AUTO="${KMAC_VAULT_AUTO:-1}"
export PYTHONPATH="${PYTHONPATH:-/app}"

# Role login configured: skip this throwaway check (it would spend the boot wait budget
# once more); the loop script's own load_kmac_vault_secrets() runs the role load.
if [ -n "${VAULT_ROLE_ID:-}" ]; then
  :
elif [ "${KMAC_VAULT_AUTO}" != "0" ]; then
  python3 -c "from backend.utils.kmac_vault import load_kmac_vault_secrets; load_kmac_vault_secrets()" \
    || echo "WARN: kmac vault preload skipped" >&2
fi

exec python3 scripts/scanner_scheduler_loop.py
