#!/bin/sh
set -eu
export KMAC_VAULT_AUTO="${KMAC_VAULT_AUTO:-1}"
export PYTHONPATH="${PYTHONPATH:-/app}"
export SCANNER_WORKER_ID="${SCANNER_WORKER_ID:-worker-${HOSTNAME:-local}}"

if [ "${KMAC_VAULT_AUTO}" != "0" ]; then
  python3 -c "from backend.utils.kmac_vault import load_kmac_vault_secrets; load_kmac_vault_secrets()" \
    || echo "WARN: kmac vault preload skipped" >&2
fi

# Bind mount ..:/app hides image-built node_modules — ensure scanner.js deps exist.
if [ ! -f /app/backend/scanner/node_modules/fs-extra/package.json ]; then
  echo "Installing backend/scanner npm dependencies (first run or empty node_modules)..." >&2
  cd /app/backend/scanner && npm ci --omit=dev
fi

PW_CHROME="$(find /root/.cache/ms-playwright -path '*/chrome-linux/chrome' -type f 2>/dev/null | sort | tail -1)"
if [ -n "$PW_CHROME" ] && [ -z "${PUPPETEER_EXECUTABLE_PATH:-}" ]; then
  export PUPPETEER_EXECUTABLE_PATH="$PW_CHROME"
  echo "Using Playwright Chromium: $PUPPETEER_EXECUTABLE_PATH" >&2
fi

exec python3 scripts/scanner_worker_loop.py
