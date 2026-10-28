#!/usr/bin/env bash
# Irvine BMW test scan — isolated 92694 manifest + scratch Postgres cars_lab (never the real inventory).
set -euo pipefail
cd "$(dirname "$0")/.."

# The inventory layer is Postgres-only (INVENTORY_DB_PATH is ignored), so an
# inherited DSN would upsert into the real database. Drop it; scanner.py loads
# .env itself, so refuse unless a scratch DSN is given explicitly.
unset DATABASE_URL INVENTORY_DATABASE_URL
LAB_DSN="${SCAN_LAB_DATABASE_URL:-${SCAN_TEST_DATABASE_URL:-}}"
if [ -z "$LAB_DSN" ]; then
  echo "Refusing to run: set SCAN_LAB_DATABASE_URL to the scratch Postgres cars_lab (never the real inventory)." >&2
  exit 2
fi
case "$LAB_DSN" in
  */cars|*/cars\?*|*/cars_cleanroom|*/cars_cleanroom\?*|*/cars_cr_*)
    echo "Refusing to run: $LAB_DSN names a real inventory database." >&2
    exit 2 ;;
esac
export INVENTORY_DATABASE_URL="$LAB_DSN"

# Force test-isolation values (do not inherit shell caps from partial scans).
export DEALERS_MANIFEST_PATH="backend/dev/fixtures/manifest_92694_25mi.json"
export SCANNER_DEALER_COM_BULK_FETCH=1
export SCANNER_VDP_COMPLETENESS_PASS=1
export SCANNER_VDP_COMPLETENESS_MAX=529
export SCANNER_VDP_EP_MAX=529
export SCANNER_VDP_DESCRIPTION_MAX=529
export SCANNER_VDP_PRICE_MAX=0
export SCANNER_MAX_VDP_CONCURRENCY=2
export SCANNER_VDP_GALLERY_MAX_SEC_BMW=300
export SCANNER_CLAUDE_VDP=0
export SCANNER_SCAN_ONLY=1

LOG_DIR="workspace/scan_lab_logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/irvinebmw_92694_$(date +%Y%m%d_%H%M%S).log"

echo "Lab database: ${LAB_DSN##*/}"
echo "Log: $LOG"

exec python3 scanner.py --dealer-id irvinebmw-com --scan-only 2>&1 | tee "$LOG"
