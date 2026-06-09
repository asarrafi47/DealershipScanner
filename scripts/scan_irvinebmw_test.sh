#!/usr/bin/env bash
# Irvine BMW test scan — isolated 92694 manifest + test DB (never production inventory.db).
set -euo pipefail
cd "$(dirname "$0")/.."

# Force test-isolation values (do not inherit shell caps from partial scans).
export INVENTORY_DB_PATH="workspace/inventory_92694.db"
export DEALERS_MANIFEST_PATH="workspace/manifest_92694_25mi.json"
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

echo "INVENTORY_DB_PATH=$INVENTORY_DB_PATH"
echo "Log: $LOG"

exec python3 scanner.py --dealer-id irvinebmw-com --scan-only 2>&1 | tee "$LOG"
