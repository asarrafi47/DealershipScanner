#!/bin/sh
# Launch a P0B.1 harness script with the local DSN (never printed) and a read-only DB session.
INVENTORY_DATABASE_URL=$(grep '^INVENTORY_DATABASE_URL=' /Users/asarrafi/Projects/DealershipScanner/.env | head -1 | cut -d= -f2- | tr -d '"'"'")
export INVENTORY_DATABASE_URL
export PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000'
cd /private/tmp/claude-501/phase0/p0b1_run
exec /Users/asarrafi/Projects/DealershipScanner/.venv/bin/python "$@"
