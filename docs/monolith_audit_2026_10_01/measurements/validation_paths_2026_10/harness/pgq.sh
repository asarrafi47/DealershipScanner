#!/bin/sh
# usage: pgq.sh "SQL"  -- local Postgres, read-only session, DSN never printed
DSN=$(grep '^INVENTORY_DATABASE_URL=' /Users/asarrafi/Projects/DealershipScanner/.env | head -1 | cut -d= -f2- | tr -d '"'"'")
PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000' psql "$DSN" -X -A -t -F '|' -c "$1"
