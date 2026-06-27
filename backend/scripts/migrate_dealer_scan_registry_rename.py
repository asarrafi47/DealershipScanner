#!/usr/bin/env python3
"""Rename legacy ``dealer_catalog`` table to ``dealer_scan_registry``."""
from __future__ import annotations

import sys


def main() -> int:
    from backend.utils.kmac_vault import load_kmac_vault_secrets

    load_kmac_vault_secrets()
    from backend.db.inventory_pg import is_inventory_postgres, pg_connect
    from backend.scanner.job_queue import ensure_job_tables, migrate_dealer_catalog_to_scan_registry

    if not is_inventory_postgres():
        print("INVENTORY_DATABASE_URL must point at Postgres.", file=sys.stderr)
        return 1

    conn = pg_connect()
    try:
        cur = conn.cursor()
        renamed = migrate_dealer_catalog_to_scan_registry(cur)
        conn.commit()
        if renamed:
            print("Renamed dealer_catalog → dealer_scan_registry")
        else:
            print("No rename needed (dealer_scan_registry already present or legacy table missing)")
        ensure_job_tables(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
