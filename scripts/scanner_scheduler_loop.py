#!/usr/bin/env python3
"""Enqueue refresh jobs when dealer_scan_registry.next_scan_at is due."""
from __future__ import annotations

import logging
import os
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_log = logging.getLogger("scanner-scheduler")

INTERVAL_SEC = float(os.environ.get("SCANNER_SCHEDULER_INTERVAL_SEC", "60"))


def main() -> int:
    from backend.utils.kmac_vault import load_kmac_vault_secrets

    load_kmac_vault_secrets()
    from backend.db.inventory_db import init_inventory_db
    from backend.db.inventory_pg import is_inventory_postgres
    from backend.scanner.job_queue import init_job_queue_schema, schedule_due_refresh_jobs

    if not is_inventory_postgres():
        _log.error("INVENTORY_DATABASE_URL must point at Postgres for scanner-scheduler.")
        return 1

    init_inventory_db()
    init_job_queue_schema()
    _log.info("Scanner scheduler started (interval=%ss)", INTERVAL_SEC)

    while True:
        try:
            n = schedule_due_refresh_jobs()
            if n:
                _log.info("Enqueued %d refresh job(s)", n)
        except Exception:
            _log.exception("Scheduler tick failed")
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
