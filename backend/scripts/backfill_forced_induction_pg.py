#!/usr/bin/env python3
"""
Backfill ``cars.forced_induction`` on Postgres from existing engine/description text.

The column is a classification cache (turbo/supercharged/NA) computed by
``classify_forced_induction_from_car_row``; the live scanner write path now populates it
going forward (see backend/scanner/database.py), but rows written before that change need
a one-time backfill.

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Usage:
  PYTHONPATH=. python backend/scripts/backfill_forced_induction_pg.py --dry-run
  PYTHONPATH=. python backend/scripts/backfill_forced_induction_pg.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_BATCH_SIZE = 500


def backfill_forced_induction(*, dry_run: bool = False) -> dict[str, int]:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect
    from backend.utils.forced_induction import classify_forced_induction_from_car_row

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    conn = pg_connect()
    scanned = 0
    classified = 0
    updated = 0
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, make, model, trim, year, cylinders, engine_l,
                   engine_description, fuel_type, description, title
            FROM cars
            WHERE forced_induction IS NULL
            """
        )
        rows = cur.fetchall()
        cols = ["id", "make", "model", "trim", "year", "cylinders", "engine_l",
                "engine_description", "fuel_type", "description", "title"]
        batch: list[tuple[str, int]] = []

        def flush(batch: list[tuple[str, int]]) -> None:
            nonlocal updated
            if not batch or dry_run:
                return
            upd_cur = conn.cursor()
            upd_cur.executemany(
                "UPDATE cars SET forced_induction = %s WHERE id = %s", batch
            )
            updated += len(batch)
            conn.commit()

        for row in rows:
            scanned += 1
            car = dict(zip(cols, row))
            fi = classify_forced_induction_from_car_row(car)
            if fi:
                classified += 1
                batch.append((fi, car["id"]))
            if len(batch) >= _BATCH_SIZE:
                flush(batch)
                logger.info("progress: scanned=%d classified=%d updated=%d", scanned, classified, updated)
                batch = []
        flush(batch)
    finally:
        conn.close()
    return {"scanned": scanned, "classified": classified, "updated": updated}


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill cars.forced_induction on Postgres.")
    parser.add_argument("--dry-run", action="store_true", help="Classify and count only; no writes.")
    args = parser.parse_args()
    stats = backfill_forced_induction(dry_run=bool(args.dry_run))
    logger.info(
        "%sscanned=%d classified=%d updated=%d",
        "dry-run: " if args.dry_run else "",
        stats["scanned"],
        stats["classified"],
        stats["updated"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
