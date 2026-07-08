#!/usr/bin/env python3
"""
Backfill ``cars.mpg_city``/``cars.mpg_highway`` on Postgres from ``epa_master``.

Matches each car missing mpg data against ``epa_master`` on
``(year, make, model, trim)`` first, falling back to ``(year, make, model)``
(averaging across trims) when no trim-level match exists. Never overwrites a
car that already has real scraped mpg values — only fills genuine gaps,
matching this project's established "never clobber real data with a derived
value" convention (see the ``COALESCE(excluded.x, cars.x)`` pattern throughout
``backend/scanner/database.py``'s upsert).

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://), and a
populated ``epa_master`` (see build_epa_master_pg.py).

Usage:
  PYTHONPATH=. python backend/scripts/backfill_mpg_from_epa.py --dry-run
  PYTHONPATH=. python backend/scripts/backfill_mpg_from_epa.py
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
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


def _load_epa_lookup(cur) -> tuple[dict, dict]:
    """(year, make, model, trim) -> (city, hwy) exact map, and (year, make, model) -> averaged map."""
    cur.execute(
        """
        SELECT year, lower(make), lower(model), lower(coalesce(trim, '')), city08, highway08
        FROM epa_master
        WHERE city08 IS NOT NULL AND highway08 IS NOT NULL
        """
    )
    by_trim: dict[tuple, tuple[float, float]] = {}
    by_model_vals: dict[tuple, list[tuple[float, float]]] = defaultdict(list)
    for year, make, model, trim, city, hwy in cur.fetchall():
        if trim:
            by_trim.setdefault((year, make, model, trim), (city, hwy))
        by_model_vals[(year, make, model)].append((city, hwy))

    by_model: dict[tuple, tuple[float, float]] = {}
    for key, vals in by_model_vals.items():
        by_model[key] = (
            sum(v[0] for v in vals) / len(vals),
            sum(v[1] for v in vals) / len(vals),
        )
    return by_trim, by_model


def backfill_mpg_from_epa(*, dry_run: bool = False) -> dict[str, int]:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    conn = pg_connect()
    scanned = 0
    matched_trim = 0
    matched_model = 0
    unmatched = 0
    updated = 0
    try:
        cur = conn.cursor()
        by_trim, by_model = _load_epa_lookup(cur)
        logger.info("epa_master lookup: %d trim-level, %d model-level keys", len(by_trim), len(by_model))

        cur.execute(
            """
            SELECT id, year, make, model, trim
            FROM cars
            WHERE (mpg_city IS NULL OR mpg_highway IS NULL)
              AND year IS NOT NULL AND make IS NOT NULL AND model IS NOT NULL
            """
        )
        rows = cur.fetchall()
        batch: list[tuple] = []

        def flush(batch: list[tuple]) -> None:
            nonlocal updated
            if not batch or dry_run:
                return
            upd_cur = conn.cursor()
            upd_cur.executemany(
                "UPDATE cars SET mpg_city = %s, mpg_highway = %s WHERE id = %s",
                batch,
            )
            updated += len(batch)
            conn.commit()

        for car_id, year, make, model, trim in rows:
            scanned += 1
            mk = (make or "").strip().lower()
            md = (model or "").strip().lower()
            tr = (trim or "").strip().lower()

            hit = by_trim.get((year, mk, md, tr)) if tr else None
            if hit:
                matched_trim += 1
            else:
                hit = by_model.get((year, mk, md))
                if hit:
                    matched_model += 1
                else:
                    unmatched += 1
                    continue

            city, hwy = hit
            batch.append((round(city, 1), round(hwy, 1), car_id))
            if len(batch) >= _BATCH_SIZE:
                flush(batch)
                logger.info(
                    "progress: scanned=%d matched_trim=%d matched_model=%d unmatched=%d updated=%d",
                    scanned, matched_trim, matched_model, unmatched, updated,
                )
                batch = []
        flush(batch)
    finally:
        conn.close()

    return {
        "scanned": scanned,
        "matched_trim": matched_trim,
        "matched_model": matched_model,
        "unmatched": unmatched,
        "updated": updated,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill cars.mpg_city/mpg_highway from epa_master on Postgres.")
    parser.add_argument("--dry-run", action="store_true", help="Match and count only; no writes.")
    args = parser.parse_args()
    stats = backfill_mpg_from_epa(dry_run=bool(args.dry_run))
    logger.info(
        "%sscanned=%d matched_trim=%d matched_model=%d unmatched=%d updated=%d",
        "dry-run: " if args.dry_run else "",
        stats["scanned"], stats["matched_trim"], stats["matched_model"],
        stats["unmatched"], stats["updated"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
