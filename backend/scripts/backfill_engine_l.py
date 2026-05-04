"""
Backfill the ``cars.engine_l`` column from ``engine_description``, cylinders, and fuel_type.

Run after upgrading upsert to persist ``engine_l``; fills older rows that only have
dealer text in ``engine_description`` but an empty ``engine_l``.

Usage:
    python scripts/backfill_engine_l.py [--dry-run] [--db PATH] [--recompute-scores]

By default, only sets ``engine_l`` (one transaction). Use ``--recompute-scores`` to refresh
``data_quality_score`` and incomplete-listing index per row (much slower for large DBs).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.db.inventory_db import get_conn, refresh_car_data_quality_score, ensure_cars_table_columns
from backend.utils.car_serialize import infer_engine_l_for_db
from backend.utils.field_clean import is_effectively_empty

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")


def _row_needs_engine_l(engine_l) -> bool:
    if engine_l is None or is_effectively_empty(engine_l):
        return True
    s = str(engine_l).strip().lower()
    if s in ("0", "0.0", "---"):
        return True
    return False


def backfill(
    *, db_path: str, dry_run: bool, recompute_scores: bool
) -> dict[str, int | list[tuple[int, str, str, str]]]:
    if db_path != DB_PATH:
        import backend.db.inventory_db as _inv

        _inv.DB_PATH = db_path
    conn = get_conn()
    try:
        try:
            conn.execute("PRAGMA busy_timeout=120000")
        except (sqlite3.Error, OSError):
            pass
        cur = conn.cursor()
        ensure_cars_table_columns(cur)
        conn.commit()
        cur.execute(
            """
            SELECT id, vin, engine_l, engine_description, cylinders, fuel_type
            FROM cars
            """
        )
        cols = [d[0] for d in cur.description]
        updated = 0
        samples: list[tuple[int, str, str, str]] = []
        touched_ids: list[int] = []
        for row in cur.fetchall():
            car = dict(zip(cols, row))
            if not _row_needs_engine_l(car.get("engine_l")):
                continue
            inferred = infer_engine_l_for_db(car)
            if inferred is None:
                continue
            cid = int(car["id"])
            if not dry_run:
                cur.execute("UPDATE cars SET engine_l = ? WHERE id = ?", (inferred, cid))
                if recompute_scores:
                    touched_ids.append(cid)
            updated += 1
            if len(samples) < 30:
                samples.append((cid, str(car.get("vin") or ""), str(car.get("engine_l") or ""), inferred))
        if not dry_run:
            conn.commit()
        if recompute_scores and not dry_run and touched_ids:
            for cid in touched_ids:
                try:
                    refresh_car_data_quality_score(cid)
                except Exception:
                    pass
    finally:
        conn.close()
    return {"updated": updated, "samples": samples}


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill cars.engine_l from engine_description and fuel/cylinders")
    p.add_argument("--dry-run", action="store_true", help="Preview without writing")
    p.add_argument("--db", default=DB_PATH, help="Path to inventory.db")
    p.add_argument(
        "--recompute-scores",
        action="store_true",
        help="Refresh data_quality_score and incomplete listings for each updated row (slow).",
    )
    args = p.parse_args()
    r = backfill(db_path=args.db, dry_run=args.dry_run, recompute_scores=args.recompute_scores)
    label = "[DRY RUN] " if args.dry_run else ""
    print(f"{label}Updated engine_l for {r['updated']} row(s).")
    for cid, vin, before, after in r["samples"]:
        print(f"  id={cid} vin={vin!r} {before!r} -> {after!r}")


if __name__ == "__main__":
    main()
