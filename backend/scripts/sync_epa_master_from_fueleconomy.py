#!/usr/bin/env python3
"""
Fill ``epa_master`` gaps from official EPA FuelEconomy.gov data.

Phase 1 (default): download ``vehicles.csv`` and match/update existing rows.
Phase 2 (``--api-fallback``): REST menu/options lookup for rows still missing
``epa_vehicle_id`` after bulk match.

Usage:
  PYTHONPATH=. python -m backend.scripts.sync_epa_master_from_fueleconomy
  PYTHONPATH=. python -m backend.scripts.sync_epa_master_from_fueleconomy --dry-run
  PYTHONPATH=. python -m backend.scripts.sync_epa_master_from_fueleconomy --api-fallback --api-limit 200
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.enrichment.fueleconomy_catalog import (
    best_fueleconomy_match,
    build_fueleconomy_index,
    download_vehicles_csv,
    load_fueleconomy_records,
    patch_from_fueleconomy,
    resolve_fueleconomy_via_api,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sync_epa_master_from_fueleconomy")

_PATCHABLE_COLS = (
    "epa_vehicle_id",
    "atv_type",
    "city_e",
    "highway_e",
    "city08",
    "highway08",
    "cylinders",
    "displacement",
    "trany",
    "drive",
    "fuel_type",
    "body_style",
)


def _row_dict(row: tuple | dict, cols: list[str]) -> dict:
    if isinstance(row, dict):
        return dict(row)
    return dict(zip(cols, row))


def _needs_fueleconomy_patch(row: dict) -> bool:
    if row.get("epa_vehicle_id") is None:
        return True
    if not (row.get("atv_type") or "").strip():
        fuel = (row.get("fuel_type") or "").lower()
        if "electric" in fuel or "hybrid" in fuel or "hydrogen" in fuel:
            return True
    if row.get("city_e") is None or row.get("highway_e") is None:
        fuel = (row.get("fuel_type") or "").lower()
        if "electric" in fuel:
            return True
    for col in ("city08", "highway08", "cylinders", "displacement", "trany", "drive", "fuel_type", "body_style"):
        if row.get(col) is None or (isinstance(row.get(col), str) and not str(row.get(col)).strip()):
            return True
    return False


def _apply_patch(cur, row_id: int, patch: dict[str, object]) -> None:
    if not patch:
        return
    sets = ", ".join(f"{col} = %s" for col in patch)
    vals = list(patch.values()) + [row_id]
    cur.execute(f"UPDATE epa_master SET {sets} WHERE id = %s", vals)


def sync_from_csv(
    *,
    csv_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    path = csv_path or download_vehicles_csv()
    records = load_fueleconomy_records(path)
    index = build_fueleconomy_index(records)
    if not csv_path:
        try:
            os.unlink(path)
        except OSError:
            pass

    from backend.db.inventory_db import get_conn

    stats = {
        "examined": 0,
        "matched": 0,
        "updated": 0,
        "epa_vehicle_id": 0,
        "atv_type": 0,
        "city_e": 0,
        "highway_e": 0,
        "unmatched": 0,
    }
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, year, make, model, trim, trany, drive, fuel_type,
                   cylinders, displacement, city08, highway08, city_e, highway_e,
                   atv_type, body_style, epa_vehicle_id
            FROM epa_master
            ORDER BY id
            """
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        for raw in rows:
            row = _row_dict(raw, cols)
            stats["examined"] += 1
            if not _needs_fueleconomy_patch(row):
                continue
            fe = best_fueleconomy_match(row, index)
            if not fe:
                stats["unmatched"] += 1
                continue
            stats["matched"] += 1
            patch = patch_from_fueleconomy(row, fe)
            if not patch:
                continue
            stats["updated"] += 1
            for k in patch:
                if k in stats:
                    stats[k] += 1
            if not dry_run:
                _apply_patch(cur, int(row["id"]), patch)
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return stats


def sync_api_fallback(*, limit: int = 200, dry_run: bool = False) -> dict[str, int]:
    from backend.db.inventory_db import get_conn

    stats = {"examined": 0, "resolved": 0, "updated": 0}
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, year, make, model, trim, trany, drive, fuel_type,
                   cylinders, displacement, city08, highway08, city_e, highway_e,
                   atv_type, body_style, epa_vehicle_id
            FROM epa_master
            WHERE epa_vehicle_id IS NULL
            ORDER BY year DESC, id
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )
        cols = [d[0] for d in cur.description]
        for raw in cur.fetchall():
            row = _row_dict(raw, cols)
            stats["examined"] += 1
            fe = resolve_fueleconomy_via_api(
                year=int(row["year"]),
                make=str(row["make"]),
                model=str(row["model"]),
                trim=row.get("trim"),
                trany=row.get("trany"),
                drive=row.get("drive"),
            )
            if not fe:
                continue
            stats["resolved"] += 1
            patch = patch_from_fueleconomy(row, fe)
            if patch and not dry_run:
                _apply_patch(cur, int(row["id"]), patch)
                stats["updated"] += 1
            elif patch:
                stats["updated"] += 1
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return stats


def report_nulls() -> dict[str, int]:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS total,
              COUNT(*) FILTER (WHERE epa_vehicle_id IS NULL) AS null_epa_id,
              COUNT(*) FILTER (WHERE atv_type IS NULL OR trim(atv_type) = '') AS null_atv,
              COUNT(*) FILTER (WHERE city_e IS NULL) AS null_city_e,
              COUNT(*) FILTER (WHERE highway_e IS NULL) AS null_hwy_e
            FROM epa_master
            """
        )
        row = cur.fetchone()
        if isinstance(row, dict):
            return dict(row)
        keys = ["total", "null_epa_id", "null_atv", "null_city_e", "null_hwy_e"]
        return dict(zip(keys, row))
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--csv-path", help="Use a local vehicles.csv instead of downloading")
    parser.add_argument("--api-fallback", action="store_true", help="REST lookup for unmatched rows")
    parser.add_argument("--api-limit", type=int, default=200, help="Max rows for API fallback")
    args = parser.parse_args(argv)

    before = report_nulls()
    log.info(
        "Before: total=%s null_epa_id=%s null_atv=%s null_city_e=%s null_hwy_e=%s",
        before["total"],
        before["null_epa_id"],
        before["null_atv"],
        before["null_city_e"],
        before["null_hwy_e"],
    )

    csv_stats = sync_from_csv(csv_path=args.csv_path, dry_run=args.dry_run)
    log.info(
        "CSV sync: examined=%d matched=%d updated=%d unmatched=%d "
        "(epa_vehicle_id=%d atv_type=%d city_e=%d highway_e=%d)",
        csv_stats["examined"],
        csv_stats["matched"],
        csv_stats["updated"],
        csv_stats["unmatched"],
        csv_stats["epa_vehicle_id"],
        csv_stats["atv_type"],
        csv_stats["city_e"],
        csv_stats["highway_e"],
    )

    if args.api_fallback:
        api_stats = sync_api_fallback(limit=args.api_limit, dry_run=args.dry_run)
        log.info(
            "API fallback: examined=%d resolved=%d updated=%d",
            api_stats["examined"],
            api_stats["resolved"],
            api_stats["updated"],
        )

    if not args.dry_run:
        after = report_nulls()
        log.info(
            "After: null_epa_id=%s null_atv=%s null_city_e=%s null_hwy_e=%s",
            after["null_epa_id"],
            after["null_atv"],
            after["null_city_e"],
            after["null_hwy_e"],
        )


if __name__ == "__main__":
    main()
