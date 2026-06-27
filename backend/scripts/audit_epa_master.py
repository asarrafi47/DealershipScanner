#!/usr/bin/env python3
"""
Audit and repair ``epa_master`` completeness against fueleconomy.gov.

Repairs (``--repair``):
  1. Re-sync from official ``vehicles.csv`` (match + patch nulls)
  2. Backfill any row with ``epa_vehicle_id`` from the EPA id index
  3. Infer ``atv_type`` from ``fuel_type`` where EPA has no alt-fuel label
  4. Recompute ``engine_display`` / ``forced_induction``

Usage:
  PYTHONPATH=. python -m backend.scripts.audit_epa_master
  PYTHONPATH=. python -m backend.scripts.audit_epa_master --repair
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("audit_epa_master")

_SELECT = """
    SELECT id, year, make, model, trim, trany, drive, fuel_type,
           cylinders, displacement, city08, highway08, city_e, highway_e,
           atv_type, body_style, epa_vehicle_id, engine_display, forced_induction
    FROM epa_master
"""


def _row_dict(row: tuple | dict, cols: list[str]) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    return dict(zip(cols, row))


def _apply_patch(cur, row_id: int, patch: dict[str, Any]) -> None:
    if not patch:
        return
    sets = ", ".join(f"{col} = %s" for col in patch)
    vals = list(patch.values()) + [row_id]
    cur.execute(f"UPDATE epa_master SET {sets} WHERE id = %s", vals)


def audit_epa_master() -> dict[str, Any]:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              COUNT(*) AS total,
              COUNT(*) FILTER (WHERE epa_vehicle_id IS NULL) AS null_epa_id,
              COUNT(*) FILTER (WHERE atv_type IS NULL OR trim(atv_type) = '') AS null_atv,
              COUNT(*) FILTER (WHERE city_e IS NULL) AS null_city_e,
              COUNT(*) FILTER (WHERE highway_e IS NULL) AS null_hwy_e,
              COUNT(*) FILTER (WHERE city08 IS NULL) AS null_city08,
              COUNT(*) FILTER (WHERE highway08 IS NULL) AS null_hwy08,
              COUNT(*) FILTER (WHERE engine_display IS NULL OR trim(engine_display) = '') AS null_engine_display,
              COUNT(*) FILTER (WHERE forced_induction IS NULL OR trim(forced_induction) = '') AS null_forced_induction,
              COUNT(*) FILTER (WHERE cylinders IS NULL) AS null_cylinders,
              COUNT(*) FILTER (WHERE drive IS NULL OR trim(drive) = '') AS null_drive
            FROM epa_master
            """
        )
        row = cur.fetchone()
        summary = dict(row) if isinstance(row, dict) else dict(
            zip(
                [
                    "total",
                    "null_epa_id",
                    "null_atv",
                    "null_city_e",
                    "null_hwy_e",
                    "null_city08",
                    "null_hwy08",
                    "null_engine_display",
                    "null_forced_induction",
                    "null_cylinders",
                    "null_drive",
                ],
                row,
            )
        )

        cur.execute(
            """
            SELECT COUNT(*) FROM epa_master
            WHERE epa_vehicle_id IS NULL AND make = 'Ram' AND model IN ('2500', '3500')
            """
        )
        summary["ram_hd_no_epa"] = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*) FROM epa_master
            WHERE (fuel_type ILIKE '%electric%' OR atv_type IN ('EV', 'Plug-in Hybrid'))
              AND city_e IS NULL
            """
        )
        summary["electric_missing_city_e"] = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*) FROM epa_master
            WHERE fuel_type ILIKE '%diesel%'
              AND (atv_type IS NULL OR trim(atv_type) = '')
            """
        )
        summary["diesel_missing_atv"] = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*) FROM epa_master
            WHERE epa_vehicle_id IS NOT NULL
              AND city08 IS NOT NULL AND highway08 IS NOT NULL
              AND trim IS NOT NULL AND trim(trim) != ''
              AND engine_display IS NOT NULL AND trim(engine_display) != ''
            """
        )
        summary["core_complete_rows"] = int(cur.fetchone()[0])
        return summary
    finally:
        conn.close()


def repair_from_epa_ids(id_index: dict[int, Any]) -> int:
    from backend.enrichment.fueleconomy_catalog import patch_from_fueleconomy
    from backend.db.inventory_db import get_conn

    updated = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(_SELECT + " WHERE epa_vehicle_id IS NOT NULL")
        cols = [d[0] for d in cur.description]
        for raw in cur.fetchall():
            row = _row_dict(raw, cols)
            vid = row.get("epa_vehicle_id")
            try:
                fe = id_index.get(int(vid))
            except (TypeError, ValueError):
                continue
            if not fe:
                continue
            patch = patch_from_fueleconomy(row, fe)
            if patch:
                _apply_patch(cur, int(row["id"]), patch)
                updated += 1
        conn.commit()
    finally:
        conn.close()
    return updated


def infer_atv_types() -> int:
    from backend.enrichment.fueleconomy_catalog import infer_atv_type_from_fuel
    from backend.db.inventory_db import get_conn

    updated = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, fuel_type FROM epa_master
            WHERE (atv_type IS NULL OR trim(atv_type) = '')
              AND fuel_type IS NOT NULL AND trim(fuel_type) != ''
            """
        )
        for rid, fuel in cur.fetchall():
            atv = infer_atv_type_from_fuel(str(fuel or ""))
            if not atv:
                continue
            cur.execute("UPDATE epa_master SET atv_type = %s WHERE id = %s", (atv, int(rid)))
            updated += 1
        conn.commit()
    finally:
        conn.close()
    return updated


def repair_all(*, csv_path: str | None = None) -> None:
    from backend.scripts.backfill_epa_master_fields import backfill as backfill_engine_fields
    from backend.scripts.sync_epa_master_from_fueleconomy import sync_from_csv

    log.info("Step 1/4: sync from fueleconomy.gov vehicles.csv")
    sync_from_csv(csv_path=csv_path, dry_run=False)

    log.info("Step 2/4: backfill from epa_vehicle_id index")
    from backend.enrichment.fueleconomy_catalog import (
        build_fueleconomy_id_index,
        download_vehicles_csv,
        load_fueleconomy_records,
    )
    import os

    path = csv_path or download_vehicles_csv()
    records = load_fueleconomy_records(path)
    id_index = build_fueleconomy_id_index(records)
    n_id = repair_from_epa_ids(id_index)
    log.info("Patched %d row(s) via epa_vehicle_id index", n_id)
    if not csv_path:
        try:
            os.unlink(path)
        except OSError:
            pass

    log.info("Step 3/4: infer atv_type from fuel_type")
    n_atv = infer_atv_types()
    log.info("Set atv_type on %d row(s) from fuel_type", n_atv)

    log.info("Step 4/4: recompute engine_display / forced_induction")
    stats = backfill_engine_fields(dry_run=False)
    log.info(
        "Engine fields: updated %d rows (display=%d forced_induction=%d)",
        stats["updated"],
        stats["engine_display"],
        stats["forced_induction"],
    )


def print_report(summary: dict[str, Any]) -> None:
    total = int(summary["total"])
    log.info("=== epa_master audit (%d rows) ===", total)
    log.info("Core complete (EPA id + MPG + trim + engine_display): %s", summary.get("core_complete_rows"))
    log.info("Missing epa_vehicle_id: %s (Ram 2500/3500 HD exempt: %s)", summary.get("null_epa_id"), summary.get("ram_hd_no_epa"))
    log.info("Missing atv_type: %s (plain-gas nulls are OK)", summary.get("null_atv"))
    log.info("Missing city_e / highway_e: %s / %s (gas cars should be null)", summary.get("null_city_e"), summary.get("null_hwy_e"))
    log.info("Missing city08 / highway08: %s / %s", summary.get("null_city08"), summary.get("null_hwy08"))
    log.info("Electric rows missing city_e: %s", summary.get("electric_missing_city_e"))
    log.info("Diesel rows missing atv_type: %s", summary.get("diesel_missing_atv"))
    log.info("Missing engine_display: %s", summary.get("null_engine_display"))
    log.info("Missing forced_induction (NA engines OK): %s", summary.get("null_forced_induction"))
    log.info("Missing cylinders (EVs OK): %s", summary.get("null_cylinders"))
    log.info("Missing drive (EPA blank OK for some classics): %s", summary.get("null_drive"))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repair", action="store_true", help="Run full repair pipeline")
    parser.add_argument("--csv-path", help="Local vehicles.csv (skip download)")
    args = parser.parse_args(argv)

    if args.repair:
        repair_all(csv_path=args.csv_path)

    summary = audit_epa_master()
    print_report(summary)

    ram_hd = int(summary.get("ram_hd_no_epa") or 0)
    null_epa = int(summary.get("null_epa_id") or 0)
    if null_epa > ram_hd:
        log.warning(
            "%d non-Ram-HD rows still lack epa_vehicle_id — candidate for backup scraping",
            null_epa - ram_hd,
        )
    if int(summary.get("electric_missing_city_e") or 0) > 0:
        log.warning("Some electric rows still missing city_e — re-run repair or API fallback")


if __name__ == "__main__":
    main()
