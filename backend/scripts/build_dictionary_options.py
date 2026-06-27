#!/usr/bin/env python3
"""
Populate dictionary_options from Complete_Options CSV files (one-time / rebuild import).

Runtime code reads dictionary_options only — not CSV files.

Usage:
  python -m backend.scripts.build_dictionary_options
  python -m backend.scripts.build_dictionary_options --rebuild
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.dictionary_schema import ensure_dictionary_options_table, ensure_epa_master_extended_columns
from backend.db.inventory_db import get_conn, init_inventory_db
from backend.db.inventory_pg import is_inventory_postgres
from backend.enrichment.dictionary_catalog import iter_dictionary_csv_paths, options_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("build_dictionary_options")


def _safe_int(v: str) -> int | None:
    try:
        return int(str(v).strip()) if v and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _safe_float(v: str) -> float | None:
    try:
        return float(str(v).strip()) if v and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _row_tuple(row: dict) -> tuple | None:
    year = _safe_int(row.get("Year"))
    make = (row.get("Make") or "").strip()
    model = (row.get("Model") or "").strip()
    if not year or not make or not model:
        return None
    return (
        year,
        make,
        model,
        (row.get("Trim") or "").strip() or None,
        (row.get("engineOptions") or "").strip() or None,
        (row.get("engineDisplay") or "").strip() or None,
        (row.get("forcedInduction") or "").strip() or None,
        (row.get("transmissionOptions") or "").strip() or None,
        (row.get("drivetrainOptions") or "").strip() or None,
        (row.get("fuelType") or "").strip() or None,
        (row.get("bodyStyle") or "").strip() or None,
        _safe_int(row.get("cylinders")),
        _safe_float(row.get("displacement")),
        _safe_float(row.get("mpg_city")),
        _safe_float(row.get("mpg_highway")),
        _safe_float(row.get("mpg_combined")),
        (row.get("exteriorColors") or "").strip() or None,
        (row.get("Packages") or "").strip() or None,
        (row.get("packageDetails") or "").strip() or None,
        (row.get("Options") or "").strip() or None,
        (row.get("optionDetails") or "").strip() or None,
    )


def _load_csv(path: Path) -> list[dict]:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        log.debug("skip %s: %s", path.name, exc)
        return []


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild", action="store_true", help="Clear dictionary_options before load")
    args = parser.parse_args(argv)

    option_files = list(iter_dictionary_csv_paths("options"))
    if not option_files:
        log.error("No *_Complete_Options.csv files found for import")
        sys.exit(1)

    log.info("Found %d options CSV files", len(option_files))

    init_inventory_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        ensure_epa_master_extended_columns(cur, postgres=is_inventory_postgres())
        ensure_dictionary_options_table(cur, postgres=is_inventory_postgres())
        conn.commit()

        if args.rebuild:
            conn.cursor().execute("DELETE FROM dictionary_options")
            conn.commit()
            log.info("Cleared dictionary_options for rebuild")

        cur = conn.cursor()
        total = 0
        batch = 0
        skipped_stub_files = 0

        for path in option_files:
            if options_status(path) == "stub":
                skipped_stub_files += 1
                continue
            rows = _load_csv(path)
            for row in rows:
                tup = _row_tuple(row)
                if tup is None:
                    continue
                cur.execute(
                    """
                    INSERT INTO dictionary_options
                        (year, make, model, trim, engine_options, engine_display, forced_induction,
                         transmission, drivetrain, fuel_type, body_style, cylinders, displacement,
                         mpg_city, mpg_highway, mpg_combined, exterior_colors,
                         packages, package_details, options, option_details)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    tup,
                )
                total += 1
                batch += 1
                if batch >= 1000:
                    conn.commit()
                    batch = 0

        conn.commit()
        count = conn.cursor().execute("SELECT COUNT(*) FROM dictionary_options").fetchone()[0]
    finally:
        conn.close()

    log.info("Skipped %d stub option files", skipped_stub_files)
    log.info("Done — inserted %d rows; dictionary_options now has %d total rows", total, count)


if __name__ == "__main__":
    main()
