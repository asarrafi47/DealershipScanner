#!/usr/bin/env python3
"""
Populate ``epa_master`` on Postgres from the in-repo ``DICTIONARY/*_EPA.csv`` files.

The knowledge engine's ``lookup_epa_aggregate()`` (and the ``backfill_mpg_from_epa.py``
script) query ``epa_master``, but the table ships empty on Postgres — it was only ever
populated via the older sqlite-only ``build_epa_master.py``. This is the Postgres-native
equivalent, loading the same curated per-model CSVs (~8,000 files, one row per
year/make/model/trim configuration).

Requires INVENTORY_DATABASE_URL or DATABASE_URL (postgresql://).

Usage:
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --dry-run
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py
  PYTHONPATH=. python backend/scripts/build_epa_master_pg.py --rebuild
"""
from __future__ import annotations

import argparse
import csv
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
_DICT_DIR = ROOT / "DICTIONARY"


def _infer_atv_type(fuel_type: str) -> str | None:
    fl = (fuel_type or "").lower().strip()
    if not fl:
        return None
    if "electricity" in fl or fl == "electric":
        return "PHEV" if ("hybrid" in fl or "plug" in fl) else "EV"
    if "plug-in" in fl or "phev" in fl:
        return "PHEV"
    if "natural gas" in fl:
        return "CNG"
    return None


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


def _load_csv(path: Path) -> list[dict]:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        logger.debug("skip %s: %s", path.name, exc)
        return []


def build_epa_master(*, dry_run: bool = False, rebuild: bool = False) -> dict[str, int]:
    from backend.db.inventory_pg import inventory_postgres_dsn, pg_connect

    dsn = inventory_postgres_dsn()
    if not dsn:
        raise SystemExit("Set INVENTORY_DATABASE_URL or DATABASE_URL to a postgresql:// DSN")

    epa_files = sorted(_DICT_DIR.glob("*_EPA.csv"))
    if not epa_files:
        raise SystemExit(f"No *_EPA.csv files found in {_DICT_DIR}")
    logger.info("Found %d EPA CSV files in %s", len(epa_files), _DICT_DIR)

    conn = pg_connect()
    inserted = 0
    skipped_existing = 0
    skipped_bad_row = 0
    try:
        cur = conn.cursor()
        if rebuild and not dry_run:
            cur.execute("DELETE FROM epa_master")
            conn.commit()
            logger.info("Cleared epa_master for rebuild")

        cur.execute("SELECT year, lower(make), lower(model), lower(coalesce(trim,'')) FROM epa_master")
        existing = set(cur.fetchall()) if not rebuild else set()
        logger.info("Existing rows: %d", len(existing))

        batch: list[tuple] = []

        def flush(batch: list[tuple]) -> None:
            if not batch or dry_run:
                return
            ins_cur = conn.cursor()
            ins_cur.executemany(
                """
                INSERT INTO epa_master
                    (year, make, model, trim, trany, drive, fuel_type, body_style,
                     engine_description, cylinders, displacement,
                     city08, highway08, atv_type)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                batch,
            )
            conn.commit()

        for path in epa_files:
            rows = _load_csv(path)
            for row in rows:
                year = _safe_int(row.get("Year"))
                make = (row.get("Make") or "").strip()
                model = (row.get("Model") or "").strip()
                trim = (row.get("Trim") or "").strip() or None
                if not year or not make or not model:
                    skipped_bad_row += 1
                    continue

                key = (year, make.lower(), model.lower(), (trim or "").lower())
                if not rebuild and key in existing:
                    skipped_existing += 1
                    continue
                existing.add(key)

                trany = (row.get("transmissionOptions") or "").strip() or None
                drive = (row.get("drivetrainOptions") or "").strip() or None
                fuel_type = (row.get("fuelType") or "").strip() or None
                body_style = (row.get("bodyStyle") or "").strip() or None
                engine_desc = (row.get("engineOptions") or "").strip() or None
                cylinders = _safe_int(row.get("cylinders"))
                displacement = _safe_float(row.get("displacement"))
                city08 = _safe_float(row.get("mpg_city"))
                highway08 = _safe_float(row.get("mpg_highway"))
                atv_type = _infer_atv_type(fuel_type or "")

                batch.append((
                    year, make, model, trim, trany, drive, fuel_type, body_style,
                    engine_desc, cylinders, displacement, city08, highway08, atv_type,
                ))
                inserted += 1
                if len(batch) >= _BATCH_SIZE:
                    flush(batch)
                    logger.info("progress: inserted=%d", inserted)
                    batch = []
        flush(batch)
    finally:
        conn.close()

    return {
        "files": len(epa_files),
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "skipped_bad_row": skipped_bad_row,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Populate epa_master on Postgres from DICTIONARY/*_EPA.csv.")
    parser.add_argument("--dry-run", action="store_true", help="Count only; no writes.")
    parser.add_argument("--rebuild", action="store_true", help="Clear table before loading (ignored with --dry-run).")
    args = parser.parse_args()
    stats = build_epa_master(dry_run=bool(args.dry_run), rebuild=bool(args.rebuild))
    logger.info(
        "%sfiles=%d inserted=%d skipped_existing=%d skipped_bad_row=%d",
        "dry-run: " if args.dry_run else "",
        stats["files"], stats["inserted"], stats["skipped_existing"], stats["skipped_bad_row"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
