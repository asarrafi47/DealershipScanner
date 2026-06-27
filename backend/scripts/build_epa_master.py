#!/usr/bin/env python3
"""
Populate epa_master from DICTIONARY EPA CSV files.

The knowledge engine's lookup_epa_aggregate() queries epa_master, but the
table ships empty. This script loads all *_EPA.csv files so the engine has
real data for every make/model/trim year combination.

Usage:
  python -m backend.scripts.build_epa_master           # insert missing rows only
  python -m backend.scripts.build_epa_master --rebuild  # drop and reload everything
  python -m backend.scripts.build_epa_master --rebuild --delete-csvs  # load then remove source CSVs
"""
from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.db.inventory_db import DB_PATH
from backend.enrichment.dictionary_paths import iter_search_roots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("build_epa_master")

_INSERT_SQL = """
    INSERT INTO epa_master
        (year, make, model, trim, trany, drive, fuel_type, body_style,
         engine_description, engine_display, forced_induction,
         cylinders, displacement, city08, highway08, atv_type)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


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


def _migrate(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(epa_master)")
    have = {row[1] for row in cur.fetchall()}
    for col, typ in [
        ("city08", "REAL"),
        ("highway08", "REAL"),
        ("city_e", "REAL"),
        ("highway_e", "REAL"),
        ("atv_type", "TEXT"),
        ("trim", "TEXT"),
        ("body_style", "TEXT"),
        ("engine_description", "TEXT"),
        ("engine_display", "TEXT"),
        ("forced_induction", "TEXT"),
    ]:
        if col not in have:
            try:
                cur.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_epa_master_trim ON epa_master(year, make, model, trim)"
    )
    conn.commit()


def _discover_epa_files() -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for root in iter_search_roots(kind="epa"):
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*_EPA.csv")):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                out.append(path)
    return out


def _load_csv(path: Path) -> list[dict]:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        log.debug("skip %s: %s", path.name, exc)
        return []


def _rows_from_csv(rows: list[dict], existing: set[tuple], rebuilt: bool) -> list[tuple]:
    batch: list[tuple] = []
    for row in rows:
        year = _safe_int(row.get("Year"))
        make = (row.get("Make") or "").strip()
        model = (row.get("Model") or "").strip()
        trim = (row.get("Trim") or "").strip() or None
        if not year or not make or not model:
            continue

        key = (year, make.lower(), model.lower(), (trim or "").lower())
        if not rebuilt and key in existing:
            continue

        trany = (row.get("transmissionOptions") or "").strip() or None
        drive = (row.get("drivetrainOptions") or "").strip() or None
        fuel_type = (row.get("fuelType") or "").strip() or None
        body_style = (row.get("bodyStyle") or "").strip() or None
        engine_desc = (row.get("engineOptions") or "").strip() or None
        engine_display = (row.get("engineDisplay") or "").strip() or None
        forced_induction = (row.get("forcedInduction") or "").strip() or None
        if not engine_display or not forced_induction:
            try:
                from backend.dictionary.epa_engine import catalog_engine_fields

                derived = catalog_engine_fields(row)
                engine_display = engine_display or (derived.get("engineDisplay") or "").strip() or None
                forced_induction = forced_induction or (derived.get("forcedInduction") or "").strip() or None
            except Exception:
                pass
        cylinders = _safe_int(row.get("cylinders"))
        displacement = _safe_float(row.get("displacement"))
        city08 = _safe_float(row.get("mpg_city"))
        highway08 = _safe_float(row.get("mpg_highway"))
        atv_type = _infer_atv_type(fuel_type or "")

        batch.append(
            (
                year,
                make,
                model,
                trim,
                trany,
                drive,
                fuel_type,
                body_style,
                engine_desc,
                engine_display,
                forced_induction,
                cylinders,
                displacement,
                city08,
                highway08,
                atv_type,
            )
        )
        existing.add(key)
    return batch


def _load_existing_keys(conn: sqlite3.Connection) -> set[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT year, lower(make), lower(model), lower(coalesce(trim,'')) FROM epa_master"
        )
        return set(cur.fetchall())
    except sqlite3.OperationalError:
        return set()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild", action="store_true", help="Clear table before loading")
    parser.add_argument(
        "--delete-csvs",
        action="store_true",
        help="Delete source *_EPA.csv files after a successful import",
    )
    args = parser.parse_args(argv)

    epa_files = _discover_epa_files()
    if not epa_files:
        log.error("No *_EPA.csv files found under dictionary EPA roots")
        sys.exit(1)

    log.info("Found %d EPA CSV files", len(epa_files))

    conn = sqlite3.connect(DB_PATH)
    _migrate(conn)

    if args.rebuild:
        conn.execute("DELETE FROM epa_master")
        conn.commit()
        log.info("Cleared epa_master for rebuild")

    existing = set() if args.rebuild else _load_existing_keys(conn)
    log.info("Existing rows: %d", len(existing))

    cur = conn.cursor()
    total_inserted = 0
    loaded_paths: list[Path] = []
    pending: list[tuple] = []
    BATCH_SIZE = 2000

    for path in epa_files:
        rows = _load_csv(path)
        if not rows:
            continue
        loaded_paths.append(path)
        new_rows = _rows_from_csv(rows, existing, args.rebuild)
        pending.extend(new_rows)
        total_inserted += len(new_rows)
        if len(pending) >= BATCH_SIZE:
            cur.executemany(_INSERT_SQL, pending)
            conn.commit()
            pending.clear()
            log.info("Inserted %d rows so far...", total_inserted)

    if pending:
        cur.executemany(_INSERT_SQL, pending)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM epa_master").fetchone()[0]
    conn.close()
    log.info("Done — inserted %d rows; epa_master now has %d total rows", total_inserted, count)

    if count <= 0:
        log.error("Import produced no rows; not deleting CSV files")
        sys.exit(1)

    if args.delete_csvs:
        deleted = 0
        for path in loaded_paths:
            try:
                path.unlink()
                deleted += 1
            except OSError as exc:
                log.warning("Could not delete %s: %s", path, exc)
        log.info("Deleted %d EPA CSV file(s)", deleted)


if __name__ == "__main__":
    main()
