#!/usr/bin/env python3
"""
Download EPA fueleconomy.gov vehicles.csv and import it into the inventory `epa_master`.

Usage (from repo root):
  PYTHONPATH=. python backend/scripts/import_epa_master.py --append-years 2027
  PYTHONPATH=. python backend/scripts/import_epa_master.py --yes   # full replace, guarded

Runs on the inventory backend (Postgres via INVENTORY_DATABASE_URL; SQLite only under
pytest). Creates `epa_master` if missing.

``--append-years`` only inserts rows whose epa_vehicle_id is new; it never deletes.

The full replace (``--yes``, or ``import_csv(append=False)``) runs ``DELETE FROM
epa_master`` and reloads with fresh ids. That deletion cascades to
``epa_extended_specs`` and leaves every ``cars.epa_master_id`` dangling, so it is
refused (RuntimeError; exit 2 from the CLI, with the counts printed) while any car is
linked or any extended-specs row exists. It still works on an empty, unlinked catalog.

Data: https://www.fueleconomy.gov/feg/epadata/vehicles.csv
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

try:
    from backend.utils.project_env import load_project_dotenv

    load_project_dotenv()
except ImportError:
    pass

EPA_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv"


def ensure_table(conn: Any) -> None:
    # epa_master's full schema (including columns this script doesn't know about —
    # trim/body_style/engine_description/forced_induction/etc.) is centrally managed
    # in backend/db/inventory_pg.py's init_postgres_inventory(). A local ad-hoc
    # CREATE TABLE + PRAGMA-based column check here would be redundant against
    # Postgres and actively dangerous (PRAGMA becomes a no-op via the SQL adapter,
    # so ``have`` would always be empty, and the un-guarded ALTER TABLE ADD COLUMN
    # calls would crash on Postgres's "column already exists" error on any run
    # after the first).
    from backend.db.inventory_db import init_inventory_db

    init_inventory_db()


def norm_float(s: str) -> float | None:
    try:
        return float(str(s).strip()) if str(s).strip() else None
    except ValueError:
        return None


def norm_int(s: str) -> int | None:
    try:
        return int(float(str(s).strip())) if str(s).strip() else None
    except ValueError:
        return None


_INSERT_SQL = (
    "INSERT INTO epa_master (epa_vehicle_id, year, make, model, cylinders, displacement, trany, drive, "
    "fuel_type, city08, highway08, city_e, highway_e, atv_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def _insert_batch(conn: Any, batch: list[tuple]) -> None:
    """Row-by-row on the Postgres adapter (no executemany), batch on SQLite."""
    if hasattr(conn, "executemany"):
        conn.executemany(_INSERT_SQL, batch)
        return
    for row in batch:
        conn.execute(_INSERT_SQL, row)


EXIT_REFUSED = 2


class FullReplaceRefused(RuntimeError):
    """A full replace of ``epa_master`` would orphan car links or extended specs."""

    def __init__(self, *, linked_cars: int, extended_specs: int, catalog_rows: int) -> None:
        self.linked_cars = linked_cars
        self.extended_specs = extended_specs
        self.catalog_rows = catalog_rows
        super().__init__(
            "Refusing to replace epa_master: "
            f"{linked_cars} car(s) have cars.epa_master_id set and epa_extended_specs holds "
            f"{extended_specs} row(s) (epa_master has {catalog_rows} row(s)). "
            "DELETE FROM epa_master would cascade to epa_extended_specs and leave every car "
            "link pointing at an id that no longer exists. Use --append-years to add model years."
        )


def _is_missing_table_error(exc: BaseException) -> bool:
    """True only for "table does not exist" (SQLite OperationalError / Postgres 42P01)."""
    if getattr(exc, "sqlstate", None) == "42P01":
        return True
    return isinstance(exc, sqlite3.OperationalError) and "no such table" in str(exc).lower()


def _scalar_count(conn: Any, sql: str) -> int:
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _count_extended_specs(conn: Any) -> int:
    """Rows in ``epa_extended_specs``; 0 only when the table does not exist.

    The probe runs inside a savepoint: on Postgres a failed statement (the table is
    absent on SQLite and on some fresh databases) would otherwise abort the whole
    transaction. Any other error is re-raised, so the guard fails closed.
    """
    conn.execute("SAVEPOINT epa_xspecs_probe")
    try:
        n = _scalar_count(conn, "SELECT COUNT(*) FROM epa_extended_specs")
    except Exception as exc:
        conn.execute("ROLLBACK TO SAVEPOINT epa_xspecs_probe")
        conn.execute("RELEASE SAVEPOINT epa_xspecs_probe")
        if _is_missing_table_error(exc):
            return 0
        raise
    conn.execute("RELEASE SAVEPOINT epa_xspecs_probe")
    return n


def assert_full_replace_allowed(conn: Any) -> None:
    """Raise :class:`FullReplaceRefused` unless ``epa_master`` is safe to delete.

    Safe means no car carries an ``epa_master_id`` and ``epa_extended_specs`` is empty
    (or absent). Errors reading ``cars`` propagate, so the replace does not run.
    """
    linked = _scalar_count(conn, "SELECT COUNT(*) FROM cars WHERE epa_master_id IS NOT NULL")
    xspecs = _count_extended_specs(conn)
    if linked or xspecs:
        catalog = _scalar_count(conn, "SELECT COUNT(*) FROM epa_master")
        raise FullReplaceRefused(linked_cars=linked, extended_specs=xspecs, catalog_rows=catalog)


def import_csv(path: str, conn: Any, *, years: set[int] | None = None, append: bool = False) -> int:
    """Import vehicles.csv. ``append=True`` keeps every existing row and inserts
    only rows (restricted to *years* when given) whose epa_vehicle_id is not in
    the table yet: the way to pull a new model year (2027: 84 rows in July, 541
    in the September file) without touching the curated columns other scripts
    added (trim, body_style, engine_description).

    ``append=False`` replaces the whole table, and raises :class:`FullReplaceRefused`
    (a RuntimeError) before the DELETE while any car is linked or any extended-specs
    row exists. The check runs again just before the commit, in the same transaction,
    so a link written while the CSV loads rolls the replace back too."""
    ensure_table(conn)
    existing_ids: set[int] = set()
    if not append:
        try:
            assert_full_replace_allowed(conn)
        except Exception:
            conn.rollback()
            raise
    if append:
        cur = conn.cursor()
        if years:
            cur.execute(
                "SELECT epa_vehicle_id FROM epa_master WHERE year IN (" + ",".join("?" * len(years)) + ")",
                tuple(sorted(years)),
            )
        else:
            cur.execute("SELECT epa_vehicle_id FROM epa_master")
        existing_ids = {int(r[0]) for r in cur.fetchall() if r[0] is not None}
    else:
        conn.execute("DELETE FROM epa_master")
    rows = 0
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return 0
        # Normalize header keys (strip BOM / spaces)
        fieldnames = [c.strip().lstrip("\ufeff") for c in reader.fieldnames]

        def norm_row(raw: dict) -> dict:
            return {(k or "").strip().lstrip("\ufeff"): v for k, v in raw.items()}

        def col(*names: str) -> str | None:
            for n in names:
                for fn in fieldnames:
                    if fn.lower() == n.lower():
                        return fn
            return None

        c_id = col("id")
        c_year = col("year")
        c_make = col("make")
        c_model = col("model")
        c_cyl = col("cylinders", "cyl")
        c_displ = col("displ", "displacement")
        c_trany = col("trany", "transmission")
        c_drive = col("drive", "drivetrain")
        c_fuel = col("fuelType", "fuel_type", "fuelType1")
        c_city = col("city08")
        c_hwy = col("highway08")
        c_citye = col("cityE")
        c_hwye = col("highwayE")
        # fueleconomy.gov uses atvType; some exports differ slightly
        c_atv = col("atvType", "ATVType", "atv_type", "vehicleType", "VehType")
        if not c_atv:
            for fn in fieldnames:
                n = (fn or "").strip().replace(" ", "").replace("_", "").lower()
                if n == "atvtype" or n.endswith("atvtype"):
                    c_atv = fn
                    break

        if not c_year or not c_make or not c_model:
            print("Could not find year/make/model columns in CSV.", file=sys.stderr)
            print("Fields:", fieldnames[:20], file=sys.stderr)
            return 0
        if not c_atv:
            print(
                "Note: no atvType column found in CSV — PHEV/Hybrid 'Electric +' prefix will use trim/VIN rules only.",
                file=sys.stderr,
            )

        batch = []
        for raw in reader:
            row = norm_row(raw)
            def get(name: str | None) -> str:
                if not name:
                    return ""
                return (row.get(name) or "").strip()

            y = norm_int(get(c_year))
            mk = get(c_make)
            md = get(c_model)
            if y is None or not mk or not md:
                continue
            if years and y not in years:
                continue
            vid = norm_int(get(c_id)) if c_id else None
            if append and (vid is None or vid in existing_ids):
                continue
            cyl = norm_int(get(c_cyl)) if c_cyl else None
            displ = norm_float(get(c_displ)) if c_displ else None
            trany = get(c_trany) or None
            drive = get(c_drive) or None
            fuel = get(c_fuel) or None
            city08 = norm_float(get(c_city)) if c_city else None
            highway08 = norm_float(get(c_hwy)) if c_hwy else None
            city_e = norm_float(get(c_citye)) if c_citye else None
            highway_e = norm_float(get(c_hwye)) if c_hwye else None
            atv_type = (get(c_atv) or None) if c_atv else None
            batch.append(
                (
                    vid,
                    y,
                    mk,
                    md,
                    cyl,
                    displ,
                    trany,
                    drive,
                    fuel,
                    city08,
                    highway08,
                    city_e,
                    highway_e,
                    atv_type,
                )
            )
            rows += 1
            if len(batch) >= 2000:
                _insert_batch(conn, batch)
                batch = []
        if batch:
            _insert_batch(conn, batch)
    if not append:
        try:
            assert_full_replace_allowed(conn)
        except Exception:
            conn.rollback()
            raise
    conn.commit()
    return rows


def main(argv: list[str] | None = None) -> int:
    import argparse
    import tempfile

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--append-years", default="", help="comma-separated model years to ADD (no delete), e.g. 2027")
    ap.add_argument("--csv", default="", help="use this local vehicles.csv instead of downloading")
    ap.add_argument(
        "--yes",
        action="store_true",
        help="Required: this replaces ALL of epa_master with the downloaded government CSV "
        "(DELETE FROM epa_master, then reimport) — including this project's own curated/merged "
        "rows built by build_epa_master_pg.py and the 2026-07-06 dump import. Refused (exit 2) "
        "while any cars.epa_master_id is set or epa_extended_specs has rows.",
    )
    args = ap.parse_args(argv)

    from backend.db.inventory_db import get_conn
    from backend.db.inventory_pg import is_inventory_postgres

    years = {int(y) for y in args.append_years.split(",") if y.strip().isdigit()}
    if years:
        path = args.csv
        if not path:
            tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False)
            tmp.close()
            path = tmp.name
            print(f"Downloading {EPA_URL} ...")
            urllib.request.urlretrieve(EPA_URL, path)
        conn = get_conn()
        try:
            n = import_csv(path, conn, years=years, append=True)
        finally:
            conn.close()
        print(f"Appended {n} EPA row(s) for {sorted(years)} into epa_master (existing rows untouched).")
        return 0

    if not args.yes:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM epa_master")
            n = cur.fetchone()[0]
        finally:
            conn.close()
        backend_name = "Postgres" if is_inventory_postgres() else "SQLite"
        print(
            f"Refusing to replace {n} existing row(s) in the {backend_name} epa_master table "
            "without --yes. This deletes everything currently in epa_master (including data "
            "from other sources) and replaces it with the fueleconomy.gov download."
        )
        return 1

    # Checked before the download (and again inside import_csv, in the replace's own
    # transaction). A short-lived connection, so no snapshot is held while downloading.
    conn = get_conn()
    try:
        ensure_table(conn)
        assert_full_replace_allowed(conn)
    except FullReplaceRefused as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_REFUSED
    finally:
        conn.close()

    tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False)
    tmp.close()
    path = tmp.name
    try:
        print(f"Downloading {EPA_URL} ...")
        urllib.request.urlretrieve(EPA_URL, path)
        conn = get_conn()
        try:
            n = import_csv(path, conn)
        except FullReplaceRefused as exc:
            print(str(exc), file=sys.stderr)
            return EXIT_REFUSED
        finally:
            conn.close()
        print(f"Imported {n} EPA vehicle rows into epa_master.")
        return 0
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
