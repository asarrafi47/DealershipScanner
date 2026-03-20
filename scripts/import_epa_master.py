#!/usr/bin/env python3
"""
Download EPA fueleconomy.gov vehicles.csv and import into SQLite table `epa_master`.

Usage (from project root):
  python scripts/import_epa_master.py

Requires: inventory.db (or INVENTORY_DB_PATH). Creates `epa_master` if missing.

Data: https://www.fueleconomy.gov/feg/epadata/vehicles.csv
"""
from __future__ import annotations

import csv
import os
import sqlite3
import sys
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.environ.get("INVENTORY_DB_PATH", os.path.join(ROOT, "inventory.db"))
EPA_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv"


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS epa_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            epa_vehicle_id INTEGER,
            year INTEGER,
            make TEXT,
            model TEXT,
            cylinders INTEGER,
            displacement REAL,
            trany TEXT,
            drive TEXT,
            fuel_type TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_epa_master_lookup ON epa_master(year, make, model)")
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(epa_master)")
    have = {row[1] for row in cur.fetchall()}
    for col, typ in [
        ("city08", "REAL"),
        ("highway08", "REAL"),
        ("city_e", "REAL"),
        ("highway_e", "REAL"),
        ("atv_type", "TEXT"),
    ]:
        if col not in have:
            conn.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {typ}")
    conn.commit()


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


def import_csv(path: str, conn: sqlite3.Connection) -> int:
    ensure_table(conn)
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
            vid = norm_int(get(c_id)) if c_id else None
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
                conn.executemany(
                    """
                    INSERT INTO epa_master (
                        epa_vehicle_id, year, make, model, cylinders, displacement, trany, drive, fuel_type,
                        city08, highway08, city_e, highway_e, atv_type
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                batch = []
        if batch:
            conn.executemany(
                """
                INSERT INTO epa_master (
                    epa_vehicle_id, year, make, model, cylinders, displacement, trany, drive, fuel_type,
                    city08, highway08, city_e, highway_e, atv_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
    conn.commit()
    return rows


def main() -> None:
    import tempfile

    tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False)
    tmp.close()
    path = tmp.name
    try:
        print(f"Downloading {EPA_URL} ...")
        urllib.request.urlretrieve(EPA_URL, path)
        print(f"Importing into {DB_PATH} ...")
        conn = sqlite3.connect(DB_PATH)
        ensure_table(conn)
        n = import_csv(path, conn)
        conn.close()
        print(f"Imported {n} EPA vehicle rows into epa_master.")
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
