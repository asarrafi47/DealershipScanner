"""
Database layer for scanner: connection to inventory.db and vehicle upsert.
"""
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

# Use same DB path as inventory_db when running from project root
DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
logger = logging.getLogger(__name__)


def get_conn():
    return sqlite3.connect(DB_PATH)


def _ensure_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            vin              TEXT UNIQUE NOT NULL,
            title            TEXT,
            year             INTEGER,
            make             TEXT,
            model            TEXT,
            trim             TEXT,
            price            REAL,
            mileage          INTEGER,
            zip_code         TEXT,
            fuel_type        TEXT,
            cylinders        INTEGER,
            transmission     TEXT,
            drivetrain       TEXT,
            exterior_color   TEXT,
            interior_color   TEXT,
            image_url        TEXT,
            dealer_name      TEXT,
            dealer_url       TEXT,
            dealer_id        TEXT,
            scraped_at       TEXT
        )
    """)
    conn.commit()
    cursor.execute("PRAGMA table_info(cars)")
    cols = [row[1] for row in cursor.fetchall()]
    for col, ctype in [
        ("dealer_id", "TEXT"),
        ("stock_number", "TEXT"),
        ("gallery", "TEXT"),
        ("carfax_url", "TEXT"),
        ("history_highlights", "TEXT"),
        ("msrp", "REAL"),
    ]:
        if col not in cols:
            logger.info("Adding %s column to cars table", col)
            cursor.execute(f"ALTER TABLE cars ADD COLUMN {col} {ctype}")
            conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_specs (
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            cylinders INTEGER,
            gears INTEGER,
            transmission TEXT,
            PRIMARY KEY (make, model)
        )
    """)
    cursor.execute("""
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
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_epa_master_lookup ON epa_master(year, make, model)"
    )
    conn.commit()
    conn.close()


def upsert_vehicles(vehicles: list[dict]) -> int:
    """
    Insert or replace vehicles by vin. Strict de-duplication: one row per VIN
    (same car in 'New' and 'Used' counts once). Uses ON CONFLICT(vin) DO UPDATE.
    """
    if not vehicles:
        return 0
    by_vin = {}
    for v in vehicles:
        vin = (v.get("vin") or "").strip()
        if vin:
            by_vin[vin] = v
    vehicles = list(by_vin.values())
    conn = get_conn()
    _ensure_schema(conn)
    conn = get_conn()
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    count = 0
    for v in vehicles:
        vin = (v.get("vin") or "").strip()
        if not vin:
            continue
        title = v.get("title") or f"{v.get('year')} {v.get('make')} {v.get('model')} {v.get('trim') or ''}".strip()
        # Price: ensure number (strip $ and , already done in parser); store as int/float
        try:
            price = v.get("price")
            price = int(round(float(price))) if price is not None and str(price).strip() != "" else 0
        except (TypeError, ValueError):
            price = 0
        # Mileage: ensure integer
        try:
            mileage = v.get("mileage")
            mileage = int(mileage) if mileage is not None and str(mileage).strip() != "" else 0
        except (TypeError, ValueError):
            mileage = 0
        try:
            msrp_val = v.get("msrp")
            msrp = int(round(float(msrp_val))) if msrp_val is not None and str(msrp_val).strip() != "" else None
            if msrp is not None and msrp <= 0:
                msrp = None
        except (TypeError, ValueError):
            msrp = None
        # Gallery: SQLite stores arrays as JSON string; always use json.dumps(list)
        gallery = v.get("gallery")
        if isinstance(gallery, list):
            gallery_json = json.dumps(gallery)
        elif gallery is not None and isinstance(gallery, str):
            try:
                json.loads(gallery)
                gallery_json = gallery
            except (TypeError, ValueError):
                gallery_json = "[]"
        else:
            gallery_json = "[]"
        highlights = v.get("history_highlights")
        highlights_json = json.dumps(highlights) if isinstance(highlights, list) else (highlights if isinstance(highlights, str) else "[]")
        cursor.execute(
            """
            INSERT INTO cars (
                vin, title, year, make, model, trim, price, mileage,
                image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                zip_code, fuel_type, cylinders, transmission, drivetrain,
                exterior_color, interior_color, stock_number, gallery, carfax_url, history_highlights, msrp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vin) DO UPDATE SET
                title=excluded.title, year=excluded.year, make=excluded.make,
                model=excluded.model, trim=excluded.trim, price=excluded.price,
                mileage=excluded.mileage, image_url=excluded.image_url,
                dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
                dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
                zip_code=excluded.zip_code, fuel_type=excluded.fuel_type,
                cylinders=excluded.cylinders, transmission=excluded.transmission,
                drivetrain=excluded.drivetrain, exterior_color=excluded.exterior_color,
                interior_color=excluded.interior_color, stock_number=excluded.stock_number,
                gallery=excluded.gallery, carfax_url=excluded.carfax_url, history_highlights=excluded.history_highlights,
                msrp=excluded.msrp
            """,
            (
                vin,
                title,
                v.get("year"),
                v.get("make") or "",
                v.get("model") or "",
                v.get("trim") or "",
                price,
                mileage,
                v.get("image_url") or "",
                v.get("dealer_name") or "",
                v.get("dealer_url") or "",
                v.get("dealer_id") or "",
                now,
                v.get("zip_code"),
                v.get("fuel_type"),
                v.get("cylinders"),
                v.get("transmission"),
                v.get("drivetrain"),
                v.get("exterior_color"),
                v.get("interior_color"),
                v.get("stock_number") or "",
                gallery_json,
                v.get("carfax_url") or "",
                highlights_json,
                msrp,
            ),
        )
        count += 1
    conn.commit()
    conn.close()
    logger.info("Upserted %d vehicles", count)
    return count
