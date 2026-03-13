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
    if "dealer_id" not in cols:
        logger.info("Adding dealer_id column to cars table")
        cursor.execute("ALTER TABLE cars ADD COLUMN dealer_id TEXT")
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
        cursor.execute(
            """
            INSERT INTO cars (
                vin, title, year, make, model, trim, price, mileage,
                image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                zip_code, fuel_type, cylinders, transmission, drivetrain,
                exterior_color, interior_color
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vin) DO UPDATE SET
                title=excluded.title, year=excluded.year, make=excluded.make,
                model=excluded.model, trim=excluded.trim, price=excluded.price,
                mileage=excluded.mileage, image_url=excluded.image_url,
                dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
                dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
                zip_code=excluded.zip_code, fuel_type=excluded.fuel_type,
                cylinders=excluded.cylinders, transmission=excluded.transmission,
                drivetrain=excluded.drivetrain, exterior_color=excluded.exterior_color,
                interior_color=excluded.interior_color
            """,
            (
                vin,
                title,
                v.get("year"),
                v.get("make") or "",
                v.get("model") or "",
                v.get("trim") or "",
                v.get("price"),
                v.get("mileage"),
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
            ),
        )
        count += 1
    conn.commit()
    conn.close()
    logger.info("Upserted %d vehicles", count)
    return count
