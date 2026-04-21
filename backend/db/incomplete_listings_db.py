"""
Sidecar SQLite DB listing cars with gaps in the regular listing spec sheet (car detail col 2).

Rows are keyed by ``cars.id`` in the main inventory DB. The index is kept in sync on writes;
``ensure_incomplete_index_built`` bootstraps from a full scan when the meta row is absent.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

from backend.utils.listing_completeness import listing_missing_field_codes

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("INCOMPLETE_LISTINGS_DB_PATH", "incomplete_listings.db")
_META_BOOTSTRAP_KEY = "index_bootstrap_v1"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS incomplete_listings (
            car_id INTEGER PRIMARY KEY NOT NULL,
            vin TEXT NOT NULL,
            missing_fields_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS incomplete_listings_meta (
            k TEXT PRIMARY KEY NOT NULL,
            v TEXT NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_incomplete_listings_updated ON incomplete_listings(updated_at DESC)"
    )
    conn.commit()


def delete_incomplete_record(car_id: int) -> None:
    conn = get_conn()
    _ensure_schema(conn)
    conn.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
    conn.commit()
    conn.close()


def sync_incomplete_listing_for_car_id(car_id: int) -> None:
    from backend.db.inventory_db import get_car_by_id

    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    car = get_car_by_id(car_id, include_inactive=True)
    if not car:
        cur.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
        conn.commit()
        conn.close()
        return
    missing = listing_missing_field_codes(car, for_public_filter=False)
    vin = str(car.get("vin") or "").strip() or "?"
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    if missing:
        cur.execute(
            """
            INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(car_id) DO UPDATE SET
                vin = excluded.vin,
                missing_fields_json = excluded.missing_fields_json,
                updated_at = excluded.updated_at
            """,
            (car_id, vin, json.dumps(missing), now),
        )
    else:
        cur.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
    conn.commit()
    conn.close()


def rebuild_incomplete_listings_index() -> int:
    """Full rescan of inventory ``cars``; returns count of rows left in the incomplete index."""
    from backend.db.inventory_db import get_conn as inv_get_conn

    conn_inc = get_conn()
    _ensure_schema(conn_inc)
    conn_inc.execute("DELETE FROM incomplete_listings")
    conn_inc.commit()
    conn_inc.close()

    inv = inv_get_conn()
    inv.row_factory = sqlite3.Row
    cur = inv.cursor()
    cur.execute("SELECT id FROM cars")
    ids = [int(r[0]) for r in cur.fetchall()]
    inv.close()
    for cid in ids:
        sync_incomplete_listing_for_car_id(cid)

    conn_inc = get_conn()
    cur = conn_inc.cursor()
    cur.execute("SELECT COUNT(*) FROM incomplete_listings")
    n = int(cur.fetchone()[0])
    conn_inc.close()
    return n


def _mark_bootstrapped() -> None:
    conn = get_conn()
    _ensure_schema(conn)
    conn.execute(
        "INSERT OR REPLACE INTO incomplete_listings_meta (k, v) VALUES (?, ?)",
        (_META_BOOTSTRAP_KEY, "1"),
    )
    conn.commit()
    conn.close()


def ensure_incomplete_index_built() -> None:
    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT v FROM incomplete_listings_meta WHERE k = ?", (_META_BOOTSTRAP_KEY,))
    row = cur.fetchone()
    conn.close()
    if row:
        return
    try:
        n = rebuild_incomplete_listings_index()
        _mark_bootstrapped()
        logger.info("Built incomplete_listings index (%d row(s) flagged).", n)
    except Exception:
        logger.exception("Failed to build incomplete_listings index")


def get_incomplete_cars_for_dev() -> list[dict]:
    """Cars referenced in the incomplete index, newest first, with ``incomplete_missing_fields``."""
    from backend.db.inventory_db import get_cars_by_ids

    ensure_incomplete_index_built()
    conn = get_conn()
    _ensure_schema(conn)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT car_id, missing_fields_json FROM incomplete_listings ORDER BY datetime(updated_at) DESC"
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return []
    ids = [int(r["car_id"]) for r in rows]
    fields_by_id = {int(r["car_id"]): json.loads(r["missing_fields_json"]) for r in rows}
    cars = get_cars_by_ids(ids)
    for c in cars:
        c["incomplete_missing_fields"] = fields_by_id.get(int(c["id"]), [])
    return cars
