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


def fast_rebuild_incomplete_listings_index() -> int:
    """
    Fast full resync of the incomplete_listings index — loads all cars in one
    query and checks raw DB fields directly, without invoking the knowledge engine
    (EPA lookups, trim decoder) on each row.  Use this after a scanner run where
    specs have already been backfilled.  Returns the number of incomplete rows.
    """
    from backend.db.inventory_db import get_conn as inv_get_conn

    _PLACEHOLDER_VALUES = frozenset({
        "", "n/a", "na", "null", "none", "unknown", "--", "-", "—",
        "/static/placeholder.svg",
    })

    def _empty(v: object) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        return not s or s.lower() in _PLACEHOLDER_VALUES

    def _has_real_image(car: dict) -> bool:
        if str(car.get("image_url") or "").startswith("http"):
            return True
        raw_g = car.get("gallery")
        try:
            g = json.loads(raw_g) if isinstance(raw_g, str) else (raw_g or [])
            return any(isinstance(u, str) and u.startswith("http") for u in g)
        except (TypeError, ValueError):
            return False

    def _missing_fields(car: dict) -> list[str]:
        missing: list[str] = []
        if _empty(car.get("title")):
            missing.append("title")
        if not _has_real_image(car):
            missing.append("images")
        try:
            price = int(float(car.get("price") or 0))
        except (TypeError, ValueError):
            price = 0
        try:
            msrp = int(float(car.get("msrp") or 0))
        except (TypeError, ValueError):
            msrp = 0
        if price <= 0 and msrp <= 0:
            missing.append("price")
        if not car.get("year"):
            missing.append("year")
        for f in ("make", "model", "trim", "drivetrain", "body_style", "fuel_type", "condition"):
            if _empty(car.get(f)):
                missing.append(f)
        if _empty(car.get("engine_description")):
            missing.append("engine")
        if _empty(car.get("transmission")):
            missing.append("transmission")
        cyl = car.get("cylinders")
        if cyl is None or str(cyl).strip() == "":
            missing.append("cylinders")
        for f in ("exterior_color", "interior_color"):
            if _empty(car.get(f)):
                missing.append(f)
        vin = str(car.get("vin") or "").strip()
        if not vin or vin.lower().startswith("unknown"):
            missing.append("vin")
        return missing

    inv = inv_get_conn()
    inv.row_factory = sqlite3.Row
    cur = inv.cursor()
    cur.execute("SELECT * FROM cars")
    all_cars = [dict(r) for r in cur.fetchall()]
    inv.close()

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows_to_upsert = [
        (car["id"], str(car.get("vin") or "?").strip(), json.dumps(_missing_fields(car)), now)
        for car in all_cars
        if _missing_fields(car)
    ]

    conn_inc = get_conn()
    _ensure_schema(conn_inc)
    conn_inc.execute("DELETE FROM incomplete_listings")
    conn_inc.executemany(
        "INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (?,?,?,?)",
        rows_to_upsert,
    )
    conn_inc.commit()
    conn_inc.close()

    logger.info(
        "fast_rebuild_incomplete_listings_index: %d incomplete / %d total",
        len(rows_to_upsert),
        len(all_cars),
    )
    return len(rows_to_upsert)


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
