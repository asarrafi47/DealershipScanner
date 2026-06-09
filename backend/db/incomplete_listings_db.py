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
import threading
import time
from datetime import datetime, timezone

from backend.db.inventory_db import DB_PATH as _INVENTORY_DB_PATH
from backend.db.inventory_pg import is_inventory_postgres
from backend.utils.listing_completeness import listing_missing_field_codes

logger = logging.getLogger(__name__)

# Keep incomplete index beside the active inventory DB (same directory as ``DB_PATH`` targets).
DB_PATH = os.environ.get(
    "INCOMPLETE_LISTINGS_DB_PATH",
    os.path.join(os.path.dirname(_INVENTORY_DB_PATH), "incomplete_listings.db"),
)
_META_BOOTSTRAP_KEY = "index_bootstrap_v1"
_bootstrap_lock = threading.Lock()


def incomplete_index_db_mtime() -> float:
    """Modification time for cache invalidation (sidecar SQLite or 60s bucket on Postgres)."""
    if is_inventory_postgres():
        return float(int(time.time()) // 60)
    try:
        return os.path.getmtime(DB_PATH)
    except OSError:
        return 0.0


def get_conn():
    """Incomplete-listings index: same PostgreSQL DB as inventory when configured; else sidecar SQLite."""
    if is_inventory_postgres():
        from backend.db.inventory_db import get_conn as inv_get_conn

        return inv_get_conn()
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


def _clear_listings_caches_after_index_write() -> None:
    try:
        from backend.db.inventory_db import clear_inventory_listings_cache

        clear_inventory_listings_cache()
    except Exception:
        logger.debug("Could not clear listings cache after incomplete index write", exc_info=True)


def delete_incomplete_record(car_id: int) -> None:
    conn = get_conn()
    _ensure_schema(conn)
    conn.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
    conn.commit()
    conn.close()


def get_missing_field_codes_for_car_id(car_id: int) -> list[str]:
    """Fast read from incomplete index; empty list when the listing is complete."""
    conn = get_conn()
    _ensure_schema(conn)
    row = conn.execute(
        "SELECT missing_fields_json FROM incomplete_listings WHERE car_id = ?",
        (int(car_id),),
    ).fetchone()
    conn.close()
    if not row:
        return []
    try:
        raw = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(c) for c in raw if c] if isinstance(raw, list) else []


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
    return fast_rebuild_incomplete_listings_index()


def fast_rebuild_incomplete_listings_index() -> int:
    """
    Full resync of the incomplete_listings index using the same rules as
    :func:`sync_incomplete_listing_for_car_id` / the car detail page
    (:func:`listing_missing_field_codes`).

    Returns the number of incomplete rows.
    """
    from backend.db.inventory_db import get_conn as inv_get_conn

    inv = inv_get_conn()
    inv.row_factory = sqlite3.Row
    cur = inv.cursor()
    cur.execute("SELECT * FROM cars")
    all_cars = [dict(r) for r in cur.fetchall()]
    inv.close()

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows_to_upsert = []
    for car in all_cars:
        missing = listing_missing_field_codes(car, for_public_filter=False)
        if missing:
            rows_to_upsert.append(
                (car["id"], str(car.get("vin") or "?").strip(), json.dumps(missing), now)
            )

    conn_inc = get_conn()
    _ensure_schema(conn_inc)
    try:
        conn_inc.execute("BEGIN IMMEDIATE")
        conn_inc.execute("DELETE FROM incomplete_listings")
        if rows_to_upsert:
            conn_inc.executemany(
                "INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (?,?,?,?)",
                rows_to_upsert,
            )
        conn_inc.commit()
    except Exception:
        conn_inc.rollback()
        raise
    finally:
        conn_inc.close()

    logger.info(
        "fast_rebuild_incomplete_listings_index: %d incomplete / %d total",
        len(rows_to_upsert),
        len(all_cars),
    )
    _clear_listings_caches_after_index_write()
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
    with _bootstrap_lock:
        conn = get_conn()
        _ensure_schema(conn)
        cur = conn.cursor()
        cur.execute("SELECT v FROM incomplete_listings_meta WHERE k = ?", (_META_BOOTSTRAP_KEY,))
        row = cur.fetchone()
        conn.close()
        if row:
            return
        try:
            n = fast_rebuild_incomplete_listings_index()
            _mark_bootstrapped()
            logger.info("Built incomplete_listings index (%d row(s) flagged).", n)
        except Exception:
            logger.exception("Failed to build incomplete_listings index")


def get_incomplete_listings_count() -> int:
    """Count of rows in the incomplete index (cheap query for dev SSR)."""
    ensure_incomplete_index_built()
    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM incomplete_listings")
    row = cur.fetchone()
    conn.close()
    try:
        return max(0, int(row[0] if row else 0))
    except (TypeError, ValueError, IndexError):
        return 0


def get_incomplete_car_id_set() -> set[int]:
    """Car ids flagged incomplete (O(1) membership for listings grid filtering)."""
    ensure_incomplete_index_built()
    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT car_id FROM incomplete_listings")
    out = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    conn.close()
    return out


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
