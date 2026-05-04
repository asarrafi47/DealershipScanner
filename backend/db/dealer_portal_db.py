"""
Dealer-managed inventory (separate from scanned public ``inventory.db``).

SQLite file default: ``dealer_portal.db`` (override with ``DEALER_PORTAL_DB_PATH``).
Each row is scoped to ``user_id`` (app ``users.id``).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

_log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DEALER_PORTAL_DB_PATH", "dealer_portal.db")


def delete_vehicles_for_user(user_id: int) -> None:
    """Remove all dealer inventory rows for an app user (e.g. account deletion)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return
    if uid <= 0:
        return
    conn = get_conn()
    try:
        conn.execute("DELETE FROM dealer_vehicles WHERE user_id = ?", (uid,))
        conn.commit()
    finally:
        conn.close()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def init_dealer_portal_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_vehicles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            vin TEXT NOT NULL,
            title TEXT,
            year INTEGER,
            make TEXT,
            model TEXT,
            trim TEXT,
            price REAL,
            mileage INTEGER,
            transmission TEXT,
            drivetrain TEXT,
            fuel_type TEXT,
            exterior_color TEXT,
            interior_color TEXT,
            cylinders INTEGER,
            body_style TEXT,
            engine_description TEXT,
            stock_number TEXT,
            notes TEXT,
            gallery_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, vin)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dealer_vehicles_user ON dealer_vehicles(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dealer_vehicles_vin ON dealer_vehicles(vin)")
    conn.commit()
    conn.close()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def list_vehicles_for_user(user_id: int) -> list[dict[str, Any]]:
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dealer_vehicles WHERE user_id = ? ORDER BY updated_at DESC",
        (int(user_id),),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for r in rows:
        r["gallery"] = _parse_gallery(r.get("gallery_json"))
    return rows


def _parse_gallery(raw: Any) -> list[str]:
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    try:
        v = json.loads(raw) if isinstance(raw, str) else []
        return [str(x) for x in v if x] if isinstance(v, list) else []
    except (TypeError, ValueError):
        return []


def get_vehicle(user_id: int, vehicle_id: int) -> dict[str, Any] | None:
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM dealer_vehicles WHERE id = ? AND user_id = ?",
        (int(vehicle_id), int(user_id)),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["gallery"] = _parse_gallery(d.get("gallery_json"))
    return d


def insert_vehicle(user_id: int, fields: dict[str, Any]) -> int:
    now = _utc_now()
    vin = str(fields.get("vin") or "").strip().upper()
    if not vin:
        raise ValueError("vin required")
    cols = {
        "user_id": int(user_id),
        "vin": vin,
        "title": fields.get("title"),
        "year": fields.get("year"),
        "make": fields.get("make"),
        "model": fields.get("model"),
        "trim": fields.get("trim"),
        "price": fields.get("price"),
        "mileage": fields.get("mileage"),
        "transmission": fields.get("transmission"),
        "drivetrain": fields.get("drivetrain"),
        "fuel_type": fields.get("fuel_type"),
        "exterior_color": fields.get("exterior_color"),
        "interior_color": fields.get("interior_color"),
        "cylinders": fields.get("cylinders"),
        "body_style": fields.get("body_style"),
        "engine_description": fields.get("engine_description"),
        "stock_number": fields.get("stock_number"),
        "notes": fields.get("notes"),
        "gallery_json": json.dumps(fields.get("gallery") or []),
        "created_at": now,
        "updated_at": now,
    }
    key_order = (
        "user_id",
        "vin",
        "title",
        "year",
        "make",
        "model",
        "trim",
        "price",
        "mileage",
        "transmission",
        "drivetrain",
        "fuel_type",
        "exterior_color",
        "interior_color",
        "cylinders",
        "body_style",
        "engine_description",
        "stock_number",
        "notes",
        "gallery_json",
        "created_at",
        "updated_at",
    )
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dealer_vehicles (
            user_id, vin, title, year, make, model, trim, price, mileage,
            transmission, drivetrain, fuel_type, exterior_color, interior_color,
            cylinders, body_style, engine_description, stock_number, notes,
            gallery_json, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        tuple(cols[k] for k in key_order),
    )
    vid = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return vid


def update_vehicle_gallery(user_id: int, vehicle_id: int, urls: list[str]) -> None:
    now = _utc_now()
    conn = get_conn()
    conn.execute(
        """
        UPDATE dealer_vehicles
        SET gallery_json = ?, updated_at = ?
        WHERE id = ? AND user_id = ?
        """,
        (json.dumps(urls), now, int(vehicle_id), int(user_id)),
    )
    conn.commit()
    conn.close()


def update_vehicle_fields(user_id: int, vehicle_id: int, fields: dict[str, Any]) -> None:
    allowed = {
        "title",
        "year",
        "make",
        "model",
        "trim",
        "price",
        "mileage",
        "transmission",
        "drivetrain",
        "fuel_type",
        "exterior_color",
        "interior_color",
        "cylinders",
        "body_style",
        "engine_description",
        "stock_number",
        "notes",
    }
    sets: list[str] = []
    vals: list[Any] = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        sets.append(f"{k} = ?")
        vals.append(v)
    if not sets:
        return
    vals.append(_utc_now())
    vals.append(int(vehicle_id))
    vals.append(int(user_id))
    conn = get_conn()
    conn.execute(
        f"UPDATE dealer_vehicles SET {', '.join(sets)}, updated_at = ? WHERE id = ? AND user_id = ?",
        vals,
    )
    conn.commit()
    conn.close()


def delete_vehicle(user_id: int, vehicle_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM dealer_vehicles WHERE id = ? AND user_id = ?", (int(vehicle_id), int(user_id)))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return bool(n)


def count_user_vehicles_with_vin(user_id: int, vin: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM dealer_vehicles WHERE user_id = ? AND UPPER(TRIM(vin)) = ?",
        (int(user_id), vin.strip().upper()),
    )
    n = int(cur.fetchone()[0])
    conn.close()
    return n
