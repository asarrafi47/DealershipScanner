"""
Dealership registry table (developer dashboard) in inventory.db.
"""

from __future__ import annotations

import math
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

import pgeocode
from thefuzz import fuzz

from backend.db.inventory_db import DB_PATH, get_conn

_DEDUPE_THRESHOLD = 88


def ensure_dealerships_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dealerships (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            name              TEXT NOT NULL,
            website_url       TEXT NOT NULL,
            city              TEXT NOT NULL,
            state               TEXT NOT NULL,
            latitude          REAL,
            longitude         REAL,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            duplicate_of_id   INTEGER,
            duplicate_score   REAL,
            is_active         INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (duplicate_of_id) REFERENCES dealerships(id)
        )
        """
    )
    cursor.execute("PRAGMA table_info(dealerships)")
    dcols = [row[1] for row in cursor.fetchall()]
    if "is_active" not in dcols:
        cursor.execute("ALTER TABLE dealerships ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealerships_created ON dealerships(created_at DESC)"
    )


def _normalize_dedupe_key(name: str, url: str, city: str, state: str) -> str:
    u = re.sub(r"^https?://(www\.)?", "", (url or "").lower())
    u = u.rstrip("/")
    return " ".join(
        [
            (name or "").lower().strip(),
            u,
            (city or "").lower().strip(),
            (state or "").upper().strip(),
        ]
    )


def geocode_city_state(city: str, state: str) -> tuple[float, float] | None:
    """Resolve lat/lon using pgeocode Nominatim place names, filtered by state."""
    city = (city or "").strip()
    state = (state or "").strip().upper()
    if not city or len(state) != 2:
        return None
    nomi = pgeocode.Nominatim("us")
    df = nomi.query_location(city, top_k=80)
    if df is None or df.empty:
        return None
    if "state_code" in df.columns:
        df = df[df["state_code"] == state]
    if df.empty:
        return None
    # Prefer rows whose place_name matches the city (not "Port Charlotte" for "Charlotte")
    place = df["place_name"].astype(str).str.lower()
    exact = df[place == city.lower()]
    use = exact if not exact.empty else df
    lat = float(use["latitude"].median())
    lon = float(use["longitude"].median())
    if math.isnan(lat) or math.isnan(lon):
        return None
    return (lat, lon)


def insert_dealership(row: dict[str, Any]) -> int:
    conn = get_conn()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute(
        """
        INSERT INTO dealerships (name, website_url, city, state, created_at, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
        """,
        (
            row["name"],
            row["website_url"],
            row["city"],
            row["state"],
            now,
        ),
    )
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return int(new_id)


def list_recent_dealerships(limit: int = 10) -> list[dict[str, Any]]:
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, website_url, city, state, latitude, longitude, created_at,
               duplicate_of_id, duplicate_score, is_active
        FROM dealerships
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def delete_dealership(dealer_id: int) -> bool:
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dealerships WHERE id = ?", (dealer_id,))
    n = cursor.rowcount
    conn.commit()
    conn.close()
    return n > 0


def geocode_missing_dealerships() -> dict[str, int]:
    """Fill latitude/longitude where null, using city + state."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, city, state FROM dealerships
        WHERE (latitude IS NULL OR longitude IS NULL)
        """
    )
    rows = list(cursor.fetchall())
    updated = 0
    for r in rows:
        coords = geocode_city_state(r["city"], r["state"])
        if not coords:
            continue
        lat, lon = coords
        cursor.execute(
            "UPDATE dealerships SET latitude = ?, longitude = ? WHERE id = ?",
            (lat, lon, r["id"]),
        )
        updated += 1
    conn.commit()
    conn.close()
    return {"updated": updated, "examined": len(rows)}


def deduplicate_dealerships() -> dict[str, Any]:
    """
    Flag likely duplicates: the row with higher id points duplicate_of_id to the lower id.
    Uses token_set_ratio on a normalized key.
    """
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, website_url, city, state, duplicate_of_id
        FROM dealerships ORDER BY id ASC
        """
    )
    all_rows = [dict(r) for r in cursor.fetchall()]
    flagged: list[dict[str, Any]] = []
    n = len(all_rows)

    for i in range(n):
        for j in range(i + 1, n):
            a, b = all_rows[i], all_rows[j]
            if b.get("duplicate_of_id"):
                continue
            ka = _normalize_dedupe_key(
                a["name"], a["website_url"], a["city"], a["state"]
            )
            kb = _normalize_dedupe_key(
                b["name"], b["website_url"], b["city"], b["state"]
            )
            score = fuzz.token_set_ratio(ka, kb)
            if score >= _DEDUPE_THRESHOLD:
                cursor.execute(
                    """
                    UPDATE dealerships
                    SET duplicate_of_id = ?, duplicate_score = ?
                    WHERE id = ? AND duplicate_of_id IS NULL
                    """,
                    (a["id"], float(score) / 100.0, b["id"]),
                )
                if cursor.rowcount:
                    flagged.append(
                        {
                            "duplicate_id": b["id"],
                            "canonical_id": a["id"],
                            "score": score,
                        }
                    )

    conn.commit()
    conn.close()
    return {"pairs_flagged": len(flagged), "details": flagged}
