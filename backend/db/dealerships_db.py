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
    from backend.db.inventory_pg import is_inventory_postgres

    if is_inventory_postgres():
        return
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dealerships (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            name              TEXT NOT NULL,
            website_url       TEXT NOT NULL,
            city              TEXT NOT NULL,
            state             TEXT NOT NULL,
            latitude          REAL,
            longitude         REAL,
            created_at        TEXT NOT NULL DEFAULT (datetime('now')),
            duplicate_of_id   INTEGER,
            duplicate_score   REAL,
            is_active         INTEGER NOT NULL DEFAULT 1,
            street_address    TEXT,
            zip_code          TEXT,
            dealer_website_url TEXT,
            source_dmv        INTEGER NOT NULL DEFAULT 0,
            source_osm        INTEGER NOT NULL DEFAULT 0,
            source_web        INTEGER NOT NULL DEFAULT 0,
            osm_id            TEXT,
            FOREIGN KEY (duplicate_of_id) REFERENCES dealerships(id)
        )
        """
    )
    cursor.execute("PRAGMA table_info(dealerships)")
    dcols = [row[1] for row in cursor.fetchall()]
    additive = [
        ("is_active",         "INTEGER NOT NULL DEFAULT 1"),
        ("street_address",    "TEXT"),
        ("zip_code",          "TEXT"),
        ("dealer_website_url","TEXT"),
        ("source_dmv",        "INTEGER NOT NULL DEFAULT 0"),
        ("source_osm",        "INTEGER NOT NULL DEFAULT 0"),
        ("source_web",        "INTEGER NOT NULL DEFAULT 0"),
        ("osm_id",            "TEXT"),
        ("sticker_provider",  "TEXT NOT NULL DEFAULT 'unknown'"),
        ("sticker_ipacket_fail_count", "INTEGER NOT NULL DEFAULT 0"),
        ("sticker_provider_updated_at", "TEXT"),
        ("google_place_id",       "TEXT"),
        ("google_rating",         "REAL"),
        ("google_review_count",   "INTEGER"),
        ("google_rating_fetched_at", "TEXT"),
    ]
    for col, coltype in additive:
        if col not in dcols:
            cursor.execute(f"ALTER TABLE dealerships ADD COLUMN {col} {coltype}")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealerships_created ON dealerships(created_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealerships_zip ON dealerships(zip_code)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealerships_google_place ON dealerships(google_place_id)"
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


def upsert_discovery_row(row: dict[str, Any]) -> int:
    """
    Insert or update a dealership discovered by the ingestion pipeline.

    Uniqueness key: ``osm_id`` when present; otherwise fuzzy-dedupe against
    existing rows via ``deduplicate_dealerships``.  Returns the canonical row id.

    Provenance booleans (source_dmv / source_osm / source_web) are ORed so a row
    that was first found by OSM and later confirmed by DDG accumulates both flags.
    """
    conn = get_conn()
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)

    now = datetime.now(timezone.utc).isoformat()
    osm_id = (row.get("osm_id") or "").strip() or None

    existing_id: int | None = None
    if osm_id:
        cursor.execute("SELECT id FROM dealerships WHERE osm_id = ?", (osm_id,))
        r = cursor.fetchone()
        if r:
            existing_id = int(r[0])

    if existing_id is None:
        # Fall back to name+city+state fuzzy match
        name = (row.get("name") or "").strip()
        city = (row.get("city") or "").strip()
        state = (row.get("state") or "").strip().upper()
        if name:
            cursor.execute(
                "SELECT id, name, website_url, city, state FROM dealerships "
                "WHERE state = ? AND duplicate_of_id IS NULL",
                (state,),
            )
            for cid, cname, curl, ccity, cstate in cursor.fetchall():
                score = fuzz.token_set_ratio(
                    _normalize_dedupe_key(name, row.get("dealer_website_url") or "", city, state),
                    _normalize_dedupe_key(cname, curl or "", ccity, cstate),
                )
                if score >= _DEDUPE_THRESHOLD:
                    existing_id = int(cid)
                    break

    website_url = (
        row.get("dealer_website_url")
        or row.get("website_url")
        or ""
    ).strip()

    if existing_id is not None:
        # Merge: fill blanks, OR provenance flags
        rating_fetched_at = datetime.now(timezone.utc).isoformat() if row.get("google_rating") is not None or row.get("google_place_id") else None
        cursor.execute(
            """
            UPDATE dealerships SET
                street_address     = COALESCE(NULLIF(TRIM(street_address),''),   NULLIF(TRIM(?),'')),
                zip_code           = COALESCE(NULLIF(TRIM(zip_code),''),         NULLIF(TRIM(?),'')),
                dealer_website_url = COALESCE(NULLIF(TRIM(dealer_website_url),''),NULLIF(TRIM(?),'')),
                website_url        = CASE WHEN TRIM(website_url)='' OR website_url IS NULL
                                         THEN NULLIF(TRIM(?), '') ELSE website_url END,
                latitude           = COALESCE(latitude,  ?),
                longitude          = COALESCE(longitude, ?),
                osm_id             = COALESCE(osm_id,    NULLIF(TRIM(?),'')),
                google_place_id    = COALESCE(NULLIF(TRIM(google_place_id), ''), NULLIF(TRIM(?), '')),
                google_rating      = COALESCE(?, google_rating),
                google_review_count = COALESCE(?, google_review_count),
                google_rating_fetched_at = COALESCE(?, google_rating_fetched_at),
                source_dmv         = source_dmv | ?,
                source_osm         = source_osm | ?,
                source_web         = source_web | ?
            WHERE id = ?
            """,
            (
                row.get("street_address") or "",
                row.get("zip_code") or "",
                website_url,
                website_url,
                row.get("latitude"),
                row.get("longitude"),
                osm_id or "",
                row.get("google_place_id") or "",
                row.get("google_rating"),
                row.get("google_review_count"),
                rating_fetched_at,
                int(bool(row.get("source_dmv"))),
                int(bool(row.get("source_osm"))),
                int(bool(row.get("source_web"))),
                existing_id,
            ),
        )
        conn.commit()
        conn.close()
        return existing_id

    # Insert new row
    name = (row.get("name") or "Unknown Dealer").strip()
    city = (row.get("city") or "").strip()
    state = (row.get("state") or "").strip().upper()
    rating_fetched_at = datetime.now(timezone.utc).isoformat() if row.get("google_rating") is not None or row.get("google_place_id") else None
    cursor.execute(
        """
        INSERT INTO dealerships
            (name, website_url, city, state, latitude, longitude, created_at,
             street_address, zip_code, dealer_website_url,
             source_dmv, source_osm, source_web, osm_id, is_active,
             google_place_id, google_rating, google_review_count, google_rating_fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)
        """,
        (
            name,
            website_url,
            city,
            state,
            row.get("latitude"),
            row.get("longitude"),
            now,
            row.get("street_address") or "",
            row.get("zip_code") or "",
            website_url,
            int(bool(row.get("source_dmv"))),
            int(bool(row.get("source_osm"))),
            int(bool(row.get("source_web"))),
            osm_id,
            row.get("google_place_id") or None,
            row.get("google_rating"),
            row.get("google_review_count"),
            rating_fetched_at,
        ),
    )
    new_id = int(cursor.lastrowid)
    conn.commit()
    conn.close()
    return new_id


def search_dealerships_by_radius(
    lat: float, lon: float, radius_miles: float
) -> list[dict[str, Any]]:
    """Return all active dealerships within *radius_miles* of (lat, lon)."""
    from backend.db.geo import haversine

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    cursor.execute(
        """
        SELECT id, name, website_url, city, state, latitude, longitude, created_at,
               street_address, zip_code, dealer_website_url,
               source_dmv, source_osm, source_web, osm_id,
               duplicate_of_id, duplicate_score, is_active
        FROM dealerships
        WHERE is_active = 1 AND duplicate_of_id IS NULL
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        """
    )
    results = []
    for row in cursor.fetchall():
        d = haversine(lat, lon, float(row["latitude"]), float(row["longitude"]))
        if d <= radius_miles:
            r = dict(row)
            r["distance_miles"] = round(d, 2)
            results.append(r)
    conn.close()
    results.sort(key=lambda x: x["distance_miles"])
    return results


def get_dealership_by_id(dealer_id: int) -> dict[str, Any] | None:
    """Return a single dealership row by primary key, or None."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    cursor.execute(
        """
        SELECT id, name, website_url, city, state, latitude, longitude,
               street_address, zip_code, dealer_website_url, is_active,
               google_place_id, google_rating, google_review_count, google_rating_fetched_at
        FROM dealerships WHERE id = ?
        """,
        (dealer_id,),
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def list_dealers_needing_google_rating(*, limit: int = 25) -> list[dict[str, Any]]:
    """Active registry rows that have never had a Google rating lookup."""
    cap = max(1, int(limit))
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    cursor.execute(
        """
        SELECT id, name, city, state, latitude, longitude, google_place_id
        FROM dealerships
        WHERE is_active = 1
          AND duplicate_of_id IS NULL
          AND google_rating_fetched_at IS NULL
        ORDER BY id ASC
        LIMIT ?
        """,
        (cap,),
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def save_dealer_google_rating(
    dealer_id: int,
    *,
    place_id: str | None,
    rating: float | None,
    review_count: int | None,
) -> None:
    """Persist Google rating cache fields on a registry dealership row."""
    conn = get_conn()
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute(
        """
        UPDATE dealerships
        SET google_place_id = ?,
            google_rating = ?,
            google_review_count = ?,
            google_rating_fetched_at = ?
        WHERE id = ?
        """,
        (
            (place_id or "").strip() or None,
            rating,
            review_count,
            now,
            int(dealer_id),
        ),
    )
    conn.commit()
    conn.close()


def get_dealer_sticker_provider_row(dealer_id: int) -> dict[str, Any] | None:
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    cursor.execute(
        """
        SELECT id, sticker_provider, sticker_ipacket_fail_count, sticker_provider_updated_at
        FROM dealerships WHERE id = ?
        """,
        (int(dealer_id),),
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def update_dealer_sticker_provider(
    dealer_id: int,
    provider: str,
    *,
    source: str = "",
    reset_fail_count: bool = False,
) -> None:
    conn = get_conn()
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    now = datetime.now(timezone.utc).isoformat()
    if reset_fail_count:
        cursor.execute(
            """
            UPDATE dealerships
            SET sticker_provider = ?, sticker_ipacket_fail_count = 0, sticker_provider_updated_at = ?
            WHERE id = ?
            """,
            (provider, now, int(dealer_id)),
        )
    else:
        cursor.execute(
            """
            UPDATE dealerships
            SET sticker_provider = ?, sticker_provider_updated_at = ?
            WHERE id = ?
            """,
            (provider, now, int(dealer_id)),
        )
    conn.commit()
    conn.close()
    if source:
        logger = __import__("logging").getLogger(__name__)
        logger.debug("dealer %s sticker_provider=%s (%s)", dealer_id, provider, source)


def bump_dealer_ipacket_fail_count(dealer_id: int) -> int:
    conn = get_conn()
    cursor = conn.cursor()
    ensure_dealerships_table(cursor)
    cursor.execute(
        """
        UPDATE dealerships
        SET sticker_ipacket_fail_count = COALESCE(sticker_ipacket_fail_count, 0) + 1
        WHERE id = ?
        """,
        (int(dealer_id),),
    )
    cursor.execute(
        "SELECT sticker_ipacket_fail_count FROM dealerships WHERE id = ?",
        (int(dealer_id),),
    )
    row = cursor.fetchone()
    conn.commit()
    conn.close()
    try:
        return int(row[0]) if row else 1
    except (TypeError, ValueError, IndexError):
        return 1


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
