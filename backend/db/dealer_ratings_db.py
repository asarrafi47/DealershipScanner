"""Per-dealership Google rating cache (``dealer_ratings``).

Split out of :mod:`backend.db.comments_db`, which re-exports every public name
here; callers keep importing from ``comments_db``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg.rows import dict_row

from backend.db.comments_db import _iso, _now_iso
from backend.db.inventory_db import get_conn


# ---------------------------------------------------------------------------
# Dealer ratings (Google Places cache)
# ---------------------------------------------------------------------------

def upsert_dealer_rating(
    dealership_id: int,
    *,
    place_id: str | None,
    rating: float | None,
    review_count: int | None,
    source: str = "google_places",
    fetched_at: str | None = None,
) -> None:
    """Store the current rating for a dealership (insert or replace in place)."""
    now = fetched_at or _now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dealer_ratings (
                dealership_id, google_place_id, google_rating, google_review_count,
                fetched_at, source
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (dealership_id) DO UPDATE SET
                google_place_id = EXCLUDED.google_place_id,
                google_rating = EXCLUDED.google_rating,
                google_review_count = EXCLUDED.google_review_count,
                fetched_at = EXCLUDED.fetched_at,
                source = EXCLUDED.source
            """,
            (
                int(dealership_id),
                (place_id or "").strip() or None,
                rating,
                review_count,
                now,
                source,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_dealer_rating(dealership_id: int) -> dict[str, Any] | None:
    """Cached Google rating for one dealership, or ``None`` if never fetched."""
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            """
            SELECT dealership_id, google_place_id, google_rating, google_review_count,
                   fetched_at, source
            FROM dealer_ratings WHERE dealership_id = ?
            """,
            (int(dealership_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        raw_rating = d.get("google_rating")
        return {
            "dealership_id": int(d["dealership_id"]),
            "google_place_id": d.get("google_place_id"),
            "google_rating": float(raw_rating) if raw_rating is not None else None,
            "google_review_count": (
                int(d["google_review_count"]) if d.get("google_review_count") is not None else None
            ),
            "fetched_at": _iso(d.get("fetched_at")),
            "source": d.get("source"),
        }
    except Exception:
        return None
    finally:
        conn.close()


def list_active_dealership_websites() -> list[dict[str, Any]]:
    """Every active rooftop's id, website columns and coordinates.

    The rating importer folds this into "which domains does more than one
    rooftop share" (echopark.com, carmax.com): on those, matching a website
    proves only that Google found the right *group*, so it also needs each
    sibling's position to work out which store a hit belongs to. Read once per
    run, not once per dealer — it is a few hundred short rows.
    """
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            """
            SELECT id, website_url, dealer_website_url, latitude, longitude
            FROM dealerships
            WHERE is_active = 1 AND duplicate_of_id IS NULL
            """
        )
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        # No registry table (fresh test DB) means no shared domains to guard.
        return []
    finally:
        conn.close()


def list_dealerships_needing_rating(
    *, limit: int = 25, refresh_after_days: int | None = None
) -> list[dict[str, Any]]:
    """The importer's work queue: active registry rows with no (or stale) rating.

    ``refresh_after_days=None`` means "never refresh what we already have" --
    the cheapest mode, and the default, because Google Places bills per call.
    """
    cap = max(1, int(limit))
    conn = get_conn()
    try:
        cur = conn.cursor(row_factory=dict_row)
        params: list[Any] = []
        if refresh_after_days is None:
            freshness = "r.dealership_id IS NULL"
        else:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=max(1, int(refresh_after_days)))
            ).isoformat()
            freshness = "(r.dealership_id IS NULL OR r.fetched_at < ?)"
            params.append(cutoff)
        params.append(cap)
        cur.execute(
            f"""
            SELECT d.id, d.name, d.city, d.state, d.latitude, d.longitude,
                   d.website_url, d.dealer_website_url,
                   COALESCE(r.google_place_id, d.google_place_id) AS google_place_id
            FROM dealerships d
            LEFT JOIN dealer_ratings r ON r.dealership_id = d.id
            WHERE d.is_active = 1
              AND d.duplicate_of_id IS NULL
              AND {freshness}
            ORDER BY d.id ASC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
