"""Persistence for ``dealer_reviews`` (Postgres in prod/dev, SQLite for tests).

Keyed by ``dealer_id`` (the hostname-derived ``cars.dealer_id``, e.g.
``longotoyota-com``) — the same key the dealership research page resolves. A
user may hold at most ONE review per dealer (``UNIQUE(dealer_id, user_id)``); a
re-submit edits the existing row. Writes go through the shared inventory
connection (:func:`backend.db.inventory_db.get_conn`), so ``?`` placeholders are
auto-adapted to ``%s`` on Postgres by the compat cursor.

Modeled on ``backend.scanner.specials.store``.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import inventory_pg

# Threshold at which a review auto-flips from ``published`` to ``flagged``.
REPORT_FLAG_THRESHOLD = 3

_DDL_PG = """
CREATE TABLE IF NOT EXISTS dealer_reviews (
    id BIGSERIAL PRIMARY KEY,
    dealer_id TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    display_name TEXT,
    is_anonymous INTEGER NOT NULL DEFAULT 0,
    rating INTEGER NOT NULL,
    body TEXT NOT NULL,
    addon_fee_reported INTEGER NOT NULL DEFAULT 0,
    addon_fee_amount DOUBLE PRECISION,
    addon_fee_desc TEXT,
    status TEXT NOT NULL DEFAULT 'published',
    report_count INTEGER NOT NULL DEFAULT 0,
    ip_hash TEXT,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE (dealer_id, user_id)
)
"""

_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS dealer_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dealer_id TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    display_name TEXT,
    is_anonymous INTEGER NOT NULL DEFAULT 0,
    rating INTEGER NOT NULL,
    body TEXT NOT NULL,
    addon_fee_reported INTEGER NOT NULL DEFAULT 0,
    addon_fee_amount REAL,
    addon_fee_desc TEXT,
    status TEXT NOT NULL DEFAULT 'published',
    report_count INTEGER NOT NULL DEFAULT 0,
    ip_hash TEXT,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE (dealer_id, user_id)
)
"""


# One row per (review, reporter) so a single actor's repeated reports count
# once — three reports flip a review to 'flagged', so without this dedup one
# unauthenticated client could hide any review by itself.
_DDL_REPORTS_PG = """
CREATE TABLE IF NOT EXISTS review_reports (
    review_id BIGINT NOT NULL,
    ip_hash TEXT NOT NULL,
    created_at TEXT,
    UNIQUE (review_id, ip_hash)
)
"""

_DDL_REPORTS_SQLITE = """
CREATE TABLE IF NOT EXISTS review_reports (
    review_id INTEGER NOT NULL,
    ip_hash TEXT NOT NULL,
    created_at TEXT,
    UNIQUE (review_id, ip_hash)
)
"""


def ensure_reviews_table(cur: Any) -> None:
    """Create the ``dealer_reviews`` table + index if absent (idempotent).

    Accepts a cursor (matching the ``ensure_*_table(cur)`` signature requested);
    it does not commit — the caller owns the transaction.
    """
    ddl = _DDL_PG if inventory_pg.is_inventory_postgres() else _DDL_SQLITE
    cur.execute(ddl)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealer_reviews_dealer "
        "ON dealer_reviews(dealer_id, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealer_reviews_user "
        "ON dealer_reviews(user_id, created_at)"
    )


def ensure_review_reports_table(cur: Any) -> None:
    """Create the ``review_reports`` dedup table if absent (idempotent)."""
    ddl = _DDL_REPORTS_PG if inventory_pg.is_inventory_postgres() else _DDL_REPORTS_SQLITE
    cur.execute(ddl)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_review(
    conn: Any,
    dealer_id: str,
    user_id: int,
    *,
    rating: int,
    body: str,
    display_name: str | None = None,
    is_anonymous: bool = False,
    addon_fee_reported: bool = False,
    addon_fee_amount: float | None = None,
    addon_fee_desc: str | None = None,
    ip_hash: str | None = None,
) -> None:
    """Insert or replace the user's single review for this dealer.

    On re-submit the prior row is updated in place (``created_at`` preserved,
    ``updated_at`` refreshed) and ``status``/``report_count`` are reset so an
    edited review re-enters the ``published`` state.
    """
    cur = conn.cursor()
    ensure_reviews_table(cur)
    now = _now_iso()
    anon = 1 if is_anonymous else 0
    fee_reported = 1 if addon_fee_reported else 0
    cur.execute(
        """
        INSERT INTO dealer_reviews (
            dealer_id, user_id, display_name, is_anonymous, rating, body,
            addon_fee_reported, addon_fee_amount, addon_fee_desc,
            status, report_count, ip_hash, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', 0, ?, ?, ?)
        ON CONFLICT (dealer_id, user_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            is_anonymous = EXCLUDED.is_anonymous,
            rating = EXCLUDED.rating,
            body = EXCLUDED.body,
            addon_fee_reported = EXCLUDED.addon_fee_reported,
            addon_fee_amount = EXCLUDED.addon_fee_amount,
            addon_fee_desc = EXCLUDED.addon_fee_desc,
            status = 'published',
            report_count = 0,
            ip_hash = EXCLUDED.ip_hash,
            updated_at = EXCLUDED.updated_at
        """,
        (
            dealer_id, int(user_id), display_name, anon, int(rating), body,
            fee_reported, addon_fee_amount, addon_fee_desc,
            ip_hash, now, now,
        ),
    )
    conn.commit()


def get_user_review(conn: Any, dealer_id: str, user_id: int) -> dict | None:
    """Return the user's own review for this dealer (any status), or ``None``."""
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM dealer_reviews WHERE dealer_id = ? AND user_id = ?",
            (dealer_id, int(user_id)),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_reviews_for_dealer(conn: Any, dealer_id: str, limit: int = 100) -> list[dict]:
    """Published reviews for a dealer, newest first (capped by *limit*)."""
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, dealer_id, user_id, display_name, is_anonymous, rating,
                   body, addon_fee_reported, addon_fee_amount, addon_fee_desc,
                   status, report_count, created_at, updated_at
            FROM dealer_reviews
            WHERE dealer_id = ? AND status = 'published'
            ORDER BY COALESCE(created_at, '') DESC, id DESC
            LIMIT ?
            """,
            (dealer_id, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        # Table may not exist yet on a dealer page viewed before any review.
        return []


def review_summary(conn: Any, dealer_id: str) -> dict:
    """Aggregate published reviews for a dealer.

    Returns ``{count, avg_rating, addon_fee_count, addon_fees:[{amount,desc}]}``.
    ``addon_fees`` lists the reported (amount, description) pairs so the page can
    surface the mandatory-fee warning prominently.
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    empty = {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS count,
                   AVG(rating) AS avg_rating,
                   SUM(CASE WHEN addon_fee_reported = 1 THEN 1 ELSE 0 END) AS addon_fee_count
            FROM dealer_reviews
            WHERE dealer_id = ? AND status = 'published'
            """,
            (dealer_id,),
        )
        row = cur.fetchone()
        count = int(row["count"] or 0) if row else 0
        if not count:
            return empty
        avg = row["avg_rating"]
        avg_rating = round(float(avg), 1) if avg is not None else None
        addon_fee_count = int(row["addon_fee_count"] or 0)

        addon_fees: list[dict] = []
        if addon_fee_count:
            cur.execute(
                """
                SELECT addon_fee_amount, addon_fee_desc
                FROM dealer_reviews
                WHERE dealer_id = ? AND status = 'published'
                      AND addon_fee_reported = 1
                ORDER BY COALESCE(created_at, '') DESC, id DESC
                """,
                (dealer_id,),
            )
            for r in cur.fetchall():
                addon_fees.append(
                    {"amount": r["addon_fee_amount"], "desc": r["addon_fee_desc"]}
                )
        return {
            "count": count,
            "avg_rating": avg_rating,
            "addon_fee_count": addon_fee_count,
            "addon_fees": addon_fees,
        }
    except Exception:
        return empty


def _review_state(conn: Any, review_id: int) -> dict | None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT id, report_count, status FROM dealer_reviews WHERE id = ?",
        (int(review_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {"report_count": int(row["report_count"] or 0), "status": row["status"]}


def increment_report(conn: Any, review_id: int, reporter_hash: str | None = None) -> dict | None:
    """Bump ``report_count`` for a review; auto-flag at the threshold.

    When *reporter_hash* is given, a given reporter counts at most once per
    review (``review_reports`` UNIQUE (review_id, ip_hash)): a repeat report by
    the same actor is idempotent and does NOT bump the count, so no single actor
    can reach ``REPORT_FLAG_THRESHOLD`` alone. Returns ``{report_count, status}``
    for the row, or ``None`` if it is absent.
    """
    cur = conn.cursor()
    ensure_reviews_table(cur)

    if reporter_hash:
        ensure_review_reports_table(cur)
        # First confirm the review exists (a report for a missing review is a 404,
        # not a silent no-op that would still record a dedup row).
        state = _review_state(conn, int(review_id))
        if state is None:
            conn.commit()
            return None
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO review_reports (review_id, ip_hash, created_at) "
            "VALUES (?, ?, ?) ON CONFLICT (review_id, ip_hash) DO NOTHING",
            (int(review_id), reporter_hash, _now_iso()),
        )
        if not cur.rowcount:
            # Already reported by this actor — return current state, no bump.
            conn.commit()
            return state

    cur.execute(
        "UPDATE dealer_reviews SET report_count = report_count + 1, updated_at = ? "
        "WHERE id = ?",
        (_now_iso(), int(review_id)),
    )
    conn.row_factory = sqlite3.Row
    cur.execute(
        "SELECT id, report_count, status FROM dealer_reviews WHERE id = ?",
        (int(review_id),),
    )
    row = cur.fetchone()
    if not row:
        conn.commit()
        return None
    report_count = int(row["report_count"] or 0)
    status = row["status"]
    if report_count >= REPORT_FLAG_THRESHOLD and status == "published":
        cur.execute(
            "UPDATE dealer_reviews SET status = 'flagged' WHERE id = ?",
            (int(review_id),),
        )
        status = "flagged"
    conn.commit()
    return {"report_count": report_count, "status": status}


def count_recent_reviews_by_user(conn: Any, user_id: int, within_hours: int = 1) -> int:
    """Number of reviews this user submitted within the rolling window (rate limit)."""
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    since = (datetime.now(timezone.utc) - timedelta(hours=within_hours)).isoformat()
    try:
        cur.execute(
            "SELECT COUNT(*) AS n FROM dealer_reviews "
            "WHERE user_id = ? AND COALESCE(created_at, '') >= ?",
            (int(user_id), since),
        )
        row = cur.fetchone()
        return int(row["n"] or 0) if row else 0
    except Exception:
        return 0
