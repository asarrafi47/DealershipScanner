"""Persistence for ``dealer_specials`` (Postgres in prod/dev, SQLite for tests).

The table is keyed by ``dealer_id`` (the hostname-derived ``cars.dealer_id``,
e.g. ``tustintoyota-com``) — the safest key for coverage since almost no cars
carry a ``dealership_registry_id`` yet. Writes go through the shared inventory
connection (:func:`backend.db.inventory_db.get_conn`), so ``?`` placeholders are
auto-adapted to ``%s`` on Postgres by the compat cursor.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from backend.db import inventory_pg

# Column order used by INSERT (id / scraped_at handled separately).
_OFFER_COLS = [
    "dealer_id", "title", "type", "vehicle_year", "vehicle_make",
    "vehicle_model", "vehicle_trim", "payment", "term_months",
    "due_at_signing", "mileage_per_year", "msrp", "expires", "fine_print",
    "source_url", "raw_html_snippet", "offer_hash",
]

_DDL_PG = """
CREATE TABLE IF NOT EXISTS dealer_specials (
    id BIGSERIAL PRIMARY KEY,
    dealer_id TEXT NOT NULL,
    title TEXT,
    type TEXT,
    vehicle_year INTEGER,
    vehicle_make TEXT,
    vehicle_model TEXT,
    vehicle_trim TEXT,
    payment DOUBLE PRECISION,
    term_months INTEGER,
    due_at_signing DOUBLE PRECISION,
    mileage_per_year INTEGER,
    msrp DOUBLE PRECISION,
    expires TEXT,
    fine_print TEXT,
    source_url TEXT,
    raw_html_snippet TEXT,
    offer_hash TEXT,
    scraped_at TEXT NOT NULL,
    UNIQUE (dealer_id, offer_hash)
)
"""

_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS dealer_specials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dealer_id TEXT NOT NULL,
    title TEXT,
    type TEXT,
    vehicle_year INTEGER,
    vehicle_make TEXT,
    vehicle_model TEXT,
    vehicle_trim TEXT,
    payment REAL,
    term_months INTEGER,
    due_at_signing REAL,
    mileage_per_year INTEGER,
    msrp REAL,
    expires TEXT,
    fine_print TEXT,
    source_url TEXT,
    raw_html_snippet TEXT,
    offer_hash TEXT,
    scraped_at TEXT NOT NULL,
    UNIQUE (dealer_id, offer_hash)
)
"""


def ensure_dealer_specials_table(conn: Any) -> None:
    """Create the ``dealer_specials`` table + index if absent (idempotent)."""
    ddl = _DDL_PG if inventory_pg.is_inventory_postgres() else _DDL_SQLITE
    cur = conn.cursor()
    cur.execute(ddl)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealer_specials_dealer "
        "ON dealer_specials(dealer_id)"
    )
    conn.commit()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_specials(
    conn: Any,
    dealer_id: str,
    offers: Iterable[dict],
    *,
    replace: bool = True,
) -> int:
    """Persist a dealer's offers. When *replace* (default), the dealer's prior
    rows are cleared first so the stored set reflects the current page — deals
    expire and rotate, so a fresh scan is the source of truth. Returns the
    number of offers written.
    """
    ensure_dealer_specials_table(conn)
    offers = list(offers)
    cur = conn.cursor()
    if replace:
        cur.execute("DELETE FROM dealer_specials WHERE dealer_id = ?", (dealer_id,))

    scraped_at = _now_iso()
    cols = _OFFER_COLS + ["scraped_at"]
    placeholders = ", ".join("?" for _ in cols)
    col_list = ", ".join(cols)
    update_cols = [c for c in cols if c not in ("dealer_id", "offer_hash")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO dealer_specials ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (dealer_id, offer_hash) DO UPDATE SET {set_clause}"
    )

    written = 0
    seen: set[str] = set()
    for off in offers:
        h = off.get("offer_hash")
        if h and h in seen:
            continue
        if h:
            seen.add(h)
        row = [dealer_id if c == "dealer_id" else off.get(c) for c in _OFFER_COLS]
        row.append(scraped_at)
        cur.execute(sql, tuple(row))
        written += 1
    conn.commit()
    return written


def get_specials_for_dealer(conn: Any, dealer_id: str) -> list[dict]:
    """Return stored offers for a dealer, ordered by type then payment.

    Excludes the (large) ``raw_html_snippet`` from the read used for rendering.
    """
    import sqlite3

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, dealer_id, title, type, vehicle_year, vehicle_make,
                   vehicle_model, vehicle_trim, payment, term_months,
                   due_at_signing, mileage_per_year, msrp, expires, fine_print,
                   source_url, scraped_at
            FROM dealer_specials
            WHERE dealer_id = ?
            ORDER BY
                CASE LOWER(COALESCE(type, ''))
                    WHEN 'lease' THEN 0 WHEN 'finance' THEN 1
                    WHEN 'manager' THEN 2 WHEN 'cash' THEN 3 ELSE 4 END,
                COALESCE(payment, 999999) ASC
            """,
            (dealer_id,),
        )
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        # Table may not exist yet on a dealer page viewed before any scan.
        return []
