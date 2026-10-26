"""Persistence for ``lease_offer_matches`` — cached LLM extractions + inventory
matches for lease specials (Postgres; the table is owned by ``migrations/``).

Keyed by ``(dealer_id, offer_hash)``. ``offer_hash`` is content-derived
(:func:`backend.scanner.specials.extract._offer_hash`), so when a dealer's offer
text changes the hash changes and a fresh row is computed; an unchanged offer
reuses its cached extraction and never re-hits the LLM. This is what keeps the
dealership research page from re-running the model on every request.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_offer_match(
    conn: Any,
    dealer_id: str,
    offer_hash: str,
    *,
    offer_id: int | None,
    extracted: dict | None,
    summary: str | None,
    matches: list[dict] | None,
    confidence: float | None,
) -> None:
    """Store (or refresh) the cached extraction + matches for one lease offer."""
    cur = conn.cursor()
    cols = [
        "dealer_id", "offer_hash", "offer_id", "extracted_json", "summary",
        "matches_json", "match_count", "confidence", "computed_at",
    ]
    placeholders = ", ".join("?" for _ in cols)
    update_cols = [c for c in cols if c not in ("dealer_id", "offer_hash")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO lease_offer_matches ({', '.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (dealer_id, offer_hash) DO UPDATE SET {set_clause}"
    )
    match_list = matches or []
    cur.execute(
        sql,
        (
            dealer_id,
            offer_hash,
            offer_id,
            json.dumps(extracted) if extracted is not None else None,
            summary,
            json.dumps(match_list),
            len(match_list),
            confidence,
            _now_iso(),
        ),
    )
    conn.commit()


def get_offer_match(conn: Any, dealer_id: str, offer_hash: str) -> dict | None:
    """Return the cached match row for one offer, or None if not computed yet."""
    if not offer_hash:
        return None
    cur = conn.cursor(row_factory=dict_row)
    try:
        cur.execute(
            "SELECT dealer_id, offer_hash, offer_id, extracted_json, summary, "
            "matches_json, match_count, confidence, computed_at "
            "FROM lease_offer_matches WHERE dealer_id = ? AND offer_hash = ?",
            (dealer_id, offer_hash),
        )
        row = cur.fetchone()
    except Exception:
        return None
    if not row:
        return None
    d = dict(row)
    d["extracted"] = _loads(d.pop("extracted_json", None))
    d["matches"] = _loads(d.pop("matches_json", None)) or []
    return d


def get_matches_for_dealer(conn: Any, dealer_id: str) -> dict[str, dict]:
    """All cached match rows for a dealer, indexed by ``offer_hash``."""
    cur = conn.cursor(row_factory=dict_row)
    try:
        cur.execute(
            "SELECT dealer_id, offer_hash, offer_id, extracted_json, summary, "
            "matches_json, match_count, confidence, computed_at "
            "FROM lease_offer_matches WHERE dealer_id = ?",
            (dealer_id,),
        )
        rows = cur.fetchall()
    except Exception:
        return {}
    out: dict[str, dict] = {}
    for r in rows:
        d = dict(r)
        d["extracted"] = _loads(d.pop("extracted_json", None))
        d["matches"] = _loads(d.pop("matches_json", None)) or []
        out[d["offer_hash"]] = d
    return out


def _loads(raw: Any) -> Any:
    if not raw:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
