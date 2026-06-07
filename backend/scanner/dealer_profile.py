"""
Per-dealer scan profile: cache winning inventory recovery strategy between runs.

Stored in ``dealer_scan_profile`` (SQLite or Postgres inventory DB).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from backend.db.inventory_db import db_conn

logger = logging.getLogger(__name__)

# Keep in sync with inventory_recovery.RECOVERY_STRATEGY_ORDER
_VALID_STRATEGIES = frozenset({
    "dealer_inspire_algolia",
    "dealer_venom_typesense",
    "pixel_motion_html",
    "dealer_on_cosmos",
    "dealer_eprocess_json",
    "html_next_data",
})


def ensure_dealer_scan_profile_table(cursor: Any) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_scan_profile (
            dealer_id TEXT PRIMARY KEY,
            last_winning_strategy TEXT,
            platform_hints_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
        """
    )


def get_cached_winning_strategy(dealer_id: str) -> str | None:
    did = (dealer_id or "").strip()
    if not did:
        return None
    with db_conn() as conn:
        cur = conn.cursor()
        ensure_dealer_scan_profile_table(cur)
        cur.execute(
            "SELECT last_winning_strategy FROM dealer_scan_profile WHERE dealer_id = ?",
            (did,),
        )
        row = cur.fetchone()
    if not row:
        return None
    val = row[0] if not isinstance(row, dict) else row.get("last_winning_strategy")
    if not val:
        return None
    strat = str(val).strip()
    return strat if strat in _VALID_STRATEGIES else None


def record_winning_strategy(
    dealer_id: str,
    strategy: str | None,
    *,
    platform_hints: set[str] | None = None,
) -> None:
    did = (dealer_id or "").strip()
    strat = (strategy or "").strip()
    if not did or not strat or strat not in _VALID_STRATEGIES:
        return
    hints_json = json.dumps(sorted(platform_hints or []), ensure_ascii=False)
    now = datetime.now(timezone.utc).isoformat()
    with db_conn() as conn:
        cur = conn.cursor()
        ensure_dealer_scan_profile_table(cur)
        cur.execute(
            """
            INSERT INTO dealer_scan_profile (dealer_id, last_winning_strategy, platform_hints_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(dealer_id) DO UPDATE SET
                last_winning_strategy = excluded.last_winning_strategy,
                platform_hints_json = excluded.platform_hints_json,
                updated_at = excluded.updated_at
            """,
            (did, strat, hints_json, now),
        )
        conn.commit()
    logger.info("Dealer profile: cached recovery strategy %s for %s", strat, did)


def prioritize_recovery_chain(
    chain: list[str],
    *,
    cached_strategy: str | None,
) -> list[str]:
    """Move a previously successful strategy to the front (after deduping)."""
    if not cached_strategy or cached_strategy not in chain:
        return list(chain)
    rest = [s for s in chain if s != cached_strategy]
    return [cached_strategy, *rest]


def manifest_recovery_strategies(dealer: dict[str, Any] | None) -> list[str] | None:
    """
    Optional ``recovery_strategies`` on a dealers.json row (ordered list of strategy names).
    Invalid names are dropped; empty after filter → None.
    """
    if not isinstance(dealer, dict):
        return None
    raw = dealer.get("recovery_strategies")
    if raw is None:
        return None
    if not isinstance(raw, list):
        return None
    out: list[str] = []
    for item in raw:
        name = str(item or "").strip()
        if name in _VALID_STRATEGIES and name not in out:
            out.append(name)
    return out or None


def manifest_skip_recovery(dealer: dict[str, Any] | None) -> bool:
    if not isinstance(dealer, dict):
        return False
    raw = dealer.get("skip_recovery")
    if raw is True:
        return True
    if isinstance(raw, str) and raw.strip().lower() in ("1", "true", "yes", "on"):
        return True
    return False
