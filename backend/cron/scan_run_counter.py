"""
Persistent counter for full scanner batch runs (one increment per cron invocation).

Used to gate infrequent maintenance tasks (e.g. dealer Google rating backfill
every N batches).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
COUNTER_PATH = ROOT / "backend" / "dictionary" / "derived" / "scanner_run_counter.json"


def _rating_scan_interval() -> int:
    raw = (os.environ.get("DEALER_GOOGLE_RATING_SCAN_INTERVAL") or "10").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 10


def _load_state() -> dict[str, Any]:
    if not COUNTER_PATH.is_file():
        return {}
    try:
        data = json.loads(COUNTER_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _write_state(state: dict[str, Any]) -> None:
    COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = COUNTER_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(COUNTER_PATH)


def record_scanner_batch_finished(*, ran_dealer_ratings: bool = False) -> dict[str, Any]:
    """
    Increment the batch counter once per completed scanner cron run.

    Returns summary with ``batch_count``, ``interval``, and ``run_dealer_ratings``.
    """
    state = _load_state()
    count = int(state.get("batch_count") or 0) + 1
    interval = _rating_scan_interval()
    run_ratings = count % interval == 0
    now = datetime.now(timezone.utc).isoformat()
    out: dict[str, Any] = {
        "batch_count": count,
        "interval": interval,
        "run_dealer_ratings": run_ratings,
        "last_batch_at": now,
        "last_rating_sync_at": state.get("last_rating_sync_at"),
    }
    if ran_dealer_ratings:
        out["last_rating_sync_at"] = now
    _write_state(
        {
            "batch_count": count,
            "last_batch_at": now,
            "last_rating_sync_at": out["last_rating_sync_at"],
        }
    )
    return out


def mark_dealer_ratings_synced() -> None:
    """Record that a dealer rating backfill completed (does not increment batch count)."""
    state = _load_state()
    state["last_rating_sync_at"] = datetime.now(timezone.utc).isoformat()
    _write_state(state)
