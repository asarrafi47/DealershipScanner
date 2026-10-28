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

from backend.utils.runtime_state import file_lock, state_path, write_json_atomic

ROOT = Path(__file__).resolve().parents[2]
COUNTER_PATH = state_path("scanner_run_counter.json")


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
    write_json_atomic(COUNTER_PATH, state)


def record_scanner_batch_finished(*, ran_dealer_ratings: bool = False) -> dict[str, Any]:
    """
    Increment the batch counter once per completed scanner cron run.

    Returns summary with ``batch_count``, ``interval``, and ``run_dealer_ratings``.
    """
    with file_lock(COUNTER_PATH):
        return _record_batch_locked(ran_dealer_ratings)


def _record_batch_locked(ran_dealer_ratings: bool) -> dict[str, Any]:
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
    with file_lock(COUNTER_PATH):
        state = _load_state()
        state["last_rating_sync_at"] = datetime.now(timezone.utc).isoformat()
        _write_state(state)
