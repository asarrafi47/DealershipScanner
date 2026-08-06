"""
Record what the rooftop attribution gate refused, and why.

The gate in :mod:`backend.parsers` already decides correctly: when a group feed carries
several rooftops and none of them is the store being scanned, it drops the rows rather
than attributing another dealer's cars to this one. That decision is right and it fires
around 1,106 times per scan.

Until now it left nothing behind. The reason went onto an in-memory dict, the rows were
discarded, and a WARNING went into a log that nobody reads -- while burying every other
warning in the run. The evidence the misattribution investigation needs was being computed
and thrown away, over a thousand times a scan.

So this module writes it down. Two properties matter:

**It must never break a scan.** Every failure is swallowed. A refusal that goes unrecorded
costs a row of history; an exception here would cost a dealer's entire inventory run.

**It must not slow the hot path.** Refusals are buffered per dealer and flushed once, so a
feed that refuses 1,100 rows produces one INSERT rather than 1,100.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Iterable

_log = logging.getLogger(__name__)

# dealer_id -> reason -> {"rows": int, "rooftops": set[str]}
_pending: dict[str, dict[str, dict[str, Any]]] = {}
_lock = threading.Lock()


def ledger_enabled() -> bool:
    """On unless explicitly disabled; set ROOFTOP_LEDGER=0 to turn it off."""
    return (os.environ.get("ROOFTOP_LEDGER") or "1").strip().lower() not in ("0", "false", "no")


def note_refusal(
    dealer_id: str,
    reason: str,
    rows_refused: int,
    rooftops_seen: Iterable[str] | None = None,
) -> None:
    """
    Buffer one refusal. Cheap, non-blocking, never raises.

    Call once per refusal decision, not per refused row -- the gate refuses a whole
    payload at a time, and ``rows_refused`` carries the count.
    """
    if not ledger_enabled() or not dealer_id or not reason:
        return
    try:
        names = {str(n).strip() for n in (rooftops_seen or []) if str(n).strip()}
        with _lock:
            by_reason = _pending.setdefault(dealer_id, {})
            entry = by_reason.setdefault(reason, {"rows": 0, "rooftops": set()})
            entry["rows"] += max(0, int(rows_refused))
            entry["rooftops"].update(names)
    except Exception:  # noqa: BLE001 - a ledger must not break a scan
        pass


def flush(dealer_id: str | None = None) -> int:
    """
    Write buffered refusals to ``rooftop_refusals``. Returns rows written.

    Safe to call at the end of a dealer run, or with no argument at the end of a sweep.
    Any failure -- no database, missing table, closed connection -- is swallowed and the
    buffer is dropped rather than retried; this is history, not inventory.
    """
    if not ledger_enabled():
        return 0
    with _lock:
        if dealer_id is not None:
            batch = {dealer_id: _pending.pop(dealer_id, {})}
        else:
            batch, _pending_clear = dict(_pending), _pending.clear()
        if not any(batch.values()):
            return 0

    written = 0
    try:
        from backend.db.inventory_pg import is_inventory_postgres, pg_connect

        if not is_inventory_postgres():
            return 0
        conn = pg_connect()
        try:
            cur = conn.cursor()
            for did, by_reason in batch.items():
                for reason, entry in by_reason.items():
                    names = sorted(entry["rooftops"])
                    cur.execute(
                        "INSERT INTO rooftop_refusals "
                        "(dealer_id, reason, rows_refused, rooftops_seen, rooftop_count) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (did, reason, entry["rows"], json.dumps(names), len(names)),
                    )
                    written += 1
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        _log.debug("rooftop ledger flush failed (ignored): %s", str(exc)[:120])
        return 0
    return written
