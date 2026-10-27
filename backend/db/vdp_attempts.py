"""Per-VIN attempt ledger for the HTTP-first VDP prefetch (N18 P12A.5, W05b, IL-12).

``vdp_fetch_attempts`` (migrations/V035) holds one row per (dealer, VIN) whose detail page
could not be fetched. The prefetch skips a car that has failed ``attempt_max()`` times until
a back-off has passed (``backoff_hours`` doubles with every further failure, capped), so a
blocked or dead URL stops eating the dealer's wall clock on every run. A fetched page
deletes the row. The table comes from the migration chain only (no runtime DDL).

Every function is best-effort: a ledger that cannot be read or written never blocks a scan.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)

_CHUNK = 400
_MAX_BACKOFF_HOURS = 14 * 24.0

def ledger_enabled() -> bool:
    """``SCANNER_VDP_ATTEMPT_LEDGER`` (default on)."""
    return (os.environ.get("SCANNER_VDP_ATTEMPT_LEDGER") or "1").strip().lower() not in ("0", "false", "no", "off")


def attempt_max() -> int:
    """``SCANNER_VDP_ATTEMPT_MAX``: failed attempts before a car is deferred (default 3)."""
    try:
        return max(1, int((os.environ.get("SCANNER_VDP_ATTEMPT_MAX") or "3").strip()))
    except ValueError:
        return 3


def base_backoff_hours() -> float:
    """``SCANNER_VDP_ATTEMPT_BACKOFF_H``: wait after the max-th failure (default 72 h)."""
    try:
        return max(0.0, float((os.environ.get("SCANNER_VDP_ATTEMPT_BACKOFF_H") or "72").strip()))
    except ValueError:
        return 72.0


def backoff_hours(attempts: int) -> float:
    """Hours to wait after *attempts* failures: 0 below the max, then the base doubling per
    extra failure, capped at 14 days."""
    over = int(attempts) - attempt_max()
    if over < 0:
        return 0.0
    return min(_MAX_BACKOFF_HOURS, base_backoff_hours() * (2 ** over))


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _parse(ts: Any) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(ts).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def is_deferred(attempts: int, last_attempt_at: Any, now: datetime | None = None) -> bool:
    """True while a car with *attempts* failures is inside its back-off window."""
    wait = backoff_hours(attempts)
    if wait <= 0:
        return False
    last = _parse(last_attempt_at)
    if last is None:
        return False
    return (now or _now()) < last + timedelta(hours=wait)


def load(dealer_id: str, vins: Iterable[str]) -> dict[str, tuple[int, str]]:
    """``{vin: (attempts, last_attempt_at)}`` for the dealer's VINs that have a row; ``{}`` on
    any failure."""
    vin_list = sorted({str(v or "").strip().upper() for v in vins if v})
    if not dealer_id or not vin_list:
        return {}
    out: dict[str, tuple[int, str]] = {}
    try:
        from backend.db.inventory_db import db_conn

        with db_conn() as conn:
            cur = conn.cursor()
            for i in range(0, len(vin_list), _CHUNK):
                chunk = vin_list[i:i + _CHUNK]
                ph = ",".join("?" * len(chunk))
                cur.execute(
                    f"SELECT vin, attempts, last_attempt_at FROM vdp_fetch_attempts "
                    f"WHERE dealer_id = ? AND vin IN ({ph})",
                    [dealer_id, *chunk],
                )
                for row in cur.fetchall():
                    out[str(row[0])] = (int(row[1] or 0), str(row[2] or ""))
    except Exception as exc:  # noqa: BLE001 - a ledger read must never block the scan
        logger.debug("vdp_attempts: load skipped: %s", exc)
        return {}
    return out


def record(
    dealer_id: str,
    failed: Sequence[tuple[str, int | None, str]],
    fetched: Iterable[str] = (),
) -> int:
    """Count one failed attempt for each ``(vin, last_status, outcome)`` in *failed* and delete
    the rows of the *fetched* VINs. Returns the number of rows written; 0 on any failure."""
    fetched_vins = sorted({str(v or "").strip().upper() for v in fetched if v})
    if not dealer_id or not (failed or fetched_vins):
        return 0
    now = _now().isoformat()
    n = 0
    try:
        from backend.db.inventory_db import db_conn

        with db_conn() as conn:
            cur = conn.cursor()
            for vin, status, outcome in failed:
                vin = str(vin or "").strip().upper()
                if not vin:
                    continue
                cur.execute(
                    "INSERT INTO vdp_fetch_attempts "
                    "(dealer_id, vin, attempts, last_status, last_outcome, first_attempt_at, last_attempt_at) "
                    "VALUES (?, ?, 1, ?, ?, ?, ?) "
                    "ON CONFLICT (dealer_id, vin) DO UPDATE SET "
                    "attempts = vdp_fetch_attempts.attempts + 1, last_status = excluded.last_status, "
                    "last_outcome = excluded.last_outcome, last_attempt_at = excluded.last_attempt_at",
                    (dealer_id, vin, status, outcome[:60], now, now),
                )
                n += 1
            for i in range(0, len(fetched_vins), _CHUNK):
                chunk = fetched_vins[i:i + _CHUNK]
                ph = ",".join("?" * len(chunk))
                cur.execute(f"DELETE FROM vdp_fetch_attempts WHERE dealer_id = ? AND vin IN ({ph})", [dealer_id, *chunk])
            conn.commit()
    except Exception as exc:  # noqa: BLE001 - a ledger write must never block the scan
        logger.warning("vdp_attempts: write skipped: %s", exc)
        return 0
    return n
