"""VIN ownership guard steps of :func:`backend.scanner.database.upsert_vehicles`.

The policy (window, predicate, conflict table) lives in
``backend.scanner.database`` (``vin_owner_guard_hours``, ``_vin_owned_elsewhere``,
``record_vin_owner_conflicts``); the orchestrator passes those in at call time
so tests that monkeypatch them on that module keep working.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, NamedTuple

# Same logger name as before the split so log routing and captures are unchanged.
logger = logging.getLogger("backend.scanner.database")

_PREFETCH_CHUNK = 500


class GuardWindow(NamedTuple):
    hours: float
    on: bool
    cutoff: datetime
    # Same text shape as ``scraped_at`` (isoformat + "Z") so the SQL backstop
    # compares like with like on both SQLite and Postgres (TEXT column).
    cutoff_iso: str


def guard_window(hours: float) -> GuardWindow:
    on = hours > 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours if on else 0)
    return GuardWindow(hours, on, cutoff, cutoff.replace(tzinfo=None).isoformat() + "Z")


def prefetch_existing(
    cursor,
    by_vin: dict[str, dict],
    window: GuardWindow,
    owned_elsewhere: Callable[..., bool],
) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    """Read stored provenance + ownership for every VIN, 500 at a time.

    Returns ``(existing_spec_src, conflicts)``: the stored ``spec_source_json``
    per VIN (merged into, not replaced by, the incoming provenance), and
    ``vin -> (owner, claimant)`` for VINs the guard refuses.
    """
    existing_spec_src: dict[str, str] = {}
    conflicts: dict[str, tuple[str, str]] = {}
    vin_keys = list(by_vin.keys())
    for i in range(0, len(vin_keys), _PREFETCH_CHUNK):
        chunk = vin_keys[i : i + _PREFETCH_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        cursor.execute(
            "SELECT vin, spec_source_json, dealer_id, listing_active, scraped_at "
            f"FROM cars WHERE vin IN ({placeholders})",
            chunk,
        )
        for row in cursor.fetchall():
            if row[1] is not None and str(row[1]).strip():
                existing_spec_src[str(row[0])] = str(row[1])
            if window.on and len(row) >= 5:
                _vin = str(row[0])
                _claimant = str((by_vin.get(_vin) or {}).get("dealer_id") or "").strip()
                if owned_elsewhere(row[2], row[3], row[4], _claimant, window.cutoff):
                    conflicts[_vin] = (str(row[2]).strip(), _claimant)
    return existing_spec_src, conflicts


def backstop_owner(cursor, vin: str) -> str:
    """dealer_id of the row the SQL backstop refused to move ('' if unreadable)."""
    try:
        cursor.execute("SELECT dealer_id FROM cars WHERE vin = ?", (vin,))
        _r = cursor.fetchone()
        return str(_r[0] or "").strip() if _r else ""
    except Exception:
        logger.debug("upsert step backstop_owner failed for %s", vin, exc_info=True)
        return ""


def report_conflicts(
    conflicts: dict[str, tuple[str, str]],
    by_vin: dict[str, dict],
    window_hours: float,
    stats: dict | None,
    record: Callable[[list[tuple[str, str, str]], str], Any],
) -> None:
    """Drop refused VINs from ``by_vin``, log the summary, record the pairs, fill ``stats``."""
    for _vin in conflicts:
        by_vin.pop(_vin, None)  # post-write steps must not touch the owner's row
    by_owner: dict[str, int] = {}
    by_claimant: dict[str, int] = {}
    for owner, claimant in conflicts.values():
        by_owner[owner] = by_owner.get(owner, 0) + 1
        by_claimant[claimant] = by_claimant.get(claimant, 0) + 1
    logger.warning(
        "vin_owner_guard %s",
        json.dumps(
            {
                "skipped": len(conflicts),
                "claimants": by_claimant,
                "owners": by_owner,
                "window_hours": window_hours,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    )
    record(
        [(vin, owner, claimant) for vin, (owner, claimant) in sorted(conflicts.items())],
        datetime.utcnow().isoformat() + "Z",
    )
    if stats is not None:
        stats["vin_owner_conflicts"] = len(conflicts)
        stats["vin_owner_conflict_vins"] = sorted(conflicts)
        stats["vin_owner_conflict_owners"] = by_owner


def reset_stats(stats: dict | None) -> None:
    """Reset the guard counters so a retried write does not double count."""
    if stats is not None:
        stats["vin_owner_conflicts"] = 0
        stats["vin_owner_conflict_vins"] = []
        stats["vin_owner_conflict_owners"] = {}
