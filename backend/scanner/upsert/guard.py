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

from backend.scanner.upsert import ownership as _own

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
    *,
    moves: list[_own.Move] | None = None,
    reasons: dict[str, str] | None = None,
) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    """Read stored provenance + ownership for every VIN, 500 at a time.

    Returns ``(existing_spec_src, conflicts)``: the stored ``spec_source_json``
    per VIN (merged into, not replaced by, the incoming provenance), and
    ``vin -> (owner, claimant)`` for VINs the guard refuses.

    The decision is :func:`backend.scanner.upsert.ownership.decide` (evidence tiers,
    group pools, aliases; the window only settles evidence-less contests outside a
    pool) plus a hold for VINs a person moved by hand. ``reasons`` receives the
    refusal reason per VIN; ``moves`` receives every allowed owner change, for the
    caller to write to ``car_move_log`` once the rows have landed.
    """
    existing_spec_src: dict[str, str] = {}
    conflicts: dict[str, tuple[str, str]] = {}
    pools = _own.load_pools()
    vin_keys = list(by_vin.keys())
    for i in range(0, len(vin_keys), _PREFETCH_CHUNK):
        chunk = vin_keys[i : i + _PREFETCH_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        cursor.execute(
            "SELECT vin, spec_source_json, dealer_id, listing_active, scraped_at, "
            "rooftop_tier, dealer_name, dealership_registry_id "
            f"FROM cars WHERE vin IN ({placeholders})",
            chunk,
        )
        contested: list[tuple[str, str, str, Any, Any, Any, Any, Any]] = []
        for row in cursor.fetchall():
            if row[1] is not None and str(row[1]).strip():
                existing_spec_src[str(row[0])] = str(row[1])
            if len(row) >= 5:
                _vin = str(row[0])
                _claimant = str((by_vin.get(_vin) or {}).get("dealer_id") or "").strip()
                _owner = str(row[2] or "").strip()
                if _owner and _claimant and _owner != _claimant:
                    contested.append((_vin, _owner, _claimant, row[3], row[4],
                                      row[5] if len(row) > 5 else None,
                                      row[6] if len(row) > 6 else "", row[7] if len(row) > 7 else None))
        held = _own.manual_holds(cursor, [c[0] for c in contested]) if window.on and contested else set()
        for _vin, _owner, _claimant, _active, _scraped, _otier, _oname, _oreg in contested:
            _src = by_vin.get(_vin) or {}
            _ctier = _src.get("_rooftop_evidence_tier")
            _is_active = True
            try:
                _is_active = _active is None or int(_active) == 1
            except (TypeError, ValueError):
                pass
            _fresh = bool(window.on and owned_elsewhere(_owner, _active, _scraped, _claimant, window.cutoff))
            verdict = _own.decide(_owner, _otier, _is_active, _fresh, _claimant, _ctier, pools)
            if window.on and _vin in held:
                verdict = _own.Verdict(False, "manual_move")
            if window.on and not verdict.allow:
                conflicts[_vin] = (_owner, _claimant)
                if reasons is not None:
                    reasons[_vin] = verdict.reason
            elif moves is not None:
                moves.append(_own.Move(
                    _vin, _owner, str(_oname or ""), _oreg, _claimant, verdict.reason,
                    json.dumps({"claimant_tier": _own.norm_tier(_ctier), "owner_tier": _own.norm_tier(_otier)},
                               separators=(",", ":")),
                ))
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
    reasons: dict[str, str] | None = None,
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
        # Rule 4 (N8.6): evidence-less contests between stores no pool covers; the pair list
        # is what group_pools.json (D-N3) is extended from.
        pairs: dict[str, int] = {}
        for vin, (owner, claimant) in conflicts.items():
            if (reasons or {}).get(vin) == "group_feed_unstamped":
                key = f"{owner}>{claimant}"
                pairs[key] = pairs.get(key, 0) + 1
        stats["group_feed_unstamped"] = pairs
        if reasons:
            by_reason: dict[str, int] = {}
            for vin in conflicts:
                r = reasons.get(vin, "backstop")
                by_reason[r] = by_reason.get(r, 0) + 1
            stats["vin_owner_conflict_reasons"] = by_reason


def reset_stats(stats: dict | None) -> None:
    """Reset the guard counters so a retried write does not double count."""
    if stats is not None:
        stats["vin_owner_conflicts"] = 0
        stats["vin_owner_conflict_vins"] = []
        stats["vin_owner_conflict_owners"] = {}
        stats["group_feed_unstamped"] = {}
        stats["vin_owner_conflict_reasons"] = {}
        stats["car_moves_logged"] = 0
