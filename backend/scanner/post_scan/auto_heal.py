"""
Quality-driven auto-heal: after a dealer scan, if key fields look bad across
the lot, go back and re-acquire them per car through the layered gap-fill
methods (structured backfill → listing-page HTML → DDG hints → window sticker).

This is the closing of the loop: scan-time recovery fixes junk feeds at the
source, and this pass catches whatever still upserted with holes — no manual
flags required. Disable with SCANNER_AUTO_HEAL=0.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from backend.scanner.post_scan.coverage_report import _has_value

logger = logging.getLogger("scanner")

# A field triggers healing when its lot-wide coverage drops below this floor.
_KEY_FIELD_MIN_PCT: dict[str, float] = {
    "price": 60.0,
    "exterior_color": 60.0,
    "interior_color": 50.0,
    "mileage": 60.0,
    "trim": 50.0,
}
_MIN_LOT_SIZE = 10


def auto_heal_enabled() -> bool:
    raw = (os.environ.get("SCANNER_AUTO_HEAL") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def deficient_fields(coverage_report: dict[str, Any]) -> dict[str, float]:
    """Key fields whose coverage sits below the heal floor, with their pct."""
    cov = coverage_report.get("coverage") or {}
    out: dict[str, float] = {}
    for field, floor in _KEY_FIELD_MIN_PCT.items():
        pct = (cov.get(field) or {}).get("pct")
        if pct is not None and pct < floor:
            out[field] = pct
    return out


def run_auto_heal_for_dealer(
    dealer_id: str,
    dealer_name: str,
    coverage_report: dict[str, Any],
    vehicles: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """
    Blocking (call via asyncio.to_thread). Returns heal stats, or None when
    nothing needed healing.
    """
    if not auto_heal_enabled():
        return None
    if coverage_report.get("count", 0) < _MIN_LOT_SIZE:
        return None
    fields = deficient_fields(coverage_report)
    if not fields:
        return None

    vins: list[str] = []
    seen: set[str] = set()
    for v in vehicles:
        vin = str(v.get("vin") or "").strip().upper()
        if len(vin) != 17 or vin in seen:
            continue
        if any(not _has_value(v, f) for f in fields):
            seen.add(vin)
            vins.append(vin)
    if not vins:
        return None

    logger.info(
        "Auto-heal [%s]: %s below floor — re-acquiring %d car(s) via gap-fill layers",
        dealer_name,
        ", ".join(f"{f}={p:.0f}%" for f, p in sorted(fields.items())),
        len(vins),
    )
    from backend.scanner.post_scan.gap_fill import run_listing_gap_fill_for_vins

    stats = run_listing_gap_fill_for_vins(vins)
    logger.info(
        "Auto-heal [%s]: done — %d row(s) patched, %d page fetch(es), %d structured backfill(s)",
        dealer_name,
        stats.get("rows_patched", 0),
        stats.get("web_fetch_ok", 0),
        stats.get("structured_backfill_applied", 0),
    )
    return {"deficient_fields": fields, "vins_targeted": len(vins), **stats}
