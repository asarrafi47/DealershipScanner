"""The per-dealer summary line (JSON) and the "Dealer complete" log."""
from __future__ import annotations

import json
import logging
from typing import Any

from backend.scanner import scan_log

logger = logging.getLogger("scanner")


def emit_dealer_run_summary(result: dict[str, Any]) -> None:
    """One parseable INFO line per dealer (JSON), capped ~2KB for log pipelines."""
    payload: dict[str, Any] = {
        "dealer_id": result.get("dealer_id"),
        "intercept_count": result.get("intercept_count"),
        "filtered_count": result.get("filtered_count"),
        "inventory_rows": result.get("inventory_rows"),
        "deduped_rows": result.get("deduped_rows"),
        "vdps_visited": result.get("vdps_visited"),
        "gallery_bins": result.get("gallery_bins"),
        "gallery_vision": result.get("gallery_vision"),
        "monroney_vision": result.get("monroney_vision"),
        "seconds": round(float(result.get("seconds") or 0.0), 2),
        "upserted": result.get("upserted"),
    }
    if result.get("vin_owner_conflicts"):
        payload["vin_owner_conflicts"] = result.get("vin_owner_conflicts")
    err = result.get("error")
    if err:
        payload["error"] = str(err)[:400]
    rec = result.get("reconcile")
    if isinstance(rec, dict):
        payload["reconcile"] = {
            "ran": rec.get("ran"),
            "scraped_candidates": rec.get("scraped_candidates"),
            "marked_inactive": rec.get("marked_inactive"),
            "skipped_reason": rec.get("skipped_reason"),
        }
    phases = result.get("phase_secs")
    if isinstance(phases, dict) and phases:
        payload["phase_secs"] = phases
    line = json.dumps(payload, separators=(",", ":"), default=str, ensure_ascii=False)
    if len(line) > 2048:
        line = line[:2045] + "..."
    logger.info("dealer_run_summary %s", line)
    scan_log.log_dealer_summary(result, result.get("provider", ""))


def log_dealer_complete(name: str, result: dict[str, Any], suffix: str = "") -> None:
    logger.info(
        "Dealer complete: %s (%d inventory rows, %d deduped, %d VDP visited, %d VDP-enriched, %d upserted, %.1fs)"
        + suffix,
        name,
        result["inventory_rows"],
        result["deduped_rows"],
        result["vdps_visited"],
        result["vehicles_vdp_enriched"],
        result["upserted"],
        result["seconds"],
    )
