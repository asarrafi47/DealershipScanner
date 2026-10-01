"""The write: upsert, VIN-owner conflicts, scan_log."""
from __future__ import annotations

import logging
import time
from typing import Any

from backend.scanner import scan_log
from backend.scanner.phases.dealer_run_steps.enrich import _capture_coverage
from backend.scanner.phases.dealer_run_steps.state import DealerRun
from backend.scanner.phases.upsert import upsert_vehicles_for_dealer

logger = logging.getLogger("scanner")


async def persist(run: DealerRun) -> None:
    result, name = run.result, run.name
    result["capture_coverage"] = _capture_coverage(run.all_vehicles)
    t_up0 = time.perf_counter()
    _upsert_stats: dict[str, Any] = {}
    count = await upsert_vehicles_for_dealer(run.write_coordinator, run.all_vehicles, _upsert_stats)
    result["phase_secs"]["upsert"] = round(time.perf_counter() - t_up0, 2)
    result["upserted"] = count
    scan_log.log_vehicles(run.dealer_id, name, result.get("provider", run.provider), run.all_vehicles)
    _conflicted = set(_upsert_stats.get("vin_owner_conflict_vins") or ())
    result["vin_owner_conflicts"] = len(_conflicted)
    if _conflicted:
        # VIN ownership guard (2026-09-29): another store owns these VINs
        # (active, scraped within SCANNER_VIN_OWNER_GUARD_HOURS). They were
        # not written; drop them so coverage, auto-heal and reconcile below
        # neither touch nor count the owner's rows.
        result["vin_owner_conflict_owners"] = _upsert_stats.get("vin_owner_conflict_owners") or {}
        run.all_vehicles = [
            v for v in run.all_vehicles if (v.get("vin") or "").strip() not in _conflicted
        ]
        logger.warning(
            "VIN owner guard [%s]: %d VIN(s) owned by other dealers were NOT written: %s",
            name, len(_conflicted), result["vin_owner_conflict_owners"],
        )
