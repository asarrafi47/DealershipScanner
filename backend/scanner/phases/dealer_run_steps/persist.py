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


# Persisted rows under this share of the replay-kept VINs are a gap worth naming (P5B.7).
REPLAY_PERSIST_GAP_SHARE = 0.5


def replay_persist_gap(kept: int, persisted: int, dropped_by: dict[str, int]) -> dict[str, Any] | None:
    """``{kept, persisted, dropped_by}`` when the write kept under half of what the replay kept, else None.

    terrylabonte kept 324 VINs through the gate and wrote 11: nothing in the result said so."""
    if kept <= 0 or persisted >= kept * REPLAY_PERSIST_GAP_SHARE:
        return None
    return {"kept": kept, "persisted": persisted, "dropped_by": {k: v for k, v in dropped_by.items() if v}}


async def persist(run: DealerRun) -> None:
    result, name = run.result, run.name
    from backend.scanner.inventory_reconcile import normalized_vin_set_from_vehicles

    replay_kept = len(normalized_vin_set_from_vehicles(run.all_vehicles))
    result["capture_coverage"] = _capture_coverage(run.all_vehicles)
    t_up0 = time.perf_counter()
    _upsert_stats: dict[str, Any] = {}
    count = await upsert_vehicles_for_dealer(run.write_coordinator, run.all_vehicles, _upsert_stats)
    result["phase_secs"]["upsert"] = round(time.perf_counter() - t_up0, 2)
    result["upserted"] = count
    if isinstance(_upsert_stats.get("timing"), dict) and _upsert_stats["timing"]:
        # Wall and thread-CPU split of the upsert (P11A.7); retry counts are added by the coordinator.
        result["phase_secs"]["upsert_breakdown"] = _upsert_stats["timing"]
    run.unattributable_vins = dict(_upsert_stats.get("unattributable_vins") or {})
    scan_log.log_vehicles(run.dealer_id, name, result.get("provider", run.provider), run.all_vehicles)
    _conflicted = set(_upsert_stats.get("vin_owner_conflict_vins") or ())
    result["vin_owner_conflicts"] = len(_conflicted)
    if result.get("recipe_replay"):
        gap = replay_persist_gap(
            replay_kept,
            count,
            {
                "vin_owner_guard": len(_conflicted),
                "unattributable": len(run.unattributable_vins),
                "identifier_gate": sum((_upsert_stats.get("identifier_gate_refused") or {}).values()),
            },
        )
        if gap:
            result["replay_persist_gap"] = gap
            logger.warning(
                "Replay->persist gap [%s]: the replay kept %d VIN(s), %d were written (dropped by %s)",
                name, gap["kept"], gap["persisted"], gap["dropped_by"] or "an unrecorded step",
            )
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
