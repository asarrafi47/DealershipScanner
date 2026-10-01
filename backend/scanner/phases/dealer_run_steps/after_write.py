"""After the write: coverage + auto-heal, registry link, rooftop disown, reconcile."""
from __future__ import annotations

import asyncio
import logging

# One shared rooftop reconcile policy for the full scan and the delta path.
from backend.scanner.rooftop_disown import (
    disown_foreign_rooftop_vins,
    split_refusals,
)
from backend.scanner.phases.dealer_run_steps.state import DealerRun
from backend.scanner.post_scan.coverage_report import compute_dealer_coverage, format_coverage_log

logger = logging.getLogger("scanner")


async def coverage_and_auto_heal(run: DealerRun) -> None:
    if not run.all_vehicles:
        return
    _cov = compute_dealer_coverage(list(run.all_vehicles), dealer_id=run.dealer_id)
    run.result["coverage"] = _cov
    logger.info("%s", format_coverage_log(_cov))
    # Quality loop: when key fields are still thin after every
    # scan-time layer, go back per-car via the gap-fill methods.
    try:
        from backend.scanner.post_scan.auto_heal import run_auto_heal_for_dealer

        _heal = await asyncio.to_thread(
            run_auto_heal_for_dealer, run.dealer_id, run.name, _cov, list(run.all_vehicles)
        )
        if _heal:
            run.result["auto_heal"] = _heal
    except Exception as _heal_e:
        logger.warning("Auto-heal failed [%s]: %s", run.name, _heal_e)


async def link_registry(run: DealerRun) -> None:
    if not (run.reg_id and run.url):
        return
    try:
        from backend.db.inventory_db import link_cars_to_dealership_registry

        linked = await asyncio.to_thread(
            link_cars_to_dealership_registry,
            int(run.reg_id),
            run.url,
            dealer_id_slug=run.dealer_id,
        )
        if linked:
            run.result["registry_linked"] = linked
    except Exception as e:
        logger.debug(
            "link_cars_to_dealership_registry failed for %s: %s",
            run.name,
            e,
        )


async def disown_sibling_rooftop_cars(run: DealerRun, scraped_norm: set[str]) -> None:
    """Cars earlier scans stamped onto this store that THIS run's feed
    hands to a named sibling rooftop. They are gone from
    ``scraped_norm`` now, but reconcile alone cannot retire them: once
    the siblings' rows are (correctly) refused this dealer can no
    longer reach reconcile's VIN-coverage bar. Only refusals that are
    evidence about a CAR are acted on — see
    rooftop_disown.EVIDENCE_BACKED_REJECTS; "we could not tell which
    rooftop is this store" is a statement about our roster and must
    never un-list anything."""
    from backend.scanner.inventory_reconcile import normalized_vin_set_from_vehicles

    result, name = run.result, run.name
    try:
        evidenced, unidentified = split_refusals(run.rooftop_refused)
        if unidentified:
            result["rooftop_unidentified_rows"] = unidentified
            logger.warning(
                "Rooftop [%s]: could not identify this store among the rooftops its feed "
                "names — %d row(s) refused for storage, NOTHING un-listed. Fix by giving "
                "this dealer a street_address in the `dealerships` roster.",
                name, unidentified,
            )
        foreign_vins = normalized_vin_set_from_vehicles(evidenced) - scraped_norm
        if foreign_vins:
            disowned = await asyncio.to_thread(
                disown_foreign_rooftop_vins, run.dealer_id, foreign_vins
            )
            result["rooftop_disowned"] = disowned
            if disowned:
                logger.info(
                    "Rooftop [%s]: %d car(s) belonged to sibling rooftops in this group "
                    "feed — unlisted from this store",
                    name, disowned,
                )
    except Exception as e:
        logger.warning("Rooftop disown failed for %s (continuing): %s", name, e)


async def reconcile(run: DealerRun, scraped_norm: set[str], scraped_conditions, reconcile_fn) -> None:
    try:
        run.result["reconcile"] = await asyncio.to_thread(
            reconcile_fn,
            run.dealer_id,
            run.url,
            scraped_norm,
            run.result,
            scraped_conditions=scraped_conditions,
        )
    except Exception as e:
        logger.warning(
            "Inventory reconcile failed for %s (inventory already saved): %s",
            run.name,
            e,
        )
        run.result["reconcile"] = {
            "ran": False,
            "scraped_candidates": 0,
            "marked_inactive": 0,
            "skipped_reason": "exception",
            "error": str(e)[:200],
        }


async def after_write(run: DealerRun) -> None:
    await coverage_and_auto_heal(run)
    await link_registry(run)
    from backend.scanner.inventory_reconcile import (
        condition_buckets_from_vehicles,
        normalized_vin_set_from_vehicles,
        reconcile_dealer_inventory_after_scan,
    )

    scraped_norm = normalized_vin_set_from_vehicles(run.all_vehicles)
    scraped_conditions = condition_buckets_from_vehicles(run.all_vehicles)
    await disown_sibling_rooftop_cars(run, scraped_norm)
    await reconcile(run, scraped_norm, scraped_conditions, reconcile_dealer_inventory_after_scan)
