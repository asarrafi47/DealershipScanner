"""Feed sufficiency and the inventory recovery chain."""
from __future__ import annotations

import logging
from typing import Any, Callable

from backend.scanner.phases.dealer_run_steps.state import DealerRun
from backend.scanner.scan_efficiency import intercept_feed_is_sufficient

logger = logging.getLogger("scanner")


def feed_is_sufficient(run: DealerRun, feed_rows: list[dict[str, Any]]) -> bool:
    """The row-count heuristics — feed sufficiency and, through it, the
    recovery trigger — must be told what the FEED answered with (kept +
    refused), not what the rooftop gate kept for this store. A group feed that
    returns 995 rows of which 135 are ours is a COMPLETE capture; report 135 and
    the recovery chain concludes the capture failed and re-fetches the lot
    through scraper strategies whose rows carry no rooftop evidence at all, so
    every sibling car it finds would be stored. Measured on
    audifletcherjones-com: 135 of 1,005 rows are this store's."""
    from backend.scanner.inventory_recovery import unique_vin_count as _unique_vin_count

    merged_unique = _unique_vin_count(feed_rows)
    return (
        bool(feed_rows)
        and bool(run.intercept_records)
        and intercept_feed_is_sufficient(
            run.intercept_records, run.url, len(feed_rows), unique_vin_count=merged_unique
        )
    )


async def recover(
    run: DealerRun,
    feed_rows: list[dict[str, Any]],
    parse_fn: Callable[[Any], list[dict[str, Any]]],
) -> Any:
    """Run the recovery chain (it decides itself whether the feed needs it)."""
    from backend.scanner.inventory_recovery import RecoveryContext, recover_inventory

    if not feed_rows:
        logger.info(
            "Extraction backup: %s — no vehicles from %d JSON intercept(s); running recovery chain",
            run.name,
            len(run.intercept_records),
        )

    return await recover_inventory(
        RecoveryContext(
            page=run.page,
            base_url=run.url,
            dealer_id=run.dealer_id,
            dealer_name=run.name,
            dealer_url=run.url,
            provider=run.provider,
            intercept_records=run.intercept_records,
            # No HTML pages are captured on any scan path (the SRP scrape is gone).
            path_htmls=[],
            vehicles=feed_rows,
            parse_fn=parse_fn,
            dealer=run.dealer,
        )
    )


def note_recovery(run: DealerRun, recovery: Any, feed_sufficient: bool) -> None:
    """Record what recovery did; runs after the all-rows attribution pass."""
    result, all_vehicles = run.result, run.all_vehicles
    if recovery.strategies_tried:
        result["inventory_recovery"] = {
            "winning_strategy": recovery.winning_strategy,
            "strategies_tried": recovery.strategies_tried,
            "replaced": recovery.replaced,
        }
    if (
        all_vehicles
        and feed_sufficient
        and not recovery.replaced
        and not recovery.strategies_tried
    ):
        logger.info(
            "Inventory feed sufficient for %s (%d rows, %d intercepts) — skipping platform augmentations",
            run.name,
            len(all_vehicles),
            len(run.intercept_records),
        )
    elif not all_vehicles and not recovery.strategies_tried:
        logger.info(
            "Inventory recovery: %s — 0 vehicles after intercept and recovery (SPA shell or unsupported)",
            run.name,
        )
