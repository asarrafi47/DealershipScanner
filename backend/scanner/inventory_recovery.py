"""
Multi-strategy inventory recovery when Playwright intercepts are empty or incomplete.

Order follows what worked on OC/Tustin CDJR runs (Algolia/DealerInspire first, then Venom,
DealerOn, EProcess, then HTML/__NEXT_DATA__). Each strategy is tried until the lot is full
enough or the chain is exhausted.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from backend.scanner.scan_efficiency import intercept_feed_is_sufficient
from backend.scanner.scrapers.scanner_intercept_filter import (
    effective_lot_total_from_intercepts,
    max_algolia_nb_hits_from_intercepts,
    max_vehicle_list_len_from_intercepts,
)
from backend.scanner.scrapers.next_data_inventory import (
    fetch_next_data_json_from_page,
    parse_next_data_json_from_html,
)

logger = logging.getLogger(__name__)

StrategyFn = Callable[[], Awaitable[list[dict[str, Any]]]]


def _recovery_enabled() -> bool:
    raw = (os.environ.get("SCANNER_INVENTORY_RECOVERY") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _min_rows_for_recovery() -> int:
    raw = (os.environ.get("SCANNER_RECOVERY_MIN_ROWS") or "8").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 8


def _recovery_strategy_timeout_sec() -> float:
    raw = (os.environ.get("SCANNER_RECOVERY_STRATEGY_TIMEOUT_SEC") or "90").strip()
    try:
        return max(5.0, float(raw))
    except ValueError:
        return 90.0


def _algolia_recovery_floor() -> int:
    """When Algolia is present but unique VINs stay below this, force API recovery (Tustin-style)."""
    raw = (os.environ.get("SCANNER_ALGOLIA_RECOVERY_FLOOR") or "80").strip()
    try:
        return max(_min_rows_for_recovery(), int(raw))
    except ValueError:
        return 80


def _intercept_urls_hint_algolia(intercept_records: list[tuple[str, Any]]) -> bool:
    for resp_url, _body in intercept_records:
        low = str(resp_url or "").lower()
        if "algolia" in low or "algolianet" in low:
            return True
    return False


def unique_vin_count(vehicles: list[dict[str, Any]]) -> int:
    seen: set[str] = set()
    for v in vehicles:
        vin = (v.get("vin") or "").strip().upper()
        if len(vin) == 17:
            seen.add(vin)
    return len(seen)


def _dedupe_vin_list(vehicles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from backend.scanner.scrapers.inventory_vin_merge import merge_inventory_rows_same_vin

    by_vin: dict[str, dict[str, Any]] = {}
    for v in vehicles:
        vin = (v.get("vin") or "").strip()
        if not vin:
            continue
        if vin in by_vin:
            merge_inventory_rows_same_vin(by_vin[vin], v)
        else:
            by_vin[vin] = v
    return list(by_vin.values())


@dataclass
class RecoveryContext:
    page: Any
    base_url: str
    dealer_id: str
    dealer_name: str
    dealer_url: str
    provider: str
    intercept_records: list[tuple[str, Any]]
    path_htmls: list[str | None]
    vehicles: list[dict[str, Any]]
    parse_fn: Callable[[Any], list[dict[str, Any]]]


@dataclass
class RecoveryResult:
    vehicles: list[dict[str, Any]]
    strategies_tried: list[str] = field(default_factory=list)
    winning_strategy: str | None = None
    replaced: bool = False


def should_run_platform_recovery(ctx: RecoveryContext) -> bool:
    """True when intercept-only data is missing or likely incomplete."""
    if not _recovery_enabled():
        return False
    n = unique_vin_count(ctx.vehicles)
    if n == 0:
        return True
    if not ctx.intercept_records:
        return True
    if not intercept_feed_is_sufficient(
        ctx.intercept_records, ctx.base_url, len(ctx.vehicles), unique_vin_count=n
    ):
        return True
    if _intercept_urls_hint_algolia(ctx.intercept_records):
        algolia_max = max_algolia_nb_hits_from_intercepts(ctx.intercept_records, ctx.base_url)
        lot_total = effective_lot_total_from_intercepts(ctx.intercept_records, ctx.base_url)
        if algolia_max is not None and n < int(algolia_max * 0.92):
            return True
        if lot_total is not None and n < int(lot_total * 0.92):
            return True
        if n < _algolia_recovery_floor():
            return True
    if n < _min_rows_for_recovery():
        return True
    best_batch = max_vehicle_list_len_from_intercepts(ctx.intercept_records, ctx.base_url)
    if best_batch >= _min_rows_for_recovery() and n < best_batch:
        return True
    return False


def _tag_vehicles(vehicles: list[dict[str, Any]], ctx: RecoveryContext) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for v in vehicles:
        row = dict(v)
        row.setdefault("dealer_name", ctx.dealer_name)
        row.setdefault("dealer_url", ctx.dealer_url)
        out.append(row)
    return out


def _prefer_new_vehicles(
    current: list[dict[str, Any]],
    candidate: list[dict[str, Any]],
    strategy: str,
) -> tuple[list[dict[str, Any]], bool]:
    """Keep whichever set has more unique VINs (replace only when strictly better)."""
    cur_n = unique_vin_count(current)
    new_n = unique_vin_count(candidate)
    if new_n > cur_n:
        logger.info(
            "Inventory recovery: %s — %d unique VIN(s) (was %d from prior step)",
            strategy,
            new_n,
            cur_n,
        )
        return candidate, True
    if new_n > 0 and cur_n == 0:
        return candidate, True
    return current, False


async def _run_strategy(
    name: str,
    fn: StrategyFn,
    *,
    ctx: RecoveryContext,
    vehicles: list[dict[str, Any]],
    tried: list[str],
) -> tuple[list[dict[str, Any]], bool, str | None]:
    tried.append(name)
    timeout_s = _recovery_strategy_timeout_sec()
    try:
        found = await asyncio.wait_for(fn(), timeout=timeout_s)
    except asyncio.TimeoutError:
        logger.warning(
            "Inventory recovery [%s] %s timed out after %.0fs",
            ctx.dealer_name,
            name,
            timeout_s,
        )
        return vehicles, False, None
    except Exception as e:
        logger.debug("Inventory recovery [%s] %s failed: %s", ctx.dealer_name, name, e)
        return vehicles, False, None
    if not found:
        logger.debug("Inventory recovery [%s] %s returned 0 rows", ctx.dealer_name, name)
        return vehicles, False, None
    tagged = _tag_vehicles(found, ctx)
    updated, replaced = _prefer_new_vehicles(vehicles, tagged, name)
    winner = name if replaced else None
    return updated, replaced, winner


async def _html_and_next_data(ctx: RecoveryContext) -> list[dict[str, Any]]:
    from backend.scanner.scrapers.pixel_motion import (
        parse_pixel_motion_inventory_html,
    )

    vehicles: list[dict[str, Any]] = []
    for path_html in ctx.path_htmls:
        if not path_html:
            continue
        batch = parse_pixel_motion_inventory_html(
            path_html, ctx.base_url, ctx.dealer_id, ctx.dealer_name, ctx.dealer_url
        )
        if batch:
            return batch
        batch = list(ctx.parse_fn(path_html))
        if batch:
            return batch
        nd = parse_next_data_json_from_html(path_html)
        if nd is not None:
            batch = list(ctx.parse_fn(nd))
            if batch:
                return batch
    nd = await fetch_next_data_json_from_page(ctx.page)
    if nd is not None:
        return list(ctx.parse_fn(nd))
    return []


async def recover_inventory(ctx: RecoveryContext) -> RecoveryResult:
    """
    Run the recovery chain when needed. Returns updated vehicles and which strategy won.
    """
    vehicles = list(ctx.vehicles)
    tried: list[str] = []
    winner: str | None = None
    any_replaced = False

    if not should_run_platform_recovery(ctx):
        return RecoveryResult(vehicles=vehicles, strategies_tried=tried, winning_strategy=None)

    logger.info(
        "Inventory recovery: %s — starting (%d row(s), %d intercept(s), algolia_hint=%s)",
        ctx.dealer_name,
        len(vehicles),
        len(ctx.intercept_records),
        _intercept_urls_hint_algolia(ctx.intercept_records),
    )

    from backend.scanner.scrapers.dealer_inspire import scrape_dealer_inspire_from_page
    from backend.scanner.scrapers.dealer_on import scrape_dealer_on_from_page
    from backend.scanner.scrapers.dealer_venom import scrape_dealer_venom_from_page

    async def _inspire() -> list[dict[str, Any]]:
        return await scrape_dealer_inspire_from_page(
            ctx.page, ctx.base_url, ctx.dealer_id, ctx.dealer_name, ctx.dealer_url
        )

    async def _venom() -> list[dict[str, Any]]:
        return await scrape_dealer_venom_from_page(
            ctx.page, ctx.base_url, ctx.dealer_id, ctx.dealer_name, ctx.dealer_url
        )

    async def _dealer_on() -> list[dict[str, Any]]:
        return await scrape_dealer_on_from_page(
            ctx.page, ctx.base_url, ctx.dealer_id, ctx.dealer_name, ctx.dealer_url
        )

    async def _eprocess() -> list[dict[str, Any]]:
        from backend.scanner.scrapers.dealer_eprocess import scrape_dealer_eprocess_from_page

        return await scrape_dealer_eprocess_from_page(
            ctx.page, ctx.base_url, ctx.dealer_id, ctx.dealer_name, ctx.dealer_url
        )

    async def _pixel_motion() -> list[dict[str, Any]]:
        from backend.scanner.scrapers.pixel_motion import scrape_pixel_motion_from_page

        return await scrape_pixel_motion_from_page(
            ctx.page, ctx.base_url, ctx.dealer_id, ctx.dealer_name, ctx.dealer_url
        )

    async def _html() -> list[dict[str, Any]]:
        return await _html_and_next_data(ctx)

    # Algolia direct API first (Tustin, Tuttle-Click, Orange Coast); PixelMotion SSR HTML for CDJR WP sites.
    for name, fn in (
        ("dealer_inspire_algolia", _inspire),
        ("dealer_venom_typesense", _venom),
        ("pixel_motion_html", _pixel_motion),
        ("dealer_on_cosmos", _dealer_on),
        ("dealer_eprocess_json", _eprocess),
        ("html_next_data", _html),
    ):
        vehicles, replaced, step_winner = await _run_strategy(
            name, fn, ctx=ctx, vehicles=vehicles, tried=tried
        )
        if replaced:
            any_replaced = True
            winner = step_winner
        # Stop early when feed looks complete after a strong win.
        uv = unique_vin_count(vehicles)
        if (
            vehicles
            and intercept_feed_is_sufficient(
                ctx.intercept_records, ctx.base_url, len(vehicles), unique_vin_count=uv
            )
            and uv >= _min_rows_for_recovery()
        ):
            break

    vehicles = _dedupe_vin_list(vehicles)
    if winner:
        logger.info(
            "Inventory recovery: %s — done (%d unique VIN(s), winner=%s, tried=%s)",
            ctx.dealer_name,
            unique_vin_count(vehicles),
            winner,
            ", ".join(tried),
        )
    elif tried:
        logger.info(
            "Inventory recovery: %s — no improvement after %s (%d unique VIN(s))",
            ctx.dealer_name,
            ", ".join(tried),
            unique_vin_count(vehicles),
        )

    return RecoveryResult(
        vehicles=vehicles,
        strategies_tried=tried,
        winning_strategy=winner,
        replaced=any_replaced,
    )
