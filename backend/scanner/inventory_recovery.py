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

from backend.scanner.scan_efficiency import intercept_feed_is_sufficient, _intercept_coverage_ratio
from backend.scanner.scrapers.scanner_intercept_filter import (
    effective_lot_total_from_intercepts,
    max_algolia_nb_hits_from_intercepts,
    max_vehicle_list_len_from_intercepts,
)
from backend.scanner.dealer_profile import (
    get_cached_winning_strategy,
    manifest_recovery_strategies,
    manifest_skip_recovery,
    prioritize_recovery_chain,
    record_winning_strategy,
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


def _recovery_hint_filter_enabled() -> bool:
    raw = (os.environ.get("SCANNER_RECOVERY_HINT_FILTER") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


# Full recovery chain order (platform-specific strategies before HTML fallback).
RECOVERY_STRATEGY_ORDER: tuple[str, ...] = (
    "shopperexpress_api",
    "dealer_inspire_algolia",
    "dealer_venom_typesense",
    "pixel_motion_html",
    "dealer_on_cosmos",
    "dealer_eprocess_json",
    "html_next_data",
    "jsonld_listing_html",
)

_STRATEGY_PLATFORM_HINTS: dict[str, frozenset[str]] = {
    "shopperexpress_api": frozenset({"shopperexpress"}),
    "dealer_inspire_algolia": frozenset({"algolia", "dealer_inspire"}),
    "dealer_venom_typesense": frozenset({"typesense", "dealer_venom"}),
    "pixel_motion_html": frozenset({"pixel_motion"}),
    "dealer_on_cosmos": frozenset({"dealer_on"}),
    "dealer_eprocess_json": frozenset({"dealer_eprocess"}),
    "html_next_data": frozenset({"html_fallback"}),
}


def detect_platform_hints(ctx: RecoveryContext) -> set[str]:
    """
    Infer DMS/platform from intercept URLs and saved path HTML.
    Used to skip irrelevant recovery strategies (each can cost up to 90s).
    """
    hints: set[str] = set()
    for resp_url, _body in ctx.intercept_records:
        low = str(resp_url or "").lower()
        if "algolia" in low or "algolianet" in low:
            hints.add("algolia")
        if "typesense" in low:
            hints.add("typesense")
        if "cosmos/srp/vehicles" in low or "vhcliaa" in low:
            hints.add("dealer_on")
        if "dealereprocess" in low or ("/assets/" in low and "vehicle-facts" in low):
            hints.add("dealer_eprocess")

    # Check manifest provider for platforms that have dedicated API scrapers.
    manifest_provider = (ctx.dealer or {}).get("provider") or ""
    if manifest_provider == "shopperexpress":
        hints.add("shopperexpress")
    if manifest_provider == "dealer_inspire":
        hints.add("dealer_inspire")
        hints.add("algolia")

    combined = "\n".join(h for h in ctx.path_htmls if h)
    if not combined:
        return hints

    low = combined.lower()
    if "dealerinspire" in low or "mvnalgoliaconfig" in low or "maven-algolia" in low:
        hints.add("dealer_inspire")
        hints.add("algolia")
    if "dealervenom" in low or "dv-framework" in low or "typesenseinstantsearchadapter" in low:
        hints.add("dealer_venom")
        hints.add("typesense")
    if "vlpm3vehicle" in low or "pixelmotion" in low:
        hints.add("pixel_motion")
    if "dealeron" in low or "vhcliaa" in low or "prsnbaa.dealeron" in low:
        hints.add("dealer_on")
    if "dealereprocess" in low:
        hints.add("dealer_eprocess")
    if "shopperexpress" in low or ("serti" in low and "btn-next" in low):
        hints.add("shopperexpress")
    return hints


# Maps manifest provider → the recovery strategy that must run first for that platform.
# Used to pin the canonical strategy to the front of the chain when false hints (e.g.
# algolia intercepts from analytics) would otherwise promote an incompatible strategy.
_PROVIDER_FIRST_STRATEGY: dict[str, str] = {
    "dealer_on": "dealer_on_cosmos",
    "shopperexpress": "shopperexpress_api",
    "dealer_inspire": "dealer_inspire_algolia",
    "dealer_eprocess": "dealer_eprocess_json",
    "dealer_venom": "dealer_venom_typesense",
    "pixel_motion": "pixel_motion_html",
}


def recovery_strategy_names(
    hints: set[str],
    *,
    manifest_strategies: list[str] | None = None,
    cached_strategy: str | None = None,
    provider: str = "",
) -> list[str]:
    """
    Return ordered recovery strategy names. When platform hints are known, run matching
    strategies only plus ``html_next_data`` fallback. Unknown platform → full chain.

    Manifest ``recovery_strategies`` overrides hint-based selection when provided.
    ``cached_strategy`` (from prior successful runs) is moved to the front of the chain.
    ``provider`` (manifest provider field) pins the canonical strategy to the front when
    set, overriding false hints from unrelated intercepts (e.g. analytics Algolia calls
    on a DealerOn site).
    """
    # Provider pin: treat the canonical strategy as the cached winner so it moves first.
    if not cached_strategy and provider:
        cached_strategy = _PROVIDER_FIRST_STRATEGY.get(provider)
    if manifest_strategies:
        chain = list(manifest_strategies)
        if "html_next_data" not in chain:
            chain.append("html_next_data")
        return prioritize_recovery_chain(chain, cached_strategy=cached_strategy)

    if not _recovery_hint_filter_enabled() or not hints:
        chain = list(RECOVERY_STRATEGY_ORDER)
        return prioritize_recovery_chain(chain, cached_strategy=cached_strategy)

    platform_hints = {h for h in hints if h != "html_fallback"}
    if not platform_hints:
        chain = list(RECOVERY_STRATEGY_ORDER)
        return prioritize_recovery_chain(chain, cached_strategy=cached_strategy)

    selected: list[str] = []
    for name in RECOVERY_STRATEGY_ORDER:
        if name == "html_next_data":
            selected.append(name)
            continue
        need = _STRATEGY_PLATFORM_HINTS.get(name, frozenset())
        if need & platform_hints:
            selected.append(name)
    if len(selected) <= 1:
        chain = list(RECOVERY_STRATEGY_ORDER)
    else:
        chain = selected
    return prioritize_recovery_chain(chain, cached_strategy=cached_strategy)


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
    dealer: dict[str, Any] | None = None


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

    # DealerOn cosmos API is never captured by the standard JSON intercept path — the
    # initial scan intercepts analytics/GTM events that look like inventory but aren't.
    # Always run DealerOn recovery so _scrape_srp_all_pages can navigate and intercept
    # the actual cosmos/srp/vehicles response.
    if ctx.provider == "dealer_on":
        return True

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
            ratio = _intercept_coverage_ratio()
            if algolia_max is not None and n >= int(algolia_max * ratio):
                pass
            elif lot_total is not None and n >= int(lot_total * ratio):
                pass
            else:
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


def _parse_jsonld_listing_html(ctx: RecoveryContext) -> list[dict[str, Any]]:
    """Parse Schema.org JSON-LD Car/Vehicle objects from captured listing page HTML.
    Works for sites like PCNA (Porsche) that embed full vehicle JSON-LD per card."""
    import re as _re
    import json as _json

    VIN_RE = _re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", _re.I)
    SCHEMA_RE = _re.compile(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        _re.S | _re.I,
    )

    vehicles: list[dict[str, Any]] = []
    seen_vins: set[str] = set()

    for path_html in ctx.path_htmls:
        if not path_html:
            continue
        for m in SCHEMA_RE.finditer(path_html):
            try:
                data = _json.loads(m.group(1))
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            types = data.get("@type", "")
            types_set = {t.lower() for t in types} if isinstance(types, list) else {str(types).lower()}
            if not (types_set & {"car", "vehicle"}):
                continue
            vin = (data.get("vehicleIdentificationNumber") or "").strip().upper()
            if not VIN_RE.match(vin) or vin in seen_vins:
                continue
            seen_vins.add(vin)

            name = data.get("name", "")
            year, make, model, trim = None, None, None, None
            nm = _re.match(r"(\d{4})\s+(\S+)\s+([\w\s]+?)(?:\s*\((.+)\))?\s*$", name.strip())
            if nm:
                try:
                    year = int(nm.group(1))
                except ValueError:
                    pass
                make = nm.group(2) or None
                model = (nm.group(3) or "").strip() or None
                trim = nm.group(4) or None

            offers = data.get("offers") or {}
            price = offers.get("price")
            try:
                price = int(price) if price is not None else None
            except (ValueError, TypeError):
                price = None
            source_url = offers.get("url") or None
            cond_raw = (offers.get("itemCondition") or "").lower()
            if "used" in cond_raw or "preowned" in cond_raw:
                condition = "Used"
            elif "new" in cond_raw:
                condition = "New"
            elif "refurbished" in cond_raw or "certified" in cond_raw:
                condition = "Certified Pre-Owned"
            else:
                condition = None

            mileage_obj = data.get("mileageFromOdometer") or {}
            try:
                mileage = int(mileage_obj.get("value")) if mileage_obj.get("value") is not None else None
            except (ValueError, TypeError):
                mileage = None

            image_url = data.get("image") or ""
            if isinstance(image_url, list):
                image_url = image_url[0] if image_url else ""
            image_url = str(image_url).strip() or None

            vehicles.append({
                "vin": vin,
                "year": year,
                "make": make,
                "model": model,
                "trim": trim,
                "price": price,
                "mileage": mileage,
                "condition": condition,
                "exterior_color": data.get("color") or None,
                "interior_color": data.get("vehicleInteriorColor") or None,
                "image_url": image_url,
                "gallery": [image_url] if image_url else [],
                "source_url": source_url,
                "_detail_url": source_url,
                "dealer_name": ctx.dealer_name,
                "dealer_url": ctx.dealer_url,
                "dealer_id": ctx.dealer_id,
            })

    if not vehicles:
        return vehicles

    # If one brand dominates (≥70% of results), drop other-brand rows that were
    # likely sourced from a "used / other-brands" path on a mono-brand dealer site.
    from collections import Counter
    makes = [v.get("make") or "" for v in vehicles]
    counts = Counter(makes)
    total = len(vehicles)
    top_make, top_n = counts.most_common(1)[0]
    if top_make and top_n / total >= 0.70 and top_n < total:
        vehicles = [v for v in vehicles if (v.get("make") or "") == top_make]
        logger.debug(
            "jsonld_listing_html: %s — filtered to dominant make '%s' (%d→%d vehicles)",
            ctx.dealer_name, top_make, total, len(vehicles),
        )

    logger.info("jsonld_listing_html: %s — %d vehicle(s) from JSON-LD", ctx.dealer_name, len(vehicles))
    return vehicles


async def recover_inventory(ctx: RecoveryContext) -> RecoveryResult:
    """
    Run the recovery chain when needed. Returns updated vehicles and which strategy won.
    """
    vehicles = list(ctx.vehicles)
    tried: list[str] = []
    winner: str | None = None
    any_replaced = False

    if manifest_skip_recovery(ctx.dealer):
        logger.info(
            "Inventory recovery: %s — skipped (manifest skip_recovery=true)",
            ctx.dealer_name,
        )
        return RecoveryResult(vehicles=vehicles, strategies_tried=tried, winning_strategy=None)

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
            ctx.page,
            ctx.base_url,
            ctx.dealer_id,
            ctx.dealer_name,
            ctx.dealer_url,
            dealer=ctx.dealer,
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

    async def _shopperexpress() -> list[dict[str, Any]]:
        from backend.scanner.scrapers.shopperexpress import fetch_shopperexpress_inventory

        return await fetch_shopperexpress_inventory(
            ctx.base_url, ctx.dealer_id, ctx.dealer_name
        )

    async def _html() -> list[dict[str, Any]]:
        return await _html_and_next_data(ctx)

    async def _jsonld_listing() -> list[dict[str, Any]]:
        return _parse_jsonld_listing_html(ctx)

    strategy_fns: dict[str, StrategyFn] = {
        "shopperexpress_api": _shopperexpress,
        "dealer_inspire_algolia": _inspire,
        "dealer_venom_typesense": _venom,
        "pixel_motion_html": _pixel_motion,
        "dealer_on_cosmos": _dealer_on,
        "dealer_eprocess_json": _eprocess,
        "html_next_data": _html,
        "jsonld_listing_html": _jsonld_listing,
    }
    hints = detect_platform_hints(ctx)
    manifest_chain = manifest_recovery_strategies(ctx.dealer)
    cached = get_cached_winning_strategy(ctx.dealer_id)
    chain = recovery_strategy_names(
        hints,
        manifest_strategies=manifest_chain,
        cached_strategy=cached,
        provider=ctx.provider,
    )
    if manifest_chain:
        logger.info(
            "Inventory recovery: %s — manifest strategies %s",
            ctx.dealer_name,
            chain,
        )
    elif cached:
        logger.info(
            "Inventory recovery: %s — cached winner %s first in chain %s",
            ctx.dealer_name,
            cached,
            chain,
        )
    elif hints and chain != list(RECOVERY_STRATEGY_ORDER):
        logger.info(
            "Inventory recovery: %s — platform hints %s → strategies %s",
            ctx.dealer_name,
            sorted(hints),
            chain,
        )

    for name in chain:
        fn = strategy_fns[name]
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

    if winner:
        record_winning_strategy(ctx.dealer_id, winner, platform_hints=hints)

    return RecoveryResult(
        vehicles=vehicles,
        strategies_tried=tried,
        winning_strategy=winner,
        replaced=any_replaced,
    )
