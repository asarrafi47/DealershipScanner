"""Single inventory URL path scrape (JSON intercept + pagination)."""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from backend.scanner.scrapers.dealer_com_bulk_fetch import (
    dealer_com_bulk_fetch_enabled,
    default_dealer_com_inventory_api_url,
    fetch_dealer_com_inventory_bulk,
    nudge_dealer_com_inventory_api,
    is_dealer_com_inventory_post_url,
    merge_post_template,
    parse_post_template,
)

from backend.parsers import parse
from backend.scanner.constants import MAX_PAGINATION_CLICKS, NEXT_SELECTORS
from backend.scanner.phases.nav import (
    DealerResponseErrorBudget,
    any_next_control_visible,
    await_inventory_hydration,
    goto_with_retries,
    hydration_timeout_ms,
    infinite_scroll_lazy_batches,
    inventory_wait_ms,
    pagination_debug_enabled,
    pagination_response_wait_ms,
    playwright_inventory_json_predicate,
    try_apply_location_filter,
)
from backend.scanner.network_observer import NetworkObserver
from backend.scanner.scrapers.scanner_intercept_filter import (
    effective_lot_total_from_intercepts,
)
from backend.scanner.scan_efficiency import (
    inventory_idle_loop_sec,
    inventory_json_wait_ms,
)

if TYPE_CHECKING:
    from backend.scanner.phases.site_profile import SiteProfile

logger = logging.getLogger("scanner")


async def scrape_inventory_path(
    context: Any,
    path: str,
    base_url: str,
    provider: str,
    dealer_id: str,
    dealer_name: str,
    inv_wait_ms: int,
    pag_wait_ms: int,
    *,
    dealer_city: str = "",
    dealer_state: str = "",
    site_profile: "SiteProfile | None" = None,
) -> tuple[list[tuple[str, Any]], str | None, int, dict[str, str], list[Any]]:
    """
    Scrape one inventory path on a dedicated page within ``context``.
    Returns ``(intercept_records, page_html, url_denied_count, card_locations,
    captured_endpoints)`` — the last is the observer's endpoint records for
    replay-recipe promotion.
    Opens and closes its own page; does not touch the warmup/VDP page.
    Structured JSON interception is primary: qualifying payloads skip saving HTML for this path.
    When no Next/Load-more control is visible, a scroll-to-bottom loop pulls lazy-loaded batches.

    ``site_profile`` (optional) is used to short-circuit provider-specific branches and
    skip unnecessary wait loops when the profiler already identified the site structure.
    """
    # ── Profile-derived hints (all fall back to existing behaviour when None) ──
    _profile_provider = (site_profile.detected_provider if site_profile else "unknown") or "unknown"
    _profile_pagination = (site_profile.pagination_type if site_profile else "unknown") or "unknown"

    # Skip infinite-scroll loops entirely when we know the site uses API pagination.
    _skip_lazy_scroll = _profile_pagination == "api"

    # PixelMotion from profile: jump straight to SSR branch, skip JSON-wait loops.
    _is_pixel_motion_early = _profile_provider == "pixel_motion"

    # autoWALL / ShopperExpress: provider-driven early dispatch before any JSON intercept.
    _is_autowall_early = provider == "autowall" or _profile_provider == "autowall"
    _is_shopperexpress_early = provider == "shopperexpress" or _profile_provider == "shopperexpress"

    if site_profile is not None:
        logger.debug(
            "Profile hints [%s]%s: provider=%s pagination=%s "
            "skip_lazy_scroll=%s pixel_motion_early=%s",
            dealer_name, path,
            _profile_provider, _profile_pagination,
            _skip_lazy_scroll, _is_pixel_motion_early,
        )
    local_records: list[tuple[str, Any]] = []
    found_data = {"value": False}
    card_locations: dict[str, str] = {}
    resp_err = DealerResponseErrorBudget()

    async def _capture_card_locations() -> None:
        from backend.scanner.inventory_card_location import scrape_inventory_card_locations

        try:
            card_locations.update(await scrape_inventory_card_locations(page))
        except Exception:
            pass

    page = await context.new_page()
    post_template: dict[str, Any] | None = None
    api_post_url: str | None = None

    async def handle_request(request: Any) -> None:
        nonlocal post_template, api_post_url
        try:
            if (request.method or "").upper() != "POST":
                return
            rurl = str(getattr(request, "url", "") or "")
            if "getinventory" not in rurl.lower():
                return
            parsed = parse_post_template(getattr(request, "post_data", None))
            if not parsed:
                return
            post_template = merge_post_template(post_template, parsed)
            if is_dealer_com_inventory_post_url(rurl):
                api_post_url = rurl.split("?", 1)[0]
            elif api_post_url is None and "getinventoryandfacets" in rurl.lower():
                api_post_url = rurl.split("?", 1)[0].replace(
                    "getInventoryAndFacets", "getInventory"
                )
        except Exception:
            pass

    page.on("request", handle_request)

    observer = NetworkObserver(
        dealer_base_url=base_url,
        dealer_id=dealer_id,
        dealer_name=dealer_name,
        path=path,
        records=local_records,
        found_data=found_data,
        resp_err=resp_err,
    )
    page.on("response", observer.on_response)
    html: str | None = None
    full_url = base_url + path
    try:
        logger.info("Navigating: %s — %s", dealer_name, full_url)
        await goto_with_retries(page, full_url, log_label=f"Nav:{dealer_name}", timeout_ms=20000)
        from backend.scanner.scrapers.pixel_motion import _dismiss_cookie_banner
        await asyncio.sleep(0.5)
        await _dismiss_cookie_banner(page)
        pred = playwright_inventory_json_predicate(base_url)
        await await_inventory_hydration(
            page,
            dealer_name,
            path,
            timeout_ms=hydration_timeout_ms(),
            json_pred=pred,
        )
        json_wait_ms = inventory_json_wait_ms(
            json_already_captured=found_data["value"],
            inv_wait_ms=inv_wait_ms,
            pag_wait_ms=pag_wait_ms,
        )
        try:
            await page.wait_for_event("response", pred, timeout=json_wait_ms)
        except Exception:
            pass

        # Attempt to apply the Location filter on pooled-inventory sites so only this
        # dealer's cars are returned (avoids pulling sister-store inventory in bulk).
        from backend.scanner.dealer_location import sister_store_filter_enabled as _sse
        if _sse():
            loc_applied = await try_apply_location_filter(
                page,
                dealer_name,
                pred,
                pag_wait_ms,
                dealer_city=dealer_city,
                dealer_state=dealer_state,
            )
            if loc_applied:
                # Clear any intercepts captured before the filter applied and wait for fresh data.
                local_records.clear()
                found_data["value"] = False
                observer.capture_event.clear()
                post_template = None
                api_post_url = None
                try:
                    await page.wait_for_event("response", pred, timeout=inv_wait_ms)
                except Exception:
                    pass
                await asyncio.sleep(1.0)

        idle_cap = inventory_idle_loop_sec(found_data["value"])
        if idle_cap > 0 and not found_data["value"]:
            # Event-driven: returns the instant a capture lands instead of polling in 1s steps.
            try:
                await asyncio.wait_for(observer.capture_event.wait(), timeout=idle_cap)
            except asyncio.TimeoutError:
                pass
        await asyncio.sleep(0.5)
        await observer.drain(2.0)
        await _capture_card_locations()

        if dealer_com_bulk_fetch_enabled() and not post_template and not found_data["value"]:
            await nudge_dealer_com_inventory_api(page, dealer_name=dealer_name, path=path)
            for _ in range(24):
                if post_template:
                    break
                await asyncio.sleep(0.25)
            if post_template and not api_post_url:
                api_post_url = default_dealer_com_inventory_api_url(page.url or full_url)

        bulk_complete = False
        if (
            dealer_com_bulk_fetch_enabled()
            and post_template
            and api_post_url
        ):
            try:
                bulk_bodies = await fetch_dealer_com_inventory_bulk(
                    page,
                    api_post_url,
                    post_template,
                    dealer_name=dealer_name,
                    path=path,
                )
                if bulk_bodies:
                    local_records = [(api_post_url, body) for body in bulk_bodies]
                    observer.records = local_records
                    found_data["value"] = True
                    bulk_complete = True
                    logger.info(
                        "Dealer.com bulk fetch complete [%s]%s — %d page(s), %d intercept row(s)",
                        dealer_name,
                        path,
                        len(bulk_bodies),
                        len(local_records),
                    )
            except Exception as e:
                logger.debug(
                    "Dealer.com bulk fetch skipped [%s]%s: %s",
                    dealer_name,
                    path,
                    str(e)[:200],
                )

        # Quick viewport ping when JSON hasn't landed — avoids dead wait when API-backed payloads are slow
        if not found_data["value"]:
            logger.info("Scrolling: %s%s — no JSON yet, initial scroll pulses", dealer_name, path)
            for _ in range(5):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

        pre_pag_scroll_ran = False
        if not _skip_lazy_scroll and not await any_next_control_visible(page):
            pre_pag_scroll_ran = True
            await infinite_scroll_lazy_batches(
                page,
                dealer_name,
                path,
                pred,
                pag_wait_ms=pag_wait_ms,
            )

        from backend.scanner.scrapers.pixel_motion import _is_pixel_motion_html

        peek_html = await page.content()
        if _is_pixel_motion_early or _is_pixel_motion_html(peek_html):
            from backend.scanner.scrapers.pixel_motion import (
                _dismiss_cookie_banner,
                parse_pixel_motion_inventory_html,
            )

            await _dismiss_cookie_banner(page)
            by_vin: dict[str, dict[str, Any]] = {}
            inv_base = base_url
            try:
                from urllib.parse import urlparse

                pu = urlparse(page.url or "")
                if pu.scheme and pu.netloc:
                    inv_base = f"{pu.scheme}://{pu.netloc}"
            except Exception:
                pass
            for _pag in range(20):
                html = await page.content()
                for v in parse_pixel_motion_inventory_html(
                    html, inv_base, dealer_id, dealer_name, base_url
                ):
                    vin = (v.get("vin") or "").strip().upper()
                    if vin:
                        by_vin[vin] = v
                next_btn = page.locator(".vlpm3Pages__next")
                try:
                    if await next_btn.count() == 0:
                        break
                    first = next_btn.first
                    if not await first.is_visible():
                        break
                    await first.click(timeout=5000)
                    await asyncio.sleep(0.8)
                except Exception:
                    break
            if by_vin:
                local_records.append(
                    (
                        f"{inv_base}/pixel_motion_inventory",
                        {"inventory": list(by_vin.values())},
                    )
                )
                found_data["value"] = True
                logger.info(
                    "PixelMotion: %s%s — %d vehicle(s) from SSR pagination",
                    dealer_name,
                    path,
                    len(by_vin),
                )
            html = await page.content()
            return local_records, html, observer.url_denied, card_locations, list(observer.ledger.endpoints.values())

        from backend.scanner.scrapers.autowall import _is_autowall_html

        if _is_autowall_early or _is_autowall_html(peek_html):
            from backend.scanner.scrapers.autowall import fetch_autowall_inventory_http

            # Use the manifest base_url directly. scrape_autowall_via_playwright creates
            # a fresh desktop-UA context and resolves the canonical domain itself (handling
            # www→non-www), so we must NOT override with page.url here — the warmup page
            # may be on a mobile-redirect domain (e.g. chattanoogavolvotn.com) which has
            # no autoWALL inventory.
            inv_base = base_url

            aw_vehicles = await fetch_autowall_inventory_http(inv_base, dealer_id, dealer_name)
            if not aw_vehicles:
                # HTTP session lacks JS-set cookies — fall back to Playwright in-page navigation
                logger.info(
                    "autoWALL HTTP empty for %s — falling back to Playwright navigation",
                    dealer_name,
                )
                from backend.scanner.scrapers.autowall import scrape_autowall_via_playwright
                aw_vehicles = await scrape_autowall_via_playwright(page, inv_base, dealer_id, dealer_name)
            if aw_vehicles:
                local_records.append(
                    (f"{inv_base}/autowall_inventory", {"inventory": aw_vehicles})
                )
                found_data["value"] = True
                logger.info("autoWALL: %s — %d vehicle(s)", dealer_name, len(aw_vehicles))
            html = await page.content()
            return local_records, html, observer.url_denied, card_locations, list(observer.ledger.endpoints.values())

        from backend.scanner.scrapers.shopperexpress import _is_shopperexpress_html

        if _is_shopperexpress_early or _is_shopperexpress_html(peek_html):
            from backend.scanner.scrapers.shopperexpress import (
                fetch_shopperexpress_inventory,
                scrape_shopperexpress_from_page,
            )

            inv_base = base_url
            try:
                from urllib.parse import urlparse as _urlparse2

                pu2 = _urlparse2(page.url or "")
                if pu2.scheme and pu2.netloc:
                    inv_base = f"{pu2.scheme}://{pu2.netloc}"
            except Exception:
                pass

            # API-first: try /wp-json/v1/vehicles + VDP crawl (no Playwright needed)
            se_vehicles = await fetch_shopperexpress_inventory(inv_base, dealer_id, dealer_name)

            # Playwright fallback if API returned nothing
            if not se_vehicles:
                logger.info("ShopperExpress: %s — API empty, falling back to Playwright", dealer_name)
                se_vehicles = await scrape_shopperexpress_from_page(
                    page, inv_base, dealer_id, dealer_name, base_url
                )

            if se_vehicles:
                local_records.append(
                    (f"{inv_base}/shopperexpress_inventory", {"inventory": se_vehicles})
                )
                found_data["value"] = True
                logger.info("ShopperExpress: %s — %d vehicle(s)", dealer_name, len(se_vehicles))
            html = await page.content()
            return local_records, html, observer.url_denied, card_locations, list(observer.ledger.endpoints.values())

        # Pagination loop — uses only this path's own intercept records
        body_parse_cache: dict[int, list[dict[str, Any]]] = {}

        def _vehicles_for_body(body: Any) -> list[dict[str, Any]]:
            bid = id(body)
            cached = body_parse_cache.get(bid)
            if cached is not None:
                return cached
            vehicles = list(
                parse(provider, body, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=base_url)
            )
            for v in vehicles:
                v.setdefault("dealer_name", dealer_name)
                v.setdefault("dealer_url", base_url)
            body_parse_cache[bid] = vehicles
            return vehicles

        prev_unique_vins = 0
        if bulk_complete:
            pag_iters = 0
        else:
            pag_iters = MAX_PAGINATION_CLICKS
        for pag_iter in range(pag_iters):
            total_count = effective_lot_total_from_intercepts(local_records, base_url)
            by_vin: dict[str, dict[str, Any]] = {}
            for _ru, body in local_records:
                for v in _vehicles_for_body(body):
                    vin = (v.get("vin") or "").strip()
                    if vin:
                        by_vin[vin] = v
            next_visible = await any_next_control_visible(page)
            need_more = total_count is not None and total_count > len(by_vin)
            # Algolia / SPA listings often omit totalCount but expose Next (Tustin Toyota, etc.).
            explore_next = (
                not need_more
                and total_count is None
                and next_visible
                and len(by_vin) > 0
            )
            if pagination_debug_enabled():
                logger.info(
                    "Pagination debug [%s]%s iter=%d intercepts=%d total_count=%s unique_vins=%d "
                    "next_visible=%s need_more=%s explore_next=%s",
                    dealer_name,
                    path,
                    pag_iter,
                    len(local_records),
                    total_count,
                    len(by_vin),
                    next_visible,
                    need_more,
                    explore_next,
                )
            if need_more or explore_next:
                clicked = False
                for sel in NEXT_SELECTORS:
                    try:
                        loc = page.locator(sel)
                        if await loc.count() > 0:
                            first = loc.first
                            if await first.is_visible():
                                await first.click()
                                logger.info("Pagination: %s%s — clicked %s", dealer_name, path, sel)
                                try:
                                    await page.wait_for_event("response", pred, timeout=pag_wait_ms)
                                except Exception:
                                    pass
                                await asyncio.sleep(0.35)
                                # Let in-flight body reads land so the next iteration's
                                # by_vin/total_count sees this page's payload.
                                await observer.drain(2.0)
                                await _capture_card_locations()
                                clicked = True
                                break
                    except Exception:
                        continue
                if not clicked:
                    if pagination_debug_enabled():
                        logger.info(
                            "Pagination debug [%s]%s stop=next_not_clickable iter=%d",
                            dealer_name,
                            path,
                            pag_iter,
                        )
                    break
                if explore_next and len(by_vin) <= prev_unique_vins:
                    if pagination_debug_enabled():
                        logger.info(
                            "Pagination debug [%s]%s stop=no_vin_growth iter=%d",
                            dealer_name,
                            path,
                            pag_iter,
                        )
                    break
                prev_unique_vins = len(by_vin)
            else:
                if pagination_debug_enabled():
                    if total_count is not None and total_count <= len(by_vin):
                        reason = "total_count_met"
                    elif not next_visible:
                        reason = "no_next_control"
                    else:
                        reason = "no_total_count"
                    logger.info(
                        "Pagination debug [%s]%s stop=%s iter=%d total_count=%s unique_vins=%d",
                        dealer_name,
                        path,
                        reason,
                        pag_iter,
                        total_count,
                        len(by_vin),
                    )
                break

        if not pre_pag_scroll_ran and not _skip_lazy_scroll and not await any_next_control_visible(page):
            await infinite_scroll_lazy_batches(
                page,
                dealer_name,
                path,
                pred,
                pag_wait_ms=pag_wait_ms,
            )

        if found_data["value"]:
            html = None
        else:
            html = await page.content()
    except Exception as e:
        logger.warning("Path scrape failed [%s] %s: %s", dealer_name, full_url, e)
    finally:
        try:
            # Body reads still in flight would be killed by page.close() below,
            # dropping their payloads (typically the last batch).
            await observer.drain(5.0)
        except Exception:
            pass
        try:
            observer.log_summary()
            observer.save_ledger()
        except Exception:
            pass
        try:
            await page.close()
        except Exception:
            pass
    return local_records, html, observer.url_denied, card_locations, list(observer.ledger.endpoints.values())

__all__ = ['scrape_inventory_path']
