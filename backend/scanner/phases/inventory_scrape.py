"""Single inventory URL path scrape (JSON intercept + pagination)."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

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
    truncate_url,
    try_apply_location_filter,
)
from backend.scanner.scrapers.scanner_intercept_filter import (
    effective_lot_total_from_intercepts,
    intercept_url_allowed,
    payload_qualifies_for_inventory_intercept,
    response_content_type_looks_json,
)
from backend.scanner.scan_efficiency import (
    inventory_idle_loop_sec,
    inventory_json_wait_ms,
)

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
) -> tuple[list[tuple[str, Any]], str | None, int]:
    """
    Scrape one inventory path on a dedicated page within ``context``.
    Returns ``(intercept_records, page_html, url_denied_count)``.
    Opens and closes its own page; does not touch the warmup/VDP page.
    Structured JSON interception is primary: qualifying payloads skip saving HTML for this path.
    When no Next/Load-more control is visible, a scroll-to-bottom loop pulls lazy-loaded batches.
    """
    local_records: list[tuple[str, Any]] = []
    found_data = {"value": False}
    url_denied = 0
    resp_err = DealerResponseErrorBudget()

    page = await context.new_page()

    async def handle_response(response: Any) -> None:
        nonlocal url_denied
        try:
            ct = response.headers.get("content-type") or ""
            if not response_content_type_looks_json(ct):
                return
            rurl = str(getattr(response, "url", "") or "")
            if not intercept_url_allowed(rurl, base_url):
                url_denied += 1
                logger.debug("Intercept URL denied [%s] path=%s: %s", dealer_name, path, truncate_url(rurl))
                return
            body = await response.json()
            if not payload_qualifies_for_inventory_intercept(body):
                return
            local_records.append((rurl, body))
            found_data["value"] = True
            logger.info(
                "Intercepting: %s%s — structured inventory JSON (%s)",
                dealer_name,
                path,
                truncate_url(rurl, 80),
            )
        except Exception as e:
            if resp_err.should_log():
                logger.debug(
                    "Intercept handler [%s] path=%s: %s %s",
                    dealer_name,
                    path,
                    type(e).__name__,
                    str(e)[:200],
                )

    page.on("response", handle_response)
    html: str | None = None
    full_url = base_url + path
    try:
        logger.info("Navigating: %s — %s", dealer_name, full_url)
        await goto_with_retries(page, full_url, log_label=f"Nav:{dealer_name}", timeout_ms=20000)
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
            loc_applied = await try_apply_location_filter(page, dealer_name, pred, pag_wait_ms)
            if loc_applied:
                # Clear any intercepts captured before the filter applied and wait for fresh data.
                local_records.clear()
                found_data["value"] = False
                try:
                    await page.wait_for_event("response", pred, timeout=inv_wait_ms)
                except Exception:
                    pass
                await asyncio.sleep(1.0)

        idle_cap = inventory_idle_loop_sec(found_data["value"])
        for _ in range(idle_cap):
            await asyncio.sleep(1)
            if found_data["value"]:
                break
        await asyncio.sleep(0.5)

        # Quick viewport ping when JSON hasn't landed — avoids dead wait when API-backed payloads are slow
        if not found_data["value"]:
            logger.info("Scrolling: %s%s — no JSON yet, initial scroll pulses", dealer_name, path)
            for _ in range(5):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

        if not await any_next_control_visible(page):
            await infinite_scroll_lazy_batches(
                page,
                dealer_name,
                path,
                pred,
                pag_wait_ms=pag_wait_ms,
            )

        from backend.scanner.scrapers.pixel_motion import _is_pixel_motion_html

        peek_html = await page.content()
        if _is_pixel_motion_html(peek_html):
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
            return local_records, html, url_denied

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
        for pag_iter in range(MAX_PAGINATION_CLICKS):
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

        if not await any_next_control_visible(page):
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
            await page.close()
        except Exception:
            pass
    return local_records, html, url_denied

__all__ = ['scrape_inventory_path']
