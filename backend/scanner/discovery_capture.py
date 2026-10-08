"""Discovery-only browser capture: learn how an unknown site serves its inventory.

This is the ONE sanctioned browser entry point (docs/HTTP_ONLY_SCANS_PLAN.md,
Phase 1). It opens the dealer's SRP(s) with the NetworkObserver attached,
scrolls/paginates once, promotes the captured inventory endpoints into the
dealer's recipe file and reports what it saw. It never writes car rows; the
HTTP-only scan replays the recipes afterwards.

Run it as its own process so ``SCANNER_ALLOW_BROWSER=1`` never lives inside a
scan process::

    python -m backend.scripts.discovery_probe --browser-capture --dealers <id>
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("scanner")


def capture_place(dealer: dict[str, Any], origin: str) -> dict[str, str] | None:
    """The store's place for the capture's validation gate: the same one the scan
    replay uses (``roster_place_with_hints``: registry row plus the page-learned
    street kept in scan hints), so a captured recipe is judged on the rows the
    scan will keep. The dealer dict the probe passes carries only id / url / name /
    provider; judging with that alone refused every street-block row of a store
    whose street lives in scan hints (Honda of Huntersville shape) and the good
    capture was turned away as ``zero_rows``."""
    from backend.scanner.dealer_place import place_kwargs, roster_place_with_hints

    place = place_kwargs(dealer)
    try:
        place.update(place_kwargs(roster_place_with_hints(origin, str(dealer.get("dealer_id") or ""))))
    except Exception as exc:  # noqa: BLE001 - registry / hint store down: judge with what the caller gave
        logger.debug("capture place lookup failed [%s]: %s", dealer.get("dealer_id"), exc)
    return place or None


def _endpoint_summary(ep: Any) -> dict[str, Any]:
    return {
        "method": str(getattr(ep, "method", "") or ""),
        "url": str(getattr(ep, "url", "") or "")[:200],
        "content_type": str(getattr(ep, "content_type", "") or "")[:60],
        "vehicle_rows": int(getattr(ep, "vehicle_rows", 0) or 0),
        "total_count": getattr(ep, "total_count", None),
        "provider_hint": str(getattr(ep, "provider_hint", "") or ""),
        "post_sample": (str(getattr(ep, "post_data_sample", "") or "")[:200]) or None,
    }


async def capture_endpoints(
    dealer_id: str,
    url: str,
    dealer_name: str = "",
    provider: str = "dealer_dot_com",
    *,
    dealer: dict[str, Any] | None = None,
    paths: list[str] | None = None,
    profile: bool = True,
    promote: bool = True,
) -> dict[str, Any]:
    """Open the SRP(s) in a headless browser, capture inventory endpoints, promote recipes."""
    os.environ["SCANNER_ALLOW_BROWSER"] = "1"
    from backend.scanner.browser_gate import describe, require_browser
    from backend.scanner.http_fetch import playwright_proxy_kwargs
    from backend.scanner.phases.inventory_scrape import scrape_inventory_path
    from backend.scanner.phases.nav import get_rotating_ua, inventory_wait_ms, pagination_response_wait_ms
    from backend.scanner.phases.site_profile import choose_inventory_paths, profile_dealer_site
    from backend.scanner.recipes import load_recipes, promote_from_ledger

    require_browser("discovery_capture")
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    dealer = dict(dealer or {})
    dealer.setdefault("dealer_id", dealer_id)
    dealer.setdefault("url", url)
    dealer.setdefault("name", dealer_name or dealer_id)
    dealer.setdefault("provider", provider)
    # Recipe-store I/O (load_recipes, promote_from_ledger: the cache file and the
    # dealer_recipes sync) runs off the event loop the browser capture shares.
    started = time.time()
    recipes_before = len([r for r in await asyncio.to_thread(load_recipes, dealer_id) if not r.stale])
    out: dict[str, Any] = {
        "dealer_id": dealer_id, "url": origin, "mode": describe(), "started": started,
        "paths": [], "endpoints": [], "records": 0, "recipes_before": recipes_before,
        "recipes_written": 0, "profile": None, "errors": [],
    }
    t0 = time.perf_counter()
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        out["errors"].append(f"playwright not installed: {exc}")
        return out
    try:
        from playwright_stealth import Stealth

        pw_cm = Stealth().use_async(async_playwright())
    except ImportError:
        pw_cm = async_playwright()
    endpoints: list[Any] = []
    async with pw_cm as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"], **playwright_proxy_kwargs())
        context = None
        try:
            context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=get_rotating_ua())
            site_profile = None
            if profile:
                try:
                    site_profile = await asyncio.wait_for(profile_dealer_site(context, origin, []), timeout=90)
                    if site_profile is not None:
                        out["profile"] = {
                            "provider": getattr(site_profile, "detected_provider", None),
                            "confidence": getattr(site_profile, "confidence_score", None),
                        }
                        if getattr(site_profile, "detected_provider", None):
                            provider = str(site_profile.detected_provider)
                except Exception as exc:  # noqa: BLE001
                    out["errors"].append(f"profiler: {str(exc)[:160]}")
            inv_paths = list(paths or choose_inventory_paths(site_profile, dealer))[:4]
            out["paths"] = inv_paths
            for path in inv_paths:
                try:
                    records, html, denied, _card_locs, path_endpoints = await asyncio.wait_for(
                        scrape_inventory_path(context, path, origin, provider, dealer_id, dealer.get("name", dealer_id),
                                              inventory_wait_ms(), pagination_response_wait_ms(), site_profile=site_profile),
                        timeout=240,
                    )
                    out["records"] += len(records)
                    endpoints.extend(path_endpoints or [])
                    logger.info("discovery capture [%s] %s: %d intercept record(s), %d endpoint(s), %d denied",
                                dealer_id, path, len(records), len(path_endpoints or []), denied)
                except asyncio.TimeoutError:
                    out["errors"].append(f"{path}: capture timed out")
                except Exception as exc:  # noqa: BLE001
                    out["errors"].append(f"{path}: {str(exc)[:160]}")
        finally:
            if context is not None:
                try:
                    await context.close()
                except Exception:  # noqa: BLE001
                    pass
            try:
                await browser.close()
            except Exception:  # noqa: BLE001
                pass
    # dedupe endpoints by (method, url without query)
    seen: set[tuple[str, str]] = set()
    uniq: list[Any] = []
    for ep in endpoints:
        url = str(getattr(ep, "url", "") or "")
        key = (str(getattr(ep, "method", "") or ""), url.split("?")[0])
        if str(getattr(ep, "reason", "") or "") == "html_fragment_cards":
            # one PixelMotion XHR path serves new / used / cpo by query: keep each section
            from backend.scanner.recipes import html_page_section_key

            key = (key[0], key[1] + "?" + html_page_section_key(url))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(ep)
    out["endpoints"] = [_endpoint_summary(ep) for ep in sorted(uniq, key=lambda e: -int(getattr(e, "vehicle_rows", 0) or 0))][:12]
    if promote and uniq:
        # The set is judged against the site's own count before the save
        # (recipe_validation): a one-condition / section-scoped / short-page /
        # dead-auth capture is refused here, not by a fleet verdict a scan later.
        validation: dict[str, Any] = {}

        def _promote() -> int:
            # capture_place reads the registry and scan hints; promote_from_ledger
            # validates over HTTP and writes the recipe file and dealer_recipes.
            return int(promote_from_ledger(
                dealer_id, provider, uniq, validate=True, base_url=origin, dealer_name=dealer.get("name") or dealer_id,
                place=capture_place(dealer, origin), validation_out=validation,
            ) or 0)

        try:
            out["recipes_written"] = await asyncio.to_thread(_promote)
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"promote: {str(exc)[:160]}")
        if validation:
            out["validation"] = validation
            if validation.get("verdict") == "reject":
                out["errors"].append("validation: " + "; ".join(validation.get("reasons") or [])[:200])
    out["recipes_after"] = len([r for r in await asyncio.to_thread(load_recipes, dealer_id) if not r.stale])
    out["seconds"] = round(time.perf_counter() - t0, 1)
    return out


def capture_endpoints_sync(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return asyncio.run(capture_endpoints(*args, **kwargs))


__all__ = ["capture_endpoints", "capture_endpoints_sync", "capture_place"]
