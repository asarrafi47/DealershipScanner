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
    out: dict[str, Any] = {
        "dealer_id": dealer_id, "url": origin, "mode": describe(), "started": time.time(),
        "paths": [], "endpoints": [], "records": 0, "recipes_before": len([r for r in load_recipes(dealer_id) if not r.stale]),
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
        try:
            out["recipes_written"] = int(promote_from_ledger(dealer_id, provider, uniq) or 0)
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"promote: {str(exc)[:160]}")
    out["recipes_after"] = len([r for r in load_recipes(dealer_id) if not r.stale])
    out["seconds"] = round(time.perf_counter() - t0, 1)
    return out


def capture_endpoints_sync(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return asyncio.run(capture_endpoints(*args, **kwargs))


__all__ = ["capture_endpoints", "capture_endpoints_sync"]
