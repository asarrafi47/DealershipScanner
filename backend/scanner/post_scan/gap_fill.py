"""
Post-scan listing gap fill: dictionary/vPIC first, then listing-page HTML (Playwright),
optional DuckDuckGo Instant Answer for residual mechanical fields.

**Condition** is only taken from listing/VDP HTML (never invented from DDG). DDG is used only
for supplemental transmission/drivetrain/fuel hints when catalog + page HTML leave gaps.

Triggered after ``scanner.py`` finishes inventory upsert + ``run_post_scan`` repair when
``--post-listing-gap-fill`` or ``SCANNER_POST_LISTING_GAP_FILL=1``.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import requests

from backend.db.inventory_db import get_car_by_id, get_car_by_vin, refresh_car_data_quality_score, update_car_row_partial
from backend.enrichment.spec_structured_backfill import apply_structured_spec_backfill_for_car
from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty
from backend.utils.listing_completeness import listing_missing_field_codes
from backend.utils.spec_provenance import merge_spec_source_json
from backend.scanner.utils.vdp_spec_parse import parse_condition_from_listing_html, parse_html_for_vehicle_specs

logger = logging.getLogger(__name__)

USER_AGENT = (
    "SarrafiCollection/1.0 (+https://example.local; listing gap-fill)"
)
_DDG_SPEC_HTML_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
DDG_HTML_URL = "https://html.duckduckgo.com/html/"

_ddg_html_spec_banned: bool = False


def _max_vins_per_run() -> int:
    try:
        v = int((os.environ.get("SCANNER_POST_LISTING_GAP_FILL_MAX") or "400").strip())
    except (TypeError, ValueError):
        return 400
    return max(1, min(5000, v))


def _requests_fetch_html(url: str, timeout_s: float = 22.0) -> str | None:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
            timeout=timeout_s,
        )
        r.raise_for_status()
        text = r.text
        return text if len(text) > 500 else None
    except requests.RequestException as e:
        logger.debug("requests fetch failed %s: %s", url[:80], e)
        return None


def _playwright_fetch_html(url: str, timeout_ms: int = 65000) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("playwright not installed; skipping JS listing fetch for %s", url[:80])
        return None
    ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(user_agent=ua)
                page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                for _ in range(15):
                    title = (page.title() or "").lower()
                    body_head = (page.inner_text("body") or "")[:240].lower()
                    if "just a moment" not in title and "just a moment" not in body_head:
                        break
                    page.wait_for_timeout(2000)
                page.wait_for_timeout(
                    int(os.environ.get("LISTING_GAP_FILL_POST_GOTO_MS") or "5000")
                )
                return page.content()
            finally:
                browser.close()
    except Exception as e:
        logger.warning("Playwright listing fetch failed %s: %s", url[:80], e)
        return None


def fetch_listing_html(url: str) -> str | None:
    """
    Prefer lightweight HTTP GET; use Playwright when the body looks like a thin SPA shell
    or condition/spec signals are missing.
    """
    if not url or not url.lower().startswith("http"):
        return None
    raw = _requests_fetch_html(url)
    if raw:
        low = raw.lower()
        has_signals = (
            "vehiclecondition" in low
            or "itemcondition" in low
            or "application/ld+json" in low
            or "condition" in low and ("inventory" in low or "vehicle" in low)
        )
        if len(raw) > 8000 and has_signals:
            return raw
    # Retry with Playwright for JS-rendered inventory/VDP pages.
    try:
        import asyncio

        asyncio.get_running_loop()
    except RuntimeError:
        return _playwright_fetch_html(url)
    # Post-scan runs inside scanner's asyncio loop — sync Playwright must run in a thread.
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_playwright_fetch_html, url).result()


def _ddg_html_search_specs(car: dict[str, Any]) -> dict[str, Any]:
    """
    DDG HTML search fallback for spec gaps. Parses real organic snippet text for
    transmission / drivetrain / fuel_type / body_style / cylinders.
    Never sets condition.
    """
    global _ddg_html_spec_banned
    if _ddg_html_spec_banned:
        return {}

    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = (car.get("trim") or "").strip()
    if not make or not model:
        return {}

    parts = [str(year) if year else None, make, model, trim or None, "specifications"]
    q = " ".join(p for p in parts if p)
    try:
        r = requests.get(
            DDG_HTML_URL,
            params={"q": q},
            headers={
                "User-Agent": _DDG_SPEC_HTML_UA,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=float(os.environ.get("LISTING_GAP_FILL_DDG_TIMEOUT") or "15"),
        )
    except requests.RequestException as e:
        logger.debug("DDG HTML spec search failed: %s", e)
        return {}

    if r.status_code in (403, 429):
        _ddg_html_spec_banned = True
        logger.warning("DDG HTML spec search blocked (%d) — disabling for this run", r.status_code)
        return {}
    if r.status_code == 202:
        logger.debug("DDG HTML spec search rate-limited (202)")
        return {}

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, "html.parser")
    snippets: list[str] = []
    for sel in (".result__snippet", ".result__body", ".result-snippet"):
        for el in soup.select(sel)[:8]:
            t = el.get_text(" ", strip=True)
            if t:
                snippets.append(t)
    if not snippets:
        for el in soup.select(".result")[:6]:
            t = el.get_text(" ", strip=True)
            if t:
                snippets.append(t)
    text = " ".join(snippets)
    low = text.lower()
    if not low:
        return {}

    out: dict[str, Any] = {}
    from backend.scanner.utils.vdp_spec_parse import _cylinders_from_engine_blob

    if is_effectively_empty(car.get("transmission")):
        if "continuously variable" in low or " cvt" in low:
            out["transmission"] = "CVT"
        else:
            m_spd = re.search(r"(\d{1,2})-speed\s+automatic", low)
            m_man = re.search(r"(\d)-speed\s+manual", low)
            if m_spd:
                out["transmission"] = f"{m_spd.group(1)}-Speed Automatic"
            elif m_man:
                out["transmission"] = f"{m_man.group(1)}-Speed Manual"
            elif re.search(r"\bmanual\s+transmission\b", low):
                out["transmission"] = "Manual"
            elif "automatic" in low and "manual" not in low:
                out["transmission"] = "Automatic"
            elif "manual" in low:
                out["transmission"] = "Manual"

    if is_effectively_empty(car.get("drivetrain")):
        if re.search(r"\ball-wheel\b|\bawd\b", low):
            out["drivetrain"] = "AWD"
        elif re.search(r"\bfour-wheel\b|\b4wd\b|\b4x4\b", low):
            out["drivetrain"] = "4WD"
        elif re.search(r"\bfront-wheel\b|\bfwd\b", low):
            out["drivetrain"] = "FWD"
        elif re.search(r"\brear-wheel\b|\brwd\b", low):
            out["drivetrain"] = "RWD"

    if is_effectively_empty(car.get("fuel_type")):
        if re.search(r"\bplug.in hybrid\b|\bphev\b", low):
            out["fuel_type"] = "Plug-In Hybrid"
        elif re.search(r"\bmild hybrid\b", low):
            out["fuel_type"] = "Hybrid"
        elif re.search(r"\bhybrid\b", low):
            out["fuel_type"] = "Hybrid"
        elif re.search(r"\belectric\b|\bbev\b|\ball.electric\b", low) and "gasoline" not in low[:300]:
            out["fuel_type"] = "Electric"
        elif re.search(r"\bdiesel\b", low):
            out["fuel_type"] = "Diesel"
        elif re.search(r"\bgasoline\b|\bgas\b|\bpetrol\b", low):
            out["fuel_type"] = "Gasoline"

    if is_effectively_empty(car.get("body_style")):
        for phrase, canonical in (
            ("sport utility vehicle", "SUV"), ("suv", "SUV"),
            ("crossover", "Crossover"), ("pickup truck", "Truck"),
            ("pickup", "Truck"), ("minivan", "Minivan"),
            ("convertible", "Convertible"), ("hatchback", "Hatchback"),
            ("wagon", "Wagon"), ("coupe", "Coupe"),
            ("sedan", "Sedan"), ("van", "Van"),
        ):
            if phrase in low:
                out["body_style"] = canonical
                break

    if is_effectively_empty(car.get("cylinders")):
        c = _cylinders_from_engine_blob(text[:1000])
        if c is not None:
            out["cylinders"] = c

    return out


def _merge_provenance(
    existing_json: str | None,
    fields: dict[str, Any],
    *,
    source: str,
    url: str | None,
) -> str:
    patch: dict[str, Any] = {}
    for k in fields:
        patch[k] = {"source": source, "detail": "listing_gap_fill", "url": url or ""}
    return merge_spec_source_json(existing_json, patch)


def _try_window_sticker_enrich(vin: str, raw: dict[str, Any], proposed: dict[str, Any]) -> None:
    """
    Gap-fill fallback when post-scan sticker stage did not run or was capped.
    Persists PDF + packages via ``ensure_window_sticker_for_car`` for OEM and listing stickers.
    """
    try:
        from backend.enrichment.listing_packages_service import ensure_listing_sticker_url_for_car
        from backend.enrichment.window_sticker_service import (
            car_sticker_packages_need_analysis,
            ensure_window_sticker_for_car,
            window_sticker_available,
        )
        from backend.scanner.post_scan.window_sticker import (
            car_listing_may_have_sticker,
            get_window_sticker_url,
            is_cdjr_stellantis_car,
        )

        needs_sticker = not window_sticker_available(raw) or car_sticker_packages_need_analysis(raw)
        if not needs_sticker:
            return
        can_fetch = is_cdjr_stellantis_car(raw) and bool(get_window_sticker_url(vin))
        if not can_fetch and not car_listing_may_have_sticker(raw):
            return
        cid = raw.get("id")
        if not cid:
            return
        if is_effectively_empty(raw.get("window_sticker_url")):
            if is_cdjr_stellantis_car(raw):
                sticker_url = get_window_sticker_url(vin)
                if sticker_url:
                    proposed.setdefault("window_sticker_url", sticker_url)
            elif ensure_listing_sticker_url_for_car(int(cid), raw):
                refreshed = get_car_by_id(int(cid), include_inactive=True) or raw
                ws = str(refreshed.get("window_sticker_url") or "").strip()
                if ws:
                    proposed.setdefault("window_sticker_url", ws)
        ensure_window_sticker_for_car(int(cid), allow_vision_fallback=False)
    except Exception as e:
        logger.debug("Window sticker enrich failed for %s: %s", vin, e)


def _heal_workers() -> int:
    try:
        return max(1, min(12, int((os.environ.get("SCANNER_HEAL_WORKERS") or "4").strip())))
    except ValueError:
        return 4


def _ddg_max_per_run() -> int:
    try:
        return max(0, min(2000, int((os.environ.get("LISTING_GAP_FILL_DDG_MAX") or "60").strip())))
    except ValueError:
        return 60


# Spec fields the DDG snippet fallback is able to fill; skip the (slow,
# rate-limited) search entirely when none of these are still missing.
_DDG_FILLABLE = frozenset({"transmission", "drivetrain", "fuel_type", "body_style", "cylinders"})


def run_listing_gap_fill_for_vins(vins: list[str]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "vins_input": len(vins),
        "rows_examined": 0,
        "structured_backfill_applied": 0,
        "rows_patched": 0,
        "web_fetch_ok": 0,
        "ddg_patch_fields": 0,
        "skipped_complete": 0,
        "skipped_cap": 0,
    }
    cap = _max_vins_per_run()
    allow_ddg = (os.environ.get("LISTING_GAP_FILL_ALLOW_DDG") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    work = vins[:cap]
    stats["skipped_cap"] = max(0, len(vins) - cap)

    import threading
    from concurrent.futures import ThreadPoolExecutor

    lock = threading.Lock()
    ddg_budget = {"left": _ddg_max_per_run() if allow_ddg else 0}

    def _bump(key: str, n: int = 1) -> None:
        with lock:
            stats[key] += n

    def _take_ddg_slot() -> bool:
        with lock:
            if ddg_budget["left"] <= 0:
                return False
            ddg_budget["left"] -= 1
            return True

    def _heal_one(vin: str) -> None:
        raw = get_car_by_vin(vin)
        if not raw:
            return
        cid = int(raw["id"])
        missing = listing_missing_field_codes(raw, for_public_filter=False)
        if not missing:
            _bump("skipped_complete")
            return
        _bump("rows_examined")

        res = apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
        if res.applied:
            _bump("structured_backfill_applied")

        raw = get_car_by_id(cid, include_inactive=True)
        if not raw:
            return
        missing = listing_missing_field_codes(raw, for_public_filter=False)
        if not missing:
            refresh_car_data_quality_score(cid)
            _bump("skipped_complete")
            return

        url = (raw.get("source_url") or "").strip()
        proposed: dict[str, Any] = {}
        html: str | None = None

        need_page = "condition" in missing or (
            {"price", "transmission", "drivetrain", "fuel_type", "body_style", "cylinders",
             "mpg_city", "mpg_highway", "exterior_color", "interior_color"} & set(missing)
        )
        if url and need_page:
            html = fetch_listing_html(url)
            if html:
                _bump("web_fetch_ok")

        if html:
            if "price" in missing:
                from backend.scanner.utils.vdp_spec_parse import parse_price_from_listing_html

                page_price = parse_price_from_listing_html(html)
                if page_price:
                    proposed["price"] = int(round(page_price))
            if "condition" in missing:
                cond = parse_condition_from_listing_html(html)
                if cond:
                    proposed["condition"] = cond
            # Color: parse labeled rows common to most server-rendered platforms
            if "exterior_color" in missing or "interior_color" in missing:
                from backend.scanner.utils.vdp_spec_parse import parse_color_from_listing_html
                colors = parse_color_from_listing_html(html)
                if "exterior_color" in missing and colors.get("exterior_color"):
                    proposed["exterior_color"] = str(colors["exterior_color"])[:120]
                if "interior_color" in missing and colors.get("interior_color"):
                    proposed["interior_color"] = str(colors["interior_color"])[:120]
            specs = parse_html_for_vehicle_specs(html)
            if "transmission" in missing and specs.get("transmission"):
                proposed["transmission"] = str(specs["transmission"])[:200]
            if "drivetrain" in missing and specs.get("drivetrain"):
                proposed["drivetrain"] = str(specs["drivetrain"])[:120]
            if "fuel_type" in missing and specs.get("fuel_type"):
                proposed["fuel_type"] = str(specs["fuel_type"])
            if "body_style" in missing and specs.get("body_style"):
                proposed["body_style"] = str(specs["body_style"])
            cyl = specs.get("cylinders")
            if "cylinders" in missing and cyl is not None:
                try:
                    ci = int(cyl)
                    if ci >= 0:
                        proposed["cylinders"] = ci
                except (TypeError, ValueError):
                    pass
            if "mpg_city" in missing and specs.get("mpg_city") is not None:
                try:
                    proposed["mpg_city"] = int(specs["mpg_city"])
                except (TypeError, ValueError):
                    pass
            if "mpg_highway" in missing and specs.get("mpg_highway") is not None:
                try:
                    proposed["mpg_highway"] = int(specs["mpg_highway"])
                except (TypeError, ValueError):
                    pass

        raw = get_car_by_id(cid, include_inactive=True)
        if not raw:
            return

        remaining = set(listing_missing_field_codes(raw, for_public_filter=False)) - set(proposed)
        if allow_ddg and (remaining & _DDG_FILLABLE) and _take_ddg_slot():
            ddg_patch = _ddg_html_search_specs(dict(raw))
            for k, v in ddg_patch.items():
                if k in proposed:
                    continue
                if not is_effectively_empty(raw.get(k)):
                    continue
                proposed[k] = v
                _bump("ddg_patch_fields")

        # OEM window sticker: store URL + parse options into packages if not already present
        _try_window_sticker_enrich(vin, dict(raw), proposed)

        if not proposed:
            return

        merged = clean_car_row_dict({**dict(raw), **proposed})
        diff: dict[str, Any] = {}
        for k in proposed:
            if merged.get(k) != raw.get(k):
                diff[k] = merged[k]
        if not diff:
            return

        diff["spec_source_json"] = _merge_provenance(
            raw.get("spec_source_json"),
            diff,
            source="listing_gap_fill",
            url=url or None,
        )
        update_car_row_partial(cid, diff)
        refresh_car_data_quality_score(cid)
        _bump("rows_patched")

    if work:
        with ThreadPoolExecutor(max_workers=min(_heal_workers(), len(work))) as pool:
            for _ in pool.map(_heal_one, work):
                pass

    return stats
