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
DDG_INSTANT_URL = "https://api.duckduckgo.com/"


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
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                page.wait_for_timeout(int(os.environ.get("LISTING_GAP_FILL_POST_GOTO_MS") or "2200"))
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
    # Retry with Playwright for JS-rendered inventory/VDP pages
    return _playwright_fetch_html(url)


def _ddg_abstract_specs(car: dict[str, Any]) -> dict[str, Any]:
    """
    Weak fallback: parse DuckDuckGo Instant Answer abstract for transmission/drivetrain/fuel keywords.
    Never sets condition.
    """
    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = (car.get("trim") or "").strip()
    if not make or not model:
        return {}
    q = f"{year} {make} {model} {trim}".strip() + " specifications transmission drivetrain"
    try:
        r = requests.get(
            DDG_INSTANT_URL,
            params={"q": q, "format": "json", "no_html": "1", "skip_disambig": "1"},
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=min(15.0, float(os.environ.get("LISTING_GAP_FILL_DDG_TIMEOUT") or "12")),
        )
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.debug("DDG supplement failed: %s", e)
        return {}

    abstract = str(data.get("AbstractText") or "")
    if len(abstract) < 40:
        return {}
    low = abstract.lower()
    out: dict[str, Any] = {}
    # Transmission
    if is_effectively_empty(car.get("transmission")):
        if "continuously variable" in low or "cvt" in low:
            out["transmission"] = "CVT"
        else:
            m_spd = re.search(r"\b(\d{1,2}-speed)\s+automatic\b", low)
            if m_spd:
                out["transmission"] = m_spd.group(0).title()
            elif "automatic" in low and "manual" not in low[:200]:
                out["transmission"] = "Automatic"
            elif "manual" in low:
                out["transmission"] = "Manual"
    # Drivetrain
    if is_effectively_empty(car.get("drivetrain")):
        if "all-wheel" in low or "awd" in low:
            out["drivetrain"] = "AWD"
        elif "four-wheel" in low or "4wd" in low:
            out["drivetrain"] = "4WD"
        elif "front-wheel" in low or "fwd" in low:
            out["drivetrain"] = "FWD"
        elif "rear-wheel" in low or "rwd" in low:
            out["drivetrain"] = "RWD"
    # Fuel
    if is_effectively_empty(car.get("fuel_type")):
        if "plug-in hybrid" in low or "phev" in low:
            out["fuel_type"] = "Plug-In Hybrid"
        elif "hybrid" in low:
            out["fuel_type"] = "Hybrid"
        elif "electric" in low and "gasoline" not in low[:120]:
            out["fuel_type"] = "Electric"
        elif "diesel" in low:
            out["fuel_type"] = "Diesel"
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

    for idx, vin in enumerate(vins):
        if idx >= cap:
            stats["skipped_cap"] = max(0, len(vins) - cap)
            break
        raw = get_car_by_vin(vin)
        if not raw:
            continue
        cid = int(raw["id"])
        missing = listing_missing_field_codes(raw, for_public_filter=False)
        if not missing:
            stats["skipped_complete"] += 1
            continue
        stats["rows_examined"] += 1

        res = apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
        if res.applied:
            stats["structured_backfill_applied"] += 1

        raw = get_car_by_id(cid, include_inactive=True)
        if not raw:
            continue
        missing = listing_missing_field_codes(raw, for_public_filter=False)
        if not missing:
            refresh_car_data_quality_score(cid)
            stats["skipped_complete"] += 1
            continue

        url = (raw.get("source_url") or "").strip()
        proposed: dict[str, Any] = {}
        html: str | None = None

        need_page = "condition" in missing or (
            {"transmission", "drivetrain", "fuel_type", "body_style", "cylinders"} & set(missing)
        )
        if url and need_page:
            html = fetch_listing_html(url)
            if html:
                stats["web_fetch_ok"] += 1

        if html:
            if "condition" in missing:
                cond = parse_condition_from_listing_html(html)
                if cond:
                    proposed["condition"] = cond
            specs = parse_html_for_vehicle_specs(html)
            if "transmission" in missing and specs.get("transmission"):
                proposed["transmission"] = str(specs["transmission"])[:200]
            if "drivetrain" in missing and specs.get("drivetrain"):
                proposed["drivetrain"] = str(specs["drivetrain"])[:120]
            cyl = specs.get("cylinders")
            if "cylinders" in missing and cyl is not None:
                try:
                    ci = int(cyl)
                    if ci >= 0:
                        proposed["cylinders"] = ci
                except (TypeError, ValueError):
                    pass

        raw = get_car_by_id(cid, include_inactive=True)
        if not raw:
            continue

        if allow_ddg:
            ddg_patch = _ddg_abstract_specs(dict(raw))
            for k, v in ddg_patch.items():
                if k in proposed:
                    continue
                if not is_effectively_empty(raw.get(k)):
                    continue
                proposed[k] = v
                stats["ddg_patch_fields"] += 1

        if not proposed:
            continue

        merged = clean_car_row_dict({**dict(raw), **proposed})
        diff: dict[str, Any] = {}
        for k in proposed:
            if merged.get(k) != raw.get(k):
                diff[k] = merged[k]
        if not diff:
            continue

        diff["spec_source_json"] = _merge_provenance(
            raw.get("spec_source_json"),
            diff,
            source="listing_gap_fill",
            url=url or None,
        )
        update_car_row_partial(cid, diff)
        refresh_car_data_quality_score(cid)
        stats["rows_patched"] += 1

    return stats
