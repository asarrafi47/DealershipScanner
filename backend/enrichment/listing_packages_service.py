"""
Orchestrate per-listing enrichment: dealer description → structured packages,
window sticker download/analysis, optional photo vision merge.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from backend.db.inventory_db import get_car_by_id, update_car_row_partial
from backend.utils.field_clean import is_effectively_empty
from backend.utils.listing_description_extract import normalize_listing_description
from backend.utils.listing_description_persist import process_listing_description_for_row

logger = logging.getLogger(__name__)

_DESCRIPTION_SELECTORS = (
    r'class=["\'][^"\']*(?:vehicle-description|vehicleDescription|dealer-comments|'
    r"dealerComments|seller-notes|sellerNotes|listing-description|vdp-description|"
    r"atcui-vdp-about|vehicle-details-description)[^\"\']*[\"'][^>]*>(.*?)</",
    r'id=["\'](?:vehicle-description|vehicleDescription)["\'][^>]*>(.*?)</',
    r'"description"\s*:\s*"([^"]{40,})"',
)
_BOILERPLATE_RE = re.compile(
    r"(call us|contact us|schedule a test drive|disclaimer|financing available|visit our|"
    r"taxes and fees|subject to change|msrp does not include)",
    re.IGNORECASE,
)


def extract_description_from_listing_html(html: str) -> str:
    """Best-effort dealer description text from listing/VDP HTML."""
    if not (html or "").strip():
        return ""
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        for sel in (
            ".vehicle-description",
            ".vehicleDescription",
            ".dealer-comments",
            ".seller-notes",
            ".listing-description",
            ".vdp-description",
            ".atcui-vdp-about",
            "[data-test='vehicle-description']",
        ):
            node = soup.select_one(sel)
            if node:
                text = node.get_text(" ", strip=True)
                if len(text) >= 40:
                    return _clean_description_text(text)
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or ""
            if "description" in raw.lower() and len(raw) > 60:
                m = re.search(r'"description"\s*:\s*"([^"]{40,})"', raw)
                if m:
                    return _clean_description_text(m.group(1).encode().decode("unicode_escape"))
    except Exception:
        pass
    for pat in _DESCRIPTION_SELECTORS:
        m = re.search(pat, html, re.I | re.DOTALL)
        if m:
            chunk = re.sub(r"<[^>]+>", " ", m.group(1))
            chunk = re.sub(r"\s+", " ", chunk).strip()
            if len(chunk) >= 40:
                return _clean_description_text(chunk)
    return ""


def _clean_description_text(raw: str) -> str:
    lines = [ln.strip() for ln in raw.replace("\r\n", "\n").split("\n")]
    cleaned = [ln for ln in lines if ln and not _BOILERPLATE_RE.search(ln)]
    return " ".join(cleaned)[:6000]


def _listing_vdp_url(car: dict[str, Any]) -> str:
    return str(
        car.get("source_url") or car.get("_detail_url") or car.get("listing_vdp_url") or ""
    ).strip()


def _discover_listing_sticker_url(car: dict[str, Any]) -> str | None:
    from backend.enrichment.window_sticker_service import _listing_sticker_urls_for_car

    urls = _listing_sticker_urls_for_car(car)
    return urls[0] if urls else None


def ensure_listing_description_for_car(
    car_id: int,
    car: dict[str, Any] | None = None,
    *,
    refetch_if_missing: bool = True,
) -> dict[str, Any]:
    """
    Parse ``description`` into packages; fetch from VDP when text is missing/short.
    """
    row = car if car is not None else get_car_by_id(int(car_id), include_inactive=True)
    if not row:
        return {"applied": False, "reason": "not_found", "updates": None, "description_fetched": False}

    desc = str(row.get("description") or "")
    norm = normalize_listing_description(desc)
    description_fetched = False

    if refetch_if_missing and len(norm) < 20:
        url = _listing_vdp_url(row)
        if url.lower().startswith("http"):
            try:
                from backend.scanner.listing_gap_fill import fetch_listing_html

                html = fetch_listing_html(url)
            except Exception as e:
                logger.debug("Listing HTML fetch for description failed: %s", e)
                html = None
            if html:
                extracted = extract_description_from_listing_html(html)
                if len(normalize_listing_description(extracted)) >= 20:
                    update_car_row_partial(int(car_id), {"description": extracted})
                    row = get_car_by_id(int(car_id), include_inactive=True) or row
                    description_fetched = True

    result = process_listing_description_for_row(row, skip_if_unchanged=True, force=False)
    result["description_fetched"] = description_fetched
    return result


def ensure_listing_sticker_url_for_car(car_id: int, car: dict[str, Any]) -> bool:
    """Persist a discovered Monroney/iPacket URL from the listing when the row has none."""
    if not is_effectively_empty(car.get("window_sticker_url")):
        return False
    discovered = _discover_listing_sticker_url(car)
    if not discovered:
        return False
    update_car_row_partial(int(car_id), {"window_sticker_url": discovered})
    return True


def ensure_listing_packages_for_car(
    car_id: int,
    *,
    allow_vision_fallback: bool = True,
    refetch_description: bool = True,
) -> dict[str, Any]:
    """
    Full per-listing packages pipeline:
    1. Fetch/parse dealer description
    2. Discover sticker URL from listing when missing
    3. Download + analyze window sticker (OEM or listing Monroney)
    4. Optional photo vision merge (does not wipe description/sticker data)
    """
    car = get_car_by_id(int(car_id), include_inactive=True)
    if not car:
        return {"ok": False, "error": "not_found"}

    out: dict[str, Any] = {"ok": True, "car_id": int(car_id)}

    desc_result = ensure_listing_description_for_car(
        int(car_id),
        car,
        refetch_if_missing=refetch_description,
    )
    out["listing_description_parsed"] = bool(desc_result.get("applied"))
    out["listing_description_reason"] = desc_result.get("reason")
    out["listing_description_fetched"] = bool(desc_result.get("description_fetched"))
    if desc_result.get("applied") and desc_result.get("updates"):
        update_car_row_partial(int(car_id), desc_result["updates"])
        car = get_car_by_id(int(car_id), include_inactive=True) or car

    if ensure_listing_sticker_url_for_car(int(car_id), car):
        car = get_car_by_id(int(car_id), include_inactive=True) or car
        out["listing_sticker_url_discovered"] = True

    from backend.enrichment.window_sticker_service import (
        ensure_window_sticker_for_car,
        should_skip_photo_package_analysis,
    )

    car = get_car_by_id(int(car_id), include_inactive=True) or car
    allow_vision = allow_vision_fallback and not should_skip_photo_package_analysis(car)
    sticker_status = ensure_window_sticker_for_car(
        int(car_id),
        allow_vision_fallback=allow_vision,
    )
    out.update(sticker_status)
    return out
