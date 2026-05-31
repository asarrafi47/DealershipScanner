"""
DealerOn (vhcliaa) inventory scraper.

DealerOn SRP pages call /api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{dealer_id}/{search_id}
returning a JSON object with a DisplayCards array. Each DisplayCard contains a VehicleCard dict
with rich vehicle data (VIN, price, colors, photos, etc.).

Strategy:
 1. Detect DealerOn from page HTML (dealeron / vhcliaa JS references).
 2. Navigate to new/used/certified SRP paths and intercept the cosmos/srp/vehicles responses.
 3. For each captured search session, paginate via Playwright's request context (shares browser
    session cookies) using the same URL + pg=N param.
 4. Map VehicleCard dicts to the standard scanner vehicle schema.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_SRP_PATHS = [
    "/new-inventory/index.htm",
    "/used-inventory/index.htm",
    "/certified-inventory/index.htm",
]
_NAV_TIMEOUT = 20_000
_API_WAIT_TIMEOUT = 10.0


def _dealer_on_max_pages() -> int:
    raw = (os.environ.get("SCANNER_DEALER_ON_MAX_PAGES") or "60").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 60


def _dealer_on_max_total() -> int:
    raw = (os.environ.get("SCANNER_DEALER_ON_MAX_TOTAL") or "5000").strip()
    try:
        return max(50, int(raw))
    except ValueError:
        return 5000


def _is_dealer_on_html(html: str) -> bool:
    low = html.lower()
    return "dealeron" in low or "vhcliaa" in low or "prsnbaa.dealeron" in low


def _parse_mileage(raw: str | None) -> int | None:
    if not raw:
        return None
    m = re.search(r"[\d,]+", str(raw).replace(",", ""))
    if m:
        try:
            return int(m.group().replace(",", ""))
        except ValueError:
            pass
    return None


def _parse_price(ip: Any, msrp: Any) -> int | None:
    for pv in (ip, msrp):
        try:
            p = float(pv or 0)
            if p > 0:
                return int(p)
        except (TypeError, ValueError):
            pass
    return None


def _parse_condition(vc: dict) -> str:
    cond_raw = str(vc.get("VehicleCondition") or "").strip().lower()
    vtype = str(vc.get("VehicleType") or "").strip().lower()
    for src in (cond_raw, vtype):
        if "new" in src or src == "n":
            return "New"
        if "certified" in src or "cpo" in src or src == "c":
            return "Certified Pre-Owned"
        if "used" in src or src == "u":
            return "Used"
    return ""


def _build_gallery(vc: dict, base_url: str, vin: str) -> list[str]:
    img_model = vc.get("VehicleImageModel") or {}
    carousel = img_model.get("VehicleImageCarouselModel") or {}
    photo_list = carousel.get("PhotoList") or []
    total_count = int(carousel.get("TotalImageCount") or 0)

    seen: set[str] = set()
    gallery: list[str] = []

    for rel in photo_list:
        if not isinstance(rel, str):
            continue
        u = rel if rel.startswith("http") else base_url.rstrip("/") + "/" + rel.lstrip("/")
        if u not in seen:
            seen.add(u)
            gallery.append(u)

    # Generate additional photo URLs when TotalImageCount > captured list
    if total_count > len(gallery) and vin:
        photo_src = str(img_model.get("VehiclePhotoSrc") or "")
        m = re.match(r"(/inventoryphotos/\d+/[^/]+/ip)/", photo_src)
        if m:
            img_base = m.group(1)
            for n in range(len(gallery) + 1, min(total_count + 1, 49)):
                extra = base_url.rstrip("/") + f"{img_base}/{n}.jpg"
                if extra not in seen:
                    seen.add(extra)
                    gallery.append(extra)

    return gallery[:48]


def _map_vehicle_card(
    vc: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> dict[str, Any] | None:
    vin = str(vc.get("VehicleVin") or "").strip().upper()
    if not vin:
        return None

    price = _parse_price(vc.get("VehicleInternetPrice"), vc.get("VehicleMsrp"))
    mileage = _parse_mileage(str(vc.get("Mileage") or ""))
    condition = _parse_condition(vc)
    gallery = _build_gallery(vc, base_url, vin)
    detail_url = str(vc.get("VehicleDetailUrl") or "").strip() or None

    return {
        "vin": vin,
        "year": int(vc.get("VehicleYear") or 0) or None,
        "make": str(vc.get("VehicleMake") or "").strip() or None,
        "model": str(vc.get("VehicleModel") or "").strip() or None,
        "trim": str(vc.get("VehicleTrim") or vc.get("VehicleRuleAdjustedTrim") or "").strip() or None,
        "price": price,
        "mileage": mileage,
        "condition": condition,
        "exterior_color": str(vc.get("ExteriorColorLabel") or "").strip() or None,
        "interior_color": str(vc.get("InteriorColorLabel") or "").strip() or None,
        "body_style": str(vc.get("VehicleBodyStyle") or vc.get("VehicleBodyType") or "").strip() or None,
        "transmission": str(vc.get("VehicleTransmission") or "").strip() or None,
        "fuel_type": str(vc.get("VehicleFuelType") or "").strip() or None,
        "drivetrain": str(vc.get("VehicleDriveTrain") or "").strip() or None,
        "engine_description": str(vc.get("VehicleEngine") or "").strip() or None,
        "stock_number": str(vc.get("VehicleStockNumber") or "").strip() or None,
        "mpg_city": int(vc.get("VehicleMpgCity") or 0) or None,
        "mpg_highway": int(vc.get("VehicleMpgHwy") or 0) or None,
        "image_url": gallery[0] if gallery else "",
        "gallery": gallery,
        "source_url": detail_url,
        "_detail_url": detail_url,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "dealer_id": dealer_id,
    }


def _extract_vehicles_from_srp_body(
    body: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    vehicles = []
    for card in body.get("DisplayCards") or []:
        vc = card.get("VehicleCard")
        if not isinstance(vc, dict):
            continue
        v = _map_vehicle_card(vc, base_url, dealer_id, dealer_name, dealer_url)
        if v:
            vehicles.append(v)
    return vehicles


async def _scrape_srp_all_pages(
    page: Any,
    srp_url: str,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
    seen_vins: set[str],
) -> list[dict[str, Any]]:
    """
    Navigate to srp_url, capture the first cosmos/srp/vehicles API response,
    then immediately fetch remaining pages via browser JS fetch (before navigating away,
    so the search session cookies remain valid).
    Returns all vehicles found across all pages.
    """
    # Capture the first page via Playwright response interception
    result: list[tuple[str, dict]] = []
    event = asyncio.Event()

    async def handle(response: Any) -> None:
        try:
            rurl = str(getattr(response, "url", "") or "")
            if "cosmos/srp/vehicles" not in rurl:
                return
            body = await response.json()
            if isinstance(body, dict) and "DisplayCards" in body:
                result.append((rurl, body))
                event.set()
        except Exception:
            pass

    page.on("response", handle)
    try:
        await page.goto(srp_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
        try:
            await asyncio.wait_for(event.wait(), timeout=_API_WAIT_TIMEOUT)
        except asyncio.TimeoutError:
            pass
    except Exception as e:
        logger.debug("DealerOn: nav to %s failed: %s", srp_url, e)
    finally:
        try:
            page.remove_listener("response", handle)
        except Exception:
            pass

    if not result:
        return []

    api_url, first_body = result[0]
    paging = (first_body.get("Paging") or {}).get("PaginationDataModel") or {}
    total_pages = int(paging.get("TotalPages") or 1)
    search_id = urlparse(api_url).path.rstrip("/").rsplit("/", 1)[-1]
    logger.info(
        "DealerOn: %s [%s] search_id=%s total_pages=%d",
        dealer_name, srp_url.split("/")[-1], search_id, total_pages,
    )

    vehicles: list[dict[str, Any]] = []

    def _add(v: dict) -> None:
        vin = v.get("vin", "")
        if vin and vin not in seen_vins:
            seen_vins.add(vin)
            vehicles.append(v)

    for v in _extract_vehicles_from_srp_body(first_body, base_url, dealer_id, dealer_name, dealer_url):
        _add(v)

    if total_pages <= 1:
        return vehicles

    # Paginate via browser JS fetch using pn=96 (DealerOn's max per-page).
    # DealerOn pagination: pg controls the page number at a given pn (page size).
    # The intercepted response used pn=12 (default); we re-fetch from pg=1 with pn=96
    # to fill in any gaps. All VINs are deduped via seen_vins.
    # Must happen BEFORE navigating away since the search session is page-context-specific.
    _PAGE_SIZE = 96
    total_count = int(paging.get("TotalCount") or total_pages * 12)
    cap_total = _dealer_on_max_total()
    if total_count > cap_total:
        logger.warning(
            "DealerOn: capping TotalCount %d to %d for %s",
            total_count,
            cap_total,
            dealer_name,
        )
        total_count = cap_total
    http_total_pages = max(1, -(-total_count // _PAGE_SIZE))  # ceiling division
    max_pages = _dealer_on_max_pages()
    if http_total_pages > max_pages:
        logger.warning(
            "DealerOn: capping pagination %d pages to %d for %s",
            http_total_pages,
            max_pages,
            dealer_name,
        )
        http_total_pages = max_pages

    # Strip any existing query params from the intercepted URL to avoid duplicate pg/pn params
    # (DealerOn's initial XHR may include ?pg=1&pn=12; we supply our own pagination params).
    parsed_api = urlparse(api_url)
    clean_api_url = urlunparse(parsed_api._replace(query="", fragment=""))

    for pg in range(1, http_total_pages + 1):
        page_url = f"{clean_api_url}?pg={pg}&pn={_PAGE_SIZE}"
        try:
            body = await page.evaluate(
                """async (url) => {
                    try {
                        const r = await fetch(url, {credentials: 'include', headers: {Accept: 'application/json'}});
                        if (!r.ok) return null;
                        return await r.json();
                    } catch(e) { return null; }
                }""",
                page_url,
            )
        except Exception as e:
            logger.warning("DealerOn: evaluate fetch pg=%d failed for %s: %s", pg, dealer_name, e)
            break
        if not isinstance(body, dict):
            # Null response means 4xx/5xx — session likely expired; further pages will also fail.
            logger.debug("DealerOn: pg=%d non-dict for %s — stopping", pg, dealer_name)
            break
        batch = _extract_vehicles_from_srp_body(body, base_url, dealer_id, dealer_name, dealer_url)
        if not batch:
            # Empty DisplayCards on a non-null response — may be transient; keep iterating
            # bounded range rather than stopping early.
            logger.debug("DealerOn: pg=%d empty DisplayCards for %s — skipping", pg, dealer_name)
            continue
        added = sum(1 for v in batch if v.get("vin") and v["vin"] not in seen_vins)
        for v in batch:
            _add(v)
        logger.debug("DealerOn: pg=%d fetched %d vehicles (+%d new) for %s", pg, len(batch), added, dealer_name)

    return vehicles


async def scrape_dealer_on_from_page(
    page: Any,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    """
    Extract DealerOn inventory from a Playwright page. Returns list of vehicle dicts
    (empty when DealerOn is not detected or no inventory found).
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.debug("DealerOn: could not get page content: %s", e)
        return []

    if not _is_dealer_on_html(html):
        return []

    logger.info("DealerOn: detected for %s — scraping SRP paths", dealer_name)

    all_vehicles: list[dict[str, Any]] = []
    seen_vins: set[str] = set()

    for path in _SRP_PATHS:
        srp_url = base_url.rstrip("/") + path
        batch = await _scrape_srp_all_pages(
            page, srp_url, base_url, dealer_id, dealer_name, dealer_url, seen_vins
        )
        all_vehicles.extend(batch)

    logger.info("DealerOn: scraped %d vehicles for %s", len(all_vehicles), dealer_name)
    return all_vehicles
