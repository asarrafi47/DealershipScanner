"""
DealerOn (vhcliaa) inventory scraper.

DealerOn SRP pages call /api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{dealer_id}/{search_id}
returning a JSON object with a DisplayCards array. Each DisplayCard contains a VehicleCard dict
with rich vehicle data (VIN, price, colors, photos, etc.).

This module is the HTTP parsing half: map VehicleCard dicts to the standard scanner vehicle
schema. The Playwright page scraper and SRP pager were retired with the browser path (P17B.5);
HTTP harvesting of the same API lives in synth/platforms/dealeron_cosmos.py.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


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


def _parse_price(*candidates: Any) -> int | None:
    for pv in candidates:
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


# Real per-car vehicle-history REPORT links embedded in the cosmos card HTML
# anchors (VehicleCarHistoryReport / *MxSrp / *MxVdp). Matches the Carfax report
# URL and the AutoCheck equivalent — never the static badge SVGs.
_HISTORY_HREF_RE = re.compile(
    r"https?://(?:www\.)?(?:carfax\.com/vehiclehistory|autocheck\.com/vehiclehistory)[^\s\"'<>]+",
    re.IGNORECASE,
)


def _extract_cosmos_carfax(vc: dict) -> str | None:
    """Browser-free Carfax/AutoCheck report URL from the cosmos card HTML anchors.

    The cosmos feed embeds the real per-car report link inside the
    ``VehicleCarHistoryReport`` anchor markup (plus MxSrp/MxVdp variants). This is
    the same link the VDP would surface, so no browser visit is needed.
    """
    for key in ("VehicleCarHistoryReport", "VehicleCarHistoryReportMxVdp", "VehicleCarHistoryReportMxSrp"):
        html = vc.get(key)
        if not isinstance(html, str) or not html:
            continue
        m = _HISTORY_HREF_RE.search(html)
        if m:
            return m.group(0).replace("&amp;", "&")
    return None


def _extract_cosmos_packages(vc: dict) -> dict | None:
    """Factory features + highlights blob from the cosmos card → cars.packages.

    The cosmos feed carries a flat ``Features`` list and a
    ``VehicleHighlightsModel.Highlights`` list (factory/equipment highlights) —
    both browser-free. Mirrors the CarsCommerce ``packages`` JSON convention.
    """
    blob: dict = {}

    feats = vc.get("Features")
    if isinstance(feats, list):
        cleaned = [s for s in (str(x).strip() for x in feats if isinstance(x, str)) if s]
        if cleaned:
            blob["features"] = cleaned[:120]

    hi_model = vc.get("VehicleHighlightsModel")
    if isinstance(hi_model, dict):
        highs = hi_model.get("Highlights")
        if isinstance(highs, list):
            cleaned = [s for s in (str(x).strip() for x in highs if isinstance(x, str)) if s]
            # Only record highlights that add beyond the flat feature list.
            extra = [s for s in cleaned if s not in set(blob.get("features") or [])]
            if extra:
                blob["highlights"] = extra[:120]

    return blob or None


def _extract_cosmos_description(vc: dict) -> str | None:
    """Dealer comments / listing copy from the cosmos card (plain text, no VDP)."""
    for key in ("VehicleComments",):
        s = vc.get(key)
        if isinstance(s, str):
            s = s.strip()
            if len(s) >= 40:
                return s[:4000]
    return None


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


def _cosmos_rooftop(vc: dict, dealer_name: str, own_site_id: str = "") -> dict[str, str] | None:
    """The lot this card sits at, verbatim from the feed (N1.2 cosmos map).

    ``DealerLocatedAtId`` / ``VehicleLocation`` / ``DealerLocatedAtCity/State``
    are on every card (17,894 of 17,894) and name the lot holding the car, which
    on a group site is often a sister store (cleveland-nissan's feed: 956 cars
    over 4 rooftops). The site's own lot is the card whose lot id equals the
    ``DealerId`` that served it; its rooftop is named by the roster ``dealer_name``
    so the gate picks it as the target, and siblings keep the lot's own label.

    Without an own site id (no ``DealerId`` on the card, no learned hint) the
    target cannot be named, so nothing is stamped: the rows are kept, as before,
    and a log line says why. Stamping sibling labels with no target would make
    the gate refuse the whole feed.
    """
    lot_id = str(vc.get("DealerLocatedAtId") or "").strip()
    label = str(vc.get("VehicleLocation") or "").strip()
    city = str(vc.get("DealerLocatedAtCity") or "").strip()
    state = str(vc.get("DealerLocatedAtState") or "").strip()
    if not (lot_id or label or city):
        return None
    site_id = str(vc.get("DealerId") or "").strip() or str(own_site_id or "").strip()
    if not site_id:
        _log_unknown_site_id(dealer_name)
        return None
    rooftop: dict[str, str] = {
        "key": lot_id,
        "name": label,
        "lot_id": lot_id,
        "city": city,
        "state": state,
    }
    if lot_id and lot_id == site_id:
        rooftop["own_site"] = "1"
        if dealer_name:
            rooftop["name"] = dealer_name
            if label and label != dealer_name:
                rooftop["alt_name"] = label
    return rooftop


_warned_unknown_site: set[str] = set()


def _log_unknown_site_id(dealer_name: str) -> None:
    if dealer_name not in _warned_unknown_site:
        _warned_unknown_site.add(dealer_name)
        logger.info("DealerOn cosmos: %s - no DealerId on the card and no own-site hint; rooftop not stamped (rows kept)", dealer_name)


def _map_vehicle_card(
    vc: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
    own_site_id: str = "",
) -> dict[str, Any] | None:
    vin = str(vc.get("VehicleVin") or "").strip().upper()
    if not vin:
        return None

    # D-N18: the advertised price only. A card with no internet price is unpriced
    # (the VDP prefetch fills it); VehicleMsrp goes to ``msrp`` and never to price.
    price = _parse_price(vc.get("VehicleInternetPrice"))
    mileage = _parse_mileage(str(vc.get("Mileage") or ""))
    condition = _parse_condition(vc)
    gallery = _build_gallery(vc, base_url, vin)
    detail_url = str(vc.get("VehicleDetailUrl") or "").strip() or None
    carfax_url = _extract_cosmos_carfax(vc)
    packages = _extract_cosmos_packages(vc)
    description = _extract_cosmos_description(vc)

    row: dict[str, Any] = {
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
    rooftop = _cosmos_rooftop(vc, dealer_name, own_site_id)
    if rooftop:
        row["_rooftop"] = rooftop
    if carfax_url:
        row["carfax_url"] = carfax_url
    if packages:
        row["packages"] = packages
    if description:
        row["description"] = description
    msrp = _parse_price(vc.get("VehicleMsrp"))
    if msrp:
        row["msrp"] = msrp  # every new-car card carries it (Cherokee County 96/96); never used as the price (D-N18)
    return row


def _extract_vehicles_from_srp_body(
    body: dict,
    base_url: str,
    dealer_id: str,
    dealer_name: str,
    dealer_url: str,
) -> list[dict[str, Any]]:
    vehicles = []
    own_site_id: str | None = None  # learned hint, read once and only if a card lacks DealerId
    for card in body.get("DisplayCards") or []:
        vc = card.get("VehicleCard")
        if not isinstance(vc, dict):
            continue
        if own_site_id is None and not vc.get("DealerId"):
            from backend.attribution.place import own_site_id as _hint

            own_site_id = _hint(dealer_id)
        v = _map_vehicle_card(vc, base_url, dealer_id, dealer_name, dealer_url, own_site_id or "")
        if v:
            vehicles.append(v)
    return vehicles
