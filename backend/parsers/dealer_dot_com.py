"""
Parser for dealer.com getInventory API. Maps exact schema:
  title (array -> join), trackingPricing.internetPrice, trackingAttributes (odometer, exteriorColor),
  images[].uri -> gallery, vin, stockNumber, fuelType.
"""
import hashlib
import json
import logging
import re
from typing import Any
from urllib.parse import urljoin

from backend.parsers.vdp_urls import dealer_style_vdp_url_candidates, suggest_dealer_style_vdp_url
from backend.parsers.inventory_carfax import extract_carfax_url
from backend.utils.listing_description_extract import strip_dealer_description_intro
from backend.parsers.base import (
    clean_image_url,
    dedupe_urls_order_prefer_large,
    find_tracking_attr,
    find_vehicle_list,
    harvest_image_urls_from_json,
    inventory_gallery_max,
    norm_float,
    norm_int,
    norm_label_str,
    norm_str,
    normalize_image_url_https,
)
from backend.utils.field_clean import normalize_optional_str

logger = logging.getLogger(__name__)

# Fallback when no images; frontend resolves /static/ relative to app
FALLBACK_IMAGE_URL = "/static/placeholder.svg"


def _opt_str(v) -> str | None:
    """Missing / placeholder → None (never persist 'N/A' to SQLite)."""
    if v is None:
        return None
    s = norm_str(v)
    return normalize_optional_str(s)


def _opt_label_str(v) -> str | None:
    """Missing / placeholder → None; unwraps dict-shaped label fields via norm_label_str.
    "Other" (dealer.com's no-value drivetrain / transmission) is a placeholder,
    not a label: stored verbatim it overwrote vPIC-healed drivetrains (F10)."""
    if v is None:
        return None
    from backend.utils.field_clean import is_spec_placeholder

    label = norm_label_str(v)
    if is_spec_placeholder(label):
        return None
    return normalize_optional_str(label)


def _extract_title(obj: dict, year: int, make: str, model: str) -> str | None:
    """Dealer.com: title is an array. Join with ' '. Fallback: year make model."""
    raw = obj.get("title") or obj.get("name")
    if raw is None:
        return normalize_optional_str(f"{year or ''} {make or ''} {model or ''}".strip())
    if isinstance(raw, list):
        s = " ".join(str(x).strip() for x in raw if x is not None).strip()
        return normalize_optional_str(s) or normalize_optional_str(
            f"{year or ''} {make or ''} {model or ''}".strip()
        )
    s = norm_str(raw)
    return normalize_optional_str(s) or normalize_optional_str(
        f"{year or ''} {make or ''} {model or ''}".strip()
    )


# dealer.com masks some used-car prices with a tiny per-account placeholder
# instead of the real internetPrice (e.g. every hidden-price row on one
# account reads 490, on another 225, on another 85 — the SAME number repeated
# across dozens of unrelated makes/models, which no real per-vehicle price
# ever does). Confirmed 2026-08-05 across 21 dealer.com accounts: 212 active
# rows this floor-guards, ranging $20-$490. carscommerce.py hit the identical
# phenomenon on the sibling CarsCommerce search API and already floors it at
# 1000; this parser (dealer.com's own getInventory API) never got the same
# guard, so a masked placeholder sailed through as if it were the price.
_MASKED_PRICE_FLOOR = 1000


# No retail vehicle this parser sees lists above this; a larger "msrp" is a
# stock-number / uuid fragment or a summed figure and must not become MSRP.
_MSRP_CEILING = 400_000


def _first_price(*vals, floor: float = 0.0, ceiling: float = float("inf")) -> float:
    for v in vals:
        if v is None or v is False:
            continue
        if isinstance(v, str) and "contact" in v.lower():
            continue
        n = norm_float(v)
        if n > floor and n <= ceiling:
            return n
    return 0.0


def _extract_price_dealer_com(obj: dict) -> int:
    """
    Priority: trackingPricing.internetPrice → pricing.internetPrice → pricing.finalPrice
    → pricing.salePrice → pricing.msrp → price → trackingAttributes price/msrp.

    Candidates at or below ``_MASKED_PRICE_FLOOR`` are skipped, not accepted —
    a masked placeholder is indistinguishable from a real price by shape alone,
    so treating it as absent (and letting the VDP scan / gap-fill path supply
    the real number later) is the only safe read here.
    """
    tracking = obj.get("trackingPricing") or obj.get("tracking_pricing")
    pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None

    raw = _first_price(
        isinstance(tracking, dict) and tracking.get("internetPrice"),
        isinstance(tracking, dict) and tracking.get("internet_price"),
        pricing and pricing.get("internetPrice"),
        pricing and pricing.get("internet_price"),
        pricing and pricing.get("finalPrice"),
        pricing and pricing.get("final_price"),
        pricing and pricing.get("salePrice"),
        pricing and pricing.get("sale_price"),
        obj.get("sellingPrice"),
        obj.get("internet_Price"),
        obj.get("internet_price"),
        pricing and pricing.get("msrp"),
        pricing and pricing.get("MSRP"),
        obj.get("price"),
        obj.get("internetPrice"),
        pricing and pricing.get("retailPrice"),
        pricing and pricing.get("retail_price"),
        floor=_MASKED_PRICE_FLOOR,
    )
    if raw == 0:
        arr = obj.get("trackingAttributes") or obj.get("tracking_attributes") or obj.get("attributes")
        if isinstance(arr, list):
            v2 = find_tracking_attr(arr, "price", "value") or find_tracking_attr(arr, "msrp", "value")
            if v2 is not None and str(v2).strip() and norm_float(v2) > _MASKED_PRICE_FLOOR:
                raw = norm_float(v2)
    return int(round(raw))


def _is_new_or_certified(obj: dict) -> bool:
    if obj.get("certified") is True:
        return True
    for key in ("condition", "type", "status"):
        v = str(obj.get(key) or "").strip().lower()
        if v in ("new", "certified", "cpo", "certified pre-owned"):
            return True
    return False


def _typed_price_entries(pricing: dict | None, type_class: str) -> list:
    """Values of ``pricing.dprice[]`` then ``pricing.eprice[]`` entries whose
    ``typeClass`` is ``type_class`` (label is free text: "Total SRP", "MSRP",
    "Retail Price"...). askingPrice / internetPrice / isFinalPrice rows are
    never MSRP and are simply not matched."""
    out = []
    if not isinstance(pricing, dict):
        return out
    for key in ("dprice", "eprice"):
        arr = pricing.get(key)
        if not isinstance(arr, list):
            continue
        for entry in arr:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("typeClass") or "").strip().lower() == type_class:
                out.append(entry.get("value"))
    return out


def _extract_msrp_dealer_com(obj: dict) -> int:
    """MSRP from a getInventory item.

    Live ws-inv-data feeds (2026-09-28, 96/96 items at four stores) carry it in
    ``trackingPricing.msrp`` ("$64,783"), ``pricing.dprice[].typeClass ==
    "msrp"`` and ``pricing.retailPrice``, never in ``pricing.msrp``; reading
    only the latter left 100% of New rows at 140 stores msrp-null. Order:
    pricing.msrp / MSRP / retailMsrp, trackingPricing.msrp, typed dprice /
    eprice entries, pricing.retailPrice (New / Certified only: on used rows it
    is the asking price), obj.msrp, then the trackingAttributes msrp. Masked
    placeholders (<= ``_MASKED_PRICE_FLOOR``) and values above
    ``_MSRP_CEILING`` are skipped.
    """
    pricing = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
    tracking = obj.get("trackingPricing") or obj.get("tracking_pricing")
    tracking = tracking if isinstance(tracking, dict) else None
    candidates = [
        pricing and pricing.get("msrp"),
        pricing and pricing.get("MSRP"),
        pricing and pricing.get("retailMsrp"),
        tracking and tracking.get("msrp"),
        tracking and tracking.get("MSRP"),
        *_typed_price_entries(pricing, "msrp"),
    ]
    if _is_new_or_certified(obj):
        candidates.append(pricing and pricing.get("retailPrice"))
        candidates.append(pricing and pricing.get("retail_price"))
    candidates.append(obj.get("msrp"))
    raw = _first_price(*candidates, floor=_MASKED_PRICE_FLOOR, ceiling=_MSRP_CEILING)
    if raw == 0:
        for arr in (obj.get("trackingAttributes"), obj.get("tracking_attributes"), obj.get("attributes")):
            if not isinstance(arr, list):
                continue
            v2 = find_tracking_attr(arr, "msrp", "value")
            if v2 is not None and str(v2).strip():
                raw = _first_price(v2, floor=_MASKED_PRICE_FLOOR, ceiling=_MSRP_CEILING)
                if raw:
                    break
    return int(round(raw))


def _extract_mileage_dealer_com(obj: dict) -> int | None:
    """trackingAttributes odometer, then obj.odometer / mileage, then the
    trackingAttributes mileage. None when the item carries no odometer: a
    default 0 on used rows read as a valid "0 mi" (crownlexus 334 rows where
    the VDP shows 10,914; F12 2026-09-28)."""
    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
    v = find_tracking_attr(arr, "odometer", "value")
    if v is not None and str(v).strip() != "":
        return norm_int(v)
    v = obj.get("odometer")
    if v is None or str(v).strip() == "":
        v = obj.get("mileage")
    if (v is None or str(v).strip() == "") and isinstance(arr, list):
        v = find_tracking_attr(arr, "mileage", "value")
    if v is None or str(v).strip() == "":
        return None
    return norm_int(v)


def _extract_featured_or_thumbnail(obj: dict, base_url: str) -> str:
    """If vehicle.images is empty, use featuredImage or thumbnail as backup. Returns URL or ''."""
    for key in ("featuredImage", "featured_image", "thumbnail", "Thumbnail", "primaryImage", "primary_image"):
        val = obj.get(key)
        if val is None:
            continue
        if isinstance(val, str) and val.strip():
            return clean_image_url(val.strip(), base_url)
        if isinstance(val, dict):
            u = val.get("uri") or val.get("url") or val.get("URL")
            if u and isinstance(u, str) and u.strip():
                return clean_image_url(u.strip(), base_url)
    return ""


def _best_image_url(item: dict, base_url: str) -> str:
    """Prefer largest / full-res Dealer.com image fields, then fall back to thumbnail."""
    if not isinstance(item, dict):
        return ""
    for key in (
        "xxlargeUri",
        "xlargeUri",
        "largeUri",
        "fullUri",
        "hiResUri",
        "uri",
        "url",
        "URL",
        "imageUrl",
        "thumbnailUri",
        "thumbUrl",
    ):
        u = item.get(key)
        if u and isinstance(u, str) and u.strip():
            return clean_image_url(u.strip(), base_url)
    return ""


def _extract_gallery(obj: dict, base_url: str) -> list[str]:
    """Map vehicle.images to list of URLs (prefer full-res keys), then deep-harvest nested JSON."""
    mx = inventory_gallery_max()
    images = obj.get("images") or obj.get("Images")
    out: list[str] = []
    if isinstance(images, list) and len(images) > 0:
        seen: set[str] = set()
        for item in images:
            u = _best_image_url(item, base_url) if isinstance(item, dict) else ""
            nu = normalize_image_url_https(u) if u else ""
            if nu.startswith("https://") and nu not in seen:
                seen.add(nu)
                out.append(nu)
        if out:
            base = dedupe_urls_order_prefer_large(out, max_len=mx)
            extra = harvest_image_urls_from_json(obj, base_url, max_urls=mx)
            return dedupe_urls_order_prefer_large(base + extra, max_len=mx)
    one = _extract_featured_or_thumbnail(obj, base_url)
    base = []
    if one:
        no = normalize_image_url_https(one)
        if no.startswith("https://"):
            base = [no]
    extra = harvest_image_urls_from_json(obj, base_url, max_urls=mx)
    return dedupe_urls_order_prefer_large(base + extra, max_len=mx)


def _extract_history_highlights(obj: dict) -> list[str]:
    """Extract history badges from callout, badges, highlightedAttributes (e.g. 'No Accidents Reported', '1-Owner')."""
    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        s = norm_str(s)
        if s and s.lower() not in seen:
            seen.add(s.lower())
            out.append(s)

    # callout: often array of badge strings or objects with text/label
    for key in ("callout", "callouts", "badges", "Badges", "historyBadges", "history_badges"):
        val = obj.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    add(item)
                elif isinstance(item, dict):
                    add(item.get("text") or item.get("label") or item.get("name") or item.get("value") or "")
        elif isinstance(val, str):
            add(val)

    # highlightedAttributes: sometimes Condition/History summary
    ha = obj.get("highlightedAttributes") or obj.get("highlighted_attributes")
    if isinstance(ha, list):
        for item in ha:
            if isinstance(item, dict):
                name = item.get("name") or item.get("key") or item.get("label")
                value = item.get("value") or item.get("text")
                if name and value:
                    add(f"{name}: {value}")
                elif value:
                    add(str(value))
                elif name:
                    add(str(name))
            elif isinstance(item, str):
                add(item)
    return out


def _extract_inventory_description(obj: dict) -> str | None:
    """Long-form listing copy when present in getInventory payloads (no VDP required)."""
    for key in (
        "extendedDescription",
        "description",
        "sellerNotes",
        "seller_notes",
        "dealerComments",
        "dealer_comments",
        "comments",
        "marketingDescription",
        "vehicleDescription",
        "dealerDescription",
        "listingDescription",
    ):
        s = _opt_str(obj.get(key))
        if s and len(s) >= 40:
            cleaned = strip_dealer_description_intro(s) or s
            return cleaned[:4000]
    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
    if isinstance(arr, list):
        for attr_name in (
            "Comments",
            "Description",
            "Dealer Comments",
            "Seller Notes",
            "dealerComments",
            "extendedDescription",
        ):
            v = find_tracking_attr(arr, attr_name, "value")
            if v is not None:
                s = _opt_str(norm_str(v))
                if s and len(s) >= 40:
                    cleaned = strip_dealer_description_intro(s) or s
                    return cleaned[:4000]
    return None


def _pick_vehicle_detail_url(obj: dict, base_url: str) -> str | None:
    """Absolute VDP URL when present in listing payload (used by scanner VDP enrichment)."""
    candidates = [
        obj.get("vdpUrl"),
        obj.get("vdp_url"),
        obj.get("vehicleUrl"),
        obj.get("vehicle_url"),
        obj.get("vehicleLink"),
        obj.get("vehicle_link"),
        obj.get("detailUrl"),
        obj.get("detail_url"),
        obj.get("detailPageUrl"),
        obj.get("detail_page_url"),
        obj.get("vehicleDetailsUrl"),
        obj.get("vehicle_details_url"),
        obj.get("inventoryUrl"),
        obj.get("inventory_url"),
        obj.get("webUrl"),
        obj.get("web_url"),
        obj.get("url"),
        obj.get("href"),
        obj.get("link"),
        obj.get("seoUri"),
        obj.get("seo_uri"),
    ]
    for raw in candidates:
        if not raw or not isinstance(raw, str):
            continue
        u = raw.strip()
        if u.startswith("//"):
            u = "https:" + u
        elif not u.lower().startswith("http"):
            try:
                u = urljoin(base_url.rstrip("/") + "/", u.lstrip("/"))
            except Exception:
                continue
        low = u.lower()
        if (
            "/vdp/" in low
            or "vehicle-inventory" in low
            or "/inventory/" in low
            or "/used/" in low
            or "/new/" in low
            or "certified" in low
        ):
            return u
    return None


def _extract_exterior_color(obj: dict) -> str | None:
    """Find in vehicle.trackingAttributes where name === 'exteriorColor'."""
    arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
    v = find_tracking_attr(arr, "exteriorColor", "value")
    if v is not None and str(v).strip():
        return _opt_str(norm_str(v))
    return _opt_str(obj.get("exteriorColor") or obj.get("exterior_color"))


def _norm_tracking_key(name: object) -> str:
    if name is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _tracking_attr_value_by_norm_substr(arr: object, needles: frozenset[str]) -> str | None:
    """First trackingAttributes entry whose normalized name matches a *needles* substring."""
    if not isinstance(arr, list):
        return None
    for item in arr:
        if not isinstance(item, dict):
            continue
        nk = _norm_tracking_key(item.get("name") or item.get("key") or item.get("id"))
        if not nk:
            continue
        for nd in needles:
            if len(nd) < 5:
                continue
            if nd in nk or nk in nd:
                v = item.get("value") or item.get("text")
                if v is not None and str(v).strip():
                    got = _opt_str(norm_str(v))
                    if got:
                        return got
    return None


_INTERIOR_ATTR_SUBSTR = frozenset(
    {
        "interiorcolor",
        "interiortrim",
        "upholstery",
        "seatcolor",
        "seattrim",
        "cabincolor",
        "leathercolor",
        "intcolor",
        "interiorpackage",
        "cabintrim",
    }
)

_BODY_ATTR_SUBSTR = frozenset(
    {
        "bodystyle",
        "bodytype",
        "vehiclebody",
        "vehicletype",
        "vehbodystyle",
        "bodyshape",
    }
)


def _extract_interior_color(obj: dict) -> str | None:
    """Prefer trackingAttributes (Dealer.com varies label casing) then top-level fields."""
    v = _tracking_attr_value_by_norm_substr(
        obj.get("trackingAttributes") or obj.get("tracking_attributes"),
        _INTERIOR_ATTR_SUBSTR,
    )
    if v:
        return v
    for nm in ("interiorColor", "interior_color", "interiorTrim", "interior_trim"):
        got = _opt_str(obj.get(nm))
        if got:
            return got
    return None


_ENGINE_SIZE_RE = re.compile(r"(\d{1,2}(?:\.\d)?)\s*L\b", re.I)


def _attr_text(obj: dict, *names: str) -> str | None:
    """First non-placeholder value of ``names`` across ``attributes`` then
    ``trackingAttributes`` ("" and "null" are absent)."""
    for arr_key in ("attributes", "trackingAttributes", "tracking_attributes"):
        arr = obj.get(arr_key)
        if not isinstance(arr, list):
            continue
        for name in names:
            v = find_tracking_attr(arr, name, "value")
            if v is None:
                continue
            t = str(v).strip()
            if t and t.lower() not in ("null", "none", "n/a"):
                return t
    return None


def _extract_engine_dealer_com(obj: dict) -> tuple[str | None, float | None]:
    """(engine_description, engine_l) from a getInventory item.

    The engine text lives in ``attributes`` / ``trackingAttributes`` as
    ``engine`` ("2.5L 4-Cyl. Hybrid Engine") with a sibling ``engineSize``
    ("2.5 L"); 96/96 items at four stores on 2026-09-28, never read before
    (F05). When the text does not name the displacement the size is prefixed;
    ``engineSize`` alone yields "2.4L" so the column is not empty.
    """
    eng = _opt_str(obj.get("engine") or obj.get("engineDescription") or obj.get("engine_description"))
    if not eng:
        eng = _attr_text(obj, "engine", "engineDescription", "engine_description")
    size = _opt_str(obj.get("engineSize") or obj.get("engine_size")) or _attr_text(obj, "engineSize", "engine_size")
    engine_l: float | None = None
    size_label: str | None = None
    if size:
        m = _ENGINE_SIZE_RE.search(size)
        if m:
            try:
                engine_l = float(m.group(1))
                size_label = f"{m.group(1)}L"
            except ValueError:
                engine_l = None
    if eng and engine_l is None:
        m = _ENGINE_SIZE_RE.search(eng)
        if m:
            try:
                engine_l = float(m.group(1))
            except ValueError:
                engine_l = None
    if eng and size_label and not _ENGINE_SIZE_RE.search(eng):
        eng = f"{size_label} {eng}"
    if not eng and size_label:
        eng = size_label
    return (eng[:200] if eng else None), engine_l


def _extract_body_style(obj: dict) -> str | None:
    """bodyStyle / bodyType on object or in trackingAttributes."""
    direct = _opt_str(
        obj.get("bodyStyle")
        or obj.get("body_style")
        or obj.get("bodyType")
        or obj.get("body_type")
    )
    if direct:
        return direct
    return _tracking_attr_value_by_norm_substr(
        obj.get("trackingAttributes") or obj.get("tracking_attributes"),
        _BODY_ATTR_SUBSTR,
    )


def _accounts_index(data: Any) -> dict[str, dict]:
    """The payload's ``accounts`` map: DDC rooftop id → that rooftop's identity.

    A ``ws-inv-data`` body pairs every vehicle's ``accountId`` with an entry in a
    sibling ``accounts`` object, e.g. (crownlexus.com, 2026-08-03)::

        "accounts": {"soniccrownlexus": {
            "name": "Crown Lexus", "url": "www.crownlexus.com", "phone": "…",
            "address": {"accountName": "Crown Lexus", "city": "Ontario",
                        "firstLineAddress": "1125 South Kettering Drive",
                        "postalCode": "91761", "state": "CA", "country": "US"}}}

    The map is PAGE-scoped — it holds only the rooftops whose cars are on that
    page — so it is read per body, not once per dealer.
    """
    index: dict[str, dict] = {}

    def visit(node: Any, depth: int) -> None:
        if depth > 6 or not isinstance(node, dict):
            return
        accounts = node.get("accounts")
        if isinstance(accounts, dict):
            for key, rec in accounts.items():
                if isinstance(key, str) and key and isinstance(rec, dict) and key not in index:
                    index[key] = rec
        for value in node.values():
            if isinstance(value, dict):
                visit(value, depth + 1)

    visit(data if isinstance(data, dict) else {}, 0)
    return index


def _rooftop_from_account(obj: dict, accounts: dict[str, dict]) -> dict | None:
    """This vehicle's storefront, verbatim from the feed — no decision made here.

    ``accountId`` is per-vehicle and names the rooftop that holds the car, which
    is not always the site being scanned: bmwofmonrovia.net's own feed returned
    9 distinct ``accountId`` values over 429 VINs on 2026-08-03, only 220 of them
    ``sonicbmwmonrovia``. Which rooftop is the store being scanned is decided by
    ``backend.parsers.resolve_rooftop_attribution``, never here.
    """
    account_id = norm_str(
        obj.get("accountId") or obj.get("accountID") or obj.get("account_id") or ""
    )
    if not account_id:
        return None
    record = accounts.get(account_id)
    record = record if isinstance(record, dict) else {}
    address = record.get("address")
    address = address if isinstance(address, dict) else {}
    street = " ".join(
        s for s in (
            norm_str(address.get("firstLineAddress") or ""),
            norm_str(address.get("secondLineAddress") or ""),
        ) if s
    ).strip()
    return {
        "key": account_id,
        "name": norm_str(record.get("name") or address.get("accountName") or ""),
        "site": norm_str(record.get("url") or ""),
        "address": street,
        "city": norm_str(address.get("city") or ""),
        "state": norm_str(address.get("state") or ""),
        "zip": norm_str(address.get("postalCode") or ""),
    }


def _map_vehicle(
    obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str,
    accounts: dict[str, dict] | None = None,
) -> dict | None:
    """Map one vehicle from Dealer.com getInventory schema. Missing text → None (not 'N/A')."""
    if not isinstance(obj, dict):
        return None

    vin = norm_str(obj.get("vin") or obj.get("VIN") or "")
    if not vin:
        vin = norm_str(obj.get("stockNumber") or "")
    if not vin:
        # Stable across process restarts (unlike Python's salted str hash()):
        # sha1 over the vehicle's own JSON so the identical source record
        # always yields the same placeholder vin, letting upsert_vehicles
        # match it to its previous row on the next scan.
        try:
            digest_src = json.dumps(obj, sort_keys=True, default=str)
        except (TypeError, ValueError):
            digest_src = str(obj)
        vin = f"unknown-{hashlib.sha1(digest_src.encode('utf-8')).hexdigest()[:8]}"

    stock_raw = _opt_str(obj.get("stockNumber"))
    stock_number = stock_raw or ""

    year = norm_int(obj.get("year") or obj.get("modelYear") or obj.get("model_year"))
    make = norm_str(obj.get("make") or obj.get("Make") or "")
    model = norm_str(obj.get("model") or obj.get("Model") or obj.get("modelName") or "")

    title = _extract_title(obj, year, make, model)
    price = _extract_price_dealer_com(obj)
    msrp = _extract_msrp_dealer_com(obj)
    mileage = _extract_mileage_dealer_com(obj)
    gallery = _extract_gallery(obj, base_url)
    image_url = gallery[0] if gallery else ""
    # Fallback image when gallery and image_url are empty
    if not image_url or not gallery:
        image_url = image_url or FALLBACK_IMAGE_URL
        gallery = gallery if gallery else [FALLBACK_IMAGE_URL]
    exterior_color = _extract_exterior_color(obj)
    fuel_type = _opt_str(obj.get("fuelType") or obj.get("fuel_type"))
    description = _extract_inventory_description(obj)
    carfax_url = extract_carfax_url(obj, vin)

    engine_description, engine_l = _extract_engine_dealer_com(obj)

    cyl = norm_int(obj.get("cylinders") or 0)
    if not cyl:
        arr = obj.get("trackingAttributes") or obj.get("tracking_attributes")
        c2 = find_tracking_attr(arr, "cylinders", "value") if isinstance(arr, list) else None
        if c2 is not None:
            cyl = norm_int(c2)

    detail_url = _pick_vehicle_detail_url(obj, base_url)
    if not detail_url and vin and not str(vin).lower().startswith("unknown"):
        detail_url = suggest_dealer_style_vdp_url(base_url, vin, obj)
    detail_alternates: list[str] = []
    if vin and not str(vin).lower().startswith("unknown"):
        detail_alternates = [
            x for x in dealer_style_vdp_url_candidates(base_url, vin, obj) if x != detail_url
        ]

    lot_location = ""
    try:
        from backend.scanner.dealer_location import extract_location_from_inventory_object

        lot_location = extract_location_from_inventory_object(obj)
    except ImportError:
        pass

    condition: str | None = None
    try:
        from backend.parsers.inventory_condition import normalize_inventory_condition

        condition = normalize_inventory_condition(obj, mileage=mileage)
    except ImportError:
        pass

    out = {
        "vin": vin,
        "stock_number": stock_number,
        "year": year,
        "make": make,
        "model": model,
        "trim": _opt_str(obj.get("trim") or obj.get("Trim") or obj.get("trimName")),
        "title": title,
        "price": price,
        "msrp": msrp,
        "mileage": mileage,
        "image_url": image_url,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "zip_code": _opt_str(obj.get("zipCode") or obj.get("zip_code")),
        "fuel_type": fuel_type,
        "transmission": _opt_label_str(obj.get("transmission") or obj.get("transmissionType")),
        "drivetrain": _opt_label_str(obj.get("drivetrain") or obj.get("driveType")),
        "exterior_color": exterior_color,
        "interior_color": _extract_interior_color(obj),
        "body_style": _extract_body_style(obj),
        "carfax_url": carfax_url if (carfax_url and str(carfax_url).strip().lower().startswith("http")) else None,
        "history_highlights": _extract_history_highlights(obj),
        "description": description,
        "cylinders": cyl or None,
    }
    if engine_description:
        out["engine_description"] = engine_description
    if engine_l:
        out["engine_l"] = engine_l
    try:
        from backend.parsers.inventory_mpg import apply_inventory_mpg

        apply_inventory_mpg(obj, out)
    except ImportError:
        pass

    if detail_url:
        out["_detail_url"] = detail_url
        out["source_url"] = detail_url
    if detail_alternates:
        out["_detail_url_alternates"] = detail_alternates[:12]
    if lot_location:
        out["_lot_location"] = lot_location
    # Same storefront evidence, in the shape the shared rooftop gate reads.
    # ``_lot_location`` stays: post-scan code reads it, and it is a merged
    # best-effort blob (it sweeps every key matching dealer|location|lot|store|
    # account|selling|physical|located, which on some accounts picks up vehicle
    # titles — coastlinecdjr-com has 117 distinct values, "2026 RAM 1500 RHO 1"
    # among them). Attribution uses ``accountId`` + the ``accounts`` map only.
    rooftop = _rooftop_from_account(obj, accounts or {})
    if rooftop:
        out["_rooftop"] = rooftop
    if condition:
        out["condition"] = condition
        if condition == "Certified":
            out["is_cpo"] = 1
        elif condition == "Used":
            out["is_cpo"] = 0
    try:
        from backend.utils.in_transit import apply_in_transit_flags_from_raw

        apply_in_transit_flags_from_raw(obj, source="dealer_dot_com")
        for _k in ("_in_transit", "_availability_status", "_availability_source"):
            if _k in obj:
                out[_k] = obj[_k]
    except ImportError:
        pass
    return out


def _has_vehicle_ident(obj: dict) -> bool:
    """True if object looks like a vehicle (vin, VIN, stockNumber, or trackingPricing)."""
    if obj.get("vin") or obj.get("VIN") or obj.get("stockNumber") or obj.get("stock"):
        return True
    tp = obj.get("trackingPricing") if isinstance(obj.get("trackingPricing"), dict) else None
    if tp and (tp.get("internetPrice") or tp.get("retailPrice")):
        return True
    p = obj.get("pricing") if isinstance(obj.get("pricing"), dict) else None
    if p and (p.get("retailPrice") or p.get("internetPrice") or p.get("salePrice")):
        return True
    if norm_float(obj.get("sellingPrice")) > 0 or norm_float(obj.get("internet_Price")) > 0:
        return True
    return False


def _parse_json_list(data, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> list[dict]:
    items = find_vehicle_list(data)
    if not items:
        return []
    accounts = _accounts_index(data)
    out = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        if not _has_vehicle_ident(obj):
            continue
        mapped = _map_vehicle(obj, base_url, dealer_id, dealer_name, dealer_url, accounts)
        if mapped:
            out.append(mapped)
    return out


def _extract_from_html(html: str) -> list | dict | None:
    if not html or "__PRELOADED_STATE__" not in html and "InventoryData" not in html:
        return None
    m = re.search(r"__PRELOADED_STATE__\s*=\s*(\{.*?\});?\s*(?:</script>|$)", html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    m = re.search(r"window\.InventoryData\s*=\s*(\[.*?\]);?\s*(?:</script>|$)", html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


def parse(raw_data, base_url: str, dealer_id: str, dealer_name: str = "", dealer_url: str = ""):
    """
    raw_data: list/dict (getInventory JSON) or HTML string.
    Maps Dealer.com schema: trackingPricing, trackingAttributes, images->gallery, etc.
    """
    if isinstance(raw_data, (list, dict)):
        return _parse_json_list(raw_data, base_url, dealer_id, dealer_name, dealer_url)
    if isinstance(raw_data, str):
        extracted = _extract_from_html(raw_data)
        if extracted is not None:
            return _parse_json_list(extracted, base_url, dealer_id, dealer_name, dealer_url)
    return []
