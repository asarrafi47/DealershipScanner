"""
Self-contained parser for the CarsCommerce search feed
(``websites-search.api.carscommerce.inc/.../listings/<ccid>/search``).

Many dealer groups sit on ONE CarsCommerce account. Their captured recipes are
provider-hinted ``dealer_dot_com`` / ``dealer_inspire``, so historically the
generic parser read the payload and produced near-empty rows: colors ~7%, and
packages / carfax / warranty / features dropped entirely — even though the feed
RETURNS all of them. This parser maps each listing to the standard row dict and
keeps the RICH fields the feed carries.

Response shapes (both handled):
  - ``{"data": {"ccid", "listings": [...]}, "meta": ...}``  (perPage >= 100)
  - ``{"data": [ ... ]}``                                    (small perPage)

Each listing carries:
  vin, year, make, model, trim, mileage, type,
  styles{exterior_color[_generic], interior_color[_generic], style_name},
  mechanical{engine, drivetrain, fuel_type, city_mpg, highway_mpg,
             engine_cylinders, displacement, transmission, ...},
  media{images:[url,...]}, features:[str,...], packages:[...],
  warranty:[{name,value},...], history_report{carfax_url},
  window_sticker_url, vdp_url, pricing{...}, dealer{...}.

Color/image/engine/mileage mapping is REUSED from
``backend.scanner.carscommerce_harvest._map_listing`` (the browser-free bulk
harvester) rather than re-implemented here; this module extends that base row
with the rich fields and the standard downstream keys (see
``backend/parsers/dealer_on.py`` ``_map_vehicle``).

Price policy mirrors the harvester: the search feed masks some used-car prices
with tiny placeholders (e.g. 85 / 122), so we accept a price only when a
candidate field clears ``_MASKED_PRICE_FLOOR``. A fully-masked account yields no
price and the delta gate correctly defers to the primary VDP scan.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.scanner.carscommerce_harvest import _map_listing as _harvest_map, _s, _style
from backend.utils.field_clean import clean_car_row_dict

logger = logging.getLogger(__name__)

# Search-feed used-car price masking uses tiny placeholders; a real listing
# price clears this floor. Below it, treat price as absent (VDP scan fills it).
_MASKED_PRICE_FLOOR = 1000


def _listings(raw_data: Any) -> list[dict]:
    if not isinstance(raw_data, dict):
        return []
    data = raw_data.get("data")
    if isinstance(data, dict) and isinstance(data.get("listings"), list):
        return [x for x in data["listings"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def detect(raw_data: Any) -> bool:
    """True when the payload is a CarsCommerce search response.

    Conservative on purpose: it must be reachable ahead of the declared
    ``dealer_dot_com`` parser without hijacking other JSON shapes. A CarsCommerce
    listing pairs a ``vin`` with at least one of its signature nested blocks
    (``styles`` / ``mechanical`` / ``history_report`` / ``media.images``) — a
    combination no other provider payload in this repo produces.
    """
    for listing in _listings(raw_data)[:5]:
        if not listing.get("vin"):
            continue
        styles = listing.get("styles")
        mech = listing.get("mechanical")
        hist = listing.get("history_report")
        media = listing.get("media")
        if (
            (isinstance(styles, (dict, list)) and styles)
            or (isinstance(mech, dict) and (mech.get("engine") or mech.get("drivetrain")))
            or (isinstance(hist, dict) and hist.get("carfax_url"))
            or (isinstance(media, dict) and isinstance(media.get("images"), list))
        ):
            return True
    return False


def _int(v: Any) -> int | None:
    try:
        n = int(float(str(v).replace(",", "").strip()))
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().lower().replace("l", "").replace(",", "")
    try:
        f = float(s)
        return round(f, 2) if f > 0 else None
    except (TypeError, ValueError):
        return None


def _price(listing: dict) -> int | None:
    pricing = listing.get("pricing")
    if not isinstance(pricing, dict):
        return None
    # Preference order: dealer selling prices first, MSRP as last resort.
    for key in ("our_price", "internet_price", "sale_price", "price", "msrp"):
        n = _int(pricing.get(key))
        if n is not None and n >= _MASKED_PRICE_FLOOR:
            return n
    return None


def _images(listing: dict) -> list[str]:
    media = listing.get("media")
    if not isinstance(media, dict):
        return []
    raw = media.get("images")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for img in raw:
        # images are URL strings; tolerate {"url": ...} objects defensively.
        url = img if isinstance(img, str) else (img.get("url") if isinstance(img, dict) else None)
        u = _s(url)
        if u and u.startswith("http"):
            out.append(u)
    return out


def _condition(listing: dict) -> str | None:
    t = _s(listing.get("type"))
    if t and "certif" in t.lower():
        return "Certified"
    return t


def _is_cpo(listing: dict) -> int | None:
    t = (_s(listing.get("type")) or "").lower()
    if "certif" in t:
        return 1
    return None


def _detail_url(listing: dict) -> str | None:
    return _s(listing.get("vdp_url"))


def _rich_options(listing: dict) -> dict[str, Any] | None:
    """Structured options/packages blob → cars.packages (JSON).

    Carries factory features, factory packages, and warranty grid — all from the
    feed — plus the window-sticker URL so it persists on the SQLite scanner path
    (which has no dedicated window_sticker_url column).
    """
    blob: dict[str, Any] = {}

    feats = listing.get("features")
    if isinstance(feats, list):
        cleaned = [f for f in (_s(x) for x in feats) if f]
        if cleaned:
            blob["features"] = cleaned

    pkgs = listing.get("packages")
    if isinstance(pkgs, list) and pkgs:
        norm_pkgs: list[Any] = []
        for p in pkgs:
            if isinstance(p, str):
                s = _s(p)
                if s:
                    norm_pkgs.append(s)
            elif isinstance(p, dict):
                name = _s(p.get("name") or p.get("title") or p.get("code"))
                norm_pkgs.append({"name": name, **{k: v for k, v in p.items() if k != "name"}} if name else p)
        if norm_pkgs:
            blob["factory_packages"] = norm_pkgs

    warranty = listing.get("warranty")
    if isinstance(warranty, list):
        w = [
            {"name": _s(x.get("name")), "value": _s(x.get("value"))}
            for x in warranty
            if isinstance(x, dict) and _s(x.get("name")) and _s(x.get("value"))
        ]
        if w:
            blob["warranty"] = w

    ws = _s(listing.get("window_sticker_url"))
    if ws:
        blob["window_sticker_url"] = ws

    return blob or None


def _map(listing: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    base = _harvest_map(listing)  # reuse color/image/engine/mileage/body mapping
    if not base:
        return None
    mech = listing.get("mechanical") if isinstance(listing.get("mechanical"), dict) else {}
    style = _style(listing)
    hist = listing.get("history_report") if isinstance(listing.get("history_report"), dict) else {}

    year = _int(listing.get("year"))
    make = _s(listing.get("make")) or ""
    model = _s(listing.get("model")) or ""
    trim = base.get("trim") or _s(listing.get("trim")) or ""
    title = " ".join(p for p in (str(year) if year else "", make, model, trim) if p).strip()

    gallery = _images(listing)
    hero = base.get("image_url") or (gallery[0] if gallery else None)
    if hero and not gallery:
        gallery = [hero]

    carfax = _s(hist.get("carfax_url"))
    options = _rich_options(listing)

    row: dict[str, Any] = {
        "vin": base["vin"],
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "title": title or _s(style.get("style_name")) or "",
        "price": _price(listing),
        "mileage": base.get("mileage"),
        "image_url": hero or "/static/placeholder.svg",
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name,
        "dealer_url": dealer_url,
        "exterior_color": base.get("exterior_color") or "",
        "interior_color": base.get("interior_color") or "",
        "fuel_type": base.get("fuel_type") or "",
        "drivetrain": base.get("drivetrain") or "",
        "transmission": _s(mech.get("transmission")),
        "engine_description": base.get("engine_description") or "",
        "engine_l": _float(mech.get("displacement") or mech.get("engine_size")),
        "cylinders": _int(mech.get("engine_cylinders")),
        "mpg_city": _int(mech.get("city_mpg")),
        "mpg_highway": _int(mech.get("highway_mpg")),
        "body_style": base.get("body_style") or "",
        "stock_number": _s(listing.get("stock")) or "",
        "condition": _condition(listing) or "",
        "is_cpo": _is_cpo(listing),
        "carfax_url": carfax,
        "packages": options,
        "window_sticker_url": _s(listing.get("window_sticker_url")),
        "zip_code": _s((listing.get("dealer") or {}).get("zipcode")) if isinstance(listing.get("dealer"), dict) else None,
    }
    du = _detail_url(listing)
    if du:
        row["_detail_url"] = du
        row["source_url"] = du
    return clean_car_row_dict(row)


def parse(raw_data, base_url: str, dealer_id: str, dealer_name: str = "", dealer_url: str = ""):
    listings = _listings(raw_data)
    if not listings:
        return []
    out: list[dict] = []
    for listing in listings:
        mapped = _map(listing, base_url, dealer_id, dealer_name, dealer_url)
        if mapped:
            out.append(mapped)
    return out
