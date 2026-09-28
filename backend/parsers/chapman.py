"""
Parser for the Chapman Auto Group in-house platform (apiv2.chapmanapps.com).

The Chapman API returns the dealer's full inventory as a single flat JSON array
of rich vehicle objects (one array per /new and /used endpoint, no pagination).
The shape matches no existing parser, so it needs a dedicated flat-array mapper.

Selling price is not a field: compute
``price = msrp + markupsTotal - discountsTotal - rebatesAppliedTotal``; an
``msrp == 0`` means call-for-price. Photos are already absolute URLs on
photos.chapmanchoice.com.
"""
import logging
import re

from backend.parsers.base import norm_int, norm_str
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)


def _opt_str(v):
    if v is None:
        return None
    return normalize_optional_str(norm_str(v))


def _first(obj: dict, *keys) -> str | None:
    for k in keys:
        got = _opt_str(obj.get(k))
        if got:
            return got
    return None


def _candidate_list(raw_data):
    """Return the list of vehicle-shaped dicts a Chapman payload might hold.

    Unlike :func:`_iter_vehicles`, this does NOT require a ``vin``/``VIN`` key —
    it is used by :func:`detect`, which must recognize the shape even before
    checking individual fields.
    """
    if isinstance(raw_data, list):
        return [v for v in raw_data if isinstance(v, dict)]
    if isinstance(raw_data, dict):
        for key in ("vehicles", "results", "inventory", "data"):
            lst = raw_data.get(key)
            if isinstance(lst, list):
                return [v for v in lst if isinstance(v, dict)]
    return []


def detect(raw_data) -> bool:
    """True when ``raw_data`` is a Chapman Auto Group flat inventory array.

    Conservative on purpose: it must be reachable ahead of the declared
    provider (recipes for these dealers have been mis-tagged as
    ``dealer_dot_com`` in production) without hijacking other JSON shapes. A
    Chapman vehicle pairs a ``vin``/``VIN`` with at least one of its
    distinctive fields (``colorExt``, ``arkona``, ``uniqueArkona``,
    ``valueArkona``) — a combination no other provider payload in this repo
    produces. Only the first few items are checked, like the other detectors.
    """
    for v in _candidate_list(raw_data)[:3]:
        if not (v.get("vin") or v.get("VIN")):
            continue
        if any(v.get(k) is not None for k in ("colorExt", "arkona", "uniqueArkona", "valueArkona")):
            return True
    return False


def _iter_vehicles(raw_data):
    """Yield each vehicle dict from a Chapman flat JSON array (or {vehicles:[...]})."""
    if isinstance(raw_data, list):
        for v in raw_data:
            if isinstance(v, dict) and (v.get("vin") or v.get("VIN")):
                yield v
        return
    if isinstance(raw_data, dict):
        for key in ("vehicles", "results", "inventory", "data"):
            lst = raw_data.get(key)
            if isinstance(lst, list):
                for v in lst:
                    if isinstance(v, dict) and (v.get("vin") or v.get("VIN")):
                        yield v
                return


def _pick_price(doc: dict) -> float:
    pricing = doc.get("pricing") if isinstance(doc.get("pricing"), dict) else {}
    msrp = norm_int(pricing.get("msrp") or doc.get("msrp"))
    if msrp <= 0:
        return 0.0  # call-for-price
    price = (
        msrp
        + norm_int(pricing.get("markupsTotal"))
        - norm_int(pricing.get("discountsTotal"))
        - norm_int(pricing.get("rebatesAppliedTotal"))
    )
    return float(price) if price > 0 else float(msrp)


def _condition(doc: dict) -> str:
    if doc.get("isCertified") is True:
        return "Certified"
    t = (norm_str(doc.get("type")) or "").upper()
    if t.startswith("N"):
        return "New"
    if t.startswith("U"):
        return "Used"
    return ""


def _gallery(doc: dict) -> list[str]:
    urls = doc.get("imageUrls")
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(urls, list):
        for u in urls:
            s = _opt_str(u)
            if s and s.lower().startswith("http") and s not in seen:
                seen.add(s)
                out.append(s)
    return out


_ENGINE_L_RE = re.compile(r"(\d(?:\.\d+)?)\s*[lL]\b")


def _engine_liters(engine: str | None) -> float | None:
    if not engine:
        return None
    m = _ENGINE_L_RE.search(engine)
    if not m:
        return None
    try:
        f = float(m.group(1))
        return round(f, 2) if 0 < f < 20 else None
    except (TypeError, ValueError):
        return None


# Below this an "msrp" is a call-for-price placeholder, not a sticker (the same
# floor carscommerce / dealer.com apply to masked prices).
_MSRP_FLOOR = 1000


def _msrp(doc: dict) -> int | None:
    """Sticker MSRP from ``pricing.msrp`` / top-level ``msrp`` (both carried by
    the apiv2 feed; the parser used it only to compute price and never emitted
    it, 1,514 msrp-null rows, F15 2026-09-28). 0 = in transit / call for price."""
    pricing = doc.get("pricing") if isinstance(doc.get("pricing"), dict) else {}
    for raw in (pricing.get("msrp"), doc.get("msrp")):
        n = norm_int(raw)
        if n >= _MSRP_FLOOR:
            return n
    return None


def _detail_url(doc: dict, base_url: str) -> str | None:
    """The feed's own VDP link when it carries one (apiv2 items sampled on
    2026-09-28 carry none; the real Chapman VDP path is open question 5 of the
    findings doc, so nothing is guessed here)."""
    for key in ("vdpUrl", "vdp_url", "detailUrl", "detail_url", "url", "link", "href"):
        u = _opt_str(doc.get(key))
        if not u:
            continue
        if u.startswith("http"):
            return u
        if u.startswith("/") and base_url:
            return base_url.rstrip("/") + u
    return None


def _map(doc: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(doc.get("vin") or doc.get("VIN") or "")
    if not vin or len(vin) < 11:
        return None
    gallery = _gallery(doc)
    hero = gallery[0] if gallery else ""
    engine = _first(doc, "engine")
    row = {
        "vin": vin,
        "year": norm_int(doc.get("year")),
        "make": norm_str(doc.get("make")),
        "model": norm_str(doc.get("model")),
        "trim": _first(doc, "trim", "style") or "",
        "price": _pick_price(doc),
        "mileage": norm_int(doc.get("mileage")),
        "condition": _condition(doc),
        "exterior_color": _first(doc, "colorExt", "exteriorColor") or "",
        "interior_color": _first(doc, "colorInt", "interiorColor") or "",
        "body_style": _first(doc, "body") or "",
        "drivetrain": _first(doc, "drive", "drivetrain") or "",
        "fuel_type": _first(doc, "fuel") or "",
        "engine_description": engine or "",
        "transmission": _first(doc, "transmission") or "",
        "stock_number": _first(doc, "stockNumber", "stock_number") or "",
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or dealer_id,
        "dealer_url": dealer_url or base_url,
        "mpg_city": norm_int(doc.get("mpgCity")) or None,
        "mpg_highway": norm_int(doc.get("mpgHwy")) or None,
        "engine_l": _engine_liters(engine),
    }
    msrp = _msrp(doc)
    if msrp is not None:
        row["msrp"] = msrp
    du = _detail_url(doc, base_url or dealer_url)
    if du:
        row["_detail_url"] = du
        row["source_url"] = du
    return row


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a Chapman inventory JSON array to standard vehicle rows.

    Returns ``[]`` for anything that isn't a Chapman vehicle array, so it is safe
    in the shared auto-detect fallback.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for doc in _iter_vehicles(raw_data):
        mapped = _map(doc, base_url, dealer_id, dealer_name, dealer_url)
        if mapped and mapped["vin"].upper() not in seen:
            seen.add(mapped["vin"].upper())
            out.append(clean_car_row_dict(mapped))
    return out
