"""
Parser for the Jazel platform (SSR Angular SRP, jazelc.com).

Jazel serves a fully server-rendered SRP whose per-card vehicle JSON is embedded
as the 2nd argument of inline JS calls
``window.jzlSetVehicleInfoContext('<VIN>', {...})``. This parser accepts the raw
SRP HTML string, regexes each call, brace-balances the object literal, and
``json.loads`` it. The shape is Jazel-specific (matches no existing parser).

Fields: vin, year, make, model, trim, name, displayPrice(int)/price(str),
mileage(int), newOrUsed, isCertified, isUsed, image (single hero on
media-cdn-tango.jazelc.com), vdpLink, extColor, intColor, engine, transmission,
drivetrain, fuelType, mpgCity/mpgHighway, stockNumber, vehicleId, accountName.
"""
import json
import logging
import re

from backend.parsers.base import norm_int, norm_str
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)

# window.jzlSetVehicleInfoContext('VIN', { ... })  — capture start of the object.
_CALL_RE = re.compile(
    r"jzlSetVehicleInfoContext\s*\(\s*['\"]([A-HJ-NPR-Z0-9]{11,17})['\"]\s*,\s*(\{)",
    re.IGNORECASE,
)


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


def _balance_object(text: str, start: int) -> str | None:
    """Return the brace-balanced ``{...}`` substring beginning at *start*.

    Tracks string literals (single/double quoted) and escapes so braces inside
    strings don't unbalance the object.
    """
    depth = 0
    i = start
    n = len(text)
    in_str = False
    quote = ""
    while i < n:
        c = text[i]
        if in_str:
            if c == "\\":
                i += 2
                continue
            if c == quote:
                in_str = False
        else:
            if c in ("'", '"'):
                in_str = True
                quote = c
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        i += 1
    return None


def _iter_vehicle_objects(html: str):
    """Yield each parsed jzlSetVehicleInfoContext vehicle object from SRP HTML."""
    for m in _CALL_RE.finditer(html):
        obj_text = _balance_object(html, m.start(2))
        if not obj_text:
            continue
        try:
            obj = json.loads(obj_text)
        except (ValueError, TypeError):
            continue
        if isinstance(obj, dict):
            if not obj.get("vin"):
                obj["vin"] = m.group(1)
            yield obj


def _pick_price(doc: dict) -> float:
    n = norm_int(doc.get("displayPrice"))
    if n > 0:
        return float(n)
    from backend.parsers.base import extract_price

    return extract_price(doc)


def _condition(doc: dict) -> str:
    if doc.get("isCertified") is True:
        return "Certified"
    nou = (norm_str(doc.get("newOrUsed")) or "").lower()
    if nou.startswith("u") or doc.get("isUsed") is True:
        return "Used"
    if nou.startswith("n"):
        return "New"
    return ""


def _gallery(doc: dict) -> list[str]:
    hero = _opt_str(doc.get("image"))
    if hero:
        if hero.startswith("//"):
            hero = "https:" + hero
        if hero.lower().startswith("http"):
            return [hero]
    return []


def _detail_url(doc: dict, dealer_url: str, base_url: str) -> str | None:
    raw = _first(doc, "vdpLink", "vdpUrl", "url")
    if not raw:
        return None
    if raw.lower().startswith("http"):
        return raw
    if raw.startswith("//"):
        return "https:" + raw
    root = (dealer_url or base_url or "").strip()
    if root and not root.lower().startswith("http"):
        root = "https://" + root
    if not root:
        return None
    return root.rstrip("/") + "/" + raw.lstrip("/")


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


def _map(doc: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(doc.get("vin") or "")
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
        "trim": norm_str(doc.get("trim")),
        "price": _pick_price(doc),
        "mileage": norm_int(doc.get("mileage")),
        "condition": _condition(doc),
        "exterior_color": _first(doc, "extColor", "exteriorColor") or "",
        "interior_color": _first(doc, "intColor", "interiorColor") or "",
        "body_style": _first(doc, "bodyStyle", "body") or "",
        "drivetrain": _first(doc, "drivetrain") or "",
        "fuel_type": _first(doc, "fuelType", "fuel") or "",
        "engine_description": engine or "",
        "transmission": _first(doc, "transmission") or "",
        "stock_number": _first(doc, "stockNumber", "stock_number") or "",
        "image_url": hero,
        "gallery": gallery,
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or _first(doc, "accountName") or dealer_id,
        "dealer_url": dealer_url or base_url,
        "title": _first(doc, "name") or "",
        "mpg_city": norm_int(doc.get("mpgCity")) or None,
        "mpg_highway": norm_int(doc.get("mpgHighway")) or None,
        "engine_l": _engine_liters(engine),
    }
    du = _detail_url(doc, dealer_url, base_url)
    if du:
        row["source_url"] = du
        row["_detail_url"] = du
    return row


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a Jazel SSR SRP (HTML string) to standard vehicle rows.

    Returns ``[]`` for anything with no jzlSetVehicleInfoContext calls, so it is
    safe in the shared auto-detect fallback (JSON payloads never match).
    """
    if not isinstance(raw_data, str):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for doc in _iter_vehicle_objects(raw_data):
        mapped = _map(doc, base_url, dealer_id, dealer_name, dealer_url)
        if mapped and mapped["vin"].upper() not in seen:
            seen.add(mapped["vin"].upper())
            out.append(clean_car_row_dict(mapped))
    return out
