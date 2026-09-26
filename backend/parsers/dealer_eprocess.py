"""
Parser for the Dealer eProcess (DEP / "Phoenix") platform.

DEP dealers have NO JSON inventory API. Each SRP is a server-rendered HTML page
that embeds one ``application/ld+json`` ``@type: "Vehicle"`` block per rendered
card (12 vehicles/page), paginated over plain HTTP with ``?p=N``. The recipe for
these dealers is therefore an HTML page-walk, and the "raw_data" handed to this
parser is the SRP HTML **string** (not a JSON dict) — recipe replay returns the
raw page body for DEP recipes (see ``recipes.PAGINATION_DEP_SRP``).

Each JSON-LD Vehicle carries: vehicleIdentificationNumber / mpn (VIN), model,
releaseDate + vehicleModelDate (year), brand.name / manufacturer.name (make),
vehicleConfiguration (trim), color / vehicleInteriorColor, fuelType,
vehicleTransmission, vehicleEngine.name, mileageFromOdometer.value (SMI), image,
a site-relative ``url`` (VDP), and an ``offers`` object/list carrying price, sku
(stock number) and itemCondition (New/Used schema.org URL).
"""
import json
import logging
import re
from urllib.parse import urljoin

from backend.parsers.base import extract_price, norm_int, norm_str
from backend.utils.field_clean import clean_car_row_dict, normalize_optional_str

logger = logging.getLogger(__name__)

_LDJSON_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _opt_str(v):
    if v is None:
        return None
    return normalize_optional_str(norm_str(v))


def _iter_vehicle_ld(raw_data):
    """Yield each JSON-LD ``@type: Vehicle`` dict from a DEP SRP.

    Accepts either the raw SRP HTML string, or a pre-extracted list/dict of
    JSON-LD objects (so callers that already parsed the page can reuse this).
    """
    blocks: list = []
    if isinstance(raw_data, str):
        for chunk in _LDJSON_RE.findall(raw_data):
            try:
                blocks.append(json.loads(chunk))
            except (ValueError, TypeError):
                continue
    elif isinstance(raw_data, list):
        blocks = list(raw_data)
    elif isinstance(raw_data, dict):
        blocks = [raw_data]

    # Schema.org spells a car as Vehicle, Car, or a Product that carries a VIN
    # (McPeek's CDJR: Car blocks; Quantum Auto Sales: an ItemList of Car items,
    # 2026-09-26). Blocks may wrap items in @graph or ItemList.itemListElement.
    def _walk(node, depth=0):
        if depth > 4:
            return
        if isinstance(node, list):
            for x in node:
                yield from _walk(x, depth + 1)
            return
        if not isinstance(node, dict):
            return
        t = node.get("@type")
        types = {str(x) for x in (t if isinstance(t, list) else [t]) if x}
        if types & {"Vehicle", "Car", "Product"} and (node.get("vehicleIdentificationNumber") or node.get("mpn")):
            yield node
            return
        for key in ("@graph", "itemListElement", "item", "mainEntity"):
            if key in node:
                yield from _walk(node[key], depth + 1)

    for obj in blocks:
        yield from _walk(obj)


def _offer(obj: dict) -> dict:
    off = obj.get("offers")
    if isinstance(off, list):
        return off[0] if off and isinstance(off[0], dict) else {}
    return off if isinstance(off, dict) else {}


def _condition(obj: dict, offer: dict) -> str:
    name = (norm_str(obj.get("name")) or "").lower()
    if "certified" in name:
        return "Certified"
    cond = (norm_str(offer.get("itemCondition")) or "").lower()
    if "used" in cond:
        return "Used"
    if "new" in cond:
        return "New"
    if name.startswith("used") or name.startswith("pre-owned"):
        return "Used"
    if name.startswith("new"):
        return "New"
    return ""


def _mileage(obj: dict) -> int:
    m = obj.get("mileageFromOdometer")
    if isinstance(m, dict):
        return norm_int(m.get("value"))
    return norm_int(m)


def _make(obj: dict) -> str:
    for key in ("brand", "manufacturer"):
        v = obj.get(key)
        if isinstance(v, dict):
            got = norm_str(v.get("name"))
            if got:
                return got
        elif isinstance(v, str):
            got = norm_str(v)
            if got:
                return got
    return ""


def _engine(obj: dict) -> str:
    e = obj.get("vehicleEngine")
    if isinstance(e, dict):
        return norm_str(e.get("name"))
    return norm_str(e)


def _price(obj: dict, offer: dict) -> float:
    p = norm_int(offer.get("price"))
    if p > 0:
        return float(p)
    return extract_price(offer) or extract_price(obj)


def _image(obj: dict) -> str:
    img = obj.get("image")
    if isinstance(img, list):
        img = img[0] if img else ""
    s = norm_str(img)
    if s.startswith("//"):
        s = "https:" + s
    return s


def _detail_url(obj: dict, offer: dict, dealer_url: str, base_url: str) -> str:
    raw = _opt_str(obj.get("url")) or _opt_str(offer.get("url"))
    if not raw:
        return ""
    if raw.lower().startswith("http"):
        return raw
    if raw.startswith("//"):
        return "https:" + raw
    root = (dealer_url or base_url or "").strip()
    if root and not root.lower().startswith("http"):
        root = "https://" + root
    if not root:
        return ""
    try:
        return urljoin(root.rstrip("/") + "/", raw.lstrip("/"))
    except ValueError:
        return ""


def _map(obj: dict, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> dict | None:
    vin = norm_str(obj.get("vehicleIdentificationNumber") or obj.get("mpn") or "")
    if not vin or len(vin) < 11:
        return None
    offer = _offer(obj)
    vdp = _detail_url(obj, offer, dealer_url, base_url)
    hero = _image(obj)
    row = {
        "vin": vin,
        "year": norm_int(obj.get("vehicleModelDate") or obj.get("releaseDate")),
        "make": _make(obj),
        "model": norm_str(obj.get("model")),
        "trim": norm_str(obj.get("vehicleConfiguration")),
        "price": _price(obj, offer),
        "mileage": _mileage(obj),
        "condition": _condition(obj, offer),
        "exterior_color": norm_str(obj.get("color")),
        "interior_color": norm_str(obj.get("vehicleInteriorColor")),
        "fuel_type": norm_str(obj.get("fuelType")),
        "engine_description": _engine(obj),
        "transmission": norm_str(obj.get("vehicleTransmission")),
        "stock_number": norm_str(offer.get("sku") or obj.get("sku") or offer.get("serialNumber")),
        "image_url": hero,
        "gallery": [hero] if hero else [],
        "dealer_id": dealer_id,
        "dealer_name": dealer_name or dealer_id,
        "dealer_url": dealer_url or base_url,
    }
    if vdp:
        row["source_url"] = vdp
        row["_detail_url"] = vdp
    return row


_INLINE_DATA_RE = re.compile(r"filter_vehicle_data\s*[:=]\s*\{")


def _brace_object(s: str, open_idx: int, max_len: int = 4_000_000) -> str | None:
    depth = 0
    in_str = False
    esc = False
    for i in range(open_idx, min(len(s), open_idx + max_len)):
        c = s[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return s[open_idx:i + 1]
    return None


def inline_vehicle_data(raw_html: str) -> dict[str, dict]:
    """VIN -> the SRP's inline ``filter_vehicle_data`` record. DEP renders the facet
    data for every card into the page as a JSON map that carries what the JSON-LD
    lacks: ``body`` (body style), ``drivetrain``, ``flag_certified``, ``highest_price``
    (Honda of El Cajon 2026-09-23: body_style 0% and drivetrain 15% from JSON-LD alone).
    Keys may be VINs or stock ids; records are matched by their own vin field when
    present. Returns {} when the blob is absent or unparsable."""
    if not isinstance(raw_html, str):
        return {}
    m = _INLINE_DATA_RE.search(raw_html)
    if not m:
        return {}
    blob = _brace_object(raw_html, m.end() - 1)
    if not blob:
        return {}
    try:
        data = json.loads(blob)
    except ValueError:
        return {}
    out: dict[str, dict] = {}
    items = data.values() if isinstance(data, dict) else (data if isinstance(data, list) else [])
    for key, rec in (data.items() if isinstance(data, dict) else enumerate(items)):
        if not isinstance(rec, dict):
            continue
        vin = norm_str(rec.get("vin") or rec.get("VIN") or (key if isinstance(key, str) and len(key) == 17 else "")).upper()
        if len(vin) == 17:
            out[vin] = rec
    return out


def _apply_inline(row: dict, rec: dict) -> None:
    body = norm_str(rec.get("body") or rec.get("body_style") or rec.get("bodystyle"))
    if body and not row.get("body_style"):
        row["body_style"] = body
    drive = norm_str(rec.get("drivetrain") or rec.get("drive") or rec.get("drive_train"))
    if drive and not row.get("drivetrain"):
        row["drivetrain"] = drive
    cert = rec.get("flag_certified") if "flag_certified" in rec else rec.get("certified")
    if cert in (True, 1, "1", "true", "True", "Y", "yes") and str(row.get("condition") or "").lower() != "certified":
        row["condition"] = "Certified"
        row["is_cpo"] = 1
    for k in ("msrp", "highest_price"):
        v = rec.get(k)
        try:
            f = float(str(v).replace(",", "").replace("$", "")) if v not in (None, "") else 0.0
        except ValueError:
            f = 0.0
        if f >= 500 and not row.get("msrp") and (not row.get("price") or f >= float(row["price"] or 0)):
            row["msrp"] = f
            break


def parse(raw_data, *, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "") -> list[dict]:
    """Map a Dealer eProcess SRP (HTML string carrying JSON-LD) to vehicle rows.

    Returns ``[]`` for anything that has no JSON-LD ``Vehicle`` blocks, so it is
    safe in the shared auto-detect fallback (JSON API payloads never match).
    """
    out: list[dict] = []
    seen: set[str] = set()
    inline = inline_vehicle_data(raw_data)
    for obj in _iter_vehicle_ld(raw_data):
        mapped = _map(obj, base_url, dealer_id, dealer_name, dealer_url)
        if mapped and mapped["vin"].upper() not in seen:
            seen.add(mapped["vin"].upper())
            rec = inline.get(mapped["vin"].upper())
            if rec:
                _apply_inline(mapped, rec)
            out.append(clean_car_row_dict(mapped))
    return out
