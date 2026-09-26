"""Server-rendered inventory pages whose cards carry ``data-vin`` attributes.

Quantum Auto Sales (a "Responsive Automotive" Next.js site, 2026-09-26) renders
all 208 cars into /inventory/ as cards::

    <div class="… inventory-image-wrapper" data-vin="19UUB6F49MA011833" …>
    <a href="/used-cars-ACURA-TLX-2021-011833-RPLZBEZR">
    <button data-sales-price="31123.25" data-vehicle-image-url="https://cdn…jpg" data-vin="…">

plus a JSON-LD ItemList of the first 50. This parser walks every element that
names a VIN, gathers the ``data-*`` attributes and the nearest detail link, and
takes identity (year / make / model) from the JSON-LD block when present, else
from the link slug. The HTTP-first detail-page pass fills the rest. Also the
parser behind the discovery capture's HTML-fragment pagers (PixelMotion).
"""
from __future__ import annotations

import html as _html
import re
from typing import Any

from backend.utils.field_clean import clean_car_row_dict

# Next.js/RSC pages carry most cards inside JS string payloads where quotes are
# escaped (data-vin=\"…\"): Quantum renders 9 cards as DOM and 199 as RSC data.
_VIN_ATTR_RE = re.compile(r"<([a-zA-Z][\w-]*)\b([^>]*?)\bdata-vin=\\?[\"']([A-HJ-NPR-Z0-9]{17})\\?[\"']([^>]*)>", re.I)
_ATTR_RE = re.compile(r"\b(data-[\w-]+|href|src)=\\?[\"']([^\"'\\]*)\\?[\"']", re.I)
_SLUG_RE = re.compile(r"/(new|used|certified|pre-owned)[-_]?(?:cars?|vehicles?|inventory)?[-/]+([A-Za-z]+)[-/]+([A-Za-z0-9]+)[-/]+((?:19|20)\d{2})\b", re.I)
_YEAR_MAKE_MODEL_RE = re.compile(r"\b((?:19|20)\d{2})\s+([A-Z][A-Za-z-]+)\s+([A-Za-z0-9 .-]{1,40}?)(?:\s{2,}|$|\s+(?:for sale|[\|•·]))", re.I)


def _money(v: Any) -> float | None:
    s = re.sub(r"[^\d.]", "", str(v or ""))
    try:
        f = float(s) if s else 0.0
    except ValueError:
        return None
    return f if 500 <= f <= 2_000_000 else None


_RSC_VIN_RE = re.compile(r'"vin"\s*:\s*"([A-HJ-NPR-Z0-9]{17})"')
_RSC_PRICE_KEYS = ("salePrice", "internetPrice", "priceInet", "price", "sellingPrice", "askingPrice", "retailPrice", "listPrice", "msrp")


def _unescape_rsc(html: str) -> str:
    """React Server Components streams embed JSON as escaped strings inside
    ``self.__next_f.push([...])``; fold the escaping so the objects parse."""
    return html.replace('\\"', '"').replace("\\u0026", "&").replace("\\/", "/")


def _enclosing_object(text: str, pos: int, max_len: int = 12000) -> dict[str, Any] | None:
    """The smallest JSON object around *pos* that parses (brace-matched, string-aware)."""
    import json as _json

    start = pos
    depth = 0
    i = pos
    while i >= 0 and pos - i < max_len:
        c = text[i]
        if c == "}":
            depth += 1
        elif c == "{":
            if depth == 0:
                start = i
                # find the matching close
                d = 0
                in_str = False
                esc = False
                for j in range(start, min(len(text), start + max_len)):
                    ch = text[j]
                    if in_str:
                        if esc:
                            esc = False
                        elif ch == "\\":
                            esc = True
                        elif ch == '"':
                            in_str = False
                        continue
                    if ch == '"':
                        in_str = True
                    elif ch == "{":
                        d += 1
                    elif ch == "}":
                        d -= 1
                        if d == 0:
                            try:
                                obj = _json.loads(text[start:j + 1])
                            except ValueError:
                                obj = None
                            if isinstance(obj, dict) and obj.get("vin"):
                                return obj
                            break
                depth = 0
                i -= 1
                continue
            depth -= 1
        i -= 1
    return None


def _rsc_vehicle_objects(html: str) -> dict[str, dict[str, Any]]:
    """{vin: vehicle json} from the RSC/hydration payload (Quantum: 208 objects with
    year, mileage, trim, stockNo, transmission, images…)."""
    text = _unescape_rsc(html)
    out: dict[str, dict[str, Any]] = {}
    for m in _RSC_VIN_RE.finditer(text):
        vin = m.group(1).upper()
        if vin in out:
            continue
        obj = _enclosing_object(text, m.start())
        if obj:
            out[vin] = obj
    return out


def _rsc_row(vin: str, o: dict[str, Any], base_url: str) -> dict[str, Any]:
    def g(*keys: str) -> Any:
        for k in keys:
            if o.get(k) not in (None, "", []):
                return o[k]
        return None

    price = None
    for k in _RSC_PRICE_KEYS:
        price = _money(o.get(k))
        if price:
            break
    images = o.get("images") if isinstance(o.get("images"), list) else []
    img_urls: list[str] = []
    for x in images:
        if isinstance(x, dict):
            u = x.get("large") or x.get("xlarge") or x.get("url") or x.get("src") or x.get("medium")
        else:
            u = x
        if isinstance(u, str) and u.startswith("http"):
            img_urls.append(u)
    hero = g("image1", "imageUrl", "image", "primaryImage")
    if not hero and o.get("image1raw") and str(o.get("image1raw")).startswith("/") and str(o.get("imagesPath") or "").startswith("http"):
        hero = f"{str(o['imagesPath']).rstrip('/')}/fit-in/720x540/filters:quality(85):no_upscale(){o['image1raw']}"
    cond = str(g("condition", "type", "vehicleType", "inventoryType", "stockType") or "").lower()
    is_new = o.get("isNew") is True or cond.startswith("new")
    row = {
        "vin": vin, "year": o.get("year"), "make": g("make", "makeName"), "model": g("model", "modelName"),
        "trim": g("trim", "trimName", "series") or "", "price": price, "msrp": _money(o.get("msrp")),
        "mileage": next((int(o[k]) for k in ("mileage", "miles", "odometer") if isinstance(o.get(k), (int, float))), None),
        "exterior_color": g("exteriorColor", "colorExterior", "extColor", "exterior_color", "color") or "",
        "interior_color": g("interiorColor", "colorInterior", "intColor", "interior_color") or "",
        "transmission": g("transmission") or "", "drivetrain": g("drivetrain", "driveTrain", "drive") or "",
        "engine_description": g("engine", "engineDescription") or "", "fuel_type": g("fuelType", "fuel") or "",
        "body_style": g("bodyStyle", "bodyType", "body", "vehType") or "", "stock_number": str(g("stockNo", "stockNumber", "stock") or ""),
        "cylinders": o.get("cylinders") if isinstance(o.get("cylinders"), int) else None,
        "condition": "New" if is_new else "Used", "image_url": hero if isinstance(hero, str) and hero.startswith("http") else (img_urls[0] if img_urls else ""),
        "gallery": img_urls[:40],
    }
    link = g("url", "vdpUrl", "detailUrl", "link", "slug")
    if isinstance(link, str) and link:
        if link.startswith("/"):
            link = base_url.rstrip("/") + link
        elif not link.startswith("http"):
            link = base_url.rstrip("/") + "/" + link.lstrip("/")
        row["_detail_url"] = link
        row["source_url"] = link
    return row


def detect(html: Any, min_cards: int = 10) -> bool:
    if not isinstance(html, str) or len(html) < 2000:
        return False
    if len({m.group(3).upper() for m in _VIN_ATTR_RE.finditer(html)}) >= min_cards:
        return True
    return len(set(_RSC_VIN_RE.findall(_unescape_rsc(html)))) >= min_cards


def _jsonld_identity(html: str) -> dict[str, dict[str, Any]]:
    """{vin: {make, model, year, price, mileage, name}} from JSON-LD Car/Vehicle blocks."""
    from backend.parsers.dealer_eprocess import _iter_vehicle_ld

    out: dict[str, dict[str, Any]] = {}
    try:
        for node in _iter_vehicle_ld(html):
            vin = str(node.get("vehicleIdentificationNumber") or node.get("mpn") or "").strip().upper()
            if len(vin) != 17:
                continue
            brand = node.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")
            offers = node.get("offers") if isinstance(node.get("offers"), dict) else ((node.get("offers") or [{}])[0] if isinstance(node.get("offers"), list) else {})
            mil = node.get("mileageFromOdometer")
            if isinstance(mil, dict):
                mil = mil.get("value")
            out[vin] = {
                "make": str(brand or "").strip(), "model": str(node.get("model") or "").strip(),
                "year": node.get("vehicleModelDate") or node.get("modelDate") or node.get("productionDate"),
                "price": _money((offers or {}).get("price")) if isinstance(offers, dict) else None,
                "mileage": mil, "name": str(node.get("name") or "").strip(),
                "exterior_color": str(node.get("color") or "").strip(), "trim": str(node.get("vehicleConfiguration") or "").strip(),
            }
    except Exception:  # noqa: BLE001
        pass
    return out


def parse(raw_data: Any, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "", **_kw: Any) -> list[dict]:
    if not isinstance(raw_data, str) or not raw_data:
        return []
    html = raw_data
    ld = _jsonld_identity(html)
    rows: dict[str, dict[str, Any]] = {}
    for m in _VIN_ATTR_RE.finditer(html):
        vin = m.group(3).upper()
        attrs = {k.lower(): _html.unescape(v) for k, v in _ATTR_RE.findall(m.group(2) + " " + m.group(4))}
        row = rows.setdefault(vin, {"vin": vin, "dealer_id": dealer_id, "dealer_name": dealer_name or dealer_id, "dealer_url": dealer_url or base_url,
                                     "gallery": [], "trim": "", "image_url": "", "price": None})
        for k, v in attrs.items():
            if "price" in k and not row.get("price"):
                row["price"] = _money(v)
            elif ("image" in k or k == "src") and v.startswith("http") and not row.get("image_url"):
                row["image_url"] = v
            elif k in ("data-year",) and v.isdigit():
                row["year"] = int(v)
            elif k in ("data-make",):
                row["make"] = v
            elif k in ("data-model",):
                row["model"] = v
            elif k in ("data-trim",):
                row["trim"] = v
            elif k in ("data-stock", "data-stock-number", "data-stocknumber"):
                row["stock_number"] = v
            elif k in ("data-mileage", "data-odometer") and re.sub(r"\D", "", v):
                row["mileage"] = int(re.sub(r"\D", "", v))
            elif k in ("data-condition", "data-type") and v:
                row["condition"] = v.title()
        # nearest detail link after this element
        tail = html[m.end(): m.end() + 1500]
        link = re.search(r"href=\\?[\"']([^\"'\\]+)\\?[\"']", tail)
        if link and not row.get("_detail_url"):
            href = _html.unescape(link.group(1))
            if href.startswith("/"):
                href = base_url.rstrip("/") + href
            if href.startswith("http") and vin.lower()[-6:] in href.lower() or (link and vin in tail):
                row["_detail_url"] = href
                row["source_url"] = href
        if row.get("_detail_url") and not row.get("make"):
            sm = _SLUG_RE.search(row["_detail_url"])
            if sm:
                row["condition"] = row.get("condition") or ("Used" if sm.group(1).lower() != "new" else "New")
                row["make"], row["model"], row["year"] = sm.group(2).title(), sm.group(3).upper() if len(sm.group(3)) <= 3 else sm.group(3).title(), int(sm.group(4))
    for vin, row in rows.items():
        info = ld.get(vin)
        if info:
            for k in ("make", "model", "trim", "exterior_color"):
                if info.get(k) and not row.get(k):
                    row[k] = info[k]
            if info.get("price") and not row.get("price"):
                row["price"] = info["price"]
            if info.get("mileage") not in (None, "") and not row.get("mileage"):
                try:
                    row["mileage"] = int(float(str(info["mileage"]).replace(",", "")))
                except ValueError:
                    pass
            if not row.get("year"):
                try:
                    row["year"] = int(str(info.get("year") or "")[:4])
                except ValueError:
                    pass
            if info.get("name") and (not row.get("make") or not row.get("model")):
                ym = _YEAR_MAKE_MODEL_RE.match(info["name"])
                if ym:
                    row.setdefault("year", int(ym.group(1)))
                    row["make"] = row.get("make") or ym.group(2).title()
                    row["model"] = row.get("model") or ym.group(3).strip().title()
        row.setdefault("condition", "Used")
        if row.get("image_url") and row["image_url"] not in row["gallery"]:
            row["gallery"].append(row["image_url"])
    # hydration objects: richer than the cards, and they carry every car the page holds
    for vin, obj in _rsc_vehicle_objects(html).items():
        rich = _rsc_row(vin, obj, base_url)
        row = rows.setdefault(vin, {"vin": vin, "dealer_id": dealer_id, "dealer_name": dealer_name or dealer_id, "dealer_url": dealer_url or base_url, "gallery": [], "trim": "", "image_url": "", "price": None})
        for k, v in rich.items():
            if v not in (None, "", []) and not row.get(k):
                row[k] = v
        row.setdefault("condition", rich["condition"])
    return [clean_car_row_dict(r) for r in rows.values() if r.get("vin")]
