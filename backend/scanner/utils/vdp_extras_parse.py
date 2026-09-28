"""Fields a detail page carries in inline JSON that the SRP feed does not:
MSRP, stock number, Carfax / AutoCheck link, interior and exterior colour,
drivetrain, trim. Read over plain HTTP, no browser.

Verified shapes (2026-09-23):

  Dealer Inspire   ``"vehicleInfo":{"vin":..,"stock":..,"msrp":"52349","price":"50434",..}``
                   and a richer sibling object with ``"ext_color"``, ``"int_color"``,
                   ``"drivetrain"``, ``"fueltype"``, ``"engine_description"``, ``"stock"``.
  dealer.com       ``<body data-vehicle='{"vin":..,"msrp":25988,"displayedPrice":24110,
                   "stockNumber":"U58691","interiorColor":"Black",..}'>`` plus an
                   ``<a class="carfax-btn" href="https://www.carfax.com/vehiclehistory/...">``.
                   The 2026-09 template has no ``data-vehicle`` (0/10 fetched VDPs);
                   the record sits in ``DDC.WS.state['ws-vehicle-ctas'][id] =
                   {"vehicle":{"vin":..,"msrp":41063,"engine":..,"normalDriveLine":..}}``
                   and the identity in ``DDC.dataLayer.vehicles[0].<key> = ... || "<v>"``.
  DealerOn cosmos  Carfax anchor only (the rest lives in the SRP card JSON).
  Team Velocity    nothing usable in the static HTML (JSON-LD offers only).

Every value is validated against the page's VIN when the blob names one, so a
"similar vehicles" widget can never contribute another car's MSRP.
"""
from __future__ import annotations

import html as _html
import json
import re
from typing import Any

_CARFAX_RE = re.compile(r"https?://(?:www\.)?carfax\.com/(?:vehiclehistory|VehicleHistory)/[^\s\"'<>]{8,}", re.I)
_AUTOCHECK_RE = re.compile(r"https?://(?:www\.)?autocheck\.com/[^\s\"'<>]*(?:vin|VIN)=[A-HJ-NPR-Z0-9]{17}[^\s\"'<>]*", re.I)
_DI_VEHICLEINFO_RE = re.compile(r'"vehicleInfo"\s*:\s*\{')
_DI_EXTCOLOR_RE = re.compile(r'"ext_color"\s*:')
_DDC_DATA_VEHICLE_RE = re.compile(r"data-vehicle=(['\"])(.*?)\1", re.S)
_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
_MSRP_CEILING = 400_000.0  # above this a page "msrp" is an id fragment, not a sticker


def _brace_object(s: str, open_idx: int, max_len: int = 20000) -> str | None:
    """The JSON object starting at ``s[open_idx] == '{'``, brace-matched, string-aware."""
    if open_idx < 0 or open_idx >= len(s) or s[open_idx] != "{":
        return None
    depth = 0
    in_str = False
    esc = False
    end = min(len(s), open_idx + max_len)
    for i in range(open_idx, end):
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


def _load(obj_text: str | None) -> dict[str, Any] | None:
    if not obj_text:
        return None
    for candidate in (obj_text, _html.unescape(obj_text)):
        try:
            v = json.loads(candidate)
        except ValueError:
            continue
        if isinstance(v, dict):
            return v
    return None


def _money(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if v > 0 else None
    s = re.sub(r"[^\d.]", "", str(v))
    try:
        f = float(s) if s else 0.0
    except ValueError:
        return None
    return f if 500 <= f <= 2_000_000 else None


def _clean(v: Any, limit: int = 120) -> str | None:
    s = str(v or "").strip()
    return s[:limit] if s else None


def _vin_ok(blob: dict[str, Any], page_vin: str | None) -> bool:
    if not page_vin:
        return True
    bv = str(blob.get("vin") or blob.get("VIN") or "").strip().upper()
    return (not bv) or bv == page_vin


def _dealer_inspire(html: str, page_vin: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for rx in (_DI_EXTCOLOR_RE, _DI_VEHICLEINFO_RE):
        for m in rx.finditer(html):
            start = html.rfind("{", 0, m.start()) if rx is _DI_EXTCOLOR_RE else m.end() - 1
            blob = _load(_brace_object(html, start))
            if not blob or not _vin_ok(blob, page_vin):
                continue
            out.setdefault("msrp", _money(blob.get("msrp")))
            out.setdefault("stock_number", _clean(blob.get("stock"), 40))
            out.setdefault("exterior_color", _clean(blob.get("ext_color")))
            out.setdefault("interior_color", _clean(blob.get("int_color")))
            out.setdefault("drivetrain", _clean(blob.get("drivetrain"), 40))
            out.setdefault("trim", _clean(blob.get("trim"), 80))
            out.setdefault("engine_description", _clean(blob.get("engine_description"), 200))
            out.setdefault("fuel_type", _clean(blob.get("fueltype") or blob.get("fuel_type"), 40))
            if all(out.get(k) for k in ("msrp", "stock_number", "exterior_color", "interior_color")):
                break
    return {k: v for k, v in out.items() if v}


_DDC_WS_STATE_RE = re.compile(r"DDC\.WS\.state\['(ws-vehicle-ctas)'\]\['[^']+'\]\s*=\s*\{")
# DDC.dataLayer.vehicles[0].msrp = DDC.dataLayer.vehicles[0].msrp || "41063";
_DDC_DATALAYER_KV_RE = re.compile(
    r"DDC\.dataLayer\.vehicles\[0\]\.(\w+)\s*=\s*DDC\.dataLayer\.vehicles\[0\]\.\w+\s*\|\|\s*"
    r"(\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|-?\d+(?:\.\d+)?)\s*;"
)


def _ddc_vehicle_fields(blob: dict[str, Any]) -> dict[str, Any]:
    """Field map shared by data-vehicle and the ws-vehicle-ctas ``vehicle`` record."""
    msrp = _money(blob.get("msrp"))
    out = {
        "msrp": msrp if msrp and msrp <= _MSRP_CEILING else None,
        "stock_number": _clean(blob.get("stockNumber"), 40),
        "exterior_color": _clean(blob.get("exteriorColor")),
        "interior_color": _clean(blob.get("interiorColor")),
        "drivetrain": _clean(blob.get("drivetrain") or blob.get("driveLine") or blob.get("normalDriveLine"), 40),
        "trim": _clean(blob.get("trim"), 80),
        "fuel_type": _clean(blob.get("fuelType") or blob.get("normalFuelType"), 40),
        "engine_description": _clean(blob.get("engine"), 200),
        "transmission": _clean(blob.get("transmission"), 120),
    }
    return {k: v for k, v in out.items() if v}


def _ddc_datalayer_vehicle(html: str) -> dict[str, Any]:
    """``DDC.dataLayer.vehicles[0]`` as a dict, from its ``key = key || value`` lines."""
    out: dict[str, Any] = {}
    for m in _DDC_DATALAYER_KV_RE.finditer(html):
        key, raw = m.group(1), m.group(2)
        if raw[:1] in "\"'":
            try:
                val = json.loads(raw) if raw[0] == '"' else raw[1:-1]
            except ValueError:
                val = raw[1:-1]
            val = val.replace("\\/", "/")
        else:
            val = raw
        out.setdefault(key, val)
    return out


def _dealer_com(html: str, page_vin: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    m = _DDC_DATA_VEHICLE_RE.search(html)
    if m:
        blob = _load(m.group(2))
        if blob and _vin_ok(blob, page_vin):
            out.update(_ddc_vehicle_fields(blob))
    if not out or not out.get("msrp"):
        for ws in _DDC_WS_STATE_RE.finditer(html):
            blob = _load(_brace_object(html, ws.end() - 1))
            veh = blob.get("vehicle") if isinstance(blob, dict) else None
            if not isinstance(veh, dict) or not _vin_ok(veh, page_vin):
                continue
            for k, v in _ddc_vehicle_fields(veh).items():
                out.setdefault(k, v)
            if out.get("msrp"):
                break
    if not out.get("msrp"):
        dl = _ddc_datalayer_vehicle(html)
        if dl and _vin_ok(dl, page_vin):
            for k, v in _ddc_vehicle_fields(dl).items():
                out.setdefault(k, v)
    return {k: v for k, v in out.items() if v}


def _history_link(html: str, page_vin: str | None) -> str | None:
    for rx in (_CARFAX_RE, _AUTOCHECK_RE):
        m = rx.search(html)
        if m:
            u = _html.unescape(m.group(0)).rstrip("\\")
            if rx is _AUTOCHECK_RE and page_vin and page_vin not in u.upper():
                continue
            return u[:900]
    return None


_LD_BLOCK_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I)


def _jsonld_vehicle(html: str, page_vin: str | None) -> dict[str, Any]:
    """schema.org Vehicle block: identity and price for feeds that only give a
    title + link (WordPress /wp-json/v1/vehicles sites)."""
    out: dict[str, Any] = {}
    for m in _LD_BLOCK_RE.finditer(html):
        try:
            data = json.loads(_html.unescape(m.group(1)).strip())
        except ValueError:
            continue
        nodes = data if isinstance(data, list) else [data]
        stack = list(nodes)
        while stack:
            node = stack.pop()
            if not isinstance(node, dict):
                continue
            for k in ("@graph", "itemListElement", "mainEntity", "item"):
                v = node.get(k)
                if isinstance(v, list):
                    stack.extend(v)
                elif isinstance(v, dict):
                    stack.append(v)
            t = node.get("@type")
            types = {str(x).lower() for x in (t if isinstance(t, list) else [t]) if x}
            if not ({"vehicle", "car", "product"} & types):
                continue
            vin = str(node.get("vehicleIdentificationNumber") or node.get("mpn") or "").strip().upper()
            if page_vin and vin and vin != page_vin:
                continue
            brand = node.get("brand")
            if isinstance(brand, dict):
                brand = brand.get("name")
            offers = node.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            mileage = node.get("mileageFromOdometer")
            if isinstance(mileage, dict):
                mileage = mileage.get("value")
            cand = {
                "make": _clean(brand, 40), "model": _clean(node.get("model"), 80), "trim": _clean(node.get("vehicleConfiguration"), 80),
                "price": _money((offers or {}).get("price")) if isinstance(offers, dict) else None,
                "exterior_color": _clean(node.get("color")), "interior_color": _clean(node.get("vehicleInteriorColor")),
                "stock_number": _clean((offers or {}).get("sku") if isinstance(offers, dict) else None, 40) or _clean(node.get("sku"), 40),
                "engine_description": _clean((node.get("vehicleEngine") or {}).get("name") if isinstance(node.get("vehicleEngine"), dict) else None, 200),
                "transmission": _clean(node.get("vehicleTransmission"), 120),
                "fuel_type": _clean(node.get("fuelType"), 40),
                "drivetrain": _clean(node.get("driveWheelConfiguration"), 40),
            }
            try:
                mi = int(float(str(mileage).replace(",", ""))) if mileage not in (None, "") else None
                if mi is not None and 0 <= mi < 1_000_000:
                    cand["mileage"] = mi
            except ValueError:
                pass
            for k, v in cand.items():
                if v and k not in out:
                    out[k] = v
            if out.get("make") and out.get("price"):
                return out
    return out


def parse_vdp_extras_from_html(html: str, page_vin: str | None = None) -> dict[str, Any]:
    """Best-effort extras from one detail page. Keys (all optional): msrp,
    stock_number, carfax_url, exterior_color, interior_color, drivetrain, trim,
    fuel_type, engine_description, transmission."""
    if not html or len(html) < 400:
        return {}
    vin = (page_vin or "").strip().upper() or None
    if vin and not _VIN_RE.match(vin):
        vin = None
    out: dict[str, Any] = {}
    for source in (_dealer_inspire, _dealer_com):
        try:
            for k, v in source(html, vin).items():
                out.setdefault(k, v)
        except Exception:  # noqa: BLE001 - one template's quirk must not lose the rest
            continue
    try:
        for k, v in _jsonld_vehicle(html, vin).items():
            out.setdefault(k, v)
    except Exception:  # noqa: BLE001
        pass
    link = _history_link(html, vin)
    if link:
        out["carfax_url"] = link
    return out
