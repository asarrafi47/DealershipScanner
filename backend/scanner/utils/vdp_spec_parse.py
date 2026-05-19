"""
Parse dealer VDP HTML for mechanical specs (no network — used by ``vdp_spec_extract``).
"""
from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup

_MPG_SLASH = re.compile(
    r"(?:mpg|fuel\s+economy|fuel\s+economy\s*est)[^\d]{0,16}(\d{1,2})\s*[/|–-]\s*(\d{1,2})",
    re.I,
)
_CYL_WORD = re.compile(
    r"(?:^|[^\d])(\d)\s*[-\s]?(?:cyl|cylinder| cyl)", re.I
)
_V6_V8 = re.compile(r"\b(V6|V8|V10|V12|I4|I6|I3)\b", re.I)


def _cylinders_from_engine_blob(blob: str) -> int | None:
    if not blob:
        return None
    m = _CYL_WORD.search(blob)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    u = blob.upper()
    if re.search(r"\bEV\b|ELECTRIC|BEV\b|KWH", u):
        return 0
    v = _V6_V8.search(blob)
    if v:
        tok = v.group(1).upper()
        if tok == "I4":
            return 4
        if tok == "I3":
            return 3
        if tok == "I6":
            return 6
        if tok.startswith("V"):
            try:
                return int(tok[1:])
            except ValueError:
                return None
    return None


_FUEL_CANONICAL: dict[str, str] = {
    "gasoline": "Gasoline", "gas": "Gasoline", "petrol": "Gasoline",
    "regular gasoline": "Gasoline", "premium gasoline": "Gasoline",
    "diesel": "Diesel",
    "electric": "Electric", "battery electric": "Electric", "bev": "Electric",
    "hybrid": "Hybrid", "mild hybrid": "Hybrid", "gas / mild hybrid": "Hybrid",
    "plug-in hybrid": "Plug-In Hybrid", "plug in hybrid": "Plug-In Hybrid", "phev": "Plug-In Hybrid",
    "hydrogen": "Hydrogen",
}

_BODY_STYLE_CANONICAL: dict[str, str] = {
    "sedan": "Sedan", "coupe": "Coupe", "convertible": "Convertible",
    "hatchback": "Hatchback", "wagon": "Wagon", "suv": "SUV",
    "sport utility vehicle": "SUV", "crossover": "Crossover",
    "pickup": "Truck", "pickup truck": "Truck", "truck": "Truck",
    "minivan": "Minivan", "van": "Van", "roadster": "Roadster",
}


def _canonical_fuel(raw: str) -> str | None:
    k = raw.strip().lower()
    for phrase, canonical in _FUEL_CANONICAL.items():
        if phrase in k or k in phrase:
            return canonical
    return None


def _canonical_body(raw: str) -> str | None:
    k = raw.strip().lower()
    return _BODY_STYLE_CANONICAL.get(k) or next(
        (v for phrase, v in _BODY_STYLE_CANONICAL.items() if phrase in k), None
    )


def _walk_json_ld(obj: Any, out: dict[str, Any]) -> None:
    if isinstance(obj, dict):
        types = obj.get("@type")
        tlist = types if isinstance(types, list) else ([types] if types else [])
        tls = {str(x).lower() for x in tlist if x}
        hit = any(
            "vehicle" in x or "car" in x or "automobile" in x or "product" in x for x in tls
        )
        if hit or obj.get("vehicleIdentificationNumber") or obj.get("vin"):
            vt = obj.get("vehicleTransmission") or obj.get("transmission")
            if isinstance(vt, dict):
                vt = vt.get("name") or vt.get("value")
            if isinstance(vt, str) and vt.strip():
                out.setdefault("transmission", vt.strip()[:200])
            dw = obj.get("driveWheelConfiguration")
            if isinstance(dw, dict):
                dw = dw.get("name") or dw.get("value")
            if isinstance(dw, str) and dw.strip():
                out.setdefault("drivetrain", dw.strip()[:120])
            ft = obj.get("fuelType")
            if isinstance(ft, str) and ft.strip():
                cf = _canonical_fuel(ft)
                if cf:
                    out.setdefault("fuel_type", cf)
            bc = obj.get("bodyType") or obj.get("vehicleBodyType")
            if isinstance(bc, str) and bc.strip():
                cb = _canonical_body(bc)
                if cb:
                    out.setdefault("body_style", cb)
            eng = obj.get("vehicleEngine")
            if isinstance(eng, dict):
                blob = str(eng.get("name") or eng.get("description") or "")[:500]
            elif isinstance(eng, str):
                blob = eng[:500]
            else:
                blob = ""
            if blob:
                c = _cylinders_from_engine_blob(blob)
                if c is not None:
                    out.setdefault("cylinders", c)
        for v in obj.values():
            _walk_json_ld(v, out)
    elif isinstance(obj, list):
        for it in obj:
            _walk_json_ld(it, out)


def _parse_json_ld_blocks(html: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    soup = BeautifulSoup(html or "", "html.parser")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.text or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            for node in data:
                _walk_json_ld(node, out)
        else:
            _walk_json_ld(data, out)
    return out


def _dom_specs_from_soup(soup: BeautifulSoup) -> dict[str, str]:
    """Label → value from common VDP spec tables."""
    pairs: dict[str, str] = {}
    for sel in ("dl", "table", ".vehicle-specs", ".specifications", ".vdp-specs"):
        for el in soup.select(sel)[:12]:
            for row in el.find_all("tr")[:80]:
                cells = row.find_all(["th", "td"])
                if len(cells) >= 2:
                    lab = cells[0].get_text(" ", strip=True)
                    val = cells[1].get_text(" ", strip=True)
                    if lab and val and len(lab) < 80 and len(val) < 400:
                        pairs[lab[:60]] = val[:300]
            for dt in el.find_all("dt")[:80]:
                dd = dt.find_next_sibling("dd")
                if dd:
                    lab = dt.get_text(" ", strip=True)
                    val = dd.get_text(" ", strip=True)
                    if lab and val:
                        pairs[lab[:60]] = val[:300]
    return pairs


def _apply_dom_pairs(pairs: dict[str, str], out: dict[str, Any]) -> None:
    for label, val in pairs.items():
        lk = label.lower()
        if re.search(r"mpg|fuel\s+economy", lk):
            m = re.search(r"(\d{1,2})\s*[/|–-]\s*(\d{1,2})", val)
            if m:
                try:
                    out.setdefault("mpg_city", int(m.group(1)))
                    out.setdefault("mpg_highway", int(m.group(2)))
                except ValueError:
                    pass
        if re.search(r"cyl|engine", lk) and "mpg" not in lk:
            c = _cylinders_from_engine_blob(val)
            if c is not None:
                out.setdefault("cylinders", c)
        if re.search(r"trans", lk) and len(val) < 200:
            out.setdefault("transmission", val.strip()[:200])
        if re.search(r"drive|drivetrain|driveline", lk) and len(val) < 120:
            out.setdefault("drivetrain", val.strip()[:120])
        if re.search(r"^fuel\b|fuel[\s_]type", lk) and len(val) < 120:
            cf = _canonical_fuel(val)
            if cf:
                out.setdefault("fuel_type", cf)
        if re.search(r"^body\b|body[\s_]style|body[\s_]type", lk) and len(val) < 120:
            cb = _canonical_body(val)
            if cb:
                out.setdefault("body_style", cb)


def parse_html_for_vehicle_specs(html: str) -> dict[str, Any]:
    """
    Best-effort cylinders / MPG / transmission / drivetrain from full VDP HTML.
    Conservative: only sets keys when patterns match clearly.
    """
    out: dict[str, Any] = {}
    if not html:
        return out
    out.update(_parse_json_ld_blocks(html))
    soup = BeautifulSoup(html, "html.parser")
    _apply_dom_pairs(_dom_specs_from_soup(soup), out)
    m = _MPG_SLASH.search(html)
    if m and "mpg_city" not in out:
        try:
            out.setdefault("mpg_city", int(m.group(1)))
            out.setdefault("mpg_highway", int(m.group(2)))
        except ValueError:
            pass
    # Dealer.com style keys in raw HTML
    for pat in (
        r"city_fuel_economy['\"]?\s*[:=]\s*['\"]?(\d{1,2})",
        r"cityFuelEconomy['\"]?\s*[:=]\s*['\"]?(\d{1,2})",
    ):
        m2 = re.search(pat, html, re.I)
        if m2 and "mpg_city" not in out:
            out.setdefault("mpg_city", int(m2.group(1)))
            break
    for pat in (
        r"highway_fuel_economy['\"]?\s*[:=]\s*['\"]?(\d{1,2})",
        r"highwayFuelEconomy['\"]?\s*[:=]\s*['\"]?(\d{1,2})",
    ):
        m3 = re.search(pat, html, re.I)
        if m3 and "mpg_highway" not in out:
            out.setdefault("mpg_highway", int(m3.group(1)))
            break
    return out


_SCHEMA_COND_SUFFIX = re.compile(r"(?:schema\.org/)?(New|Used|Damaged|Refurbished)Condition", re.I)
_EMBEDDED_VEH_COND = re.compile(
    r'vehicleCondition["\']?\s*[:=]\s*["\']?([A-Za-z_]+)',
    re.I,
)


def _normalize_raw_condition_token(raw: str) -> str | None:
    """Map scraped dealer/schema tokens to stored listing condition strings."""
    if not raw:
        return None
    t = raw.strip()
    if not t:
        return None
    u = t.upper().replace("-", "_").replace(" ", "_")
    if u in ("NEW", "N"):
        return "New"
    if u in ("USED", "U", "PREOWNED", "PRE_OWNED", "PRE-OWNED"):
        return "Used"
    if u in ("CERTIFIED", "CPO", "CP", "CERTIFIED_USED", "CERTIFIEDPREOWNED"):
        return "Certified Pre-Owned"
    low = t.lower()
    if "certified" in low or "cpo" in low:
        return "Certified Pre-Owned"
    if low == "new" or low.startswith("new "):
        return "New"
    if "used" in low or "pre-owned" in low or "preowned" in low:
        return "Used"
    return None


def _condition_from_schema_url(url: str) -> str | None:
    if not url:
        return None
    m = _SCHEMA_COND_SUFFIX.search(url)
    if not m:
        return None
    return _normalize_raw_condition_token(m.group(1))


def _walk_json_ld_condition(obj: Any, out: list[str]) -> None:
    """Collect human-readable condition hints from JSON-LD (schema.org Offer / Vehicle)."""
    if isinstance(obj, dict):
        for key in ("itemCondition", "vehicleCondition"):
            vc = obj.get(key)
            if not isinstance(vc, str):
                continue
            s = vc.strip()
            if not s:
                continue
            if s.lower().startswith("http"):
                c = _condition_from_schema_url(s)
            else:
                c = _normalize_raw_condition_token(s)
            if c:
                out.append(c)
        for v in obj.values():
            _walk_json_ld_condition(v, out)
    elif isinstance(obj, list):
        for it in obj:
            _walk_json_ld_condition(it, out)


def parse_condition_from_listing_html(html: str) -> str | None:
    """
    Best-effort condition for ``cars.condition`` from full listing/VDP HTML.

    Priority: embedded inventory JSON keys → schema.org JSON-LD → DOM label/value pairs.
    Does not fetch network.
    """
    if not html:
        return None
    # Dealer platform embedded JSON (common on CDK / Dealer.com pages)
    for m in _EMBEDDED_VEH_COND.finditer(html[:800000]):
        c = _normalize_raw_condition_token(m.group(1))
        if c:
            return c
    # Quoted JSON snippets
    for m in re.finditer(r'"itemCondition"\s*:\s*"([^"]+)"', html[:800000]):
        c = _condition_from_schema_url(m.group(1))
        if c:
            return c
    soup = BeautifulSoup(html, "html.parser")
    cond_hints: list[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.text or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            _walk_json_ld_condition(node, cond_hints)
    if cond_hints:
        # Prefer most specific (CPO) over generic Used
        for pref in ("Certified Pre-Owned", "New", "Used"):
            if pref in cond_hints:
                return pref
        return cond_hints[0]

    pairs = _dom_specs_from_soup(soup)
    for label, val in pairs.items():
        if re.match(r"(?i)^condition\s*$", label.strip()):
            c = _normalize_raw_condition_token(val)
            if c:
                return c
            return _normalize_raw_condition_token(val.split()[0]) if val else None
    return None
