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


def _canonical_fuel(raw: str) -> str | None:
    from backend.utils.field_clean import coerce_fuel_type_stored

    return coerce_fuel_type_stored(raw)


def _canonical_body(raw: str) -> str | None:
    from backend.utils.field_clean import coerce_body_style_stored

    return coerce_body_style_stored(raw)


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
        if re.search(r"^engine\b", lk) and "mpg" not in lk and len(val) < 300:
            out.setdefault("engine_description", val.strip()[:300])
            c = _cylinders_from_engine_blob(val)
            if c is not None:
                out.setdefault("cylinders", c)
        elif re.search(r"cyl", lk) and "mpg" not in lk:
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


_COLOR_LABEL_EXT = re.compile(
    r"(?i)^(exterior\s*(color|colour)?|ext\.?\s*(color|colour)?|color|colour)\s*$"
)
_COLOR_LABEL_INT = re.compile(
    r"(?i)^(interior\s*(color|colour)?|int\.?\s*(color|colour)?)\s*$"
)
_COLOR_RE_EXT = re.compile(
    r"(?i)exterior\s*(?:color|colour)[:\s]+([A-Za-z][\w /\-]{1,40}?)(?:\s*[,;<\n]|$)"
)
_COLOR_RE_INT = re.compile(
    r"(?i)interior\s*(?:color|colour)[:\s]+([A-Za-z][\w /\-]{1,40}?)(?:\s*[,;<\n]|$)"
)


def parse_color_from_listing_html(html: str) -> dict[str, str | None]:
    """
    Best-effort exterior/interior color from full listing/VDP HTML.

    Priority: JSON-LD color fields → labeled DOM pairs → regex in text.
    Returns a dict with keys ``exterior_color`` and ``interior_color`` (values may be None).
    """
    out: dict[str, str | None] = {"exterior_color": None, "interior_color": None}
    if not html:
        return out

    # JSON-LD: schema.org Car / Vehicle has a "color" field (usually exterior)
    for m in re.finditer(
        r'"(?:color|vehicleColor|exteriorColor)"\s*:\s*"([^"]{2,80})"', html[:800000], re.I
    ):
        val = m.group(1).strip()
        if val and out["exterior_color"] is None:
            out["exterior_color"] = val
    # schema.org uses "vehicleInteriorColor"; some feeds use "interiorColor".
    for m in re.finditer(
        r'"(?:vehicleInteriorColor|interiorColor)"\s*:\s*"([^"]{2,80})"', html[:800000], re.I
    ):
        val = m.group(1).strip()
        if val and out["interior_color"] is None:
            out["interior_color"] = val

    # Labeled DOM pairs: dl/table (via _dom_specs_from_soup) + autoWALL div.row>div.col pattern
    if not (out["exterior_color"] and out["interior_color"]):
        try:
            soup = BeautifulSoup(html, "html.parser")
            pairs = _dom_specs_from_soup(soup)
            # Also collect div.row > div.col pairs (autoWALL, ShopperExpress style)
            for row in soup.find_all(class_="row"):
                cols = row.find_all(class_="col", recursive=False)
                if len(cols) == 2:
                    lab = cols[0].get_text(strip=True).rstrip(":")
                    val = cols[1].get_text(strip=True)
                    if lab and val and len(lab) < 60 and len(val) < 200:
                        pairs[lab] = val
            for label, val in pairs.items():
                label_s = label.strip()
                if out["exterior_color"] is None and _COLOR_LABEL_EXT.match(label_s):
                    out["exterior_color"] = val.strip() or None
                if out["interior_color"] is None and _COLOR_LABEL_INT.match(label_s):
                    out["interior_color"] = val.strip() or None
        except Exception:
            pass

    # Regex text fallback
    if out["exterior_color"] is None:
        m = _COLOR_RE_EXT.search(html[:500000])
        if m:
            out["exterior_color"] = m.group(1).strip() or None
    if out["interior_color"] is None:
        m = _COLOR_RE_INT.search(html[:500000])
        if m:
            out["interior_color"] = m.group(1).strip() or None

    return out


def parse_price_from_listing_html(html: str) -> float | None:
    """
    Vehicle price from structured page data ONLY — schema.org JSON-LD offers
    and price meta tags. Never free-text regex over the page: footers and
    finance disclaimers are full of dollar amounts that are not the price.
    """
    from backend.scanner.utils.vdp_price_merge import _clamp_vehicle_price

    if not html:
        return None

    for m in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html[:1_500_000],
        re.S | re.I,
    ):
        try:
            data = json.loads(m.group(1))
        except (ValueError, TypeError):
            continue
        items = data if isinstance(data, list) else [data]
        # schema.org @graph wrapper: {"@graph": [WebSite, Vehicle{offers}]}
        # (dealer.com VDPs) — the priced Vehicle lives one level down.
        expanded: list = []
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("@graph"), list):
                expanded.extend(g for g in it["@graph"] if isinstance(g, dict))
            else:
                expanded.append(it)
        for item in expanded:
            if not isinstance(item, dict):
                continue
            types = item.get("@type", "")
            types_set = {t.lower() for t in types} if isinstance(types, list) else {str(types).lower()}
            if not (types_set & {"car", "vehicle", "product"}):
                continue
            offers = item.get("offers")
            for offer in offers if isinstance(offers, list) else [offers]:
                if not isinstance(offer, dict):
                    continue
                raw = offer.get("price")
                try:
                    price = _clamp_vehicle_price(float(raw)) if raw not in (None, "") else None
                except (TypeError, ValueError):
                    price = None
                if price:
                    return price

    for pat in (
        r'<meta[^>]+itemprop=["\']price["\'][^>]+content=["\']([\d.,]+)["\']',
        r'<meta[^>]+property=["\'](?:og|product):price:amount["\'][^>]+content=["\']([\d.,]+)["\']',
        r'itemprop=["\']price["\'][^>]+content=["\']([\d.,]+)["\']',
    ):
        m2 = re.search(pat, html[:1_500_000], re.I)
        if m2:
            try:
                price = _clamp_vehicle_price(float(m2.group(1).replace(",", "")))
            except (TypeError, ValueError):
                price = None
            if price:
                return price
    return None
