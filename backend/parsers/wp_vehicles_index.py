"""WordPress dealer sites that expose ``/wp-json/v1/vehicles``: a search index of
every car as ``{"title", "link", "search"}`` (Burns Honda, Honda of Cleveland,
Honda of Pasadena, Kia of Chattanooga, 2026-09-24: 469 items, whole lot, one GET).

The title carries condition, year, make/model/trim text, the VIN and the stock
number: ``"Used 2021 Ram 1500 Big Horn 1C6SRFFTXMN566184 MN566184T"``. The link is
the detail page, whose JSON-LD Vehicle block gives price, colours, mileage and the
proper make/model split; the HTTP-first pass reads that (vdp_extras_parse). So this
parser emits the identity fields and leaves the rest for the detail page.
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.field_clean import clean_car_row_dict

_TITLE_RE = re.compile(
    r"^\s*(?P<cond>New|Used|Certified(?:\s+Pre-Owned)?|Pre-Owned|CPO)\s+(?P<year>(?:19|20)\d{2})\s+(?P<rest>.+?)\s+(?P<vin>[A-HJ-NPR-Z0-9]{17})(?:\s+(?P<stock>\S+))?\s*$",
    re.I,
)


def detect(raw_data: Any) -> bool:
    if not isinstance(raw_data, dict):
        return False
    v = raw_data.get("vehicles")
    if not isinstance(v, list) or not v:
        return False
    first = v[0]
    return isinstance(first, dict) and "title" in first and "link" in first


def _split_make_model(rest: str) -> tuple[str, str]:
    """Best effort: first token is the make, the remainder the model text. The
    detail page's JSON-LD (brand.name / model / vehicleConfiguration) replaces this."""
    parts = rest.split()
    if not parts:
        return "", ""
    if len(parts) >= 2 and parts[0].lower() in ("alfa", "aston", "land", "mercedes", "mercedes-benz") and parts[0].lower() != "mercedes-benz":
        return " ".join(parts[:2]), " ".join(parts[2:])
    return parts[0], " ".join(parts[1:])


def _condition(raw: str) -> str:
    low = raw.lower()
    if low.startswith("new"):
        return "New"
    if "certified" in low or low == "cpo":
        return "Certified Pre-Owned"
    return "Used"


def parse(raw_data: Any, base_url: str = "", dealer_id: str = "", dealer_name: str = "", dealer_url: str = "", **_kw: Any) -> list[dict]:
    if not detect(raw_data):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for item in raw_data["vehicles"]:
        if not isinstance(item, dict):
            continue
        m = _TITLE_RE.match(str(item.get("title") or item.get("search") or ""))
        if not m:
            continue
        vin = m.group("vin").upper()
        if vin in seen:
            continue
        seen.add(vin)
        make, model = _split_make_model(m.group("rest").strip())
        link = str(item.get("link") or "").strip()
        row = {
            "vin": vin,
            "year": int(m.group("year")),
            "make": make,
            "model": model,
            "trim": "",
            "condition": _condition(m.group("cond")),
            "stock_number": (m.group("stock") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "price": None,
            "image_url": "",
            "gallery": [],
            "dealer_id": dealer_id,
            "dealer_name": dealer_name or dealer_id,
            "dealer_url": dealer_url or base_url,
        }
        if link.startswith("http"):
            row["source_url"] = link
            row["_detail_url"] = link
        out.append(clean_car_row_dict(row))
    return out
