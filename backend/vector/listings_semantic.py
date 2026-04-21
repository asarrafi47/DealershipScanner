"""
Rich embedding text for inventory semantic search (pgvector listing documents).

Produces one normalized prose-style description per vehicle for better recall on
natural-language queries (e.g. “white X5 under 70k”, “AWD plug-in hybrid sedan”).
"""
from __future__ import annotations

import json
import re
from typing import Any

from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty
from backend.utils.listing_description_extract import semantic_packages_snippet


def _money(n: Any) -> str | None:
    try:
        v = float(n)
        if v > 0:
            return f"${int(round(v)):,}"
    except (TypeError, ValueError):
        pass
    return None


def _mi(n: Any) -> str | None:
    try:
        v = int(n)
        if v >= 0:
            return f"{v:,} miles"
    except (TypeError, ValueError):
        pass
    return None


def build_semantic_listing_document(
    car: dict[str, Any],
    *,
    dealer_city: str | None = None,
    dealer_state: str | None = None,
) -> str:
    """
    Single embedding document: natural prose + key specs (max ~8k for pgvector text column).
    """
    c = clean_car_row_dict(dict(car))
    year = c.get("year")
    make = (c.get("make") or "").strip()
    model = (c.get("model") or "").strip()
    trim = (c.get("trim") or "").strip()
    title = (c.get("title") or "").strip()

    head_bits = [str(x) for x in (year, make, model) if x not in (None, "", 0)]
    if trim:
        head_bits.append(trim)
    headline = " ".join(head_bits) if head_bits else (title or "Vehicle")

    segments: list[str] = []

    # Opening sentence
    seg = headline
    if title and title.lower() != seg.lower() and len(title) < 180:
        seg = f"{headline}. {title}" if seg else title
    segments.append(seg)

    # Body / segment hints
    body = (c.get("body_style") or "").strip()
    ml = (make + " " + model).lower()
    if body:
        segments.append(f"{make} {body}." if make else f"{body} body style.")
    elif "suv" in ml or re.search(r"\bX[0-9]\b", model, re.I):
        segments.append(f"{make} SUV crossover." if make else "SUV crossover.")
    elif "sedan" in ml or re.search(r"\b[0-9]{3}[ie]", model, re.I):
        segments.append("Sedan.")

    dt = (c.get("drivetrain") or "").strip()
    if dt:
        segments.append(f"{dt} drivetrain.")

    eng = (c.get("engine_description") or "").strip()
    if eng:
        segments.append(f"{eng}.")
    cyl = c.get("cylinders")
    try:
        if cyl is not None and int(cyl) > 0 and not eng:
            segments.append(f"{int(cyl)}-cylinder engine.")
    except (TypeError, ValueError):
        pass

    ft = (c.get("fuel_type") or "").strip()
    if ft:
        segments.append(f"{ft}.")

    tr = (c.get("transmission") or "").strip()
    if tr:
        segments.append(f"{tr} transmission.")

    cond = (c.get("condition") or "").strip()
    if cond:
        cl = cond.lower()
        if "certif" in cl or "cpo" in cl:
            segments.append("Certified pre-owned.")
        elif cond:
            segments.append(f"Condition: {cond}.")

    ext = (c.get("exterior_color") or "").strip()
    intc = (c.get("interior_color") or "").strip()
    if ext:
        segments.append(f"{ext} exterior.")
    if intc:
        segments.append(f"{intc} interior.")

    pr = _money(c.get("price"))
    if pr:
        segments.append(f"Listed at {pr}.")

    mileage_s = _mi(c.get("mileage"))
    if mileage_s:
        segments.append(f"{mileage_s}.")

    dn = (c.get("dealer_name") or "").strip()
    if dn:
        loc_parts = [x for x in (dealer_city, dealer_state) if x and str(x).strip()]
        if loc_parts:
            segments.append(f"{dn} in {', '.join(loc_parts)}.")
        else:
            segments.append(f"{dn}.")

    z = (c.get("zip_code") or "").strip()
    if z and not dealer_city:
        segments.append(f"ZIP {z}.")

    try:
        dq = float(c.get("data_quality_score") or 0)
        if dq > 0:
            segments.append(f"Listing quality score {dq:.0f}.")
    except (TypeError, ValueError):
        pass

    pkg_raw = c.get("packages")
    if pkg_raw and str(pkg_raw).strip() not in ("{}", "[]", "null"):
        try:
            pj = json.loads(pkg_raw) if isinstance(pkg_raw, str) else pkg_raw
        except (json.JSONDecodeError, TypeError):
            pj = None
        if isinstance(pj, dict):
            desc_seg = semantic_packages_snippet(
                {
                    "packages": pj.get("packages_normalized") or [],
                    "standalone_features": pj.get("standalone_features_from_description") or [],
                },
                max_chars=420,
            )
            if desc_seg:
                segments.append(f"Equipment hints: {desc_seg}")

    text = " ".join(segments)
    text = re.sub(r"\s+", " ", text).strip()
    if is_effectively_empty(text):
        # Fallback to compact labeled form
        from backend.utils.field_clean import build_inventory_chroma_document

        return build_inventory_chroma_document(c)[:8000]
    return text[:8000]
