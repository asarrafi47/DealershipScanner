"""
VDP price hints → single numeric value + conservative merge into listing ``price``.

Rule: VDP-derived price is applied only when the listing has no positive price
(empty, zero, or unparseable). Provenance is recorded for ``spec_source_json``
via merge_spec_source_json keys (caller supplies patch dict).
"""
from __future__ import annotations

import re
from typing import Any

from backend.parsers.base import norm_float
from backend.utils.field_clean import is_effectively_empty

# Higher wins when choosing among same numeric candidates
_SOURCE_PRIORITY: dict[str, float] = {
    "json_ld_offer": 100.0,
    "json_ld_product": 92.0,
    "dataLayer": 88.0,
    "dom_itemprop": 55.0,
    "dom_dealer": 45.0,
}


def _clamp_vehicle_price(n: float) -> float | None:
    if n != n:  # NaN
        return None
    if n < 500 or n > 2_500_000:
        return None
    return n


def parse_dom_price_text(text: str | None) -> float | None:
    """Strip currency noise; return None if not a plausible vehicle price."""
    if text is None or is_effectively_empty(text):
        return None
    s = str(text).strip()
    if re.search(r"call|contact|request|quote|inquire", s, re.I):
        return None
    for m in reversed(re.findall(r"\$[\s]*([\d,]+(?:\.\d{2})?)", s)):
        n = norm_float(m)
        c = _clamp_vehicle_price(n)
        if c is not None:
            return c
    n = norm_float(s)
    return _clamp_vehicle_price(n)


def pick_vdp_price_from_hints(hints: list[dict[str, Any]] | None) -> tuple[float | None, dict[str, Any] | None]:
    """
    *hints* entries: ``{"value": number, "source": str, "raw"?: str}`` (from PAGE_EXTRACT_JS / Python).

    Returns (price_float_or_none, meta_dict) where meta includes chosen source and alternates count.
    """
    if not hints:
        return None, None
    best: tuple[float, str, float, str] | None = None  # value, source, pri, raw
    for h in hints:
        if not isinstance(h, dict):
            continue
        v = h.get("value")
        try:
            vf = float(v)
        except (TypeError, ValueError):
            continue
        vf2 = _clamp_vehicle_price(vf)
        if vf2 is None:
            continue
        src = str(h.get("source") or "unknown")
        pri = float(_SOURCE_PRIORITY.get(src, 20.0))
        raw = str(h.get("raw") or "")[:80]
        if best is None or pri > best[2] or (pri == best[2] and vf2 > best[0]):
            best = (vf2, src, pri, raw)
    if not best:
        return None, None
    return best[0], {
        "source": best[1],
        "raw": best[3],
        "candidates": len([x for x in hints if isinstance(x, dict) and x.get("value") is not None]),
    }


def listing_price_is_empty(vehicle: dict[str, Any]) -> bool:
    p = vehicle.get("price")
    if p is None:
        return True
    if isinstance(p, str) and is_effectively_empty(p):
        return True
    try:
        return float(p) <= 0
    except (TypeError, ValueError):
        return True


def merge_vdp_price_into_vehicle(
    vehicle: dict[str, Any],
    price_value: float | None,
    *,
    provenance_source: str,
    detail_url: str = "",
) -> dict[str, Any]:
    """
    If *price_value* is set and listing price is empty, set ``vehicle["price"]`` to rounded int.

    Returns diagnostics: ``updated`` (bool), ``reason``, ``value``.
    """
    diag: dict[str, Any] = {"updated": False, "reason": "", "value": None}
    if price_value is None:
        diag["reason"] = "no_vdp_price"
        return diag
    if not listing_price_is_empty(vehicle):
        diag["reason"] = "listing_price_kept"
        diag["value"] = vehicle.get("price")
        return diag
    try:
        iv = int(round(float(price_value)))
    except (TypeError, ValueError):
        diag["reason"] = "bad_numeric"
        return diag
    if iv <= 0:
        diag["reason"] = "non_positive"
        return diag
    vehicle["price"] = iv
    diag["updated"] = True
    diag["reason"] = "filled_from_vdp"
    diag["value"] = iv
    diag["provenance_source"] = provenance_source
    diag["detail_url"] = (detail_url or "")[:300]
    return diag
