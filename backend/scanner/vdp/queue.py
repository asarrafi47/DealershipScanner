"""
Per-row gap predicates for the HTTP detail-page passes.

``_vehicle_needs_spec_gap_vdp`` (vdp/prefetch.py) and ``_count_https_gallery_urls``
(vdp/prefetch.py, vdp/vdp_recipes.py). The browser visit-queue scoring that used to
live here (field-gap scores, gallery-thin boost, rotation tie-break, sort key) was
deleted 2026-10-01 with the browser VDP pool it ordered. Leaf module, stdlib only.
"""
from __future__ import annotations

from typing import Any


def _vehicle_needs_spec_gap_vdp(vehicle: dict[str, Any]) -> bool:
    """True when listing JSON left obvious spec gaps worth a targeted VDP visit."""
    for key in (
        "engine_description",
        "transmission",
        "drivetrain",
        "fuel_type",
        "body_style",
        "trim",  # 23 dealers were "thin" on trim alone (41-83%) while the detail page carries it (2026-09-26)
    ):
        val = vehicle.get(key)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            return True
    return False


def _count_https_gallery_urls(vehicle: dict[str, Any]) -> int:
    seen: set[str] = set()
    n = 0
    g = vehicle.get("gallery")
    if isinstance(g, list):
        for u in g:
            if isinstance(u, str) and u.strip().lower().startswith("https://") and u not in seen:
                seen.add(u)
                n += 1
    iu = vehicle.get("image_url")
    if isinstance(iu, str) and iu.strip().lower().startswith("https://") and iu not in seen:
        n += 1
    return n
