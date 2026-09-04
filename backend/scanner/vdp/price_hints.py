"""
VDP price-hint application and cross-row price ripple.

Split out of ``backend/scanner/vdp/core.py`` (2026-09): ``_apply_vdp_price_hints`` merges a
single visited VDP's price hints (JSON-LD offers, dataLayer keys, DOM) into a vehicle row, and
``_ripple_vdp_price_same_detail_url`` copies the result onto sibling rows that share the same
``_detail_url`` but did not themselves receive a browser visit. Re-imported from
``backend.scanner.vdp.core`` for compatibility with the historical import surface.
"""
from __future__ import annotations

import json
from typing import Any

from backend.utils.spec_provenance import merge_spec_source_json
from backend.scanner.utils.vdp_price_merge import (
    listing_price_is_empty,
    merge_vdp_price_into_vehicle,
    pick_vdp_price_from_hints,
)


def _apply_vdp_price_hints(
    vehicle: dict[str, Any],
    bundle: dict[str, Any] | None,
    detail_url: str,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {"updated": False}
    hints = [h for h in (bundle.get("vdpPriceHints") or []) if isinstance(h, dict)]
    picked, meta = pick_vdp_price_from_hints(hints)
    src = (meta or {}).get("source") or "vdp"
    diag = merge_vdp_price_into_vehicle(vehicle, picked, provenance_source=str(src), detail_url=detail_url or "")
    if diag.get("updated"):
        vehicle["spec_source_json"] = merge_spec_source_json(
            vehicle.get("spec_source_json"),
            {
                "vdp_price": {
                    "source": "vdp_scan",
                    "origin": str(src)[:120],
                    "value": diag.get("value"),
                    "url": (detail_url or "")[:500],
                }
            },
        )
    return diag


def _ripple_vdp_price_same_detail_url(vehicles: list[dict[str, Any]]) -> None:
    """
    After VDP visits, copy ``price`` / ``spec_source_json.vdp_price`` onto sibling rows that share
    ``_detail_url`` but did not receive the browser visit (same URL, different queue positions).
    """
    donors: dict[str, dict[str, Any]] = {}
    for v in vehicles:
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        cur = donors.get(u)
        if cur is None:
            donors[u] = v
            continue
        if listing_price_is_empty(cur) and not listing_price_is_empty(v):
            donors[u] = v
            continue
        if listing_price_is_empty(v) or listing_price_is_empty(cur):
            continue
        try:
            if float(v.get("price") or 0) > float(cur.get("price") or 0):
                donors[u] = v
        except (TypeError, ValueError):
            pass

    for v in vehicles:
        u = (v.get("_detail_url") or "").strip()
        if not u.startswith("http"):
            continue
        donor = donors.get(u)
        if donor is None or donor is v:
            continue
        if listing_price_is_empty(v) and not listing_price_is_empty(donor):
            v["price"] = donor["price"]
            ds = donor.get("spec_source_json")
            if isinstance(ds, str) and ds.strip():
                try:
                    dj = json.loads(ds)
                    vp = dj.get("vdp_price")
                    if isinstance(vp, dict):
                        v["spec_source_json"] = merge_spec_source_json(
                            v.get("spec_source_json"),
                            {"vdp_price": dict(vp)},
                        )
                except json.JSONDecodeError:
                    pass
