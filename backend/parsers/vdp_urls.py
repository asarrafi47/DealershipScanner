"""
Best-effort VDP URLs when the listing JSON omits explicit detail links.

Dealer.com sites commonly resolve /{new|used|certified}-inventory/vin-{VIN}.htm patterns.
"""
from __future__ import annotations

import re


def looks_like_real_vin(vin: str) -> bool:
    s = (vin or "").strip().upper()
    return bool(re.match(r"^[A-HJ-NPR-Z0-9]{17}$", s))


def suggest_dealer_style_vdp_url(base_url: str, vin: str, obj: dict | None = None) -> str | None:
    """
    When no href is present, try common Dealer.com / DMS-style VDP paths.
    obj may contain inventoryType / condition hints.
    """
    if not looks_like_real_vin(vin):
        return None
    obj = obj or {}
    vu = vin.strip().upper()
    base = (base_url or "").strip().rstrip("/")
    if not base.lower().startswith("http"):
        return None

    inv = str(
        obj.get("inventoryType")
        or obj.get("inventory_type")
        or obj.get("condition")
        or obj.get("vehicleCondition")
        or ""
    ).lower()
    title_blob = str(obj.get("title") or obj.get("vehicleTitle") or "").lower()
    buckets: list[str] = []
    if "certif" in inv or "cpo" in inv or "bmw certified" in title_blob or "certified pre-owned" in title_blob:
        buckets.extend(
            [
                "certified-inventory",
                "certified-pre-owned-inventory",
                "cpo-inventory",
                "used-inventory",
            ]
        )
    elif "new" in inv and "certif" not in inv:
        buckets.extend(["new-inventory", "used-inventory"])
    else:
        buckets.extend(
            [
                "used-inventory",
                "certified-inventory",
                "certified-pre-owned-inventory",
            ]
        )

    seen_b: set[str] = set()
    ordered_buckets: list[str] = []
    for b in buckets:
        if b not in seen_b:
            seen_b.add(b)
            ordered_buckets.append(b)

    candidates: list[str] = []
    for bucket in ordered_buckets:
        candidates.append(f"{base}/{bucket}/vin-{vu}.htm")
    candidates.append(f"{base}/vdp/{vu}.htm")
    return candidates[0]


def dealer_style_vdp_url_candidates(base_url: str, vin: str, obj: dict | None = None) -> list[str]:
    """All constructed Dealer.com-style VDP paths to try when the first returns no EP data (CPO vs used)."""
    first = suggest_dealer_style_vdp_url(base_url, vin, obj)
    if not first:
        return []
    vu = vin.strip().upper()
    base = (base_url or "").strip().rstrip("/")
    obj = obj or {}
    inv = str(
        obj.get("inventoryType")
        or obj.get("inventory_type")
        or obj.get("condition")
        or obj.get("vehicleCondition")
        or ""
    ).lower()
    title_blob = str(obj.get("title") or obj.get("vehicleTitle") or "").lower()
    buckets: list[str] = []
    if "certif" in inv or "cpo" in inv or "bmw certified" in title_blob or "certified pre-owned" in title_blob:
        buckets.extend(
            [
                "certified-inventory",
                "certified-pre-owned-inventory",
                "cpo-inventory",
                "used-inventory",
            ]
        )
    elif "new" in inv and "certif" not in inv:
        buckets.extend(["new-inventory", "used-inventory"])
    else:
        buckets.extend(
            [
                "used-inventory",
                "certified-inventory",
                "certified-pre-owned-inventory",
            ]
        )
    seen_b: set[str] = set()
    ordered: list[str] = []
    for b in buckets:
        if b not in seen_b:
            seen_b.add(b)
            ordered.append(b)
    out: list[str] = []
    seen_url: set[str] = set()
    for bucket in ordered:
        u = f"{base}/{bucket}/vin-{vu}.htm"
        if u not in seen_url:
            seen_url.add(u)
            out.append(u)
    u_vdp = f"{base}/vdp/{vu}.htm"
    if u_vdp not in seen_url:
        out.append(u_vdp)
    # Prefer suggest_dealer_style_vdp_url primary first in list
    if first in out:
        out.remove(first)
        out.insert(0, first)
    elif first:
        out.insert(0, first)
    return out


def merge_pick_and_suggest_detail_url(
    pick_fn,
    base_url: str,
    vin: str,
    obj: dict,
) -> str | None:
    """Try explicit listing fields first, then constructed URL."""
    u = pick_fn(obj, base_url) if callable(pick_fn) else None
    if u:
        return u
    return suggest_dealer_style_vdp_url(base_url, vin, obj)
