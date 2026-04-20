"""
Merge image gallery URLs from VDP / network into vehicle rows.

Does not touch merge_analytics_ep_into_vehicle (specs only there). Gallery is merged
separately after EP merge on the VDP path.

Merge rules (avoid wiping good listing data):
- Count existing HTTPS URLs in ``gallery`` plus ``image_url`` (deduped set).
- If that count is **below** ``SCANNER_GALLERY_MERGE_REPLACE_IF_BELOW`` (default 3):
  **replace** gallery with deduped, ordered candidates (capped); set ``image_url`` to first.
- Otherwise **extend**: keep existing order, append new HTTPS URLs not already present, cap.
"""
from __future__ import annotations

import os
from typing import Any

from backend.parsers.base import (
    dedupe_urls_order_prefer_large,
    inventory_gallery_max,
    normalize_image_url_https,
)


def _replace_if_below() -> int:
    try:
        return max(1, int((os.environ.get("SCANNER_GALLERY_MERGE_REPLACE_IF_BELOW") or "3").strip()))
    except ValueError:
        return 3


def _https_urls_in_vehicle(vehicle: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for key in ("gallery",):
        g = vehicle.get(key)
        if not isinstance(g, list):
            continue
        for u in g:
            if not isinstance(u, str):
                continue
            nu = normalize_image_url_https(u.strip())
            if not nu.startswith("https://"):
                continue
            if nu in seen:
                continue
            seen.add(nu)
            out.append(nu)
    hero = vehicle.get("image_url")
    if isinstance(hero, str):
        nh = normalize_image_url_https(hero.strip())
        if nh.startswith("https://") and nh not in seen:
            seen.add(nh)
            out.insert(0, nh)
    return out


def merge_inventory_row_galleries(dst: dict[str, Any], src: dict[str, Any]) -> None:
    """
    When merging duplicate VIN rows from multiple intercept payloads, union galleries
    (dst order first, then src-only URLs). Mutates dst in place.
    """
    mx = inventory_gallery_max()
    a = _https_urls_in_vehicle(dst)
    b = _https_urls_in_vehicle(src)
    merged = dedupe_urls_order_prefer_large(a + [u for u in b if u not in set(a)], max_len=mx)
    if merged:
        dst["gallery"] = merged
        dst["image_url"] = merged[0]
    elif b and not a:
        dst["gallery"] = b[:mx]
        dst["image_url"] = b[0]


def merge_vdp_gallery_into_vehicle(
    vehicle: dict[str, Any],
    candidate_urls: list[str],
    *,
    max_gallery: int | None = None,
) -> dict[str, Any]:
    """
    Merge VDP-harvested URLs into ``vehicle``. Returns small diagnostics dict:
    ``action`` (replace|extend|skip), ``added``, ``final_len``.
    """
    mx = max_gallery if max_gallery is not None else inventory_gallery_max()
    norm: list[str] = []
    seen: set[str] = set()
    for u in candidate_urls:
        if not isinstance(u, str):
            continue
        nu = normalize_image_url_https(u.strip())
        if not nu.startswith("https://"):
            continue
        if nu in seen:
            continue
        seen.add(nu)
        norm.append(nu)
    cand = dedupe_urls_order_prefer_large(norm, max_len=mx)
    if not cand:
        return {"action": "skip", "added": 0, "final_len": len(_https_urls_in_vehicle(vehicle))}

    existing = _https_urls_in_vehicle(vehicle)
    ex_set = set(existing)
    thresh = _replace_if_below()

    if len(existing) < thresh:
        vehicle["gallery"] = cand[:mx]
        vehicle["image_url"] = cand[0]
        return {
            "action": "replace",
            "added": len([u for u in cand if u not in ex_set]),
            "final_len": len(vehicle["gallery"]),
        }

    merged_list = dedupe_urls_order_prefer_large(
        existing + [u for u in cand if u not in ex_set],
        max_len=mx,
    )
    before_extra = len([u for u in merged_list if u not in ex_set])
    vehicle["gallery"] = merged_list
    if merged_list:
        vehicle["image_url"] = merged_list[0]
    return {"action": "extend", "added": before_extra, "final_len": len(merged_list)}


def gallery_https_bin_histogram(vehicles: list[dict[str, Any]]) -> dict[str, int]:
    """Bins: 0, 1, 2-4, 5+ HTTPS image URLs (gallery ∪ image_url)."""
    bins = {"0": 0, "1": 0, "2_4": 0, "5p": 0}
    for v in vehicles:
        n = len(_https_urls_in_vehicle(v))
        if n <= 0:
            bins["0"] += 1
        elif n == 1:
            bins["1"] += 1
        elif n < 5:
            bins["2_4"] += 1
        else:
            bins["5p"] += 1
    return bins
