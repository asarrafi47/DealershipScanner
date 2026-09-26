"""
VDP visit-queue scoring and ordering.

Pure functions that decide which inventory rows earn a browser VDP visit and in what
order: field-gap scoring, gallery-thin boosts, daily rotation tie-breaks. Leaf module —
imports only ``backend.scanner.vdp.config`` inside the package.
"""
from __future__ import annotations

import hashlib
import os
from typing import Any

from backend.scanner.vdp.config import (
    _vdp_gallery_min_https,
    _vdp_gallery_priority_enabled,
)


def _vdp_field_gap_score(vehicle: dict[str, Any]) -> int:
    """Prefer VDP visits for rows missing many dealer fields (CPO/EV listing gaps)."""
    keys = (
        "transmission",
        "drivetrain",
        "body_style",
        "condition",
        "exterior_color",
        "interior_color",
        "engine_description",
        "description",
    )
    n = 0
    for k in keys:
        val = vehicle.get(k)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            n += 1
    return n


def _vdp_public_incomplete_gap_score(vehicle: dict[str, Any]) -> int:
    """Boost rows that fail the public listings spec sheet (Phase 3 completeness passes)."""
    try:
        from backend.utils.listing_completeness import listing_missing_field_codes

        return len(listing_missing_field_codes(vehicle, for_public_filter=True))
    except Exception:
        return 0


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


def _vehicle_needs_description_vdp(vehicle: dict[str, Any]) -> bool:
    """True when dealer notes / description are missing or too short."""
    desc = str(vehicle.get("description") or "").strip()
    return len(desc) < 40


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


def _vdp_gallery_thin_boost(vehicle: dict[str, Any]) -> int:
    """Higher score → higher priority for limited VDP budget when gallery is thin."""
    if not _vdp_gallery_priority_enabled():
        return 0
    have = _count_https_gallery_urls(vehicle)
    need = _vdp_gallery_min_https()
    if have >= need:
        return 0
    return (need - have) * 5


def _vdp_visit_priority_tuple(vehicle: dict[str, Any]) -> tuple[int, int, int]:
    """Sort key: public-incomplete boost, gallery-thin boost, then field-gap score."""
    pub_gap = _vdp_public_incomplete_gap_score(vehicle)
    field_gap = _vdp_field_gap_score(vehicle)
    thin = _vdp_gallery_thin_boost(vehicle)
    # Weight public spec gaps heavily so Phase 3 visits colors/transmission first.
    return (pub_gap * 10 + thin + field_gap, pub_gap, field_gap)


def _vdp_rotation_enabled() -> bool:
    return (os.environ.get("SCANNER_VDP_ROTATION") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _vdp_rotation_seed(dealer_id: str) -> str:
    explicit = (os.environ.get("SCANNER_VDP_ROTATION_SEED") or "").strip()
    if explicit:
        return explicit
    from datetime import datetime, timezone

    day = datetime.now(timezone.utc).date().isoformat()
    return f"{day}|{(dealer_id or '').strip()}"


def _vdp_rotation_tie_hash(vehicle: dict[str, Any], seed: str) -> int:
    vin = (vehicle.get("vin") or "").strip().upper()
    digest = hashlib.blake2b(f"{seed}\0{vin}".encode(), digest_size=6, usedforsecurity=False).digest()
    return int.from_bytes(digest, "big")


def _vdp_queue_sort_key(vehicle: dict[str, Any], seed: str, *, rotation: bool) -> tuple[Any, ...]:
    """Descending priority: public-incomplete + field gaps first; tie-break by rotation hash or VIN."""
    t = _vdp_visit_priority_tuple(vehicle)
    if rotation:
        return (-t[0], -t[1], -t[2], _vdp_rotation_tie_hash(vehicle, seed))
    return (-t[0], -t[1], -t[2], (vehicle.get("vin") or "").strip().upper())
