"""Merge dealer history badge text from VDP DOM into vehicle dicts."""
from __future__ import annotations

from typing import Any

from backend.utils.history_highlights import (
    coalesce_history_highlights_for_storage,
    extract_history_highlights_from_badges,
    merge_history_highlights,
)


def merge_vdp_history_highlights_into_vehicle(vehicle: dict[str, Any], bundle: dict[str, Any]) -> bool:
    """
    Populate ``history_highlights`` from VDP ``domBadges`` and dealer notes/description.

    Returns True when the vehicle dict was updated.
    """
    if not isinstance(vehicle, dict) or not isinstance(bundle, dict):
        return False

    badges = bundle.get("domBadges") if isinstance(bundle.get("domBadges"), list) else []
    dom_notes = str(bundle.get("domDealerNotes") or "").strip()
    dom_desc = str(bundle.get("domDescription") or "").strip()

    prior = merge_history_highlights(vehicle.get("history_highlights"))
    from_badges = extract_history_highlights_from_badges(badges)
    from_text = merge_history_highlights(dom_notes, dom_desc, vehicle.get("description"))

    merged = merge_history_highlights(prior, from_badges, from_text)
    if not merged:
        return False

    current = coalesce_history_highlights_for_storage(vehicle)
    if current == merged:
        return False

    vehicle["history_highlights"] = merged
    return True
