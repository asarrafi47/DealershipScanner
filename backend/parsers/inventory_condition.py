"""Normalize new / used / certified condition from Dealer.com inventory objects."""
from __future__ import annotations

import re
from typing import Any

# Whole-word tests: "renewed", "unused", "newly listed" and "premium" must not read
# as new / used / pre-owned (N21.3, W18).
_NEW_RE = re.compile(r"\bnew\b")
_USED_RE = re.compile(r"\b(?:used|pre[- ]?owned)\b")
# Odometer-only inference (no classification text at all): below this is delivery
# mileage, new; at or above ``_USED_MIN_ODO`` is used; the band between is unknown
# (a dealer-spec'd used car and a demo both live there), so it stays None.
_NEW_MAX_ODO = 100
_USED_MIN_ODO = 500


def _truthy_certified(val: Any) -> bool:
    if val is True or val == 1:
        return True
    if isinstance(val, str):
        s = val.strip().lower()
        return s in ("1", "true", "yes", "y", "certified", "cpo")
    return False


def normalize_inventory_condition(
    obj: dict[str, Any],
    *,
    mileage: int | None = None,
) -> str | None:
    """
    Derive a stable condition label from inventory JSON (before VDP).

    Returns ``New``, ``Used``, ``Certified``, or None when unknown.
    """
    if not isinstance(obj, dict):
        return None

    if _truthy_certified(obj.get("certified")) or _truthy_certified(obj.get("isCpo")):
        return "Certified"

    blobs = " ".join(
        str(obj.get(k) or "")
        for k in (
            "classification",
            "classificationName",
            "categoryName",
            "inventoryType",
            "inventory_type",
            "newOrUsed",
            "type",
        )
    ).lower()
    # ``status`` is a lifecycle word ("New Arrival", "Just Listed"), not a
    # condition: it may say used or certified but never new (a used car read "New").
    status = str(obj.get("status") or "").lower()
    if "certif" in status or re.search(r"\bcpo\b", status):
        blobs = f"{blobs} certified"
    elif _USED_RE.search(status):
        blobs = f"{blobs} used"

    if "certif" in blobs or re.search(r"\bcpo\b", blobs):
        return "Certified"
    if _NEW_RE.search(blobs) and not _USED_RE.search(blobs):
        return "New"
    if _USED_RE.search(blobs):
        return "Used"

    # A missing odometer is unknown, not 0: never infer New from it.
    odo: int | None = None
    if mileage is not None:
        try:
            odo = int(mileage)
        except (TypeError, ValueError):
            odo = None
    else:
        raw = obj.get("odometer")
        if raw in (None, "", 0, "0"):
            raw = obj.get("mileage")
        if raw not in (None, ""):
            try:
                odo = int(raw)
            except (TypeError, ValueError):
                odo = None

    if odo is not None:
        if odo < _NEW_MAX_ODO:
            return "New"
        if odo >= _USED_MIN_ODO:
            return "Used"

    return None


__all__ = ["normalize_inventory_condition"]
