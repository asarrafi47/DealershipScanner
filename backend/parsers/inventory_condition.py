"""Normalize new / used / certified condition from Dealer.com inventory objects."""
from __future__ import annotations

from typing import Any


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
            "status",
        )
    ).lower()

    if "certif" in blobs or "cpo" in blobs:
        return "Certified"
    if "new" in blobs and "used" not in blobs and "pre" not in blobs:
        return "New"
    if any(x in blobs for x in ("used", "pre-owned", "preowned", "pre owned")):
        return "Used"

    try:
        odo = mileage if mileage is not None else int(obj.get("odometer") or obj.get("mileage") or 0)
    except (TypeError, ValueError):
        odo = None

    if odo is not None:
        if odo < 500:
            return "New"
        return "Used"

    return None


__all__ = ["normalize_inventory_condition"]
