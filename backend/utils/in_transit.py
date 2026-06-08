"""
Detect dealer listings that are not yet physically on the lot (pipeline / in transit).

Used to flag incomplete listings until the vehicle arrives and full listing data is available.
"""
from __future__ import annotations

import json
import re
from typing import Any

_IN_TRANSIT_RE = re.compile(
    r"vehicle\s+is\s+currently\s+in\s+transit|vehicle\s+in\s+transit|\bin\s+transit\b",
    re.I,
)


def normalize_availability_status(raw: Any) -> str | None:
    """Return ``in_transit``, ``on_lot``, or None."""
    s = str(raw or "").strip().lower().replace("-", " ")
    if not s:
        return None
    if "transit" in s:
        return "in_transit"
    if "on lot" in s or s in ("onlot", "lot"):
        return "on_lot"
    return None


def vehicle_is_in_transit(car: dict[str, Any] | None) -> bool:
    """True when scrape metadata indicates the car is not yet on the dealer lot."""
    if not isinstance(car, dict):
        return False
    if car.get("_in_transit") is True:
        return True
    status = normalize_availability_status(car.get("_availability_status"))
    if status == "in_transit":
        return True
    if status == "on_lot":
        return False

    desc = str(car.get("description") or "")
    if len(desc) > 20 and _IN_TRANSIT_RE.search(desc):
        return True

    raw_spec = car.get("spec_source_json")
    if isinstance(raw_spec, str) and raw_spec.strip():
        try:
            raw_spec = json.loads(raw_spec)
        except (TypeError, ValueError, json.JSONDecodeError):
            raw_spec = None
    if isinstance(raw_spec, dict):
        avail = raw_spec.get("availability")
        if isinstance(avail, dict):
            if avail.get("in_transit") is True:
                return True
            st = normalize_availability_status(avail.get("status") or avail.get("value"))
            if st == "in_transit":
                return True
            if st == "on_lot":
                return False
    return False


def apply_in_transit_flags_from_raw(
    vehicle: dict[str, Any],
    *,
    source: str,
) -> None:
    """
    Set ``_in_transit`` / ``_availability_status`` on a scanner vehicle dict from known fields.
    """
    if not isinstance(vehicle, dict):
        return
    for key in ("in_transit", "availability", "availability_status", "stock_status"):
        if key not in vehicle:
            continue
        status = normalize_availability_status(vehicle.get(key))
        if status:
            vehicle["_availability_status"] = status
            vehicle["_in_transit"] = status == "in_transit"
            vehicle["_availability_source"] = source
            return


def availability_spec_source_patch(vehicle: dict[str, Any]) -> dict[str, Any]:
    """Provenance patch for ``spec_source_json`` when availability is known."""
    if not isinstance(vehicle, dict):
        return {}
    status = None
    source = str(vehicle.get("_availability_source") or "scanner").strip() or "scanner"
    if vehicle.get("_in_transit") is True:
        status = "in_transit"
    else:
        status = normalize_availability_status(vehicle.get("_availability_status"))
    if not status:
        return {}
    return {
        "availability": {
            "source": source,
            "status": status,
            "in_transit": status == "in_transit",
        }
    }
