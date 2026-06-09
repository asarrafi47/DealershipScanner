"""Resolve Carfax / vehicle-history URLs from dealer inventory JSON payloads."""
from __future__ import annotations

from typing import Any

from backend.parsers.base import norm_str


def first_carfax_http_url(val: Any) -> str | None:
    if isinstance(val, str):
        s = norm_str(val)
        if s.startswith("http") and "carfax" in s.lower():
            return s
        return None
    if isinstance(val, dict):
        for k in ("url", "href", "link", "value", "src", "uri"):
            hit = first_carfax_http_url(val.get(k))
            if hit:
                return hit
        return None
    if isinstance(val, list):
        for item in val:
            hit = first_carfax_http_url(item)
            if hit:
                return hit
    return None


def inventory_signals_carfax(obj: dict) -> bool:
    for key in (
        "carfaxOneOwner",
        "showCarfax",
        "hasCarfaxReport",
        "carfaxAvailable",
        "displayCarfax",
        "carfax",
    ):
        v = obj.get(key)
        if v in (True, 1, "1", "true", "True", "yes", "Yes"):
            return True
    for key in ("callout", "callouts", "badges", "Badges", "highlightedAttributes", "highlighted_attributes"):
        val = obj.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str) and "carfax" in item.lower():
                    return True
                if isinstance(item, dict):
                    blob = " ".join(
                        str(item.get(k) or "")
                        for k in ("text", "label", "name", "value", "title")
                    ).lower()
                    if "carfax" in blob:
                        return True
        elif isinstance(val, str) and "carfax" in val.lower():
            return True
    return False


def extract_carfax_url(obj: dict, vin: str) -> str | None:
    """Resolve Carfax / vehicle-history URL from inventory JSON when available."""
    explicit = norm_str(
        obj.get("carfax_url")
        or obj.get("carfaxUrl")
        or obj.get("carfaxLink")
        or obj.get("history_report_url")
        or obj.get("vehicleHistoryUrl")
        or obj.get("vehicle_history_url")
        or ""
    )
    if explicit.startswith("http"):
        return explicit

    for key in ("callout", "callouts", "badges", "Badges", "highlightedAttributes", "highlighted_attributes"):
        hit = first_carfax_http_url(obj.get(key))
        if hit:
            return hit

    vhr_url = obj.get("vhr_url") or obj.get("carfax_token")
    if vhr_url and isinstance(vhr_url, str) and vhr_url.strip().startswith("http"):
        return norm_str(vhr_url)
    if vhr_url and vin and not vin.startswith("unknown"):
        return f"https://vhr.carfax.com/main?vin={vin}"
    if inventory_signals_carfax(obj) and vin and not vin.startswith("unknown"):
        return f"https://vhr.carfax.com/main?vin={vin}"
    return explicit if explicit.startswith("http") else None
