"""Extract city/highway MPG from dealer inventory JSON objects."""
from __future__ import annotations

from typing import Any

from backend.parsers.base import find_tracking_attr, norm_int


_MPG_CITY_NEEDLES = frozenset(
    {"mpgcity", "citympg", "epacity", "fuelcity", "cityfueleconomy", "cityfuelefficiency"}
)
_MPG_HWY_NEEDLES = frozenset(
    {"mpghighway", "highwaympg", "epahighway", "fuelhighway", "hwympg", "mpghwy", "highwayfueleconomy", "highwayfuelefficiency"}
)


def _attr_arrays(obj: dict[str, Any]) -> list[list]:
    out: list[list] = []
    for key in ("trackingAttributes", "tracking_attributes", "attributes", "highlightedAttributes"):
        arr = obj.get(key)
        if isinstance(arr, list):
            out.append(arr)
    return out


def _first_tracking_value(obj: dict[str, Any], needles: frozenset[str]) -> str | None:
    for arr in _attr_arrays(obj):
        for row in arr:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("key") or row.get("attribute") or "").lower()
            norm = name.replace("_", "").replace("-", "").replace(" ", "")
            if any(n in norm for n in needles):
                val = row.get("value") or row.get("text") or row.get("label")
                if val is not None and str(val).strip():
                    return str(val).strip()
    return None


def pick_mpg_city(obj: dict[str, Any]) -> int | None:
    if not isinstance(obj, dict):
        return None
    for key in (
        "mpgCity",
        "mpg_city",
        "cityMpg",
        "city_mpg",
        "epaCityMpg",
        "epa_city_mpg",
        "cityFuelEconomy",
        "cityFuelEfficiency",
        "city_fuel_economy",
        "city_fuel_efficiency",
    ):
        n = norm_int(obj.get(key))
        if n > 0:
            return n
    fe = obj.get("fuelEconomy") or obj.get("fuel_economy")
    if isinstance(fe, dict):
        for key in ("city", "cityMpg", "mpgCity", "combined"):
            n = norm_int(fe.get(key))
            if n > 0:
                return n
    got = _first_tracking_value(obj, _MPG_CITY_NEEDLES)
    if got:
        n = norm_int(got)
        if n > 0:
            return n
    return None


def pick_mpg_highway(obj: dict[str, Any]) -> int | None:
    if not isinstance(obj, dict):
        return None
    for key in (
        "mpgHighway",
        "mpg_highway",
        "highwayMpg",
        "highway_mpg",
        "epaHighwayMpg",
        "epa_highway_mpg",
        "highwayFuelEconomy",
        "highwayFuelEfficiency",
        "highway_fuel_economy",
        "highway_fuel_efficiency",
    ):
        n = norm_int(obj.get(key))
        if n > 0:
            return n
    fe = obj.get("fuelEconomy") or obj.get("fuel_economy")
    if isinstance(fe, dict):
        for key in ("highway", "highwayMpg", "mpgHighway", "hwy"):
            n = norm_int(fe.get(key))
            if n > 0:
                return n
    got = _first_tracking_value(obj, _MPG_HWY_NEEDLES)
    if got:
        n = norm_int(got)
        if n > 0:
            return n
    return None


def apply_inventory_mpg(obj: dict[str, Any], row: dict[str, Any]) -> None:
    """Set ``mpg_city`` / ``mpg_highway`` on a mapped vehicle when inventory JSON has values."""
    mc = pick_mpg_city(obj)
    mh = pick_mpg_highway(obj)
    if mc is not None:
        row["mpg_city"] = mc
    if mh is not None:
        row["mpg_highway"] = mh


__all__ = ["apply_inventory_mpg", "pick_mpg_city", "pick_mpg_highway"]
