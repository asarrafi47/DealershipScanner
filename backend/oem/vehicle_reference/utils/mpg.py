"""Canonical MPG strings for export (EPA-sourced rows are formatted at ingest time)."""
from __future__ import annotations


def format_epa_mpg_ratings(vehicle: dict[str, str | int | float | None]) -> str:
    """Build a single normalized MPG line from EPA vehicle XML fields (already parsed to dict)."""
    atv = (str(vehicle.get("atvType") or "")).strip()
    fuel1 = (str(vehicle.get("fuelType1") or "")).strip()

    def _i(key: str) -> int | None:
        v = vehicle.get(key)
        if v is None or v == "":
            return None
        try:
            return int(round(float(v)))
        except (TypeError, ValueError):
            return None

    city, hwy, comb = _i("city08"), _i("highway08"), _i("comb08")
    city_a, hwy_a, comb_a = _i("cityA08"), _i("highwayA08"), _i("combA08")

    if city is None or hwy is None or comb is None:
        return ""

    is_ev = atv == "EV" or fuel1 == "Electricity"
    if is_ev:
        return f"{city}/{hwy}/{comb} MPGe city/hwy/comb (EPA estimated)"

    if atv == "Plug-in Hybrid" and comb_a is not None and comb_a > 0:
        cc, hh, cb = city_a, hwy_a, comb_a
        if cc is None or hh is None:
            return f"{city}/{hwy}/{comb} mpg city/hwy/comb gasoline (EPA estimated); electric MPGe partial from EPA"
        return (
            f"{city}/{hwy}/{comb} mpg city/hwy/comb gasoline (EPA estimated); "
            f"{cc}/{hh}/{cb} MPGe city/hwy/comb electricity (EPA estimated)"
        )

    return f"{city}/{hwy}/{comb} mpg city/hwy/comb (EPA estimated)"


def normalize_mpg_cell(text: str | None) -> str:
    """Light touch: trim; future versions could re-parse legacy formats."""
    if not text:
        return ""
    return " ".join(text.split())
