"""Fuel economy display (MPG, or MPGe for electric drive)."""

from __future__ import annotations

from typing import Any

from backend.enrichment.verified_specs.sources import SpecSources


def resolve_fuel_economy(src: SpecSources, is_bev: bool, sticker_pkg: dict[str, Any]) -> Any:
    """EPA catalog > (BEV) window sticker > (non-BEV) dealer mpg columns."""
    from backend.enrichment import knowledge_engine as ke

    fuel_economy_display = ke.format_fuel_economy_display(src.epa, is_bev)
    if is_bev and sticker_pkg.get("mpg_city") and sticker_pkg.get("mpg_highway"):
        fuel_economy_display = ke.format_fuel_economy_display(
            {
                "city08": sticker_pkg.get("mpg_city"),
                "highway08": sticker_pkg.get("mpg_highway"),
            },
            True,
        )
    if not fuel_economy_display and not is_bev:
        from backend.utils.field_clean import format_mpg_city_highway_display

        fuel_economy_display = format_mpg_city_highway_display(
            src.car.get("mpg_city"), src.car.get("mpg_highway")
        )
    return fuel_economy_display
