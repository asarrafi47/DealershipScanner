"""
``serialize_car_for_api`` steps tied to the selling dealer: the listing's VDP link
and the dealer-location block (state, fuel requirement, running-cost inputs).

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

from typing import Any

from backend.utils.field_clean import is_effectively_empty

from .location_tco import resolve_car_fuel_requirement, resolve_car_state_code


def apply_listing_vdp_url(out: dict[str, Any], c: dict[str, Any]) -> None:
    """``listing_vdp_url``; a real ``source_url`` is replaced by the resolved VDP."""
    from backend.parsers.vdp_urls import resolve_vehicle_source_url

    src_was_placeholder = is_effectively_empty(c.get("source_url"))
    listing_vdp = resolve_vehicle_source_url(c)
    if listing_vdp:
        out["listing_vdp_url"] = listing_vdp
        if not src_was_placeholder:
            out["source_url"] = listing_vdp
    else:
        out["listing_vdp_url"] = out.get("source_url") or out.get("dealer_url")


def apply_location_and_running_costs(
    out: dict[str, Any],
    c: dict[str, Any],
    *,
    engine_disp: Any,
    vs: dict[str, Any],
    tank_gal: float | None,
    factory_range: Any,
) -> None:
    """Dealer state, fuel requirement and the TCO block's inputs."""
    out["state"] = resolve_car_state_code(c)
    out["fuel_requirement"] = resolve_car_fuel_requirement(
        c, engine_display=engine_disp, verified_specs=vs if vs else None
    )
    from backend.intelligence.tco_fuel_estimates import (
        resolve_tco_avg_mpg,
        resolve_tco_ev_efficiency,
    )

    out["tco_avg_mpg"] = resolve_tco_avg_mpg(c)
    out["tco_ev_efficiency"] = resolve_tco_ev_efficiency(c)
    # Both resolved above, already None when no traceable figure exists (the
    # rounding is done there, so ``round(None, 1)`` cannot be reached). The spec
    # list and this block must never show different numbers for the same tank.
    out["factory_range"] = factory_range
    out["fuel_tank_gallons"] = tank_gal
