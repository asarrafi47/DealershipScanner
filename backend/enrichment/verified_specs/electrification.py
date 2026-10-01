"""Is this car electric-drive (BEV / fuel cell)? Decides cylinders=0, MPGe and single-speed."""

from __future__ import annotations

from typing import Any

from backend.enrichment.verified_specs import plausibility
from backend.enrichment.verified_specs.sources import SpecSources


def _dealer_label_is_pure_ev(car: dict[str, Any]) -> bool:
    """Dealer-supplied fuel_type "Electric" / "Electricity" (pure BEV, not PHEV/hybrid),
    unless the row's own evidence contradicts the label."""
    _dealer_ft_lower = (car.get("fuel_type") or "").strip().lower()
    _dealer_is_pure_ev = (
        "electric" in _dealer_ft_lower
        and not any(x in _dealer_ft_lower for x in ("gas", "gasoline", "hybrid", "plug"))
    )
    if _dealer_is_pure_ev and not plausibility.dealer_electric_label_is_plausible(car):
        _dealer_is_pure_ev = False
    return _dealer_is_pure_ev


def _feed_and_catalog_say_electric(src: SpecSources) -> bool:
    """Trim decoder, catalog and dealer signals (vPIC, sticker and Daytona come after).

    Fuel-cell vehicles (Mirai, NEXO) are electric-drive: no cylinders, MPGe. EPA
    dual-fuel strings ("Premium Gasoline / Electricity" = PHEV) must not count.
    """
    regex, epa = src.regex, src.epa
    _dealer_ft_lower = (src.car.get("fuel_type") or "").strip().lower()
    _dealer_is_pure_ev = _dealer_label_is_pure_ev(src.car)
    _epa_fuel_lower = (epa.get("fuel_type") or "").lower()
    return (
        regex.get("cylinders") == 0
        or (regex.get("fuel_type_hint") or "").strip().lower() == "electric"
        or (epa.get("atv_type") or "").strip().upper() in ("EV", "FCV")
        or ("electric" in _epa_fuel_lower and "gas" not in _epa_fuel_lower)
        or "hydrogen" in _dealer_ft_lower
        or _dealer_is_pure_ev
    )


def resolve_electric_drive(src: SpecSources) -> tuple[bool, dict[str, Any]]:
    """``(is_bev, sticker_pkg)``; the parsed Monroney fields are reused downstream.

    Order: feed/catalog signals, Charger Daytona sticker rule, the window
    sticker's own fuel type, then NHTSA vPIC, which outranks every signal above
    (owner rule, vehicle_facts.electrification): a decode saying BEV / FCEV makes
    the car electric-drive; one naming a combustion engine, a hybrid or a plug-in
    hybrid vetoes it.
    """
    from backend.enrichment import knowledge_engine_specs as kes

    is_bev = _feed_and_catalog_say_electric(src)
    try:
        from backend.scanner.window_sticker import dodge_charger_daytona_is_bev

        if dodge_charger_daytona_is_bev(src.car):
            is_bev = True
    except Exception:
        pass

    sticker_pkg = kes._sticker_specs_from_packages(src.car)
    if sticker_pkg.get("fuel_type") == "Electric":
        is_bev = True
    from backend.vehicle_facts.electrification import NO_ENGINE, electrification

    vpic = src.vpic
    _vpic_el = electrification({}, vpic=vpic) if vpic else None
    if _vpic_el in NO_ENGINE:
        is_bev = True
    elif _vpic_el is not None:
        is_bev = False
    return is_bev, sticker_pkg
