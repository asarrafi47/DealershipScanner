"""Cross-checks that reject a source before any field chain may rank it.

Each check fails open exactly as the pre-split code did: an import or runtime
error leaves the source in place (or, for vPIC cylinders, NOT trusted).
"""

from __future__ import annotations

from typing import Any

from backend.enrichment.verified_specs._coerce import int_or_none

#: Engine-level catalog keys that a different engine family cannot lend this car.
ENGINE_LEVEL_EPA_KEYS: tuple[str, ...] = (
    "displacement", "cylinders", "engine_description", "city08", "highway08",
    "city_e", "highway_e", "fuel_type", "atv_type", "trany",
)


def text_corrected_dealer_cylinders(car: dict[str, Any]) -> Any:
    """The stored count can be enrichment-written; the engine text was observed.

    "2.7L I4 L3B Turbo" with cylinders=8 renders as an 8-cylinder without this.
    """
    dealer_cyl = car.get("cylinders")
    try:
        from backend.utils.engine_consistency import cylinders_from_engine_text as _cyl_from_text

        _text_cyl = _cyl_from_text(car.get("engine_description"))
        if _text_cyl is not None and int_or_none(dealer_cyl) not in (None, 0, _text_cyl):
            dealer_cyl = _text_cyl
    except Exception:
        pass
    return dealer_cyl


def linked_catalog_row_rejection(car: dict[str, Any], epa_trim: dict[str, Any]) -> str | None:
    """Why a linked catalog row contradicts the dealer's own engine text, or None.

    A rejected row must not feed cylinders / hp / MPG for this car; the caller
    falls back to the fuzzy path, which is anchored on the text-derived count.
    """
    try:
        from backend.utils.engine_consistency import catalog_row_conflicts_with_engine_text

        return catalog_row_conflicts_with_engine_text(
            car.get("engine_description"), epa_trim.get("displacement"), epa_trim.get("cylinders")
        )
    except Exception:
        return None


def vpic_cylinders_ok(car: dict[str, Any], vpic_cyl: int | None) -> bool:
    """Whether the VIN-decoded cylinder count may be trusted.

    VIN decode (vPIC) is the top authority for the cylinder count: it agrees
    with the dealer's engine text on 99.9% of active cars while the stored
    cylinders column disagrees with it on 3,589 (2026-09-21). Only when the
    decode does not contradict the engine text on the listing — vPIC itself
    warns that a decoded model year can be off.
    """
    if vpic_cyl is not None and vpic_cyl >= 0:
        try:
            from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text as _cyl_conflict

            return not _cyl_conflict(vpic_cyl, car.get("engine_description"), car.get("fuel_type"))
        except Exception:
            return False
    return False


def drop_fuzzy_engine_family_mismatch(
    car: dict[str, Any], epa: dict[str, Any], linked_exact: bool
) -> tuple[dict[str, Any], str | None]:
    """The fuzzy path can land on the wrong engine family too (X5 M -> X5 3.0L V6).

    Engine-level catalog values that contradict the listing's own engine text
    are dropped; MPG from a different engine is not this car's MPG. Returns
    ``(epa, reason-or-None)``.
    """
    epa_fuzzy_rejected: str | None = None
    if epa and not linked_exact:
        try:
            from backend.utils.engine_consistency import catalog_row_conflicts_with_engine_text as _row_conf

            epa_fuzzy_rejected = _row_conf(
                car.get("engine_description"), epa.get("displacement"), epa.get("cylinders")
            )
        except Exception:
            epa_fuzzy_rejected = None
        if epa_fuzzy_rejected:
            epa = {k: v for k, v in epa.items() if k not in ENGINE_LEVEL_EPA_KEYS}
    return epa, epa_fuzzy_rejected


def dealer_electric_label_is_plausible(car: dict[str, Any]) -> bool:
    """Dealer "Electric" labels have landed on gas/hybrid cars (GX 550 gas V6 x25).

    Forcing cylinders_display=0 off that label made the engine line read
    "Electric" too — the bad label erased its own disproof. Only let the dealer
    label force BEV treatment when the row's own evidence does not contradict
    it. An unavailable assessor leaves the label trusted.
    """
    try:
        from backend.utils.fuel_label_plausibility import (
            PLAUSIBLE,
            assess_electric_claim,
        )

        if assess_electric_claim(car).verdict != PLAUSIBLE:
            return False
    except Exception:
        pass
    return True
