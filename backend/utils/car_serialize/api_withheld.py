"""
``serialize_car_for_api`` extended-figure policy: which hp / torque / 0-60 / tank /
range figures are shown, the per-VIN vPIC horsepower fill-behind, and the figures
that are always withheld (curb weight, battery kWh, tow) or EV-only.

Split out of ``serialize.serialize_car_for_api`` (2026-10-01) with no behaviour
change; ``backend/tests/test_serialize_car_for_api_golden.py`` pins the output.
"""
from __future__ import annotations

from typing import Any


def resolve_tank_and_range(c: dict[str, Any]) -> tuple[float | None, Any]:
    """``(tank_gallons, factory_epa_range)``, resolved once for the spec list AND the TCO block."""
    from backend.intelligence.ev_range_estimates import resolve_factory_epa_range
    from backend.intelligence.tco_fuel_estimates import resolve_fuel_tank_gallons

    _tank_gal_raw = resolve_fuel_tank_gallons(c)
    _tank_gal = round(_tank_gal_raw, 1) if _tank_gal_raw is not None else None
    _factory_range = resolve_factory_epa_range(c)
    return _tank_gal, _factory_range


def apply_extended_figures(
    out: dict[str, Any], c: dict[str, Any], vs: dict[str, Any], engine_disp: Any
) -> None:
    """Horsepower / torque / 0-60, plus ``horsepower_source`` / ``horsepower_note``."""
    # Extended specs. ``merge_verified_specs`` now admits horsepower / torque /
    # 0-60 only when a page whose title names THIS car's model year printed that
    # exact number (``knowledge_engine_specs.sourced_extended_specs``), so these
    # three are a pass-through of an already-gated value. Note the gate is at the
    # merge, not here, because the AI chat agent reads the merge output directly
    # and never calls this serializer.
    out["horsepower"] = vs.get("horsepower")
    out["torque_lb_ft"] = vs.get("torque_lb_ft")
    out["zero_to_60_sec"] = vs.get("zero_to_60_sec")
    # Fill horsepower behind the suppression, from THIS VIN's own vPIC decode.
    #
    # The gate above blanks hp whenever a (year, make, model) family scraped to
    # one value, and `_extended_family_suspicious` says outright that nothing
    # fills in behind it. That is why cars render with no horsepower: the wrong
    # number is correctly withheld and no right one exists behind it. vPIC is the
    # manufacturer's filing for the individual VIN, so it cannot carry
    # model-level contamination by construction, and 23,089 active listings
    # already have one cached locally. Only ever used when the gated value is
    # absent — a real per-trim figure that survived the gate still wins.
    #
    # Every hp figure carries its source (``horsepower_source``): a gated value
    # came from a page naming this model year ("trim page"), the fill-behind
    # from the VIN decode ("NHTSA vPIC"). On a hybrid the vPIC ``EngineHP`` is
    # the combustion engine alone, so the number stays (it is true to the
    # filing) with ``horsepower_note`` saying what it is not: 145 hp on a CR-V
    # Hybrid whose system makes 204 must never read as the car's output.
    out["horsepower_source"] = "trim page" if out["horsepower"] is not None else None
    out["horsepower_note"] = None
    if out["horsepower"] is None:
        from backend.utils.vpic_specs import hybrid_text, vpic_is_hybrid, vpic_specs_for_vin

        _vp = vpic_specs_for_vin(c.get("vin"))
        _vp_hp = _vp.get("horsepower")
        if _vp_hp is not None:
            out["horsepower"] = _vp_hp
            out["horsepower_source"] = "NHTSA vPIC"
            _is_hybrid = (
                vpic_is_hybrid(_vp)
                or hybrid_text(vs.get("vpic_electrification"))
                or hybrid_text(out.get("fuel_type"))
                or hybrid_text(engine_disp)
            )
            if _is_hybrid:
                out["horsepower_note"] = "engine only; hybrid system output not filed"
    # NOT filled from vPIC: 0-60 (vPIC does not carry it, and estimating it from
    # power-to-weight would need the very curb-weight figures suppressed below)
    # and curb weight (present on 0.2% of decodes).


def withhold_contaminated_figures(out: dict[str, Any]) -> None:
    """Curb weight, battery kWh and tow rating are never shown (no per-trim source)."""
    # curb_weight_lb is suppressed here as well as at the merge, so a caller that
    # hands in its own ``verified_specs`` dict cannot reintroduce it. Measured
    # 2026-08-02 over the 49,912 epa_extended_specs rows: 23,803 carry a curb
    # weight, 2,134 of them under 2,500 lb and 2,042 under 2,100 — lighter than
    # the lightest car sold in the US (2,095 lb Mirage). Every Mazda CX-50 row
    # from 2023 to 2026 stores 2,000 lb for BOTH curb weight and tow capacity;
    # 2,000 is the tow rating and the car weighs about 3,700. The page extraction
    # repeats the same swapped figure, so quoting it proves nothing.
    out["curb_weight_lb"] = None
    # battery_kwh is suppressed, not passed through. epa_extended_specs carries it on 427
    # rows across 12 nameplates, and ALL TWELVE stamp a single value on every trim and
    # model year -- the same model-level contamination as curb weight and 0-60. BMW
    # 7 Series is 14.4 kWh on all 211 rows (the 750e plug-in pack, shown on gas cars);
    # Tesla Model S is 100.0 on all 59, so a 2016 75D reads as a 100 kWh car; Lucid Air
    # 88.0 on 41; i4 70.2 on 33. Pack size is precisely what varies BETWEEN trims, so a
    # nameplate-wide value is never right except by accident, and a shopper reads it as a
    # range/charging-cost proxy. Restore this only from a per-trim source.
    out["battery_kwh"] = None
    # tow_capacity_lb is suppressed for a stronger reason than any of the above:
    # there is no gate that could admit it. 9,864 epa_extended_specs rows carry a
    # towing figure, spread across 2,037 distinct (year, make, model) groups, and
    # the number of those groups holding more than one distinct value is ZERO
    # (counted 2026-08-02). One tow rating per nameplate-year, stamped on every
    # trim — it cannot tell a Tradesman from a Limited, so it describes neither.
    out["tow_capacity_lb"] = None


def apply_tank_and_range(out: dict[str, Any], tank_gal: float | None, factory_range: Any) -> None:
    # Tank and EV range do NOT come from ``vs``. They are the two extended-spec
    # numbers a shopper reads as a running cost — the tank is multiplied by a
    # live fuel price for the cost of a fill-up, the range anchors the battery
    # health readout — so both go through the resolvers that admit only a
    # figure traceable to a document, and return None otherwise.
    #
    # ``vs`` cannot be used for either. ``merge_verified_specs`` builds it with
    # ``lookup_epa_extended_specs``, which ends in ``_merge_ai_model_specs``, so
    # its ``fuel_tank_gal`` may have come out of the AI ``ai_model_specs`` table;
    # and even when it comes from ``epa_extended_specs`` the column is 99.9%
    # body-class and per-model defaults rather than anything the scrape read.
    # Its ``ev_range_miles`` is a total driving range mislabelled as an
    # all-electric one. Both are documented in ``backend/intelligence``.
    #
    # These two keys used to be assigned here and are the second, older shopper
    # path — ``car.fuel_tank_gal`` / ``car.ev_range_miles`` in the car.html spec
    # list, separate from the ``fuel_tank_gallons`` / ``factory_range`` keys the
    # TCO block reads. Same fact, so they are now the same number.
    out["fuel_tank_gal"] = tank_gal
    out["ev_range_miles"] = factory_range


def gate_ev_only_fields(out: dict[str, Any], c: dict[str, Any]) -> None:
    # EV range / battery are ONLY real for battery-electric and plug-in hybrids. The
    # model-level spec match can pull an EV trim's row onto a gas car of the same
    # nameplate (e.g. gas Kona matching Kona Electric), so gate on the car's own fuel
    # type and drop these fields for anything that can't be plugged in.
    _ft = str(out.get("fuel_type") or c.get("fuel_type") or "").strip().lower()
    _ev_capable = ("plug-in" in _ft) or ("plug in" in _ft) or _ft in ("electric", "ev") or (
        "electric" in _ft and "gas" not in _ft and "hybrid" not in _ft
    )
    if not _ev_capable:
        out["ev_range_miles"] = None
        out["battery_kwh"] = None
