"""P10C.2: resolve_car_forced_induction and its precedence rule."""
from backend.utils.forced_induction import (
    NA_LABEL, SUPERCHARGED, TURBO, TWIN_TURBO, resolve_car_forced_induction as resolve,
)
from backend.vehicle_facts import precedence as P


def _car(**kw):
    base = {"year": 2022, "make": "Honda", "model": "Accord", "trim": "EX", "fuel_type": "Gasoline"}
    base.update(kw)
    return base


def test_dealer_engine_text_beats_vpic_and_catalog():
    car = _car(engine_description="2.0L Turbo I4")
    assert resolve(car, vpic_boosted=False, family_flags=[NA_LABEL]) == (TURBO, "dealer_feed")


def test_dealer_text_twin_and_supercharged_survive():
    assert resolve(_car(engine_description="Twin Turbo V8")) == (TWIN_TURBO, "dealer_feed")
    assert resolve(_car(engine_description="5.0L Supercharged V8"))[0] == "Supercharged"


def test_vpic_true_typed_by_family():
    assert resolve(_car(), vpic_boosted=True, family_flags=[SUPERCHARGED, SUPERCHARGED]) == (
        SUPERCHARGED, "nhtsa_vpic")


def test_vpic_true_without_family_defaults_to_turbo():
    assert resolve(_car(model="Civic"), vpic_boosted=True) == (TURBO, "nhtsa_vpic")


def test_vpic_false_is_na_even_against_catalog_boost():
    assert resolve(_car(), vpic_boosted=False, family_flags=[TURBO]) == (NA_LABEL, "nhtsa_vpic")


def test_family_unanimous_catalog_label():
    assert resolve(_car(), family_flags=[TURBO, TURBO, ""]) == (TURBO, "epa_catalog")
    assert resolve(_car(), family_flags=[NA_LABEL]) == (NA_LABEL, "epa_catalog")


def test_mixed_family_is_not_evidence():
    assert resolve(_car(model="Camry"), family_flags=[TURBO, NA_LABEL])[1] != "epa_catalog"
    assert resolve(_car(model="Camry"), family_flags=[TURBO, TWIN_TURBO])[1] != "epa_catalog"


def test_description_marker_after_catalog():
    car = _car(model="Camry", description="Loaded with a turbocharged engine.")
    assert resolve(car) == (TURBO, "dealer_text_heuristic")
    assert resolve(car, family_flags=[NA_LABEL]) == (NA_LABEL, "epa_catalog")


def test_heuristic_is_last_resort():
    car = _car(year=2023, make="Ford", model="Mustang", trim="EcoBoost Premium", cylinders=4, engine_l=2.3)
    label, src = resolve(car)
    assert src in ("trim_heuristic", "dealer_feed") and label


def test_nothing_speaks():
    assert resolve(_car(model="Camry", year=2010)) == (None, "unknown")


def test_pure_ev_gets_nothing_even_with_vpic():
    assert resolve(_car(fuel_type="Electric"), vpic_boosted=True) == (None, "unknown")


def test_phev_fuel_is_not_an_ev():
    assert resolve(_car(fuel_type="Plug-In Hybrid"), vpic_boosted=False)[0] == NA_LABEL


def test_precedence_rule_matches_resolver_order():
    r = P.RANKS["forced_induction"]
    assert r[P.DEALER_FEED] > r[P.NHTSA_VPIC] > r[P.EPA_CATALOG] > r[P.DEALER_TEXT] > r[P.TRIM_HEURISTIC]
    assert P.TRIM_HEURISTIC in P.FILL_ONLY["forced_induction"]
    assert P.EPA_CATALOG not in P.FILL_ONLY["forced_induction"]


def test_evidence_overwrites_heuristic_label_but_not_the_reverse():
    prov = {"forced_induction": {"source": "trim_heuristic", "value": TURBO}}
    assert P.decide("forced_induction", TURBO, prov, NA_LABEL, "nhtsa_vpic").write
    prov = {"forced_induction": {"source": "nhtsa_vpic", "value": NA_LABEL}}
    assert not P.decide("forced_induction", NA_LABEL, prov, TURBO, "trim_heuristic").write
    assert not P.decide("forced_induction", NA_LABEL, prov, TURBO, "epa_catalog").write
    assert P.decide("forced_induction", NA_LABEL, prov, TURBO, "dealer_feed").write


def test_heuristic_fills_blank():
    assert P.decide("forced_induction", None, None, TURBO, "trim_heuristic").write
