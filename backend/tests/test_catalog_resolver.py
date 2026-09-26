"""
Catalog resolver scoring/normalization — every case here is a real miss
pattern measured in the 2026-08 unlinked-fleet audit (27,756 active cars with
no epa_master link, ~72% of them MY2026/2027).
"""

from __future__ import annotations

from backend.catalog.resolver import (
    MIN_CONFIDENCE,
    _drive_bucket,
    _electrification,
    _match_models,
    _model_variants,
    resolve_from_candidates,
    score_candidate,
)
from backend.enrichment.knowledge_engine import _model_epa_fallbacks


def _epa(id_, trim, cyl, disp, drive, fuel, atv, model="X", year=2026):
    return {
        "id": id_, "year": year, "make": "", "model": model, "trim": trim,
        "cylinders": cyl, "displacement": disp, "trany": None, "drive": drive,
        "fuel_type": fuel, "engine_description": None, "forced_induction": None,
        "atv_type": atv,
    }


# --- drive bucketing: EPA spells out "Four-Wheel Drive"; dealers say "4WD" ---

def test_drive_bucket_epa_four_wheel_drive() -> None:
    assert _drive_bucket("Four-Wheel Drive") == "4WD"
    assert _drive_bucket("4WD") == "4WD"
    assert _drive_bucket("All-Wheel Drive") == "AWD"
    assert _drive_bucket("Rear-Wheel Drive") == "RWD"


def test_2026_tundra_gas_links_via_drive_agreement() -> None:
    # 1,788 unlinked 2026 Tundras sat at 0.45 because "Four-Wheel Drive"
    # never bucketed, dropping the drive point.
    car = {
        "year": 2026, "make": "Toyota", "model": "TUNDRA", "trim": "Limited",
        "cylinders": 6, "engine_l": None, "engine_description": "3.4L V6 V35A-FTS",
        "drivetrain": "4WD", "fuel_type": "Gasoline", "title": "2026 Toyota Tundra Limited",
    }
    cand = _epa(19946, "4WD", 6, 3.4, "Four-Wheel Drive", "Regular Gasoline", None, model="Tundra")
    score, method = score_candidate(car, cand)
    assert score >= MIN_CONFIDENCE + 0.05
    assert "drive" in method


# --- electrification classification ---

def test_epa_phev_fuel_string_is_phev_not_ev() -> None:
    # BMW X5 xDrive50e: fuel "Premium Gasoline / Electricity", atv "EV" —
    # classified 'ev' before, conflicting with the car's 'phev'.
    assert _electrification("xdrive50e premium gasoline / electricity ev") == "phev"
    assert _electrification("4dr 4xe regular gasoline / electricity ev") == "phev"
    assert _electrification("electricity ev") == "ev"


def test_x5_phev_resolves_to_50e_not_40i() -> None:
    car = {
        "year": 2026, "make": "BMW", "model": "X5 PHEV", "trim": "xDrive50e",
        "cylinders": 6, "engine_l": None, "engine_description": "3L",
        "drivetrain": "AWD", "fuel_type": "Plug-In Hybrid", "title": "2026 BMW X5 PHEV",
    }
    cands = [
        _epa(19108, "xDrive40i", 6, 3.0, "All-Wheel Drive", "Premium Gasoline", None, model="X5"),
        _epa(19111, "xDrive50e", 6, 3.0, "All-Wheel Drive", "Premium Gasoline / Electricity", "EV", model="X5"),
    ]
    match = resolve_from_candidates(car, cands)
    assert match is not None and match.epa_master_id == 19111


def test_f150_inc_boilerplate_does_not_poison_electrification() -> None:
    # Real 2026 F-150 STX 2.7L: engine_description carries "-inc:" option
    # boilerplate naming the "3.5L PowerBoost full hybrid" — the gas truck was
    # scored variant_conflict against every conventional row.
    car = {
        "year": 2026, "make": "Ford", "model": "F150", "trim": "STX",
        "cylinders": 6, "engine_l": "2.7",
        "engine_description": (
            "Engine: 2.7L V6 EcoBoost -inc: auto start-stop technology, "
            "Available 3.5L Ecoboost (998) and 3.5L PowerBoost full hybrid (99D)"
        ),
        "drivetrain": "RWD", "fuel_type": "Gasoline", "title": "2026 Ford F-150 STX",
    }
    gas = _epa(19296, "Pickup 2WD", 6, 2.7, "Rear-Wheel Drive", "Regular Gasoline", None, model="F150")
    hev = _epa(40190, "Pickup 4WD HEV", 6, 3.5, "Four-Wheel Drive", "Regular Gasoline", "Hybrid", model="F150")
    match = resolve_from_candidates(car, [hev, gas])
    assert match is not None and match.epa_master_id == 19296
    _, method = score_candidate(car, gas)
    assert "variant_conflict" not in method


# --- mild hybrids: EPA atv="Hybrid" on 48V BMWs / eTorque Rams ---

def test_2027_530i_links_despite_mild_hybrid_atv() -> None:
    # 130 unlinked 2027 530i: only EPA row is '530i xDrive Sedan' atv=Hybrid
    # while the dealer says Gasoline; model+trim combo carries the match.
    car = {
        "year": 2027, "make": "BMW", "model": "530I", "trim": "xDrive",
        "cylinders": 4, "engine_l": None, "engine_description": "2L",
        "drivetrain": "AWD", "fuel_type": "Gasoline", "title": "2027 BMW 530i xDrive",
    }
    c530 = _epa(26307, "530i xDrive Sedan", 4, 2.0, "All-Wheel Drive", "Premium Gasoline", "Hybrid", model="5 Series", year=2027)
    c540 = _epa(26308, "540i xDrive Sedan", 6, 3.0, "All-Wheel Drive", "Premium Gasoline", "Hybrid", model="5 Series", year=2027)
    match = resolve_from_candidates(car, [c540, c530])
    assert match is not None and match.epa_master_id == 26307
    assert "trim_model_combo" in match.method


def test_gas_tundra_still_prefers_conventional_twin_over_hybrid_row() -> None:
    # The softened penalty must not let a gas car outrank its conventional
    # twin ("a gas Corolla must never get hybrid MPG").
    car = {
        "year": 2026, "make": "Toyota", "model": "TUNDRA", "trim": "SR5",
        "cylinders": 6, "engine_l": None, "engine_description": "3.4L V6",
        "drivetrain": "4WD", "fuel_type": "Gasoline", "title": "2026 Toyota Tundra SR5",
    }
    gas = _epa(19946, "4WD", 6, 3.4, "Four-Wheel Drive", "Regular Gasoline", None, model="Tundra")
    hyb = _epa(67654, "Hybrid 4WD", 6, 3.4, "Four-Wheel Drive", "Regular Gasoline", "Hybrid", model="Tundra")
    match = resolve_from_candidates(car, [hyb, gas])
    assert match is not None and match.epa_master_id == 19946


def test_phev_car_still_conflicts_with_plain_hybrid_rows() -> None:
    # 751 unlinked 2026 "RAV4 Plug-In Hybrid": EPA has only hybrid rows for
    # 2026 so far — the PHEV must STAY unlinked, not borrow hybrid MPG.
    car = {
        "year": 2026, "make": "Toyota", "model": "RAV4 Plug-In Hybrid", "trim": "XSE",
        "cylinders": 4, "engine_l": None, "engine_description": "2.5L",
        "drivetrain": "AWD", "fuel_type": "Plug-In Hybrid", "title": "2026 Toyota RAV4 Plug-In Hybrid XSE",
    }
    hyb = _epa(67625, "AWD Limited & XSE", 4, 2.5, "All-Wheel Drive", "Regular Gasoline", "Hybrid", model="RAV4")
    assert resolve_from_candidates(car, [hyb]) is None


# --- EV powertrain agreement + ambiguity guard ---

def test_ev_with_distinct_trim_links() -> None:
    # 2026 Toyota bZ "Limited" AWD (998 unlinked "BZ" cars).
    car = {
        "year": 2026, "make": "Toyota", "model": "BZ", "trim": "Limited",
        "cylinders": 0, "engine_l": None, "engine_description": "AC synchronous electric generator",
        "drivetrain": "AWD", "fuel_type": "Electric", "title": "2026 Toyota bZ Limited",
    }
    awd = _epa(19954, "AWD", None, None, "All-Wheel Drive", "Electricity", "EV", model="bZ")
    awd_ltd = _epa(19955, "AWD LIMITED", None, None, "All-Wheel Drive", "Electricity", "EV", model="bZ")
    match = resolve_from_candidates(car, [awd, awd_ltd])
    assert match is not None and match.epa_master_id == 19955


def test_ev_tie_between_different_variants_stays_unlinked() -> None:
    # 2023 BMW i4, trim=None, RWD: eDrive35 vs eDrive40 tie — different
    # range/hp, ambiguous, must not link.
    car = {
        "year": 2023, "make": "BMW", "model": "I4", "trim": None,
        "cylinders": 0, "engine_l": None, "engine_description": None,
        "drivetrain": "RWD", "fuel_type": "Electric", "title": "2023 BMW i4",
    }
    e35 = _epa(16051, "eDrive35 Gran Coupe (18 inch Wheels)", None, None, "Rear-Wheel Drive", "Electricity", "EV", model="i4", year=2023)
    e40 = _epa(16050, "eDrive40 Gran Coupe (18 inch wheels)", None, None, "Rear-Wheel Drive", "Electricity", "EV", model="i4", year=2023)
    assert resolve_from_candidates(car, [e35, e40]) is None


def test_ev_tie_with_duplicate_row_shielding_different_variant_stays_unlinked() -> None:
    # Duplicate EPA rows: scored[1] shares the best's trim (so a second-only
    # guard passes), but a DIFFERENT variant sits at scored[2] inside the tie
    # window — still ambiguous, must not link.
    car = {
        "year": 2023, "make": "BMW", "model": "I4", "trim": None,
        "cylinders": 0, "engine_l": None, "engine_description": None,
        "drivetrain": "RWD", "fuel_type": "Electric", "title": "2023 BMW i4",
    }
    e35a = _epa(16051, "eDrive35 Gran Coupe (18 inch Wheels)", None, None, "Rear-Wheel Drive", "Electricity", "EV", model="i4", year=2023)
    e35b = _epa(16052, "eDrive35 Gran Coupe (18 inch Wheels)", None, None, "Rear-Wheel Drive", "Electricity", "EV", model="i4", year=2023)
    e40 = _epa(16050, "eDrive40 Gran Coupe (18 inch wheels)", None, None, "Rear-Wheel Drive", "Electricity", "EV", model="i4", year=2023)
    assert resolve_from_candidates(car, [e35a, e35b, e40]) is None


def test_ev_single_variant_for_drivetrain_links() -> None:
    # 2026 Honda Prologue AWD (397 unlinked): drive splits the variants.
    car = {
        "year": 2026, "make": "Honda", "model": "Prologue", "trim": None,
        "cylinders": 0, "engine_l": None, "engine_description": "X0C+EC5",
        "drivetrain": "AWD", "fuel_type": "Electric", "title": "2026 Honda Prologue",
    }
    fwd = _epa(1, "FWD", None, None, "Front-Wheel Drive", "Electricity", "EV", model="Prologue")
    awd = _epa(2, "AWD", None, None, "All-Wheel Drive", "Electricity", "EV", model="Prologue")
    match = resolve_from_candidates(car, [fwd, awd])
    assert match is not None and match.epa_master_id == 2


# --- model-name normalization ---

def test_heavy_duty_prefix_never_inherits_light_duty_model() -> None:
    # 374 unlinked SILVERADO 2500HD must NOT fall through to EPA 'Silverado'
    # (which is the 1500 — EPA has no >8500-GVWR trucks).
    rows = [_epa(1, "4WD", 8, 6.2, "Four-Wheel Drive", "Regular Gasoline", None, model="Silverado")]
    assert _match_models(rows, "Chevrolet", "SILVERADO 2500HD") == []
    assert _match_models(rows, "Chevrolet", "SILVERADO 1500") == rows


def test_silverado_ev_never_links_to_gas_silverado() -> None:
    # The " ev" model-suffix strip may surface gas-base candidates, but the
    # electrification conflict must keep them below the link floor.
    car = {
        "year": 2026, "make": "Chevrolet", "model": "Silverado EV", "trim": "RST",
        "cylinders": 0, "engine_l": None, "engine_description": "Dual Electric Motors",
        "drivetrain": "4WD", "fuel_type": "Electric", "title": "2026 Chevrolet Silverado EV RST",
    }
    gas = _epa(1, "4WD", 8, 6.2, "Four-Wheel Drive", "Regular Gasoline", None, model="Silverado")
    assert resolve_from_candidates(car, [gas]) is None


def test_hd_fallbacks_removed_from_knowledge_engine() -> None:
    assert "Silverado" in _model_epa_fallbacks("Chevrolet", "Silverado 1500")
    assert "Silverado" not in _model_epa_fallbacks("Chevrolet", "Silverado 2500HD")
    assert "Sierra" in _model_epa_fallbacks("GMC", "Sierra 1500")
    assert "Sierra" not in _model_epa_fallbacks("GMC", "Sierra 2500HD")


def test_lexus_phev_long_form_model_suffix() -> None:
    # 127 unlinked "NX PLUG-IN HYBRID ELECTRIC VEHICLE" — too short for the
    # prefix tier (norm 'nx' < 4 chars), needs explicit suffix stripping.
    assert "NX" in _model_variants("Lexus", "NX PLUG-IN HYBRID ELECTRIC VEHICLE")
    assert "ES" in _model_variants("Lexus", "ES HYBRID")


# --- engine size / cylinders read from the dealer's engine text ---------------

def test_liters_from_engine_text_breaks_cylinders_only_tie() -> None:
    # 2026-09-21: 2.7L I4 Silverados linked to the 5.3L V8 row on cylinders+drive
    # alone because engine_l was NULL and the stored cylinders column said 8.
    car = {
        "year": 2026, "make": "Chevrolet", "model": "SILVERADO 1500", "trim": "RST",
        "cylinders": 8, "engine_l": None, "engine_description": "2.7L I4 L3B Turbo",
        "drivetrain": "4WD", "fuel_type": "Gasoline", "title": "2026 Chevrolet Silverado 1500 RST",
    }
    v8 = _epa(1, "4WD  (Flex Fuel)", 8, 5.3, "Four-Wheel Drive", "Regular Gasoline", None, model="Silverado 1500")
    i4 = _epa(2, "4WD", 4, 2.7, "Four-Wheel Drive", "Regular Gasoline", None, model="Silverado 1500")
    s8, m8 = score_candidate(car, v8)
    s4, m4 = score_candidate(car, i4)
    assert s4 > s8
    assert "engine_l" in m4 and "cylinders" in m4 and "row_cyl_overridden" in m4
    assert "engine_l_conflict" in m8 and "cylinders_conflict" in m8
    match = resolve_from_candidates(car, [v8, i4])
    assert match is not None and match.epa_master_id == 2


def test_engine_l_column_still_wins_when_present() -> None:
    car = {
        "year": 2023, "make": "BMW", "model": "X5", "trim": "xDrive40i",
        "cylinders": None, "engine_l": "3.0", "engine_description": "3.0L",
        "drivetrain": "AWD", "fuel_type": "Gasoline", "title": "2023 BMW X5 xDrive40i",
    }
    m50 = _epa(1, "M50i", 8, 4.4, "All-Wheel Drive", "Premium Gasoline", None, model="X5")
    i6 = _epa(2, "xDrive40i", 6, 3.0, "All-Wheel Drive", "Premium Gasoline", None, model="X5")
    assert score_candidate(car, i6)[0] > score_candidate(car, m50)[0]


def test_vin_decode_outranks_dealer_drivetrain_and_fuel(monkeypatch) -> None:
    """2026-09-23 lab: Ridgeline listed 'FWD' (VIN says AWD) and Accord Hybrid
    listed 'Gasoline' (VIN says strong hybrid) each pulled the wrong catalog row."""
    import backend.catalog.resolver as r

    monkeypatch.setattr(
        "backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
        lambda vin: {"drivetrain": "AWD", "electrification": "hybrid", "fuel_type": "Gasoline"},
    )
    car = {"vin": "1HGCY2F63TA066331", "year": 2026, "make": "Honda", "model": "Accord",
           "trim": "EX-L Hybrid", "drivetrain": "FWD", "fuel_type": "Gasoline",
           "engine_description": "2.0L I4", "cylinders": 4}
    gas = _epa(1, "EX-L", 4, 1.5, "Front-Wheel Drive", "Regular Gasoline", None, model="Accord")
    hyb = _epa(2, "Hybrid AWD", 4, 2.0, "All-Wheel Drive", "Regular Gasoline", "Hybrid", model="Accord")
    m = resolve_from_candidates(car, [gas, hyb])
    assert m is not None and m.epa_master_id == 2
    fixed = r.apply_vin_facts(car)
    assert fixed["drivetrain"] == "AWD" and fixed["fuel_type"] == "Hybrid"
    assert car["drivetrain"] == "FWD", "input must not be mutated"


def test_blank_vin_decode_changes_nothing(monkeypatch) -> None:
    import backend.catalog.resolver as r

    monkeypatch.setattr(
        "backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
        lambda vin: {"drivetrain": None, "electrification": None, "fuel_type": None},
    )
    car = {"vin": "1HGCY2F63TA066331", "drivetrain": "FWD", "fuel_type": "Gasoline"}
    assert r.apply_vin_facts(car) == car


def test_drive_bucket_treats_two_wheel_drive_as_no_signal() -> None:
    from backend.catalog.resolver import _drive_bucket

    assert _drive_bucket("4x2/2-Wheel Drive") == ""
    assert _drive_bucket("2WD") == ""
    assert _drive_bucket("4WD/4-Wheel Drive/4x4") == "4WD"
    assert _drive_bucket("Front-Wheel Drive") == "FWD"
