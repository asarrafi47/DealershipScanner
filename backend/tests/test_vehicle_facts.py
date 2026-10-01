"""
Golden / parity tests for ``backend.vehicle_facts`` (monolith audit 2026-10-01,
enrich.md P1 #3): one drivetrain normalizer, one electrification classifier, one
listing-model -> EPA-model module (one named list per consumer), one ``epa_extended_specs`` reader.

The drivetrain table below is every distinct value found in local Postgres on
2026-10-01 (``cars.drivetrain``, vPIC ``DriveType`` in ``nhtsa_vpic_cache``,
``epa_master.drive``) plus the fixture strings of the older tests, with the
answer the single source now gives. Every old entry point must agree with it.
Hermetic: no database (the extended-spec reader is exercised with a fake conn).
"""
from __future__ import annotations

import pytest

from backend.vehicle_facts import (
    BEV,
    FCEV,
    HEV,
    ICE,
    PHEV,
    electrification,
    epa_model_candidates,
    fuel_label_electrification,
    normalize_drivetrain,
)
from backend.vehicle_facts import extended_specs
from backend.vehicle_facts.drivetrain import drivetrain_storage_value
from backend.vehicle_facts.electrification import vpic_electrification

# (raw, source="dealer", source="vpic", source="epa", cars.drivetrain storage value)
_REAL_DRIVE_VALUES = [
    ('', None, None, None, None),
    ('2-Wheel Drive', None, None, None, '2WD'),
    ('2WD', None, None, None, '2WD'),
    ('2WD RWD', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('2WD/4WD', None, None, None, '2WD/4WD'),
    ('2wd', None, None, None, '2WD'),
    ('4 WD', '4WD', '4WD', '4WD', '4WD'),
    ('4-Wheel Drive', '4WD', '4WD', '4WD', '4WD'),
    ('4-Wheel or All-Wheel Drive', '4WD', '4WD', '4WD', '4WD'),
    ('4MATIC', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('4MATIC all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('4MATIC®', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('4MOTION all-wheel', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('4MOTION w/Active Control all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('4WD', '4WD', '4WD', '4WD', '4WD'),
    ('4WD 4WD', '4WD', '4WD', '4WD', '4WD'),
    ('4WD/4-Wheel Drive/4x4', '4WD', '4WD', '4WD', '4WD'),
    ('4WD; Dual Rear Wheels', '4WD', '4WD', '4WD', '4WD'),
    ('4X2', None, None, None, '2WD'),
    ('4X4', '4WD', '4WD', '4WD', '4WD'),
    ('4x2', None, None, None, '2WD'),
    ('4x2/2-Wheel Drive', None, None, None, '2WD'),
    ('4x2; Dual Rear Wheels', None, None, None, '2WD'),
    ('4x4', '4WD', '4WD', '4WD', '4WD'),
    ('4x4 with Part Time Selectable Engagement', '4WD', '4WD', '4WD', '4WD'),
    ('A', 'AWD', None, None, 'AWD'),
    ('ALL4', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AWD', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AWD (4MATIC)', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AWD (quattro)', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AWD (xDrive)', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AWD 4MATIC®', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AWD/All-Wheel Drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Advanced 4WD', '4WD', '4WD', '4WD', '4WD'),
    ('Advanced 4x4', '4WD', '4WD', '4WD', '4WD'),
    ('Advanced 4x4 with Automatic On Demand Engagement', '4WD', '4WD', '4WD', '4WD'),
    ('All Wheel Drive 4MATIC', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('All-Wheel Drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('AllWheelDrive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Expedition® 4X2', None, None, None, '2WD'),
    ('Dual Rear Wheel', None, None, None, 'Dual Rear Wheel'),  # reviewer: singular DRW was read as RWD
    ('Expedition® 4X4', '4WD', '4WD', '4WD', '4WD'),
    ('F', 'FWD', None, None, 'FWD'),
    ('FOUR_WHEEL_DRIVE', '4WD', '4WD', '4WD', '4WD'),
    ('FRONT_WHEEL_DRIVE', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('FT4WD', '4WD', '4WD', '4WD', '4WD'),
    ('FWD', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('FWD / AWD', None, None, None, 'FWD / AWD'),
    ('FWD/Front-Wheel Drive', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('FWDP', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('Four-Wheel Drive', '4WD', '4WD', '4WD', '4WD'),
    ('FourWheelDrive', '4WD', '4WD', '4WD', '4WD'),
    ('Front-Wheel Drive', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('Front-Wheel Drive (FWD)', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('Front-Wheel Drive Plus', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('FrontTrak', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('FrontWheelDrive', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('Full-time 4-Wheel Drive', '4WD', '4WD', '4WD', '4WD'),
    ('HTRAC all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Intelligent AWD', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('M xDrive all-wheel', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('N / A', None, None, None, 'N / A'),
    ('N/A', None, None, None, None),
    ('NA', None, None, None, None),
    ('NA 4MATIC', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Not Applicable', None, None, None, 'Not Applicable'),
    ('Oth', None, None, None, 'Oth'),
    ('Other', None, None, None, None),
    ('Other drive systems', None, None, None, 'Other drive systems'),
    ('PT4WD', '4WD', '4WD', '4WD', '4WD'),
    ('Part-time 4-Wheel Drive', '4WD', '4WD', '4WD', '4WD'),
    ('Quattro all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Quattro ultra all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('R', 'RWD', None, None, 'RWD'),
    ('REAR_WHEEL_DRIVE', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('RWD', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('RWD/Rear-Wheel Drive', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('Rear-Wheel Drive', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('RearWheelDrive', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('Super Handling All-Wheel Drive&trade; (SH-AWD&reg;)', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Symmetrical All-Wheel Drive all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('Unknown', None, None, None, None),
    ('XDrive all-wheel drive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ("['All-Wheel Drive']", 'AWD', 'AWD', 'AWD', 'AWD'),
    ('all-wheel', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('eAWD', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('http://schema.org/AllWheelDriveConfiguration', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('https://schema.org/FourWheelDriveConfiguration', '4WD', '4WD', '4WD', '4WD'),
    ('https://schema.org/FrontWheelDriveConfiguration', 'FWD', 'FWD', 'FWD', 'FWD'),
    ('https://schema.org/RearWheelDriveConfiguration', 'RWD', 'RWD', 'RWD', 'RWD'),
    ('other', None, None, None, None),
    ('quattro', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('xDrive', 'AWD', 'AWD', 'AWD', 'AWD'),
    ('xDrive all-wheel', 'AWD', 'AWD', 'AWD', 'AWD'),
]


@pytest.mark.parametrize("raw,dealer,vpic,epa,stored", _REAL_DRIVE_VALUES)
def test_single_source_answers(raw, dealer, vpic, epa, stored):
    assert normalize_drivetrain(raw, "dealer") == dealer
    assert normalize_drivetrain(raw, "vpic") == vpic
    assert normalize_drivetrain(raw, "epa") == epa
    from backend.utils.field_clean import coerce_drivetrain_stored

    assert coerce_drivetrain_stored(raw) == stored


@pytest.mark.parametrize("raw", [r[0] for r in _REAL_DRIVE_VALUES])
def test_every_old_entry_point_delegates(raw):
    """The five old normalizers now return the single source's answer."""
    from backend.catalog.resolver import _drive_bucket
    from backend.enrichment.knowledge_engine import _norm_drive_epa, _normalize_vpic_response
    from backend.enrichment.nhtsa_vpic import _normalize_drivetrain, flat_vpic_result_to_car_patch
    from backend.enrichment.vpic_facts import _drive_bucket as vf_bucket

    assert _drive_bucket(raw) == (normalize_drivetrain(raw, "dealer") or "")
    assert vf_bucket(raw) == (normalize_drivetrain(raw, "dealer") or "")
    assert _normalize_vpic_response({"DriveType": raw})["drivetrain"] == normalize_drivetrain(raw, "vpic")
    assert _normalize_drivetrain(raw) == normalize_drivetrain(raw, "vpic")
    assert flat_vpic_result_to_car_patch({"DriveType": raw}).get("drivetrain") == normalize_drivetrain(raw, "vpic")
    assert _norm_drive_epa(raw) == (normalize_drivetrain(raw, "epa") if raw else None)


@pytest.mark.parametrize(
    "raw", ["4x2", "4X2", "2WD", "2wd", "4x2/2-Wheel Drive", "2-Wheel Drive", "Expedition® 4X2", "4x2; Dual Rear Wheels"]
)
def test_4x2_is_never_an_end(raw):
    """Owner rule: 4x2 / 2WD say two driven wheels, not which end."""
    from backend.utils.analytics_ep import _norm_drivetrain
    from backend.utils.spec_field_normalize import drivetrain_from_blob

    for src in ("dealer", "vpic", "epa", "text"):
        assert normalize_drivetrain(raw, src) is None
    assert drivetrain_from_blob(f"New 2026 Toyota Tacoma SR5 {raw}") is None
    assert drivetrain_storage_value(raw) == "2WD"  # the dealer's claim, never FWD/RWD
    assert _norm_drivetrain(raw) == "2WD"  # was FWD for "2wd"


def test_ambiguous_and_explicit_end():
    assert normalize_drivetrain("2WD/4WD", "vpic") is None  # vPIC Ridgeline decode: was 4WD
    assert normalize_drivetrain("FWD / AWD") is None
    assert normalize_drivetrain("2WD RWD") == "RWD"
    assert normalize_drivetrain("4x4") == "4WD"  # was AWD in the storage table
    assert normalize_drivetrain("New 2026 Toyota Tundra Platinum 4x4", "text") == "4WD"
    assert normalize_drivetrain("2026 Buick Encore GX Allure", "text") is None


@pytest.mark.parametrize("raw,expected", [
    ("2 Wheel Drive - Rear", "RWD"),
    ("2 Wheel Drive (Rear)", "RWD"),
    ("2WD Front", "FWD"),
    ("4x2 Front", "FWD"),
    ("2WD RWD", "RWD"),
])
def test_two_wheel_with_an_explicit_end_is_honoured(raw, expected):
    for src in ("dealer", "vpic", "epa", "text"):
        assert normalize_drivetrain(raw, src) == expected, src
    assert drivetrain_storage_value(raw) == expected
    from backend.vehicle_facts.drivetrain import is_two_wheel_unknown

    assert not is_two_wheel_unknown(raw)


@pytest.mark.parametrize("raw", ["Dual Rear Wheels", "Dual Rear Wheel", "4x2; Dual Rear Wheels", "Dual Rear Wheels 2WD"])
def test_dual_rear_wheels_is_not_rwd(raw):
    for src in ("dealer", "vpic", "epa", "text"):
        assert normalize_drivetrain(raw, src) != "RWD"


def test_scanner_trim_inference_delegates():
    from backend.scanner.database import _infer_drivetrain_from_trim as infer

    # Same answers as the old inline copy...
    assert infer("Platinum 4x4", "2026 Toyota Tundra") == "4WD"
    assert infer("SE AWD", None) == "AWD"
    assert infer("Limited FWD", None) == "FWD"
    assert infer("Allure", "2026 Buick Encore GX Allure") is None
    assert infer("SR5 4x2", None) is None
    # ...and the parity run's changed answers (6,000 real cars: 184 changed, all None -> value)
    assert infer("xDrive40i", "2027 BMW X7 xDrive40i") == "AWD"  # fused badge (was None)
    assert infer("sDrive28i", "2019 BMW X1 sDrive28i") == "RWD"  # fused badge (was None)
    assert infer("SR5", "2025 Toyota Tacoma SR5 Truck Double Cab Four Wheel Drive") == "4WD"  # was None
    assert infer("DRW", "2024 Ram 3500 Dual Rear Wheel") is None  # was RWD


# ---------------------------------------------------------------------------
# Electrification
# ---------------------------------------------------------------------------
_RAW_BEV = {"ElectrificationLevel": "BEV (Battery Electric Vehicle)", "FuelTypePrimary": "Electric", "FuelTypeSecondary": ""}
_RAW_PHEV = {"ElectrificationLevel": "PHEV (Plug-in Hybrid Electric Vehicle)", "FuelTypePrimary": "Electric", "FuelTypeSecondary": "Gasoline"}
_RAW_HEV = {"ElectrificationLevel": "Strong HEV (Hybrid Electric Vehicle)", "FuelTypePrimary": "Gasoline", "FuelTypeSecondary": "Electric"}
_RAW_MILD = {"ElectrificationLevel": "Mild HEV (Hybrid Electric Vehicle)", "FuelTypePrimary": "Gasoline", "FuelTypeSecondary": "Electric"}
_RAW_GAS = {"ElectrificationLevel": "", "FuelTypePrimary": "Gasoline", "FuelTypeSecondary": ""}
_RAW_FCEV = {"ElectrificationLevel": "FCEV (Fuel Cell Electric Vehicle)", "FuelTypePrimary": "Fuel Cell", "FuelTypeSecondary": ""}


@pytest.mark.parametrize(
    "fuel,expected",
    [
        ("Electric", BEV), ("Electricity", BEV), ("Electric Fuel System", BEV),
        ("Plug-In Hybrid", PHEV), ("Electric with Gas Generator", PHEV),
        ("Premium Gasoline / Electricity", PHEV),
        ("Hybrid", HEV), ("Gas/Electric Hybrid", HEV), ("Gasoline / Electric", HEV),
        ("Electric / Gasoline", HEV), ("Gasoline/Mild Electric Hybrid", HEV), ("HYB", HEV),
        ("Gasoline", ICE), ("Diesel", ICE), ("Flex Fuel Capability", ICE), ("G", ICE), ("UNL", ICE),
        ("Hydrogen", FCEV),
        ("Direct Injection", None), ("Other", None), ("", None), (None, None),
    ],
)
def test_fuel_labels(fuel, expected):
    assert fuel_label_electrification(fuel) == expected


def test_vpic_outranks_the_feed():
    # Real rows from the 2026-10-01 sample: dealer label vs decode.
    assert electrification({"fuel_type": "Hybrid"}, vpic=_RAW_PHEV) == PHEV  # BMW X5 xDrive50e
    assert electrification({"fuel_type": "Electric"}, vpic=_RAW_GAS) == ICE  # 2023 Silverado 1500 5.3L
    assert electrification({"fuel_type": "Gasoline"}, vpic=_RAW_BEV) == BEV  # 2027 Cadillac Optiq
    assert electrification({"fuel_type": "Hybrid"}, vpic=_RAW_GAS) == HEV  # decode silent on hybrid
    assert electrification({"fuel_type": "Gasoline"}, vpic=_RAW_MILD) == ICE  # mild: no override
    assert electrification({"fuel_type": None}, vpic=_RAW_MILD) == HEV
    assert electrification({}, vpic=_RAW_FCEV) == FCEV
    assert electrification({"fuel_type": "Electric", "vpic_electrification": "phev"}) == PHEV


def test_catalog_never_outranks_the_dealer():
    assert electrification({"fuel_type": "Gasoline"}, catalog={"atv_type": "EV"}) == ICE
    assert electrification({"fuel_type": None}, catalog={"atv_type": "EV"}) == BEV


def test_vpic_level_mapping_keeps_legacy_codes():
    from backend.enrichment.knowledge_engine import _normalize_vpic_response

    assert _normalize_vpic_response(_RAW_BEV)["electrification"] == "ev"
    assert _normalize_vpic_response(_RAW_PHEV)["electrification"] == "phev"
    assert _normalize_vpic_response(_RAW_HEV)["electrification"] == "hybrid"
    assert _normalize_vpic_response(_RAW_MILD)["electrification"] is None  # 48V: no override
    assert _normalize_vpic_response(_RAW_FCEV)["electrification"] == "fcev"  # was None
    assert vpic_electrification(_RAW_GAS) is None


def test_old_detectors_delegate():
    from backend.enrichment.generated_spec_sheet import _can_be_plugged_in
    from backend.enrichment.knowledge_engine_specs import _is_battery_electric
    from backend.enrichment.spec_backfill import _is_ev_row
    from backend.enrichment.spec_structured_backfill import _is_ev_fuel_hint
    from backend.intelligence.ev_range_estimates import _is_electrified_car
    from backend.utils.engine_consistency import is_bev_fuel
    from backend.utils.vpic_specs import vpic_is_hybrid

    hybrid = {"fuel_type": "Gas/Electric Hybrid"}
    assert not _is_electrified_car(hybrid)  # was True: a hybrid has no battery-only range
    assert not _can_be_plugged_in(hybrid, {})
    assert not _is_ev_row(hybrid) and not _is_ev_fuel_hint(hybrid)  # was True
    assert _is_ev_fuel_hint({"fuel_type": "Hydrogen"})  # FCEV: no cylinders (was False)
    assert _is_battery_electric({"fuel_type": "Electric"}, {})
    assert not _is_battery_electric({"fuel_type": "Electric"}, {"vpic_electrification": "hybrid"})
    assert _can_be_plugged_in({"fuel_type": "Hybrid"}, {"vpic_electrification": "phev"})
    assert is_bev_fuel("Electric") and not is_bev_fuel("Electric / Gasoline")
    assert vpic_is_hybrid({"electrification_level": "Mild HEV (Hybrid Electric Vehicle)"})
    assert not vpic_is_hybrid(
        {"electrification_level": "BEV (Battery Electric Vehicle)", "fuel_type_secondary": "Electric"}
    )  # 2025 BMW i7 decode: was True


def test_resolver_text_classifier_unchanged():
    from backend.catalog.resolver import _electrification

    assert _electrification("2024 Toyota RAV4 Prime XSE") == "phev"
    assert _electrification("x", fuel_text="Premium Gasoline / Electricity") == "phev"
    assert _electrification("F-150 PowerBoost") == "hybrid"
    assert _electrification("Ioniq 5 Electric") == "ev"
    assert _electrification("Premium AWD", fuel_text="Electricity") == ""


# ---------------------------------------------------------------------------
# Listing model -> EPA model
# ---------------------------------------------------------------------------
# Each consumer gets exactly the list it had before phase 4 (golden: the reviewer's
# 5,000 by-trim/aggregate rows, 8,422 fetch_epa_rows triples and 2,380 make/models
# all equal commit b1578241d except the guards below).
_BY_TRIM_EXPECTED = [
    # (make, model, list): the pre-merge ``_model_epa_fallbacks`` answers
    ("BMW", "M2", []),                                   # merged list added "M" -> 4.4L V8 AWD
    ("BMW", "M2 Competition", ["M2"]),                   # merged list added "M" -> V8
    ("BMW", "840i", ["8 Series"]),
    ("BMW", "428i Gran Coupe", ["428i Gran", "428i"]),   # merged list added "4 Series" -> 6 cyl
    ("BMW", "430i Gran Coupe", ["430i Gran", "430i"]),   # merged "4 Series" -> AWD
    ("BMW", "M440i", ["4 Series"]),
    ("BMW", "335i xDrive", ["335i"]),                    # merged "3 Series" -> RWD
    ("MINI", "Hardtop 2 Door", ["Hardtop 2", "Hardtop"]),  # merged "Cooper" -> 1.5L 3-cyl; Cooper SE got a gas engine
    ("MINI", "Convertible", []),
    ("Porsche", "Macan Electric", ["Macan"]),            # merged EV guard lost the correct EV row
    ("Mercedes-Benz", "CLA 350 Electric", ["CLA-Class", "CLA-Class", "CLA 350", "CLA"]),  # ditto
    ("Volvo", "C40 Recharge", ["C40"]),                  # ditto
    ("Volvo", "C40 Recharge Pure Electric", ["C40 Recharge Pure", "C40 Recharge", "C40"]),
]


@pytest.mark.parametrize("make,model,expected", _BY_TRIM_EXPECTED)
def test_trim_lookups_get_their_pre_merge_list(make, model, expected):
    from backend.enrichment.knowledge_engine import _model_epa_fallbacks

    assert epa_model_candidates(make, model, strategy="by_trim") == expected
    assert epa_model_candidates(make, model, strategy="aggregate") == expected
    assert _model_epa_fallbacks(make, model) == expected  # legacy view unchanged


@pytest.mark.parametrize("make,model,dropped", [
    ("Kia", "Niro EV", "Niro"),                                  # gas Niro rows on an EV
    ("Volvo", "XC40 Recharge Pure Electric", "XC40"),            # gas XC40 rows on an EV
    ("Chevrolet", "Silverado 1500 HD", "Silverado"),            # light-duty specs on an HD
])
def test_trim_lookup_guards_keep_the_confirmed_fixes(make, model, dropped):
    from backend.enrichment.knowledge_engine import _model_epa_fallbacks

    assert dropped in _model_epa_fallbacks(make, model)  # the raw rule still sheds...
    assert dropped not in epa_model_candidates(make, model, strategy="aggregate")  # ...the lookup never uses it
    assert dropped not in epa_model_candidates(make, model, strategy="by_trim")


@pytest.mark.parametrize("make,model,expected", [
    ("BMW", "M2", ["M2", "M"]),
    ("BMW", "428i Gran Coupe", ["428i Gran Coupe", "4 Series"]),
    ("MINI", "Hardtop 2 Door", ["Hardtop 2 Door", "Cooper"]),
    ("Audi", "S5 Sedan", ["S5 Sedan", "A4"]),  # ladder FAMILY label, as before (known, left)
    # EV / plug-in nameplates never got their gas sibling's rows here
    ("Ford", "Mustang Mach-E", ["Mustang Mach-E"]),
    ("Cadillac", "Escalade IQ", ["Escalade IQ"]),
    ("Jeep", "Wrangler 4xe", ["Wrangler 4xe"]),
    ("Jeep", "Grand Cherokee 4xe", ["Grand Cherokee 4xe"]),
    ("Kia", "Niro EV", ["Niro EV"]),
    # HD: the separator no longer sheds to the light-duty name (confirmed fix)
    ("Chevrolet", "Silverado 2500 HD", ["Silverado 2500 HD"]),
    ("Chevrolet", "Silverado 1500 HD", ["Silverado 1500 HD"]),
    ("GMC", "Sierra 3500 HD Chassis Cab", ["Sierra 3500 HD Chassis Cab"]),
    ("Chevrolet", "Silverado 1500", ["Silverado 1500", "Silverado"]),
])
def test_fetch_rows_list(make, model, expected):
    from backend.enrichment.epa_master_store import _model_search_variants

    assert epa_model_candidates(make, model, strategy="fetch_rows") == expected
    assert _model_search_variants(make, model) == expected


@pytest.mark.parametrize("make,model,expected", [
    ("Chevrolet", "Equinox EV", ["Equinox EV", "Equinox"]),  # EPA files it as Equinox / "EV FWD"
    ("Kia", "Niro EV", ["Niro EV", "Niro"]),  # the resolver scores powertrain on each row
    ("BMW", "M2", ["M2"]),
    ("Audi", "S5 Sedan", ["S5 Sedan", "S5"]),
    ("", "", [""]),
])
def test_catalog_list(make, model, expected):
    from backend.catalog.resolver import _model_variants

    assert epa_model_candidates(make, model, strategy="catalog") == expected
    assert _model_variants(make, model) == expected


def test_model_specs_list():
    from backend.enrichment.model_specs_dictionary import iter_model_lookup_variants

    assert epa_model_candidates("Hyundai", "Elantra Hybrid", strategy="model_specs") == ["Elantra Hybrid", "elantra"]
    assert iter_model_lookup_variants("Elantra Hybrid") == ["Elantra Hybrid", "elantra"]


def test_strategy_is_required_and_checked():
    with pytest.raises(TypeError):
        epa_model_candidates("BMW", "M2")  # type: ignore[call-arg]
    with pytest.raises(ValueError):
        epa_model_candidates("BMW", "M2", strategy="merged")


# --- aggregate / by-trim lookups end to end, on an in-memory epa_master -------
_EPA_COLS = ("year", "make", "model", "trim", "cylinders", "drive", "trany", "displacement", "city08",
             "highway08", "city_e", "highway_e", "fuel_type", "atv_type", "body_style", "engine_description")
_EPA_ROWS = [
    # BMW "M" series: V8 AWD M5/M8 rows outnumber the I6
    (2025, "BMW", "M", "M5", 8, "All-Wheel Drive", "Automatic (S8)", 4.4, 15, 21, None, None, "Premium Gasoline", "Hybrid", "Sedan", ""),
    (2025, "BMW", "M", "M8", 8, "All-Wheel Drive", "Automatic (S8)", 4.4, 15, 21, None, None, "Premium Gasoline", None, "Coupe", ""),
    (2025, "BMW", "M", "M8 Gran", 8, "All-Wheel Drive", "Automatic (S8)", 4.4, 15, 21, None, None, "Premium Gasoline", None, "Sedan", ""),
    # MINI "Cooper": 1.5L 3-cyl gas rows
    (2023, "MINI", "Cooper", "Hardtop 2 Door", 3, "Front-Wheel Drive", "Automatic (S7)", 1.5, 28, 38, None, None, "Premium Gasoline", None, "", ""),
    (2023, "MINI", "Cooper", "Hardtop 4 Door", 3, "Front-Wheel Drive", "Automatic (S7)", 1.5, 28, 38, None, None, "Premium Gasoline", None, "", ""),
    # Porsche "Macan": EV row filed under the base model
    (2025, "Porsche", "Macan", "Electric", None, "All-Wheel Drive", "Automatic (A1)", None, 98, 87, 34.0, 38.0, "Electricity", "EV", "", ""),
    # Kia "Niro": hybrid gas rows (wrong for a Niro EV)
    (2024, "Kia", "Niro", "FE", 4, "Front-Wheel Drive", "Automatic (AM6)", 1.6, 53, 54, None, None, "Regular Gasoline", "Hybrid", "", ""),
    # Chevrolet "Silverado": light-duty rows (wrong for an HD)
    (2007, "Chevrolet", "Silverado", "1500", 8, "4-Wheel Drive", "Automatic 4-spd", 5.3, 14, 19, None, None, "Regular Gasoline", "FFV", "", ""),
]


@pytest.fixture
def epa_db(monkeypatch):
    import sqlite3

    from backend.enrichment import knowledge_engine as ke

    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE epa_master ({', '.join(_EPA_COLS)})")
    conn.executemany(f"INSERT INTO epa_master VALUES ({', '.join('?' * len(_EPA_COLS))})", _EPA_ROWS)

    class _Keep:
        def __getattr__(self, name):
            return getattr(conn, name)

        def close(self):
            pass

    monkeypatch.setattr(ke, "_conn", lambda: _Keep())
    yield ke
    conn.close()


@pytest.mark.parametrize("year,make,model,trim", [
    (2025, "BMW", "M2", ""),                    # was 4.4L V8 AWD via "M"
    (2023, "MINI", "Hardtop 2 Door", "Cooper SE"),  # an EV: was a 1.5L gas 3-cyl via "Cooper"
    (2023, "MINI", "Convertible", "Cooper S"),  # was 1.5L 3-cyl via "Cooper"
    (2024, "Kia", "Niro EV", "Wind"),           # confirmed fix: was gas Niro rows
    (2007, "Chevrolet", "Silverado 1500 HD", "LT1"),  # confirmed fix: was light-duty specs
])
def test_aggregate_and_by_trim_do_not_borrow_another_nameplate(epa_db, year, make, model, trim):
    agg = epa_db._lookup_epa_aggregate_uncached(year, make, model, trim=trim)
    assert agg["cylinders"] is None and agg["displacement"] is None and agg["fuel_type"] is None
    assert not (epa_db._lookup_epa_by_trim_uncached(year, make, model, trim) or {}).get("cylinders")
    # The rows are there: the series/base name the merged list fell back to answers.
    sibling = {"M2": "M", "Hardtop 2 Door": "Cooper", "Convertible": "Cooper", "Niro EV": "Niro",
               "Silverado 1500 HD": "Silverado"}[model]
    assert epa_db._lookup_epa_aggregate_uncached(year, make, sibling, trim=trim)["cylinders"]


def test_aggregate_keeps_the_ev_row_filed_under_the_base_model(epa_db):
    agg = epa_db._lookup_epa_aggregate_uncached(2025, "Porsche", "Macan Electric", trim="Base")
    assert agg["atv_type"] == "EV" and agg["fuel_type"] == "Electricity" and agg["cylinders"] is None


# ---------------------------------------------------------------------------
# epa_extended_specs: one reader, one cache
# ---------------------------------------------------------------------------
class _Cur:
    def __init__(self, log, row):
        self.log, self.row = log, row

    def execute(self, sql, params=None):
        self.log.append(sql)

    def fetchone(self):
        return self.row


class _Conn:
    def __init__(self, log, row):
        self.log, self.row = log, row

    def cursor(self):
        return _Cur(self.log, self.row)

    def close(self):
        pass


def test_three_consumers_share_one_cached_row(monkeypatch):
    import json

    from backend.enrichment import generated_spec_sheet as gss
    from backend.enrichment import knowledge_engine as ke
    from backend.intelligence import tco_fuel_estimates as tco

    payload = {"pages": [{"url": "https://example.test/x", "extracted": {"fuel_tank_gal": 15.8}}]}
    row = tuple(
        {"horsepower": 300, "fuel_tank_gal": 15.8, "year": 2021, "make": "Honda", "model": "Accord",
         "trim": "Sport", "specs_json": json.dumps(payload)}.get(c)
        for c in extended_specs.COLUMNS
    )
    log: list[str] = []
    monkeypatch.setattr("backend.db.inventory_db.get_conn", lambda: _Conn(log, row))
    extended_specs.clear_cache()
    try:
        car = {"year": 2021, "make": "Honda", "model": "Accord", "trim": "Sport"}
        assert gss._extended_row_for_car(car)[0] == 300
        assert tco._quoted_specs_for_car(car)["fuel_tank_gal"][0] == 15.8
        assert ke.lookup_epa_extended_specs(2021, "Honda", "Accord", "Sport")["fuel_tank_gal"] == 15.8
        row_reads = [s for s in log if s.startswith("SELECT horsepower")]
        assert len(row_reads) == 1, row_reads  # one SELECT served all three
        assert all("epa_extended_specs" in s for s in log)
    finally:
        extended_specs.clear_cache()


def test_reader_error_is_not_permission_to_render(monkeypatch):
    from backend.enrichment import generated_spec_sheet as gss

    def _boom():
        raise RuntimeError("down")

    monkeypatch.setattr("backend.db.inventory_db.get_conn", _boom)
    extended_specs.clear_cache()
    try:
        assert extended_specs.row_by_master_id(5) is None
        assert extended_specs.fields_shared_across_trims(2021, "Honda", "Accord", ("horsepower",)) is None
        assert gss._fields_shared_across_trims(2021, "Honda", "Accord") == frozenset(gss._ATTRIBUTABLE_SPEC_FIELDS)
    finally:
        extended_specs.clear_cache()
