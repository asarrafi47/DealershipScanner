"""Section 7 of docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md: the incomplete tally
and verify_accuracy count only fixable gaps. Section 2 exemptions: EV engine +
cylinders, msrp on used rows and on feeds that never publish it, allocation
VINs, mileage on New rows, trim when the decode names one. Raw counts stay
available under *_raw."""
from __future__ import annotations

from backend.scripts.scan_lab_report import (
    NO_MSRP_PROVIDERS,
    fixable_missing,
    is_allocation_vin,
    msrp_expected,
)


def test_allocation_vin_shapes():
    assert is_allocation_vin("JTEABFAJ9V136AJ97")      # letters in serial positions 13-17, Toyota WMI
    assert is_allocation_vin("4T1DAACK1VU42O521")      # O in the serial
    assert is_allocation_vin("A08-41048-00067")        # a stock number stored as vin (mymetrohonda)
    assert not is_allocation_vin("4T1DAACK7TU358044")  # real Toyota VIN
    assert not is_allocation_vin("WBA43DA00SCU34030")  # BMW: letters in the serial are not the Toyota pattern
    assert not is_allocation_vin("")


def test_ev_engine_and_cylinders_exempt():
    car = {"vin": "1GYKPMRL5RZ100001", "fuel_type": "Electric", "condition": "New"}
    fixable, exempt = fixable_missing(car, None, ["engine", "cylinders", "interior_color"])
    assert fixable == ["interior_color"]
    assert exempt == {"engine": "ev", "cylinders": "ev"}
    # vPIC says BEV even when the feed label is blank
    car = {"vin": "1GYKPMRL5RZ100001", "fuel_type": None, "condition": "Used"}
    fixable, exempt = fixable_missing(car, {"ElectrificationLevel": "BEV (Battery Electric Vehicle)", "FuelTypePrimary": "Electric"},
                                      ["engine", "fuel_type"])
    assert fixable == ["fuel_type"] and exempt == {"engine": "ev"}


def test_allocation_vin_exempts_vpic_derived_and_stock_but_not_colour():
    car = {"vin": "JTEABFAJ9V136AJ97", "fuel_type": "Gasoline", "condition": "New"}
    fixable, exempt = fixable_missing(car, None, ["engine", "transmission", "drivetrain", "stock_number", "exterior_color", "price"])
    assert fixable == ["exterior_color", "price"]
    assert set(exempt) == {"engine", "transmission", "drivetrain", "stock_number"}
    assert all(v == "allocation_vin" for v in exempt.values())


def test_mileage_on_new_and_trim_with_vpic_trim():
    car = {"vin": "4T1DAACK7TU358044", "fuel_type": "Hybrid", "condition": "New"}
    fixable, exempt = fixable_missing(car, {"Trim": "XSE"}, ["mileage", "trim", "body_style"])
    assert fixable == ["body_style"]
    assert exempt["mileage"] == "new_row" and exempt["trim"].startswith("vpic_trim:XSE")
    used = {"vin": "4T1DAACK7TU358044", "fuel_type": "Hybrid", "condition": "Used"}
    fixable, exempt = fixable_missing(used, {"Trim": "", "Series": ""}, ["mileage", "trim"])
    assert fixable == ["mileage", "trim"] and exempt == {}


def test_msrp_expected_rules():
    assert msrp_expected({"vin": "4T1DAACK7TU358044", "condition": "New"}, "dealer_dot_com")
    assert msrp_expected({"vin": "4T1DAACK7TU358044", "condition": "CTP"}, None)
    assert not msrp_expected({"vin": "4T1DAACK7TU358044", "condition": "Used"}, "dealer_dot_com")
    assert not msrp_expected({"vin": "4T1DAACK7TU358044", "condition": "Certified"}, "dealer_dot_com")
    assert not msrp_expected({"vin": "JTEABFAJ9V136AJ97", "condition": "New"}, "dealer_dot_com")
    for hint in NO_MSRP_PROVIDERS:
        assert not msrp_expected({"vin": "4T1DAACK7TU358044", "condition": "New"}, hint)


def test_verify_accuracy_surfaces_raw_and_fixable(monkeypatch):
    from backend.scripts import dealer_pipeline as dp

    fake = {"rows": 10, "vpic_cached": 9, "incomplete_rows": 2, "missing": {"trim": 2},
            "incomplete_rows_raw": 7, "missing_raw": {"engine": 5, "trim": 2}, "missing_exempt": {"engine:ev": 5},
            "msrp_expected_rows": 4, "msrp_missing_rows": 1,
            "discrepancies": {"vpic_drive": 1, "catalog_trim": 3}, "discrepancy_examples": {"vpic_drive": [{"vin": "x"}]},
            "car_issues": [{"disc": {"vpic_drive": "a"}}, {"disc": {"catalog_trim": "b"}}]}
    monkeypatch.setattr("backend.scripts.scan_lab_report.tally_dealer", lambda conn, d, s: fake)
    out = dp.verify_accuracy(None, "d", "2026-09-28T00:00:00Z")
    assert out["incomplete_rows"] == 2 and out["missing"] == {"trim": 2}
    assert out["incomplete_rows_raw"] == 7 and out["missing_raw"] == {"engine": 5, "trim": 2}
    assert out["missing_exempt"] == {"engine:ev": 5}
    assert out["msrp_expected_rows"] == 4 and out["msrp_missing_rows"] == 1
    assert out["hard"] == {"vpic_drive": 1} and out["hard_rows"] == 1
