"""N21.3 (W18, FP14): condition labels at the five dealers the clean-room scan named.

Fixtures are the shapes read from cars_cleanroom (welbornchevroletofrome-com,
toyotasb-com, normreeveshondairvine-com, machaikchevy-com, northparklexus-com)."""
from __future__ import annotations

import pytest

from backend.parsers.inventory_condition import normalize_inventory_condition
from backend.utils.car_serialize.condition import infer_condition_for_storage
from backend.utils.field_clean import clean_car_row_dict
from backend.utils.listing_completeness import listing_missing_field_codes, new_car_high_mileage

VIN = "1GNERGKS5TJ372846"


def _row(**kw):
    return clean_car_row_dict({"vin": VIN, **kw})


# welbornchevroletofrome-com: a 2027 Equinox "New" at 19,383 mi, and used GM cars.
def test_welborn_new_over_1000_flagged_not_relabelled():
    r = _row(condition="New", mileage=19383, year=2027)
    assert r["condition"] == "New" and "_condition_flag" not in r
    assert new_car_high_mileage({"condition": "New", "mileage": 19383})
    assert new_car_high_mileage({"condition": "New", "mileage": 2684})


def test_welborn_carbravo_is_used_and_cpo_carbravo_certified():
    assert _row(condition="CarBravo", mileage=30000)["condition"] == "Used"
    r = _row(condition="CarBravo", is_cpo=1, mileage=30000)
    assert r["condition"] == "Certified" and r["is_cpo"] == 1


# toyotasb-com: 708 new at 0 mi; used at 10 mi stays Used from the feed label.
def test_toyotasb_new_at_zero_not_flagged_and_low_mile_used_stays_used():
    assert not new_car_high_mileage({"condition": "New", "mileage": 0})
    assert normalize_inventory_condition({"classification": "Used", "odometer": 10}) == "Used"
    assert normalize_inventory_condition({"classification": "New", "odometer": 0}) == "New"


# normreeveshondairvine-com: Used at 0 mi stays Used; new arrival status is not New.
def test_normreeves_status_new_arrival_does_not_make_used_car_new():
    assert normalize_inventory_condition({"status": "New Arrival", "odometer": 30000}) == "Used"
    assert normalize_inventory_condition({"status": "New Arrival", "odometer": 250}) is None
    assert normalize_inventory_condition({"status": "Used", "odometer": 0}) == "Used"
    assert _row(condition="Used", mileage=0, year=2027)["condition"] == "Used"


# machaikchevy-com: CTP loaners (New, miles legitimate), unlabelled over-1000 New flagged.
def test_machaik_ctp_loaner_not_flagged_but_plain_new_is():
    ctp = _row(condition="CTP", mileage=3835, year=2026)
    assert ctp["condition"] == "New" and ctp["_condition_program"] == "CTP"
    assert not new_car_high_mileage({"condition": "CTP", "mileage": 3835})
    assert not new_car_high_mileage({"condition": "Demo", "mileage": 4000})
    assert new_car_high_mileage({"condition": "New", "mileage": "3,835"})


# northparklexus-com: new at 60-397 mi is a normal new car; used 16k stays used.
@pytest.mark.parametrize("mi", [60, 62, 133, 397, 1000])
def test_northpark_low_mile_new_not_flagged(mi):
    assert not new_car_high_mileage({"condition": "New", "mileage": mi})


def test_inventory_condition_whole_word_matching():
    assert normalize_inventory_condition({"classification": "Renewed"}) is None
    assert normalize_inventory_condition({"classification": "Unused"}) is None
    assert normalize_inventory_condition({"classification": "Premium"}) is None
    assert normalize_inventory_condition({"classification": "Pre-Owned"}) == "Used"
    assert normalize_inventory_condition({"classification": "New"}) == "New"
    assert normalize_inventory_condition({"status": "Certified Pre-Owned"}) == "Certified"


def test_new_inventory_url_with_delivery_miles_is_new_not_used():
    car = {"title": "Lexus RX", "year": 2026, "mileage": 133,
           "source_url": "https://x.com/new-inventory/index.htm"}
    assert infer_condition_for_storage(car) == "New"
    car["source_url"] = "https://x.com/used-inventory/vin-1.htm"
    assert infer_condition_for_storage(car) == "Used"
    car["source_url"] = "https://x.com/new-inventory/index.htm"
    car["mileage"] = 19383
    assert infer_condition_for_storage(car) == "Used"


def test_high_mileage_new_is_a_flag_not_an_incomplete_code():
    car = {"vin": VIN, "condition": "New", "mileage": 19383, "year": 2027, "make": "Chevrolet",
           "model": "Equinox", "price": 30000}
    assert "mileage" not in listing_missing_field_codes(car, for_public_filter=False)
