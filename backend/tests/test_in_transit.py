"""In-transit availability detection and incomplete listing flag."""

from __future__ import annotations

import json
from unittest.mock import patch

from backend.utils.in_transit import (
    apply_in_transit_flags_from_raw,
    availability_spec_source_patch,
    normalize_availability_status,
    vehicle_is_in_transit,
)
from backend.utils.listing_completeness import listing_missing_field_codes


def test_normalize_availability_status():
    assert normalize_availability_status("In-Transit") == "in_transit"
    assert normalize_availability_status("On Lot") == "on_lot"
    assert normalize_availability_status("") is None


def test_vehicle_is_in_transit_from_algolia_flag():
    car = {"_in_transit": True}
    assert vehicle_is_in_transit(car) is True


def test_vehicle_is_in_transit_from_spec_source_json():
    car = {
        "spec_source_json": json.dumps(
            {"availability": {"in_transit": True, "status": "in_transit", "source": "algolia"}}
        )
    }
    assert vehicle_is_in_transit(car) is True


def test_apply_in_transit_flags_from_raw():
    hit = {"in_transit": "In-Transit", "vin": "1" * 17}
    apply_in_transit_flags_from_raw(hit, source="test")
    assert hit["_in_transit"] is True
    assert hit["_availability_status"] == "in_transit"


def test_availability_spec_source_patch():
    v = {"_in_transit": True, "_availability_source": "dealer_inspire_algolia"}
    patch = availability_spec_source_patch(v)
    assert patch["availability"]["in_transit"] is True
    assert patch["availability"]["status"] == "in_transit"


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_in_transit_adds_on_lot_missing_field(_mock_prep) -> None:
    row = {
        "vin": "WMW33DH03R2U42771",
        "title": "2024 MINI Cooper",
        "year": 2024,
        "make": "MINI",
        "model": "Cooper",
        "trim": "Signature",
        "price": 22973,
        "mileage": 7874,
        "transmission": "Automatic",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "exterior_color": "White",
        "interior_color": "Black",
        "image_url": "https://example.com/1.jpg",
        "gallery": [],
        "body_style": "Coupe",
        "engine_description": "1.5L",
        "condition": "Certified Pre-Owned",
        "cylinders": 3,
        "_in_transit": True,
    }
    missing = listing_missing_field_codes(row, for_public_filter=True)
    assert "on_lot" in missing


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_on_lot_vehicle_not_flagged_in_transit(_mock_prep) -> None:
    row = {
        "vin": "WMW33DH03R2U42771",
        "title": "2024 MINI Cooper",
        "year": 2024,
        "make": "MINI",
        "model": "Cooper",
        "trim": "Signature",
        "price": 22973,
        "mileage": 7874,
        "transmission": "Automatic",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "exterior_color": "White",
        "interior_color": "Black",
        "image_url": "https://example.com/1.jpg",
        "gallery": [],
        "body_style": "Coupe",
        "engine_description": "1.5L",
        "condition": "Certified Pre-Owned",
        "cylinders": 3,
        "spec_source_json": json.dumps(
            {"availability": {"in_transit": False, "status": "on_lot", "source": "algolia"}}
        ),
    }
    missing = listing_missing_field_codes(row, for_public_filter=True)
    assert "on_lot" not in missing
