"""Regular listing spec completeness (car.html spec sheet vs listings grid)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.utils.car_serialize import DISPLAY_DASH, serialize_car_for_api as real_serialize_car_for_api
from backend.utils.listing_completeness import (
    is_car_incomplete_for_public_listings,
    listing_missing_field_codes,
    summarize_incomplete_missing_fields,
)


def _minimal_row(**kwargs):
    base = {
        "vin": "1HGBH41JXMN109186",
        "title": "2020 Honda Civic LX",
        "year": 2020,
        "make": "Honda",
        "model": "Civic",
        "trim": "LX",
        "price": 22000,
        "mileage": 5000,
        "transmission": "CVT",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "exterior_color": "Crystal Black",
        "interior_color": "Gray",
        "image_url": "https://example.com/hero.jpg",
        "gallery": [],
        "body_style": "Sedan",
        "engine_description": "1.5L turbo I4",
        "condition": "Used",
        "cylinders": 4,
    }
    base.update(kwargs)
    return base


def _serialize_force_condition_unset(*args, **kwargs):
    out = dict(real_serialize_car_for_api(*args, **kwargs))
    out["condition"] = DISPLAY_DASH
    return out


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_public_incomplete_when_transmission_missing(_mock_prep) -> None:
    """Without verified master specs, empty dealer transmission counts as missing."""
    row = _minimal_row(transmission=None)
    assert "transmission" in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is True


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_public_incomplete_when_engine_missing(_mock_prep) -> None:
    row = _minimal_row(engine_description=None, cylinders=None)
    pub = listing_missing_field_codes(row, for_public_filter=True)
    assert "engine" in pub or "cylinders" in pub
    assert is_car_incomplete_for_public_listings(row) is True


@patch(
    "backend.utils.listing_completeness.serialize_car_for_api",
    side_effect=_serialize_force_condition_unset,
)
def test_public_incomplete_when_condition_display_missing(_mock_ser) -> None:
    """Row may infer condition from title/mileage; force display dash to test detection."""
    row = _minimal_row()
    assert "condition" in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is True


def test_public_incomplete_when_mileage_missing() -> None:
    row = _minimal_row(mileage=None)
    assert "mileage" in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is True


def test_public_complete_when_mileage_zero() -> None:
    """New inventory may show 0 mi — same as car.html (not treated as missing)."""
    row = _minimal_row(mileage=0)
    assert "mileage" not in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is False


def test_public_price_set_when_list_price_is_explicit_zero() -> None:
    """``price=0`` in the row is a stored value (not the same as NULL) for public completeness."""
    row = _minimal_row(price=0, msrp=None)
    assert "price" not in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is False


def test_public_price_only_msrp() -> None:
    row = _minimal_row(price=None, msrp=45_000)
    assert "price" not in listing_missing_field_codes(row, for_public_filter=True)


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_queue_includes_engine_when_absent(_mock_prep) -> None:
    row = _minimal_row(engine_description=None, cylinders=None)
    q = listing_missing_field_codes(row, for_public_filter=False)
    assert "engine" in q or "cylinders" in q


def test_summarize_incomplete_missing_fields_sorts_by_count() -> None:
    cars = [
        {"id": 1, "incomplete_missing_fields": ["images", "transmission"]},
        {"id": 2, "incomplete_missing_fields": ["images", "vin"]},
        {"id": 3, "incomplete_missing_fields": ["transmission"]},
    ]
    rows = summarize_incomplete_missing_fields(cars)
    assert [r["code"] for r in rows[:2]] == ["images", "transmission"]
    assert rows[0]["count"] == 2
    assert rows[0]["pct"] == pytest.approx(66.7, rel=0.01)
