"""Regular listing spec completeness (public filter vs dev queue)."""

from __future__ import annotations

import pytest

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


def test_public_incomplete_when_transmission_missing() -> None:
    row = _minimal_row(transmission=None)
    assert "transmission" in listing_missing_field_codes(row, for_public_filter=True)
    assert is_car_incomplete_for_public_listings(row) is True


def test_public_complete_when_optional_engine_gap_only() -> None:
    row = _minimal_row(engine_description=None, cylinders=None)
    pub = listing_missing_field_codes(row, for_public_filter=True)
    assert "engine" not in pub
    assert is_car_incomplete_for_public_listings(row) is False


def test_queue_includes_engine_when_absent() -> None:
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
