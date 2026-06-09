"""TCO state and fuel_requirement fields on serialize_car_for_api."""

from __future__ import annotations

from backend.utils.car_serialize import (
    resolve_car_fuel_requirement,
    resolve_car_state_code,
    serialize_car_for_api,
)


def test_resolve_car_state_from_zip() -> None:
    assert resolve_car_state_code({"zip_code": "90210"}) == "CA"


def test_resolve_car_state_from_location_text() -> None:
    assert resolve_car_state_code({"dealer_location": "123 Main St, Raleigh, NC 27601"}) == "NC"


def test_resolve_car_state_defaults_nc() -> None:
    assert resolve_car_state_code({"make": "Honda"}) == "NC"


def test_resolve_car_fuel_requirement_turbo() -> None:
    assert (
        resolve_car_fuel_requirement(
            {"make": "Ford", "engine_description": "2.7L EcoBoost Twin Turbo V6"}
        )
        == "premium"
    )


def test_resolve_car_fuel_requirement_luxury_make() -> None:
    assert resolve_car_fuel_requirement({"make": "BMW", "engine_description": "2.0L I4"}) == "premium"


def test_resolve_car_fuel_requirement_regular() -> None:
    assert resolve_car_fuel_requirement({"make": "Toyota", "engine_description": "2.5L I4"}) == "regular"


def test_serialize_car_for_api_includes_tco_fields() -> None:
    row = {
        "vin": "1HGBH41JXMN109186",
        "year": 2019,
        "make": "Toyota",
        "model": "Camry",
        "zip_code": "27513",
        "engine_description": "2.5L 4-Cylinder",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["state"] == "NC"
    assert out["fuel_requirement"] == "regular"
