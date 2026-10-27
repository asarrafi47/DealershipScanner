"""Body style corrections (Jeep Wrangler → SUV)."""
from __future__ import annotations

from backend.enrichment.knowledge_engine import decode_trim_logic, merge_verified_specs
from backend.utils.car_serialize import serialize_car_for_api, serialize_car_for_listings_grid
from backend.utils.field_clean import is_jeep_wrangler_car, normalize_body_style_for_car
from backend.utils.spec_field_normalize import collect_raw_spec_heuristic_updates


def test_is_jeep_wrangler_car() -> None:
    assert is_jeep_wrangler_car("Jeep", "Wrangler", "Rubicon", "2024 Jeep Wrangler Rubicon")
    assert is_jeep_wrangler_car("Jeep", "Wrangler Unlimited", "Sahara", "")
    assert is_jeep_wrangler_car("Wrangler", "Wrangler", "", "")
    assert not is_jeep_wrangler_car("Jeep", "Gladiator", "Rubicon", "2024 Jeep Gladiator")
    assert not is_jeep_wrangler_car("Ford", "Mustang", "GT", "")


def test_normalize_body_style_wrangler_to_suv() -> None:
    assert (
        normalize_body_style_for_car(
            "Convertible",
            make="Jeep",
            model="Wrangler",
            trim="Unlimited Sahara",
            title="2022 Jeep Wrangler Unlimited Sahara",
        )
        == "SUV"
    )


def test_decode_trim_logic_wrangler_body_style_hint() -> None:
    hints = decode_trim_logic("Jeep", "Wrangler Unlimited", "Sahara", "2022 Jeep Wrangler Unlimited")
    assert hints.get("body_style_hint") == "SUV"


def test_serialize_car_wrangler_body_suv() -> None:
    car = {
        "make": "Jeep",
        "model": "Wrangler",
        "trim": "Rubicon",
        "title": "2023 Jeep Wrangler Rubicon",
        "body_style": "Convertible",
        "year": 2023,
        "price": 45000,
        "mileage": 12000,
    }
    ser = serialize_car_for_api(car, include_verified=False, verified_specs={})
    assert ser.get("body_style") == "SUV"


def test_listings_grid_wrangler_body_suv() -> None:
    car = {
        "id": 1,
        "make": "Jeep",
        "model": "Wrangler Unlimited",
        "trim": "Sport S",
        "title": "2021 Jeep Wrangler Unlimited Sport S",
        "body_style": "Convertible",
        "year": 2021,
        "price": 35000,
        "mileage": 40000,
        "fuel_type": "Gasoline",
        "drivetrain": "4WD",
    }
    out = serialize_car_for_listings_grid(car)
    assert out.get("body_style") == "SUV"


def test_collect_heuristic_repairs_wrangler_body_style() -> None:
    patch = collect_raw_spec_heuristic_updates(
        {
            "make": "Jeep",
            "model": "Wrangler",
            "trim": "Willys",
            "title": "2020 Jeep Wrangler Willys",
            "body_style": "Convertible",
        }
    )
    assert patch.get("body_style") == "SUV"


def test_merge_verified_specs_wrangler_body_suv() -> None:
    car = {
        "make": "Jeep",
        "model": "Wrangler",
        "trim": "Rubicon",
        "title": "2024 Jeep Wrangler Rubicon",
        "body_style": "Convertible",
        "vin": "",
    }
    vs = merge_verified_specs(car)
    assert vs.get("body_style_display") == "SUV"


def test_vpic_bodyclass_vocabulary_folds_to_presets() -> None:
    from backend.utils.field_clean import BODY_STYLE_PRESETS, canonical_body_style_or_none

    cases = {
        "Sedan/Saloon": "Sedan",
        "Hatchback/Liftback/Notchback": "Hatchback",
        "Convertible/Cabriolet": "Convertible",
        "Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)": "SUV",
        "Crossover Utility Vehicle (CUV)": "Crossover",
        "Pickup": "Truck",
        "Extended Cab": "Truck",
        "Cargo Van": "Van",
        "Sportback": "Hatchback",
        "Estate": "Wagon",
    }
    for raw, want in cases.items():
        assert canonical_body_style_or_none(raw) == want, raw
        assert want in BODY_STYLE_PRESETS
    assert canonical_body_style_or_none("Bus") is None
    assert canonical_body_style_or_none(None) is None


def test_is_canonical_body_style() -> None:
    from backend.utils.field_clean import is_canonical_body_style

    assert is_canonical_body_style("SUV")
    assert not is_canonical_body_style("Sport Utility")
    assert not is_canonical_body_style("")


def test_inventory_repair_catalog_respells_noncanonical_body(monkeypatch) -> None:
    from backend.utils import inventory_repair as ir

    monkeypatch.setattr(ir, "merge_verified_specs", lambda raw: {"body_style_display": "Wagon"})
    out = ir.collect_merge_spec_storage_updates({"id": 1, "body_style": "Mid-Size Estate Thing"})
    assert out["body_style"] == "Wagon"
    monkeypatch.setattr(ir, "merge_verified_specs", lambda raw: {"body_style_display": "Free Text"})
    assert "body_style" not in ir.collect_merge_spec_storage_updates({"id": 1, "body_style": "Mid-Size Estate Thing"})
