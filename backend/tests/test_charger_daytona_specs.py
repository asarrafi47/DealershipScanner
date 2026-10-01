"""Dodge Charger Daytona BEV spec merge from window sticker."""

from __future__ import annotations

import pytest

from backend.db import inventory_pg
from backend.db.inventory_db import get_car_by_id
from backend.enrichment.knowledge_engine import merge_verified_specs
from backend.enrichment.window_sticker_service import _analyze_local_sticker_pdf, window_sticker_local_path
from backend.utils.car_serialize import build_engine_display, serialize_car_for_api


def test_charger_daytona_scat_pack_specs_after_sticker_reparse() -> None:
    # The re-parse below writes the merged packages back to car 3608, so it may
    # only ever run against the SQLite test inventory, never a real database.
    if inventory_pg.is_inventory_postgres():
        pytest.skip("inventory is Postgres; this test writes car 3608 and runs on the SQLite test inventory only")
    car = get_car_by_id(3608)
    if not car:
        pytest.skip("car 3608 (Charger Daytona Scat Pack) absent from the test inventory")
    path = window_sticker_local_path(
        car.get("vin"),
        dealer_id=car.get("dealer_id"),
        dealership_registry_id=car.get("dealership_registry_id"),
    )
    if not path or not path.is_file():
        pytest.skip("window sticker PDF for car 3608 missing locally")
    _analyze_local_sticker_pdf(3608, car, path)
    car2 = get_car_by_id(3608) or car
    vs = merge_verified_specs(car2)
    assert vs.get("cylinders_display") == 0
    assert "MPGe" in (vs.get("fuel_economy_display") or "")
    assert build_engine_display(car2, vs) != "UNITED STATES"
    assert "Electric" in build_engine_display(car2, vs) or "Motor" in build_engine_display(car2, vs)
    ser = serialize_car_for_api(car2, verified_specs=vs)
    assert ser.get("fuel_type") == "Electric"
    assert ser.get("engine_display") != "5.7L V8"
