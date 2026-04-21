from __future__ import annotations

from backend.db.inventory_db import is_dummy_placeholder_vin


def test_dummy_vin_numeric_suffix() -> None:
    assert is_dummy_placeholder_vin("VIN001")
    assert is_dummy_placeholder_vin("vin016")
    assert not is_dummy_placeholder_vin("1HGBH41JXMN109185")


def test_dummy_vin_xxx_substring() -> None:
    assert is_dummy_placeholder_vin("ABCVINXXXZZZ")
    assert is_dummy_placeholder_vin("VINXXX")
    assert is_dummy_placeholder_vin("VINXXXX")


def test_dummy_vin_x_only_suffix() -> None:
    assert is_dummy_placeholder_vin("VINXX")


def test_real_vin_not_dummy() -> None:
    assert not is_dummy_placeholder_vin("")
    assert not is_dummy_placeholder_vin(None)
    assert not is_dummy_placeholder_vin("   ")
