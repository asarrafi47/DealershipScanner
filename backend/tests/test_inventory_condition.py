"""Inventory condition normalization from Dealer.com JSON."""

from __future__ import annotations

from backend.parsers.inventory_condition import normalize_inventory_condition


def test_certified_from_flag():
    assert normalize_inventory_condition({"certified": True, "newOrUsed": "used"}) == "Certified"


def test_new_from_classification():
    assert normalize_inventory_condition({"classification": "New", "odometer": 12}) == "New"


def test_used_from_mileage():
    assert normalize_inventory_condition({"odometer": 25000}) == "Used"


def test_cpo_from_classification_text():
    assert normalize_inventory_condition({"classificationName": "BMW Certified"}) == "Certified"


def test_missing_odometer_is_unknown_not_new():
    from backend.parsers.inventory_condition import normalize_inventory_condition

    assert normalize_inventory_condition({}, mileage=None) is None
    assert normalize_inventory_condition({"odometer": None}) is None
    assert normalize_inventory_condition({}, mileage=12000) == "Used"
    assert normalize_inventory_condition({}, mileage=5) == "New"
