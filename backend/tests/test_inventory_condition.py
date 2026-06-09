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
