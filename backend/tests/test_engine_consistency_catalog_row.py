"""Linked catalog rows must not contradict the dealer's own engine text."""

from __future__ import annotations

import pytest

from backend.utils.engine_consistency import (
    catalog_row_conflicts_with_engine_text,
    liters_from_engine_text,
)


@pytest.mark.parametrize(
    "text,expect",
    [
        ("2.7L I4 L3B Turbo", 2.7),
        ("2.5-Liter 4-Cylinder Hybrid Engine", 2.5),
        ("3.5 L V6", 3.5),
        ("Intercooled Turbo Premium Unleaded I-4 2.0 L/122", 2.0),
        ("Electric", None),
        ("24V DOHC", None),  # a valve count is not a displacement
        ("", None),
        (None, None),
    ],
)
def test_liters_from_engine_text(text, expect):
    assert liters_from_engine_text(text) == expect


def test_catalog_row_conflict_on_displacement():
    why = catalog_row_conflicts_with_engine_text("2.7L I4 L3B Turbo", 5.3, 8)
    assert why and why.startswith("displacement 2.7L vs catalog 5.3L")


def test_catalog_row_conflict_on_cylinders_only():
    why = catalog_row_conflicts_with_engine_text("3.0L V6", 3.0, 8)
    assert why == "cylinders 6 vs catalog 8"


def test_catalog_row_agrees():
    assert catalog_row_conflicts_with_engine_text("2.0L I4 K20C2", 2.0, 4) is None
    assert catalog_row_conflicts_with_engine_text("2.0L I4", "2.0", "4") is None


def test_catalog_row_conflict_ignores_missing_evidence():
    assert catalog_row_conflicts_with_engine_text("V6", 3.5, 6) is None
    assert catalog_row_conflicts_with_engine_text("", 3.5, 6) is None
    assert catalog_row_conflicts_with_engine_text("3.5L V6", None, None) is None
    assert catalog_row_conflicts_with_engine_text("3.5L V6", "n/a", "x") is None
