"""Model-name equivalence across dealer / vPIC / EPA vocabularies (2026-09-23 lab:
31 false 'model differs from VIN decode' rows)."""
from __future__ import annotations

import pytest

from backend.utils.model_aliases import alias_spellings, bmw_series_of, canonical_model, models_equivalent


@pytest.mark.parametrize(
    "make, dealer_model, dealer_trim, other",
    [
        ("Toyota", "Prius Plug-in Hybrid", "XSE", "Prius Prime (PHEV)"),
        ("Toyota", "RAV4 Plug-In Hybrid", "SE", "RAV4 Prime (PHEV)"),
        ("GMC", "Sierra 1500 Limited", "Denali", "Sierra Limited"),
        ("GMC", "Sierra 2500HD", "AT4", "Sierra HD"),
        ("Chevrolet", "Silverado 2500HD", "LTZ", "Silverado HD"),
        ("BMW", "2 Series", "228 xDrive Gran Coupe", "228i"),
        ("BMW", "3 Series", "330i", "330i"),
        ("BMW", "7 Series", "740i", "740i"),
        ("BMW", "4 Series", "M440i", "M440i"),
        ("Honda", "Accord Hybrid", "EX-L", "Accord"),
        ("Toyota", "Camry", "LE", "Camry"),
    ],
)
def test_same_car_line_is_equivalent(make, dealer_model, dealer_trim, other):
    assert models_equivalent(make, dealer_model, dealer_trim, other)


@pytest.mark.parametrize(
    "make, dealer_model, dealer_trim, other",
    [
        ("Toyota", "Camry", "LE", "Corolla"),
        ("BMW", "3 Series", "330i", "X3"),
        ("Honda", "Civic", "Sport", "Accord"),
        ("Toyota", "bZ4X", "XLE", "Solterra"),  # different makes; vPIC splits them by VIN pos 7
    ],
)
def test_different_lines_are_not_equivalent(make, dealer_model, dealer_trim, other):
    assert not models_equivalent(make, dealer_model, dealer_trim, other)


def test_canonical_and_spellings():
    assert canonical_model("Toyota", "Prius Prime") == canonical_model("Toyota", "Prius Plug-in Hybrid")
    assert "Prius Prime" in alias_spellings("Toyota", "Prius Plug-in Hybrid")
    assert "Silverado HD" in alias_spellings("Chevrolet", "Silverado 2500HD")
    assert "Sierra Limited" in alias_spellings("GMC", "Sierra 1500 Limited")
    assert bmw_series_of("M440i") == "4 Series" and bmw_series_of("X5") is None


def test_family_and_code_equivalence_2026_09_26():
    from backend.utils.model_aliases import makes_equivalent, models_equivalent

    assert models_equivalent("Ford", "Super Duty", "XL", "F-350")
    assert models_equivalent("MINI", "Cooper S", "Signature", "Hardtop")
    assert models_equivalent("Mercedes-Benz", "GLC 300", "", "GLC-Class")
    assert models_equivalent("Chevrolet", "Silverado 6500HD", "Work Truck", "GM515")
    assert not models_equivalent("Toyota", "bZ4X", "", "Solterra")
    assert makes_equivalent("Chevrolet", "GM") and makes_equivalent("Wagoneer", "JEEP") and makes_equivalent("RAM", "Dodge")
    assert not makes_equivalent("Toyota", "Subaru")

