"""Canonical fuel_type and body_style preset mapping."""

from __future__ import annotations

import unittest

from backend.utils.field_clean import (
    BODY_STYLE_PRESETS,
    FUEL_TYPE_PRESETS,
    body_styles_for_filter,
    clean_car_row_dict,
    coerce_body_style_stored,
    coerce_fuel_type_stored,
    fuel_types_for_filter,
    normalize_body_style_for_car,
    sort_body_style_presets,
    sort_fuel_type_presets,
)


class FuelBodyPresetsTest(unittest.TestCase):
    def test_fuel_exact_and_dealer_variants(self) -> None:
        cases = {
            "Gasoline Fuel": "Gasoline",
            "Premium Unleaded": "Gasoline",
            "Regular Unleaded": "Gasoline",
            "Gas": "Gasoline",
            "Flex Fuel Capability": "Gasoline",
            "Gasoline/Mild Electric Hybrid": "Hybrid",
            "Full Hybrid Electric (FHEV)": "Hybrid",
            "Performance Plug-In Hybrid": "Plug-In Hybrid",
            "Plug-In Electric/Gas": "Plug-In Hybrid",
            "Diesel Fuel": "Diesel",
            "Battery Electric": "Electric",
            "Hydrogen Fuel Cell": "Hydrogen",
        }
        for raw, want in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(coerce_fuel_type_stored(raw), want)

    def test_body_style_variants(self) -> None:
        cases = {
            "Sport Utility Vehicle": "SUV",
            "4dr Sedan": "Sedan",
            "Crew Cab Pickup": "Truck",
            "Double Cab": "Truck",
            "Crossover Utility Vehicle (CUV)": "Crossover",
            "2dr Coupe": "Coupe",
            "5dr Hatchback": "Hatchback",
            "Truck Double Cab": "Truck",
        }
        for raw, want in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(coerce_body_style_stored(raw), want)

    def test_wrangler_body_style_override(self) -> None:
        self.assertEqual(
            normalize_body_style_for_car(
                "Convertible",
                make="Jeep",
                model="Wrangler",
                title="2024 Jeep Wrangler Sahara",
            ),
            "SUV",
        )

    def test_clean_car_row_dict_normalizes_fuel_and_body(self) -> None:
        row = {
            "vin": "1",
            "make": "Ford",
            "model": "F-150",
            "title": "2024 Ford F-150 XLT",
            "fuel_type": "Premium Unleaded",
            "body_style": "Crew Cab Pickup",
        }
        out = clean_car_row_dict(row)
        self.assertEqual(out.get("fuel_type"), "Gasoline")
        self.assertEqual(out.get("body_style"), "Truck")

    def test_facet_sort_presets_first(self) -> None:
        raw_fuels = ["Diesel Fuel", "Gasoline Fuel", "Electric", "Hybrid Fuel"]
        self.assertEqual(
            sort_fuel_type_presets(raw_fuels),
            ["Gasoline", "Hybrid", "Diesel", "Electric"],
        )
        raw_bodies = ["Truck", "Sedan", "Sport Utility Vehicle", "Coupe"]
        self.assertEqual(
            sort_body_style_presets(raw_bodies),
            ["Sedan", "SUV", "Truck", "Coupe"],
        )

    def test_fuel_filter_expansion_includes_legacy(self) -> None:
        expanded = fuel_types_for_filter("Gasoline")
        self.assertIn("Gasoline", expanded)
        self.assertIn("Premium Unleaded", expanded)

    def test_body_filter_expansion_includes_legacy(self) -> None:
        expanded = body_styles_for_filter("SUV")
        self.assertIn("SUV", expanded)
        self.assertIn("Sport Utility Vehicle", expanded)

    def test_preset_lists_non_empty(self) -> None:
        self.assertGreater(len(FUEL_TYPE_PRESETS), 3)
        self.assertGreater(len(BODY_STYLE_PRESETS), 5)


if __name__ == "__main__":
    unittest.main()
