"""Regression: regex/mapping spec normalization for inventory repair."""

from __future__ import annotations

import unittest

from backend.utils.spec_field_normalize import (
    collect_raw_spec_heuristic_updates,
    extract_cylinder_count,
    infer_trim_from_name_title,
    normalize_drivetrain_from_fields,
    normalize_engine_description_storage,
)


class SpecFieldNormalizeTest(unittest.TestCase):
    def test_drivetrain_aliases(self) -> None:
        self.assertEqual(normalize_drivetrain_from_fields("All Wheel Drive"), "AWD")
        self.assertEqual(normalize_drivetrain_from_fields("All-Wheel Drive"), "AWD")
        self.assertEqual(normalize_drivetrain_from_fields("A"), "AWD")
        self.assertEqual(normalize_drivetrain_from_fields("4x4"), "AWD")
        self.assertEqual(normalize_drivetrain_from_fields("Front Wheel Drive"), "FWD")
        self.assertEqual(normalize_drivetrain_from_fields("F"), "FWD")
        self.assertEqual(normalize_drivetrain_from_fields("4x2"), "FWD")
        self.assertEqual(normalize_drivetrain_from_fields("Rear Wheel Drive"), "RWD")
        self.assertEqual(normalize_drivetrain_from_fields("R"), "RWD")
        self.assertEqual(normalize_drivetrain_from_fields("4WD"), "4WD")
        self.assertEqual(normalize_drivetrain_from_fields("Four Wheel Drive"), "4WD")

    def test_drivetrain_from_title_when_column_empty(self) -> None:
        self.assertIsNone(normalize_drivetrain_from_fields(None))
        self.assertEqual(
            normalize_drivetrain_from_fields(None, title="2024 Subaru Outback Touring XT AWD"),
            "AWD",
        )

    def test_cylinder_extraction(self) -> None:
        self.assertEqual(extract_cylinder_count("3.5L V6", None), 6)
        self.assertEqual(extract_cylinder_count("EcoBoost V-6", None), 6)
        self.assertEqual(extract_cylinder_count("Intercooled Turbo I-4", None), 4)
        self.assertEqual(extract_cylinder_count("Inline-4 2.0L", None), 4)
        self.assertEqual(extract_cylinder_count(None, "6 cyl"), 6)
        self.assertEqual(extract_cylinder_count("Electric Motor", None, fuel_type="Electric"), 0)

    def test_engine_cleanup_examples(self) -> None:
        self.assertEqual(
            normalize_engine_description_storage("3.5L V6 Cylinder Engine"),
            "3.5L V6",
        )
        self.assertEqual(
            normalize_engine_description_storage(
                "Intercooled Turbo Regular Unleaded I-4 2.0 L/122"
            ),
            "2.0L I4",
        )

    def test_trim_from_title(self) -> None:
        self.assertEqual(
            infer_trim_from_name_title(
                None,
                "2024 Ford F-150 Lariat",
                make="Ford",
                model="F-150",
            ),
            "Lariat",
        )

    def test_collect_heuristic_transmission_type(self) -> None:
        patch = collect_raw_spec_heuristic_updates(
            {
                "vin": "X",
                "year": 2024,
                "make": "Ford",
                "model": "F-150",
                "trim": "",
                "title": "2024 Ford F-150 XLT",
                "transmission": "10-Speed Automatic",
                "transmission_type": None,
                "drivetrain": "4WD",
                "engine_description": "5.0L V8",
                "cylinders": 8,
            }
        )
        self.assertEqual(patch.get("transmission_type"), "Automatic")


if __name__ == "__main__":
    unittest.main()
