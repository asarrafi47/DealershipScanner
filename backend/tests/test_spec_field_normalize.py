"""Regression: regex/mapping spec normalization for inventory repair."""

from __future__ import annotations

import unittest

from backend.utils.spec_field_normalize import (
    collect_raw_spec_heuristic_updates,
    extract_cylinder_count,
    infer_trim_from_name_title,
    normalize_drivetrain_from_fields,
    normalize_engine_description_storage,
    split_trim_components,
    trim_named_in_listing,
)


class SpecFieldNormalizeTest(unittest.TestCase):
    def test_drivetrain_aliases(self) -> None:
        self.assertEqual(normalize_drivetrain_from_fields("All Wheel Drive"), "AWD")
        self.assertEqual(normalize_drivetrain_from_fields("All-Wheel Drive"), "AWD")
        self.assertEqual(normalize_drivetrain_from_fields("A"), "AWD")
        # 4x4 is four-wheel drive (vPIC spells its 4WD value "4WD/4-Wheel Drive/4x4").
        self.assertEqual(normalize_drivetrain_from_fields("4x4"), "4WD")
        self.assertEqual(normalize_drivetrain_from_fields("Front Wheel Drive"), "FWD")
        self.assertEqual(normalize_drivetrain_from_fields("F"), "FWD")
        self.assertEqual(normalize_drivetrain_from_fields("4x2"), "2WD")  # two driven wheels, end unknown (2026-09-26)
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


class TrimNamedInListingTest(unittest.TestCase):
    """Corroboration predicate for decoder-derived (vPIC) trims."""

    def _row(self, **overrides) -> dict:
        base = {
            "title": None,
            "model_full_raw": None,
            "description": None,
            "source_url": None,
        }
        base.update(overrides)
        return base

    def test_match_in_title(self) -> None:
        self.assertTrue(
            trim_named_in_listing(self._row(title="2024 Ford F-150 Lariat 4WD"), "Lariat")
        )

    def test_match_in_model_full_raw(self) -> None:
        self.assertTrue(
            trim_named_in_listing(self._row(model_full_raw="F-150 Lariat SuperCrew"), "Lariat")
        )

    def test_match_in_description(self) -> None:
        self.assertTrue(
            trim_named_in_listing(
                self._row(description="This Lariat comes loaded with leather."), "Lariat"
            )
        )

    def test_match_in_source_url(self) -> None:
        self.assertTrue(
            trim_named_in_listing(
                self._row(source_url="https://dealer.example/2024-ford-f150-lariat-id123"),
                "Lariat",
            )
        )

    def test_digit_boundary_insensitive_token_spaced_in_title(self) -> None:
        # Decoder says "GLC300", dealer title spells it "GLC 300" — must match,
        # or the stock-code guard would null a legitimate dealer trim.
        self.assertTrue(
            trim_named_in_listing(
                self._row(title="2023 Mercedes-Benz GLC 300 4MATIC"), "GLC300"
            )
        )

    def test_digit_boundary_insensitive_token_unspaced_in_title(self) -> None:
        self.assertTrue(
            trim_named_in_listing(self._row(title="2023 Mercedes GLC300 Coupe"), "GLC 300")
        )

    def test_case_and_punctuation_insensitive(self) -> None:
        self.assertTrue(
            trim_named_in_listing(self._row(title="2023 BMW 330i SPORT LINE package"), "Sport-Line")
        )
        self.assertTrue(
            trim_named_in_listing(self._row(title="mustang shelby gt350 fastback"), "GT350")
        )

    def test_whole_token_required(self) -> None:
        # "LT" must not match inside "XLT"
        self.assertFalse(trim_named_in_listing(self._row(title="2024 Ford F-150 XLT"), "LT"))
        self.assertTrue(trim_named_in_listing(self._row(title="2024 Ford F-150 XLT"), "XLT"))

    def test_comma_list_any_component_matches(self) -> None:
        self.assertTrue(
            trim_named_in_listing(self._row(title="2023 VW ID.4 Pro S rear motor"), "Pro S, Pro")
        )
        self.assertTrue(
            trim_named_in_listing(self._row(title="2022 Jeep Compass North 4x4"), "Latitude/North")
        )

    def test_unnamed_trim_is_false(self) -> None:
        self.assertFalse(
            trim_named_in_listing(
                self._row(
                    title="2023 Ford Bronco",
                    description="Great condition, one owner.",
                    source_url="https://dealer.example/inventory/123",
                ),
                "Wildtrak",
            )
        )

    def test_empty_trim_and_empty_row(self) -> None:
        self.assertFalse(trim_named_in_listing(self._row(title="2024 Ford F-150"), None))
        self.assertFalse(trim_named_in_listing(self._row(title="2024 Ford F-150"), "   "))
        self.assertFalse(trim_named_in_listing(self._row(), "Lariat"))

    def test_split_trim_components(self) -> None:
        self.assertEqual(
            split_trim_components("Light, Light Long Range, Wind"),
            ["Light", "Light Long Range", "Wind"],
        )
        self.assertEqual(split_trim_components("Latitude/North"), ["Latitude", "North"])
        self.assertEqual(split_trim_components("SE or SEL"), ["SE", "SEL"])
        self.assertEqual(split_trim_components("Lariat"), ["Lariat"])
        self.assertEqual(split_trim_components(None), [])


if __name__ == "__main__":
    unittest.main()
