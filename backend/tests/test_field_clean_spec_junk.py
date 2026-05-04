"""Regression: manufacturer boilerplate → NULL on normalize."""

from __future__ import annotations

import unittest

from backend.utils.field_clean import (
    clean_car_row_dict,
    coerce_drivetrain_stored,
    is_effectively_empty,
    is_spec_overlay_junk,
    normalize_optional_str,
)


class FieldCleanSpecJunkTest(unittest.TestCase):
    def test_is_effectively_empty_bool_not_treated_as_int(self) -> None:
        # ``bool`` subclasses ``int``; only ``False`` is empty, not numeric zero
        self.assertTrue(is_effectively_empty(False))
        self.assertFalse(is_effectively_empty(True))
        self.assertFalse(is_effectively_empty(0))
        self.assertFalse(is_effectively_empty(1))

    def test_normalize_strips_manufacturer_boilerplate(self) -> None:
        self.assertIsNone(
            normalize_optional_str("See manufacturer specifications for details."),
        )

    def test_is_spec_overlay_junk(self) -> None:
        self.assertTrue(is_spec_overlay_junk("Refer to manufacturer"))
        self.assertFalse(is_spec_overlay_junk("Oxford Green Metallic"))

    def test_coerce_drivetrain_stored_schema_org_urls(self) -> None:
        self.assertEqual(
            coerce_drivetrain_stored("https://schema.org/AllWheelDriveConfiguration"),
            "AWD",
        )
        self.assertEqual(
            coerce_drivetrain_stored("https://schema.org/FrontWheelDriveConfiguration"),
            "FWD",
        )
        self.assertIsNone(
            coerce_drivetrain_stored("https://schema.org/UnknownType"),
        )
        self.assertEqual(coerce_drivetrain_stored("xDrive AWD"), "xDrive AWD")

    def test_clean_car_row_dict_drivetrain_schema(self) -> None:
        d = {
            "vin": "1",
            "drivetrain": "https://schema.org/AllWheelDriveConfiguration",
        }
        out = clean_car_row_dict(d)  # type: ignore[arg-type]
        self.assertEqual(out.get("drivetrain"), "AWD")


if __name__ == "__main__":
    unittest.main()
