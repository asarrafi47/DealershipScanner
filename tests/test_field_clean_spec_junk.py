"""Regression: manufacturer boilerplate → NULL on normalize."""

from __future__ import annotations

import unittest

from backend.utils.field_clean import is_spec_overlay_junk, normalize_optional_str


class FieldCleanSpecJunkTest(unittest.TestCase):
    def test_normalize_strips_manufacturer_boilerplate(self) -> None:
        self.assertIsNone(
            normalize_optional_str("See manufacturer specifications for details."),
        )

    def test_is_spec_overlay_junk(self) -> None:
        self.assertTrue(is_spec_overlay_junk("Refer to manufacturer"))
        self.assertFalse(is_spec_overlay_junk("Oxford Green Metallic"))


if __name__ == "__main__":
    unittest.main()
