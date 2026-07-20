"""
Toyota bZ Woodland: dealer feeds pack the "Woodland" grade into the MODEL field
("bZ Woodland") and leave trim blank / duplicated / drivetrain-filled, which
flags the car incomplete for a missing trim. clean_car_row_dict must split it
back to model="bZ", trim="Woodland" (+ any real sub-grade), so the car is
complete and future scans self-heal.
"""

from __future__ import annotations

import unittest

from backend.utils.field_clean import clean_car_row_dict, split_bz_woodland_model_trim


class BzWoodlandSplitTest(unittest.TestCase):
    def _split(self, model, trim):
        out = {"make": "Toyota", "model": model, "trim": trim}
        split_bz_woodland_model_trim(out)
        return out["model"], out["trim"]

    def test_blank_trim_becomes_woodland(self) -> None:
        self.assertEqual(self._split("bZ Woodland", None), ("bZ", "Woodland"))

    def test_redundant_model_in_trim_collapses(self) -> None:
        self.assertEqual(self._split("bZ Woodland", "bZ Woodland"), ("bZ", "Woodland"))

    def test_drivetrain_in_trim_is_not_a_grade(self) -> None:
        self.assertEqual(self._split("bZ Woodland", "All-Wheel Drive"), ("bZ", "Woodland"))

    def test_premium_subgrade_preserved(self) -> None:
        self.assertEqual(self._split("bZ Woodland", "Premium"), ("bZ", "Woodland Premium"))
        self.assertEqual(
            self._split("bZ Woodland", "bZ Woodland Premium"), ("bZ", "Woodland Premium")
        )

    def test_already_canonical_untouched(self) -> None:
        # model="bZ" already: not a compound model, leave exactly as-is.
        self.assertEqual(self._split("bZ", "Woodland"), ("bZ", "Woodland"))
        self.assertEqual(self._split("bZ", "XLE"), ("bZ", "XLE"))

    def test_non_woodland_bz_untouched(self) -> None:
        self.assertEqual(self._split("bZ4X", "Limited"), ("bZ4X", "Limited"))

    def test_other_makes_untouched(self) -> None:
        out = {"make": "Subaru", "model": "Outback Woodland", "trim": None}
        split_bz_woodland_model_trim(out)
        self.assertEqual((out["model"], out["trim"]), ("Outback Woodland", None))

    def test_wired_into_clean_car_row_dict(self) -> None:
        cleaned = clean_car_row_dict(
            {"make": "Toyota", "model": "bZ Woodland", "trim": None, "year": 2026}
        )
        self.assertEqual(cleaned["model"], "bZ")
        self.assertEqual(cleaned["trim"], "Woodland")


if __name__ == "__main__":
    unittest.main()
