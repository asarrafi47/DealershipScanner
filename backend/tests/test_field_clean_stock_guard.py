"""
Defense-in-depth: stock-code-shaped tokens must never survive in
drivetrain / transmission / exterior_color / interior_color, and an identical
token stamped across all four (McPeek PixelMotion leak) is recovered as the
stock number.
"""

from __future__ import annotations

import unittest

from backend.utils.field_clean import clean_car_row_dict, looks_like_stock_code


class LooksLikeStockCodeTest(unittest.TestCase):
    def test_mcpeek_tokens_match(self) -> None:
        for tok in ("R1111", "FR040", "T0386", "T0147", "UK0005", "U0105", "U0113A"):
            self.assertTrue(looks_like_stock_code(tok), tok)

    def test_legit_drivetrain_values_do_not_match(self) -> None:
        for val in ("4WD", "AWD", "4x4", "4X4", "FWD", "RWD", "2WD", "4x2"):
            self.assertFalse(looks_like_stock_code(val), val)

    def test_legit_transmission_values_do_not_match(self) -> None:
        for val in (
            "9-Speed",
            "9-Speed Automatic",
            "8-Speed Automatic 8HP75",
            "Automatic",
            "CVT",
            "6MT",
            "Manual",
        ):
            self.assertFalse(looks_like_stock_code(val), val)

    def test_paint_codes_with_color_words_do_not_match(self) -> None:
        for val in (
            "PW7 Bright White Clear Coat",
            "Bright White Clearcoat PW7",
            "Fathom Blue Pearl Coat",
            "Black",
            "GB8 Black",
        ):
            self.assertFalse(looks_like_stock_code(val), val)

    def test_empty_and_non_strings(self) -> None:
        self.assertFalse(looks_like_stock_code(None))
        self.assertFalse(looks_like_stock_code(""))
        self.assertFalse(looks_like_stock_code("   "))
        self.assertFalse(looks_like_stock_code(1234))


class CleanCarRowStockGuardTest(unittest.TestCase):
    def _row(self, token: str, stock_number: str | None = None) -> dict:
        return {
            "vin": "3C7WRMCL2RG204890",
            "drivetrain": token,
            "transmission": token,
            "exterior_color": token,
            "interior_color": token,
            "stock_number": stock_number,
        }

    def test_identical_token_moves_to_stock_number(self) -> None:
        for token in ("R1111", "FR040", "T0386"):
            out = clean_car_row_dict(self._row(token))
            self.assertIsNone(out["drivetrain"], token)
            self.assertIsNone(out["transmission"], token)
            self.assertIsNone(out["exterior_color"], token)
            self.assertIsNone(out["interior_color"], token)
            self.assertEqual(out["stock_number"], token)

    def test_existing_stock_number_is_not_overwritten(self) -> None:
        out = clean_car_row_dict(self._row("T0386", stock_number="REAL99"))
        self.assertEqual(out["stock_number"], "REAL99")
        self.assertIsNone(out["drivetrain"])
        self.assertIsNone(out["exterior_color"])

    def test_single_contaminated_field_is_rejected(self) -> None:
        out = clean_car_row_dict(
            {
                "vin": "3C7WRMCL2RG204890",
                "drivetrain": "T0386",
                "transmission": "8-Speed Automatic",
                "exterior_color": "Bright White Clearcoat",
                "interior_color": "Black",
                "stock_number": None,
            }
        )
        self.assertIsNone(out["drivetrain"])
        self.assertEqual(out["transmission"], "8-Speed Automatic")
        self.assertEqual(out["exterior_color"], "Bright White Clearcoat")
        self.assertEqual(out["interior_color"], "Black")
        # Not all four carried the token → stock_number is not fabricated
        self.assertIsNone(out["stock_number"])

    def test_legit_values_pass_through_untouched(self) -> None:
        out = clean_car_row_dict(
            {
                "vin": "3C7WRMCL2RG204890",
                "drivetrain": "4WD",
                "transmission": "9-Speed Automatic",
                "exterior_color": "PW7 Bright White Clear Coat",
                "interior_color": "Black",
                "stock_number": "FR040",
            }
        )
        self.assertEqual(out["drivetrain"], "4WD")
        self.assertEqual(out["transmission"], "9-Speed Automatic")
        self.assertEqual(out["exterior_color"], "PW7 Bright White Clear Coat")
        self.assertEqual(out["interior_color"], "Black")
        self.assertEqual(out["stock_number"], "FR040")

    def test_4x4_drivetrain_still_canonicalized_not_rejected(self) -> None:
        out = clean_car_row_dict({"vin": "1", "drivetrain": "4x4"})
        # coerce_drivetrain_stored maps 4x4 → AWD; guard must not null it
        self.assertEqual(out["drivetrain"], "AWD")

    def test_identical_but_legit_value_across_four_fields_untouched(self) -> None:
        # Degenerate feed with the same real word everywhere: not a stock code
        out = clean_car_row_dict(self._row("Automatic"))
        self.assertEqual(out["transmission"], "Automatic")
        self.assertIsNone(out["stock_number"])


if __name__ == "__main__":
    unittest.main()
