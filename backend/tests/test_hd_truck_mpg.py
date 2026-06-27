"""Tests for EPA-exempt HD truck MPG helpers."""

from __future__ import annotations

import unittest

from backend.enrichment.hd_truck_mpg import (
    is_epa_exempt_hd_truck,
    lookup_hd_truck_mpg_reference,
    mpg_for_epa_master_trim,
    parse_mpg_from_listing_text,
    resolve_hd_truck_mpg,
)


class HdTruckMpgTest(unittest.TestCase):
    def test_is_epa_exempt_ram_2500(self) -> None:
        self.assertTrue(is_epa_exempt_hd_truck("Ram", "2500"))
        self.assertFalse(is_epa_exempt_hd_truck("Ram", "1500"))

    def test_epa_master_trim_gas_4wd(self) -> None:
        self.assertEqual(mpg_for_epa_master_trim("Big Horn 4WD 6.4L Gas"), (13, 17))

    def test_epa_master_trim_diesel(self) -> None:
        self.assertEqual(mpg_for_epa_master_trim("Laramie 4WD 6.7L Cummins"), (16, 21))

    def test_parse_description_mpg(self) -> None:
        self.assertEqual(
            parse_mpg_from_listing_text("Fuel Economy 16/21 MPG combined highway driving"),
            (16, 21),
        )

    def test_reference_lookup_tradesman(self) -> None:
        got = lookup_hd_truck_mpg_reference(
            make="Ram",
            model="2500",
            trim="Tradesman",
            drivetrain="RWD",
            fuel_type="Gasoline",
            engine_description="6.4L V8 HEMI",
        )
        self.assertEqual(got, (14, 18))

    def test_resolve_from_listing_beats_reference(self) -> None:
        car = {
            "make": "Ram",
            "model": "2500",
            "trim": "Laramie",
            "description": "15/20 MPG city highway",
        }
        got = resolve_hd_truck_mpg(car)
        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(got[:2], (15, 20))
        self.assertEqual(got[2], "listing_text")


if __name__ == "__main__":
    unittest.main()
