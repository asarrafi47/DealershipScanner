"""EV motor count inference from listing metadata."""

from __future__ import annotations

import unittest

from backend.utils.ev_motor_count import infer_ev_motor_count


class EvMotorCountTest(unittest.TestCase):
    def test_ex30_twin_motor_returns_two(self) -> None:
        self.assertEqual(
            infer_ev_motor_count(
                make="Volvo",
                model="EX30",
                trim="Twin Motor",
                fuel_type="Electric",
            ),
            2,
        )

    def test_ex90_plus_rwd_returns_one(self) -> None:
        self.assertEqual(
            infer_ev_motor_count(
                make="Volvo",
                model="EX90",
                trim="Plus RWD",
                fuel_type="Electric",
            ),
            1,
        )

    def test_tesla_model_y_long_range_awd_returns_two(self) -> None:
        self.assertEqual(
            infer_ev_motor_count(
                make="Tesla",
                model="Model Y",
                trim="Long Range",
                drivetrain="AWD",
                fuel_type="Electric",
            ),
            2,
        )

    def test_mercedes_eqe_awd_returns_two(self) -> None:
        self.assertEqual(
            infer_ev_motor_count(
                make="Mercedes-Benz",
                model="EQE",
                drivetrain="AWD",
                fuel_type="Electric",
            ),
            2,
        )

    def test_non_ev_returns_none(self) -> None:
        self.assertIsNone(
            infer_ev_motor_count(
                make="Honda",
                model="Accord",
                trim="Sport",
                drivetrain="AWD",
                fuel_type="Gas",
            )
        )


if __name__ == "__main__":
    unittest.main()
