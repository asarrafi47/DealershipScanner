"""Unit tests for the pure conversion/consensus/propagation logic in
backend/scripts/backfill_extended_specs.py (no DB required)."""
from backend.scripts.backfill_extended_specs import (
    PROPAGATION_COLUMNS,
    compute_cross_year_fills,
    compute_same_year_fills,
    consensus,
    kg_to_lb,
    lb_ft_to_nm,
    lb_to_kg,
    merge_fill_maps,
    nm_to_lb_ft,
    unit_conversion_fills,
)


def _row(rid, year, make="Ram", model="1500", trim=None, **specs):
    base = {
        "epa_master_id": rid,
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "horsepower": None,
        "torque_lb_ft": None,
        "torque_nm": None,
        "curb_weight_lb": None,
        "curb_weight_kg": None,
        "zero_to_60_sec": None,
        "fuel_tank_gal": None,
        "ev_range_miles": None,
        "battery_kwh": None,
        "tow_capacity_lb": None,
    }
    base.update(specs)
    return base


class TestUnitConversions:
    def test_round_trip_factors(self):
        assert lb_ft_to_nm(410) == 556  # 410 * 1.3558 = 555.878
        assert nm_to_lb_ft(556) == 410
        assert kg_to_lb(2000) == 4409  # 2000 * 2.20462
        assert lb_to_kg(4409) == 2000

    def test_fills_only_null_side(self):
        row = _row(1, 2020, torque_nm=556)
        assert unit_conversion_fills(row) == {"torque_lb_ft": 410}
        row = _row(2, 2020, curb_weight_lb=4409)
        assert unit_conversion_fills(row) == {"curb_weight_kg": 2000}

    def test_no_overwrite_when_both_present(self):
        row = _row(3, 2020, torque_nm=556, torque_lb_ft=999,
                   curb_weight_lb=4409, curb_weight_kg=1234)
        assert unit_conversion_fills(row) == {}

    def test_all_null_yields_nothing(self):
        assert unit_conversion_fills(_row(4, 2020)) == {}


class TestConsensus:
    def test_all_agree(self):
        assert consensus([395, 395, 395]) == 395

    def test_disagree_returns_none(self):
        assert consensus([395, 410]) is None

    def test_ignores_nulls(self):
        assert consensus([None, 26.0, None, 26.0]) == 26.0

    def test_empty_and_all_null(self):
        assert consensus([]) is None
        assert consensus([None, None]) is None

    def test_tolerance_within(self):
        # 2% tolerance: 5000 vs 5080 -> spread 1.57%, accepted (first value)
        assert consensus([5000, 5080], rel_tolerance=0.02) == 5000

    def test_tolerance_exceeded(self):
        assert consensus([5000, 5500], rel_tolerance=0.02) is None


class TestSameYearPropagation:
    def test_fills_null_when_siblings_agree(self):
        rows = [
            _row(1, 2020, trim="Big Horn", fuel_tank_gal=26.0),
            _row(2, 2020, trim="Laramie", fuel_tank_gal=26.0),
            _row(3, 2020, trim="Rebel"),  # NULL tank
        ]
        fills = compute_same_year_fills(rows)
        assert fills == {3: {"fuel_tank_gal": 26.0}}

    def test_blocked_when_siblings_disagree(self):
        rows = [
            _row(1, 2020, trim="A", fuel_tank_gal=23.0),
            _row(2, 2020, trim="B", fuel_tank_gal=26.0),
            _row(3, 2020, trim="C"),
        ]
        assert compute_same_year_fills(rows) == {}

    def test_group_boundaries_respected(self):
        rows = [
            _row(1, 2020, model="1500", tow_capacity_lb=11000),
            _row(2, 2020, model="2500"),  # different model, no donor
        ]
        assert compute_same_year_fills(rows) == {}

    def test_only_propagation_columns(self):
        rows = [
            _row(1, 2020, trim="A", curb_weight_lb=5000),
            _row(2, 2020, trim="B"),
        ]
        fills = compute_same_year_fills(rows)
        assert fills == {}  # curb_weight_lb is not a propagation column
        assert "curb_weight_lb" not in PROPAGATION_COLUMNS


class TestCrossYearPropagation:
    def test_fills_from_adjacent_year_same_trim(self):
        rows = [
            _row(1, 2019, trim="Laramie", tow_capacity_lb=11000),
            _row(2, 2020, trim="Laramie"),
        ]
        fills, prov = compute_cross_year_fills(rows)
        assert fills == {2: {"tow_capacity_lb": 11000}}
        assert prov == {2: {"tow_capacity_lb_source": "sibling_trim_2019"}}

    def test_blocked_when_donor_years_disagree(self):
        rows = [
            _row(1, 2019, trim="Laramie", horsepower=395),
            _row(2, 2021, trim="Laramie", horsepower=420),
            _row(3, 2020, trim="Laramie"),
        ]
        fills, _ = compute_cross_year_fills(rows)
        assert fills == {}

    def test_agreeing_donors_from_both_years(self):
        rows = [
            _row(1, 2019, trim="Laramie", horsepower=395),
            _row(2, 2021, trim="Laramie", horsepower=395),
            _row(3, 2020, trim="Laramie"),
        ]
        fills, prov = compute_cross_year_fills(rows)
        assert fills == {3: {"horsepower": 395}}
        assert prov[3]["horsepower_source"] == "sibling_trim_2019_2021"

    def test_trim_mismatch_blocks(self):
        rows = [
            _row(1, 2019, trim="Laramie", tow_capacity_lb=11000),
            _row(2, 2020, trim="Rebel"),
        ]
        fills, _ = compute_cross_year_fills(rows)
        assert fills == {}

    def test_year_gap_of_two_blocks(self):
        rows = [
            _row(1, 2018, trim="Laramie", tow_capacity_lb=11000),
            _row(2, 2020, trim="Laramie"),
        ]
        fills, _ = compute_cross_year_fills(rows)
        assert fills == {}

    def test_skips_rows_already_filled_same_year(self):
        rows = [
            _row(1, 2019, trim="Laramie", fuel_tank_gal=26.0),
            _row(2, 2020, trim="Laramie"),
        ]
        fills, _ = compute_cross_year_fills(
            rows, already_filled={2: {"fuel_tank_gal": 26.0}}
        )
        assert fills == {}

    def test_null_trim_matches_null_trim(self):
        rows = [
            _row(1, 2019, trim=None, zero_to_60_sec=6.5),
            _row(2, 2020, trim=None),
        ]
        fills, _ = compute_cross_year_fills(rows)
        assert fills == {2: {"zero_to_60_sec": 6.5}}


def test_merge_fill_maps():
    merged = merge_fill_maps(
        {1: {"torque_nm": 556}},
        {1: {"fuel_tank_gal": 26.0}, 2: {"horsepower": 395}},
    )
    assert merged == {
        1: {"torque_nm": 556, "fuel_tank_gal": 26.0},
        2: {"horsepower": 395},
    }
