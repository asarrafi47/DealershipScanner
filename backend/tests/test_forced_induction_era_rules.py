"""P10C.5: the 2016+ make/model heuristic against a hand-checked truth fixture.

Every row of fixtures/forced_induction_epa_truth_2016plus.csv is a disagreement the
old heuristic got wrong (naturally aspirated engines labelled boosted, supercharged
labelled turbo, the PHEV miss, the Raptor R shadow, the missed turbo engines) or a
true-positive control. Blank expected = no boost label.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from backend.utils.forced_induction import classify_forced_induction

_FIXTURE = Path(__file__).parent / "fixtures" / "forced_induction_epa_truth_2016plus.csv"


def _rows() -> list[dict[str, str]]:
    with _FIXTURE.open(newline="") as fh:
        lines = [ln for ln in fh if not ln.startswith("#") or ln.startswith("# year")]
    lines[0] = lines[0].lstrip("# ")
    return list(csv.DictReader(lines))


def _classify(r: dict[str, str]) -> str | None:
    return classify_forced_induction(
        make=r["make"],
        model=r["model"],
        trim=r["trim"],
        year=int(r["year"]),
        cylinders=int(r["cylinders"]) if r["cylinders"] else None,
        engine_l=float(r["engine_l"]) if r["engine_l"] else None,
        fuel_type=r["fuel_type"],
    )


_ROWS = _rows()


def test_fixture_size() -> None:
    assert len(_ROWS) >= 70


@pytest.mark.parametrize(
    "row", _ROWS, ids=lambda r: f"{r['year']}-{r['make']}-{r['model']}-{r['trim']}"
)
def test_fixture_row(row: dict[str, str]) -> None:
    assert _classify(row) == (row["expected"] or None), row["note"]


def test_phev_fuel_string_is_not_ev() -> None:
    # 'Premium Gasoline / Electricity' used to hit the pure-EV early return.
    assert classify_forced_induction(
        "BMW", "X5", "xDrive45e", 2021, 6, 3.0, fuel_type="Premium Gasoline / Electricity"
    ) == "Twin Turbocharged" or classify_forced_induction(
        "BMW", "X5", "xDrive45e", 2021, 6, 3.0, fuel_type="Premium Gasoline / Electricity"
    ) == "Turbocharged"
    assert classify_forced_induction("Tesla", "Model 3", "", 2021, None, None, fuel_type="Electricity") is None


def test_pre_2016_era_guard_kept() -> None:
    assert classify_forced_induction("Audi", "A6", "", 2014, 6, 3.0) is None


def test_raptor_r_not_shadowed_by_nameplate_rule() -> None:
    assert classify_forced_induction("Ford", "F-150", "Raptor R", 2023, None, None) == "Supercharged"


def test_cylinders_missing_honda_1_5t() -> None:
    assert classify_forced_induction("Honda", "CR-V", "EX", 2019, None, 1.5) == "Turbocharged"
