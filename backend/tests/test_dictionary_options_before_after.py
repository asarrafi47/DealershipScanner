"""P9.2b: the 20-car before/after diff script's sampling and verdict logic (no DB, no git)."""

from __future__ import annotations

from pathlib import Path

from backend.scripts import dictionary_options_before_after as ba

_BODY = "x" * 400


def _write(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_BODY)


def test_candidate_groups_split_raw_only_from_root_duplicates(tmp_path):
    _write(tmp_path / "options/raw/Honda/2023_Honda_CR-V_Complete_Options.csv")
    _write(tmp_path / "options/raw/Land_Rover/2021_Land_Rover_Defender_Complete_Options.csv")
    _write(tmp_path / "options/raw/Buick/2020_Buick_Enclave_Complete_Options.csv")
    _write(tmp_path / "2020_Buick_Enclave_Complete_Options.csv")
    _write(tmp_path / "options/raw/Buick/2010_Buick_Lucerne_Complete_Options.csv")  # too old
    (tmp_path / "options/raw/Buick/2021_Buick_Stub_Complete_Options.csv").write_text("h\n")  # stub
    only, dup = ba.candidate_groups(tmp_path)
    assert [c["id"] for c in only] == ["2023 Honda CR-V", "2021 Land_Rover Defender"] or [
        c["id"] for c in only
    ] == ["2021 Land_Rover Defender", "2023 Honda CR-V"]
    assert [c["id"] for c in dup] == ["2020 Buick Enclave"]


def test_strided_is_even_and_deterministic():
    assert ba._strided(list(range(100)), 10) == list(range(0, 100, 10))
    assert ba._strided([1, 2, 3], 10) == [1, 2, 3]


def test_compare_verdicts():
    steps = [{"name": "LX", "adds": 1, "sticker_adds": 0}]
    same = {"options_csv": "a.csv", "steps": steps}
    assert ba.compare(same, dict(same)) == "same"
    assert ba.compare(same, {**same, "options_csv": "b.csv"}) == "same-rungs-different-file"
    other = [{"name": "EX", "adds": 1, "sticker_adds": 0}]
    assert ba.compare(same, {**same, "steps": other}) == "rungs-changed"
    assert ba.compare(same, {"error": "boom"}) == "error"


def test_render_md_has_one_row_per_car():
    rec = {"options_csv": "a.csv", "steps": [{"name": "LX"}]}
    row = {"group": "raw-only", "car": "2023 Honda CR-V", "verdict": "same", "before": rec, "after": rec}
    md = ba.render_md([row], "abc")
    assert "| 1 | raw-only | 2023 Honda CR-V | same |" in md
    assert "Signed by" in md
