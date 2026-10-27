"""EPA importer: flag-derived labels, new columns, --years, --patch-induction-only, --verify (P10A.5)."""
from __future__ import annotations

import csv
import shutil
from pathlib import Path

import pytest

from backend.enrichment.dictionary_catalog import normalize_csv_columns
from backend.enrichment.dictionary_paths import CANONICAL_CSV_COLUMNS, EPA_CSV_COLUMNS
from backend.scripts import import_epa_to_dictionary as imp

FIXTURE = Path(__file__).parent / "fixtures" / "epa_vehicles_sample.csv"
EXTRA = ("epaId", "atvType", "cityE", "highwayE")


def _read(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _all_rows(directory: Path) -> dict[str, dict]:
    """Rows of every CSV under directory keyed by epaId (or Make+Model when blank)."""
    out: dict[str, dict] = {}
    for path in sorted(directory.glob("*_EPA.csv")):
        for i, row in enumerate(_read(path)):
            out[row.get("epaId") or f"{path.name}#{i}"] = row
    return out


@pytest.fixture
def generated(tmp_path: Path) -> Path:
    out = tmp_path / "gen"
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(out)]) == 0
    return out


def _legacy_copy(generated: Path, dest: Path) -> Path:
    """The pre-patch state: 21 columns, the old guess for labels, no EPA columns."""
    dest.mkdir()
    for path in generated.glob("*_EPA.csv"):
        rows = _read(path)
        for row in rows:
            if row["Make"] == "Ford":  # the old file already held the twin label
                row["forcedInduction"], row["engineDisplay"] = "Twin Turbocharged", "3.5L V6 Twin Turbo"
            elif row["Make"] == "Acura" and row["Model"] == "Integra":
                row["forcedInduction"], row["engineDisplay"] = "", "1.8L I4"
        with open(dest / path.name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(CANONICAL_CSV_COLUMNS), extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
    return dest


# --- full import ---------------------------------------------------------------

def test_fixture_labels_and_displays(generated: Path) -> None:
    rows = _all_rows(generated)
    got = {r["epaId"]: (r["forcedInduction"], r["engineDisplay"]) for r in rows.values()}
    assert got["1001"] == ("Naturally Aspirated", "1.8L I4")  # Integra: no 'Turbo' anywhere
    assert got["1002"] == ("Turbocharged", "2.0L I4 Turbo")
    assert got["1003"] == ("Supercharged", "3.0L V6 Supercharged")
    assert got["1004"][0] == "Turbocharged"  # T8: both flags -> Turbocharged (D-ET3)
    assert got["1005"][0] == ""  # Bolt EV
    assert got["1006"] == ("Turbocharged", "1.6L I4 Turbo")  # DSL,TRBO
    assert got["1007"] == ("Supercharged", "1.8L I4 Supercharged")  # Kompressor badge
    assert got["1008"] == ("Naturally Aspirated", "2.3L I4")  # slash-list badge is not an engine
    assert got["1009"][0] == "Turbocharged"
    assert got["1010"][0] == ""  # Taycan Turbo EV


def test_new_columns_are_written(generated: Path) -> None:
    rows = _all_rows(generated)
    assert list(next(iter(rows.values())).keys()) == list(EPA_CSV_COLUMNS)
    bolt = rows["1005"]
    assert (bolt["epaId"], bolt["atvType"], bolt["cityE"], bolt["highwayE"]) == ("1005", "EV", "28.0", "33.0")


def test_regeneration_keeps_existing_twin_label(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(legacy)]) == 0
    ford = [r for r in _all_rows(legacy).values() if r["Make"] == "Ford"][0]
    assert ford["forcedInduction"] == "Twin Turbocharged"
    assert ford["engineDisplay"] == "3.5L V6 Twin Turbo"


def test_broken_row_exits_non_zero(monkeypatch, tmp_path: Path) -> None:
    real = imp.epa_label

    def boom(dict_row, source, existing=None):
        if source.get("id") == "1002":
            raise ValueError("broken row")
        return real(dict_row, source, existing)

    monkeypatch.setattr(imp, "epa_label", boom)
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(tmp_path / "o")]) == 1


def test_years_scope(tmp_path: Path) -> None:
    out = tmp_path / "o"
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(out), "--years", "2019,2000"]) == 0
    assert {p.name[:4] for p in out.glob("*_EPA.csv")} == {"2019", "2000"}


# --- --patch-induction-only ----------------------------------------------------

def test_patch_changes_only_owned_columns(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    before = {p.name: _read(p) for p in legacy.glob("*_EPA.csv")}
    integra = next(legacy.glob("2000_Acura_*_EPA.csv"))
    integra_line = integra.read_text(encoding="utf-8").splitlines()[1]

    rc = imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(legacy), "--patch-induction-only", "--apply"])
    assert rc == 0
    owned = {"forcedInduction", "engineDisplay", *EXTRA}
    for path in legacy.glob("*_EPA.csv"):
        after = _read(path)
        assert list(after[0].keys()) == list(EPA_CSV_COLUMNS)
        for old, new in zip(before[path.name], after):
            for col in CANONICAL_CSV_COLUMNS:
                if col not in owned:
                    assert old[col] == new[col], (path.name, col)
    # the Integra gained a label but kept every other byte of its line
    new_line = integra.read_text(encoding="utf-8").splitlines()[1]
    expected = integra_line.replace(",1.8L I4,,Manual", ",1.8L I4,Naturally Aspirated,Manual", 1) + ",1001,,0,0"
    assert new_line == expected
    rows = _all_rows(legacy)
    assert rows["1001"]["forcedInduction"] == "Naturally Aspirated"
    assert rows["1001"]["engineDisplay"] == "1.8L I4"
    # the twin label survives where the classifier is silent (pre-2016)
    assert rows["1009"]["forcedInduction"] == "Twin Turbocharged"


def test_patch_dry_run_writes_nothing(generated: Path, tmp_path: Path, capsys) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    snap = {p.name: p.read_bytes() for p in legacy.glob("*_EPA.csv")}
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(legacy), "--patch-induction-only"]) == 0
    assert {p.name: p.read_bytes() for p in legacy.glob("*_EPA.csv")} == snap
    out = capsys.readouterr().out
    assert "sha256:" in out and "rows: 14" in out and "DRY RUN" in out and "->" in out


def test_patch_is_idempotent(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    args = ["--epa-csv", str(FIXTURE), "--dictionary", str(legacy), "--patch-induction-only", "--apply"]
    imp.main(args)
    snap = {p.name: p.read_bytes() for p in legacy.glob("*_EPA.csv")}
    imp.main(args)
    assert {p.name: p.read_bytes() for p in legacy.glob("*_EPA.csv")} == snap


def test_flag_conflict_and_duplicates_are_ambiguous(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    stats = imp.patch_induction(FIXTURE, legacy, apply=True)
    assert stats["ambiguous_flags"] == 2  # 1011 (T) and 1012 (blank) share a key
    assert stats["ambiguous_multi"] == 2  # 1013/1014 identical
    assert stats["unique"] == 10
    rows = [r for r in _all_rows(legacy).values() if r["Make"] == "Testmake"]
    assert all(r["epaId"] == "" for r in rows)  # no id on a key that matches several rows
    assert all(r["forcedInduction"] == "" and r["engineDisplay"] == "2.0L I4" for r in rows)


def test_unmatched_row_blank_unless_explicit_marker(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    path = next(legacy.glob("2019_Acura_*_EPA.csv"))
    rows = _read(path)
    plain = dict(rows[0], Trim="Not In EPA", engineDisplay="2.0L I4 Turbo")
    marked = dict(rows[0], Trim="Marked", engineOptions="2.0L I4 (TRBO)", engineDisplay="2.0L I4 Turbo")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(CANONICAL_CSV_COLUMNS))
        w.writeheader()
        w.writerows([plain, marked])
    stats = imp.patch_induction(FIXTURE, legacy, apply=True)
    assert stats["unmatched"] == 2
    got = {r["Trim"]: r for r in _read(path)}
    assert (got["Not In EPA"]["forcedInduction"], got["Not In EPA"]["engineDisplay"]) == ("", "2.0L I4")
    assert got["Marked"]["forcedInduction"] == "Turbocharged"


def test_patch_reports_base_display_change(generated: Path, tmp_path: Path, capsys) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    path = next(legacy.glob("2017_Chevrolet_*_EPA.csv"))
    rows = _read(path)
    rows[0]["engineDisplay"] = "9.9L V6"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(CANONICAL_CSV_COLUMNS))
        w.writeheader()
        w.writerows(rows)
    imp.patch_induction(FIXTURE, legacy)
    assert "base-display changes" in capsys.readouterr().out


# --- --verify ------------------------------------------------------------------

def test_verify_clean_after_import_and_after_patch(generated: Path, tmp_path: Path) -> None:
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(generated), "--verify"]) == 0
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(legacy), "--patch-induction-only", "--apply"])
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(legacy), "--verify"]) == 0


def test_verify_fails_on_a_wrong_label(generated: Path) -> None:
    path = next(generated.glob("2000_Acura_*_EPA.csv"))
    rows = _read(path)
    rows[0]["forcedInduction"] = "Turbocharged"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(EPA_CSV_COLUMNS))
        w.writeheader()
        w.writerows(rows)
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(generated), "--verify"]) == 1


def test_verify_fails_on_unpatched_dictionary(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(legacy), "--verify"]) == 1


def test_apply_requires_patch_mode(tmp_path: Path) -> None:
    assert imp.main(["--epa-csv", str(FIXTURE), "--dictionary", str(tmp_path), "--apply"]) == 2


# --- normalize_csv_columns ------------------------------------------------------

def test_normalize_preserves_epa_columns(generated: Path, tmp_path: Path) -> None:
    path = tmp_path / "2019_Acura_RDX_EPA.csv"
    shutil.copy(next(generated.glob("2019_Acura_*_EPA.csv")), path)
    before = path.read_bytes()
    assert normalize_csv_columns(path) is False
    assert path.read_bytes() == before


def test_normalize_leaves_21_column_epa_file_alone(generated: Path, tmp_path: Path) -> None:
    legacy = _legacy_copy(generated, tmp_path / "legacy")
    path = next(legacy.glob("2019_Acura_*_EPA.csv"))
    before = path.read_bytes()
    assert normalize_csv_columns(path) is False
    assert path.read_bytes() == before


def test_normalize_options_csv_still_canonical_only(tmp_path: Path) -> None:
    path = tmp_path / "2019_Acura_RDX_Complete_Options.csv"
    path.write_text("Year,Make,epaId\n2019,Acura,5\n", encoding="utf-8")
    assert normalize_csv_columns(path) is True
    assert path.read_text(encoding="utf-8").splitlines()[0] == ",".join(CANONICAL_CSV_COLUMNS)
