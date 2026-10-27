"""Tests for dictionary catalog manifest and lookups."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from backend.enrichment.dictionary_catalog import (
    build_manifest_entries,
    catalog_key,
    canonical_make,
    find_epa_csv,
    normalize_csv_columns,
    options_status,
    parse_csv_filename,
    rebuild_catalog,
)
from backend.enrichment.dictionary_paths import CANONICAL_CSV_COLUMNS, DICTIONARY_ROOT


def test_canonical_make_aliases() -> None:
    assert canonical_make("chevy") == "Chevrolet"
    assert canonical_make("landrover") == "Land Rover"


def test_parse_csv_filename_epa() -> None:
    meta = parse_csv_filename(Path("2024_Jeep_Grand Cherokee_EPA.csv"))
    assert meta is not None
    assert meta["kind"] == "epa"
    assert meta["year"] == 2024
    assert meta["make"] == "Jeep"
    assert meta["model"] == "Grand Cherokee"


def test_catalog_key_stable() -> None:
    assert catalog_key(2024, "Jeep", "Grand Cherokee") == catalog_key(2024, "jeep", "Grand Cherokee")


def test_find_epa_csv_jeep(scratch_dictionary_root: Path) -> None:
    """A full rebuild + lookup, run entirely inside an isolated dictionary tree.

    This test used to hand-redirect ``dictionary_paths`` and then call
    ``rebuild_catalog()``. ``dictionary_catalog`` binds ``INDEX_DIR`` /
    ``MANIFEST_PATH`` / ``CATALOG_DB_PATH`` into its own namespace at import
    time, so the rebuild wrote through to the REAL backend/dictionary/index and
    rebuilt it from this one-row fixture -- manifest entry_count 14,598 -> 1 on
    2026-07-31. The ``scratch_dictionary_root`` fixture redirects every imported
    copy of every dictionary path constant, and the session guard in conftest
    makes the real tree unwritable regardless.
    """
    from backend.enrichment import dictionary_catalog

    tmp_path = scratch_dictionary_root
    epa = tmp_path / "2024_Jeep_Grand_Cherokee_EPA.csv"
    with epa.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS))
        writer.writeheader()
        writer.writerow(
            {
                "Year": "2024",
                "Make": "Jeep",
                "Model": "Grand Cherokee",
                "Trim": "Limited",
                "engineOptions": "3.6L V6",
            }
        )

    rebuild_catalog(build_manifest_entries())
    dictionary_catalog.invalidate_catalog_cache()
    found = find_epa_csv("Jeep", "Grand Cherokee", 2024)
    assert found is not None
    assert found.name.endswith("_EPA.csv")


def test_normalize_csv_columns_adds_missing(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["Year", "Make", "Model", "Trim", "engineOptions"],
        )
        writer.writeheader()
        writer.writerow(
            {"Year": "2024", "Make": "Jeep", "Model": "Wrangler", "Trim": "Sport", "engineOptions": "3.6L"}
        )
    assert normalize_csv_columns(path) is True
    with path.open(encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
    assert header == list(CANONICAL_CSV_COLUMNS)


def test_options_status_stub(tmp_path: Path) -> None:
    path = tmp_path / "stub.csv"
    path.write_text(",".join(CANONICAL_CSV_COLUMNS) + "\n", encoding="utf-8")
    assert options_status(path) == "stub"


@pytest.mark.skipif(not DICTIONARY_ROOT.is_dir(), reason="dictionary not present")
def test_manifest_builds_on_real_dictionary() -> None:
    entries = build_manifest_entries()
    assert len(entries) > 100
    with_epa = sum(1 for e in entries if e.get("epa_path"))
    assert with_epa > 100


# --- the real index cannot be written from a test ---------------------------
#
# ``index/manifest.json`` is gitignored (.gitignore:109), so a clean checkout
# (CI) has the tracked dictionary tree but no manifest. The guard has to hold
# there too: the write is refused either way, and "unchanged" then means the
# blocked write did not create the file.


def _manifest_state(path: Path) -> bytes | None:
    """The manifest's bytes, or None when this checkout has no manifest."""
    return path.read_bytes() if path.is_file() else None


@pytest.mark.skipif(not DICTIONARY_ROOT.is_dir(), reason="dictionary not present")
def test_writing_the_real_manifest_is_blocked() -> None:
    """The exact move that destroyed the catalog now raises instead.

    Goes through ``dictionary_catalog``'s OWN ``MANIFEST_PATH`` -- the private
    copy that made redirecting ``dictionary_paths`` insufficient -- and through
    ``write_manifest()``, the writer ``rebuild_catalog()`` calls first. Runs
    with or without the gitignored manifest on disk.
    """
    from backend.enrichment import dictionary_catalog
    from backend.tests.conftest import RealDictionaryWriteBlocked

    before = _manifest_state(dictionary_catalog.MANIFEST_PATH)
    real = str(dictionary_catalog.MANIFEST_PATH.resolve())

    with pytest.raises(RealDictionaryWriteBlocked) as e1:
        dictionary_catalog.MANIFEST_PATH.write_text("{}", encoding="utf-8")
    with pytest.raises(RealDictionaryWriteBlocked) as e2:
        dictionary_catalog.write_manifest([])
    with pytest.raises(RealDictionaryWriteBlocked) as e3:
        with open(dictionary_catalog.MANIFEST_PATH, "w", encoding="utf-8") as fh:
            fh.write("{}")

    # Not vacuous: each block names the REAL manifest, i.e. that write was
    # aimed at production data and would have landed without the guard.
    for excinfo in (e1, e2, e3):
        assert real in str(excinfo.value)
    assert _manifest_state(dictionary_catalog.MANIFEST_PATH) == before


@pytest.mark.skipif(not DICTIONARY_ROOT.is_dir(), reason="dictionary not present")
def test_the_2026_07_31_incident_shape_is_now_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replay the exact unsafe pattern: redirect ``dictionary_paths``, then rebuild.

    That is what a test did on 2026-07-31, and because ``dictionary_catalog``
    holds its own copies of the index paths the rebuild landed on the real index
    (entry_count 14,598 -> 1). The same code now raises before it can write, and
    the manifest is byte-identical afterwards (or still absent, in a checkout
    without the gitignored manifest).
    """
    from backend.enrichment import dictionary_catalog, dictionary_paths
    from backend.tests.conftest import RealDictionaryWriteBlocked

    before = _manifest_state(dictionary_catalog.MANIFEST_PATH)
    for attr, value in (
        ("DICTIONARY_ROOT", tmp_path),
        ("INDEX_DIR", tmp_path / "index"),
        ("EPA_DIR", tmp_path / "epa"),
        ("CATALOG_DB_PATH", tmp_path / "index" / "dictionary_catalog.db"),
    ):
        monkeypatch.setattr(dictionary_paths, attr, value)

    with pytest.raises(RealDictionaryWriteBlocked):
        rebuild_catalog([], enrich_derived=False)

    assert _manifest_state(dictionary_catalog.MANIFEST_PATH) == before


@pytest.mark.skipif(not DICTIONARY_ROOT.is_dir(), reason="dictionary not present")
def test_real_catalog_db_is_readable_but_not_writable() -> None:
    """Lookups still read the catalog DB; a rebuild of it cannot land.

    ``find_epa_csv`` reads this SQLite file on every EPA lookup, so the guard
    downgrades the connection to ``mode=ro`` rather than refusing it.
    """
    import sqlite3

    from backend.enrichment import dictionary_catalog

    if not dictionary_catalog.CATALOG_DB_PATH.is_file():
        pytest.skip("catalog db not built")

    conn = sqlite3.connect(str(dictionary_catalog.CATALOG_DB_PATH))
    try:
        (rows,) = conn.execute("SELECT COUNT(*) FROM dictionary_entries").fetchone()
        assert rows > 100, "read path must keep working"
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM dictionary_entries")
    finally:
        conn.close()

    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        dictionary_catalog.rebuild_catalog_db([])

    conn = sqlite3.connect(str(dictionary_catalog.CATALOG_DB_PATH))
    try:
        (after,) = conn.execute("SELECT COUNT(*) FROM dictionary_entries").fetchone()
    finally:
        conn.close()
    assert after == rows


def test_sqlite_inventory_fixture_gives_fleet_backed_code_a_real_database(
    sqlite_inventory,
) -> None:
    """Contract test for the other half of the isolation problem.

    ``INVENTORY_DATABASE_URL`` is blank under pytest, so fleet-backed lookups
    (``trim_ladder._inventory_rung_evidence`` -> ``resolve_trim_ladder``) read an
    empty inventory and return ``()``; tests layered on top then fail for a
    reason they never meant to assert. Seeded rows have to reach that query, and
    the counts have to be the ones seeded -- not a leak from the shared dev
    ``inventory.db`` the root conftest otherwise points ``DB_PATH`` at.
    """
    from backend.enrichment import trim_ladder

    assert trim_ladder._inventory_rung_evidence("Ram", "1500", 2023) == ()

    sqlite_inventory.add_cars(
        [{"year": 2023, "make": "Ram", "model": "1500", "trim": "Limited"}] * 3
        + [{"year": 2023, "make": "RAM", "model": "1500", "trim": "Laramie"}] * 2
        + [{"year": 2022, "make": "Ram", "model": "1500", "trim": "Big Horn"}]
    )

    assert trim_ladder._inventory_rung_evidence("Ram", "1500", 2023) == (
        ("Limited", 3),
        ("Laramie", 2),
    )
    assert trim_ladder._inventory_rung_evidence("Ram", "1500", 2022) == (("Big Horn", 1),)


def test_scratch_root_redirects_the_private_copies(scratch_dictionary_root: Path) -> None:
    """The fixture has to reach modules other than ``dictionary_paths``.

    ``dictionary_catalog`` holding its own ``MANIFEST_PATH`` is what turned a
    tmp-dir test into a production data loss, so assert that copy specifically.
    """
    from backend.enrichment import dictionary_catalog, dictionary_paths

    for module in (dictionary_paths, dictionary_catalog):
        for attr in ("DICTIONARY_ROOT", "INDEX_DIR", "MANIFEST_PATH", "CATALOG_DB_PATH"):
            value = Path(getattr(module, attr))
            assert scratch_dictionary_root in value.parents or value == scratch_dictionary_root, (
                f"{module.__name__}.{attr} still points at {value}"
            )

    # And a writer aimed at it lands in the scratch tree, not the real one.
    dictionary_catalog.write_manifest([])
    assert (scratch_dictionary_root / "index" / "manifest.json").is_file()


# --- P9.2: the in-memory options index is the sole options resolver ----------

_BIG_ROW = "A Very Long Option Name That Keeps The File Above The Stub Size Limit"


def _write_options(path: Path, year: int, make: str, model: str, rows: int = 6) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["Year", "Make", "Model", "Trim", "Options", "optionDetails"])
        for i in range(rows):
            writer.writerow([year, make, model, f"T{i}", f"{_BIG_ROW} {i}", _BIG_ROW])
    return path


def _find_options(make: str, model: str, year: int) -> Path | None:
    from backend.enrichment import dictionary_catalog

    dictionary_catalog.invalidate_catalog_cache()
    return dictionary_catalog.find_complete_options_csv(make, model, year)


def test_options_index_hyphenated_model(scratch_dictionary_root: Path) -> None:
    raw = scratch_dictionary_root / "options" / "raw" / "Honda"
    want = _write_options(raw / "2022_Honda_CR-V_Complete_Options.csv", 2022, "Honda", "CR-V")
    assert _find_options("Honda", "CR V", 2022) == want
    assert _find_options("Honda", "CR-V", 2022) == want


def test_options_index_make_alias(scratch_dictionary_root: Path) -> None:
    (scratch_dictionary_root / "index" / "make_aliases.json").write_text('{"chevy": "Chevrolet"}')
    raw = scratch_dictionary_root / "options" / "raw" / "Chevrolet"
    want = _write_options(raw / "2021_Chevrolet_Tahoe_Complete_Options.csv", 2021, "Chevrolet", "Tahoe")
    assert _find_options("chevy", "Tahoe", 2021) == want


def test_options_index_ignores_stale_catalog_row(scratch_dictionary_root: Path) -> None:
    """A catalog row for a deleted 2020 file must not send 2020 to the 2019 file."""
    root = scratch_dictionary_root
    gone = _write_options(root / "options" / "raw" / "Buick" / "2020_Buick_Enclave_Complete_Options.csv", 2020, "Buick", "Enclave")
    _write_options(root / "options" / "raw" / "Buick" / "2019_Buick_Enclave_Complete_Options.csv", 2019, "Buick", "Enclave")
    rebuild_catalog(build_manifest_entries(), enrich_derived=False)
    gone.unlink()
    live = _write_options(root / "options" / "raw" / "Buick" / "2020_Buick_Enclave_Complete_Options.csv", 2020, "Buick", "Enclave")
    assert _find_options("Buick", "Enclave", 2020) == live


def test_options_index_prefers_raw_over_root_and_skips_stubs(scratch_dictionary_root: Path) -> None:
    root = scratch_dictionary_root
    name = "2020_Buick_Enclave_Complete_Options.csv"
    raw = _write_options(root / "options" / "raw" / "Buick" / name, 2020, "Buick", "Enclave", rows=6)
    _write_options(root / name, 2020, "Buick", "Enclave", rows=1)
    assert _find_options("Buick", "Enclave", 2020) == raw
    (root / "options" / "stubs").mkdir(exist_ok=True)
    (root / "options" / "stubs" / "2024_Buick_Encore_Complete_Options.csv").write_text("Year,Make,Model\n")
    assert _find_options("Buick", "Encore", 2024) is None


def test_options_index_without_catalog_resolves_to_raw(scratch_dictionary_root: Path) -> None:
    raw = scratch_dictionary_root / "options" / "raw" / "Toyota"
    want = _write_options(raw / "2025_Toyota_Camry_Complete_Options.csv", 2025, "Toyota", "Camry")
    assert not (scratch_dictionary_root / "index" / "dictionary_catalog.db").exists()
    assert _find_options("Toyota", "Camry", 2025) == want


def test_options_index_refuses_sibling_model(scratch_dictionary_root: Path) -> None:
    raw = scratch_dictionary_root / "options" / "raw" / "Chevrolet"
    _write_options(raw / "2025_Chevrolet_Blazer EV_Complete_Options.csv", 2025, "Chevrolet", "Blazer EV")
    assert _find_options("Chevrolet", "Blazer", 2025) is None


def test_pf1_epa_prefers_raw_model_over_family_label(scratch_dictionary_root: Path) -> None:
    """Without a catalog DB the 2026 A5 must not resolve to the A4 family file (PF-1)."""
    epa = scratch_dictionary_root / "epa" / "Audi"
    epa.mkdir(parents=True)
    for year, model in ((2025, "A4"), (2025, "A5")):
        with (epa / f"{year}_Audi_{model}_EPA.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS))
            writer.writeheader()
            writer.writerow({"Year": str(year), "Make": "Audi", "Model": model, "Trim": "Premium"})
    from backend.enrichment import dictionary_catalog

    dictionary_catalog.invalidate_catalog_cache()
    found = find_epa_csv("Audi", "A5", 2026)
    assert found is not None and found.name == "2025_Audi_A5_EPA.csv"
