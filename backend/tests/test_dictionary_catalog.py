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


@pytest.mark.skipif(not DICTIONARY_ROOT.is_dir(), reason="dictionary not present")
def test_writing_the_real_manifest_is_blocked() -> None:
    """The exact move that destroyed the catalog now raises instead.

    Goes through ``dictionary_catalog``'s OWN ``MANIFEST_PATH`` -- the private
    copy that made redirecting ``dictionary_paths`` insufficient -- and through
    ``write_manifest()``, the writer ``rebuild_catalog()`` calls first.
    """
    from backend.enrichment import dictionary_catalog
    from backend.tests.conftest import RealDictionaryWriteBlocked

    before = dictionary_catalog.MANIFEST_PATH.read_bytes()
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
    assert dictionary_catalog.MANIFEST_PATH.read_bytes() == before


@pytest.mark.skipif(not DICTIONARY_ROOT.is_dir(), reason="dictionary not present")
def test_the_2026_07_31_incident_shape_is_now_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replay the exact unsafe pattern: redirect ``dictionary_paths``, then rebuild.

    That is what a test did on 2026-07-31, and because ``dictionary_catalog``
    holds its own copies of the index paths the rebuild landed on the real index
    (entry_count 14,598 -> 1). The same code now raises before it can write, and
    the manifest is byte-identical afterwards.
    """
    from backend.enrichment import dictionary_catalog, dictionary_paths
    from backend.tests.conftest import RealDictionaryWriteBlocked

    before = dictionary_catalog.MANIFEST_PATH.read_bytes()
    for attr, value in (
        ("DICTIONARY_ROOT", tmp_path),
        ("INDEX_DIR", tmp_path / "index"),
        ("EPA_DIR", tmp_path / "epa"),
        ("CATALOG_DB_PATH", tmp_path / "index" / "dictionary_catalog.db"),
    ):
        monkeypatch.setattr(dictionary_paths, attr, value)

    with pytest.raises(RealDictionaryWriteBlocked):
        rebuild_catalog([], enrich_derived=False)

    assert dictionary_catalog.MANIFEST_PATH.read_bytes() == before


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
