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


def test_find_epa_csv_jeep(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.enrichment import dictionary_paths, dictionary_catalog

    monkeypatch.setattr(dictionary_paths, "DICTIONARY_ROOT", tmp_path)
    monkeypatch.setattr(dictionary_paths, "INDEX_DIR", tmp_path / "index")
    monkeypatch.setattr(dictionary_paths, "EPA_DIR", tmp_path / "epa")
    monkeypatch.setattr(dictionary_paths, "OPTIONS_RAW_DIR", tmp_path / "options" / "raw")
    monkeypatch.setattr(dictionary_paths, "OPTIONS_STUBS_DIR", tmp_path / "options" / "stubs")
    monkeypatch.setattr(dictionary_paths, "CATALOG_DB_PATH", tmp_path / "index" / "dictionary_catalog.db")
    dictionary_catalog.invalidate_catalog_cache()

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
