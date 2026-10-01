"""EPA file identity: an EPA file has to be this model's file.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

import pathlib

import pytest

from backend.enrichment.trim_ladder import resolve_trim_ladder


# --- an EPA file has to be this model's file --------------------------------


def test_find_epa_csv_refuses_a_sibling_models_file(tmp_path, monkeypatch) -> None:
    """
    "Blazer EV" must not be served the gas Blazer's CSV. Nothing downstream
    re-checks the model — ``knowledge_engine._lookup_epa_from_dictionary_csv``
    matches rows by trim alone — so a sibling file is read as this car's specs.

    NOTE for whoever owns backend/tests/test_dictionary_catalog.py:
    ``test_find_epa_csv_jeep`` there redirects only ``dictionary_paths`` and then
    calls ``rebuild_catalog``, which writes through ``dictionary_catalog``'s own
    module-level ``MANIFEST_PATH`` / ``CATALOG_DB_PATH`` — i.e. it rebuilds the
    REAL backend/dictionary/index from a one-row tmp fixture, and every later
    EPA lookup in that process (and every later process, until someone re-runs
    ``rebuild_catalog()``) sees a 1-entry catalog. Confirmed 2026-07-31 by
    running that test alone: manifest entry_count 14,598 -> 1.
    """
    import csv as _csv

    from backend.enrichment import dictionary_catalog, dictionary_paths
    from backend.enrichment.dictionary_paths import CANONICAL_CSV_COLUMNS

    # dictionary_catalog binds these names at import time, so redirecting only
    # dictionary_paths leaves the writers (rebuild_catalog, write_manifest)
    # pointed at the REAL backend/dictionary/index — this test rebuilt the live
    # catalog from a one-row fixture the first time it was written. Both modules
    # have to be redirected.
    for module in (dictionary_paths, dictionary_catalog):
        monkeypatch.setattr(module, "DICTIONARY_ROOT", tmp_path, raising=False)
        monkeypatch.setattr(module, "INDEX_DIR", tmp_path / "index", raising=False)
        monkeypatch.setattr(module, "EPA_DIR", tmp_path / "epa", raising=False)
        monkeypatch.setattr(module, "OPTIONS_RAW_DIR", tmp_path / "options" / "raw", raising=False)
        monkeypatch.setattr(
            module, "OPTIONS_STUBS_DIR", tmp_path / "options" / "stubs", raising=False
        )
        monkeypatch.setattr(
            module, "CATALOG_DB_PATH", tmp_path / "index" / "dictionary_catalog.db", raising=False
        )
        monkeypatch.setattr(module, "MANIFEST_PATH", tmp_path / "index" / "manifest.json", raising=False)
    dictionary_catalog.invalidate_catalog_cache()
    try:
        epa = tmp_path / "2025_Chevrolet_Blazer_EPA.csv"
        with epa.open("w", newline="", encoding="utf-8") as fh:
            writer = _csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS))
            writer.writeheader()
            writer.writerow(
                {
                    "Year": "2025",
                    "Make": "Chevrolet",
                    "Model": "Blazer",
                    "Trim": "RS",
                    "engineOptions": "3.6L V6",
                }
            )
        dictionary_catalog.rebuild_catalog(dictionary_catalog.build_manifest_entries())
        dictionary_catalog.invalidate_catalog_cache()

        assert dictionary_catalog.find_epa_csv("Chevrolet", "Blazer", 2025) is not None
        assert dictionary_catalog.find_epa_csv("Chevrolet", "Blazer EV", 2025) is None
    finally:
        dictionary_catalog.invalidate_catalog_cache()


@pytest.mark.skipif(
    not (pathlib.Path(__file__).resolve().parents[2] / "dictionary" / "epa").is_dir(),
    reason="EPA dictionary not present",
)
def test_shipped_epa_files_are_never_another_models_file() -> None:
    """On the real dictionary: whatever comes back carries a row for this model."""
    from backend.enrichment.dictionary_catalog import epa_csv_is_for_model, find_epa_csv

    for make, model, year in [
        ("Jeep", "Grand Cherokee", 2021),
        ("Toyota", "RAV4", 2022),
        ("Chevrolet", "Blazer", 2025),
        ("Chevrolet", "Blazer EV", 2025),
        ("Ford", "F-250SD", 2026),
        ("Land Rover", "Range Rover Sport", 2023),
        ("Audi", "A6", 2017),
        ("Ram", "2500", 2018),
    ]:
        path = find_epa_csv(make, model, year)
        if path is None:
            continue
        assert epa_csv_is_for_model(path, make, model), (make, model, year, path.name)


def test_position_describing_copy_is_not_a_quotation() -> None:
    """
    "Mid-level trim with added convenience features…" is a sentence we write, so
    it can never carry a citation. It used to reach the page from the
    oem_knowledge and inventory ladders, which the overlay gate did not cover.
    """
    import re

    prose = re.compile(
        r"Entry-level trim|Mid-level trim|Upper trim|Highest trim level|"
        r"Sport trim with performance styling",
        re.I,
    )
    for make, model, year, trim in [
        ("Lexus", "IS 350", 2022, "F SPORT"),
        ("GMC", "Sierra 1500", 2024, "SLT"),
        ("Ford", "Edge", 2017, "SE"),
        ("Mazda", "Mazda3", 2014, "s Grand Touring"),
    ]:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        if not result:
            continue
        for step in result["steps"]:
            for bullet in step.get("adds") or []:
                assert not prose.search(bullet), (make, model, year, step["name"], bullet)


def test_store_table_is_the_only_switch() -> None:
    """
    Widening the policy is one row in LADDER_BULLET_STORES. If a later phase
    starts citing the curated ladders, flipping that row is what re-admits them —
    no call site in trim_ladder.py names a store by hand.
    """
    from backend.enrichment import brochure_extract as bx

    assert bx.ladder_bullet_store_for_source("curated") == "curated_trim_ladder"
    assert bx.ladder_bullet_store_for_source("EPA CSV") == "epa_csv"
    assert bx.ladder_bullet_store_for_source("2022_Toyota_RAV4_EPA.csv") == "epa_csv"
    assert (
        bx.ladder_bullet_store_for_source("2022_Jeep_Grand_Cherokee_Complete_Options.csv")
        == "complete_options_csv"
    )
    assert bx.ladder_bullet_store_for_source("oem_knowledge") == "oem_knowledge_prose"
    assert bx.ladder_bullet_store_for_source("inventory") == "inventory_prose"
    # An unknown producer is silent until someone adds it on purpose.
    assert not bx.ladder_bullet_store_admissible(
        bx.ladder_bullet_store_for_source("something_new_v2")
    )
    # The overlay set is derived from the same table rather than kept in parallel.
    assert bx.ADMISSIBLE_OVERLAY_SOURCES <= bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "curated_trim_ladder" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "trim_spec_sheet" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "complete_options_csv" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert "epa_csv" not in bx.ADMISSIBLE_LADDER_BULLET_STORES


def test_every_real_ladder_source_classifies_to_a_known_store() -> None:
    """Every ``source`` string the shipped ladder files actually use is accounted for."""
    import json
    import pathlib

    from backend.enrichment.brochure_extract import (
        LADDER_BULLET_STORES,
        ladder_bullet_store_for_source,
    )

    curated_dir = pathlib.Path(__file__).resolve().parents[2] / "dictionary/curated"
    sources: set[str] = set()
    for name in ("trim_ladders.json", "trim_ladders_generated.json", "trim_ladders_epa.json"):
        path = curated_dir / name
        if not path.is_file():
            continue
        for ladder in (json.loads(path.read_text(encoding="utf-8")).get("ladders") or []):
            sources.add(str(ladder.get("source") or ""))
    sources |= {"curated", "inventory", "oem_knowledge", "brochure", "EPA CSV"}
    assert len(sources) > 100
    for source in sources:
        assert ladder_bullet_store_for_source(source) in LADDER_BULLET_STORES, source


def test_gate_off_restores_the_blocked_stores(monkeypatch) -> None:
    """The whole cross-store rule is reversible with one environment variable."""
    blocked = resolve_trim_ladder(make="Jeep", model="Compass", year=2024, trim="Latitude")
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    restored = resolve_trim_ladder(make="Jeep", model="Compass", year=2024, trim="Latitude")
    assert blocked is not None and restored is not None
    blocked_lines = [b for s in blocked["steps"] for b in (s.get("adds") or [])]
    restored_lines = [b for s in restored["steps"] for b in (s.get("adds") or [])]
    assert not blocked_lines
    assert restored_lines


def test_gate_on_never_invents_a_line_the_gate_off_page_did_not_have(monkeypatch) -> None:
    """
    The refill check, as a test. Blocking a store must not let a rung reach for a
    line it was never showing. Any bullet present with the gate ON must also be
    present with it OFF — the gate only ever subtracts.
    """
    vehicles = [
        ("Jeep", "Grand Cherokee", 2018, "Limited"),
        ("Toyota", "RAV4", 2022, "XLE"),
        ("Nissan", "Sentra", 2026, "SV"),
        ("Ram", "1500", 2025, "Limited"),
        ("Dodge", "Durango", 2020, "R/T"),
        ("Lexus", "IS 350", 2022, "F SPORT"),
    ]
    on = {}
    for make, model, year, trim in vehicles:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        on[(make, model, year, trim)] = {
            b for s in ((result or {}).get("steps") or []) for b in (s.get("adds") or [])
        }
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    for key, gated in on.items():
        make, model, year, trim = key
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        ungated = {
            b for s in ((result or {}).get("steps") or []) for b in (s.get("adds") or [])
        }
        assert gated <= ungated, (key, sorted(gated - ungated))
