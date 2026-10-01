"""epa_csv revoked: our own composed sentence is not a source.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

import pathlib

import pytest

from backend.enrichment.trim_ladder import resolve_trim_ladder


# --- epa_csv revoked: our own composed sentence is not a source -------------


def test_epa_csv_is_not_an_admissible_store() -> None:
    """
    The table says it, and the derived sets follow. ``engineOptions`` is built by
    ``backend/scripts/import_epa_to_dictionary.py`` out of displacement, cylinder
    count and ``eng_dscr``; a bullet quoting it quotes our own synthesis.
    """
    from backend.enrichment import brochure_extract as bx

    assert bx.LADDER_BULLET_STORES["epa_csv"] == "uncited"
    assert "epa_csv" not in bx.ADMISSIBLE_LADDER_BULLET_STORES
    assert not bx.ladder_bullet_store_admissible("epa_csv")
    # The classifier still recognises the store — it is admissibility that changed.
    assert bx.ladder_bullet_store_for_source("EPA CSV") == "epa_csv"


def test_citation_register_drops_a_revoked_store() -> None:
    """One place enforces the table: a register entry from a revoked store is refused."""
    from backend.enrichment.trim_ladder import _CitationRegister

    register = _CitationRegister()
    register.add_quote(
        "6.2L V8 (Hellcat engine)",
        {"store": "epa_csv", "source": "2021_Jeep_Grand Cherokee_EPA.csv", "row": 6},
    )
    register.add_exact("6.2L V8 (Hellcat engine)", {"store": "epa_csv", "source": "x", "row": 6})
    assert register.citation_for("6.2L V8 (Hellcat engine)") is None
    assert not register

    register.add_quote(
        "Quoted brochure line",
        {"store": "brochure_text_quoted", "source": "derived/brochure_text/x.json", "page": 7},
    )
    assert register.citation_for("Quoted brochure line") is not None
    # A citation with no store at all is not admissible either.
    register.add_quote("Storeless line", {"source": "somewhere", "page": 1})
    assert register.citation_for("Storeless line") is None


def test_grand_cherokee_2021_shows_no_hellcat_engine_line() -> None:
    """
    /car/94731 is a 2021 Grand Cherokee 80th Anniversary. Its Trackhawk rung
    printed "6.2L V8 (Hellcat engine)" — the engineOptions cell of row 6 of
    2021_Jeep_Grand Cherokee_EPA.csv, which our importer composed as
    "6.2L" + "V8" + "(Hellcat engine)".
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2021, trim="80th Anniversary"
    )
    assert result is not None
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or [])).lower()
    assert "hellcat" not in everything, everything


def test_the_hellcat_engine_line_really_is_in_the_epa_store(monkeypatch) -> None:
    """Counterpart: with the gate off that same line comes back, from that same row.

    Without this, the test above would pass just as well if the Trackhawk rung
    had disappeared for an unrelated reason.
    """
    import csv

    # Read the shipped file directly rather than through find_epa_csv: another
    # test module rebuilds the real dictionary catalog index from its own tmp
    # fixture (see the note in the sibling-model test below), and this assertion
    # is about what is in the document, not about the resolver.
    path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "dictionary/epa/Jeep/2021_Jeep_Grand Cherokee_EPA.csv"
    )
    if not path.is_file():
        pytest.skip("2021 Grand Cherokee EPA CSV not present")
    with path.open(encoding="utf-8", newline="") as fh:
        cells = [(r.get("Trim") or "", r.get("engineOptions") or "") for r in csv.DictReader(fh)]
    assert any("Hellcat" in engine for _trim, engine in cells), cells

    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2021, trim="80th Anniversary"
    )
    assert result is not None
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or [])).lower()
    assert "hellcat" in everything, everything


def test_epa_store_gate_synthetic_hellcat_companion(
    scratch_dictionary_root, monkeypatch
) -> None:
    """The resolver half of the Hellcat pair, with no shipped EPA file needed.

    The test above pins the CORPUS FACT (the shipped CSV really carries the
    line) and skips wherever the dictionary is absent. This one writes the same
    row into a scratch dictionary at test time, so the resolver behaviour —
    EPA-store bullets blocked by default, restored by
    ``TRIM_ADDS_REQUIRE_PROVENANCE=0`` — is exercised on every machine.
    """
    import csv

    from backend.enrichment import dictionary_catalog
    from backend.enrichment.dictionary_paths import CANONICAL_CSV_COLUMNS

    epa_dir = scratch_dictionary_root / "epa" / "Jeep"
    epa_dir.mkdir(parents=True, exist_ok=True)
    epa = epa_dir / "2021_Jeep_Grand Cherokee_EPA.csv"
    with epa.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS))
        writer.writeheader()
        for trim_name, engine in [
            ("Laredo", "3.6L V6"),
            ("Limited", "3.6L V6"),
            ("Overland", "5.7L V8"),
            ("Trackhawk", "6.2L V8 (Hellcat engine)"),
        ]:
            writer.writerow(
                {
                    "Year": "2021",
                    "Make": "Jeep",
                    "Model": "Grand Cherokee",
                    "Trim": trim_name,
                    "engineOptions": engine,
                    "transmissionOptions": "Automatic (S8)",
                    "drivetrainOptions": "4WD",
                    "fuelType": "Gasoline",
                    "cylinders": "6",
                    "displacement": "3.6",
                }
            )
    dictionary_catalog.rebuild_catalog(dictionary_catalog.build_manifest_entries())
    dictionary_catalog.invalidate_catalog_cache()
    try:
        def bullets() -> str:
            result = resolve_trim_ladder(
                make="Jeep", model="Grand Cherokee", year=2021, trim="Limited"
            )
            return " ".join(
                b
                for s in ((result or {}).get("steps") or [])
                for b in (s.get("adds") or [])
            ).lower()

        # Adds gate at its default: the composed EPA sentence may not render.
        assert "hellcat" not in bullets()

        monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
        restored = bullets()
        assert "hellcat" in restored, restored
    finally:
        dictionary_catalog.invalidate_catalog_cache()
