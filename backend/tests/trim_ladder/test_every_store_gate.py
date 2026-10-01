"""The provenance gate covers every store, not just the brochure overlays.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

from backend.enrichment.trim_ladder import resolve_trim_ladder


# --- the gate now covers every store, not just the overlays -----------------


def test_trackhawk_hellcat_engine_does_not_render_on_a_limited() -> None:
    """
    The live report that started this: /car/282480 is a 2018 Grand Cherokee
    Limited and its ladder printed "6.2L Supercharged Hellcat V8" on the
    Trackhawk rung. That line is typed into curated/trim_ladders.json and cites
    nothing, so it does not render — even though it sits on a curated ladder,
    which the overlay-only gate never looked at.
    """
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2018, trim="Limited"
    )
    assert result is not None
    assert str(result.get("source") or "") == "curated"
    everything = " ".join(
        b for s in result["steps"] for b in (s.get("adds") or [])
    ).lower()
    assert "hellcat" not in everything, everything
    assert "6.2l supercharged" not in everything, everything


def test_the_hellcat_line_really_is_in_the_curated_store(monkeypatch) -> None:
    """Counterpart to the test above: with the gate off, that same line comes back.

    Without this the test above would pass just as well if the rung had gone
    missing for some unrelated reason.
    """
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2018, trim="Limited"
    )
    assert result is not None
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or [])).lower()
    assert "hellcat" in everything, everything


def test_every_rendered_bullet_names_a_document_and_a_place_in_it() -> None:
    """No bullet reaches a rung without a store, a source file and a page or row."""
    from backend.enrichment.brochure_extract import ADMISSIBLE_LADDER_BULLET_STORES

    vehicles = [
        # Vehicles that DO render — every one of these cites a brochure PDF we
        # still hold, so the verifier could re-open it. Without them the loop
        # below asserts nothing (the sample went silent once the trim walk and
        # the un-reopenable overlays were revoked, and the test passed anyway).
        ("Toyota", "RAV4", 2026, "XSE"),
        ("Toyota", "Highlander", 2026, "Platinum"),
        ("Nissan", "Pathfinder", 2026, "SL"),
        ("Nissan", "Titan XD", 2022, "SV"),
        ("Mazda", "Mazda6", 2018, "Grand Touring"),
        # Vehicles that render nothing, kept as the "no filler" half.
        ("Jeep", "Grand Cherokee", 2018, "Limited"),
        ("Toyota", "RAV4", 2022, "XLE"),
        ("Ram", "1500", 2025, "Limited"),
        ("Honda", "Accord", 2023, "EX-L"),
        ("Dodge", "Durango", 2020, "R/T"),
        ("Hyundai", "IONIQ 5", 2023, "SEL"),
        ("Ford", "F-150", 2024, "Lariat"),
        ("BMW", "3 Series", 2016, "328i"),
    ]
    seen_any = False
    for make, model, year, trim in vehicles:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        if not result:
            continue
        for step in result["steps"]:
            bullets = step.get("adds") or []
            cited = {str(c.get("text")) for c in step.get("adds_citations") or []}
            assert set(bullets) <= cited, (make, model, year, step["name"], bullets)
            for citation in step.get("adds_citations") or []:
                seen_any = True
                assert citation["store"] in ADMISSIBLE_LADDER_BULLET_STORES, citation
                assert str(citation.get("source") or "").strip(), citation
                assert (
                    citation.get("page") is not None or citation.get("row") is not None
                ), citation
    assert seen_any, "sample rendered no bullets at all — the assertions were vacuous"


def test_no_epa_row_cell_reaches_a_rung() -> None:
    """
    /car/89790 rendered "All-Wheel Drive" attributed to 2022_Toyota_RAV4_EPA.csv.
    Neither that cell nor the engine cell beside it may put a bullet on a rung
    now: ``drivetrainOptions`` describes the one configuration the row was filed
    for rather than something a rung ADDS, and ``engineOptions`` is a sentence
    ``import_epa_to_dictionary.build_engine_desc`` composed out of several EPA
    columns, so quoting it quotes us.
    """
    from backend.enrichment.trim_ladder import _epa_engine_citations

    # The reader still works; it is the ADMISSIBILITY of what it reads that changed.
    citations = _epa_engine_citations("Toyota", "RAV4", 2022)
    assert citations, "expected EPA rows for the 2022 RAV4"
    for _trim, engine, file_name, row_no in citations:
        assert file_name.endswith("_EPA.csv")
        assert row_no >= 2
        assert "wheel drive" not in engine.lower(), (engine, file_name, row_no)

    result = resolve_trim_ladder(make="Toyota", model="RAV4", year=2022, trim="XLE")
    if result:
        everything = " ".join(
            b for s in result["steps"] for b in (s.get("adds") or [])
        ).lower()
        assert "all-wheel drive" not in everything, everything
        assert "front-wheel drive" not in everything, everything
        for step in result["steps"]:
            for citation in step.get("adds_citations") or []:
                assert citation["store"] != "epa_csv", citation
