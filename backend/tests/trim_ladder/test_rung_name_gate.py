"""The rung-name gate (TRIM_RUNGS_REQUIRE_PROVENANCE).

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

from backend.enrichment.trim_ladder import resolve_trim_ladder


# ===========================================================================
# THE RUNG-NAME GATE (TRIM_RUNGS_REQUIRE_PROVENANCE)
#
# The rest of this package runs with the gate off (see ``_legacy_rung_gate_off``
# in conftest.py). Tests here that request ``rung_gate_on`` run the way
# production does.
#
# The claim a ladder makes is "these are the trims of this vehicle". A rung is
# allowed to make it only when we can point at something outside our own
# synthesis: an ACTIVE row in ``cars`` for this exact year/make/model whose trim
# string EQUALS the rung's displayed name once case and punctuation are removed,
# or a brochure citation for this exact model year that
# ``verify_trim_citations.py`` re-opened the PDF and confirmed, filed under a
# heading that equals the rung name by the same rule.
#
# EXACT, not fuzzy, and the tests below pin that specifically — the first version
# of this gate matched at a >= 80 similarity score and consequently minted rung
# names out of near-miss listing labels while stamping them "active_inventory".
# ===========================================================================


def _ladder_def(*names: str) -> dict:
    return {
        "id": "test_ladder",
        "make": "Ram",
        "models": ["1500"],
        "label": "Ram 1500 trim lineup",
        "source": "curated",
        "steps": [{"name": n, "aliases": [], "adds": []} for n in names],
    }


def _build(ladder_def, *, year=2023, trim="Limited", overlay=None):
    from backend.enrichment.trim_ladder import _build_ladder_result

    return _build_ladder_result(
        ladder_def,
        make="Ram",
        model="1500",
        year=year,
        trim=trim,
        brochure_overlay=overlay,
    )


def test_a_rung_with_no_listing_and_no_citation_is_not_shown(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(
        tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7), ("Laramie", 9))
    )
    result = _build(_ladder_def("Limited", "Laramie Longhorn", "Laramie", "Lone Star"))
    assert [s["name"] for s in result["steps"]] == ["Limited", "Laramie"]


def test_every_shown_rung_names_what_justifies_it(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl
    from backend.enrichment.brochure_extract import ladder_rung_store_admissible

    monkeypatch.setattr(
        tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7), ("Laramie", 9))
    )
    result = _build(_ladder_def("Limited", "Laramie", "Tradesman"))
    assert result["steps"]
    for step in result["steps"]:
        prov = step["name_provenance"]
        assert ladder_rung_store_admissible(prov.get("store")), prov
        assert prov["active_listings"] >= 1


def test_the_evidence_is_this_model_year_not_a_neighbouring_one(rung_gate_on, monkeypatch) -> None:
    """A trim listed in 2023 does not put that rung on a 2024 car."""
    from backend.enrichment import trim_ladder as tl

    tl._active_trims_by_make_year.cache_clear()
    calls: dict[int, int] = {}

    def fake(make_norm, year):
        calls[year] = calls.get(year, 0) + 1
        return (("1500", "Limited", 7),) if year == 2023 else ()

    monkeypatch.setattr(tl.evidence, "_active_trims_by_make_year", fake)
    monkeypatch.setattr(tl.evidence, "_active_make_spellings", lambda: {"ram": ("Ram",)})
    assert tl._inventory_rung_evidence("Ram", "1500", 2023) == (("Limited", 7),)
    assert tl._inventory_rung_evidence("Ram", "1500", 2024) == ()
    assert set(calls) == {2023, 2024}


def test_a_trim_of_a_different_model_is_not_evidence(rung_gate_on, monkeypatch) -> None:
    """"1500 Classic" Warlocks do not put a Warlock rung on a "1500"."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_active_make_spellings", lambda: {"ram": ("Ram",)})
    monkeypatch.setattr(
        tl.evidence,
        "_active_trims_by_make_year",
        lambda m, y: (("1500", "Limited", 7), ("1500 Classic", "Warlock", 4)),
    )
    assert tl._inventory_rung_evidence("Ram", "1500", 2023) == (("Limited", 7),)


def test_one_listing_justifies_at_most_one_rung(rung_gate_on, monkeypatch) -> None:
    """A "Sport" on a lot is not evidence that a "Sport Touring" is."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Sport", 3),))
    result = _build(_ladder_def("Sport Touring", "Sport"), trim="Sport")
    assert [s["name"] for s in result["steps"]] == ["Sport"]


def test_an_alias_is_not_evidence_for_the_name_it_is_an_alias_of(
    rung_gate_on, monkeypatch
) -> None:
    """An alias table is our own synthesis. An observed "Limited" may print a
    rung called "Limited"; it may not print one called "Limited Plus" on the
    strength of "Limited" appearing in that rung's alias list."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 5),))
    steps = _ladder_def("Limited Plus", "Tradesman")["steps"]
    steps[0]["aliases"] = ["Limited"]
    hits = tl._exact_inventory_rung_hits(steps, make="Ram", model="1500", year=2023)
    assert hits == {}


def test_a_near_miss_listing_label_never_mints_a_rung_name(
    rung_gate_on, monkeypatch
) -> None:
    """The live regression this gate had to be rewritten for.

    2026 Ford Explorer: 3 active listings spelled "Sport Utility" (a body style
    a dealer typed into the trim field), a ladder def carrying a "Sport" step,
    and a fuzzy >= 80 prefix match produced a rung named "Sport" — a trim Ford
    does not sell — stamped ``{"store": "active_inventory"}``. ``Tremor®`` is in
    the same list to pin the one normalisation that IS allowed.
    """
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(
        tl.evidence,
        "_inventory_rung_evidence",
        lambda *a, **k: (("Sport Utility", 3), ("Tremor®", 17), ("Tremor", 37)),
    )
    steps = _ladder_def("Tremor", "Sport")["steps"]
    hits = tl._exact_inventory_rung_hits(steps, make="Ford", model="Explorer", year=2026)
    assert hits == {0: (54, ("Tremor", "Tremor®"))}


def test_a_derived_spelling_is_not_an_observed_spelling(rung_gate_on, monkeypatch) -> None:
    """``canonical_trim_name`` rewrites "Platinum RWD" to "Platinum". That is a
    guess we make, not a string a dealer typed, so it is not rung evidence."""
    from backend.enrichment import trim_ladder as tl
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name

    assert canonical_trim_name("Platinum RWD", "Ford", "Explorer") == "Platinum"
    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Platinum RWD", 1),))
    steps = _ladder_def("Platinum", "Tremor")["steps"]
    assert tl._exact_inventory_rung_hits(steps, make="Ford", model="Explorer", year=2026) == {}


def test_the_rung_provenance_names_the_spellings_it_rests_on(
    rung_gate_on, monkeypatch
) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(
        tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7), ("LIMITED", 2))
    )
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"))
    prov = result["steps"][0]["name_provenance"]
    assert prov["store"] == "active_inventory"
    assert prov["active_listings"] == 9
    assert prov["observed_trims"] == ["LIMITED", "Limited"]


def test_a_brochure_heading_justifies_only_the_rung_it_names(
    rung_gate_on, monkeypatch
) -> None:
    """A verified "XSE Premium" heading does not justify a rung printed "XSE"."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel GT": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel GT": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2023_Ram_1500.pdf",
                    "page": 14,
                    "verified": True,
                }
            ]
        },
    }
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"), overlay=overlay)
    assert [s["name"] for s in result["steps"]] == ["Limited"]


def test_a_verified_citation_justifies_a_rung_with_no_listings(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2023_Ram_1500.pdf",
                    "page": 14,
                    "verified": True,
                }
            ]
        },
    }
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"), overlay=overlay)
    names = [s["name"] for s in result["steps"]]
    assert names == ["Limited", "Rebel"]
    rebel = result["steps"][1]
    assert rebel["name_provenance"]["store"] == "brochure_text_quoted"


def test_an_unverified_citation_does_not_justify_a_rung(rung_gate_on, monkeypatch) -> None:
    """The verifier's stamp, not the mere presence of a page number."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2023_Ram_1500.pdf",
                    "page": 14,
                }
            ]
        },
    }
    result = _build(_ladder_def("Limited", "Rebel", "Tradesman"), overlay=overlay)
    assert [s["name"] for s in result["steps"]] == ["Limited"]


def test_trims_available_alone_never_justifies_a_rung(rung_gate_on) -> None:
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "trims_available": ["Tradesman", "Big Horn", "Rebel", "Limited"],
        "adds_by_trim": {},
        "adds_provenance": {},
    }
    assert verified_overlay_trim_names(overlay, for_year=2023) == []


def test_a_neighbouring_years_citation_justifies_nothing(rung_gate_on) -> None:
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay = {
        "year": 2022,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {
                    "text": "Bilstein shock absorbers",
                    "source": "2022_Ram_1500.pdf",
                    "page": 14,
                    "verified": True,
                }
            ]
        },
    }
    assert verified_overlay_trim_names(overlay, for_year=2022) == ["Rebel"]
    assert verified_overlay_trim_names(overlay, for_year=2023) == []


def test_an_llm_overlay_citation_justifies_nothing(rung_gate_on) -> None:
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay = {
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "source": "brochure_llm",
        "adds_by_trim": {"Rebel": ["Bilstein shock absorbers"]},
        "adds_provenance": {
            "Rebel": [
                {"text": "Bilstein shock absorbers", "source": "x.pdf", "page": 1, "verified": True}
            ]
        },
    }
    assert verified_overlay_trim_names(overlay, for_year=2023) == []


def test_a_single_justified_rung_is_not_a_ladder(rung_gate_on, monkeypatch) -> None:
    """One rung tells a shopper nothing about where their trim sits, and the one
    that survives is often not even their own trim (a 2025 Ford Escape Base
    resolved to a lone "Platinum" rung). 7,178 active cars land here."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    monkeypatch.setattr(tl.selection, "_pick_ladder_def", lambda *a, **k: _ladder_def("Limited", "Rebel"))
    monkeypatch.setattr(
        tl, "_generic_trim_ladder_def", lambda *a, **k: _ladder_def("Limited", "Rebel")
    )
    assert resolve_trim_ladder(make="Ram", model="1500", year=2023, trim="Limited") is None


def test_the_generic_oem_fallback_cannot_smuggle_rungs_back_in(rung_gate_on, monkeypatch) -> None:
    """``resolve_trim_ladder`` falls back to a hand-typed generic ladder four
    separate times. Each fallback re-enters ``_build_ladder_result``, so each
    faces the same test — emptying one producer must not be refillable."""
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: ())
    result = resolve_trim_ladder(make="Ram", model="1500", year=2023, trim="Limited")
    assert result is None


def test_kill_switch_restores_the_unjustified_rungs(rung_gate_on, monkeypatch) -> None:
    from backend.enrichment import trim_ladder as tl

    monkeypatch.setattr(tl.evidence, "_inventory_rung_evidence", lambda *a, **k: (("Limited", 7),))
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")
    result = _build(_ladder_def("Limited", "Laramie Longhorn", "Laramie"))
    assert [s["name"] for s in result["steps"]] == ["Limited", "Laramie Longhorn", "Laramie"]


def test_the_query_really_reads_active_rows(rung_gate_on, monkeypatch, tmp_path) -> None:
    """End-to-end over a real inventory database, not a stubbed evidence list.

    Everything above stubs ``_inventory_rung_evidence``, which leaves the SQL,
    the make-spelling resolution and the model normalisation untested. This test
    seeds a throwaway inventory and drives the whole path:

      * "CHEVROLET" and "Chevrolet" are both live spellings of one make and must
        be counted together — a plain ``LOWER(make) = ?`` splits them,
      * a SOLD (``listing_active = 0``) Z71 is not a trim anybody is listing,
      * a 2024 High Country does not put that rung on a 2023 car,
      * a "Silverado 2500HD" LTZ does not put that rung on a "Silverado 1500".
    """
    from backend.db import inventory_db
    from backend.db.repositories import base_repo
    from backend.enrichment import trim_ladder as tl

    db = str(tmp_path / "inv.db")
    monkeypatch.setattr(inventory_db, "DB_PATH", db)
    monkeypatch.setattr(base_repo, "DB_PATH", db)
    inventory_db.init_inventory_db()
    rows = [
        ("V1", 2023, "Chevrolet", "Silverado 1500", "LT", 1),
        ("V2", 2023, "CHEVROLET", "Silverado 1500", "LT Trail Boss", 1),
        ("V3", 2023, "Chevrolet", "Silverado 1500", "Z71", 0),
        ("V4", 2024, "Chevrolet", "Silverado 1500", "High Country", 1),
        ("V5", 2023, "Chevrolet", "Silverado 2500HD", "LTZ", 1),
    ]
    with inventory_db.db_conn() as conn:
        cur = conn.cursor()
        for row in rows:
            cur.execute(
                "INSERT INTO cars (vin, year, make, model, trim, listing_active) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
        conn.commit()
    tl._active_make_spellings.cache_clear()
    tl._active_trims_by_make_year.cache_clear()

    observed = dict(tl._inventory_rung_evidence("Chevrolet", "Silverado 1500", 2023))
    assert observed == {"LT": 1, "LT Trail Boss": 1}

    ladder = {
        "id": "t",
        "make": "Chevrolet",
        "models": ["Silverado 1500"],
        "label": "l",
        "source": "curated",
        "steps": [
            {"name": n, "aliases": [], "adds": []}
            for n in ("High Country", "LTZ", "LT Trail Boss", "LT", "Z71", "WT")
        ],
    }
    from backend.enrichment.trim_ladder import _build_ladder_result

    result = _build_ladder_result(
        ladder, make="Chevrolet", model="Silverado 1500", year=2023, trim="LT"
    )
    assert [s["name"] for s in result["steps"]] == ["LT Trail Boss", "LT"]
    tl._active_make_spellings.cache_clear()
    tl._active_trims_by_make_year.cache_clear()


def test_brochure_adds_key_refuses_a_differently_named_section() -> None:
    """A rung may only read the brochure section that NAMES it.

    The removed whole-token containment fallback took the heading with the most
    tokens all present in the step name, which is how the 2022 Dodge Charger
    rung "SRT Hellcat Redeye Widebody" read the section filed under
    "SRT HELLCAT WIDEBODY" — a 797 hp car's rung printing a 717 hp car's
    equipment. Measured on the live fleet the day it was removed: 12 rendered
    ladder steps bound to a differently-named heading, all of them that one
    rung, one of them as a car's own "This vehicle" rung.
    """
    from backend.enrichment.trim_ladder import _lookup_brochure_adds_key

    adds = {"SRT HELLCAT WIDEBODY": ["797-hp supercharged 6.2L HEMI V8"]}
    assert (
        _lookup_brochure_adds_key(
            "SRT Hellcat Redeye Widebody", [], adds, make="Dodge", model="Charger"
        )
        is None
    )
    # The rung that IS that heading still reads it, case and spacing aside.
    assert (
        _lookup_brochure_adds_key(
            "SRT Hellcat Widebody", [], adds, make="Dodge", model="Charger"
        )
        == "SRT HELLCAT WIDEBODY"
    )


def test_brochure_adds_key_refuses_a_canonical_truncation() -> None:
    """``canonical_trim_name`` truncates one trim onto another's name.

    The removed ``_trim_identity_keys`` overlap score ran both labels through
    it, so "Sport Prestige" and a heading called "Sport" could be declared one
    trim by our own reduction of both. Verified directly rather than assumed:
    ``canonical_trim_name("Sport Prestige", "Acura", "TLX") == "Sport"``.
    """
    from backend.enrichment.trim_ladder import _lookup_brochure_adds_key
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name

    assert canonical_trim_name("Sport Prestige", "Acura", "TLX") == "Sport"
    adds = {"Sport": ["19-inch alloy wheels"]}
    assert (
        _lookup_brochure_adds_key("Sport Prestige", [], adds, make="Acura", model="TLX")
        is None
    )


def test_brochure_adds_key_ignores_aliases_and_fails_closed_on_ties() -> None:
    """Aliases cannot bind a section, and two candidate sections bind neither."""
    from backend.enrichment.trim_ladder import _lookup_brochure_adds_key

    # An alias list cannot reach a section the rung name does not spell.
    adds = {"Raptor R": ["700-hp supercharged 5.2L V8"]}
    assert (
        _lookup_brochure_adds_key("Raptor", ["Raptor R"], adds, make="Ford", model="F-150")
        is None
    )
    # Two headings both reduce to "Limited" once the drivetrain token is
    # dropped; the document cannot say which is this rung's, so neither is used.
    two = {"Limited 4WD": ["4WD"], "Limited RWD": ["RWD"]}
    assert _lookup_brochure_adds_key("Limited", [], two, make="Toyota", model="Tundra") is None
    # ...but an exact heading is never ambiguous with anything.
    with_exact = dict(two)
    with_exact["Limited"] = ["shared"]
    assert (
        _lookup_brochure_adds_key("Limited", [], with_exact, make="Toyota", model="Tundra")
        == "Limited"
    )
