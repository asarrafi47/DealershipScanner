""""What this trim adds" ranking and the bullet gate's narrative/derived-comparison rules.

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

import pathlib

from backend.enrichment.trim_ladder import resolve_trim_ladder
from backend.tests.trim_ladder._helpers import _all_bullets


# --- "what this trim adds" ranking ------------------------------------------


def test_trim_add_category_ranks_hardware_above_cabin() -> None:
    from backend.enrichment.trim_ladder_knowledge import trim_add_category

    def rank(text: str) -> int:
        return trim_add_category(text)[1]

    assert rank("5.7L HEMI V8 engine") > rank("Quadra-Lift air suspension system")
    assert rank("Quadra-Lift air suspension system") > rank(
        "Uconnect 4C NAV with 8.4-inch display"
    )
    assert rank("Uconnect 4C NAV with 8.4-inch display") > rank(
        "Alpine Premium Audio System with 9 speakers and 506-watt amplifier"
    )
    assert rank("Alpine Premium Audio System with 9 speakers and 506-watt amplifier") > rank(
        "Three-Zone Automatic Temperature Control with infrared sensors"
    )
    assert rank("Three-Zone Automatic Temperature Control with infrared sensors") > rank(
        "Perforated leather-wrapped steering wheel"
    )


def test_trim_add_category_keys() -> None:
    from backend.enrichment.trim_ladder_knowledge import trim_add_category

    assert trim_add_category("5.7L HEMI V8 engine")[0] == "powertrain"
    assert trim_add_category("Quadra-Trac II with 2-speed transfer case")[0] == "drivetrain"
    assert trim_add_category("Four-corner air suspension")[0] == "suspension"
    assert trim_add_category("Brembo six-piston front calipers")[0] == "brakes"
    assert trim_add_category("Max towing capacity of 8,700 lb")[0] == "capability"
    assert trim_add_category("Three-row seating with 3rd-row 50/50 split-folding seats")[0] == (
        "seating"
    )
    # A heated second row is comfort content, not a seating-capacity change.
    assert trim_add_category("2nd-row heated seats")[0] == "seat_comfort"


def test_universal_trim_adds_are_year_aware() -> None:
    from backend.enrichment.trim_ladder_knowledge import is_universal_trim_add

    assert is_universal_trim_add("Security alarm system", year=2020) is True
    assert is_universal_trim_add("Bright scuff plates in cargo area", year=2020) is True
    assert is_universal_trim_add("Electronic Stability Control", year=2020) is True
    assert is_universal_trim_add("ParkView rear back-up camera with display", year=2020) is True
    # Federal rear-visibility rule starts at MY2018 — before that it really was
    # a trim differentiator, so the same words must survive on an older car.
    assert is_universal_trim_add("ParkView rear back-up camera with display", year=2009) is False
    assert is_universal_trim_add("Electronic Stability Control", year=2004) is False
    assert is_universal_trim_add("Quadra-Lift air suspension system", year=2020) is False


def test_rank_trim_adds_leads_with_hardware_and_drops_commodity() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        [
            "Security alarm system",
            "Bright scuff plates in cargo area",
            "Perforated leather-wrapped steering wheel with audio and cruise controls",
            "Uconnect 4C NAV with 8.4-inch display and 1-year trial subscription",
            "SiriusXM Traffic Plus with 5-year trial subscription",
            "5.7L HEMI V8 engine",
        ],
        year=2020,
        max_items=8,
    )
    assert out[0] == "5.7L HEMI V8 engine"
    assert "Uconnect 4C NAV with 8.4-inch display" in out
    joined = " ".join(out).lower()
    assert "security alarm" not in joined
    assert "scuff plates" not in joined
    assert "trial subscription" not in joined
    assert "siriusxm" not in joined


def test_rank_trim_adds_keeps_something_when_everything_is_commodity() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        ["Security alarm system", "Floor mats", "Cup holders"],
        year=2020,
        max_items=8,
    )
    # Blanking the trim sends the caller into generic marketing prose, which is
    # worse than a weak-but-true bullet.
    assert out


def test_rank_trim_adds_drops_bare_spec_labels() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        ["All-Wheel Drive", "8-Speed Automatic", "Quadra-Lift air suspension system"],
        year=2021,
        max_items=8,
    )
    assert out == ["Quadra-Lift air suspension system"]


def test_rank_trim_adds_is_stable_within_a_category() -> None:
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    out = rank_trim_adds(
        [
            "Heated steering wheel",
            "Three-Zone Automatic Temperature Control with infrared sensors",
        ],
        year=2020,
        max_items=8,
    )
    assert out == [
        "Heated steering wheel",
        "Three-Zone Automatic Temperature Control with infrared sensors",
    ]


def test_strip_trial_subscription_clause() -> None:
    from backend.enrichment.trim_ladder_knowledge import strip_trial_subscription_clause

    assert (
        strip_trial_subscription_clause(
            "Uconnect 4C NAV with 8.4-inch display and 1-year trial subscription"
        )
        == "Uconnect 4C NAV with 8.4-inch display"
    )
    assert strip_trial_subscription_clause("Heated steering wheel") == "Heated steering wheel"


def test_no_derived_engine_comparison_survives() -> None:
    """The (displacement, cylinders) engine-delta machinery must stay deleted.

    It shipped "2.0L Hurricane I4 engine - up from the 3.6L V6" onto a car whose
    own engine IS the 2.0L I4, because nothing in it ever compared direction, and
    it resolved a step by its aliases so an F-150 "Raptor" inherited the "Raptor R"
    5.2L V8. Neither premise can be repaired from data we hold: cars.forced_induction
    is wrong on real rows and epa_extended_specs.horsepower is null/degenerate.
    """
    import backend.enrichment.trim_ladder as tl

    for gone in (
        "_engine_step_up_bullet",
        "_engine_by_trim_from_inventory",
        "_dominant_engine_by_trim",
        "_inventory_trim_engine_rows",
        "_engine_display_label",
    ):
        assert not hasattr(tl, gone), f"{gone} came back - so did the false bullet"
    # No f-string / literal in the module may build a directional engine phrase.
    src = pathlib.Path(tl.__file__).read_text()
    code = "\n".join(ln for ln in src.split("\n") if not ln.lstrip().startswith("#"))
    assert "up from the" not in code, "the directional 'up from' phrasing is back"


def test_f150_raptor_never_inherits_the_raptor_r_v8() -> None:
    """Cited repro #1: a 2025 F-150 Raptor is a 3.5L V6 truck, not a 5.2L V8 one."""
    result = resolve_trim_ladder(make="Ford", model="F-150", year=2025, trim="Raptor")
    steps = {s["name"]: (s.get("adds") or []) for s in result["steps"]}
    raptor = " ".join(steps.get("Raptor", [])).lower()
    assert "up from" not in raptor, steps.get("Raptor")
    # "Raptor R adds supercharged V8" is fine - it names the OTHER trim. A bare
    # claim that THIS trim runs a 5.2 is not.
    assert "5.2l v8 engine" not in raptor, steps.get("Raptor")


def test_grand_cherokee_limited_makes_no_backwards_engine_claim() -> None:
    """Cited repro #2: car id 139652 is a 2.0L I4; nothing may call it a step up."""
    result = resolve_trim_ladder(make="Jeep", model="Grand Cherokee", year=2026, trim="Limited")
    everything = " ".join(
        b for s in result["steps"] for b in (s.get("adds") or [])
    ).lower()
    assert "up from" not in everything, everything


def test_brochure_trim_walk_is_revoked_because_it_composed_its_bullet() -> None:
    """
    ``brochure_trim_walk`` used to emit ``f"{engine} — added over the {base}"``.

    Only ``engine`` was printed in the brochure. The em dash, the phrase and the
    baseline trim name (read out of OUR ladder definition) were ours, and the
    composed string was then handed to the citation register, so the gate
    certified a sentence no document contains. The store is uncited now and the
    producer returns nothing at all — the same shape as the ``epa_csv``
    revocation before it.
    """
    from backend.enrichment.brochure_extract import (
        LADDER_BULLET_STORES,
        ladder_bullet_store_admissible,
    )
    from backend.enrichment.trim_ladder import (
        _brochure_engine_bullet_by_step,
        _brochure_engine_step_ups,
    )

    assert LADDER_BULLET_STORES["brochure_trim_walk"] == "uncited"
    assert not ladder_bullet_store_admissible("brochure_trim_walk")

    steps = [{"name": "R/T", "aliases": []}, {"name": "GT", "aliases": []}]
    assert _brochure_engine_bullet_by_step(steps, "Nonexistent", "Nothing", 2020) == {}
    # The 2020 Durango is the case that used to produce "5.7L HEMI V8 — added
    # over the GT". The underlying parser still finds the page (it is the honest
    # half of the job); the bullet builder refuses to dress it up.
    assert _brochure_engine_bullet_by_step(steps, "Dodge", "Durango", 2020) == {}
    assert _brochure_engine_step_ups("Dodge", "Durango", 2020), (
        "parser should still read the trim-walk page; only the bullet is withheld"
    )


def test_no_rendered_bullet_uses_the_composed_step_up_wording() -> None:
    """The composed phrase must not reach a rung by any route."""
    for make, model, year, trim in [
        ("Dodge", "Durango", 2020, "R/T"),
        ("Dodge", "Charger", 2022, "R/T"),
        ("Nissan", "Titan XD", 2022, "SV"),
        ("Toyota", "RAV4", 2026, "XSE"),
    ]:
        result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)
        for step in (result or {}).get("steps") or []:
            for bullet in step.get("adds") or []:
                assert "added over the" not in bullet.lower(), (make, model, year, bullet)
            for citation in step.get("adds_citations") or []:
                assert citation.get("store") != "brochure_trim_walk", citation


def test_lineup_standard_equipment_is_not_a_trim_add() -> None:
    """A row the brochure grid marks standard in every column is not an "add"."""
    from backend.enrichment.trim_ladder import _lineup_standard_features, _suppress_non_adds

    grid = _lineup_standard_features("Dodge", "Durango", 2020)
    assert grid, "2020 Durango brochure grid should parse"
    steps = [
        {
            "adds": [
                "Three-Zone Automatic Temperature Control with infrared sensors",
                "Alpine Premium Audio System with 9 speakers and 506-watt amplifier",
                "Rear ParkSense Park Assist System with obstacle detection",
            ]
        },
        {"adds": ["Rear ParkSense Park Assist System with obstacle detection"]},
        {"adds": ["Three-row seating with 3rd-row 50/50 split-folding seats"]},
    ]
    _suppress_non_adds(steps, make="Dodge", model="Durango", year=2020)
    top = steps[0]["adds"]
    # Standard on all five columns of the brochure grid -> not an R/T add.
    assert not any("Three-Zone" in b for b in top), top
    # Verbatim duplicate of a LOWER rung -> not an add either.
    assert not any("ParkSense" in b for b in top), top
    # Genuinely R/T-only content survives.
    assert any("Alpine" in b for b in top), top
    assert steps[1]["adds"] == ["Rear ParkSense Park Assist System with obstacle detection"]


def test_durango_2020_renders_only_what_the_reopened_pdf_prints() -> None:
    """
    2020 Durango: was the showcase for the trim walk. Two things happened to it.

    * The R/T's first bullet was "5.7L HEMI V8 — added over the GT". Only the
      engine name was printed; the dash, the phrase and the baseline trim were
      ours (see the ``brochure_trim_walk`` revocation). That store is uncited,
      so the composed line is gone and stays gone.
    * The overlay bullets went silent too, but for a different reason: we did
      not hold ``2020_Dodge_Durango_Brochure.pdf``, so none of its 44 citations
      could be re-opened. The PDF has since been reacquired and
      ``verify_trim_citations.py --apply`` re-read every cited page; 42 of the
      44 are printed there and render again, 2 are not and stay silent.

    So the assertion is no longer "says nothing" — it is "says only what the
    verifier re-read off the page". Every rendered bullet must carry a
    ``verified`` citation, and the composed claims must still be absent.
    """
    import json

    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    raw = json.loads(
        (TRIM_ADDS_BY_YEAR_DIR / "2020__dodge__durango.json").read_text(encoding="utf-8")
    )
    signed = {
        e["text"]
        for v in raw["adds_provenance"].values()
        for e in v
        if e.get("verified") is True
    }
    assert signed

    result = resolve_trim_ladder(make="Dodge", model="Durango", year=2020, trim="GT")
    assert result is not None
    by_name = {s["name"]: (s.get("adds") or []) for s in result["steps"]}
    assert "R/T" in by_name and "GT" in by_name
    rendered = [b for adds in by_name.values() for b in adds]
    assert rendered, by_name
    for bullet in rendered:
        assert bullet in signed, bullet

    everything = " ".join(rendered).lower()
    # The specific false claims this vehicle used to be the regression case for.
    # "hemi" is no longer among them: "5.7L HEMI® V8 engine" on its own IS
    # printed on the cited page and verifies. What was fabricated was the
    # comparison clause, not the engine name.
    assert "air suspension" not in everything
    assert "three-zone" not in everything
    assert "added over the" not in everything


def test_rank_trim_adds_never_drops_content_while_slots_are_free() -> None:
    """The per-category cap reorders; it must not delete inside the budget.

    _MAX_PER_CATEGORY=3 used to apply to the catch-all "other" bucket, so six
    unclassified-but-real bullets came back as three even with max_items=8.
    """
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds, trim_add_category

    bullets = [
        "Mopar Finishing Package with paint protection film",
        "Interior LED courtesy group",
        "Semi-gloss black exhaust tips",
        "Class IV receiver hookup group",
        "Mopar splash guard set",
        "Underbody protection group",
    ]
    assert {trim_add_category(b)[0] for b in bullets} == {"other"}, "fixture must be unclassified"
    out = rank_trim_adds(bullets, year=2026, max_items=8)
    assert len(out) == len(bullets), out
    assert set(out) == set(bullets)


def test_rank_trim_adds_still_spreads_a_dominant_named_category() -> None:
    """Four audio bullets must not bury the sunroof — but none may vanish."""
    from backend.enrichment.trim_ladder_knowledge import rank_trim_adds

    bullets = [
        "Bose 12-speaker audio system",
        "Harman Kardon 16-speaker audio system",
        "JBL 9-speaker audio system",
        "Revel 14-speaker audio system",
        "Panoramic sunroof",
    ]
    out = rank_trim_adds(bullets, max_items=8)
    assert len(out) == 5, out
    assert out.index("Panoramic sunroof") < out.index("Revel 14-speaker audio system"), out
    # A genuinely tight budget still truncates, and does so from the bottom.
    assert len(rank_trim_adds(bullets, max_items=3)) == 3


def test_lineup_narrative_and_orphaned_spec_cells_are_not_trim_adds() -> None:
    """Wikipedia model-history prose scraped into per-trim cells must not render."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    for junk in (
        "In terms of trim-level changes, the STX is now offered as a separate trim level",
        "Added to the lineup was a new Tremor trim level",
        "L PowerBoost twin-turbo V6",
        "L Cyclone V6",
        "L Power Stroke turbo V6",
        "Gasoline Hybrid:",
        "Electric motor 35 kW (47 hp) BorgWarner HVH250 (hybrid)",
        "Front-engine, rear-wheel drive",
        "Automatic (S10)",
    ):
        assert is_generic_trim_add(junk), junk

    for keep in (
        "3.5L V6 Twin Turbo",
        "Alpine Premium Audio System with 9 speakers and subwoofer",
        "Power liftgate",
        "12-inch Sync 4 screen (2024+)",
    ):
        assert not is_generic_trim_add(keep), keep


def test_f150_2025_ladder_carries_no_fabricated_engine_bullets() -> None:
    """None of the mangled Wikipedia engine cells may reach a rendered rung."""
    result = resolve_trim_ladder(make="Ford", model="F-150", year=2025, trim="XLT")
    everything = " ".join(b for s in result["steps"] for b in (s.get("adds") or []))
    low = everything.lower()
    for bogus in ("cyclone", "power stroke", "electric motor 35", "l powerboost twin-turbo"):
        assert bogus not in low, everything


def test_bullet_gate_rejects_narrative_and_derived_comparisons() -> None:
    """The gate is the contract: quotable spec text renders, prose does not."""
    from backend.enrichment.trim_spec_extractor import (
        is_derived_comparison_bullet,
        is_displayable_trim_bullet,
    )

    for prose in (
        "In the United States, the 4Runner carried over the same engine options from the",
        'The optional 4WD systems were full-time on V8 models while "Multi-Mode" or part-',
        "Most turbocharged 4Runners were equipped with the SR5 package, and all turbo tru",
        "Based on a design proposal originally used in the development of the previous-ge",
        "The XLE models offered leather seats and a wood trim package.",
        "The Base and LX both come standard with the 2.4-l",
        "Terms of Use and Master Data Consent. Data charges may apply.",
    ):
        assert not is_displayable_trim_bullet(prose), prose

    for derived in (
        "Upgrades Engine Options: 6.7L V6 Turbo; Automatic 6-spd (was Automatic 8-spd)",
        "Adds Engine Options: Layout Front-engine, front-wheel-drive",
        "Automatic 6-spd (was Automatic 8-spd",
    ):
        assert is_derived_comparison_bullet(derived), derived
        assert not is_displayable_trim_bullet(derived), derived

    for keep in (
        "5.7L HEMI V8 engine — added over the GT",
        "6.7L Cummins Turbo Diesel I6",
        "Locking rear differential",
        "20-in. split 6-spoke alloy wheels with P245/60R20 tires",
        "4x4 drivetrain with electronic shift-on-the-fly transfer case",
        (
            "SRT-inspired power dome hood with functional heat extractors, SRT-style front and "
            "rear fascias, body-color grille surround, gloss-black headlamp bezels, and 20-inch "
            "gloss-black aluminum wheels"
        ),
    ):
        assert is_displayable_trim_bullet(keep), keep


def test_ram_2500_laramie_has_no_derived_or_fabricated_engine_text() -> None:
    """/car/284257 repro: 2026 Ram 2500 Laramie (engine_l=6.7, cylinders=6)."""
    result = resolve_trim_ladder(make="RAM", model="2500", year=2026, trim="Laramie")
    bullets = _all_bullets(result)
    joined = " ".join(bullets).lower()
    assert "upgrades engine options" not in joined
    assert "(was " not in joined
    # engineDisplay guesses "V6" for the 6.7L Cummins; engineOptions names it correctly.
    assert "6.7l v6" not in joined


def test_4runner_limited_has_no_wikipedia_narrative(monkeypatch) -> None:
    """/car/384849 repro: 2026 Toyota 4Runner Limited.

    Uncited overlay, so nothing renders here under the gate; the narrative-leak
    check runs with the gate off, where the bullets still exist to be checked.
    """
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(make="Toyota", model="4Runner", year=2026, trim="Limited")
    bullets = _all_bullets(result)
    assert bullets
    joined = " ".join(bullets).lower()
    for bogus in ("in the united states", "carried over", "turbo tru", "upgrades engine options"):
        assert bogus not in joined, joined


def test_durango_rt_no_longer_shows_the_composed_hemi_bullet() -> None:
    """
    This test used to assert the bullet "5.7L HEMI V8 engine — added over the GT".

    That string is not printed in any brochure. The 2020 Durango trim-walk page
    prints the engine; ``_brochure_engine_bullet_by_step`` supplied the dash, the
    phrase and the baseline trim from our own ladder definition, then asked the
    citation register to bless the result. The store is revoked; assert the
    absence instead.
    """
    result = resolve_trim_ladder(make="Dodge", model="Durango", year=2020, trim="R/T")
    rt = next(s for s in result["steps"] if s["name"] == "R/T")
    # The R/T does render again now that the PDF is back on disk, but only the
    # bare engine name the page actually prints — never the comparison clause.
    assert "5.7L HEMI® V8 engine" in rt["adds"], rt["adds"]
    assert not any("added over" in b for b in rt["adds"]), rt["adds"]
    joined = " ".join(_all_bullets(result)).lower()
    assert "added over the" not in joined
    # The brochure grid marks Three-Zone ATC standard on all five trims.
    assert "three-zone" not in joined
