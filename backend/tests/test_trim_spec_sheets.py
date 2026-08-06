"""Trim spec sheet loading and extraction."""

from __future__ import annotations

import pytest

from backend.enrichment.trim_spec_sheets import lookup_trim_specs
from backend.enrichment.trim_ladder import resolve_trim_ladder


def test_jeep_grand_cherokee_uses_curated_spec_sheet() -> None:
    specs = lookup_trim_specs(
        "Limited",
        make="Jeep",
        model="Grand Cherokee",
        year=2019,
        ladder_id="jeep_grand_cherokee_wk2",
    )
    assert len(specs) >= 6
    labels = {row["label"] for row in specs}
    assert "Engine Options" in labels
    assert "Screen Size" in labels


def test_ram_1500_limited_has_structured_specs(monkeypatch, sqlite_inventory) -> None:
    # Exercises the trim-spec-sheet / Complete_Options merge. Neither store can
    # cite a document, so the provenance gate blocks both by default; the merge
    # behaviour still has to hold for anyone who turns the gate off.
    #
    # ``sqlite_inventory`` is not optional here. ``INVENTORY_DATABASE_URL`` is
    # blank under pytest, so ``_inventory_rung_evidence`` reads an empty fleet,
    # returns ``()`` and ``resolve_trim_ladder`` answers None -- a failure about
    # the test environment, not about the merge this test is for. Seeding the
    # rungs is giving the test its subject, not weakening the assertion.
    sqlite_inventory.add_cars(
        [{"year": 2025, "make": "Ram", "model": "1500", "trim": "Limited"}] * 3
        + [{"year": 2025, "make": "Ram", "model": "1500", "trim": "Laramie"}] * 2
        + [{"year": 2025, "make": "Ram", "model": "1500", "trim": "Big Horn"}] * 2
    )
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    result = resolve_trim_ladder(make="Ram", model="1500", year=2025, trim="Limited")
    assert result is not None
    limited = next(s for s in result["steps"] if s["name"] == "Limited")
    bullets = limited.get("adds") or []
    assert bullets
    joined = " ".join(bullets).lower()
    assert "screen" in joined or "uconnect" in joined or "12-inch" in joined


def test_generated_spec_sheet_file_exists_for_ford_f150() -> None:
    specs = lookup_trim_specs(
        "Lariat",
        make="Ford",
        model="F-150",
        year=2024,
        ladder_id="ford_f150",
    )
    assert specs
    assert any(s["label"] in {"Engine Options", "Screen Size", "Interior Materials"} for s in specs)


def test_display_gate_rejects_the_four_proven_leaks() -> None:
    """Bullets found live in derived/trim_adds_by_year that used to render."""
    from backend.enrichment.trim_spec_extractor import is_displayable_trim_bullet

    leaked = [
        "(cid:2) All-weather floor mats (front)",
        "is a registered trademark of Harman International Industries, Inc. Quiet Steel",
        "river and front passenger lumbar control, memory package with presets for two drivers",
        "3rd row) Vehicles shown may contain optional equipment. Features shown may be offered",
        "NavWeather™ subscription. ZDX with Technology Package includes 90 days of XM",
    ]
    for text in leaked:
        assert not is_displayable_trim_bullet(text), text


def test_display_gate_keeps_lowercase_oem_names_and_unit_abbreviations() -> None:
    from backend.enrichment.trim_spec_extractor import is_displayable_trim_bullet

    kept = [
        "iPod storage net inside center console",
        "xDrive all-wheel drive system",
        "i-Activ AWD with Off-road Traction Assist",
        "4.2-in. TFT Multi-Information Display (MID)",
        "5570 lb. Gross Vehicle Weight Rating",
        "5.7L HEMI® V8 engine",
        "8.4-inch Uconnect® touchscreen with NAV",
    ]
    for text in kept:
        assert is_displayable_trim_bullet(text), text


# --- cell-grid extraction: the hazards a previous lane shipped ------------


def test_cell_marker_hyphen_is_a_whole_cell_not_a_character_class() -> None:
    """The ASCII hyphen means "not available" only when it IS the cell.

    A flattened-text parser matched the "-" inside "Tex Leatherette"-style
    hyphenated words and cut the quote in half. Cells are tested whole, so a
    hyphenated feature name can never be read as a marker.
    """
    from backend.enrichment.brochure_trim_candidates import _cell_mark

    assert _cell_mark("-") == "neg"
    assert _cell_mark("—") == "neg"
    assert _cell_mark("●") == "std"
    assert _cell_mark("S") == "std"
    assert _cell_mark("O") == "neg"
    assert _cell_mark("") == "blank"
    assert _cell_mark("Leather-trimmed seats") is None
    assert _cell_mark("Tex Leatherette seating surfaces") is None


def test_rotated_column_label_is_read_back_exactly() -> None:
    from backend.enrichment.brochure_trim_candidates import _decode_rotated_cell

    assert _decode_rotated_cell("D\nW\nA\nd\nn\na\nld\no\no\nW") == "WoodlandAWD"
    assert _decode_rotated_cell("D\nW\nA\nd\ne\nt\nim\niL") == "LimitedAWD"
    assert _decode_rotated_cell("D\nW\nF\nE\nL") == "LEFWD"
    # Not a rotated label: ordinary wrapped cell text is left alone.
    assert _decode_rotated_cell("Heated front seats") is None


def test_header_cell_resolves_a_grade_through_its_powertrain_qualifiers() -> None:
    from backend.enrichment.brochure_trim_candidates import _cell_header_trim

    assert _cell_header_trim("GasLEFWD", "Toyota", "Grand Highlander") == "LE"
    assert _cell_header_trim("HybridMAXPlatinumAWD", "Toyota", "Grand Highlander") == "Platinum"
    assert _cell_header_trim("LE2.0LCVTFWD", "Toyota", "Corolla Cross") == "LE"
    # A grade this model is not known to offer stays unattributed.
    assert _cell_header_trim("HybridNightshadeAWD", "Toyota", "Grand Highlander") == ""
    # One column covering three grades cannot be attributed to any of them.
    assert _cell_header_trim("SPT/LTD/PLT", "Toyota", "4Runner") == ""
    # A row of markers is never a header, even when the letter is a real trim.
    assert _cell_header_trim("S", "Audi", "A4") == ""


def test_a_legend_that_reuses_one_glyph_disqualifies_the_grids() -> None:
    """GM prints "● Standard ● Available"; both discs reach us as U+25CF."""
    from backend.enrichment.brochure_trim_candidates import brochure_legend_is_ambiguous

    assert brochure_legend_is_ambiguous("● Standard ● Available — Not Available")
    assert not brochure_legend_is_ambiguous("● Standard ○ Available — Not Available")
    assert not brochure_legend_is_ambiguous("S=Standard, O=Optional, -=Not Available")
    # A hyphen inside running prose is not a legend.
    assert not brochure_legend_is_ambiguous(
        "The tow package is standard on 4x4 models - available on every other trim - "
        "and adds a seven-pin connector to the hitch that is standard on all of them."
    )


def test_a_page_break_fragment_is_not_quoted_as_a_whole_feature() -> None:
    """A table cell cut in half by a page break leaves an unclosed bracket."""
    from backend.enrichment.brochure_trim_candidates import _acceptable_equipment_line

    assert not _acceptable_equipment_line(
        "Leather-trimmed tilt/telescopic 3-spoke steering wheel with silver accent and "
        "controls for audio, Multi-Information Display (MID, Bluetooth® hands-free phone"
    )
    assert _acceptable_equipment_line(
        "Panoramic View Monitor with 3D 360-degree Overhead View, and Curb View"
    )


def test_wrapped_cell_lines_rejoin_without_inventing_a_space() -> None:
    from backend.enrichment.brochure_trim_candidates import _join_wrapped_cell

    assert (
        _join_wrapped_cell("Panoramic View Monitor with 3D 360-\ndegree Overhead View")
        == "Panoramic View Monitor with 3D 360-degree Overhead View"
    )
    assert (
        _join_wrapped_cell("Tire Pressure Monitor System (TPMS) with\ndirect pressure readout")
        == "Tire Pressure Monitor System (TPMS) with direct pressure readout"
    )


def test_comma_run_split_keeps_leading_measurements_and_drops_footnote_calls() -> None:
    """Nissan sets footnote calls tight against the comma of the PREVIOUS feature."""
    from backend.enrichment.brochure_trim_candidates import _split_comma_features

    got = _split_comma_features(
        '20" Machine-finished aluminum-alloy wheels, Motion Activated Liftgate, '
        'Traffic Sign Recognition,20 18" Machine-finished aluminum-alloy wheels, '
        "Streaming Audio via Bluetooth,®19 Nissan Intelligent Key®"
    )
    assert '20" Machine-finished aluminum-alloy wheels' in got
    assert '18" Machine-finished aluminum-alloy wheels' in got
    assert "Nissan Intelligent Key®" in got
    assert not any(g.startswith(('"', "®")) for g in got), got

    engine = _split_comma_features(
        "3.5-liter V6 engine with 295 hp and 270 lb-ft of torque, Standard Intelligent 4x4"
    )
    assert engine[0].startswith("3.5-liter V6 engine")


def test_a_sub_block_heading_never_welds_two_features_together() -> None:
    """"XD PRO-4X Adds:" starts a new block inside the same printed column."""
    from backend.enrichment.brochure_trim_candidates import _COLUMN_SUBHEADING_RE

    assert _COLUMN_SUBHEADING_RE.search("XD PRO-4X Adds: Front tow hooks")
    assert _COLUMN_SUBHEADING_RE.search("King Cab® Adds: Wide-opening rear doors")
    assert not _COLUMN_SUBHEADING_RE.search("Class IV tow hitch receiver with 7-pin harness")


def test_big_ticket_equipment_outranks_universal_equipment() -> None:
    """The user's standing requirement: engines and screens before cup holders."""
    from backend.enrichment.brochure_trim_candidates import _bullet_priority

    assert _bullet_priority("5.7L HEMI® V8 engine") > _bullet_priority(
        "Security alarm system"
    )
    assert _bullet_priority("12.3-inch touchscreen with navigation") > _bullet_priority(
        "All-weather floor mats"
    )
    assert _bullet_priority("Adaptive damping suspension") > _bullet_priority(
        "Two 12-volt power outlets"
    )
    assert _bullet_priority("Front and rear cup holders") < 0


# --- the five hazards a corpus-wide run has to survive --------------------
#
# Each of these SHIPPED at least once. They are tested at the level the damage
# happens, not at the level of the display gate, because a bullet that reaches
# the gate already carries a page citation and a shopper-facing claim.


def test_a_line_carrying_two_bullet_glyphs_disqualifies_the_page() -> None:
    """Hazard: two features welded into one claim.

    pdfplumber flattens a multi-column bullet spread row by row, so a wrapped
    bullet in the left column is emitted beside the continuation of a DIFFERENT
    bullet in the right column. The 2022 Challenger page 41 spread produced
    "Uconnect® 4C Navigation with lightweight aluminum wheels 8.4-inch
    touchscreen display" that way. One offending line is enough to refuse.
    """
    from backend.enrichment.brochure_trim_candidates import _bullet_columns_are_ambiguous

    single_column = [
        "• Leather-trimmed seats",
        "• Heated front seats",
        "• 20-inch aluminum wheels",
        "• Power liftgate",
        "• Panoramic sunroof",
        "• Remote start",
    ]
    assert not _bullet_columns_are_ambiguous(single_column)
    assert _bullet_columns_are_ambiguous(
        single_column + ["• Uconnect® 4C Navigation • 8.4-inch touchscreen display"]
    )
    # Too few single-bullet lines to tell one column from two is also a refusal.
    assert _bullet_columns_are_ambiguous(single_column[:3])


def test_equipment_standard_on_every_column_is_never_emitted_as_a_trim_add() -> None:
    """Hazard: a grid row marked standard on ALL trims sold as something a rung adds.

    The grid path subtracts the union of every lower column, so a feature the
    brochure marks standard across the whole lineup belongs to no rung's adds.
    """
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    rows = [
        "Feature | LE | XLE | Limited",
        "Security alarm system | S | S | S",
        "8-in. touchscreen display | S | S | S",
        "18-in. alloy wheels | - | S | S",
        "Power liftgate | - | S | S",
        "Heated front seats | - | - | S",
        "Panoramic glass roof | - | - | S",
    ]
    walk = extract_trim_walk(
        {"pages": [{"page": 5, "text": "", "table_lines": rows}]},
        make="Toyota",
        model="RAV4",
        year=2026,
    )
    assert walk.usable
    assert walk.trims_available == ["LE", "XLE", "Limited"]
    emitted = {b for bullets in walk.adds_by_trim.values() for b in bullets}
    assert "Security alarm system" not in emitted
    assert "8-in. touchscreen display" not in emitted
    assert walk.adds_by_trim["XLE"] == ["18-in. alloy wheels", "Power liftgate"]
    assert walk.adds_by_trim["Limited"] == ["Heated front seats", "Panoramic glass roof"]


def test_a_grid_whose_columns_cannot_be_attributed_to_trims_yields_nothing() -> None:
    """Hazard: column -> trim mis-attribution. No provable header, no bullets."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    rows = [
        "Feature | Package A | Package B | Package C",
        "Security alarm system | S | S | S",
        "8-in. touchscreen display | S | S | S",
        "18-in. alloy wheels | - | S | S",
        "Power liftgate | - | S | S",
        "Heated front seats | - | - | S",
        "Panoramic glass roof | - | - | S",
    ]
    walk = extract_trim_walk(
        {"pages": [{"page": 5, "text": "", "table_lines": rows}]},
        make="Toyota",
        model="RAV4",
        year=2026,
    )
    assert walk.adds_by_trim == {}
    assert not walk.usable
    assert "cell_grid_no_trim_header_row" in walk.reject_reasons


def test_a_column_covering_three_grades_takes_no_equipment_with_it() -> None:
    """One printed column headed "SPT/LTD/PLT" belongs to no single trim."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    rows = [
        "Feature | SPT/LTD/PLT | LE | XLE",
        "Security alarm system | S | S | S",
        "8-in. touchscreen display | S | S | S",
        "18-in. alloy wheels | S | - | S",
        "Power liftgate | S | - | S",
        "Heated front seats | S | - | S",
        "Panoramic glass roof | S | - | S",
    ]
    walk = extract_trim_walk(
        {"pages": [{"page": 5, "text": "", "table_lines": rows}]},
        make="Toyota",
        model="4Runner",
        year=2024,
    )
    assert "SPT/LTD/PLT" not in walk.trims_available
    assert not any("SPT" in t or "PLT" in t for t in walk.adds_by_trim)


def test_a_mangled_quote_can_never_pass_the_citation_check() -> None:
    """Hazard: a footnote-stripping rule eating a digit out of "4x4".

    ``normalize_quoted_line`` currently rewrites the 2022/2023 Titan XD line
    "Front tow hooks (4x4 only)" to "(4x only)" — see the xfail below. That is a
    different claim, so the guarantee that matters is that the verifier's
    comparison refuses it: the token run of the mangled text is not the token
    run the page prints. Both Titan XD overlays fail exactly this way on disk.
    """
    from backend.scripts.verify_trim_citations import matches_printed_run, normalize

    # The page prints these as two cells: a feature label and the next one.
    printed = (
        normalize("Front tow hooks (4x4 only)"),
        normalize("Prewiring for off-road lighting"),
    )
    assert matches_printed_run(normalize("Front tow hooks (4x4 only)"), printed)
    assert not matches_printed_run(normalize("Front tow hooks (4x only)"), printed)


def test_a_drivetrain_designation_is_not_read_as_a_footnote_call() -> None:
    from backend.enrichment.brochure_trim_candidates import normalize_quoted_line

    assert normalize_quoted_line("Front tow hooks (4x4 only)") == "Front tow hooks (4x4 only)"
    assert (
        normalize_quoted_line("Available 4x2 and 4x4 drivetrains")
        == "Available 4x2 and 4x4 drivetrains"
    )


def test_a_tyre_size_is_not_read_as_a_footnote_call() -> None:
    """A lowercase tyre size ends digit-letter-digit, just like "4x4"."""
    from backend.enrichment.brochure_trim_candidates import normalize_quoted_line

    assert (
        normalize_quoted_line("245/70r18 all-terrain tires")
        == "245/70r18 all-terrain tires"
    )


def test_the_verifier_separates_a_wrong_quote_from_a_glued_footnote_call() -> None:
    """Both are refusals; only one of them means our text is wrong about the car.

    pdfplumber runs a superscript into the word before it ("Rear Park Assist5"),
    which is a property of the text extractor, not of the document. That case is
    reported under its own code so it is not mistaken for a bad quote — the
    bullet is still stamped ``verified: false`` and still does not render.
    """
    from backend.scripts.verify_trim_citations import PageReading

    page = PageReading(
        cells=(("12 way power driver s seat", "parksense rear park assist5", "options packages"),)
    )
    assert not page.prints("ParkSense® Rear Park Assist")
    assert page.prints_but_for_a_footnote_call("ParkSense® Rear Park Assist")

    # The relaxation is diagnostic and it is narrow: it never turns a mangled
    # drivetrain designation into a match.
    tow = PageReading(
        cells=(("front tow hooks 4x4 only", "prewiring for off road lighting"),)
    )
    assert not tow.prints("Front tow hooks (4x only)")
    assert not tow.prints_but_for_a_footnote_call("Front tow hooks (4x only)")


def test_quoted_overlays_carry_the_orders_evidence_not_just_the_sequence() -> None:
    """Every quoted overlay on disk says WHY each rung sits where it sits."""
    import json

    from backend.enrichment.brochure_extract import overlay_rung_order
    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    checked = 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("source") != "brochure_text_quoted":
            continue
        assert isinstance(data.get("order_basis"), dict), path.name
        assert data.get("rung_order"), path.name
        order, basis = overlay_rung_order(data, for_year=data.get("year"))
        # An overlay that records an order has to record a citable basis for it.
        assert order, path.name
        for trim in order:
            assert basis[trim]["store"], (path.name, trim)
        for edge in data.get("rung_edges") or []:
            assert edge["page"] and edge["trim"] and edge["below"], path.name
            assert edge["trim_quote"] and edge["below_quote"], path.name
        checked += 1
    assert checked >= 20, checked


def test_every_overlay_bullet_we_publish_carries_a_file_and_a_page() -> None:
    """No provenance, no bullet — checked against what is actually on disk."""
    import json

    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    checked = 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("source") != "brochure_text_quoted":
            continue
        provenance = data.get("adds_provenance") or {}
        for trim, bullets in (data.get("adds_by_trim") or {}).items():
            cited = {
                str(e.get("text") or "")
                for e in (provenance.get(trim) or [])
                if e.get("source") and e.get("page") is not None
            }
            for bullet in bullets:
                assert bullet in cited, f"{path.name} [{trim}]: {bullet}"
                checked += 1
    assert checked >= 100, checked


# --- the anchored citation rule, and the two fragment classes it refuses ----


def test_an_interior_fragment_of_a_printed_line_is_refused() -> None:
    """The live defect fixed 2026-08-02: a head-chopped quote is a different claim.

    Both examples are real, off page 3 of the 2021 VW Passat brochure, and both
    were stamped ``verified: true`` on disk under the previous unanchored
    ``" needle " in " haystack "`` test. The head has been cut off each of them,
    which changes what the page says: "Tex Leatherette" is not a material, and
    "Line(R) grille" is not a part.
    """
    from backend.scripts.verify_trim_citations import PageReading, normalize

    page = PageReading(
        cells=(
            (
                normalize("Perforated V-Tex Leatherette seating surfaces"),
                normalize("R-Line grille, front bumper, side skirts and rear valance"),
            ),
        )
    )
    # what the page actually prints, quoted whole, is admitted
    assert page.prints("Perforated V-Tex Leatherette seating surfaces")
    assert page.prints("R-Line grille, front bumper, side skirts and rear valance")
    # the beheaded versions are not
    assert not page.prints("Tex Leatherette seating surfaces")
    assert not page.prints("Line grille, front bumper, side skirts and rear valance")
    # nor is a tail-chopped one, nor a bag of adjacent-looking words
    assert not page.prints("Perforated V-Tex Leatherette seating")
    assert not page.prints("Leatherette seating surfaces R-Line grille")


def test_a_comma_list_item_is_refused_because_it_is_not_a_printed_unit() -> None:
    """Nissan sets a whole trim's equipment as one comma-run inside a column.

    Splitting that run at its commas produces claims that are TRUE but are not
    printed units, so the anchored rule refuses them and they never render.
    This is the largest single class of refusal in the corpus: 122 of the 620
    citations checked on 2026-08-02, all four of them Nissan books
    (Pathfinder 2026, Titan XD 2019/2022/2023).

    The test pins the refusal, not the split. Silence is the correct outcome —
    an unquotable true feature costs a rung a bullet; a fragment that reads as a
    printed claim when it is not costs the shopper a fact.
    """
    from backend.scripts.verify_trim_citations import PageReading, normalize

    printed = "20\" Aluminum-alloy wheels with all-season tires, LED headlights, fog lights"
    page = PageReading(cells=((normalize(printed),),))
    assert page.prints(printed)
    for item in ('20" Aluminum-alloy wheels with all-season tires', "LED headlights", "fog lights"):
        assert not page.prints(item), item


def test_ranking_puts_big_ticket_first_and_never_drops_a_bullet() -> None:
    """Requirement E, and the ordering bug that cost a rung a bullet.

    Ranking must not truncate before the display gate runs: a gate rejection
    inside the top N used to leave the rung short while a good candidate sat
    unused just below the cut.
    """
    from backend.enrichment.brochure_trim_candidates import QuotedTrimBullet, _rank_bullets

    def b(text: str) -> QuotedTrimBullet:
        return QuotedTrimBullet(text=text, page=1, layout="cell_grid", source="x.json")

    bullets = [
        b("Front and rear cup holders"),
        b("Security alarm"),
        b("5.7L HEMI® V8 engine"),
        b("Panoramic sunroof"),
        b("12-speaker Harman Kardon audio"),
    ]
    ranked = [x.text for x in _rank_bullets(bullets, limit=None)]
    assert ranked[0] == "5.7L HEMI® V8 engine"
    assert ranked.index("12-speaker Harman Kardon audio") < ranked.index("Panoramic sunroof")
    # universal equipment sinks, but is never deleted
    assert set(ranked) == {x.text for x in bullets}
    assert ranked[-2:] == ["Front and rear cup holders", "Security alarm"]
    # limit=None must rank ALL of them, so the gate can pick further down
    assert len(_rank_bullets(bullets, limit=None)) == len(bullets)
    assert len(_rank_bullets(bullets, limit=2)) == 2


# --- fail closed: no document in our hands, no citation out of it -----------


def test_a_transcript_whose_pdf_we_no_longer_hold_is_refused_before_extraction(tmp_path) -> None:
    """2,633 of the 2,815 transcripts on disk name a PDF that is gone.

    A citation exists so a reader can re-open the page. If the document is not
    in our hands the citation cannot be checked by the verifier or by a person,
    so the transcript is refused up front rather than quoted out of and then
    failed one bullet at a time.
    """
    from backend.scripts.build_trim_spec_sheets import (
        SKIP_NO_SHA,
        SKIP_PDF_CHANGED,
        SKIP_PDF_NOT_HELD,
        document_is_held,
    )

    assert document_is_held({"source_pdf": "/nowhere/2021_Nothing.pdf"}) == SKIP_PDF_NOT_HELD
    assert document_is_held({}) == SKIP_PDF_NOT_HELD

    pdf = tmp_path / "held.pdf"
    pdf.write_bytes(b"%PDF-1.4 not really a pdf, but it is a file we hold")
    import hashlib

    real = hashlib.sha256(pdf.read_bytes()).hexdigest()
    assert document_is_held({"source_pdf": str(pdf)}) == SKIP_NO_SHA
    assert document_is_held({"source_pdf": str(pdf), "source_pdf_sha256": "0" * 64}) == SKIP_PDF_CHANGED
    assert document_is_held({"source_pdf": str(pdf), "source_pdf_sha256": real}) is None


# --- requirement A: the run is checkpointed and resumable -------------------


def test_the_checkpoint_survives_a_kill_and_the_rerun_skips_finished_work(tmp_path, monkeypatch) -> None:
    """A run that dies mid-flight must not lose the transcripts it finished.

    Two runs died in this project on the same day; the one that checkpointed
    kept all its results and the one that did not lost everything. So the
    checkpoint is flushed in a ``finally``, not only on the interval, and a
    re-run skips anything already recorded.
    """
    import backend.scripts.build_trim_spec_sheets as B

    progress = tmp_path / "extraction_progress.json"

    calls = {"n": 0}
    real = B.document_is_held

    def die_after_137(data):
        calls["n"] += 1
        if calls["n"] == 137:
            raise RuntimeError("simulated mid-flight death")
        return real(data)

    monkeypatch.setattr(B, "document_is_held", die_after_137)
    with pytest.raises(RuntimeError):
        B._write_quoted_overlays(restart=True, checkpoint_every=50, progress_path=progress)

    saved = B.load_progress(progress)
    # NOT truncated back to the last 50-file boundary: the finally-flush keeps
    # every transcript that was actually processed.
    assert len(saved["done"]) == 136, len(saved["done"])

    monkeypatch.setattr(B, "document_is_held", real)
    B._write_quoted_overlays(restart=False, checkpoint_every=50, limit=5, progress_path=progress)
    after = B.load_progress(progress)
    assert len(after["done"]) == 141, len(after["done"])
    # everything recorded by the first run is still recorded
    assert set(saved["done"]) <= set(after["done"])


def test_a_retranscribed_brochure_is_reprocessed_not_skipped(tmp_path) -> None:
    """Resume keys on the transcript's sha256, not just its name.

    Skipping by filename alone would mean a re-captured brochure — the whole
    point of re-capturing being that the old transcript was lossy — is never
    re-read.
    """
    import json

    import backend.scripts.build_trim_spec_sheets as B

    progress_file = tmp_path / "p.json"
    src = tmp_path / "2021__volkswagen__passat.json"
    src.write_text(json.dumps({"year": 2021}), encoding="utf-8")
    digest_v1 = B._digest(src)

    progress = {"schema_version": B.PROGRESS_SCHEMA, "done": {src.name: {"digest": digest_v1}}}
    assert B._already_done(progress, src, digest_v1) is True

    src.write_text(json.dumps({"year": 2021, "recaptured": True}), encoding="utf-8")
    assert B._already_done(progress, src, B._digest(src)) is False

    # A corrupt checkpoint restarts the run rather than being trusted.
    progress_file.write_text('{"schema_version": 1, "done": {"a.json"', encoding="utf-8")
    assert B.load_progress(progress_file) == {"schema_version": 1, "done": {}, "runs": []}


def test_the_checkpoint_write_is_atomic(tmp_path) -> None:
    """A half-written checkpoint is the thing the checkpoint exists to prevent."""
    import json

    import backend.scripts.build_trim_spec_sheets as B

    path = tmp_path / "p.json"
    B.save_progress({"schema_version": B.PROGRESS_SCHEMA, "done": {"x.json": {"digest": "d"}}}, path)
    assert json.loads(path.read_text(encoding="utf-8"))["done"]["x.json"]["digest"] == "d"
    # no temp files left behind
    assert [p.name for p in tmp_path.iterdir()] == ["p.json"]
