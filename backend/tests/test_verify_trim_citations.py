"""The citation checker is the only thing that re-opens the document.

Everything downstream of it — ``LADDER_BULLET_STORES``, the
``TRIM_ADDS_REQUIRE_PROVENANCE`` gate, the exact trim match — trusts the
``verified`` flag this script stamps. So the checker's admission rule is the
last place a wrong quote can be stopped, and these tests are about that rule
and nothing else.

The rule under test: a stored bullet is admitted only when it EQUALS a whole
printed cell of the cited page, or a run of consecutive whole cells joined in
printing order (the PDF line-wrap accommodation). Anchored at both ends.

Regression these tests exist for
-------------------------------
Until 2026-08-02 the comparison was ``" needle " in " haystack "`` against a
flattened per-column string. That is contiguous but UNANCHORED, so any interior
fragment of a printed line was admitted, and the module docstring claimed the
opposite — that fuzzy and partial matches were refused. Two live examples off
the 2021 Volkswagen Passat brochure, both stamped ``verified: true`` on disk
and both pinned below as real-document tests:

    stored  "Tex Leatherette seating surfaces"
    printed "Perforated V-Tex Leatherette seating surfaces"

    stored  "Line® grille, front bumper, fender badges & exterior trim; …"
    printed "R-Line grille, front bumper, fender badges & exterior trim; …"

Both have had their head chopped off. Both are a different claim from what the
page says. A substring test admits both.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from backend.scripts.verify_trim_citations import (
    OK,
    PageReading,
    matches_printed_run,
    normalize,
    read_page,
)

BROCHURES = pathlib.Path(__file__).resolve().parents[1] / "data" / "brochures"


def page(*cells: str) -> PageReading:
    """A one-column page printing ``cells``, in that order."""
    return PageReading(cells=(tuple(normalize(c) for c in cells),))


# --- normalisation: glyphs, whitespace, case. Nothing else. ----------------


def test_normalisation_forgives_glyphs_whitespace_and_case_only() -> None:
    assert normalize("driver’s  seat") == normalize("driver's seat")
    assert normalize("505 watt – 14 speakers") == normalize("505 watt - 14 speakers")
    assert normalize("8-way power-\nadjustable") == normalize("8 way power adjustable")
    assert normalize("• Heated front seats.") == "heated front seats"
    assert normalize("HEATED Front Seats") == normalize("heated front seats")


# --- the five refusals, on synthetic pages ---------------------------------


def test_a_dropped_character_fails() -> None:
    """The known Titan XD case: our text says "(4x only)", the page says "(4x4 only)"."""
    titan = page("Front tow hooks (4x4 only)", "Prewiring for off-road lighting")
    assert titan.prints("Front tow hooks (4x4 only)")
    assert not titan.prints("Front tow hooks (4x only)")
    # and the footnote diagnostic must not rescue it either — a drivetrain
    # designation is not a footnote call.
    assert not titan.prints_but_for_a_footnote_call("Front tow hooks (4x only)")


def test_a_dropped_footnote_digit_fails() -> None:
    """Stripping a superscript changes the token run, so the bullet is refused."""
    p = page("Remote Engine Start System45 with X")
    assert not p.prints("Remote Engine Start System with X")
    # It is reported under its own code, but it is still a refusal.
    assert p.prints_but_for_a_footnote_call("Remote Engine Start System with X")


def test_a_reordered_token_fails() -> None:
    p = page("Panorama sunroof")
    assert p.prints("Panorama sunroof")
    assert not p.prints("sunroof Panorama")


def test_a_subset_of_a_longer_line_fails() -> None:
    """THE regression. A fragment of a printed line is not what the line says.

    Every assertion below passed the pre-2026-08-02 substring test.
    """
    row = page("Perforated V-Tex Leatherette seating surfaces")
    assert row.prints("Perforated V-Tex Leatherette seating surfaces")
    assert not row.prints("Tex Leatherette seating surfaces")  # head chopped
    assert not row.prints("Perforated V-Tex Leatherette")  # tail chopped
    assert not row.prints("V-Tex Leatherette")  # both ends chopped

    # The interior fragment that reverses the meaning of the line it sits in.
    caveat = page("Heated steering wheel not available on S")
    assert not caveat.prints("Heated steering wheel")

    # A bag of words that is present but not adjacent, and our own words added
    # around a real quote.
    grid = page("Heated front seats", "Panorama sunroof", "Power tailgate")
    assert grid.prints("Panorama sunroof")
    assert not grid.prints("Heated front seats Power tailgate")
    assert not grid.prints("Panorama sunroof — added over the base")


def test_a_needle_that_merely_contains_a_cell_fails() -> None:
    """Anchoring cuts both ways: our text may not be longer than the print either."""
    p = page("Panorama sunroof")
    assert not p.prints("Panorama sunroof with power sunshade")


# --- the one accommodation: PDF line-wrap ----------------------------------


def test_a_genuine_line_wrap_passes() -> None:
    """A label the printer wrapped, with the grid marks dropped from between.

    This is why cells are joined at all. The halves are consecutive printed
    cells, so the whole label is admitted; a fragment of it still is not.
    """
    wrapped = page("6-way manual driver’s", "seat with lumbar support")
    assert wrapped.prints("6-way manual driver's seat with lumbar support")
    assert not wrapped.prints("6-way manual driver's seat")
    assert not wrapped.prints("manual driver's seat with lumbar support")


def test_a_join_may_not_skip_a_cell() -> None:
    """Consecutive means consecutive — the run may not gap over printed text."""
    p = page("Front tow hooks", "(4x4 only)", "Prewiring for off-road lighting")
    assert p.prints("Front tow hooks (4x4 only)")
    assert p.prints("Front tow hooks (4x4 only) Prewiring for off-road lighting")
    assert not p.prints("Front tow hooks Prewiring for off-road lighting")


def test_a_run_may_not_cross_a_column() -> None:
    left_and_right = PageReading(
        cells=(
            (normalize("Heated front seats"),),
            (normalize("Panorama sunroof"),),
        )
    )
    assert left_and_right.prints("Heated front seats")
    assert left_and_right.prints("Panorama sunroof")
    assert not left_and_right.prints("Heated front seats Panorama sunroof")


# --- the matcher's own contract, without a PageReading around it -----------


def test_matches_printed_run_is_equality_over_consecutive_cells() -> None:
    cells = ("front tow hooks", "4x4 only", "prewiring for off road lighting")
    assert matches_printed_run("front tow hooks", cells)
    assert matches_printed_run("front tow hooks 4x4 only", cells)
    assert not matches_printed_run("tow hooks", cells)
    assert not matches_printed_run("front tow", cells)
    assert not matches_printed_run("4x4 only front tow hooks", cells)
    assert not matches_printed_run("", cells)
    assert not matches_printed_run("front tow hooks", ())


# --- the pdfplumber column rebuild, on a PDF built at test time ------------


def synthetic_wrapped_grid_pdf(tmp_path: pathlib.Path) -> pathlib.Path:
    """A one-page grid printing a wrapped label with S/O/- marks between halves.

    The geometry of the 2026 RAV4 regression, without the 2026 RAV4: the
    feature label wraps over two lines, and the per-trim marks land on their
    own line between the halves at the trim-column x positions. Flat
    ``page.extract_text()`` therefore interleaves the marks into the label and
    can never contain the whole sentence; only the cell rebuild can.
    """
    fitz = pytest.importorskip("fitz", reason="pymupdf not installed")

    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 80), "EQUIPMENT", fontsize=14)
    page.insert_text((60, 200), "Leather-trimmed shift lever", fontsize=9)
    for x, mark in ((300.0, "S"), (390.0, "O"), (480.0, "-")):
        page.insert_text((x, 212), mark, fontsize=9)
    page.insert_text((60, 224), "with sequential mode", fontsize=9)
    # An unwrapped row beside it, marks on the same line.
    page.insert_text((60, 260), "Heated front seats", fontsize=9)
    for x, mark in ((300.0, "S"), (390.0, "S"), (480.0, "O")):
        page.insert_text((x, 260), mark, fontsize=9)
    out = tmp_path / "wrapped_grid.pdf"
    doc.save(str(out))
    doc.close()
    return out


def test_synthetic_pdf_a_wrapped_grid_row_is_admitted_and_fragments_refused(
    tmp_path: pathlib.Path,
) -> None:
    """Ungated companion to the real-PDF tests below.

    Everything above tests ``matches_printed_run`` on hand-built cells; the
    real-PDF tests exercise ``read_page``'s column rebuild but skip wherever
    the gitignored brochures are absent. This runs the rebuild everywhere.
    """
    pdfplumber = pytest.importorskip("pdfplumber")
    pdf_path = synthetic_wrapped_grid_pdf(tmp_path)

    bullet = "Leather-trimmed shift lever with sequential mode"
    with pdfplumber.open(str(pdf_path)) as pdf:
        raw = pdf.pages[0].extract_text() or ""
        reading = read_page(pdf.pages[0])

    # The marks really did land between the halves in the flat reading.
    assert normalize(bullet) not in normalize(raw), "flat page text should NOT contain it"
    # The rebuild drops the mark cells and re-joins the wrapped halves…
    assert reading.prints(bullet)
    assert reading.prints("Heated front seats")
    # …and anchoring still refuses a run that ends mid-cell, a fragment that
    # starts mid-cell, and an invented line. (The bare first half IS a whole
    # printed cell and stays admissible — that is the rule, not a loophole.)
    assert not reading.prints("Leather-trimmed shift lever with sequential")
    assert not reading.prints("shift lever with sequential mode")
    assert not reading.prints("Leather-trimmed shift lever with heated grips")


# --- the same rule, against documents we actually hold ---------------------


def _skip_unless(pdf: pathlib.Path) -> None:
    if not pdf.is_file():
        pytest.skip(f"{pdf.name} not on disk")


def test_real_pdf_a_wrapped_grid_row_is_still_admitted() -> None:
    """The false-negative guard: anchoring must not break the wrap it exists for.

    The 2026 RAV4 grid prints this label wrapped over two lines with the S/O/-
    marks landing between the halves. ``page.extract_text()`` cannot find it at
    all; the cell rebuild can, and anchoring keeps it.
    """
    pdfplumber = pytest.importorskip("pdfplumber")
    pdf_path = BROCHURES / "2026_Toyota_RAV4_Brochure.pdf"
    _skip_unless(pdf_path)

    bullet = "Leather-trimmed shift lever with sequential mode"
    with pdfplumber.open(str(pdf_path)) as pdf:
        raw = pdf.pages[8].extract_text() or ""
        reading = read_page(pdf.pages[8])

    assert normalize(bullet) not in normalize(raw), "flat page text should NOT contain it"
    assert reading.prints(bullet)
    assert not reading.prints("Leather-trimmed shift lever — added over the LE")


@pytest.mark.parametrize(
    ("stored", "printed"),
    [
        (
            "Tex Leatherette seating surfaces",
            "Perforated V-Tex Leatherette seating surfaces",
        ),
        (
            "Line® grille, front bumper, fender badges & exterior trim; "
            "black trunk lid lip spoiler",
            "R-Line grille, front bumper, fender badges & exterior trim; "
            "black trunk lid lip spoiler",
        ),
    ],
)
def test_real_pdf_a_head_chopped_quote_is_refused(stored: str, printed: str) -> None:
    """Two bullets that were stamped ``verified: true`` on disk and are wrong.

    Both are printed on page 3 of the 2021 Passat brochure with a leading token
    our stored text lost — "Perforated V-" and "R-". The page prints the longer
    line; it does not print ours.
    """
    pdfplumber = pytest.importorskip("pdfplumber")
    pdf_path = BROCHURES / "2021_Volkswagen_Passat_Brochure.pdf"
    _skip_unless(pdf_path)

    with pdfplumber.open(str(pdf_path)) as pdf:
        reading = read_page(pdf.pages[2])

    assert reading.prints(printed), "the full printed line must still verify"
    assert not reading.prints(stored), "the head-chopped fragment must not"
    assert not reading.prints_but_for_a_footnote_call(stored)


# --- end to end: the flag the render path reads ----------------------------


def test_verify_overlay_stamps_the_fragment_false_and_the_whole_line_true(
    tmp_path: pathlib.Path,
) -> None:
    """A fragment and its full printed line, side by side in one overlay."""
    pytest.importorskip("pdfplumber")
    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR
    from backend.scripts.verify_trim_citations import FAIL_NOT_PRINTED, verify_overlay

    doc = BROCHURE_TEXT_DIR / "2021__volkswagen__passat.json"
    if not doc.is_file():
        pytest.skip("2021 Passat brochure text not on disk")
    _skip_unless(BROCHURES / "2021_Volkswagen_Passat_Brochure.pdf")

    whole = "Perforated V-Tex Leatherette seating surfaces"
    fragment = "Tex Leatherette seating surfaces"
    cite = "derived/brochure_text/2021__volkswagen__passat.json"
    overlay = {
        "catalog_key": "2021|volkswagen|passat",
        "year": 2021,
        "make": "Volkswagen",
        "model": "Passat",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"SE": [whole, fragment]},
        "adds_provenance": {
            "SE": [
                {"text": whole, "source": cite, "page": 3},
                {"text": fragment, "source": cite, "page": 3},
            ]
        },
    }
    path = tmp_path / "2021__volkswagen__passat.json"
    path.write_text(json.dumps(overlay), encoding="utf-8")

    report = verify_overlay(path, apply=True)
    assert report is not None
    assert report.outcomes[OK] == 1
    assert report.outcomes[FAIL_NOT_PRINTED] == 1

    stamped = json.loads(path.read_text(encoding="utf-8"))
    flags = {e["text"]: e["verified"] for e in stamped["adds_provenance"]["SE"]}
    assert flags == {whole: True, fragment: False}

    # And the render path drops it, which is the whole point of the flag.
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    assert admissible_overlay_adds(stamped, for_year=2021) == {"SE": [whole]}
