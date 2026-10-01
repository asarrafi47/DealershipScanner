"""The build-time verifier: does it really re-open the document?

Split out of the former backend/tests/test_trim_ladder.py. The rung-name gate
fixtures (``_legacy_rung_gate_off`` autouse, ``rung_gate_on``) live in this
package's conftest.py.
"""

from __future__ import annotations

import pathlib

import pytest


# --- the build-time verifier: does it really re-open the document? ----------
#
# The other modules in this package test what the RENDER path does with a flag. These tests are
# about where that flag comes from. The verifier is the only thing in the
# pipeline that goes back to the PDF, so if it is wrong, the gate is decorative.


def test_verifier_normalisation_forgives_glyphs_and_nothing_else() -> None:
    from backend.scripts.verify_trim_citations import normalize

    # Quote glyphs, dash flavours, non-breaking space, run of spaces, line wrap.
    assert normalize("driver’s  seat") == normalize("driver's seat")
    assert normalize("505 watt – 14 speakers") == normalize("505 watt - 14 speakers")
    assert normalize("8-way power-\nadjustable") == normalize("8 way power adjustable")
    assert normalize("• Heated front seats.") == "heated front seats"
    # A dropped character is a different claim, not a formatting difference.
    assert normalize("Front tow hooks (4x only)") != normalize("Front tow hooks (4x4 only)")
    # So is a dropped footnote marker.
    assert normalize("Remote Engine Start System with X") != normalize(
        "Remote Engine Start System45 with X"
    )


def test_verifier_requires_a_contiguous_run_not_a_bag_of_words() -> None:
    from backend.scripts.verify_trim_citations import PageReading, normalize

    # One column printing three cells in this order. ``PageReading`` no longer
    # accepts a flat per-column string (the old ``columns=`` field) — flattening
    # is what let interior fragments pass, and handing a plain string over
    # would iterate it character by character.
    page = PageReading(
        cells=(
            (
                normalize("Heated front seats"),
                normalize("Panorama sunroof"),
                normalize("Power tailgate"),
            ),
        )
    )
    assert page.prints("Panorama sunroof")
    assert page.prints("Heated front seats Panorama sunroof")
    # Words that are all present but not adjacent, and words in the wrong order.
    assert not page.prints("Heated front seats Power tailgate")
    assert not page.prints("sunroof Panorama")
    # Extra words of ours around a real quote.
    assert not page.prints("Panorama sunroof — added over the base")


def test_verifier_reads_a_wrapped_grid_row_off_the_real_pdf() -> None:
    """
    The 2026 RAV4 grid prints this label wrapped over two lines with the S/O/-
    marks landing between the halves. ``page.extract_text()`` cannot find it;
    the column rebuild can, and that is the whole reason the rebuild exists.
    """
    pdfplumber = pytest.importorskip("pdfplumber")
    from backend.scripts.verify_trim_citations import normalize, read_page

    pdf_path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "data/brochures/2026_Toyota_RAV4_Brochure.pdf"
    )
    if not pdf_path.is_file():
        pytest.skip("2026 RAV4 brochure PDF not on disk")
    bullet = "Leather-trimmed shift lever with sequential mode"
    with pdfplumber.open(str(pdf_path)) as pdf:
        raw = pdf.pages[8].extract_text() or ""
        reading = read_page(pdf.pages[8])
    assert normalize(bullet) not in normalize(raw), "flat page text should NOT contain it"
    assert reading.prints(bullet)
    # And it still refuses a sentence we made up out of the same page's words.
    assert not reading.prints("Leather-trimmed shift lever — added over the LE")


def test_verifier_fails_a_bullet_that_is_not_printed_in_the_cited_document(tmp_path) -> None:
    """End-to-end: a real citation passes, an invented one next to it fails."""
    pytest.importorskip("pdfplumber")
    import json

    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR
    from backend.scripts.verify_trim_citations import (
        FAIL_NOT_PRINTED,
        OK,
        verify_overlay,
    )

    doc = BROCHURE_TEXT_DIR / "2026__toyota__rav4.json"
    if not doc.is_file():
        pytest.skip("2026 RAV4 brochure text not on disk")
    blob = json.loads(doc.read_text(encoding="utf-8"))
    if not pathlib.Path(str(blob.get("source_pdf") or "")).is_file():
        pytest.skip("2026 RAV4 brochure PDF not on disk")

    real = "18-in multi-spoke black sport alloy wheels with black lug nuts"
    invented = "18-in multi-spoke black sport alloy wheels with a panoramic glass roof"
    overlay = {
        "catalog_key": "2026|toyota|rav4",
        "year": 2026,
        "make": "Toyota",
        "model": "RAV4",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"SE": [real, invented]},
        "adds_provenance": {
            "SE": [
                {"text": real, "source": "derived/brochure_text/2026__toyota__rav4.json",
                 "page": 7},
                {"text": invented, "source": "derived/brochure_text/2026__toyota__rav4.json",
                 "page": 7},
            ]
        },
    }
    path = tmp_path / "2026__toyota__rav4.json"
    path.write_text(json.dumps(overlay), encoding="utf-8")

    report = verify_overlay(path, apply=True)
    assert report is not None
    assert report.outcomes[OK] == 1
    assert report.outcomes[FAIL_NOT_PRINTED] == 1

    stamped = json.loads(path.read_text(encoding="utf-8"))
    flags = {e["text"]: e["verified"] for e in stamped["adds_provenance"]["SE"]}
    assert flags == {real: True, invented: False}

    from backend.enrichment.brochure_extract import admissible_overlay_adds

    assert admissible_overlay_adds(stamped, for_year=2026) == {"SE": [real]}


def test_verifier_stamps_a_synthetic_document_end_to_end(tmp_path) -> None:
    """Ungated companion to the two corpus-gated verifier tests above.

    Builds the WHOLE chain at test time — PDF, brochure_text transcript with
    the recorded sha256, overlay citing it — then runs ``verify_overlay`` with
    ``apply=True`` and checks the render path keeps only what is printed. No
    gitignored brochure or derived corpus file is touched.
    """
    fitz = pytest.importorskip("fitz", reason="pymupdf not installed")
    pytest.importorskip("pdfplumber")
    import hashlib
    import json

    from backend.enrichment.brochure_extract import admissible_overlay_adds
    from backend.scripts.verify_trim_citations import (
        FAIL_NOT_PRINTED,
        OK,
        verify_overlay,
    )

    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((60, 200), "Leather-trimmed shift lever", fontsize=9)
    for x, mark in ((300.0, "S"), (390.0, "O"), (480.0, "-")):
        page.insert_text((x, 212), mark, fontsize=9)
    page.insert_text((60, 224), "with sequential mode", fontsize=9)
    page.insert_text((60, 260), "Heated front seats", fontsize=9)
    pdf_path = tmp_path / "2026_Synthetic_Ranger_Brochure.pdf"
    doc.save(str(pdf_path))
    doc.close()

    transcript = tmp_path / "2026__synthetic__ranger.json"
    transcript.write_text(
        json.dumps(
            {
                "year": 2026,
                "make": "Synthetic",
                "model": "Ranger",
                "source_pdf": str(pdf_path),
                "source_pdf_sha256": hashlib.sha256(pdf_path.read_bytes()).hexdigest(),
            }
        ),
        encoding="utf-8",
    )

    # A wrapped grid row the flat text cannot contain, and an invented line
    # built from the same page's words.
    printed = "Leather-trimmed shift lever with sequential mode"
    invented = "Leather-trimmed shift lever with heated grips"
    cite = str(transcript)
    overlay_path = tmp_path / "2026__synthetic__ranger_overlay.json"
    overlay_path.write_text(
        json.dumps(
            {
                "catalog_key": "2026|synthetic|ranger",
                "year": 2026,
                "make": "Synthetic",
                "model": "Ranger",
                "source": "brochure_text_quoted",
                "adds_by_trim": {"Summit": [printed, invented]},
                "adds_provenance": {
                    "Summit": [
                        {"text": printed, "source": cite, "page": 1},
                        {"text": invented, "source": cite, "page": 1},
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    report = verify_overlay(overlay_path, apply=True)
    assert report is not None
    assert report.outcomes[OK] == 1
    assert report.outcomes[FAIL_NOT_PRINTED] == 1

    stamped = json.loads(overlay_path.read_text(encoding="utf-8"))
    flags = {e["text"]: e["verified"] for e in stamped["adds_provenance"]["Summit"]}
    assert flags == {printed: True, invented: False}
    assert admissible_overlay_adds(stamped, for_year=2026) == {"Summit": [printed]}


def test_verifier_rejects_a_citation_naming_another_vehicles_document(tmp_path) -> None:
    """The transcript a bullet cites has to be this year/make/model's book."""
    pytest.importorskip("pdfplumber")
    import json

    from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR
    from backend.scripts.verify_trim_citations import (
        FAIL_CITATION_OTHER_VEHICLE,
        verify_overlay,
    )

    if not (BROCHURE_TEXT_DIR / "2026__toyota__rav4.json").is_file():
        pytest.skip("2026 RAV4 brochure text not on disk")
    overlay = {
        "catalog_key": "2026|toyota|highlander",
        "year": 2026,
        "make": "Toyota",
        "model": "Highlander",
        "source": "brochure_text_quoted",
        "adds_by_trim": {"XSE": ["Body-colored grille with dark chrome accents"]},
        "adds_provenance": {
            "XSE": [
                {
                    "text": "Body-colored grille with dark chrome accents",
                    # a real, readable document — of the wrong vehicle
                    "source": "derived/brochure_text/2026__toyota__rav4.json",
                    "page": 6,
                }
            ]
        },
    }
    path = tmp_path / "2026__toyota__highlander.json"
    path.write_text(json.dumps(overlay), encoding="utf-8")
    report = verify_overlay(path, apply=False)
    assert report is not None
    assert report.outcomes[FAIL_CITATION_OTHER_VEHICLE] == 1


def test_every_overlay_bullet_on_disk_has_a_verifier_verdict() -> None:
    """
    No admissible overlay may sit in the data without having been checked.

    A file the verifier has never seen renders nothing (the flag is missing, and
    missing is not True), so this is about keeping the corpus honest rather than
    about safety: a silent file is easy to miss.
    """
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        CITATION_VERIFICATION_KEY,
    )
    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    seen = 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if str(data.get("source") or "") not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        assert CITATION_VERIFICATION_KEY in data, (
            f"{path.name}: run backend/scripts/verify_trim_citations.py --apply"
        )
        for trim, entries in (data.get("adds_provenance") or {}).items():
            for entry in entries:
                assert isinstance(entry.get("verified"), bool), (path.name, trim)
                seen += 1
    assert seen > 100, seen


def test_rendered_bullets_are_exactly_the_verified_ones() -> None:
    """
    For every admissible overlay on disk, what the ladder renders equals the set
    of bullets the verifier signed off — no more, and nothing signed off gets
    dropped for a reason other than the ladder not having that rung.
    """
    import json

    from backend.enrichment.brochure_extract import (
        ADMISSIBLE_OVERLAY_SOURCES,
        load_brochure_trim_overlay,
    )
    from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

    checked = 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if str(raw.get("source") or "") not in ADMISSIBLE_OVERLAY_SOURCES:
            continue
        overlay = load_brochure_trim_overlay(raw["year"], raw["make"], raw["model"]) or {}
        for trim, bullets in (overlay.get("adds_by_trim") or {}).items():
            signed = {
                str(e.get("text") or "")
                for e in (raw.get("adds_provenance") or {}).get(trim) or []
                if e.get("verified") is True
            }
            for bullet in bullets:
                assert bullet in signed, f"{path.name} [{trim}]: {bullet}"
                checked += 1
    assert checked > 100, checked
