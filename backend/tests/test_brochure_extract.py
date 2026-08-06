"""Tests for brochure PDF trim extraction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.enrichment.brochure_extract import (
    BROCHURE_TEXT_SCHEMA_VERSION,
    archive_source_pdf,
    brochure_archive_dir,
    brochure_text_payload,
    dumps_brochure_text,
    extract_brochure_pdf,
    extract_brochure_text_pdf,
    load_brochure_trim_overlay,
    parse_brochure_filename,
)
from backend.enrichment.trim_ladder import resolve_trim_ladder

_REPO = Path(__file__).resolve().parents[2]
_GC_2016 = _REPO / "backend" / "data" / "brochures" / "2016_Jeep_Grand_Cherokee_Brochure.pdf"
_OVERLAY = (
    _REPO
    / "backend"
    / "dictionary"
    / "derived"
    / "trim_adds_by_year"
    / "2016__jeep__grandcherokee.json"
)


def test_parse_brochure_filename():
    ymm = parse_brochure_filename(Path("2016_Jeep_Grand_Cherokee_Brochure.pdf"))
    assert ymm is not None
    assert ymm.year == 2016
    assert ymm.make == "Jeep"
    assert "Grand" in ymm.model


@pytest.mark.skipif(not _GC_2016.is_file(), reason="brochure PDF not present")
def test_extract_brochure_text_2016_grand_cherokee():
    result = extract_brochure_text_pdf(_GC_2016)
    assert result is not None
    assert result.pages
    assert result.combined_trim_pages_text
    assert result.trim_hint_pages or "no_trim_hint" in str(result.warnings)


@pytest.mark.skipif(not _GC_2016.is_file(), reason="brochure PDF not present")
def test_extract_2016_grand_cherokee_trims():
    result = extract_brochure_pdf(_GC_2016)
    assert result is not None
    assert result.ladder_id == "jeep_grand_cherokee_wk2"
    assert "Limited" in result.trims_available or "Limited" in result.adds_by_trim
    assert "Laredo" in result.trims_available or "Laredo" in result.standard_by_trim
    limited = result.adds_by_trim.get("Limited") or []
    assert len(limited) >= 3
    assert any("Uconnect" in b or "8.4" in b for b in limited)


def _synthetic_buyers_guide_pdf(tmp_path: Path) -> Path:
    """A two-page brochure whose page 2 is a FCA-style "TRIM STANDARD -" guide.

    The padding line between the two trim blocks is load-bearing:
    ``_find_trim_sections`` refuses a header whose 220-character lookahead
    window holds a second ``STANDARD -``, so without it the two single-line
    blocks read as one interleaved two-column page and Sport is dropped.
    It starts with ``- Available`` so the feature splitter cuts it off as its
    own part and ``_normalize_feature`` then rejects it — it must never become
    a bullet.
    """
    fitz = pytest.importorskip("fitz", reason="pymupdf not installed")

    doc = fitz.open()
    cover = doc.new_page(width=612, height=792)
    cover.insert_text((72, 200), "2026 SYNTHETIC MOTORS RANGER", fontsize=24)
    guide = doc.new_page(width=612, height=792)
    guide.insert_text(
        (72, 120),
        "SPORT STANDARD - 2.0L I4 engine - Cloth bucket seats - 17-inch steel"
        " wheels - 7-inch touchscreen display - Manual air conditioning",
        fontsize=8,
    )
    guide.insert_text(
        (72, 140),
        "- Available at participating dealers with a full range of accessories,"
        " delivery timing varies by region and configuration, ask your retailer"
        " about current production schedules, colors and interior combinations"
        " for every model in the lineup this year and next year too",
        fontsize=8,
    )
    guide.insert_text(
        (72, 180),
        "SUMMIT STANDARD - 2.0L I4 engine - Leather-trimmed bucket seats -"
        " 19-inch alloy wheels - 10.1-inch touchscreen display - Dual-zone"
        " automatic climate control",
        fontsize=8,
    )
    out = tmp_path / "2026_Synthetic_Ranger_Brochure.pdf"
    doc.save(str(out))
    doc.close()
    return out


def test_extract_brochure_pdf_full_pipeline_on_a_synthetic_buyers_guide(tmp_path):
    """The full ``extract_brochure_pdf`` trim pipeline, with no corpus asset.

    The real-PDF test above stays as the golden pin for documents we hold; this
    is the companion that runs everywhere, so the section finder, the feature
    splitter and the adds-over-lower delta cannot regress silently on CI where
    the gitignored PDFs do not exist.
    """
    result = extract_brochure_pdf(_synthetic_buyers_guide_pdf(tmp_path))
    assert result is not None
    assert result.ymm.catalog_key == "2026|synthetic|ranger"
    assert result.trims_available == ["Sport", "Summit"]
    assert result.standard_by_trim["Sport"] == [
        "2.0L I4 engine",
        "Cloth bucket seats",
        "17-inch steel wheels",
        "7-inch touchscreen display",
        "Manual air conditioning",
    ]
    # Summit's adds are its features minus Sport's; the shared engine drops out.
    summit = result.adds_by_trim["Summit"]
    assert "Leather-trimmed bucket seats" in summit
    assert "Dual-zone automatic climate control" in summit
    assert "2.0L I4 engine" not in summit
    # The padding sentence never becomes equipment.
    joined = " ".join(b for feats in result.standard_by_trim.values() for b in feats)
    assert "participating dealers" not in joined
    assert "no_trim_sections_found" not in result.warnings


@pytest.mark.skipif(not _OVERLAY.is_file(), reason="run process_brochure_queue first")
def test_resolve_trim_ladder_uses_brochure_overlay(monkeypatch):
    # The real 2016 GC overlay is ``manual_brochure_review`` — no per-bullet
    # citations — and under pytest the inventory is empty, so with the
    # provenance gates at their defaults every rung is (correctly) dropped and
    # the resolver returns None for a reason unrelated to what this test pins:
    # that the overlay's trim list and adds are consumed at all. Run it under
    # the documented kill switches (same pattern as test_trim_ladder.py's
    # ``_legacy_rung_gate_off``); the gates themselves are tested with
    # verified synthetic citations in
    # ``test_resolve_trim_ladder_uses_a_synthetic_verified_overlay`` below.
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    ladder = resolve_trim_ladder(
        make="Jeep",
        model="Grand Cherokee",
        year=2016,
        trim="Limited",
    )
    assert ladder is not None
    limited_step = next(s for s in ladder["steps"] if s["name"] == "Limited")
    assert limited_step.get("adds")
    assert "Trackhawk" not in {s["name"] for s in ladder["steps"]}


@pytest.mark.skipif(not _OVERLAY.is_file(), reason="overlay not built")
def test_load_brochure_overlay():
    data = load_brochure_trim_overlay(2016, "Jeep", "Grand Cherokee")
    assert data is not None
    assert data.get("catalog_key") == "2016|jeep|grandcherokee"


def test_load_brochure_overlay_year_fallback(tmp_path, monkeypatch):
    from backend.enrichment import brochure_extract as be

    prior = be.TRIM_ADDS_BY_YEAR_DIR
    be.TRIM_ADDS_BY_YEAR_DIR = tmp_path
    try:
        payload = {
            "catalog_key": "2025|hyundai|palisade",
            "year": 2025,
            "make": "Hyundai",
            "model": "Palisade",
            "source": "manual",
            "trims_available": ["Calligraphy", "SEL"],
            "adds_by_trim": {
                "Calligraphy": ["Nappa leather seating"],
                "SEL": ["Cloth seating"],
            },
        }
        (tmp_path / "2025__hyundai__palisade.json").write_text(
            json.dumps(payload),
            encoding="utf-8",
        )
        data = be.load_brochure_trim_overlay(2026, "Hyundai", "Palisade")
        assert data is not None
        assert data["year"] == 2025
    finally:
        be.TRIM_ADDS_BY_YEAR_DIR = prior


# --- synthetic companions for the overlay-gated tests above -------------------
#
# ``test_resolve_trim_ladder_uses_brochure_overlay`` / ``test_load_brochure_overlay``
# skip wherever ``process_brochure_queue`` has not been run (every CI box), so
# the overlay consumption path had no test that runs everywhere. These build the
# same-shaped overlay at test time instead of reading the derived corpus.

_SYNTH_OVERLAY_CITE = "derived/brochure_text/2016__jeep__grandcherokee.json"


def _synthetic_gc_overlay() -> dict:
    def prov(text: str) -> dict:
        return {"text": text, "source": _SYNTH_OVERLAY_CITE, "page": 4, "verified": True}

    return {
        "catalog_key": "2016|jeep|grandcherokee",
        "year": 2016,
        "make": "Jeep",
        "model": "Grand Cherokee",
        "source": "brochure_text_quoted",
        "trims_available": ["Laredo", "Limited", "Overland"],
        "adds_by_trim": {
            "Limited": [
                "Uconnect 8.4-inch touchscreen",
                "Heated second-row seats",
                "Power liftgate",
            ],
            "Overland": ["Air suspension"],
        },
        "adds_provenance": {
            "Limited": [
                prov("Uconnect 8.4-inch touchscreen"),
                prov("Heated second-row seats"),
                prov("Power liftgate"),
            ],
            "Overland": [prov("Air suspension")],
        },
    }


def test_load_brochure_overlay_synthetic_exact_year_hit(tmp_path, monkeypatch):
    from backend.enrichment import brochure_extract as be

    monkeypatch.setattr(be, "TRIM_ADDS_BY_YEAR_DIR", tmp_path)
    (tmp_path / "2016__jeep__grandcherokee.json").write_text(
        json.dumps(_synthetic_gc_overlay()), encoding="utf-8"
    )
    data = be.load_brochure_trim_overlay(2016, "Jeep", "Grand Cherokee")
    assert data is not None
    assert data.get("catalog_key") == "2016|jeep|grandcherokee"
    assert data["year"] == 2016
    # Verified per-bullet citations survive the provenance gate on load.
    assert data["adds_by_trim"]["Limited"]


def test_resolve_trim_ladder_uses_a_synthetic_verified_overlay(scratch_dictionary_root):
    """``resolve_trim_ladder`` consumes overlay adds — no derived corpus needed.

    ``scratch_dictionary_root`` rather than a bare monkeypatch: the resolver
    reaches the overlay through several modules' imported path constants, and
    the fixture redirects every copy, so the test cannot fall through to the
    real ``derived/trim_adds_by_year`` where a same-named overlay exists.
    """
    overlay_dir = scratch_dictionary_root / "derived" / "trim_adds_by_year"
    overlay_dir.mkdir(parents=True, exist_ok=True)
    (overlay_dir / "2016__jeep__grandcherokee.json").write_text(
        json.dumps(_synthetic_gc_overlay()), encoding="utf-8"
    )

    ladder = resolve_trim_ladder(
        make="Jeep", model="Grand Cherokee", year=2016, trim="Limited"
    )
    assert ladder is not None
    limited_step = next(s for s in ladder["steps"] if s["name"] == "Limited")
    assert limited_step.get("adds")
    assert "Uconnect 8.4-inch touchscreen" in limited_step["adds"]
    assert "Trackhawk" not in {s["name"] for s in ladder["steps"]}


# --- quoted trim-walk extraction from derived/brochure_text -------------------

_BROCHURE_TEXT = _REPO / "backend" / "dictionary" / "derived" / "brochure_text"
_DURANGO_2020 = _BROCHURE_TEXT / "2020__dodge__durango.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.skipif(not _DURANGO_2020.is_file(), reason="brochure_text corpus not present")
def test_durango_2020_adds_to_block_is_quoted_with_pages():
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    walk = extract_trim_walk(_load(_DURANGO_2020), make="Dodge", model="Durango", year=2020)
    assert walk.usable
    assert walk.layouts == ["adds_to_block"]
    assert {"GT", "R/T", "Citadel", "SRT"} <= set(walk.adds_by_trim)

    rt = walk.adds_by_trim["R/T"]
    assert "5.7L HEMI® V8 engine" in rt
    # Big-ticket first: the engine leads the rung, not the park assist.
    assert rt[0] == "5.7L HEMI® V8 engine"

    # Every bullet carries a page, and that page really contains the text.
    pages = {p["page"] for rows in walk.provenance.values() for p in rows}
    assert pages <= set(walk.pages_used)
    page_text = {
        p["page"]: p["text"] for p in _load(_DURANGO_2020)["pages"] if isinstance(p, dict)
    }
    for trim, rows in walk.provenance.items():
        for row in rows:
            assert row["source"].endswith("2020__dodge__durango.json")
            head = row["text"].split("®")[0].split("™")[0][:18]
            assert head in page_text[row["page"]], (trim, row)


@pytest.mark.skipif(not _DURANGO_2020.is_file(), reason="brochure_text corpus not present")
def test_options_packages_are_not_reported_as_trim_adds():
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    walk = extract_trim_walk(_load(_DURANGO_2020), make="Dodge", model="Durango", year=2020)
    # "Power sunroof" is under OPTIONS/PACKAGES on the GT page and standard on
    # Citadel; it must not be attributed to the GT.
    assert not any("sunroof" in b.lower() for b in walk.adds_by_trim["GT"])
    assert any("sunroof" in b.lower() for b in walk.adds_by_trim["Citadel"])


def test_multi_column_bullet_page_yields_nothing():
    """Toyota-style comparison spreads flatten into unattributable text."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    payload = {
        "pages": [
            {
                "page": 9,
                "text": (
                    "RAV4 MODELS\n"
                    "LE XLE XLE Premium\n"
                    "Includes these key features Adds to or replaces features offered on LE\n"
                    "• 2.5L 4-Cylinder engine • Available All-Wheel Drive\n"
                    "• Front-Wheel Drive • Multi-Terrain Select\n"
                    "• LED taillights • 17-in. alloy wheels\n"
                    "• Fabric seats • Power liftgate\n"
                    "• Cloth trim • Moonroof\n"
                    "• Steel wheels • Roof rails\n"
                ),
            }
        ]
    }
    walk = extract_trim_walk(payload, make="Toyota", model="RAV4", year=2023)
    assert walk.adds_by_trim == {}
    assert not walk.usable


def test_every_quoted_bullet_passes_the_display_gate():
    """If the prose gate has to catch our own output, the extraction is wrong."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk
    from backend.enrichment.trim_spec_extractor import is_displayable_trim_bullet

    if not _DURANGO_2020.is_file():
        pytest.skip("brochure_text corpus not present")
    walk = extract_trim_walk(_load(_DURANGO_2020), make="Dodge", model="Durango", year=2020)
    assert walk.gate_rejected == 0
    for bullets in walk.adds_by_trim.values():
        for b in bullets:
            assert is_displayable_trim_bullet(b), b


# --- lossless capture --------------------------------------------------------
#
# The corpus these tests defend was captured by an extractor that persisted only
# the pages whose text matched a trim-hint regex. For 1,368 of 3,204 brochures
# that discarded the equipment grid, and the PDFs were deleted afterwards. The
# fixture below is a four-page brochure where exactly ONE page trips that regex:
# under the old behaviour three of its four pages were unrecoverable.

_SYNTH_TRIMS = ("Base", "Sport", "Summit")
_SYNTH_COLUMN_X = (300.0, 390.0, 480.0)


def _synthetic_brochure(tmp_path: Path) -> Path:
    """A 4-page brochure whose page 3 is a 3-column equipment grid."""
    fitz = pytest.importorskip("fitz", reason="pymupdf not installed")

    doc = fitz.open()
    label_x = 60.0

    cover = doc.new_page(width=612, height=792)
    cover.insert_text((72, 200), "2026 SYNTHETIC MOTORS RANGER", fontsize=24)
    cover.insert_text((72, 240), "A brochure fixture with four pages.", fontsize=12)

    prose = doc.new_page(width=612, height=792)
    for i, line in enumerate(
        [
            "Designed around the people who use it.",
            "Every journey begins somewhere.",
            "Photography shown throughout.",
        ]
    ):
        prose.insert_text((72, 120 + i * 24), line, fontsize=12)

    grid = doc.new_page(width=612, height=792)
    grid.insert_text((72, 80), "SPECIFICATIONS", fontsize=16)
    for name, x in zip(_SYNTH_TRIMS, _SYNTH_COLUMN_X):
        grid.insert_text((x, 120), name, fontsize=10)
    rows = [
        ("Engine", ["2.0L I4", "2.0L I4", "3.6L V6"]),
        ("Horsepower", ["180 hp", "180 hp", "290 hp"]),
        ("Touchscreen display", ["7-inch", "8.4-inch", "10.1-inch"]),
        ("Adaptive suspension", ["-", "-", "S"]),
    ]
    for r, (label, values) in enumerate(rows):
        y = 150 + r * 24
        grid.insert_text((label_x, y), label, fontsize=10)
        for value, x in zip(values, _SYNTH_COLUMN_X):
            grid.insert_text((x, y), value, fontsize=10)

    legal = doc.new_page(width=612, height=792)
    legal.insert_text((72, 120), "Always drive attentively and obey all laws.", fontsize=9)
    legal.insert_text((72, 140), "Printed in the U.S.A.", fontsize=9)

    out = tmp_path / "2026_Synthetic_Ranger_Brochure.pdf"
    doc.save(str(out))
    doc.close()
    return out


def test_synthetic_capture_keeps_every_page_not_just_the_hinted_one(tmp_path):
    pdf = _synthetic_brochure(tmp_path)
    result = extract_brochure_text_pdf(pdf)
    assert result is not None

    # Exactly one page trips the trim-hint regex, so the page filter that used to
    # decide what got persisted would have kept 1 of 4 pages.
    assert result.trim_hint_pages == [3]
    assert result.page_count == 4
    assert len(result.pages) == 4
    assert [p.page for p in result.pages] == [1, 2, 3, 4]
    assert all(p.has_text_layer for p in result.pages)
    assert "Every journey begins somewhere." in result.pages[1].text
    assert "Printed in the U.S.A." in result.pages[3].text


def test_synthetic_capture_keeps_column_x_positions(tmp_path):
    """The grid columns stay separable: header and cells share an x span."""
    pdf = _synthetic_brochure(tmp_path)
    result = extract_brochure_text_pdf(pdf)
    assert result is not None
    grid_page = result.pages[2]
    assert grid_page.width == 612.0 and grid_page.height == 792.0
    assert grid_page.lines

    def row_with(label: str) -> dict:
        for line in grid_page.lines:
            if line["cells"] and line["cells"][0][2].startswith(label):
                return line
        raise AssertionError(f"no row starting {label!r} in {grid_page.lines}")

    header = row_with("Base")
    assert [c[2] for c in header["cells"]] == list(_SYNTH_TRIMS)
    # The header cells sit where they were drawn, in ascending x.
    xs = [c[0] for c in header["cells"]]
    assert xs == sorted(xs)
    for cell, drawn_x in zip(header["cells"], _SYNTH_COLUMN_X):
        assert abs(cell[0] - drawn_x) < 2.0

    # Flattened text says "7-inch 8.4-inch 10.1-inch" with no way to tell which
    # trim owns which. The x-extents do: each value overlaps exactly one header.
    screens = row_with("Touchscreen")
    values = [c for c in screens["cells"] if not c[2].startswith("Touchscreen")]
    assert [c[2] for c in values] == ["7-inch", "8.4-inch", "10.1-inch"]

    def owning_trim(cell: list) -> str:
        hits = [
            name
            for name, head in zip(_SYNTH_TRIMS, header["cells"])
            if cell[0] < head[1] + 1 and cell[1] > head[0] - 1
        ]
        assert len(hits) == 1, (cell, hits)
        return hits[0]

    assert [owning_trim(c) for c in values] == list(_SYNTH_TRIMS)
    # The top trim is the only one with the adaptive suspension marker.
    marks = [c for c in row_with("Adaptive")["cells"] if c[2] in {"-", "S"}]
    assert [(owning_trim(c), c[2]) for c in marks] == [
        ("Base", "-"),
        ("Sport", "-"),
        ("Summit", "S"),
    ]


def test_persisted_payload_is_backward_compatible_and_deterministic(tmp_path):
    pdf = _synthetic_brochure(tmp_path)
    result = extract_brochure_text_pdf(pdf)
    payload = brochure_text_payload(result)

    # v1 keys every existing reader depends on, unchanged in name and meaning.
    for key in (
        "catalog_key",
        "year",
        "make",
        "model",
        "source_pdf",
        "page_count",
        "all_pages",
        "trim_hint_pages",
        "warnings",
        "pages",
        "combined_trim_pages_text",
    ):
        assert key in payload, key
    for page in payload["pages"]:
        assert set(page) >= {"page", "text", "trim_hint", "table_lines"}

    assert payload["schema_version"] == BROCHURE_TEXT_SCHEMA_VERSION
    assert payload["capture"] == "lossless"
    assert payload["pages_captured"] == payload["page_count"] == 4
    assert payload["layout_captured"] is True
    assert len(payload["source_pdf_sha256"]) == 64

    # combined_trim_pages_text still holds only the trim-hint pages: the LLM and
    # promote lanes that read it see exactly what they saw before.
    combined = payload["combined_trim_pages_text"]
    assert "SPECIFICATIONS" in combined
    assert "Every journey begins somewhere." not in combined

    serialized = dumps_brochure_text(payload)
    assert json.loads(serialized) == payload
    # Each layout row is one physical line, so the file stays greppable.
    assert '"lines": [' in serialized
    assert dumps_brochure_text(brochure_text_payload(extract_brochure_text_pdf(pdf))) == serialized


def test_capture_layout_false_still_captures_every_page(tmp_path):
    pdf = _synthetic_brochure(tmp_path)
    result = extract_brochure_text_pdf(pdf, capture_layout=False)
    assert len(result.pages) == 4
    assert all(p.lines == [] for p in result.pages)
    assert result.layout_captured is False


def test_all_pages_false_no_longer_drops_pages(tmp_path):
    """The old kwarg is inert: callers that pass all_pages=False are lossless now."""
    pdf = _synthetic_brochure(tmp_path)
    result = extract_brochure_text_pdf(pdf, all_pages=False)
    assert len(result.pages) == 4
    assert result.all_pages is True


# --- durable source-PDF archive ---------------------------------------------


def test_archive_stores_pdf_with_checksum_and_never_deletes_it(tmp_path, monkeypatch):
    monkeypatch.setenv("BROCHURE_ARCHIVE_DIR", str(tmp_path / "archive"))
    pdf = _synthetic_brochure(tmp_path)

    archived = archive_source_pdf(pdf, source_url="https://example.invalid/b.pdf")
    assert archived is not None
    assert archived.is_file()
    assert pdf.is_file(), "archiving must never remove the source"
    assert archived.parent == brochure_archive_dir() / "synthetic"
    assert archived.read_bytes() == pdf.read_bytes()

    meta = json.loads(archived.with_name(archived.name + ".meta.json").read_text())
    assert len(meta["sha256"]) == 64
    assert meta["bytes"] == pdf.stat().st_size
    assert meta["catalog_key"] == "2026|synthetic|ranger"
    assert meta["source_url"] == "https://example.invalid/b.pdf"

    # The archive tree ignores itself so the binaries cannot be committed.
    assert (brochure_archive_dir() / ".gitignore").read_text().strip().endswith("*")

    # Idempotent: same bytes, same destination, no duplicate.
    again = archive_source_pdf(pdf)
    assert again == archived
    assert len(list(archived.parent.glob("*.pdf"))) == 1


def test_archive_never_overwrites_a_different_document(tmp_path, monkeypatch):
    monkeypatch.setenv("BROCHURE_ARCHIVE_DIR", str(tmp_path / "archive"))
    pdf = _synthetic_brochure(tmp_path)
    first = archive_source_pdf(pdf)

    other_dir = tmp_path / "second"
    other_dir.mkdir()
    other = other_dir / pdf.name
    other.write_bytes(pdf.read_bytes() + b"\n% revised printing\n")

    second = archive_source_pdf(other)
    assert second is not None and second != first
    assert first.read_bytes() != second.read_bytes()
    assert first.is_file() and second.is_file()


def test_archive_dry_run_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("BROCHURE_ARCHIVE_DIR", str(tmp_path / "archive"))
    pdf = _synthetic_brochure(tmp_path)
    planned = archive_source_pdf(pdf, dry_run=True)
    assert planned is not None
    assert not planned.exists()
    assert not (tmp_path / "archive").exists()


def test_process_brochure_file_keeps_the_pdf_when_archiving_fails(tmp_path, monkeypatch):
    """delete_after must never outrun the archive: an unarchived PDF is kept."""
    from backend.enrichment import brochure_extract as be

    pdf = _synthetic_brochure(tmp_path)
    ymm = parse_brochure_filename(pdf)
    monkeypatch.setattr(be, "extract_brochure_pdf", lambda p: be.BrochureExtractResult(ymm=ymm))
    monkeypatch.setattr(be, "persist_brochure_extract", lambda r: tmp_path / "overlay.json")
    monkeypatch.setattr(be, "archive_source_pdf", lambda p, **kw: None)

    result = be.process_brochure_file(pdf, delete_after=True)
    assert pdf.is_file()
    assert "delete_skipped_not_archived" in result.warnings

    monkeypatch.setattr(be, "archive_source_pdf", lambda p, **kw: tmp_path / "archived.pdf")
    be.process_brochure_file(pdf, delete_after=True)
    assert not pdf.exists()


# --------------------------------------------------------------------------
# Private Use Area residue: the bullet, not the document
# --------------------------------------------------------------------------
#
# ``brochure_sources.MAX_PRIVATE_USE_CODEPOINTS`` refuses a DOCUMENT when a font
# subset remapped a whole character class. It deliberately keeps the 110 live
# documents whose only PUA character is a dingbat or a punctuation glyph, so the
# residue has to be stopped one bullet at a time -- a bullet is the unit that
# reaches a shopper.
#
# Codepoints are built with chr() and never written as literals: a literal PUA
# character in source is invisible in a diff, and the first draft of
# ``_BULLET_PRIVATE_USE_AREA`` lost its BMP block that way.


def test_bullet_pua_pattern_covers_bmp_block():
    """
    The block every real contamination in this corpus lives in.

    A pattern that lost ``U+E000..U+F8FF`` still compiles and still matches the
    two supplementary planes, which nothing in this corpus uses -- so it would
    pass every bullet while looking like a gate.
    """
    from backend.enrichment import brochure_extract as be
    from backend.enrichment.brochure_sources import _PRIVATE_USE_AREA

    for codepoint in (0xE000, 0xE044, 0xEA04, 0xF06E, 0xF6BA, 0xF8FF):
        assert be._BULLET_PRIVATE_USE_AREA.search(chr(codepoint)), hex(codepoint)
    for codepoint in (0xF0000, 0x100000):
        assert be._BULLET_PRIVATE_USE_AREA.search(chr(codepoint)), hex(codepoint)
    for codepoint in (ord("A"), ord("8"), 0x2022, 0xF900):
        assert not be._BULLET_PRIVATE_USE_AREA.search(chr(codepoint)), hex(codepoint)
    assert be._BULLET_PRIVATE_USE_AREA.pattern == _PRIVATE_USE_AREA.pattern


def test_bullet_text_is_readable_rejects_unresolved_glyphs():
    from backend.enrichment.brochure_extract import bullet_text_is_readable

    assert bullet_text_is_readable("2.0L TwinPower Turbo, 248 hp") is True
    # 2012__jaguar__xf prints the inch mark as U+E044.
    assert bullet_text_is_readable("18" + chr(0xE044) + " lyra alloy wheels") is False
    # 2012__cadillac__cts prints the hyphen as U+F6BA.
    assert bullet_text_is_readable("DIRECT" + chr(0xF6BA) + "INJECTION V6") is False
    assert bullet_text_is_readable("PREM�i� PLUS") is False
    # Empty is "readable" -- it carries no unresolved glyph. Empty bullets are
    # dropped by the ``str(a).strip()`` filter beside this one, not here.
    assert bullet_text_is_readable("") is True


def _overlay_with_one_garbled_bullet():
    garbled = "18" + chr(0xE044) + " lyra alloy wheels"
    return {
        "source": "brochure_text_quoted",
        "year": 2018,
        "adds_by_trim": {"Sport": ["Heated front seats", garbled]},
        "adds_provenance": {
            "Sport": [
                {
                    "text": "Heated front seats",
                    "source": "derived/brochure_text/2018__x__y.json",
                    "page": 4,
                    "verified": True,
                },
                {
                    "text": garbled,
                    "source": "derived/brochure_text/2018__x__y.json",
                    "page": 4,
                    "verified": True,
                },
            ]
        },
    }, garbled


def test_a_verified_citation_does_not_make_an_unreadable_bullet_renderable():
    """
    The citation is sound and the bullet is still refused.

    The verifier re-opened the PDF and found that text on that page -- both
    entries are ``verified: true``. What it confirms is that we copied the page
    faithfully, not that what we copied is readable. A PUA codepoint means the
    font's glyph never resolved to a character, so the string is not the
    document's words.
    """
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    overlay, garbled = _overlay_with_one_garbled_bullet()
    kept = admissible_overlay_adds(overlay, for_year=2018)
    assert kept == {"Sport": ["Heated front seats"]}
    assert garbled not in kept["Sport"]


def test_the_provenance_kill_switch_does_not_reopen_unreadable_bullets(monkeypatch):
    """
    ``TRIM_ADDS_REQUIRE_PROVENANCE=0`` restores uncited bullets, not unreadable ones.

    ``admissible_overlay_adds`` has three returns that never consult a citation
    -- the kill switch, a non-per-bullet source, and an unparseable provenance
    block. The readability filter is applied during normalisation so it covers
    all of them rather than only the cited path.
    """
    from backend.enrichment.brochure_extract import admissible_overlay_adds

    overlay, garbled = _overlay_with_one_garbled_bullet()
    monkeypatch.setenv("TRIM_ADDS_REQUIRE_PROVENANCE", "0")
    kept = admissible_overlay_adds(overlay, for_year=2018)
    assert kept == {"Sport": ["Heated front seats"]}
    assert garbled not in kept["Sport"]


def test_an_unreadable_bullet_cannot_justify_listing_a_rung():
    """A trim whose only cited bullet is garbled is not a rung we can show."""
    from backend.enrichment.brochure_extract import verified_overlay_trim_names

    overlay, garbled = _overlay_with_one_garbled_bullet()
    overlay["adds_by_trim"] = {"Sport": [garbled]}
    overlay["adds_provenance"]["Sport"] = [overlay["adds_provenance"]["Sport"][1]]
    assert verified_overlay_trim_names(overlay, for_year=2018) == []


# --------------------------------------------------------------------------
# persist_brochure_text is the chokepoint every writer goes through
# --------------------------------------------------------------------------


def _capture(pages, *, year=2018, make="BMW", model="3 Series"):
    from backend.enrichment import brochure_extract as be

    return be.BrochureTextResult(
        ymm=be.BrochureYMM(year=year, make=make, model=model),
        source_pdf="synthetic.pdf",
        page_count=len(pages),
        pages=[
            be.BrochurePageText(page=i, trim_hint=True, text=text)
            for i, text in enumerate(pages, 1)
        ],
    )


def test_persist_brochure_text_writes_a_sound_capture_to_the_live_corpus(tmp_path, monkeypatch):
    from backend.enrichment import brochure_extract as be

    live = tmp_path / "brochure_text"
    monkeypatch.setattr(be, "BROCHURE_TEXT_DIR", live)
    body = "The 3 Series Sedan. 2.0L 248 hp 258 lb-ft 26 city 36 highway mpg. " * 60
    out = be.persist_brochure_text(_capture([body, body]))

    assert out.parent == live
    assert out.is_file()


def test_persist_brochure_text_quarantines_a_capture_that_fails_a_corpus_gate(
    tmp_path, monkeypatch
):
    """
    THE HOLE THIS CLOSES. Checked 2026-08-02: ``persist_brochure_text`` has three
    callers and only ``fetch_oem_brochures.py`` ran the corpus gates before
    calling it. ``reingest_brochures.py`` and ``extract_brochure_text.py`` wrote
    straight into ``derived/brochure_text`` with no quality or subject test at
    all, and those are the scripts a corpus-wide extraction run uses.

    Quarantined rather than refused: the extraction is expensive, the reason can
    turn out to be wrong, and the file has to be movable back.
    """
    from backend.enrichment import brochure_extract as be
    from backend.enrichment import brochure_sources as bs

    live = tmp_path / "brochure_text"
    quarantine = tmp_path / "brochure_text_quarantine"
    monkeypatch.setattr(be, "BROCHURE_TEXT_DIR", live)
    monkeypatch.setattr(bs, "BROCHURE_TEXT_QUARANTINE_DIR", quarantine)

    digits = [chr(0xEA01 + i) for i in range(10)]
    remapped = "".join(digits)
    body = (
        "The 3 Series Sedan sDrive"
        + digits[1]
        + digits[7]
        + "i "
        + remapped
        + " liter inline six. "
    ) * 60
    out = be.persist_brochure_text(_capture([body, body]))

    assert out.parent == quarantine
    assert out.is_file()
    assert not live.exists() or not list(live.glob("*.json"))


def test_persist_brochure_text_quarantines_the_real_m3_book_filed_as_a_3_series(
    tmp_path, monkeypatch
):
    """
    The wrong-vehicle case, driven through the write path rather than the check.

    Page text verbatim from ``backend/data/brochures/2018_BMW_3_Series_Brochure.pdf``,
    re-opened with pdfplumber on 2026-08-02: the running header on all nine
    pages, and the single line in the whole book that says "3 Series".
    """
    from backend.enrichment import brochure_extract as be
    from backend.enrichment import brochure_sources as bs

    live = tmp_path / "brochure_text"
    quarantine = tmp_path / "brochure_text_quarantine"
    monkeypatch.setattr(be, "BROCHURE_TEXT_DIR", live)
    monkeypatch.setattr(bs, "BROCHURE_TEXT_QUARANTINE_DIR", quarantine)

    header = (
        "BMW M3 SEDAN EXTERIOR COLORS UPHOLSTERY INTERIOR TRIMS "
        "WHEELS / TIRES PACKAGES TECHNICAL DATA"
    )
    prose = (
        "FOUR DOORS, FAST. The 3 Series is the best-selling BMW sedan. Add M to "
        "the equation and its personality transforms for 425 hp and 406 lb-ft. "
    ) * 20
    pages = [header + "\n" + prose] + [header + "\nM3 SEDAN 2 of 9 425 hp"] * 8
    out = be.persist_brochure_text(_capture(pages))

    assert out.parent == quarantine
    assert not live.exists() or not list(live.glob("*.json"))


# --- rung ORDER: quoted "<TRIM> adds to <LOWER>" edges -----------------------
#
# Page text below is verbatim from
# ``backend/dictionary/derived/brochure_text/2020__dodge__durango.json``
# (pages 28-31 of the 2020 Dodge Durango brochure), trimmed to the heading and
# the first two bullets of each walk block. The GT page additionally carries an
# ``OPTIONS/PACKAGES`` block in the same shape the real page prints one, so the
# options-vs-adds separation that the corpus-gated tests above pin against the
# real file is also exercised here, where no asset is needed.

_DURANGO_WALK_PAGES = {
    28: (
        "GT\nAdds to SXT\n"
        "• 20 by 8-inch Satin Carbon wheels\n"
        "• Dual exhaust with bright tips\n"
        "• Premium LED fog lamps\n"
        "• LED daytime running lamps (DRLs)\n"
        "• 7-passenger seating\n"
        "• 12-way power driver’s seat\n"
        "OPTIONS/PACKAGES\n"
        "• Power sunroof\n"
        "• Second-row captain’s chairs\n"
    ),
    29: (
        "R/T\nAdds to GT\n"
        "• 5.7L HEMI® V8 engine\n"
        "• Performance steering and suspension\n"
        "• Rear load-leveling suspension\n"
        "• Performance hood\n"
        "• Heated steering wheel\n"
        "• 12-way power passenger seat\n"
    ),
    30: (
        "CITADEL\nAdds to GT\n"
        "• 20 by 8-inch Platinum Chrome wheels\n"
        "• Projector fog lamps\n"
        "• Bright roof rails\n"
        "• Power sunroof\n"
        "• Capri leather-trimmed seating\n"
        "• Heated steering wheel\n"
    ),
    31: (
        "SRT®\nAdds to R/T\n"
        "• 6.4L 392 HEMI® V8 engine\n"
        "• Performance-tuned All-Wheel Drive (AWD)\n"
        "• TorqueFlite® 8-speed automatic transmission\n"
        "• Brembo® 6-piston brakes\n"
        "• Bilstein® active-damping suspension\n"
        "• 20 by 10-inch Black Noise Split-Spoke wheels\n"
    ),
}


def _durango_walk_payload() -> dict:
    return {
        "pages": [
            {"page": no, "text": text} for no, text in sorted(_DURANGO_WALK_PAGES.items())
        ]
    }


def test_adds_to_page_returns_the_edge_with_its_page_and_printed_lines():
    """The ordering fact is quoted, not inferred: file, page, and both lines."""
    from backend.enrichment.brochure_trim_candidates import _extract_adds_to_page

    _adds, edges, _reasons = _extract_adds_to_page(
        29,
        _DURANGO_WALK_PAGES[29],
        make="Dodge",
        model="Durango",
        source="derived/brochure_text/2020__dodge__durango.json",
    )
    edge = edges["R/T"]
    assert (edge.trim, edge.below) == ("R/T", "GT")
    assert edge.page == 29
    assert edge.layout == "adds_to_block"
    assert edge.source.endswith("2020__dodge__durango.json")
    # Both quotes have to be findable in the page text as printed.
    assert edge.trim_quote in _DURANGO_WALK_PAGES[29]
    assert edge.below_quote in _DURANGO_WALK_PAGES[29]


def test_durango_walk_orders_itself_from_its_own_pages():
    """GT -> R/T -> Citadel -> SRT, and the R/T-vs-Citadel tie is declared."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    extract = extract_trim_walk(
        _durango_walk_payload(), make="Dodge", model="Durango", year=2020
    )
    assert extract.trims_available == ["SXT", "GT", "R/T", "Citadel", "SRT"]
    assert all(v["basis"] == "adds_to_edge" for v in extract.order_basis.values())
    # The book says both R/T and Citadel add to GT. It does NOT say which of the
    # two is higher, and the overlay has to admit that.
    assert extract.order_basis["R/T"]["tied_with"] == ["Citadel"]
    assert extract.order_basis["Citadel"]["tiebreak"] == "page_sequence"
    # SXT is placed by GT's page naming it, not by a page of its own.
    assert extract.order_basis["SXT"]["direction"] == "named_as_baseline_by"


def test_synthetic_walk_options_block_is_not_reported_as_trim_adds():
    """Ungated companion to the corpus-gated OPTIONS/PACKAGES test above.

    The GT page lists "Power sunroof" under OPTIONS/PACKAGES; Citadel prints it
    as one of its own bullets. It must appear under Citadel and never under GT.
    """
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    walk = extract_trim_walk(
        _durango_walk_payload(), make="Dodge", model="Durango", year=2020
    )
    assert walk.usable
    assert not any("sunroof" in b.lower() for b in walk.adds_by_trim["GT"])
    assert not any("captain" in b.lower() for b in walk.adds_by_trim["GT"])
    assert any("sunroof" in b.lower() for b in walk.adds_by_trim["Citadel"])


def test_synthetic_walk_provenance_pages_really_contain_their_text():
    """Ungated companion to the corpus-gated quoted-with-pages test above."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk

    walk = extract_trim_walk(
        _durango_walk_payload(), make="Dodge", model="Durango", year=2020
    )
    assert {"GT", "R/T", "Citadel", "SRT"} <= set(walk.adds_by_trim)
    rt = walk.adds_by_trim["R/T"]
    assert "5.7L HEMI® V8 engine" in rt
    assert rt[0] == "5.7L HEMI® V8 engine"

    pages = {p["page"] for rows in walk.provenance.values() for p in rows}
    assert pages <= set(walk.pages_used)
    for trim, rows in walk.provenance.items():
        for row in rows:
            head = row["text"].split("®")[0].split("™")[0][:18]
            assert head in _DURANGO_WALK_PAGES[row["page"]], (trim, row)


def test_synthetic_walk_bullets_pass_the_display_gate():
    """Ungated companion to the corpus-gated display-gate test above."""
    from backend.enrichment.brochure_trim_candidates import extract_trim_walk
    from backend.enrichment.trim_spec_extractor import is_displayable_trim_bullet

    walk = extract_trim_walk(
        _durango_walk_payload(), make="Dodge", model="Durango", year=2020
    )
    assert walk.gate_rejected == 0
    for bullets in walk.adds_by_trim.values():
        for b in bullets:
            assert is_displayable_trim_bullet(b), b


def test_disconnected_walks_are_ordered_by_page_not_interleaved_by_depth():
    """Two separate trees in one book: whole trees move, they do not interleave.

    Ordering purely by chain depth put the 2021 Challenger's SRT Hellcat below
    R/T Scat Pack Widebody, because the two walks are unrelated in the document.
    """
    from backend.enrichment.brochure_trim_candidates import (
        QuotedRungEdge,
        order_rungs_with_basis,
    )

    def edge(trim, below, page):
        return QuotedRungEdge(
            trim=trim,
            below=below,
            page=page,
            layout="adds_to_block",
            source="x.json",
            trim_quote=trim,
            below_quote=f"ADDS TO {below}",
        )

    edges = {
        "Scat Pack Widebody": edge("Scat Pack Widebody", "Scat Pack", 47),
        "Hellcat Widebody": edge("Hellcat Widebody", "Hellcat", 49),
    }
    adds = dict.fromkeys(["Scat Pack", "Scat Pack Widebody", "Hellcat", "Hellcat Widebody"], [])
    order, basis = order_rungs_with_basis(
        edges,
        adds,
        page_by_trim={
            "Scat Pack": 46,
            "Scat Pack Widebody": 47,
            "Hellcat": 48,
            "Hellcat Widebody": 49,
        },
    )
    assert order == ["Scat Pack", "Scat Pack Widebody", "Hellcat", "Hellcat Widebody"]
    # Nothing in the book relates one tree to the other, and every rung says so.
    assert all(v["cross_component_order"] == "page_sequence" for v in basis.values())


def test_overlay_rung_order_refuses_an_uncitable_or_wrong_year_overlay():
    """Same discipline as the bullets: name a page for this model year, or nothing."""
    from backend.enrichment.brochure_extract import overlay_rung_order

    good = {
        "source": "brochure_text_quoted",
        "year": 2020,
        "rung_order": ["GT", "R/T"],
        "order_basis": {
            "GT": {
                "basis": "adds_to_edge",
                "edge": {
                    "trim": "GT",
                    "below": "SXT",
                    "page": 28,
                    "source": "derived/brochure_text/2020__dodge__durango.json",
                    "trim_quote": "GT",
                    "below_quote": "Adds to SXT",
                },
            },
            "R/T": {
                "basis": "adds_to_edge",
                "edge": {
                    "trim": "R/T",
                    "below": "GT",
                    "page": 29,
                    "source": "derived/brochure_text/2020__dodge__durango.json",
                    "trim_quote": "R/T",
                    "below_quote": "Adds to GT",
                },
            },
        },
    }
    order, basis = overlay_rung_order(good, for_year=2020)
    assert order == ["GT", "R/T"]
    assert basis["R/T"]["store"] == "brochure_adds_to_edge"
    assert basis["R/T"]["proven"] is True

    # A neighbouring model year's book is not evidence about this car.
    assert overlay_rung_order(good, for_year=2021) == ([], {})
    # ...nor is an overlay whose source records no ordering evidence at all.
    assert overlay_rung_order({**good, "source": "brochure_llm"}, for_year=2020) == ([], {})

    # An edge with no page cannot be re-checked, so it does not order anything.
    pageless = json.loads(json.dumps(good))
    for entry in pageless["order_basis"].values():
        entry["edge"].pop("page")
    assert overlay_rung_order(pageless, for_year=2020) == ([], {})
