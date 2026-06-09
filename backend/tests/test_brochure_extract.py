"""Tests for brochure PDF trim extraction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.enrichment.brochure_extract import (
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


@pytest.mark.skipif(not _OVERLAY.is_file(), reason="run process_brochure_queue first")
def test_resolve_trim_ladder_uses_brochure_overlay():
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
