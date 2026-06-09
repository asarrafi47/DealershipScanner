"""Tests for brochure-derived trim candidate parsing."""

from __future__ import annotations

import json
from pathlib import Path

from backend.enrichment.brochure_trim_candidates import (
    build_trim_candidate,
    parse_trim_names_from_text,
)


def test_parse_trim_names_2025_camry():
    path = (
        Path(__file__).resolve().parents[1]
        / "dictionary/derived/brochure_text/2025__toyota__camry.json"
    )
    if not path.is_file():
        return
    data = json.loads(path.read_text(encoding="utf-8"))
    text = data.get("combined_trim_pages_text") or ""
    trims = parse_trim_names_from_text(text, make="Toyota", model="Camry")
    names = {t.upper() for t in trims}
    assert "LE" in names
    assert "SE" in names
    assert "XSE" in names or "XLE" in names


def test_build_trim_candidate_structure():
    data = {
        "catalog_key": "2025|toyota|camry",
        "year": 2025,
        "make": "Toyota",
        "model": "Camry",
        "combined_trim_pages_text": "CAMRY MODELS\nLE SE XSE XLE\nIncludes these key features",
        "trim_hint_pages": [11],
        "warnings": [],
        "pages": [],
    }
    cand = build_trim_candidate(data, make="Toyota", model="Camry", year=2025)
    assert cand["status"] == "draft"
    assert len(cand.get("trims_available") or []) >= 2
