"""Tests for brochure overlay promotion (GC quality bar)."""

from __future__ import annotations

import json
from pathlib import Path

from backend.enrichment.brochure_promote import (
    extract_promotable_overlay,
    meets_gc_quality_bar,
)
from backend.enrichment.brochure_trim_candidates import load_brochure_text_json


def test_meets_gc_quality_bar_reference_shape() -> None:
    ref = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "dictionary/derived/trim_adds_by_year/2016__jeep__grandcherokee.json"
        ).read_text(encoding="utf-8")
    )
    trims = list(ref.get("trims_available") or [])
    adds = ref.get("adds_by_trim") or {}
    assert meets_gc_quality_bar(trims, adds)


def test_promote_2012_honda_accord_from_brochure_text() -> None:
    data = load_brochure_text_json("2012|honda|accord")
    if not data:
        return
    payload = extract_promotable_overlay(data)
    assert payload is not None
    adds = payload.get("adds_by_trim") or {}
    assert "EX-L" in adds
    assert any("leather" in a.lower() for a in adds["EX-L"])


def test_promote_2025_camry_from_brochure_text() -> None:
    data = load_brochure_text_json("2025|toyota|camry")
    if not data:
        return
    payload = extract_promotable_overlay(data)
    assert payload is not None
    assert payload.get("source") == "promoted_brochure_auto"
    assert len(payload.get("trims_available") or []) >= 2
    adds = payload.get("adds_by_trim") or {}
    assert any(len(v) >= 4 for v in adds.values())
