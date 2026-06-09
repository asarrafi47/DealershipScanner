"""Trim spec sheet loading and extraction."""

from __future__ import annotations

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


def test_ram_1500_limited_has_structured_specs() -> None:
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
