"""Gate for fencing script-only dictionary trees out of the images (P9.7). No network."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from backend.enrichment import brochure_extract
from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

ROOT = Path(__file__).resolve().parents[2]
D = "backend/dictionary"

FENCED = [
    f"{D}/derived/trim_candidates/",
    f"{D}/derived/html_spec_text/",
    f"{D}/derived/html_spec_overlays/",
    f"{D}/derived/trim_overlay_validation.jsonl",
    f"{D}/derived/brochure_content_index.json",
    f"{D}/derived/trim_citation_verification.json",
    f"{D}/index/manifest.json",
    f"{D}/derived/brochure_text/",
]
NEVER_FENCED = [
    "epa", "options/raw", "curated", "derived/trim_adds_by_year", "derived/brochure_text_slim",
    "derived/trim_spec_sheets", "derived/epa_ev_range_miles.json", "index/make_aliases.json",
    "platform_registry.json",
]


def _entries(name: str) -> set[str]:
    return {
        l.strip()
        for l in (ROOT / name).read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.lstrip().startswith("#")
    }


def test_both_ignore_files_fence_every_script_only_tree():
    for name in (".dockerignore", ".railwayignore"):
        missing = [p for p in FENCED if p not in _entries(name)]
        assert not missing, f"{name} does not fence {missing}"


def test_request_path_trees_are_never_fenced():
    for name in (".dockerignore", ".railwayignore"):
        for entry in _entries(name):
            bare = entry.strip("/").removeprefix(f"{D}/")
            for keep in NEVER_FENCED:
                assert not (bare == keep or bare.startswith(keep + "/")), f"{name} fences {keep}: {entry}"


def test_every_brochure_text_quoted_overlay_carries_order_basis():
    # brochure_text/ is fenced only because this holds: attach_rung_order never
    # needs the brochure text file for a shipped overlay.
    bad, seen = [], 0
    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if str(data.get("source") or "").strip().lower() not in brochure_extract.ORDER_BEARING_OVERLAY_SOURCES:
            continue
        seen += 1
        basis = data.get("order_basis")
        if not (isinstance(basis, dict) and basis):
            bad.append(path.name)
    assert seen, "no brochure_text_quoted overlays found; the gate would be vacuous"
    assert not bad, f"overlays missing order_basis: {bad}"


def test_fallback_branch_is_logged(monkeypatch, caplog):
    monkeypatch.setattr(brochure_extract, "_rung_order_from_brochure_text", lambda *a: ((), "{}"))
    overlay = {"source": "brochure_text_quoted", "catalog_key": "2024|x|y", "year": 2024, "make": "x", "model": "y"}
    with caplog.at_level(logging.WARNING, logger=brochure_extract.logger.name):
        assert brochure_extract.attach_rung_order(overlay) == overlay
    assert any("no order_basis" in r.message for r in caplog.records)
