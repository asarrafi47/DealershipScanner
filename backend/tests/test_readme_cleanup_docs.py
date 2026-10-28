"""Doc consistency for the README, debug/README and the cloud restructure plan (P18B.2, P18B.3)."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _text(rel):
    return (ROOT / rel).read_text(encoding="utf-8")


def test_scanner_node_doc_removed_and_unlinked():
    assert not (ROOT / "docs/SCANNER_NODE.md").exists()
    assert "SCANNER_NODE" not in _text("README.md")
    assert "Optional: Node" not in _text("README.md")


def test_readme_trim_ladders_point_at_curated():
    readme = _text("README.md")
    assert "backend/dictionary/curated/trim_ladders.json" in readme
    assert (ROOT / "backend/dictionary/curated/trim_ladders.json").exists()


def test_debug_readme_drops_removed_tools():
    text = _text("debug/README.md")
    assert "scanner.js" not in text
    assert "e2e_deep_browser" not in text


def test_cloud_plan_banner_and_every_decision_has_outcome():
    text = _text("docs/CLOUD_RESTRUCTURE_PLAN.md")
    assert "Status 2026-10: superseded by" in text
    for d in ("D1", "D2", "D3", "D4", "D5"):
        line = next(l for l in text.splitlines() if l.startswith(f"- **{d}**"))
        assert "**Decided" in line or "**Open**" in line, d
