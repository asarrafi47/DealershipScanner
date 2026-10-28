"""P18B.7a: NETWORK_SCAN_PROCESS.md, CLAUDE.md and pipeline/dealer_logs.py agree on where location evidence goes."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = (ROOT / "docs" / "NETWORK_SCAN_PROCESS.md").read_text(encoding="utf-8")
CODE = (ROOT / "backend" / "scanner" / "pipeline" / "dealer_logs.py").read_text(encoding="utf-8")
CLAUDE = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")


def test_code_writes_what_the_doc_names():
    assert "- location:" in CODE and "`- location:` line" in DOC
    assert "store location for rooftop attribution" in CODE and "store location for rooftop attribution" in DOC
    assert "def log_location" in CODE and "log_location" in DOC
    assert "log_location(r, stamp)" in CODE.split("def log_run_files")[1]


def test_claude_md_and_doc_list_the_same_log_files():
    for name in ("discovery.md", "location.md", "scan_runs.md", "summary.md", "scan_instructions.md"):
        assert name in CLAUDE and name in DOC and name in CODE


def test_location_patterns_doc_is_referenced():
    assert "_learning/location_patterns.md" in DOC
