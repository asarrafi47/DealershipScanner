"""merge_dealer_logs (P6A.5): idempotent, no duplicate blocks, nothing deleted."""
from __future__ import annotations

from backend.scripts import merge_dealer_logs as m

A = "## 2026-09-25 20:44 UTC scan\n- rows 944"
B = "## 2026-09-28 17:44 UTC scan\n- rows 936"
C = "## 2026-10-14 09:00 UTC scan\n- rows 12"
HEAD = "# d — scan_runs\n\nProcess: x\n\n"


def _doc(*blocks):
    return HEAD + "\n\n".join(blocks) + "\n"


def test_missing_blocks_added_in_date_order_and_second_run_is_noop():
    dest, src = _doc(A, C), _doc(A, B)
    once = m.merge_text(dest, src, name="scan_runs.md")
    assert once.count(A) == 1 and once.index(B) < once.index(C)
    assert m.merge_text(once, src, name="scan_runs.md") == once


def test_same_heading_different_body_keeps_both_once():
    dest, src = _doc(A), _doc(A.replace("944", "900"))
    once = m.merge_text(dest, src, name="scan_runs.md")
    assert "rows 944" in once and "rows 900" in once and "[pulled]" in once
    assert m.merge_text(once, src, name="scan_runs.md") == once


def test_fixed_section_lines_union():
    d = "# d\n\n## Issues faced and how they were fixed\n\n- one\n"
    s = "# d\n\n## Issues faced and how they were fixed\n\n- one\n- two\n"
    out = m.merge_text(d, s, name="summary.md")
    assert out.count("- one") == 1 and "- two" in out
    assert m.merge_text(out, s, name="summary.md") == out


def test_instructions_provisional_replaced_only_by_real():
    prov = "# d\nPROVISIONAL: none yet\n"
    real = "# d\nwritten after the first verified run\n"
    assert m.merge_text(prov, real, name="scan_instructions.md") == real
    assert m.merge_text(real, prov, name="scan_instructions.md") == real


def test_tree_dry_run_then_apply_then_clean(tmp_path):
    src, dest = tmp_path / "src", tmp_path / "dest"
    (src / "a-com").mkdir(parents=True)
    (dest / "b-com").mkdir(parents=True)
    (src / "a-com" / "scan_runs.md").write_text(_doc(A, B))
    (src / "_learning").mkdir()
    (src / "_learning" / "errors_index.md").write_text("# e\n- x\n- y\n")
    (dest / "_learning").mkdir()
    (dest / "_learning" / "errors_index.md").write_text("# e\n- x\n")
    (dest / "b-com" / "summary.md").write_text("keep")
    rep = m.merge_tree(src, dest)
    assert rep["created"] == ["a-com/scan_runs.md"] and rep["merged"] == ["_learning/errors_index.md"]
    assert not (dest / "a-com").exists()  # dry run wrote nothing
    m.merge_tree(src, dest, apply=True)
    assert (dest / "b-com" / "summary.md").read_text() == "keep"
    assert "- y" in (dest / "_learning" / "errors_index.md").read_text()
    again = m.merge_tree(src, dest, apply=True)
    assert again["created"] == [] and again["merged"] == []
