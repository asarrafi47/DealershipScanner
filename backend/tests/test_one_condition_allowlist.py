"""P5A.3: the one-condition allow-list is a tracked file with one loader."""
from __future__ import annotations

from pathlib import Path

from backend.scanner import recipe_validation as rv
from backend.scanner.pipeline import assess

REPO = Path(__file__).resolve().parents[2]


def _point(monkeypatch, tracked: Path, workspace: Path) -> None:
    monkeypatch.setattr(rv, "ONE_CONDITION_OK_TRACKED_PATH", tracked)
    monkeypatch.setattr(rv, "ONE_CONDITION_OK_PATH", workspace)


def test_tracked_file_carries_the_known_used_only_lots():
    ids = rv.one_condition_ok_ids(rv.ONE_CONDITION_OK_TRACKED_PATH)
    assert {"quantumautosales-com", "honestcardeal-com"} <= ids
    assert rv.ONE_CONDITION_OK_TRACKED_PATH == REPO / "backend" / "config" / "one_condition_ok.txt"


def test_loader_returns_the_union_of_both_files(tmp_path, monkeypatch):
    tracked, local = tmp_path / "tracked.txt", tmp_path / "local.txt"
    tracked.write_text("# c\na-com  # one\nb-com\n")
    local.write_text("b-com\nc-com # local only\n\n")
    _point(monkeypatch, tracked, local)
    assert rv.one_condition_ok_ids() == {"a-com", "b-com", "c-com"}


def test_either_file_may_be_missing(tmp_path, monkeypatch):
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("a-com\n")
    _point(monkeypatch, tracked, tmp_path / "nope.txt")
    assert rv.one_condition_ok_ids() == {"a-com"}
    _point(monkeypatch, tmp_path / "nope1.txt", tmp_path / "nope2.txt")
    assert rv.one_condition_ok_ids() == set()


def test_explicit_path_reads_that_file_alone(tmp_path, monkeypatch):
    tracked, other = tmp_path / "tracked.txt", tmp_path / "other.txt"
    tracked.write_text("a-com\n")
    other.write_text("z-com\n")
    _point(monkeypatch, tracked, tmp_path / "nope.txt")
    assert rv.one_condition_ok_ids(other) == {"z-com"}


def test_assess_wrapper_uses_the_same_loader(tmp_path, monkeypatch):
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("a-com\n")
    _point(monkeypatch, tracked, tmp_path / "nope.txt")
    assert assess._one_condition_ok() == {"a-com"} == rv.one_condition_ok_ids()
    monkeypatch.setattr(rv, "one_condition_ok_ids", lambda path=None: {"patched"})
    assert assess._one_condition_ok() == {"patched"}


def test_dockerignore_lets_the_tracked_file_into_the_build_context():
    import fnmatch

    rel = "backend/config/one_condition_ok.txt"
    for raw in (REPO / ".dockerignore").read_text().splitlines():
        pat = raw.strip()
        if not pat or pat.startswith("#") or pat.startswith("!"):
            continue
        pat = pat.rstrip("/")
        if pat.startswith("**/"):
            pat = pat[3:]
        assert not (fnmatch.fnmatch(rel, pat) or rel.startswith(pat + "/") or fnmatch.fnmatch(Path(rel).name, pat)), raw
