"""Layout invariants for backend/dictionary Complete_Options CSVs (Phase 9).

P9.1: every row of a legacy root file exists, as a full tuple, in its sharded copy under
options/raw/<Make>/. P9.3 and P9.4 extend this file when the root copies are deleted.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from backend.scripts.merge_root_summary_rows import SUFFIX, apply_merge, plan_merge

_DICT = Path(__file__).resolve().parents[1] / "dictionary"
_HEADER = ["Year", "Make", "Model", "Trim", "Options", "optionDetails"]
_SUMMARY = ["2021", "Kia", "Telluride", "[Brochure Summary]", "Harman Kardon audio", "[Brochure] Harman Kardon audio"]


def _write(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)


def _read(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.reader(fh))


def test_merge_appends_only_missing_root_rows(tmp_path: Path) -> None:
    root, raw = tmp_path, tmp_path / "options" / "raw"
    name = f"2021_Kia_Telluride{SUFFIX}"
    base = ["2021", "Kia", "Telluride", "LX", "a, b", "c \"q\""]
    _write(root / name, [_HEADER, base, _SUMMARY])
    _write(raw / "Kia" / name, [_HEADER, base])
    plan = plan_merge(root, raw)
    assert [m["rows"] for m in plan["merges"]] == [[_SUMMARY]]
    assert apply_merge(plan) == 1
    assert _read(raw / "Kia" / name) == [_HEADER, base, _SUMMARY]
    assert plan_merge(root, raw)["merges"] == []  # idempotent


def test_merge_reports_stubs_and_header_mismatch(tmp_path: Path) -> None:
    root, raw = tmp_path, tmp_path / "options" / "raw"
    _write(root / f"a{SUFFIX}", [_HEADER])
    _write(root / f"b{SUFFIX}", [_HEADER, _SUMMARY])
    _write(raw / "Kia" / f"b{SUFFIX}", [list(reversed(_HEADER))])
    plan = plan_merge(root, raw)
    assert plan["no_shard"] == [f"a{SUFFIX}"]
    assert plan["ambiguous"] == [f"b{SUFFIX}"]
    assert plan["merges"] == []


def test_merge_terminates_unterminated_last_line(tmp_path: Path) -> None:
    root, raw = tmp_path, tmp_path / "options" / "raw"
    name = f"x{SUFFIX}"
    _write(root / name, [_HEADER, _SUMMARY])
    shard = raw / "Kia" / name
    shard.parent.mkdir(parents=True)
    shard.write_bytes(",".join(_HEADER).encode())  # header only, no trailing newline
    apply_merge(plan_merge(root, raw))
    assert _read(shard) == [_HEADER, _SUMMARY]


def test_every_root_row_exists_in_sharded_copy() -> None:
    """P9.1 acceptance: 0 root-only rows across the root files (vacuous after P9.3)."""
    if not list(_DICT.glob(f"*{SUFFIX}")):
        pytest.skip("root copies already removed")
    plan = plan_merge()
    assert plan["merges"] == []
    assert plan["ambiguous"] == []


def test_root_preflight_flags_content_that_would_be_lost(tmp_path: Path) -> None:
    from backend.scripts.delete_dictionary_duplicates import preflight_root

    raw = tmp_path / "options" / "raw" / "Kia"
    base = ["2021", "Kia", "Telluride", "LX", "a", "b"]
    _write(tmp_path / f"covered{SUFFIX}", [_HEADER, base])
    _write(raw / f"covered{SUFFIX}", [_HEADER, base])
    _write(tmp_path / f"stub{SUFFIX}", [_HEADER])
    _write(tmp_path / f"lost{SUFFIX}", [_HEADER, base, _SUMMARY])
    _write(raw / f"lost{SUFFIX}", [_HEADER, base])
    _write(tmp_path / f"orphan{SUFFIX}", [_HEADER, base])
    plan = preflight_root(tmp_path)
    assert plan["delete"] == [f"covered{SUFFIX}", f"stub{SUFFIX}"]
    assert plan["lost"] == {f"lost{SUFFIX}": [_SUMMARY]}
    assert plan["unsharded"] == [f"orphan{SUFFIX}"]


def test_stub_preflight_flags_stubs_with_rows(tmp_path: Path) -> None:
    from backend.scripts.delete_dictionary_duplicates import preflight_stubs

    stubs = tmp_path / "options" / "stubs" / "Kia"
    _write(stubs / f"a{SUFFIX}", [_HEADER])
    _write(stubs / f"b{SUFFIX}", [_HEADER, _SUMMARY])
    plan = preflight_stubs(tmp_path)
    assert plan["delete"] == [f"options/stubs/Kia/a{SUFFIX}"]
    assert plan["with_rows"] == [f"options/stubs/Kia/b{SUFFIX}"]


def test_root_copies_are_gone() -> None:
    """P9.3 acceptance: no root-level Complete_Options CSV remains."""
    assert not list(_DICT.glob(f"*{SUFFIX}"))


def test_stub_directory_is_gone() -> None:
    """P9.4 acceptance: no header-only stub files are tracked."""
    assert not list((_DICT / "options" / "stubs").glob(f"*/*{SUFFIX}"))
