#!/usr/bin/env python3
"""Content pre-flight for deleting dictionary duplicates and stubs (P9.3, P9.4).

``--root`` (P9.3): every legacy root ``backend/dictionary/*_Complete_Options.csv`` must be
header-only or have each of its rows, as a full tuple, in exactly one sharded copy under
``options/raw`` or ``options/stubs``. ``--stubs`` (P9.4): every file under ``options/stubs``
must hold no data row. The pre-flight is read-only and exits 1 when any file would lose
content; the deletion itself is a plain ``git rm`` (rollback is ``git revert``).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from backend.scripts.merge_root_summary_rows import SUFFIX, _read

_DICT = Path(__file__).resolve().parents[1] / "dictionary"


def _sharded(dict_dir: Path) -> dict[str, list[Path]]:
    index: dict[str, list[Path]] = {}
    for sub in ("raw", "stubs"):
        for p in sorted((dict_dir / "options" / sub).glob(f"*/*{SUFFIX}")):
            index.setdefault(p.name, []).append(p)
    return index


def preflight_root(dict_dir: Path = _DICT) -> dict:
    """``{"delete": [names], "lost": {name: [rows]}, "unsharded": [names]}``."""
    index = _sharded(dict_dir)
    delete: list[str] = []
    lost: dict[str, list[list[str]]] = {}
    unsharded: list[str] = []
    for root in sorted(dict_dir.glob(f"*{SUFFIX}")):
        rows = [r for r in _read(root)[1:] if r]
        targets = index.get(root.name, [])
        if not rows:
            delete.append(root.name)
            continue
        have = {tuple(r) for t in targets for r in _read(t)[1:]}
        missing = [r for r in rows if tuple(r) not in have]
        if not targets:
            unsharded.append(root.name)
        elif missing:
            lost[root.name] = missing
        else:
            delete.append(root.name)
    return {"delete": delete, "lost": lost, "unsharded": unsharded}


def preflight_stubs(dict_dir: Path = _DICT) -> dict:
    """``{"delete": [paths], "with_rows": [paths]}`` for ``options/stubs``."""
    delete: list[str] = []
    with_rows: list[str] = []
    stubs = dict_dir / "options" / "stubs"
    for p in sorted(stubs.glob(f"*/*{SUFFIX}")):
        rel = str(p.relative_to(dict_dir))
        (with_rows if any(_read(p)[1:]) else delete).append(rel)
    return {"delete": delete, "with_rows": with_rows}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--root", action="store_true", help="P9.3 root duplicates")
    g.add_argument("--stubs", action="store_true", help="P9.4 header-only stubs")
    args = ap.parse_args(argv)
    if args.root:
        plan = preflight_root()
        bad = len(plan["lost"]) + len(plan["unsharded"])
        print(f"root files safe to delete: {len(plan['delete'])}; would lose content: {bad}")
    else:
        plan = preflight_stubs()
        bad = len(plan["with_rows"])
        print(f"stubs safe to delete: {len(plan['delete'])}; with data rows: {bad}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
