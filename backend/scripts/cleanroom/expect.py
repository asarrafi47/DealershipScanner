"""Compare a tool output against a hand-built expected fixture.

Fixture shape::

    {"_meta": {...},
     "runs": {"<key>": {"args": {...}, "expected": {<nested subset of the output>}}}}

Every leaf under ``expected`` must equal the output value at the same path
(exact equality; lists compare element-wise). Keys the fixture does not name are
ignored, so the output may grow without breaking the baseline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_MISSING = object()


def leaves(tree: Any, prefix: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    if isinstance(tree, dict):
        out: list[tuple[tuple[str, ...], Any]] = []
        for k, v in tree.items():
            if str(k).startswith("_"):
                continue
            out.extend(leaves(v, prefix + (str(k),)))
        return out
    return [(prefix, tree)]


def lookup(tree: Any, path: tuple[str, ...]) -> Any:
    cur = tree
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return _MISSING
    return cur


def compare(output: dict[str, Any], expected: dict[str, Any]) -> list[dict[str, Any]]:
    bad = []
    for path, want in leaves(expected):
        got = lookup(output, path)
        if got is _MISSING or got != want:
            bad.append({"path": ".".join(path), "expected": want,
                        "actual": "<missing>" if got is _MISSING else got})
    return bad


def load_expected(fixture: Path, key: str) -> dict[str, Any]:
    data = json.loads(Path(fixture).read_text())
    runs = data.get("runs") or {}
    if key not in runs:
        raise KeyError(f"fixture {fixture} has no run {key!r} (has: {', '.join(sorted(runs))})")
    return runs[key]["expected"]
