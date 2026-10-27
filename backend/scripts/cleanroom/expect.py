"""Compare a tool output against a hand-built expected fixture.

Fixture shape::

    {"_meta": {...},
     "runs": {"<key>": {"args": {...}, "expected": {<nested subset of the output>}}}}

Every leaf under ``expected`` must equal the output value at the same path
(exact equality; lists compare element-wise). Keys the fixture does not name are
ignored, so the output may grow without breaking the baseline.

A leaf may also be a bound, for gates that are limits rather than exact numbers
(N11.3 pass A: ``{"$lte": 2100}``). A dict whose keys all start with ``$`` is one leaf;
every operator in it must hold: ``$eq $ne $lt $lte $gt $gte``. A bound against a
missing or non-numeric output value is a mismatch.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_MISSING = object()

_OPS = {
    "$eq": lambda a, b: a == b,
    "$ne": lambda a, b: a != b,
    "$lt": lambda a, b: a < b,
    "$lte": lambda a, b: a <= b,
    "$gt": lambda a, b: a > b,
    "$gte": lambda a, b: a >= b,
}


def is_bound(v: Any) -> bool:
    return isinstance(v, dict) and bool(v) and all(str(k).startswith("$") for k in v)


def _holds(got: Any, want: Any) -> bool:
    if not is_bound(want):
        return got == want
    for op, limit in want.items():
        fn = _OPS.get(op)
        if fn is None:
            raise ValueError(f"unknown expect operator {op!r} (has: {', '.join(sorted(_OPS))})")
        if op in ("$eq", "$ne"):
            if not fn(got, limit):
                return False
            continue
        if isinstance(got, bool) or not isinstance(got, (int, float)):
            return False
        if not fn(got, limit):
            return False
    return True


def leaves(tree: Any, prefix: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    if isinstance(tree, dict) and not is_bound(tree):
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
        if got is _MISSING or not _holds(got, want):
            bad.append({"path": ".".join(path), "expected": want,
                        "actual": "<missing>" if got is _MISSING else got})
    return bad


def load_expected(fixture: Path, key: str) -> dict[str, Any]:
    data = json.loads(Path(fixture).read_text())
    runs = data.get("runs") or {}
    if key not in runs:
        raise KeyError(f"fixture {fixture} has no run {key!r} (has: {', '.join(sorted(runs))})")
    return runs[key]["expected"]
