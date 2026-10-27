#!/usr/bin/env python3
"""P9.2b: 20-car before/after ``resolve_trim_ladder`` diff for the owner to sign (D-HY9).

BEFORE is the tree at ``--before-ref`` (default: the commit before the P9.2
options index), extracted with ``git archive``; AFTER is this working tree.
Both run in a subprocess with no SQLite catalog (``DICTIONARY_CATALOG_DB_PATH``
points nowhere), which is what prod web has. Sample: 10 YMMs whose options file
exists only under ``options/raw`` and 10 that also have a root duplicate; evenly
strided over the sorted candidates, and YMMs whose ladder resolves on at least
one side come first (the diff is about what customers see). Read-only: no DB, no
network. Output: ``workspace/phase_runs/N20/p9_2_before_after.{json,md}``.

    python -m backend.scripts.dictionary_options_before_after
    python -m backend.scripts.dictionary_options_before_after --before-ref <sha> --out-dir <dir>
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

DEFAULT_BEFORE_REF = "27ad5f918^"  # parent of "N20 P9.2: in-memory options index"
DEFAULT_OUT_DIR = _REPO / "workspace" / "phase_runs" / "N20"
_MIN_YEAR = 2015

# Runs inside each tree (cwd + PYTHONPATH = that tree): reads the sample from
# stdin, prints one JSON list on stdout. Only public/stable names are used so
# it works on both sides of the P9.2 change.
_EMIT = r"""
import json, sys
from backend.enrichment.dictionary_catalog import find_complete_options_csv
from backend.enrichment.trim_ladder.selection import resolve_trim_ladder

out = []
for car in json.load(sys.stdin):
    mk, md, yr, tr = car["make"], car["model"], car["year"], car["trim"]
    rec = {"id": car["id"]}
    try:
        p = find_complete_options_csv(mk, md, yr)
        rec["options_csv"] = None if p is None else str(p).split("backend/dictionary/", 1)[-1]
        r = resolve_trim_ladder(make=mk, model=md, year=yr, trim=tr)
        if r:
            rec["ladder_id"] = r.get("id")
            rec["source"] = r.get("source")
            rec["steps"] = [
                {"name": s.get("name"), "adds": len(s.get("adds") or []),
                 "sticker_adds": len(s.get("sticker_adds") or [])}
                for s in (r.get("steps") or [])
            ]
        else:
            rec["steps"] = None
    except Exception as exc:  # a crash is a finding, not a script failure
        rec["error"] = f"{type(exc).__name__}: {exc}"
    out.append(rec)
json.dump(out, sys.stdout)
"""

_NAME_RE = re.compile(r"^(\d{4})_(.+)_Complete_Options\.csv$")


def _files(dictionary: Path) -> tuple[dict[str, Path], set[str]]:
    raw: dict[str, Path] = {}
    for p in (dictionary / "options" / "raw").glob("*/*_Complete_Options.csv"):
        if not p.name.endswith("_DT_Complete_Options.csv") and p.stat().st_size > 300:
            raw[p.name] = p
    root = {p.name for p in dictionary.glob("*_Complete_Options.csv")}
    return raw, root


def _split_make_model(path: Path, name: str) -> tuple[int, str, str] | None:
    """Year/make/model of a raw file: the make is its shard directory."""
    m = _NAME_RE.match(name)
    if not m:
        return None
    make = path.parent.name
    stem = m.group(2)
    prefix = make.replace(" ", "_")
    if not stem.lower().startswith(prefix.lower() + "_"):
        return None
    return int(m.group(1)), make, stem[len(prefix) + 1 :].replace("_", " ")


def _strided(items: list, n: int) -> list:
    if len(items) <= n:
        return items
    return [items[(i * len(items)) // n] for i in range(n)]


def candidate_groups(dictionary: Path) -> tuple[list[dict], list[dict]]:
    """(raw_only, with_root_duplicate) candidate cars, sorted, year >= 2015."""
    raw, root = _files(dictionary)
    only: list[dict] = []
    dup: list[dict] = []
    for name in sorted(raw):
        ymm = _split_make_model(raw[name], name)
        if not ymm or ymm[0] < _MIN_YEAR:
            continue
        year, make, model = ymm
        car = {"id": f"{year} {make} {model}", "year": year, "make": make, "model": model, "trim": None}
        (dup if name in root else only).append(car)
    return only, dup


def _run_tree(tree: Path, cars: list[dict], inventory_url: str | None = None) -> list[dict]:
    env = dict(os.environ)
    if inventory_url:
        env["INVENTORY_DATABASE_URL"] = inventory_url
    env.update(
        PYTHONPATH=str(tree),
        DICTIONARY_CATALOG_DB_PATH=str(tree / "no_such_catalog.db"),
        TESTS_BLOCK_NETWORK="1",
    )
    env.pop("DICTIONARY_ROOT", None)
    proc = subprocess.run(
        [sys.executable, "-c", _EMIT],
        input=json.dumps(cars),
        capture_output=True,
        text=True,
        cwd=str(tree),
        env=env,
        timeout=140,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"emit failed in {tree}: {proc.stderr[-800:]}")
    return json.loads(proc.stdout)


def _extract_ref(ref: str, dest: Path) -> None:
    blob = subprocess.run(
        ["git", "-C", str(_REPO), "archive", ref, "backend"],
        capture_output=True,
        check=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
        tf.extractall(dest, filter="data")


def _pick(cars: list[dict], after: dict[str, dict], before: dict[str, dict], n: int) -> list[dict]:
    live = [c for c in cars if (after[c["id"]].get("steps") or before[c["id"]].get("steps"))]
    rest = [c for c in cars if c not in live]
    chosen = _strided(live, n)
    if len(chosen) < n:
        chosen += _strided([c for c in rest], n - len(chosen))
    return chosen


def _names(rec: dict) -> list[str] | None:
    return None if rec.get("steps") is None else [s["name"] for s in rec["steps"]]


def compare(b: dict, a: dict) -> str:
    if b.get("error") or a.get("error"):
        return "error"
    if b.get("options_csv") == a.get("options_csv") and b.get("steps") == a.get("steps"):
        return "same"
    if _names(b) == _names(a):
        return "same-rungs-different-adds" if b.get("steps") != a.get("steps") else "same-rungs-different-file"
    return "rungs-changed"


def render_md(rows: list[dict], ref: str) -> str:
    lines = [
        "# P9.2 before/after resolve_trim_ladder (20 cars)",
        "",
        f"Before: `{ref}` (no catalog DB, as on prod web). After: this tree. Owner signs per D-HY9.",
        "",
        "| # | group | car | verdict | before file | after file | before rungs | after rungs |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(rows, 1):
        fmt = lambda x: "none" if x is None else ", ".join(x)  # noqa: E731
        lines.append(
            f"| {i} | {r['group']} | {r['car']} | {r['verdict']} | {r['before'].get('options_csv') or 'none'} "
            f"| {r['after'].get('options_csv') or 'none'} | {fmt(_names(r['before']))} | {fmt(_names(r['after']))} |"
        )
    lines += ["", "Signed by: ______  Date: ______", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--before-ref", default=DEFAULT_BEFORE_REF)
    ap.add_argument("--per-group", type=int, default=10)
    ap.add_argument(
        "--inventory-url",
        default=None,
        help="Postgres URL the ladder reads rung evidence from (SELECT only). Without "
        "it (and with INVENTORY_DATABASE_URL blank) resolve_trim_ladder answers None for "
        "most cars and the diff shows file changes only.",
    )
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args(argv)

    only, dup = candidate_groups(_REPO / "backend" / "dictionary")
    pool = {c["id"]: c for c in only + dup}
    # Over-sample so the picker can prefer YMMs whose ladder resolves.
    over = args.per_group * 6
    probe = _strided(only, over) + _strided(dup, over)
    with tempfile.TemporaryDirectory() as td:
        _extract_ref(args.before_ref, Path(td))
        before = {r["id"]: r for r in _run_tree(Path(td), probe, args.inventory_url)}
    after = {r["id"]: r for r in _run_tree(_REPO, probe, args.inventory_url)}
    rows = []
    for group, cars in (("raw-only", only), ("root-duplicate", dup)):
        for c in _pick([p for p in probe if p["id"] in {x["id"] for x in cars}], after, before, args.per_group):
            rows.append(
                {
                    "group": group,
                    "car": c["id"],
                    "verdict": compare(before[c["id"]], after[c["id"]]),
                    "before": before[c["id"]],
                    "after": after[c["id"]],
                }
            )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "p9_2_before_after.json").write_text(json.dumps(rows, indent=1) + "\n")
    (args.out_dir / "p9_2_before_after.md").write_text(render_md(rows, args.before_ref))
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print(f"{len(rows)} cars -> {args.out_dir}: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
