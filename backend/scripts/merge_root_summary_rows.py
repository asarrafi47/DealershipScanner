#!/usr/bin/env python3
"""Fold root-only Complete_Options rows into the sharded ``options/raw`` copies (P9.1).

``import_brochures_to_dictionary`` wrote a ``[Brochure Summary]`` row to the legacy flat
file ``backend/dictionary/<name>_Complete_Options.csv`` when a model had no matching trim
(97 files); the sharded copy under ``options/raw/<Make>/`` never got it. Before the root
copies are deleted (P9.3) every root row must exist, as a full tuple, in its sharded copy.

Default is a dry run that prints the manifest; ``--apply`` appends the missing rows to the
sharded file (header order kept, CRLF like the existing files). Rollback is ``git revert``.
Root files with no sharded copy are reported separately (header-only stubs are P9.4).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path

_DICT = Path(__file__).resolve().parents[1] / "dictionary"
SUFFIX = "_Complete_Options.csv"


def _read(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.reader(fh))


def _sharded_index(raw_dir: Path) -> dict[str, list[Path]]:
    index: dict[str, list[Path]] = {}
    for p in sorted(raw_dir.glob(f"*/*{SUFFIX}")):
        index.setdefault(p.name, []).append(p)
    return index


def plan_merge(root_dir: Path = _DICT, raw_dir: Path = _DICT / "options" / "raw") -> dict:
    """Return ``{"merges": [...], "no_shard": [...], "ambiguous": [...]}`` without writing."""
    index = _sharded_index(raw_dir)
    merges, no_shard, ambiguous = [], [], []
    for root in sorted(root_dir.glob(f"*{SUFFIX}")):
        targets = index.get(root.name, [])
        if not targets:
            no_shard.append(root.name)
            continue
        if len(targets) > 1:
            ambiguous.append(root.name)
            continue
        root_rows = _read(root)
        shard_rows = _read(targets[0])
        if not root_rows or not shard_rows:
            continue
        if root_rows[0] != shard_rows[0]:
            ambiguous.append(root.name)  # column order differs: needs a human
            continue
        have = {tuple(r) for r in shard_rows[1:]}
        missing = [r for r in root_rows[1:] if r and tuple(r) not in have]
        if missing:
            merges.append({"file": root.name, "target": str(targets[0]), "rows": missing})
    return {"merges": merges, "no_shard": no_shard, "ambiguous": ambiguous}


def apply_merge(plan: dict) -> int:
    n = 0
    for m in plan["merges"]:
        path = Path(m["target"])
        data = path.read_bytes()
        with path.open("ab") as fh:
            if data and not data.endswith(b"\n"):
                fh.write(b"\r\n")
            for row in m["rows"]:
                buf = io.StringIO(newline="")
                csv.writer(buf).writerow(row)  # default CRLF terminator
                fh.write(buf.getvalue().encode("utf-8"))
                n += 1
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true", help="append the missing rows (default: dry run)")
    ap.add_argument("--manifest", help="write the manifest JSON here")
    args = ap.parse_args(argv)
    plan = plan_merge()
    rows = sum(len(m["rows"]) for m in plan["merges"])
    print(f"files with root-only rows: {len(plan['merges'])}; rows: {rows}; "
          f"no sharded copy: {len(plan['no_shard'])}; ambiguous: {len(plan['ambiguous'])}")
    if args.manifest:
        Path(args.manifest).write_text(json.dumps(plan, indent=1))
    if plan["ambiguous"]:
        print("ambiguous (not merged):", *plan["ambiguous"], sep="\n  ")
    if args.apply:
        print(f"appended {apply_merge(plan)} rows")
    else:
        print("dry run; pass --apply to write")
    return 0


if __name__ == "__main__":
    sys.exit(main())
