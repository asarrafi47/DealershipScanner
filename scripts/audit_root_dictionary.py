#!/usr/bin/env python3
"""Content audit for the root ``DICTIONARY/`` folder before it is ``git rm``'d (P10D.8).

Read-only. For each CSV under the legacy root folder it finds the same-named file under
``backend/dictionary/`` (epa/, options/raw/, options/stubs/, or a flat legacy copy) and
compares the data rows (header order and blank rows ignored):

  identical            same row set
  covered              every root row is in the counterpart (counterpart is richer or equal)
  root_unique_rows     the root file holds rows the counterpart lacks (reported with counts)
  no_counterpart       no same-named file under backend/dictionary/

``root_unique_rows`` and ``no_counterpart`` are the files an owner must look at before
deletion ("0 unique content lost"). Nothing is written except the optional ``--report``.

Usage:
  python scripts/audit_root_dictionary.py
  python scripts/audit_root_dictionary.py --report workspace/phase_runs/N25/dictionary_audit.json
  python scripts/audit_root_dictionary.py --strict     # exit 1 when anything is unique
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = REPO_ROOT / "DICTIONARY"
DEFAULT_TARGET = REPO_ROOT / "backend" / "dictionary"


def _norm(value: str | None) -> str:
    return " ".join((value or "").split())


def read_rows(path: Path, columns: frozenset[str] | None = None):
    """(header, rows): rows as hashable (column, value) tuples, restricted to ``columns``
    when given (the catalog copies carry extra columns the legacy files never had)."""
    rows = set()
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as fh:
        reader = csv.DictReader(fh)
        header = frozenset(_norm(c) for c in (reader.fieldnames or []))
        for rec in reader:
            items = tuple(
                sorted(
                    (_norm(k), _norm(v))
                    for k, v in rec.items()
                    if k is not None and (columns is None or _norm(k) in columns)
                )
            )
            if any(v for _, v in items):
                rows.add(items)
    return header, frozenset(rows)


def index_target(target: Path) -> dict[str, list[Path]]:
    """Every CSV under the target tree by lower-cased basename."""
    out: dict[str, list[Path]] = {}
    for p in target.rglob("*.csv"):
        out.setdefault(p.name.lower(), []).append(p)
    return out


def classify(root_file: Path, candidates: list[Path]) -> dict:
    if not candidates:
        return {"file": root_file.name, "status": "no_counterpart"}
    root_header, _ = read_rows(root_file)
    best = None
    for cand in candidates:
        cand_header, _ = read_rows(cand)
        shared = root_header & cand_header
        _, root_rows = read_rows(root_file, shared)
        _, cand_rows = read_rows(cand, shared)
        missing = root_rows - cand_rows
        if best is None or len(missing) < best[1]:
            best = (cand, len(missing), cand_rows, root_rows)
    cand, n_missing, cand_rows, root_rows = best
    if n_missing:
        status = "root_unique_rows"
    elif root_rows == cand_rows:
        status = "identical"
    else:
        status = "covered"
    return {
        "file": root_file.name,
        "status": status,
        "counterpart": str(cand.relative_to(REPO_ROOT)) if cand.is_relative_to(REPO_ROOT) else str(cand),
        "root_rows": len(root_rows),
        "counterpart_rows": len(cand_rows),
        "root_unique": n_missing,
    }


def audit(root: Path, target: Path) -> dict:
    index = index_target(target)
    results = [classify(p, index.get(p.name.lower(), [])) for p in sorted(root.rglob("*.csv"))]
    counts = Counter(r["status"] for r in results)
    return {
        "root": str(root),
        "target": str(target),
        "files": len(results),
        "counts": dict(counts),
        "needs_review": [r for r in results if r["status"] in ("root_unique_rows", "no_counterpart")],
        "other_files": sorted(
            str(p.relative_to(root)) for p in root.rglob("*") if p.is_file() and p.suffix.lower() != ".csv"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    ap.add_argument("--target", type=Path, default=DEFAULT_TARGET)
    ap.add_argument("--report", type=Path, help="write the full JSON report here")
    ap.add_argument("--strict", action="store_true", help="exit 1 when any file needs review")
    args = ap.parse_args(argv)
    if not args.root.is_dir():
        print(f"{args.root} does not exist: nothing to audit")
        return 0
    report = audit(args.root, args.target)
    print(f"{report['files']} CSVs under {args.root}")
    for status, n in sorted(report["counts"].items()):
        print(f"  {status:18} {n}")
    for r in report["needs_review"][:50]:
        print(f"  REVIEW {r['status']}: {r['file']} (unique rows: {r.get('root_unique', 'n/a')})")
    if len(report["needs_review"]) > 50:
        print(f"  ... {len(report['needs_review']) - 50} more in the report")
    if report["other_files"]:
        print(f"  non-CSV files: {len(report['other_files'])}")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=1))
    return 1 if args.strict and report["needs_review"] else 0


if __name__ == "__main__":
    sys.exit(main())
