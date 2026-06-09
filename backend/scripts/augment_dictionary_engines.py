#!/usr/bin/env python3
"""
Add ``engineDisplay`` and ``forcedInduction`` columns to every ``*_EPA.csv`` in DICTIONARY.

Usage:
  python backend/scripts/augment_dictionary_engines.py
  python backend/scripts/augment_dictionary_engines.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.dictionary.epa_engine import catalog_engine_fields  # noqa: E402

DICTIONARY = _REPO_ROOT / "backend" / "dictionary"
ENGINE_COLS = ("engineDisplay", "forcedInduction")


def augment_file(path: Path, *, dry_run: bool) -> tuple[int, int]:
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return 0, 0

    fieldnames = list(rows[0].keys())
    for col in ENGINE_COLS:
        if col not in fieldnames:
            fieldnames.append(col)

    changed = 0
    for row in rows:
        catalog = catalog_engine_fields(row)
        touched = False
        for col in ENGINE_COLS:
            val = catalog.get(col, "")
            if row.get(col) != val:
                row[col] = val
                touched = True
        if touched:
            changed += 1

    if not dry_run and changed:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    return len(rows), changed


def main() -> None:
    ap = argparse.ArgumentParser(description="Augment DICTIONARY EPA CSVs with engine catalog columns")
    ap.add_argument("--dry-run", action="store_true", help="Report only; do not write files")
    args = ap.parse_args()

    files = sorted(DICTIONARY.glob("*_EPA.csv"))
    total_rows = 0
    total_changed = 0
    updated_files = 0

    for path in files:
        n_rows, n_changed = augment_file(path, dry_run=args.dry_run)
        total_rows += n_rows
        total_changed += n_changed
        if n_changed:
            updated_files += 1

    mode = "would update" if args.dry_run else "updated"
    print(
        f"{mode} {updated_files} files | {total_changed}/{total_rows} rows gained engineDisplay/forcedInduction"
    )


if __name__ == "__main__":
    main()
