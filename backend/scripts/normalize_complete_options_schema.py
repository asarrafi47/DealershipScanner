#!/usr/bin/env python3
"""Normalize Complete_Options and EPA CSV columns to the canonical schema."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_catalog import (  # noqa: E402
    iter_dictionary_csv_paths,
    normalize_csv_columns,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize dictionary CSV column order.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--kind", choices=("epa", "options", "all"), default="all")
    args = parser.parse_args()

    kinds = ("epa", "options") if args.kind == "all" else (args.kind,)
    changed = 0
    scanned = 0
    for kind in kinds:
        for path in iter_dictionary_csv_paths(kind):
            scanned += 1
            if normalize_csv_columns(path, dry_run=args.dry_run):
                changed += 1
                if args.dry_run:
                    print(f"would normalize: {path}")
    print(f"scanned={scanned} changed={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
