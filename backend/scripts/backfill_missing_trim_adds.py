#!/usr/bin/env python3
"""Backfill missing trim adds in trim_adds_by_year overlays."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.brochure_overlay_backfill import (  # noqa: E402
    backfill_overlay_missing_adds,
    missing_trim_names,
)
from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report only; do not write files")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing empty-only entries")
    args = parser.parse_args()

    stats = {"files": 0, "filled_files": 0, "filled_trims": 0, "still_missing": 0}
    still_missing_examples: list[str] = []

    for path in sorted(TRIM_ADDS_BY_YEAR_DIR.glob("*.json")):
        try:
            overlay = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(overlay, dict):
            continue
        before = missing_trim_names(overlay)
        if not before:
            continue
        stats["files"] += 1
        enriched, filled = backfill_overlay_missing_adds(overlay, overwrite=args.overwrite)
        after = missing_trim_names(enriched)
        if filled:
            stats["filled_files"] += 1
            stats["filled_trims"] += len(filled)
            if not args.dry_run:
                path.write_text(json.dumps(enriched, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        stats["still_missing"] += len(after)
        if after and len(still_missing_examples) < 25:
            still_missing_examples.append(f"{path.name}: {', '.join(after[:4])}")

    print(f"overlays with gaps: {stats['files']}")
    print(f"files updated: {stats['filled_files']} ({'dry-run' if args.dry_run else 'written'})")
    print(f"trims filled: {stats['filled_trims']}")
    print(f"trims still missing: {stats['still_missing']}")
    if still_missing_examples:
        print("examples still missing:")
        for line in still_missing_examples:
            print(f"  {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
