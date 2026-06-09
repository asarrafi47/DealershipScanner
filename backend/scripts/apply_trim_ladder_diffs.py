#!/usr/bin/env python3
"""
Compute trim-ladder ``adds`` from ``trim_spec_sheets/*.json`` and write into
``trim_ladders_generated.json``.

Run from repo root::

    python -m backend.scripts.apply_trim_ladder_diffs
    python -m backend.scripts.apply_trim_ladder_diffs --dry-run
    python -m backend.scripts.apply_trim_ladder_diffs --ladder-id 2025_toyota_highlander_complete_options
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.trim_diff_engine import (  # noqa: E402
    _LADDERS_GENERATED_JSON,
    _SHEETS_DIR,
    apply_trim_diffs_to_generated_ladders,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply trim spec sheet deltas to generated trim ladders.")
    parser.add_argument(
        "--generated",
        type=Path,
        default=_LADDERS_GENERATED_JSON,
        help="Path to trim_ladders_generated.json",
    )
    parser.add_argument(
        "--sheets-dir",
        type=Path,
        default=_SHEETS_DIR,
        help="Directory containing trim_spec_sheets/*.json",
    )
    parser.add_argument(
        "--ladder-id",
        action="append",
        dest="ladder_ids",
        help="Process only this ladder id (repeatable)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute stats without writing trim_ladders_generated.json",
    )
    args = parser.parse_args()

    ids = set(args.ladder_ids) if args.ladder_ids else None
    stats = apply_trim_diffs_to_generated_ladders(
        generated_path=args.generated,
        sheets_dir=args.sheets_dir,
        ladder_ids=ids,
        dry_run=args.dry_run,
    )

    mode = "dry-run" if args.dry_run else "wrote"
    print(
        f"{mode}: ladders_seen={stats['ladders_seen']} "
        f"sheets_found={stats['sheets_found']} "
        f"ladders_updated={stats['ladders_updated']} "
        f"steps_updated={stats['steps_updated']} "
        f"skipped_no_sheet={stats['skipped_no_sheet']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
