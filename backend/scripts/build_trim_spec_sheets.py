#!/usr/bin/env python3
"""
Generate ``backend/dictionary/trim_spec_sheets/*.json`` from trim ladder definitions.

Run from repo root::

    python -m backend.scripts.build_trim_spec_sheets
    python -m backend.scripts.build_trim_spec_sheets --write-adds
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.trim_ladder import _LADDERS_GENERATED_JSON, _LADDERS_JSON, _read_ladders_file  # noqa: E402
from backend.enrichment.trim_spec_extractor import build_spec_sheet, extract_trim_specs  # noqa: E402
from backend.enrichment.trim_diff_engine import apply_trim_diffs_to_generated_ladders  # noqa: E402

from backend.enrichment.dictionary_paths import trim_spec_sheets_dir

_SHEETS_DIR = trim_spec_sheets_dir()
_SKIP_FILES = frozenset({"jeep_grand_cherokee_wk2.json"})


def _all_ladders() -> list[dict]:
    seen: set[str] = set()
    merged: list[dict] = []
    for path in (_LADDERS_JSON, _LADDERS_GENERATED_JSON):
        for ladder in _read_ladders_file(path):
            if not isinstance(ladder, dict):
                continue
            lid = str(ladder.get("id") or "").strip()
            if not lid or lid in seen:
                continue
            seen.add(lid)
            merged.append(ladder)
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description="Build trim spec sheets from ladder definitions.")
    parser.add_argument(
        "--write-adds",
        action="store_true",
        help="After building sheets, compute adds deltas into trim_ladders_generated.json",
    )
    args = parser.parse_args()

    _SHEETS_DIR.mkdir(parents=True, exist_ok=True)
    extract_trim_specs.cache_clear()

    written = 0
    skipped = 0
    empty = 0

    for ladder in _all_ladders():
        lid = str(ladder.get("id") or "").strip()
        if not lid:
            continue
        out_path = _SHEETS_DIR / f"{lid}.json"
        if out_path.name in _SKIP_FILES:
            skipped += 1
            continue

        sheet = build_spec_sheet(ladder)
        if not sheet:
            empty += 1
            continue

        out_path.write_text(json.dumps(sheet, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written += 1

    print(f"Wrote {written} trim spec sheets to {_SHEETS_DIR} ({skipped} hand-curated skipped, {empty} ladders had insufficient specs)")

    if args.write_adds:
        stats = apply_trim_diffs_to_generated_ladders(generated_path=_LADDERS_GENERATED_JSON, sheets_dir=_SHEETS_DIR)
        print(
            "Updated trim_ladders_generated.json: "
            f"ladders_updated={stats['ladders_updated']} steps_updated={stats['steps_updated']} "
            f"skipped_no_sheet={stats['skipped_no_sheet']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
