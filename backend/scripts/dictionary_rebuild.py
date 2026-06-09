#!/usr/bin/env python3
"""
Orchestrate dictionary rebuild: manifest, schema normalize, ladders, spec sheets.

Run from repo root::

    python -m backend.scripts.dictionary_rebuild
    python -m backend.scripts.dictionary_rebuild --skip-migrate --skip-spec-sheets
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]


def _run(module: str, *args: str) -> None:
    cmd = [sys.executable, "-m", module, *args]
    print(f"\n>> {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(_REPO), check=True)


def _clear_trim_ladder_caches() -> None:
    from backend.enrichment.trim_ladder import (
        _all_ladder_defs,
        _load_epa_ladders,
        _load_generated_ladders,
        _load_generated_ladders_raw,
        _load_json_ladders,
        _load_merged_ladders,
    )

    for fn in (
        _load_json_ladders,
        _load_generated_ladders_raw,
        _load_generated_ladders,
        _load_merged_ladders,
        _load_epa_ladders,
        _all_ladder_defs,
    ):
        fn.cache_clear()


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild dictionary derived artifacts.")
    parser.add_argument("--migrate", action="store_true", help="Run layout migration first.")
    parser.add_argument("--skip-migrate", action="store_true")
    parser.add_argument(
        "--skip-normalize",
        action="store_true",
        help="Skip CSV column normalize (fast re-run after first rebuild).",
    )
    parser.add_argument("--skip-ladders", action="store_true")
    parser.add_argument("--skip-brochure-derived", action="store_true")
    parser.add_argument("--skip-spec-sheets", action="store_true")
    parser.add_argument("--skip-manifest", action="store_true")
    parser.add_argument(
        "--delete-brochure-pdfs",
        action="store_true",
        help="After rebuild, delete PDFs when brochure_text exists for each.",
    )
    args = parser.parse_args()

    if args.migrate and not args.skip_migrate:
        _run("backend.scripts.migrate_dictionary_layout")

    if not args.skip_normalize:
        _run("backend.scripts.normalize_complete_options_schema")

    if not args.skip_brochure_derived:
        _run("backend.scripts.build_brochure_text_slim", "--overwrite")
        _run("backend.scripts.build_brochure_trim_candidates", "--overwrite")
        _run("backend.scripts.build_trim_ladders_from_brochure")

    if not args.skip_ladders:
        _run("backend.scripts.build_trim_ladders")
        _run("backend.scripts.build_trim_ladders_from_epa")
        _run("backend.scripts.merge_trim_ladders")
        _clear_trim_ladder_caches()

    if not args.skip_spec_sheets:
        _run("backend.scripts.build_trim_spec_sheets", "--write-adds")

    if not args.skip_brochure_derived:
        _run("backend.scripts.promote_trim_candidates", "--year-min", "2015")

    if not args.skip_manifest:
        _run("backend.scripts.build_dictionary_manifest")
        if not args.skip_brochure_derived:
            _run("backend.scripts.validate_trim_overlays")

    if args.delete_brochure_pdfs:
        _run("backend.scripts.delete_brochure_pdfs", "--confirm")

    print("\nDictionary rebuild complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
