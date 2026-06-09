#!/usr/bin/env python3
"""
Move flat dictionary CSVs into sharded layout (no data loss).

  epa/{Make}/{Year}_{Model}_EPA.csv
  options/raw/{Make}/{Year}_{Model}_Complete_Options.csv
  options/stubs/{Make}/{Year}_{Model}_Complete_Options.csv

Also moves curated JSON + trim_spec_sheets when present at legacy paths.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_catalog import (  # noqa: E402
    canonical_make,
    iter_dictionary_csv_paths,
    options_status,
    parse_csv_filename,
)
from backend.enrichment.dictionary_paths import (  # noqa: E402
    CURATED_DIR,
    DERIVED_DIR,
    DICTIONARY_ROOT,
    EPA_DIR,
    LEGACY_TRIM_LADDERS,
    LEGACY_TRIM_LADDERS_EPA,
    LEGACY_TRIM_LADDERS_GENERATED,
    LEGACY_TRIM_LADDERS_MERGED,
    LEGACY_TRIM_SPEC_SHEETS,
    OPTIONS_RAW_DIR,
    OPTIONS_STUBS_DIR,
    TRIM_SPEC_SHEETS_DIR,
    BROCHURES_DIR,
)


def _shard_make_dir(base: Path, make: str) -> Path:
    token = canonical_make(make).replace(" ", "_").replace("/", "-")
    return base / token


def _move_file(src: Path, dest: Path, *, dry_run: bool) -> bool:
    if dest.resolve() == src.resolve():
        return False
    if dest.exists():
        return False
    if dry_run:
        print(f"  would move {src.name} -> {dest.relative_to(DICTIONARY_ROOT)}")
        return True
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))
    return True


def migrate_csvs(*, dry_run: bool) -> tuple[int, int]:
    moved = skipped = 0
    for kind in ("epa", "options"):
        for path in list(iter_dictionary_csv_paths(kind)):
            if path.parent != DICTIONARY_ROOT:
                continue
            meta = parse_csv_filename(path)
            if not meta:
                continue
            make = meta.get("make") or "Unknown"
            if kind == "epa":
                dest_dir = _shard_make_dir(EPA_DIR, make)
            elif options_status(path) == "stub":
                dest_dir = _shard_make_dir(OPTIONS_STUBS_DIR, make)
            else:
                dest_dir = _shard_make_dir(OPTIONS_RAW_DIR, make)
            dest = dest_dir / path.name
            if _move_file(path, dest, dry_run=dry_run):
                moved += 1
            else:
                skipped += 1
    return moved, skipped


def migrate_curated(*, dry_run: bool) -> int:
    moved = 0
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    for src in (
        LEGACY_TRIM_LADDERS,
        LEGACY_TRIM_LADDERS_GENERATED,
        LEGACY_TRIM_LADDERS_EPA,
        LEGACY_TRIM_LADDERS_MERGED,
    ):
        if not src.is_file():
            continue
        dest = CURATED_DIR / src.name
        if _move_file(src, dest, dry_run=dry_run):
            moved += 1
    return moved


def migrate_spec_sheets(*, dry_run: bool) -> int:
    if not LEGACY_TRIM_SPEC_SHEETS.is_dir():
        return 0
    if TRIM_SPEC_SHEETS_DIR.resolve() == LEGACY_TRIM_SPEC_SHEETS.resolve():
        return 0
    count = 0
    TRIM_SPEC_SHEETS_DIR.mkdir(parents=True, exist_ok=True)
    for src in LEGACY_TRIM_SPEC_SHEETS.glob("*.json"):
        dest = TRIM_SPEC_SHEETS_DIR / src.name
        if _move_file(src, dest, dry_run=dry_run):
            count += 1
    return count


def migrate_brochures(*, dry_run: bool) -> int:
    legacy = DICTIONARY_ROOT / "tmp_brochures"
    if not legacy.is_dir():
        return 0
    if dry_run:
        n = sum(1 for _ in legacy.rglob("*") if _.is_file())
        print(f"  would move tmp_brochures/ ({n} files) -> {BROCHURES_DIR.relative_to(_REPO)}")
        return n
    BROCHURES_DIR.mkdir(parents=True, exist_ok=True)
    moved = 0
    for src in legacy.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(legacy)
        dest = BROCHURES_DIR / rel
        if dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest))
        moved += 1
    try:
        legacy.rmdir()
    except OSError:
        pass
    return moved


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate dictionary to sharded layout.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-brochures", action="store_true")
    args = parser.parse_args()

    print("Migrating flat CSVs...")
    csv_moved, csv_skipped = migrate_csvs(dry_run=args.dry_run)
    print(f"  csv moved={csv_moved} skipped={csv_skipped}")

    curated = migrate_curated(dry_run=args.dry_run)
    print(f"  curated json moved={curated}")

    sheets = migrate_spec_sheets(dry_run=args.dry_run)
    print(f"  spec sheets moved={sheets}")

    if not args.skip_brochures:
        brochures = migrate_brochures(dry_run=args.dry_run)
        print(f"  brochures moved={brochures}")

    if not args.dry_run:
        DERIVED_DIR.mkdir(parents=True, exist_ok=True)
        print("Run: python -m backend.scripts.build_dictionary_manifest")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
