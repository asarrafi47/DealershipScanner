#!/usr/bin/env python3
"""
Backfill missing or stub Complete_Options CSV years (2006+) from the nearest
model year that has real data. Prefers the next newer year, then older years.

Example: 2006 Toyota Camry empty → copy rows from 2007 (or 2008, …).

Writes to options/raw/{Make}/ and removes matching stub files when present.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

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
    CANONICAL_CSV_COLUMNS,
    DICTIONARY_ROOT,
    OPTIONS_RAW_DIR,
    OPTIONS_STUBS_DIR,
)

MIN_YEAR = 2006
MAX_YEAR = 2024
_BACKFILL_TAG = "[Backfilled from {source_year}]"


def _model_key(make: str, model: str) -> tuple[str, str]:
    return canonical_make(make).lower(), (model or "").strip().lower()


def _dest_path(year: int, make: str, model: str) -> Path:
    make_token = canonical_make(make).replace(" ", "_").replace("/", "-")
    model_token = (model or "").replace(" ", "_")
    out_dir = OPTIONS_RAW_DIR / make_token
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{year}_{make_token}_{model_token}_Complete_Options.csv"


def _stub_path(year: int, make: str, model: str) -> Path:
    make_token = canonical_make(make).replace(" ", "_").replace("/", "-")
    model_token = (model or "").replace(" ", "_")
    return OPTIONS_STUBS_DIR / make_token / f"{year}_{make_token}_{model_token}_Complete_Options.csv"


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="", errors="replace") as fh:
        return list(csv.DictReader(fh))


def _pick_best_path(paths: list[Path]) -> Path:
    """Prefer rich raw files over stubs/legacy duplicates."""
    ranked: list[tuple[int, Path]] = []
    for path in paths:
        status = options_status(path)
        score = {"rich": 0, "empty": 1, "stub": 2, "missing": 3}.get(status, 3)
        in_raw = 1 if "options/raw" in str(path) else 0
        ranked.append((score - in_raw, path))
    ranked.sort(key=lambda item: (item[0], item[1].name))
    return ranked[0][1]


def _index_options_files() -> dict[tuple[str, str], dict[str, Any]]:
    by_model: dict[tuple[str, str], dict[str, Any]] = {}
    for path in iter_dictionary_csv_paths("options"):
        meta = parse_csv_filename(path)
        if not meta or meta.get("kind") != "options":
            continue
        year = meta["year"]
        if year is None or year < MIN_YEAR:
            continue
        key = _model_key(meta["make"], meta["model"])
        bucket = by_model.setdefault(
            key,
            {"make": meta["make"], "model": meta["model"], "years": defaultdict(list)},
        )
        bucket["years"][year].append(path)
    return by_model


def _needs_backfill(paths: list[Path] | None) -> bool:
    if not paths:
        return True
    best = _pick_best_path(paths)
    return options_status(best) != "rich"


def _find_source_year(target_year: int, rich_years: set[int]) -> int | None:
    for delta in range(1, MAX_YEAR - MIN_YEAR + 2):
        forward = target_year + delta
        if forward in rich_years:
            return forward
        backward = target_year - delta
        if backward in rich_years:
            return backward
    return None


def _copy_rows(
    *,
    source_year: int,
    target_year: int,
    make: str,
    model: str,
    source_path: Path,
) -> list[dict[str, str]]:
    rows = _load_rows(source_path)
    out: list[dict[str, str]] = []
    tag = _BACKFILL_TAG.format(source_year=source_year)
    for row in rows:
        new_row = {col: (row.get(col) or "") for col in CANONICAL_CSV_COLUMNS}
        new_row["Year"] = str(target_year)
        new_row["Make"] = canonical_make(make)
        new_row["Model"] = model
        details = (new_row.get("optionDetails") or "").strip()
        if tag not in details:
            new_row["optionDetails"] = f"{details} | {tag}".strip(" |")
        out.append(new_row)
    return out


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def backfill(*, dry_run: bool = False) -> tuple[int, int, int]:
    indexed = _index_options_files()
    filled = skipped = no_source = 0

    for _key, bucket in sorted(indexed.items()):
        year_paths: dict[int, list[Path]] = bucket["years"]
        make = bucket["make"]
        model = bucket["model"]
        rich_years = {
            year
            for year, paths in year_paths.items()
            if options_status(_pick_best_path(paths)) == "rich"
        }
        if not rich_years:
            continue

        span_start = max(MIN_YEAR, min(year_paths.keys()))
        span_end = min(MAX_YEAR, max(rich_years | set(year_paths.keys())))
        for year in range(span_start, span_end + 1):
            paths = year_paths.get(year, [])
            if not _needs_backfill(paths if paths else None):
                skipped += 1
                continue

            source_year = _find_source_year(year, rich_years)
            if source_year is None:
                no_source += 1
                continue

            source_path = _pick_best_path(year_paths[source_year])
            rows = _copy_rows(
                source_year=source_year,
                target_year=year,
                make=make,
                model=model,
                source_path=source_path,
            )
            if not rows:
                no_source += 1
                continue

            dest = _dest_path(year, make, model)
            stub = _stub_path(year, make, model)
            if dry_run:
                print(f"would fill {year} {make} {model} from {source_year} ({len(rows)} rows)")
            else:
                _write_csv(dest, rows)
                if stub.is_file():
                    stub.unlink()
                # remove empty legacy flat duplicate if present
                legacy = DICTIONARY_ROOT / dest.name
                if legacy.is_file() and legacy.resolve() != dest.resolve():
                    if options_status(legacy) != "rich":
                        legacy.unlink()
            filled += 1

    return filled, skipped, no_source


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing CSVs")
    args = parser.parse_args()

    filled, skipped, no_source = backfill(dry_run=args.dry_run)
    print(f"filled={filled} already_ok={skipped} no_source={no_source}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
