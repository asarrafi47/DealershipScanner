#!/usr/bin/env python3
"""
Clean Wikipedia-scraped vehicle CSV files in csv_out_needs_cleaning/.

This pipeline produces ``*Complete_Options.csv`` files. It is separate from
``DICTIONARY/``, which holds EPA fueleconomy.gov exports (``*_EPA.csv``) used by
``import_epa_to_dictionary.py`` and ``enrich_from_dictionary.py``. Do not mix the
two directories without an explicit import step.

Problems identified:
  1. Empty files - just header + blank row
  2. Wikidata "not found" errors - Wikipedia returned error page
  3. Wrong Wikipedia article - scraper fetched non-car pages (city names, dealer names)
  4. Messy data - real specs mixed with Wikipedia citations, footnotes, prose

Output:
  csv_out_cleaned/   - cleaned CSVs with actual spec data
  csv_out_rejected/  - files that had no recoverable car spec data

Run: python clean_vehicle_csvs.py
"""
import csv
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse as _ap
_p = _ap.ArgumentParser(add_help=False)
_p.add_argument("--input", default="csv_out_needs_cleaning")
_p.add_argument("--cleaned", default="csv_out_cleaned")
_p.add_argument("--rejected", default="csv_out_rejected")
_args, _ = _p.parse_known_args()

INPUT_DIR = Path(_args.input)
CLEANED_DIR = Path(_args.cleaned)
REJECTED_DIR = Path(_args.rejected)

SPEC_COLUMNS = [
    "Year", "Make", "Model", "Trim",
    "engineOptions", "transmissionOptions", "drivetrainOptions",
    "exteriorColors", "Packages", "packageDetails", "Options", "optionDetails",
]

# Models that are clearly not real car models (dealer names, city names, etc.)
KNOWN_GARBAGE_MODELS = {
    "Carolina Trikes & Minis",
    "Miller",
    "MiniKamp",
    "Minitears Company",
    "Mobile Mini Inc.",
    "Pony",
    "R.V. Mini Mart, Inc.",
    "Santa Barbara",
    "MINI MONSOON",
    "Miami",           # Kia Miami → dealership name
    "Genesis",         # Genesis Genesis → disambiguation page
    "Genesis Supreme", # Genesis Genesis Supreme → wrong page
    "Black Jaguar",    # Jaguar Black Jaguar → wrong page
}

WIKIDATA_ERROR_STRINGS = [
    "Wikidata (linked database)",
    "Titles on Wikipedia are case sensitive",
    "If a page was recently created here",
]

CITATION_PATTERNS = [
    re.compile(r'(?:^|\s)\^\s'),                                      # literal ^ bullet (Wikipedia citation)
    re.compile(r'Archived.*from the original'),
    re.compile(r'Retrieved \d{1,2} \w+ \d{4}'),                      # Retrieved 19 January 2022
    re.compile(r'Retrieved \w+ \d{1,2},?\s+\d{4}'),                  # Retrieved January 19, 2022
    re.compile(r'Retrieved \d{4}-\d{2}-\d{2}'),                      # Retrieved 2025-07-11
    re.compile(r'https?://'),
    re.compile(r'Wayback Machine'),
    re.compile(r'(?:^|\s)a\s+b(\s+[a-z])+\s+["""]'),               # "a b c d" multi-citation marker
    re.compile(r'carsalesbase\.com'),
    re.compile(r'newsroom|press release', re.IGNORECASE),
]

FOOTNOTE_RE = re.compile(r'\[\d+\]|\[citation needed\]|\[edit\]|\[which\?\]|\[N \d+\]|\[note \d+\]', re.IGNORECASE)


def is_wikidata_error(rows: list[dict]) -> bool:
    for row in rows:
        for val in row.values():
            if any(err in (val or "") for err in WIKIDATA_ERROR_STRINGS):
                return True
    return False


def is_garbage_model(model: str) -> bool:
    return model.strip() in KNOWN_GARBAGE_MODELS


def _has_citation(text: str) -> bool:
    return any(p.search(text) for p in CITATION_PATTERNS)


def is_citation_row(row: dict) -> bool:
    """True if this row is a Wikipedia citation/reference line."""
    cells = [row.get(c) or "" for c in SPEC_COLUMNS]
    # Check each cell individually (so ^ anchor works) and also full joined text
    return any(_has_citation(c) for c in cells)


def has_any_spec_data(row: dict) -> bool:
    """True if any spec column (engine, transmission, drivetrain) has meaningful content."""
    spec_fields = ["engineOptions", "transmissionOptions", "drivetrainOptions",
                   "exteriorColors", "Trim"]
    for f in spec_fields:
        val = (row.get(f) or "").strip()
        if val and not any(err in val for err in WIKIDATA_ERROR_STRINGS):
            return True
    return False


def clean_cell(val: str) -> str:
    """Strip Wikipedia footnote markers and [edit] tags from a cell value."""
    if not val:
        return val
    val = FOOTNOTE_RE.sub("", val)
    val = val.strip()
    return val


def clean_row(row: dict) -> dict:
    return {k: clean_cell(v or "") for k, v in row.items()}


def is_prose_row(row: dict) -> bool:
    """True if row looks like Wikipedia article prose, not a spec."""
    trim = (row.get("Trim") or "").strip()
    # Long Wikipedia article sentences in the Trim column
    if len(trim) > 200:
        return True
    # Citation-style references
    if is_citation_row(row):
        return True
    return False


def is_all_empty_spec(row: dict) -> bool:
    """True if all spec columns are empty (Year/Make/Model may be present)."""
    spec_fields = ["engineOptions", "transmissionOptions", "drivetrainOptions",
                   "exteriorColors", "Packages", "packageDetails", "Options", "optionDetails", "Trim"]
    return all(not (row.get(f) or "").strip() for f in spec_fields)


def classify_and_clean(filepath: Path):
    """
    Returns (status, cleaned_rows, reason)
    status: 'cleaned' | 'rejected'
    """
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return "rejected", [], "empty file"

    # Check model name from filename or first row
    first = rows[0]
    model = (first.get("Model") or "").strip()
    make = (first.get("Make") or "").strip()

    if is_garbage_model(model):
        return "rejected", [], f"non-car model name: {make} {model}"

    if is_wikidata_error(rows):
        return "rejected", [], "Wikipedia page not found (Wikidata error)"

    # Check if completely empty (no spec data at all)
    if all(is_all_empty_spec(r) for r in rows):
        return "rejected", [], "no spec data found"

    # Clean: filter out citation rows and prose rows, clean footnotes
    cleaned = []
    for row in rows:
        if is_citation_row(row):
            continue
        if is_prose_row(row):
            continue
        if is_all_empty_spec(row) and not (row.get("Year") or row.get("Make") or row.get("Model")):
            continue
        cleaned.append(clean_row(row))

    if not cleaned:
        return "rejected", [], "no rows remained after cleaning"

    # Must have at least one row with real spec data
    if not any(has_any_spec_data(r) for r in cleaned):
        return "rejected", [], "no meaningful spec data after cleaning"

    return "cleaned", cleaned, "ok"


def write_csv(rows: list[dict], dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = SPEC_COLUMNS
    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    CLEANED_DIR.mkdir(exist_ok=True)
    REJECTED_DIR.mkdir(exist_ok=True)

    files = sorted(INPUT_DIR.glob("*.csv"))
    if not files:
        print(f"No CSV files found in {INPUT_DIR}/")
        sys.exit(1)

    stats = {"cleaned": 0, "rejected": 0}
    rejected_reasons: list[tuple[str, str]] = []

    for fp in files:
        status, rows, reason = classify_and_clean(fp)
        dest = (CLEANED_DIR if status == "cleaned" else REJECTED_DIR) / fp.name

        if status == "cleaned":
            write_csv(rows, dest)
            stats["cleaned"] += 1
            print(f"  CLEANED  {fp.name}  ({len(rows)} rows)")
        else:
            # Copy original to rejected dir for reference
            import shutil
            shutil.copy2(fp, dest)
            stats["rejected"] += 1
            rejected_reasons.append((fp.name, reason))
            print(f"  REJECTED {fp.name}  → {reason}")

    print()
    print("=" * 60)
    print(f"Total files:  {len(files)}")
    print(f"  Cleaned:    {stats['cleaned']}")
    print(f"  Rejected:   {stats['rejected']}")
    print()
    if rejected_reasons:
        print("Rejected files by reason:")
        by_reason: dict[str, list[str]] = {}
        for name, reason in rejected_reasons:
            by_reason.setdefault(reason, []).append(name)
        for reason, names in sorted(by_reason.items()):
            print(f"  [{reason}] ({len(names)} files)")
            for n in names:
                print(f"    - {n}")
    print(f"\nCleaned CSVs → {CLEANED_DIR}/")
    print(f"Rejected CSVs → {REJECTED_DIR}/")


if __name__ == "__main__":
    main()
