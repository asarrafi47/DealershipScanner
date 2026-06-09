"""Canonical paths for the vehicle dictionary tree (flat legacy + sharded layout)."""

from __future__ import annotations

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
DICTIONARY_ROOT = Path(
    os.environ.get("DICTIONARY_ROOT", str(_REPO_ROOT / "backend" / "dictionary"))
).resolve()

INDEX_DIR = DICTIONARY_ROOT / "index"
EPA_DIR = DICTIONARY_ROOT / "epa"
OPTIONS_RAW_DIR = DICTIONARY_ROOT / "options" / "raw"
OPTIONS_STUBS_DIR = DICTIONARY_ROOT / "options" / "stubs"
CURATED_DIR = DICTIONARY_ROOT / "curated"
DERIVED_DIR = DICTIONARY_ROOT / "derived"
TRIM_SPEC_SHEETS_DIR = DERIVED_DIR / "trim_spec_sheets"
BROCHURE_FACTS_DIR = DERIVED_DIR / "brochure_facts"
BROCHURE_TEXT_DIR = DERIVED_DIR / "brochure_text"
BROCHURE_TEXT_SLIM_DIR = DERIVED_DIR / "brochure_text_slim"
BROCHURE_TRIM_CANDIDATES_DIR = DERIVED_DIR / "trim_candidates"
TRIM_ADDS_BY_YEAR_DIR = DERIVED_DIR / "trim_adds_by_year"
TRIM_OVERLAY_VALIDATION_PATH = DERIVED_DIR / "trim_overlay_validation.jsonl"

MANIFEST_PATH = INDEX_DIR / "manifest.json"
MAKE_ALIASES_PATH = INDEX_DIR / "make_aliases.json"
CATALOG_DB_PATH = Path(
    os.environ.get(
        "DICTIONARY_CATALOG_DB_PATH",
        str(INDEX_DIR / "dictionary_catalog.db"),
    )
).resolve()

BROCHURES_DIR = Path(
    os.environ.get(
        "DICTIONARY_BROCHURES_DIR",
        str(_REPO_ROOT / "backend" / "data" / "brochures"),
    )
).resolve()

# Legacy flat filenames at dictionary root (pre-migration).
LEGACY_TRIM_LADDERS = DICTIONARY_ROOT / "trim_ladders.json"
LEGACY_TRIM_LADDERS_GENERATED = DICTIONARY_ROOT / "trim_ladders_generated.json"
LEGACY_TRIM_LADDERS_EPA = DICTIONARY_ROOT / "trim_ladders_epa.json"
LEGACY_TRIM_LADDERS_MERGED = DICTIONARY_ROOT / "trim_ladders_merged.json"
LEGACY_TRIM_SPEC_SHEETS = DICTIONARY_ROOT / "trim_spec_sheets"

CANONICAL_CSV_COLUMNS = (
    "Year",
    "Make",
    "Model",
    "Trim",
    "engineOptions",
    "engineDisplay",
    "forcedInduction",
    "transmissionOptions",
    "drivetrainOptions",
    "fuelType",
    "bodyStyle",
    "cylinders",
    "displacement",
    "mpg_city",
    "mpg_highway",
    "mpg_combined",
    "exteriorColors",
    "Packages",
    "packageDetails",
    "Options",
    "optionDetails",
)


def trim_ladders_curated_path() -> Path:
    p = CURATED_DIR / "trim_ladders.json"
    return p if p.is_file() else LEGACY_TRIM_LADDERS


def trim_ladders_generated_path() -> Path:
    p = CURATED_DIR / "trim_ladders_generated.json"
    return p if p.is_file() else LEGACY_TRIM_LADDERS_GENERATED


def trim_ladders_epa_path() -> Path:
    p = CURATED_DIR / "trim_ladders_epa.json"
    return p if p.is_file() else LEGACY_TRIM_LADDERS_EPA


def trim_ladders_merged_path() -> Path:
    for p in (
        CURATED_DIR / "trim_ladders_merged.json",
        LEGACY_TRIM_LADDERS_MERGED,
    ):
        if p.is_file():
            return p
    return CURATED_DIR / "trim_ladders_merged.json"


def trim_ladders_brochure_path() -> Path:
    p = CURATED_DIR / "trim_ladders_brochure.json"
    return p if p.is_file() else CURATED_DIR / "trim_ladders_brochure.json"


def trim_spec_sheets_dir() -> Path:
    if TRIM_SPEC_SHEETS_DIR.is_dir():
        return TRIM_SPEC_SHEETS_DIR
    if LEGACY_TRIM_SPEC_SHEETS.is_dir():
        return LEGACY_TRIM_SPEC_SHEETS
    return TRIM_SPEC_SHEETS_DIR


def iter_search_roots(*, kind: str) -> list[Path]:
    """Directory roots to scan for CSV files (sharded first, then legacy flat)."""
    roots: list[Path] = []
    if kind == "epa":
        if EPA_DIR.is_dir():
            roots.append(EPA_DIR)
    elif kind == "options":
        if OPTIONS_RAW_DIR.is_dir():
            roots.append(OPTIONS_RAW_DIR)
        if OPTIONS_STUBS_DIR.is_dir():
            roots.append(OPTIONS_STUBS_DIR)
    roots.append(DICTIONARY_ROOT)
    return roots
