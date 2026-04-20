"""Filesystem paths for the multi-brand vehicle reference database."""
from __future__ import annotations

from pathlib import Path

from SCRAPING.paths import ROOT

_PKG_ROOT = Path(__file__).resolve().parent.parent

REF_DATA_DIR = ROOT / "data" / "vehicle_reference"
REF_DB_PATH = REF_DATA_DIR / "bmw_reference.db"
REF_SCHEMA_PATH = _PKG_ROOT / "schema" / "schema.sql"
REF_SEEDS_DIR = _PKG_ROOT / "seeds" / "bmw"
REF_SAMPLES_DIR = REF_DATA_DIR / "samples"


def ensure_ref_dirs() -> None:
    REF_DATA_DIR.mkdir(parents=True, exist_ok=True)
    REF_SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    REF_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
