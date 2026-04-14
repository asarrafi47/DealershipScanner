from __future__ import annotations

from pathlib import Path

from SCRAPING.paths import ROOT

OEM_DATA = ROOT / "data" / "oem"
BMW_DIR = OEM_DATA / "bmw"
BMW_DB_PATH = BMW_DIR / "bmw_intake.db"
BMW_RAW_DIR = BMW_DIR / "raw"


def ensure_bmw_dirs() -> None:
    BMW_DIR.mkdir(parents=True, exist_ok=True)
    BMW_RAW_DIR.mkdir(parents=True, exist_ok=True)
