#!/usr/bin/env python3
"""
Import EPA fueleconomy.gov vehicles.csv into ``backend/dictionary/``.

That directory is the canonical EPA-based reference for enrichment
(``backend/dictionary/enrich_from_dictionary.py``). Wikipedia / NHTSA ``*Complete_Options.csv`` output
from ``car_data_scraper.py`` lives under ``csv_out*`` and is a different dataset.

Downloads all make/model/trim/engine data for model years 2020+
and writes one CSV per Year+Make+Model, matching the DICTIONARY format.

Usage:
  python import_epa_to_dictionary.py
  python import_epa_to_dictionary.py --epa-csv /path/to/vehicles.csv
  python import_epa_to_dictionary.py --min-year 2000
"""
import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# Repo root → backend/dictionary (canonical EPA CSV dir; same folder ``enrich_from_dictionary.py`` uses).
_REPO_ROOT = Path(__file__).resolve().parents[2]
DICTIONARY = _REPO_ROOT / "backend" / "dictionary"
DEFAULT_EPA_CSV = Path("/tmp/vehicles.csv")

DICT_COLUMNS = [
    "Year", "Make", "Model", "Trim",
    "engineOptions", "transmissionOptions", "drivetrainOptions",
    "fuelType", "bodyStyle", "cylinders", "displacement",
    "mpg_city", "mpg_highway", "mpg_combined",
    "exteriorColors", "Packages", "packageDetails", "Options", "optionDetails",
]

DRIVE_MAP = {
    "Rear-Wheel Drive": "Rear-Wheel Drive",
    "Front-Wheel Drive": "Front-Wheel Drive",
    "All-Wheel Drive": "All-Wheel Drive",
    "4-Wheel Drive": "Four-Wheel Drive",
    "4-Wheel or All-Wheel Drive": "All-Wheel Drive",
    "Part-time 4-Wheel Drive": "Four-Wheel Drive",
    "2-Wheel Drive": "Front-Wheel Drive",
}


def normalize_drive(drive: str) -> str:
    return DRIVE_MAP.get(drive.strip(), drive.strip())


def build_engine_desc(row: dict) -> str:
    parts = []
    displ = row.get("displ", "").strip()
    cyl = row.get("cylinders", "").strip()
    eng = row.get("eng_dscr", "").strip()
    atv = row.get("atvType", "").strip()
    fuel = row.get("fuelType1", "").strip()

    if atv and atv.lower() not in ("", "null"):
        parts.append(atv)  # "EV", "Plug-in Hybrid", etc.

    if displ and displ not in ("0", "0.0", ""):
        parts.append(f"{displ}L")
    if cyl and cyl not in ("0", ""):
        parts.append(f"I{cyl}" if int(float(cyl)) <= 4 else f"V{cyl}")
    if eng:
        # strip noisy tokens
        eng = re.sub(r'\s+', ' ', eng).strip()
        parts.append(f"({eng})")
    if fuel and fuel not in ("Regular Gasoline", "Premium Gasoline"):
        parts.append(fuel)

    return " ".join(parts)


def safe_filename(s: str) -> str:
    return re.sub(r'[^\w\-. ]', '_', s).strip()


def process_epa_csv(epa_csv: Path, min_year: int, dictionary_dir: Path):
    print(f"Reading {epa_csv}...")
    rows_by_file: dict[str, list[dict]] = defaultdict(list)

    with open(epa_csv, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        total = 0
        kept = 0
        for row in reader:
            total += 1
            try:
                year = int(row.get("year", 0))
            except (ValueError, TypeError):
                continue
            if year < min_year:
                continue

            make = (row.get("make") or "").strip()
            model = (row.get("model") or "").strip()
            base_model = (row.get("baseModel") or model).strip()
            if not make or not model:
                continue

            transmission = (row.get("trany") or "").strip()
            drive = normalize_drive(row.get("drive") or "")
            fuel_type = (row.get("fuelType1") or "").strip()
            fuel2 = (row.get("fuelType2") or "").strip()
            if fuel2:
                fuel_type = f"{fuel_type} / {fuel2}"
            body_style = (row.get("VClass") or "").strip()
            cylinders = (row.get("cylinders") or "").strip()
            displ = (row.get("displ") or "").strip()

            mpg_city = (row.get("city08") or "").strip()
            mpg_hwy = (row.get("highway08") or "").strip()
            mpg_comb = (row.get("comb08") or "").strip()

            engine_desc = build_engine_desc(row)

            # Trim: use full model name minus base_model prefix (approximation)
            trim = model
            if base_model and model.startswith(base_model):
                trim = model[len(base_model):].strip()
            if not trim:
                trim = model

            dict_row = {
                "Year": year,
                "Make": make,
                "Model": base_model,
                "Trim": trim,
                "engineOptions": engine_desc,
                "transmissionOptions": transmission,
                "drivetrainOptions": drive,
                "fuelType": fuel_type,
                "bodyStyle": body_style,
                "cylinders": cylinders,
                "displacement": displ,
                "mpg_city": mpg_city,
                "mpg_highway": mpg_hwy,
                "mpg_combined": mpg_comb,
                "exteriorColors": "",
                "Packages": "",
                "packageDetails": "",
                "Options": "",
                "optionDetails": "",
            }

            file_key = f"{year}_{safe_filename(make)}_{safe_filename(base_model)}"
            rows_by_file[file_key].append(dict_row)
            kept += 1

    print(f"  Total rows: {total}, kept (year >= {min_year}): {kept}")
    print(f"  Unique year+make+model files: {len(rows_by_file)}")

    dictionary_dir.mkdir(exist_ok=True)
    written = 0
    for file_key, file_rows in sorted(rows_by_file.items()):
        fname = dictionary_dir / f"{file_key}_EPA.csv"
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DICT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(file_rows)
        written += 1

    print(f"  Wrote {written} EPA CSV files to {dictionary_dir}/")
    return written


def main():
    ap = argparse.ArgumentParser(description="Import EPA vehicles.csv into DICTIONARY/")
    ap.add_argument("--epa-csv", default=str(DEFAULT_EPA_CSV), help="Path to EPA vehicles.csv")
    ap.add_argument("--min-year", type=int, default=2020, help="Minimum model year (default 2020)")
    ap.add_argument("--dictionary", default=str(DICTIONARY), help="Output DICTIONARY directory")
    args = ap.parse_args()

    epa_csv = Path(args.epa_csv)
    if not epa_csv.exists():
        print(f"ERROR: EPA CSV not found at {epa_csv}")
        print("Download with: curl -L -o /tmp/vehicles.csv.zip https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip && unzip /tmp/vehicles.csv.zip -d /tmp/")
        sys.exit(1)

    written = process_epa_csv(epa_csv, args.min_year, Path(args.dictionary))
    print(f"\nDone. {written} EPA files added to DICTIONARY.")


if __name__ == "__main__":
    main()
