#!/usr/bin/env python3
"""
Fill sparse Complete_Options CSVs (2006+) using EPA rows plus curated trim/options
from manufacturer specs, Edmunds, KBB, and fueleconomy.gov.

Usage:
  python -m backend.scripts.fill_options_from_research
  python -m backend.scripts.fill_options_from_research --dry-run
  python -m backend.scripts.fill_options_from_research --year 2013 --make tesla --model "Model S"
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_catalog import (  # noqa: E402
    canonical_make,
    find_complete_options_csv,
    iter_dictionary_csv_paths,
    options_status,
    parse_csv_filename,
)
from backend.enrichment.dictionary_paths import (  # noqa: E402
    CANONICAL_CSV_COLUMNS,
    DICTIONARY_ROOT,
    EPA_DIR,
    OPTIONS_RAW_DIR,
)

MIN_YEAR = 2006
_SOURCE_TAG = "[Research]"

def _research_key(year: int, make: str, model: str) -> tuple[int, str, str]:
    return year, _norm(make), _norm(model)


# Curated packages/options keyed by (year, make_norm, model_norm) -> trim -> fields
_RESEARCH: dict[tuple[int, str, str], dict[str, dict[str, str]]] = {
    (2013, "tesla", "models"): {
        "60 kWh": {
            "Packages": "Supercharger Enabled; High Power Home Charging; Tech Package",
            "packageDetails": "Supercharger Enabled: DC fast-charging network access. High Power Home Charging: dual onboard chargers + HPWC. Tech Package: LED foglights, auto-dimming mirrors, power liftgate, keyless entry, navigation.",
            "Options": "21-inch wheels; Adaptive air suspension; Panoramic sunroof; Third-row seats; 12-speaker audio; Leather upholstery",
            "optionDetails": f"{_SOURCE_TAG} 302 hp rear motor, 208-mile EPA range. Optional 21-inch performance tires, parking sensors, sport seats, heated rear seats, ambient LED lighting.",
        },
        "85 kWh": {
            "Packages": "Tech Package; Supercharger Enabled (standard)",
            "packageDetails": "Tech Package: LED foglights, cornering lights, auto-dimming mirrors, power liftgate, driver memory, turn-by-turn navigation.",
            "Options": "21-inch wheels; Adaptive air suspension; Panoramic sunroof; Third-row seats; 12-speaker surround audio; Extended Nappa leather",
            "optionDetails": f"{_SOURCE_TAG} 362 hp rear motor, 265-mile EPA range. Standard Supercharger access and upgraded interior on 85 kWh models.",
        },
        "P85 Performance": {
            "Packages": "Tech Package; Performance Upgrade",
            "packageDetails": "Performance trim: 416 hp, 0-60 in 4.4 sec, top speed 130 mph. Includes performance drive inverter and tuning.",
            "Options": "21-inch gray performance wheels; Carbon fiber spoiler; Red brake calipers; Smart air suspension",
            "optionDetails": f"{_SOURCE_TAG} Top 2013 Model S trim. 85 kWh performance battery, 265-mile range, 88/90 MPGe.",
        },
        "Signature Performance": {
            "Packages": "Launch Signature Series",
            "packageDetails": "Limited early-production Signature Performance with 85 kWh pack and performance hardware.",
            "Options": "Performance wheels; Premium interior; Tech Package",
            "optionDetails": f"{_SOURCE_TAG} Limited Signature Performance sedan; 416 hp, 265-mile range.",
        },
        "(60 kW-hr battery pack)": {
            "Packages": "Supercharger Enabled; High Power Home Charging; Tech Package",
            "packageDetails": "EPA 60 kWh pack: 94/97 MPGe, ~208-mile range.",
            "Options": "21-inch wheels; Adaptive air suspension; Panoramic sunroof; Tech Package",
            "optionDetails": f"{_SOURCE_TAG} Matches EPA 60 kWh configuration.",
        },
        "(85 kW-hr battery pack)": {
            "Packages": "Tech Package; Supercharger Enabled",
            "packageDetails": "EPA 85 kWh pack: 88/90 MPGe, 265-mile range.",
            "Options": "21-inch wheels; Smart air suspension; 12-speaker audio; Third-row seats",
            "optionDetails": f"{_SOURCE_TAG} Matches EPA 85 kWh configuration.",
        },
    },
    (2022, "hyundai", "ioniq"): {
        "Blue": {
            "Packages": "Hybrid Efficiency Package",
            "packageDetails": "Most efficient hybrid trim: 58/60/59 MPG (city/hwy/combined).",
            "Options": "8-inch touchscreen; Apple CarPlay; Android Auto; Heated front seats; LED daytime running lights",
            "optionDetails": f"{_SOURCE_TAG} 1.6L I4 hybrid, 6-speed DCT, FWD, 139 hp combined.",
        },
        "SE": {
            "Packages": "Hybrid SE; Plug-in Hybrid SE",
            "packageDetails": "Hybrid SE: 54/57/55 MPG. PHEV SE: 29-mile EV range, 52 combined MPG, 8.9 kWh battery.",
            "Options": "8-inch touchscreen; 6-speaker audio; Bluetooth; USB; Heated front seats; LED exterior lighting",
            "optionDetails": f"{_SOURCE_TAG} Base trim. Hybrid or PHEV powertrain with 1.6L engine + electric motor, 6-speed dual-clutch automatic.",
        },
        "SEL": {
            "Packages": "Convenience & Safety Package",
            "packageDetails": "Adds 10-way power driver seat, auto-dimming mirror, blind-spot warning, rear cross-traffic alert, wireless charging, 7-inch LCD cluster.",
            "Options": "10-way power driver seat; Blind-spot warning; Rear cross-traffic alert; Wireless device charging",
            "optionDetails": f"{_SOURCE_TAG} Mid trim. PHEV: 29-mile electric range, Harman Kardon available on Limited only.",
        },
        "Limited": {
            "Packages": "Premium & Technology Package",
            "packageDetails": "Power sunroof, leather seating, driver memory, 10.25-inch touchscreen, navigation, Harman Kardon 8-speaker audio with subwoofer.",
            "Options": "Harman Kardon premium audio; 10.25-inch navigation display; Leather seats; Power-folding mirrors with puddle lights; Sunroof",
            "optionDetails": f"{_SOURCE_TAG} Top Ioniq trim (Hybrid or PHEV). PHEV Limited includes Harman Kardon and 29-mile EV range.",
        },
        "Ioniq": {
            "Packages": "Hybrid",
            "packageDetails": "Standard hybrid: 1.6L I4 with electric motor, 6-speed automatic, FWD.",
            "Options": "8-inch touchscreen; Apple CarPlay; Android Auto",
            "optionDetails": f"{_SOURCE_TAG} EPA hybrid row.",
        },
        "Plug-in Hybrid": {
            "Packages": "Plug-in Hybrid",
            "packageDetails": "1.6L PHEV: 45 kW motor, 8.9 kWh battery, 29-mile electric range, 52 combined MPG.",
            "Options": "SE / SEL / Limited trim equipment",
            "optionDetails": f"{_SOURCE_TAG} EPA PHEV row; see SE/SEL/Limited for feature breakdown.",
        },
    },
    (2018, "bmw", "6series"): {
        "640i Gran Coupe": {
            "Packages": "M Sport; Executive; Cold Weather; Driving Assistance",
            "packageDetails": "315 hp 3.0L turbo I6, 8-speed automatic, RWD. M Sport: sport body kit, sport steering wheel, 19-inch wheels. Executive: ventilated seats. Cold Weather: heated steering wheel.",
            "Options": "Harman Kardon audio; Nappa leather; Adaptive cruise (Driving Assistance Plus); Head-up display; Parking Assistant",
            "optionDetails": f"{_SOURCE_TAG} Gran Coupe 4-door. 20/29/23 MPG. Optional xDrive AWD.",
        },
        "640i xDrive Gran Coupe": {
            "Packages": "M Sport; Executive; Cold Weather; Driving Assistance",
            "packageDetails": "320 hp 3.0L turbo I6, xDrive AWD, 8-speed automatic.",
            "Options": "Harman Kardon audio; Nappa leather; Panoramic sunroof; Ventilated seats (Executive)",
            "optionDetails": f"{_SOURCE_TAG} AWD Gran Coupe. 19/28/22 MPG.",
        },
        "650i Gran Coupe": {
            "Packages": "M Sport; Executive; Cold Weather; Driving Assistance",
            "packageDetails": "445 hp 4.4L twin-turbo V8, 8-speed automatic, RWD. Standard 19-inch wheels, upgraded audio.",
            "Options": "Harman Kardon audio; Nappa leather; Soft-close doors; Bowers & Wilkins (optional)",
            "optionDetails": f"{_SOURCE_TAG} V8 Gran Coupe. 17/25/20 MPG.",
        },
        "650i xDrive Gran Coupe": {
            "Packages": "M Sport; Executive; Cold Weather; Driving Assistance Plus",
            "packageDetails": "445 hp 4.4L twin-turbo V8 with xDrive AWD.",
            "Options": "Harman Kardon audio; Nappa leather; Adaptive cruise; Lane departure warning",
            "optionDetails": f"{_SOURCE_TAG} V8 AWD Gran Coupe. 16/25/19 MPG.",
        },
        "640i Convertible": {
            "Packages": "M Sport; Executive; Cold Weather",
            "packageDetails": "315 hp 3.0L turbo I6 convertible, RWD, 8-speed automatic.",
            "Options": "Harman Kardon audio; Nappa leather; Wind deflector; Neck heater (Cold Weather)",
            "optionDetails": f"{_SOURCE_TAG} 2-door convertible. 20/29/23 MPG.",
        },
        "650i Convertible": {
            "Packages": "M Sport; Executive; Cold Weather",
            "packageDetails": "445 hp 4.4L twin-turbo V8 convertible.",
            "Options": "Harman Kardon audio; Nappa leather; Adaptive suspension",
            "optionDetails": f"{_SOURCE_TAG} V8 convertible. 17/25/20 MPG.",
        },
        "Alpina B6 xDrive Gran Coupe": {
            "Packages": "Alpina Performance Package",
            "packageDetails": "600 hp Alpina-tuned 4.4L twin-turbo V8, xDrive, 8-speed automatic.",
            "Options": "Alpina interior; 20-inch Alpina wheels; Harman Kardon audio",
            "optionDetails": f"{_SOURCE_TAG} High-performance Alpina B6. 16/25/19 MPG.",
        },
        "640i xDrive Gran Turismo": {
            "Packages": "M Sport; Executive; Cold Weather",
            "packageDetails": "335 hp 3.0L turbo I6 hatchback, xDrive, increased cargo versatility.",
            "Options": "Harman Kardon audio; Nappa leather; Power tailgate",
            "optionDetails": f"{_SOURCE_TAG} Gran Turismo liftback. 20/28/23 MPG.",
        },
    },
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _options_dest(year: int, make: str, model: str) -> Path:
    make_token = canonical_make(make).replace(" ", "_").replace("/", "-")
    model_token = model.replace(" ", "_")
    out_dir = OPTIONS_RAW_DIR / make_token
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{year}_{make_token}_{model_token}_Complete_Options.csv"


def _build_epa_index() -> dict[tuple[int, str, str], Path]:
    index: dict[tuple[int, str, str], Path] = {}
    for root in (EPA_DIR, DICTIONARY_ROOT):
        if not root.is_dir():
            continue
        for path in root.rglob("*_EPA.csv"):
            meta = parse_csv_filename(path)
            if not meta or meta.get("kind") != "epa" or meta["year"] is None:
                continue
            key = _research_key(meta["year"], meta["make"], meta["model"])
            index.setdefault(key, path)
    return index


def _find_epa_csv(
    year: int, make: str, model: str, epa_index: dict[tuple[int, str, str], Path] | None = None
) -> Path | None:
    if epa_index is not None:
        return epa_index.get(_research_key(year, make, model))
    return _build_epa_index().get(_research_key(year, make, model))


def _load_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="", errors="replace") as fh:
        return list(csv.DictReader(fh))


def _has_research_tag(rows: list[dict[str, str]]) -> bool:
    return any(_SOURCE_TAG in (r.get("optionDetails") or "") for r in rows)


def _is_backfill_only(rows: list[dict[str, str]]) -> bool:
    """True when rows are only year-gap copies or empty brochure summaries."""
    if not rows:
        return False
    for row in rows:
        trim = (row.get("Trim") or "").strip()
        details = (row.get("optionDetails") or "")
        if trim == "[Brochure Summary]" or "[Backfilled from" in details:
            continue
        if any((row.get(c) or "").strip() for c in ("engineOptions", "Packages", "Options")):
            return False
    return True


def _needs_fill(
    rows: list[dict[str, str]],
    status: str,
    *,
    has_epa: bool,
    has_research: bool,
    force: bool = False,
) -> bool:
    if force:
        return True
    if status in ("stub", "empty", "missing") or not rows:
        return has_epa or has_research
    if _is_sparse(rows) and has_epa:
        return True
    if has_epa and _is_backfill_only(rows):
        return True
    if has_research and not _has_research_tag(rows):
        return True
    return False


def _is_sparse(rows: list[dict[str, str]]) -> bool:
    if not rows:
        return True
    real = 0
    for row in rows:
        trim = (row.get("Trim") or "").strip()
        if trim in ("", "[Brochure Summary]"):
            continue
        if trim.startswith("[Brochure"):
            continue
        if any((row.get(c) or "").strip() for c in ("engineOptions", "Packages", "Options", "transmissionOptions")):
            real += 1
    return real == 0


def _best_existing_path(year: int, make: str, model: str) -> Path | None:
    dest = _options_dest(year, make, model)
    if dest.is_file():
        return dest
    legacy = DICTIONARY_ROOT / dest.name
    if legacy.is_file():
        return legacy
    found = find_complete_options_csv(make, model, year)
    return found


def _existing_status(year: int, make: str, model: str) -> tuple[list[dict[str, str]], str]:
    path = _best_existing_path(year, make, model)
    if path is None:
        return [], "missing"
    rows = _load_csv(path)
    return rows, options_status(path)


def _enrich_row(row: dict[str, str], year: int, make: str, model: str) -> dict[str, str]:
    out = {col: (row.get(col) or "") for col in CANONICAL_CSV_COLUMNS}
    out["Year"] = str(year)
    out["Make"] = canonical_make(make)
    out["Model"] = model
    key = _research_key(year, make, model)
    trim = (out.get("Trim") or "").strip()
    research = _RESEARCH.get(key, {}).get(trim, {})
    for field in ("Packages", "packageDetails", "Options", "optionDetails"):
        if research.get(field):
            out[field] = research[field]
    return out


def _expand_research_trims(
    rows: list[dict[str, str]], year: int, make: str, model: str
) -> list[dict[str, str]]:
    key = _research_key(year, make, model)
    research = _RESEARCH.get(key, {})
    if not research:
        return rows
    existing_trims = {(r.get("Trim") or "").strip() for r in rows}
    out = [_enrich_row(r, year, make, model) for r in rows]
    template = out[0] if out else {col: "" for col in CANONICAL_CSV_COLUMNS}
    for trim, fields in research.items():
        if trim in existing_trims:
            continue
        if trim.startswith("("):  # skip EPA-style aliases if we already have named trims
            continue
        new_row = {col: (template.get(col) or "") for col in CANONICAL_CSV_COLUMNS}
        new_row["Year"] = str(year)
        new_row["Make"] = canonical_make(make)
        new_row["Model"] = model
        new_row["Trim"] = trim
        for field, val in fields.items():
            new_row[field] = val
        out.append(new_row)
    return out


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANONICAL_CSV_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _build_from_epa(epa_path: Path, year: int, make: str, model: str) -> list[dict[str, str]]:
    rows = _load_csv(epa_path)
    built = [_enrich_row(r, year, make, model) for r in rows]
    return _expand_research_trims(built, year, make, model)


def _build_from_research_only(year: int, make: str, model: str) -> list[dict[str, str]]:
    key = _research_key(year, make, model)
    research = _RESEARCH.get(key, {})
    rows: list[dict[str, str]] = []
    for trim, fields in research.items():
        if trim.startswith("("):
            continue
        row = {col: "" for col in CANONICAL_CSV_COLUMNS}
        row["Year"] = str(year)
        row["Make"] = canonical_make(make)
        row["Model"] = model
        row["Trim"] = trim
        for field, val in fields.items():
            row[field] = val
        rows.append(row)
    return rows


def fill_one(
    year: int,
    make: str,
    model: str,
    *,
    dry_run: bool = False,
    force: bool = False,
    epa_index: dict[tuple[int, str, str], Path] | None = None,
) -> bool:
    existing, status = _existing_status(year, make, model)
    epa = _find_epa_csv(year, make, model, epa_index)
    has_epa = epa is not None
    has_research = _research_key(year, make, model) in _RESEARCH

    if not _needs_fill(
        existing, status, has_epa=has_epa, has_research=has_research, force=force
    ):
        return False

    if epa:
        rows = _build_from_epa(epa, year, make, model)
    elif has_research:
        rows = _build_from_research_only(year, make, model)
    else:
        return False

    if not rows:
        return False

    dest = _options_dest(year, make, model)
    if dry_run:
        print(f"would write {dest.name} ({len(rows)} rows) from {'EPA' if epa else 'research'}")
        return True

    _write_csv(dest, rows)
    legacy = DICTIONARY_ROOT / dest.name
    if legacy.is_file() and legacy.resolve() != dest.resolve():
        legacy.unlink(missing_ok=True)
    print(f"wrote {dest.relative_to(DICTIONARY_ROOT)} ({len(rows)} rows)")
    return True


def fill_all(*, dry_run: bool = False, force: bool = False) -> dict[str, int]:
    seen: set[tuple[int, str, str]] = set()
    stats = {"filled": 0, "skipped_has_data": 0, "skipped_no_source": 0}
    epa_index = _build_epa_index()

    def _attempt(year: int, make: str, model: str) -> None:
        if fill_one(year, make, model, dry_run=dry_run, force=force, epa_index=epa_index):
            stats["filled"] += 1
            return
        existing, status = _existing_status(year, make, model)
        epa = _find_epa_csv(year, make, model, epa_index)
        has_research = _research_key(year, make, model) in _RESEARCH
        if not epa and not has_research:
            stats["skipped_no_source"] += 1
        else:
            stats["skipped_has_data"] += 1

    for path in iter_dictionary_csv_paths("options"):
        meta = parse_csv_filename(path)
        if not meta or meta.get("kind") != "options":
            continue
        year = meta["year"]
        if year is None or year < MIN_YEAR:
            continue
        key = _research_key(year, meta["make"], meta["model"])
        if key in seen:
            continue
        seen.add(key)
        _attempt(year, meta["make"], meta["model"])

    for epa_key, epa_path in epa_index.items():
        year, _, _ = epa_key
        if year < MIN_YEAR or epa_key in seen:
            continue
        meta = parse_csv_filename(epa_path)
        if not meta:
            continue
        seen.add(epa_key)
        _attempt(year, meta["make"], meta["model"])

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite existing Complete_Options when EPA or curated research exists",
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--make")
    parser.add_argument("--model")
    args = parser.parse_args()

    if args.year and args.make and args.model:
        ok = fill_one(args.year, args.make, args.model, dry_run=args.dry_run, force=True)
        return 0 if ok or args.dry_run else 1

    stats = fill_all(dry_run=args.dry_run, force=args.force)
    print(
        "filled={filled} skipped_has_data={skipped_has_data} skipped_no_source={skipped_no_source}".format(
            **stats
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
