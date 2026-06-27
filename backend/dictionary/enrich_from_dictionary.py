#!/usr/bin/env python3
"""
Enrich cars in inventory.db using the /DICTIONARY EPA CSV files.

For each car missing transmission, drivetrain, fuel_type, cylinders,
displacement, mpg_city, mpg_highway, body_style, or exterior_color —
looks up the matching EPA CSV (year+make+model) and fills in what's available.

Transmission: EPA trim row first; if blank, any EPA row with transmission for that
YMM; then structured ``spec_source_json``; then phrases in ``description``.
Exterior color: EPA almost never lists paint; uses ``spec_source_json`` and color
phrases in ``description`` (same heuristics as ``extract_keffer_colors.py``).

Uses trim matching when possible; falls back to the most common value
for that year+make+model when no trim match is found.

Listing **title** and **trim** are merged with **description** for text extraction (many sites
omit specs from the body). When transmission is still blank after EPA + text heuristics, optional
NHTSA ``DecodeVinValuesExtended`` is used (disable with ``--no-vpic`` or offline workflows).

Usage:
  python enrich_from_dictionary.py             # fill only gaps
  python enrich_from_dictionary.py --all       # reapply to all cars
  python enrich_from_dictionary.py --dry-run   # show changes without writing
  python enrich_from_dictionary.py --no-vpic   # no NHTSA HTTP (EPA + text only)
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
# Repo root is two levels up: backend/dictionary/ → backend/ → repo root
_REPO_ROOT = ROOT.parent.parent
os.chdir(_REPO_ROOT)
sys.path.insert(0, str(ROOT.parent))  # backend/ on sys.path for package imports

try:
    from backend.utils.project_env import load_project_dotenv
    load_project_dotenv()
except ImportError:
    pass

try:
    from extract_keffer_colors import extract_color_from_description
except ImportError:
    extract_color_from_description = None  # type: ignore[misc, assignment]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("enrich_dict")

# CSVs live in the same directory as this script (backend/dictionary/)
DICTIONARY = ROOT
# Align with backend/db/inventory_db.py — prefer backend/inventory.db, fall back to repo root.
def _default_db_path() -> str:
    backend_p = _REPO_ROOT / "backend" / "inventory.db"
    root_p = _REPO_ROOT / "inventory.db"
    return str(backend_p) if backend_p.exists() else str(root_p)

DB_PATH = os.environ.get("INVENTORY_DB_PATH", _default_db_path())

FILLABLE_FIELDS = {
    "transmission":   "transmissionOptions",
    "drivetrain":     "drivetrainOptions",
    "fuel_type":      "fuelType",
    "cylinders":      "cylinders",
    "engine_l":       "displacement",
    "mpg_city":       "mpg_city",
    "mpg_highway":    "mpg_highway",
    "body_style":     "bodyStyle",
    "engine_description": "engineOptions",
    "forced_induction": "forcedInduction",
    "exterior_color": "exteriorColors",
}

_epa_cache: dict[str, list[dict]] = {}


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


_BMW_SERIES_MAP: dict[str, str] = {
    # Numbered series — model code prefix → EPA file model name
    "2":  "2 Series",
    "3":  "3 Series",
    "4":  "4 Series",
    "5":  "5 Series",
    "6":  "6 Series",
    "7":  "7 Series",
    "8":  "8 Series",
    # M performance — numbered M cars use the "M" EPA file
    "M2": "M", "M3": "M", "M4": "M", "M5": "M", "M6": "M", "M8": "M",
}

def _bmw_epa_model(model: str) -> str | None:
    """Map a BMW model code (330i, M4) to the EPA CSV model name, or None if not applicable."""
    m = model.strip()
    # Exact M-car match (M2/M3/M4/M5/M6/M8)
    if m in _BMW_SERIES_MAP:
        return _BMW_SERIES_MAP[m]
    # M2xx, M3xx, M4xx, M5xx numbered M-performance (e.g. M235i, M340i) → series file
    m_perf = re.match(r'^(M\d)(\d{2}[a-z]?)$', m, re.IGNORECASE)
    if m_perf:
        series_digit = m_perf.group(1)[1]  # e.g. M2 → "2"
        return _BMW_SERIES_MAP.get(series_digit)
    # Standard series: first digit maps to series (330i → "3", 530e → "5")
    digit_match = re.match(r'^([2-9])(\d{2}[a-z]?)', m, re.IGNORECASE)
    if digit_match:
        return _BMW_SERIES_MAP.get(digit_match.group(1))
    return None


# MINI: dealer systems use body-style names; EPA uses platform names.
# "2 Door" / "Hardtop 2 Door" etc. → "Cooper"; "Convertible" → "Cooper" (no separate conv. EPA file).
_MINI_MODEL_MAP: dict[str, str] = {
    "2 door":          "Cooper",
    "4 door":          "Cooper",
    "hardtop 2 door":  "Cooper",
    "hardtop 4 door":  "Cooper",
    "hardtop":         "Cooper",
    "cooper hardtop":  "Cooper",
    "convertible":     "Cooper",
    "cooper roadster":  "Roadster",
}

def _mini_epa_model(model: str) -> str | None:
    return _MINI_MODEL_MAP.get(model.strip().lower())


def _load_epa_csv(year: int, make: str, model: str) -> list[dict]:
    key = f"{year}_{make}_{model}"
    if key in _epa_cache:
        return _epa_cache[key]

    rows: list[dict] = []
    try:
        from backend.enrichment.epa_master_store import fetch_epa_rows

        rows = list(fetch_epa_rows(year, make, model))
    except ImportError:
        pass

    if rows:
        _epa_cache[key] = rows
        return rows

    try:
        from backend.enrichment.dictionary_catalog import find_epa_csv

        catalog_path = find_epa_csv(make, model, year)
        if catalog_path and catalog_path.is_file():
            with catalog_path.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
    except ImportError:
        catalog_path = None

    make_safe = re.sub(r"[^\w\-. ]", "_", make.strip())
    model_safe = re.sub(r"[^\w\-. ]", "_", model.strip())
    fname = DICTIONARY / f"{year}_{make_safe}_{model_safe}_EPA.csv"

    if not rows and fname.exists():
        with open(fname, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    elif not rows:
        # BMW series mapping: "330i" → "3 Series", "M4" → "M", etc.
        if make.strip().upper() == "BMW":
            series_model = _bmw_epa_model(model)
            if series_model:
                series_safe = re.sub(r"[^\w\-. ]", "_", series_model)
                series_fname = DICTIONARY / f"{year}_{make_safe}_{series_safe}_EPA.csv"
                # Space-preserving variant (e.g. "3 Series" not "3_Series")
                series_fname_space = DICTIONARY / f"{year}_{make_safe}_{series_model}_EPA.csv"
                for candidate in (series_fname, series_fname_space):
                    if candidate.exists():
                        with open(candidate, newline="", encoding="utf-8") as f:
                            rows = list(csv.DictReader(f))
                        break

        # MINI model mapping: "2 Door" / "Hardtop 2 Door" / "Convertible" → "Cooper", etc.
        if not rows and make.strip().upper() == "MINI":
            mini_model = _mini_epa_model(model)
            if mini_model:
                mini_safe = re.sub(r"[^\w\-. ]", "_", mini_model)
                for candidate in (
                    DICTIONARY / f"{year}_{make_safe}_{mini_model}_EPA.csv",
                    DICTIONARY / f"{year}_{make_safe}_{mini_safe}_EPA.csv",
                ):
                    if candidate.exists():
                        with open(candidate, newline="", encoding="utf-8") as f:
                            rows = list(csv.DictReader(f))
                        break

        if not rows:
            # Try fuzzy matching: strip common variant/trim suffixes to find base model file
            for variant_sep in [
                # Chevrolet/GMC truck variants — longest suffix first to avoid partial strip
                " 3500 HD Chassis Cab", " 3500 HD", " 2500 HD", " 1500",
                " i-FORCE MAX",                                      # Toyota hybrid powertrain suffix
                " PHEV", " Plug-In Hybrid", " Hybrid", " Energi",   # powertrain variants
                " GT", " GTS", " N Line",                            # sport/performance variants
                " L", " XL", " LS", " LT", " Limited", " Pro", " Sport",  # trim levels
            ]:
                if variant_sep.lower() in model.lower():
                    base_model = re.sub(re.escape(variant_sep), "", model, flags=re.IGNORECASE).strip()
                    base_model_safe = re.sub(r"[^\w\-. ]", "_", base_model)
                    base_fname = DICTIONARY / f"{year}_{make_safe}_{base_model_safe}_EPA.csv"
                    if base_fname.exists():
                        with open(base_fname, newline="", encoding="utf-8") as f:
                            rows = list(csv.DictReader(f))
                        break
    _epa_cache[key] = rows
    return rows


def _best_row(epa_rows: list[dict], trim: str) -> dict | None:
    if not epa_rows:
        return None

    norm_trim = _normalize(trim or "")

    # Exact trim match
    for row in epa_rows:
        if _normalize(row.get("Trim", "")) == norm_trim:
            return row

    # Partial trim match — car trim is substring of EPA trim or vice versa
    if norm_trim:
        for row in epa_rows:
            epa_t = _normalize(row.get("Trim", ""))
            if norm_trim in epa_t or epa_t in norm_trim:
                return row

    # Fallback: most common transmission (proxy for "base/most popular trim")
    trans_counter: Counter = Counter()
    for row in epa_rows:
        t = (row.get("transmissionOptions") or "").strip()
        if t:
            trans_counter[t] += 1
    if trans_counter:
        most_common_trans = trans_counter.most_common(1)[0][0]
        for row in epa_rows:
            if (row.get("transmissionOptions") or "").strip() == most_common_trans:
                return row

    return epa_rows[0]


def _is_empty(val: Any) -> bool:
    """Treat placeholders as empty so we overwrite junk DMS values with EPA / parsed listing text."""
    if val is None:
        return True
    s = str(val).strip()
    if not s:
        return True
    low = s.lower()
    return low in (
        "n/a",
        "na",
        "unknown",
        "null",
        "none",
        "-",
        "--",
        "—",
        "tbd",
        "not specified",
        "unspecified",
        "undefined",
        "0",
    )


def _first_nonempty_transmission(epa_rows: list[dict]) -> str:
    """Any EPA row may carry transmission when the trim-matched row does not."""
    for row in epa_rows:
        t = (row.get("transmissionOptions") or "").strip()
        if t:
            return t
    return ""


def _combined_listing_text(car: dict) -> str:
    """Join title, trim, and description — dealers often put trans / paint only in the title line."""
    parts: list[str] = []
    for key in ("title", "trim", "description"):
        raw = car.get(key)
        if raw is None:
            continue
        s = str(raw).strip()
        if s:
            parts.append(s)
    return "\n".join(parts)


_TRANS_DESC_PATTERNS = (
    # Speed + style (with optional hyphen; titles often omit "transmission")
    re.compile(r"\b\d{1,2}[- ]speed\s+(?:automatic|auto)\b", re.I),
    re.compile(r"\b\d{1,2}[- ]speed\s+automatic\s+transmission\b", re.I),
    re.compile(r"\b\d{1,2}[- ]speed\s+dual\s+clutch\b", re.I),
    re.compile(r"\b\d{1,2}[- ]speed\s+manual\b", re.I),
    re.compile(r"\b\d{1,2}[- ]speed\s+manual\s+transmission\b", re.I),
    re.compile(r"\b(?:continuously\s+variable\s+transmission|CVT)\b", re.I),
    re.compile(r"\be[- ]?CVT\b", re.I),
    re.compile(r"\bmanual\s+transmission\b", re.I),
    re.compile(r"\b(?:automatic|auto)\s+transmission\b", re.I),
    re.compile(r"\bshiftable\s+automatic\b", re.I),
    re.compile(r"\bdual\s+clutch\b", re.I),
    re.compile(r"\b(?:single[- ]speed|direct[- ]drive)\b", re.I),
    re.compile(r"\b(?:torque\s+converter)\s+automatic\b", re.I),
    # Standalone tokens common in inventory titles
    re.compile(r"\bCVT\b"),
    re.compile(r"\bDCT\b"),
    re.compile(r"\b(?:PDK|SMG|DSG)\b"),
)


def _extract_transmission_from_description(description: str) -> str:
    if not description:
        return ""
    for pat in _TRANS_DESC_PATTERNS:
        m = pat.search(description)
        if m:
            return m.group(0).strip().title()
    return ""


def _vpic_transmission_fallback(car: dict) -> str:
    """NHTSA vPIC DecodeVin — fills transmission when listing text has no usable phrase."""
    try:
        from backend.enrichment.nhtsa_vpic import (
            fetch_decode_vin_values_extended,
            flat_vpic_result_to_car_patch,
            looks_like_decode_vin,
        )
    except ImportError:
        return ""
    vin = car.get("vin") or ""
    if not looks_like_decode_vin(str(vin)):
        return ""
    _body, flat, err = fetch_decode_vin_values_extended(str(vin).strip())
    if err or not flat:
        return ""
    patch = flat_vpic_result_to_car_patch(flat)
    return str(patch.get("transmission") or "").strip()


def _spec_json_trans_and_color(spec_raw: str | None) -> tuple[str, str]:
    """Pull embedded transmission / exterior color strings from structured capture JSON."""
    if not spec_raw or not str(spec_raw).strip():
        return "", ""
    try:
        obj = json.loads(spec_raw)
    except json.JSONDecodeError:
        return "", ""

    trans_out, color_out = "", ""

    def walk(o: Any) -> None:
        nonlocal trans_out, color_out
        if isinstance(o, dict):
            for k, v in o.items():
                kl = str(k).lower().replace(" ", "")
                if isinstance(v, str) and v.strip():
                    vs = v.strip()[:160]
                    if not trans_out and "transmission" in kl and "interior" not in kl:
                        trans_out = vs
                    if not color_out and (
                        ("exterior" in kl and "color" in kl)
                        or kl in ("exteriorcolor", "primaryexteriorcolor", "paintcolor", "bodycolor")
                    ):
                        color_out = vs
                elif isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    walk(obj)
    return trans_out, color_out


def enrich_car(car: dict, dry_run: bool = False, fill_all: bool = False, *, use_vpic: bool = True) -> dict:
    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = car.get("trim") or ""

    updates: dict[str, Any] = {}

    if not year or not make or not model:
        return updates

    combo = _combined_listing_text(car)
    spec_trans, spec_color = _spec_json_trans_and_color(car.get("spec_source_json"))

    epa_rows = _load_epa_csv(int(year), make, model)
    if not epa_rows:
        # No DICTIONARY EPA file — still fill transmission / color from capture JSON or listing text.
        if fill_all or _is_empty(car.get("transmission")):
            v = spec_trans or _extract_transmission_from_description(combo)
            if not v and use_vpic:
                v = _vpic_transmission_fallback(car)
            if v:
                updates["transmission"] = v
        if fill_all or _is_empty(car.get("exterior_color")):
            c = spec_color
            if not c and extract_color_from_description is not None:
                c = extract_color_from_description(combo) or ""
            if c:
                updates["exterior_color"] = c
        try:
            from backend.utils.forced_induction import classify_forced_induction_from_car_row

            fi = classify_forced_induction_from_car_row({**car, **updates})
            if fi and (fill_all or _is_empty(car.get("forced_induction"))):
                updates["forced_induction"] = fi
        except Exception:
            pass
        return updates

    best = _best_row(epa_rows, trim)
    if not best:
        return updates

    try:
        from backend.dictionary.epa_engine import (
            engine_fields_need_correction,
            format_epa_engine_display,
            pick_epa_engine_row,
        )
        from backend.utils.car_serialize import (
            _effective_cylinder_count,
            parse_engine_displacement_liters,
        )

        car_lit = parse_engine_displacement_liters(car)
        car_cyl = _effective_cylinder_count(car, {})
        engine_row = pick_epa_engine_row(epa_rows, car, car_lit=car_lit, car_cyl=car_cyl)
        if engine_row and engine_fields_need_correction(
            car, engine_row, car_lit=car_lit, car_cyl=car_cyl
        ):
            epa_lit = engine_row.get("displacement")
            epa_cyl = engine_row.get("cylinders")
            epa_eng = (engine_row.get("engineOptions") or "").strip()
            if epa_eng:
                updates["engine_description"] = epa_eng
            try:
                lit_f = float(epa_lit)
                if lit_f > 0:
                    updates["engine_l"] = lit_f
            except (TypeError, ValueError):
                pass
            try:
                cyl_i = int(float(epa_cyl))
                if cyl_i > 0:
                    updates["cylinders"] = cyl_i
            except (TypeError, ValueError):
                pass
            if format_epa_engine_display(engine_row, car):
                best = engine_row
    except Exception:
        pass

    try:
        from backend.utils.forced_induction import classify_forced_induction_from_car_row

        merged = {**car, **updates}
        fi = classify_forced_induction_from_car_row(merged)
        if fi and (fill_all or _is_empty(car.get("forced_induction"))):
            updates["forced_induction"] = fi
    except Exception:
        pass

    try:
        from backend.dictionary.epa_engine import enrich_car_engine_from_dictionary

        dict_engine = enrich_car_engine_from_dictionary({**car, **updates}, overwrite=fill_all)
        if dict_engine:
            for k, v in dict_engine.items():
                if fill_all or _is_empty(car.get(k)):
                    updates[k] = v
    except Exception:
        pass

    for db_col, csv_col in FILLABLE_FIELDS.items():
        current = car.get(db_col)
        if not fill_all and not _is_empty(current):
            continue

        epa_val = (best.get(csv_col) or "").strip()

        if db_col == "transmission":
            if not epa_val:
                epa_val = _first_nonempty_transmission(epa_rows)
            if not epa_val and spec_trans:
                epa_val = spec_trans
            if not epa_val:
                epa_val = _extract_transmission_from_description(combo)
            if not epa_val and use_vpic:
                epa_val = _vpic_transmission_fallback(car)
        elif db_col == "exterior_color":
            if not epa_val and spec_color:
                epa_val = spec_color
            if not epa_val and extract_color_from_description is not None:
                epa_val = extract_color_from_description(combo) or ""
        else:
            epa_val = (best.get(csv_col) or "").strip()

        if not epa_val or epa_val in ("0", "0.0"):
            continue
        # Type coercions
        if db_col == "cylinders":
            try:
                epa_val = int(float(epa_val))
                if epa_val == 0:
                    continue
            except (ValueError, TypeError):
                continue
        elif db_col in ("mpg_city", "mpg_highway"):
            try:
                epa_val = int(float(epa_val))
                if epa_val <= 0:
                    continue
            except (ValueError, TypeError):
                continue
        elif db_col == "engine_l":
            try:
                epa_val = float(epa_val)
                if epa_val <= 0:
                    continue
            except (ValueError, TypeError):
                continue
        updates[db_col] = epa_val

    return updates


# SQL: common DMS placeholder strings — align with _is_empty()
_SQL_JUNK_TEXT_IN = (
    "'unknown','n/a','na','null','none','--','-','—','tbd','not specified','unspecified','undefined'"
)


def main():
    ap = argparse.ArgumentParser(description="Enrich cars from DICTIONARY EPA data")
    ap.add_argument("--all", action="store_true", help="Reapply to all cars (not just those with gaps)")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be filled without writing")
    ap.add_argument(
        "--no-vpic",
        action="store_true",
        help="Do not call NHTSA vPIC when transmission is still missing (EPA + listing text only)",
    )
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    if args.all:
        query = "SELECT * FROM cars WHERE make IS NOT NULL AND model IS NOT NULL ORDER BY id"
    else:
        # Cars with any gap in dictionary-fillable fields. Per-field updates still respect _is_empty /
        # fill_all inside enrich_car — we never overwrite real values here.
        junk = _SQL_JUNK_TEXT_IN
        query = (
            "SELECT * FROM cars WHERE make IS NOT NULL AND TRIM(IFNULL(make,''))!='' "
            "AND model IS NOT NULL AND TRIM(IFNULL(model,''))!='' "
            "AND year IS NOT NULL AND IFNULL(year,0)!=0 "
            "AND ("
            " transmission IS NULL OR TRIM(IFNULL(transmission,''))='' "
            f" OR LOWER(TRIM(IFNULL(transmission,''))) IN ({junk})"
            " OR drivetrain IS NULL OR TRIM(IFNULL(drivetrain,''))='' "
            f" OR LOWER(TRIM(IFNULL(drivetrain,''))) IN ({junk})"
            " OR fuel_type IS NULL OR TRIM(IFNULL(fuel_type,''))='' "
            f" OR LOWER(TRIM(IFNULL(fuel_type,''))) IN ({junk})"
            " OR cylinders IS NULL OR cylinders=0 "
            " OR engine_l IS NULL OR TRIM(COALESCE(CAST(engine_l AS TEXT),'')) IN ('','0','0.0') "
            " OR mpg_city IS NULL OR mpg_city=0 "
            " OR mpg_highway IS NULL OR mpg_highway=0 "
            " OR body_style IS NULL OR TRIM(IFNULL(body_style,''))='' "
            f" OR LOWER(TRIM(IFNULL(body_style,''))) IN ({junk})"
            " OR engine_description IS NULL OR TRIM(IFNULL(engine_description,''))='' "
            f" OR LOWER(TRIM(IFNULL(engine_description,''))) IN ({junk})"
            " OR exterior_color IS NULL OR TRIM(IFNULL(exterior_color,''))='' "
            f" OR LOWER(TRIM(IFNULL(exterior_color,''))) IN ({junk})"
            ") ORDER BY id"
        )

    cars = [dict(r) for r in conn.execute(query).fetchall()]
    logger.info("Cars to process: %d", len(cars))
    if args.dry_run:
        logger.info("DRY-RUN mode — no DB writes")

    stats = {"total": len(cars), "updated": 0, "no_epa": 0}
    updated_ids: list[int] = []

    logger.info("inventory.db path: %s", os.path.abspath(DB_PATH))

    for car in cars:
        updates = enrich_car(car, dry_run=args.dry_run, fill_all=args.all, use_vpic=not args.no_vpic)
        if not updates:
            stats["no_epa"] += 1
            continue

        if not args.dry_run:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            vals = list(updates.values()) + [car["id"]]
            conn.execute(f"UPDATE cars SET {set_clause} WHERE id=?", vals)
            updated_ids.append(int(car["id"]))

        stats["updated"] += 1
        fields_str = ", ".join(f"{k}={v}" for k, v in updates.items())
        logger.info("[%s] %s %s %s → %s", car["vin"], car["year"], car["make"], car["model"], fields_str)

    if not args.dry_run:
        conn.commit()
    conn.close()

    # Dev incomplete listings read incomplete_listings.db — refresh rows we touched or counts stay stale.
    if not args.dry_run and updated_ids:
        try:
            from backend.db.incomplete_listings_db import sync_incomplete_listing_for_car_id

            for cid in updated_ids:
                sync_incomplete_listing_for_car_id(cid)
            logger.info(
                "Synced incomplete_listings index for %d car(s) (dev /incomplete UI uses this).",
                len(updated_ids),
            )
        except Exception:
            logger.exception(
                "Incomplete listings index sync failed — run: python rebuild_listings_index.py"
            )

    logger.info("=" * 55)
    logger.info("Complete: total=%d updated=%d no_epa_match=%d", stats["total"], stats["updated"], stats["no_epa"])
    if stats["updated"]:
        logger.info("Optional: python rebuild_listings_index.py --fast (full resync if sync step failed)")


if __name__ == "__main__":
    main()
