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

Every value goes through fact precedence (``backend.vehicle_facts``): the writer proposes
``(field, value, source)`` and the rank rules decide. The EPA catalog fills blanks and never
outranks the dealer's own engine text or the VIN decode (NHTSA vPIC). An engine correction
(displacement / cylinders / engine text) is applied only when vPIC or the dealer's own title
or trim corroborates the catalog row (D-PR3 b+d); otherwise the disagreement is recorded as a
discrepancy and the stored value stays. EPA exterior colors and the CSV ``forcedInduction``
label are not written (the labels are about half wrong; P10C.4 wires the resolver).

``--all`` refills only fields whose current, labeled source ranks at or below the catalog
(P8B.5 replaces the P1A.5 env guard). Unlabeled filled values are left alone until the
legacy provenance is relabeled (P8C.5).

Usage:
  python enrich_from_dictionary.py             # fill only gaps
  python enrich_from_dictionary.py --dry-run   # show changes (and the skip count) without writing
  python enrich_from_dictionary.py --no-vpic   # no NHTSA HTTP (EPA + text only)
  python enrich_from_dictionary.py --all       # also refill fields at or below the catalog's rank
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

# The legacy top-level ``extract_keffer_colors`` script this used to import is
# gone from the repo, so the ``except ImportError: None`` fallback had silently
# disabled description-based color fill everywhere. The function is now
# vendored in ``backend.dictionary.color_extract`` and always importable.
from backend.dictionary.color_extract import extract_color_from_description

logger = logging.getLogger("enrich_dict")


def _configure_cli_process() -> None:
    """Process-wide setup for the ``__main__`` entry point ONLY.

    ``os.chdir``, ``sys.path`` mutation, ``.env`` loading and ``logging.basicConfig``
    used to run at import time. This module is not only a script: ``epa_engine``
    imports ``_load_epa_csv`` / ``_best_row`` / ``_is_empty`` from it lazily inside
    ``resolve_engine_display_from_epa``, which runs on ordinary car serialization.
    Every one of those side effects therefore fired inside the web app and the test
    suite the first time a car was serialized -- changing the process working
    directory (which relative DB paths resolve against), installing a root log
    handler, and re-injecting ``.env`` over the running process's environment.
    In pytest that last one restored ``INVENTORY_DATABASE_URL`` after conftest had
    cleared it, so every DB call after the first serialization in the session went
    to the real Postgres inventory instead of the test SQLite file.
    """
    os.chdir(_REPO_ROOT)
    backend_dir = str(ROOT.parent)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)  # backend/ on sys.path for package imports
    try:
        from backend.utils.project_env import load_project_dotenv

        load_project_dotenv()
    except ImportError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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
}
# Columns that are not precedence fields: the catalog is their only writer here.
_PLAIN_COLUMNS = ("mpg_city", "mpg_highway", "body_style")

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


def _all_wheel_row(row: dict) -> bool:
    from backend.vehicle_facts.drivetrain import normalize_drivetrain

    return normalize_drivetrain(row.get("drivetrainOptions"), "dealer") in ("AWD", "4WD")


def _best_row(epa_rows: list[dict], trim: str, *, two_wheel: bool = False) -> dict | None:
    """Trim match, else the most common transmission's row.

    *two_wheel*: the VIN decode says 4x2 (a {FWD, RWD, 2WD} constraint). The
    most-common-row fallback is a guess about "the base trim", so it skips rows
    that are all-wheel when any other row exists; a trim match is still trusted
    (the precedence rules refuse an all-wheel fill against 4x2 either way).
    """
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

    if two_wheel:
        epa_rows = [r for r in epa_rows if not _all_wheel_row(r)] or epa_rows

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


class DictionaryUpdates(dict):
    """Decided column updates from :func:`enrich_car`, plus what a writer needs.

    The dict itself is the accepted ``{column: value}`` (precedence fields that won,
    plus the plain mpg / body columns). ``proposals`` and ``ctx`` replay the same
    decisions through ``apply_fact_proposals`` (the one persistence path), ``skipped``
    counts candidates the rank rules or the ``--all`` filter refused.
    """

    def __init__(self) -> None:
        super().__init__()
        self.proposals: list[tuple[str, Any, str]] = []
        self.ctx: dict[str, Any] = {}
        self.extras: dict[str, Any] = {}
        self.provenance: dict[str, dict] = {}
        self.decisions: list[Any] = []
        self.skipped = 0


_NOT_A_REFUSAL = ("unchanged", "blank candidate", "junk candidate")

_TITLE_LITERS_RE = re.compile(r"\b(\d{1,2}\.\d)\s*(?:L|-?liter|litre)s?\b", re.I)


def vpic_context_for(flat: dict | None) -> dict[str, Any]:
    """Precedence ctx (``vpic`` / ``vpic_drive``) from a raw vPIC ``Results[0]`` row."""
    if not flat:
        return {}
    ctx: dict[str, Any] = {"vpic": flat}
    drive = str(flat.get("DriveType") or "").strip()
    if drive:
        ctx["vpic_drive"] = drive
    return ctx


def vpic_contexts_for_vins(vins: list[str]) -> dict[str, dict[str, Any]]:
    """``{VIN: vpic ctx}`` read fresh from ``nhtsa_vpic_cache`` in one connection.

    Empty when there is no decode or the cache cannot be read: the catalog then
    fills blanks only and cannot correct an engine on a VIN-less ground truth.
    """
    wanted = sorted({str(v).strip().upper() for v in vins if v})
    if not wanted:
        return {}
    conn = None
    try:
        from backend.enrichment.knowledge_engine import _conn
        from backend.enrichment.vpic_facts import _vpic_flat_rows

        conn = _conn()
        flats = _vpic_flat_rows(conn, wanted)
    except Exception:
        return {}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return {vin: vpic_context_for(flat) for vin, flat in flats.items()}


def _title_engine_signal(car: dict) -> tuple[float | None, int | None]:
    """Displacement and cylinder count the dealer's own title / trim state.

    Description and the stored engine columns are deliberately not read: the
    columns are what is being corrected, and the description is long marketing
    copy. Each value is returned only when the text names exactly one.
    """
    from backend.dictionary.epa_engine import (
        _EIGHT_CYL_V8_RE,
        _FOUR_CYL_RE,
        _INLINE_SIX_RE,
        _SIX_CYL_V6_RE,
    )

    text = " ".join(str(car.get(k) or "") for k in ("title", "trim"))
    lits = {float(m) for m in _TITLE_LITERS_RE.findall(text)}
    cyls: set[int] = set()
    if _FOUR_CYL_RE.search(text):
        cyls.add(4)
    if _SIX_CYL_V6_RE.search(text) or _INLINE_SIX_RE.search(text):
        cyls.add(6)
    if _EIGHT_CYL_V8_RE.search(text):
        cyls.add(8)
    return (next(iter(lits)) if len(lits) == 1 else None,
            next(iter(cyls)) if len(cyls) == 1 else None)


def engine_correction_corroborated(car: dict, epa_row: dict, vpic_flat: dict | None) -> str | None:
    """Who backs a catalog engine correction: ``'vpic'``, ``'dealer_title'`` or ``None``.

    D-PR3 (b)+(d). vPIC corroborates when its displacement is within 0.15 L of the
    catalog row and the cylinder count is the same; a decode that names a different
    engine vetoes the correction whatever the title says. The dealer's title or trim
    corroborates when it states the catalog row's displacement (within 0.15 L) or
    cylinder count and states nothing that contradicts it.
    """
    epa_lit = None
    epa_cyl = None
    try:
        epa_lit = float(epa_row.get("displacement")) or None
    except (TypeError, ValueError):
        pass
    try:
        epa_cyl = int(float(epa_row.get("cylinders"))) or None
    except (TypeError, ValueError):
        pass
    if epa_lit is None and epa_cyl is None:
        return None

    if vpic_flat:
        try:
            v_lit = float(str(vpic_flat.get("DisplacementL") or "").strip())
        except ValueError:
            v_lit = None
        try:
            v_cyl = int(float(str(vpic_flat.get("EngineCylinders") or "").strip()))
        except ValueError:
            v_cyl = None
        if v_lit is not None and v_cyl is not None and epa_lit is not None and epa_cyl is not None:
            if abs(v_lit - epa_lit) <= 0.15 and v_cyl == epa_cyl:
                return "vpic"
            return None

    t_lit, t_cyl = _title_engine_signal(car)
    backed = False
    if t_lit is not None and epa_lit is not None:
        if abs(t_lit - epa_lit) > 0.15:
            return None
        backed = True
    if t_cyl is not None and epa_cyl is not None:
        if t_cyl != epa_cyl:
            return None
        backed = True
    return "dealer_title" if backed else None


def _vpic_engine_row(epa_rows: list[dict], flat: dict | None, trim: str) -> dict | None:
    """The catalog row that matches the VIN decode's engine (0.15 L, same cylinders)."""
    if not flat:
        return None
    try:
        v_lit = float(str(flat.get("DisplacementL") or "").strip())
        v_cyl = int(float(str(flat.get("EngineCylinders") or "").strip()))
    except ValueError:
        return None
    matching = []
    for r in epa_rows:
        try:
            if abs(float(r.get("displacement")) - v_lit) <= 0.15 and int(float(r.get("cylinders"))) == v_cyl:
                matching.append(r)
        except (TypeError, ValueError):
            continue
    return _best_row(matching, trim) if matching else None


def _refillable(field: str, car: dict) -> bool:
    """``--all``: may the catalog refill this filled value?

    Only when the value's labeled source ranks at or below the catalog. An unlabeled
    value could be the dealer's own, so it stays until the legacy provenance is
    relabeled (P8C.5).
    """
    from backend.vehicle_facts import precedence as prec

    src = prec.field_source(field, car.get(field), car.get("spec_source_json"))
    if src == prec.UNKNOWN:
        return False
    stored_rank = prec.rank_of(field, src)
    catalog_rank = prec.rank_of(field, prec.EPA_CATALOG)
    return stored_rank is not None and catalog_rank is not None and stored_rank <= catalog_rank


def _resolve_forced_induction(car: dict, flat: dict | None) -> tuple[str | None, str]:
    """P10C.4: the P10C.2 resolver with the VIN decode and any catalog labels in hand.

    The CSV forcedInduction labels are ~51% wrong, so they are not family flags;
    the epa_master family is reconciled by the linker (P10C.3).
    """
    from backend.utils.forced_induction import resolve_car_forced_induction

    turbo = str((flat or {}).get("Turbo") or "").strip().lower()
    boosted = True if turbo == "yes" else False if turbo == "no" else None
    try:
        return resolve_car_forced_induction(car, vpic_boosted=boosted)
    except Exception:
        return None, "unknown"


def enrich_car(
    car: dict,
    dry_run: bool = False,
    fill_all: bool = False,
    *,
    use_vpic: bool = True,
    vpic_ctx: dict | None = None,
) -> DictionaryUpdates:
    """Decide what the EPA dictionary may write to *car*; nothing is persisted here.

    *vpic_ctx* is the stored VIN decode (:func:`vpic_context_for`); without it the
    catalog can fill blanks but only the dealer's title / trim can back an engine
    correction. Persist with :func:`apply_dictionary_updates`.
    """
    from backend.vehicle_facts import precedence as prec
    from backend.vehicle_facts.drivetrain import is_two_wheel_unknown
    from backend.vehicle_facts.electrification import BEV, electrification

    year = car.get("year")
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = car.get("trim") or ""

    result = DictionaryUpdates()
    if not year or not make or not model:
        return result

    vctx = dict(vpic_ctx or {})
    flat = vctx.get("vpic") if isinstance(vctx.get("vpic"), dict) else None
    vpic_drive = vctx.get("vpic_drive")
    vpic_two = bool(vpic_drive) and is_two_wheel_unknown(vpic_drive)

    # field -> (value, source). EPA values carry 'epa_dictionary' (an alias of the
    # catalog rank); transmission text found elsewhere keeps its honest label.
    cands: dict[str, tuple[Any, str]] = {}
    corr_fields: set[str] = set()
    corroborated = False

    combo = _combined_listing_text(car)
    spec_trans, _spec_color = _spec_json_trans_and_color(car.get("spec_source_json"))

    def _text_transmission() -> tuple[str, str]:
        if spec_trans:
            return spec_trans, prec.DEALER_NORMALIZED
        v = _extract_transmission_from_description(combo)
        if v:
            return v, prec.DEALER_TEXT
        if use_vpic:
            v = _vpic_transmission_fallback(car)
            if v:
                return v, prec.NHTSA_VPIC
        return "", ""

    epa_rows = _load_epa_csv(int(year), make, model)
    if not epa_rows:
        # No DICTIONARY EPA file: transmission can still come from capture JSON or listing text.
        if fill_all or _is_empty(car.get("transmission")):
            v, src = _text_transmission()
            if v:
                cands["transmission"] = (v, src)
        best = None
    else:
        best = _best_row(epa_rows, trim, two_wheel=vpic_two)
        if not best:
            return result

        eng_corr: dict[str, Any] = {}
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
            engine_row = _vpic_engine_row(epa_rows, flat, trim) or pick_epa_engine_row(
                epa_rows, car, car_lit=car_lit, car_cyl=car_cyl
            )
            if engine_row and engine_fields_need_correction(
                car, engine_row, car_lit=car_lit, car_cyl=car_cyl
            ):
                epa_lit = engine_row.get("displacement")
                epa_cyl = engine_row.get("cylinders")
                epa_eng = (engine_row.get("engineOptions") or "").strip()
                if epa_eng:
                    eng_corr["engine_description"] = epa_eng
                try:
                    lit_f = float(epa_lit)
                    if lit_f > 0:
                        eng_corr["engine_l"] = lit_f
                except (TypeError, ValueError):
                    pass
                try:
                    cyl_i = int(float(epa_cyl))
                    if cyl_i > 0:
                        eng_corr["cylinders"] = cyl_i
                except (TypeError, ValueError):
                    pass
                if format_epa_engine_display(engine_row, car):
                    best = engine_row
                # D-PR3 (b)+(d): the correction is proposed either way; the rank rules
                # accept it only when vPIC or the dealer's own title / trim backs it,
                # otherwise it is recorded as a discrepancy.
                if engine_correction_corroborated(car, engine_row, flat):
                    corroborated = True
                    corr_fields = set(eng_corr)
        except Exception:
            pass
        for k, v in eng_corr.items():
            cands[k] = (v, "epa_dictionary")

        try:
            from backend.dictionary.epa_engine import enrich_car_engine_from_dictionary

            dict_engine = enrich_car_engine_from_dictionary({**car, **eng_corr}, overwrite=fill_all)
            for k, v in (dict_engine or {}).items():
                if k == "forced_induction":
                    continue  # CSV labels are ~51% wrong; the resolver below decides
                if fill_all or _is_empty(car.get(k)):
                    cands[k] = (v, "epa_dictionary")
        except Exception:
            pass

        for db_col, csv_col in FILLABLE_FIELDS.items():
            current = car.get(db_col)
            if not fill_all and not _is_empty(current):
                continue

            src = "epa_dictionary"
            epa_val = (best.get(csv_col) or "").strip()

            if db_col == "transmission":
                if not epa_val:
                    epa_val = _first_nonempty_transmission(epa_rows)
                if not epa_val:
                    epa_val, src = _text_transmission()
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
            cands[db_col] = (epa_val, src)

    fi_label, fi_src = _resolve_forced_induction(car, flat)
    if fi_label and (fill_all or _is_empty(car.get("forced_induction"))):
        cands["forced_induction"] = (fi_label, fi_src)
        if fi_src == prec.DEALER_FEED and car.get("forced_induction") != fi_label:
            # The dealer's own engine text states it; a stale stored label yields.
            corr_fields.add("forced_induction")

    # A battery-electric vehicle has no engine displacement or cylinders to fill.
    if cands and electrification(car, flat) == BEV:
        for k in ("engine_l", "cylinders", "engine_description"):
            cands.pop(k, None)

    proposals: list[tuple[str, Any, str]] = []
    for field, (value, src) in cands.items():
        if field in prec.FIELDS:
            if (fill_all and field not in corr_fields
                    and not prec.is_blank(field, car.get(field)) and not _refillable(field, car)):
                result.skipped += 1
                continue
            proposals.append((field, value, src))
        elif field in _PLAIN_COLUMNS:
            filled = not _is_empty(car.get(field))
            if filled and (field == "body_style" or car.get(field) == value):
                continue
            result.extras[field] = value

    ctx = dict(vctx)
    ctx["catalog_engine_correction"] = True
    if corroborated:
        ctx["corroborated"] = True
    changes, patch, decisions = prec.plan_updates(car, proposals, ctx)
    result.proposals = proposals
    result.ctx = ctx
    result.provenance = patch
    result.decisions = decisions
    result.skipped += sum(1 for d in decisions if not d.write and d.reason not in _NOT_A_REFUSAL)
    result.update(changes)
    result.update(result.extras)
    return result


def apply_dictionary_updates(car: dict, updates: DictionaryUpdates, *, dry_run: bool = False) -> dict[str, Any]:
    """Persist :func:`enrich_car` output through the fact-precedence apply helper.

    The precedence fields and their ``spec_source_json`` provenance (including
    discrepancies for refused corrections) go out in one UPDATE; the plain
    mpg / body columns follow in their own partial update. A dry run writes nothing.
    """
    from backend.vehicle_facts.apply import apply_fact_proposals

    res = apply_fact_proposals(None, car, updates.proposals, ctx=updates.ctx, dry_run=dry_run)
    if updates.extras and not dry_run and car.get("id") is not None:
        from backend.db.repositories.cars_repo import update_car_row_partial

        update_car_row_partial(int(car["id"]), dict(updates.extras))
    return {"applied": {**res.applied, **updates.extras}, "kept": res.kept, "written": res.written}


# SQL: common DMS placeholder strings — align with _is_empty()
_SQL_JUNK_TEXT_IN = (
    "'unknown','n/a','na','null','none','--','-','—','tbd','not specified','unspecified','undefined'"
)


def main():
    ap = argparse.ArgumentParser(description="Enrich cars from DICTIONARY EPA data")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Also refill filled values whose labeled source ranks at or below the EPA catalog",
    )
    ap.add_argument("--dry-run", action="store_true", help="Show what would be filled (and skipped) without writing")
    ap.add_argument(
        "--no-vpic",
        action="store_true",
        help="Do not call NHTSA vPIC when transmission is still missing (EPA + listing text only)",
    )
    args = ap.parse_args()
    _configure_cli_process()

    from backend.db.inventory_db import get_conn

    conn = get_conn()
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
            ") ORDER BY id"
        )

    cars = [dict(r) for r in conn.execute(query).fetchall()]
    logger.info("Cars to process: %d", len(cars))
    if args.dry_run:
        logger.info("DRY-RUN mode — no DB writes")

    stats = {"total": len(cars), "updated": 0, "no_epa": 0, "skipped": 0}
    updated_ids: list[int] = []

    from backend.db.inventory_pg import is_inventory_postgres

    logger.info(
        "inventory backend: %s",
        "Postgres"
        if is_inventory_postgres()
        else os.path.abspath(os.environ.get("INVENTORY_DB_PATH") or _default_db_path()),
    )

    vpic_by_vin: dict[str, dict] = {}
    for i, car in enumerate(cars):
        if i % 500 == 0:
            vpic_by_vin = vpic_contexts_for_vins([c.get("vin") for c in cars[i:i + 500]])
        updates = enrich_car(
            car, dry_run=args.dry_run, fill_all=args.all, use_vpic=not args.no_vpic,
            vpic_ctx=vpic_by_vin.get(str(car.get("vin") or "").strip().upper()),
        )
        stats["skipped"] += getattr(updates, "skipped", 0)
        if not updates:
            stats["no_epa"] += 1
            continue

        if not args.dry_run:
            apply_dictionary_updates(car, updates)
            updated_ids.append(int(car["id"]))

        stats["updated"] += 1
        fields_str = ", ".join(f"{k}={v}" for k, v in updates.items())
        logger.info("[%s] %s %s %s → %s", car["vin"], car["year"], car["make"], car["model"], fields_str)

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
    logger.info(
        "Complete: total=%d updated=%d no_epa_match=%d skipped_by_precedence=%d",
        stats["total"], stats["updated"], stats["no_epa"], stats["skipped"],
    )
    if stats["updated"]:
        logger.info("Optional: python rebuild_listings_index.py --fast (full resync if sync step failed)")


if __name__ == "__main__":
    main()
