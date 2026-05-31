"""
Resolve OEM trim ladder position for a listing (premium VDP feature).

Sources (first match wins):
1. ``trim_ladders.json`` curated ladders
2. ``trim_ladders_generated.json`` (built from dictionary CSVs)
3. ``*_DT_Complete_Options.csv`` dictionary files
4. Year-specific ``*_Make_Model_Complete_Options.csv`` (+ text extraction)
5. Active inventory for the same make/model (price-ordered trims)
6. Make-specific OEM trim knowledge (generic fallback; always shows a ladder)
"""
from __future__ import annotations

import csv
import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.utils.spec_field_normalize import normalize_trim_text

logger = logging.getLogger(__name__)

_DICTIONARY_DIR = Path(__file__).resolve().parent.parent / "dictionary"
_LADDERS_JSON = _DICTIONARY_DIR / "trim_ladders.json"
_LADDERS_GENERATED_JSON = _DICTIONARY_DIR / "trim_ladders_generated.json"

_TRIM_NOISE_RE = re.compile(
    r"\s+(?:AWD|FWD|RWD|4WD|4X4|4X2|"
    r"\d+[\s-]*SPEED\s+AUTOMATIC|"
    r"\d+[\s-]*SPEED|"
    r"AUTOMATIC|MANUAL|CVT|"
    r"SUPERCREW|SUPERCAB|CREW\s+CAB|QUAD\s+CAB|"
    r"REGULAR\s+CAB)\s*$",
    re.I,
)
_YEAR_SUFFIX_RE = re.compile(r"\s*\(\d{4}\+\)\s*$")
_NON_TRIM_RE = re.compile(
    r"^(?:wheelbase|trim levels?|engine|transmission|varies\b|model\b|standard\b|optional\b)",
    re.I,
)
_JUNK_TRIM_TOKENS = frozenset(
    {
        "fwd",
        "4wd",
        "2wd",
        "awd",
        "rwd",
        "4x4",
        "4x2",
        "2wd",
        "hybrid",
        "electric",
        "gasoline",
        "diesel",
    }
)


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _norm_make(s: str) -> str:
    t = _norm_token(s)
    if t in ("chevrolet", "chevy"):
        return "chevrolet"
    if t in ("mercedesbenz", "mercedes"):
        return "mercedesbenz"
    return t


def _norm_model(s: str) -> str:
    raw = (s or "").strip()
    raw = re.sub(r"^ram\s+", "", raw, flags=re.I)
    raw = re.sub(r"\s*\(dt\)\s*$", "", raw, flags=re.I)
    raw = re.sub(r"\s+", " ", raw).strip()
    return _norm_token(raw)


def _normalize_listing_model(make: str, model: str) -> str:
    """Strip duplicated make tokens from listing model labels."""
    m = (model or "").strip()
    if not m:
        return m
    make_label = (make or "").strip()
    if make_label and m.lower().startswith(make_label.lower() + " "):
        m = m[len(make_label) :].strip()
    if _norm_make(make) == "toyota" and m.lower().startswith("toyota "):
        m = m[7:].strip()
    return m


def _filename_token(s: str) -> str:
    return re.sub(r"[^\w]+", "_", (s or "").strip()).strip("_")


def _clean_trim_label(trim: str) -> str:
    t = str(trim or "").strip()
    if "/" in t:
        parts = [p.strip() for p in t.split("/") if p.strip()]
        if parts and len(set(p.lower() for p in parts)) == 1:
            t = parts[0]
    t = _YEAR_SUFFIX_RE.sub("", t).strip()
    t = _TRIM_NOISE_RE.sub("", t).strip()
    normalized = normalize_trim_text(t)
    return (normalized or t).strip()


def _is_valid_trim_name(name: str, *, make: str = "", model: str = "") -> bool:
    from backend.enrichment.trim_ladder_knowledge import (
        canonical_trim_name,
        preserve_trim_label,
        trim_name_is_acceptable,
    )

    canonical = canonical_trim_name(name, make, model) if make else name
    preserved = preserve_trim_label(name, make, model) if make else ""
    label = canonical or preserved or name
    if not label:
        return False
    return trim_name_is_acceptable(label, make, model) if make else bool(label)


def _extract_trim_from_cell(trim_raw: str, make: str, model: str) -> str | None:
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    t = trim_raw.strip()
    if not t:
        return None
    prefixes = [
        f"{make} {model}",
        f"{make} {model.replace('_', ' ')}",
        model,
        make,
    ]
    for prefix in prefixes:
        p = prefix.strip()
        if not p:
            continue
        if t.lower().startswith(p.lower()):
            t = t[len(p) :].strip(" -–—")
    preserved = preserve_trim_label(t, make, model)
    if preserved:
        return preserved
    name = canonical_trim_name(_clean_trim_label(t) or t, make, model)
    return name if _is_valid_trim_name(name, make=make, model=model) else None


_YEAR_MIN_SUFFIX_RE = re.compile(r"\((20\d{2})\+\)", re.I)
_YEAR_RANGE_SUFFIX_RE = re.compile(r"\((20\d{2})\s*[–-]\s*(20\d{2})\)", re.I)


def _clean_trim_step_display_name(name: str) -> str:
    n = _YEAR_MIN_SUFFIX_RE.sub("", name or "")
    n = _YEAR_RANGE_SUFFIX_RE.sub("", n)
    return re.sub(r"\s{2,}", " ", n).strip()


def _step_applies_to_year(
    step: dict[str, Any],
    year: Any,
    *,
    make: str | None = None,
    model: str | None = None,
) -> bool:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return True
    name = str(step.get("name") or "")
    display_name = _clean_trim_step_display_name(name)
    ymin = int(step.get("year_min") or 0)
    ymax = int(step.get("year_max") or 9999)
    m_min = _YEAR_MIN_SUFFIX_RE.search(name)
    if m_min:
        ymin = max(ymin, int(m_min.group(1)))
    m_range = _YEAR_RANGE_SUFFIX_RE.search(name)
    if m_range:
        ymin = max(ymin, int(m_range.group(1)))
        ymax = min(ymax, int(m_range.group(2)))
    if not (ymin <= y <= ymax):
        return False
    if make and model and display_name:
        from backend.enrichment.trim_ladder_knowledge import trim_step_year_windows

        windows = trim_step_year_windows(make, model, display_name)
        if windows:
            return any(lo <= y <= hi for lo, hi in windows)
    return True


def _filter_ladder_steps_for_year(
    steps: list[dict[str, Any]],
    year: Any,
    *,
    make: str | None = None,
    model: str | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for step in steps or []:
        if not _step_applies_to_year(step, year, make=make, model=model):
            continue
        cleaned = dict(step)
        cleaned["name"] = _clean_trim_step_display_name(str(cleaned.get("name") or ""))
        cleaned["aliases"] = [
            _clean_trim_step_display_name(str(a))
            for a in (cleaned.get("aliases") or [])
            if str(a).strip()
        ]
        out.append(cleaned)
    return out


def _trim_match_score(
    listing_trim: str,
    step_name: str,
    aliases: list[str],
    *,
    make: str,
    model: str | None = None,
) -> int:
    """Higher score = better match. 0 = no match."""
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    lt = (
        canonical_trim_name(listing_trim, make, model)
        or preserve_trim_label(listing_trim, make, model)
        or _clean_trim_label(listing_trim)
    )
    if not lt:
        return 0
    lt_norm = _norm_token(lt)
    package_listing = bool(re.search(r"\bpackage\b", lt, re.I))
    candidates = [step_name, *aliases]
    best = 0
    if _norm_make(make) == "audi":
        from backend.enrichment.trim_ladder_knowledge import audi_dealer_trim_tokens

        for token in audi_dealer_trim_tokens(listing_trim):
            tn = _norm_token(token)
            for cand in candidates:
                c = (
                    canonical_trim_name(cand, make, model)
                    or preserve_trim_label(cand, make, model)
                    or _clean_trim_label(cand)
                )
                if not c:
                    continue
                cn = _norm_token(c)
                if tn == cn:
                    best = max(best, 95)
                elif cn in tn or tn in cn:
                    best = max(best, 85)
    for cand in candidates:
        c = (
            canonical_trim_name(cand, make, model)
            or preserve_trim_label(cand, make, model)
            or _clean_trim_label(cand)
        )
        if not c:
            continue
        cn = _norm_token(c)
        if not cn:
            continue
        if package_listing and cn != lt_norm:
            continue
        if lt_norm == cn:
            return 100
        if lt_norm.startswith(cn) or cn.startswith(lt_norm):
            best = max(best, 80)
        if cn in lt_norm or lt_norm in cn:
            best = max(best, 60)
        if re.search(rf"\b{re.escape(c)}\b", lt, re.I):
            best = max(best, 70)
    return best


def _row_trim_adds(row: dict[str, str]) -> list[str]:
    """Extract display-worthy feature bullets from a Complete_Options CSV row."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add

    out: list[str] = []
    pkg = (row.get("Packages") or "").strip()
    pkg_d = (row.get("packageDetails") or "").strip()
    opt = (row.get("Options") or "").strip()
    opt_d = (row.get("optionDetails") or "").strip()
    if pkg and pkg_d and len(pkg_d) >= 20 and not is_generic_trim_add(pkg_d):
        out.append(f"{pkg}: {pkg_d}"[:240])
    elif pkg and len(pkg) >= 12 and not is_generic_trim_add(pkg):
        out.append(pkg[:240])
    if opt and opt_d and len(opt_d) >= 20 and not is_generic_trim_add(opt_d):
        out.append(f"{opt}: {opt_d}"[:240])
    elif opt and len(opt) >= 12 and not is_generic_trim_add(opt):
        out.append(opt[:240])
    return out[:4]


def _dictionary_adds_by_trim(make: str, model: str, year: Any) -> dict[str, list[str]]:
    """Load trim→feature bullets from the best Complete_Options CSV for this vehicle."""
    csv_path = _find_complete_options_csv(make, model, year)
    if not csv_path:
        return {}
    try:
        with csv_path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return {}
    out: dict[str, list[str]] = {}
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        name = _extract_trim_from_cell(trim_raw, make, model)
        if not name:
            continue
        adds = _row_trim_adds(row)
        if not adds:
            continue
        key = _norm_token(name)
        if key not in out:
            out[key] = adds
    return out


def _build_ladder_result(
    ladder_def: dict[str, Any],
    *,
    make: str,
    model: str,
    year: Any,
    trim: str | None,
) -> dict[str, Any]:
    steps_def = ladder_def.get("steps") or []
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    listing_trim = (
        canonical_trim_name(trim or "", make, model)
        or preserve_trim_label(trim or "", make, model)
        or _clean_trim_label(trim or "")
        or "—"
    )
    current_index = -1
    best_score = 0
    for i, step in enumerate(steps_def):
        score = _trim_match_score(
            listing_trim,
            str(step.get("name") or ""),
            list(step.get("aliases") or []),
            make=make,
            model=model,
        )
        if score > best_score:
            best_score = score
            current_index = i

    steps_out: list[dict[str, Any]] = []
    dict_adds = _dictionary_adds_by_trim(make, model, year)
    for i, step in enumerate(steps_def):
        name = str(step.get("name") or "").strip()
        raw_adds = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
        from backend.enrichment.trim_ladder_knowledge import sanitize_trim_adds

        adds = sanitize_trim_adds(raw_adds, name)
        if not adds:
            adds = dict_adds.get(_norm_token(name), [])
        is_current = i == current_index and best_score > 0
        # Steps are luxury-first (index 0 = top). Below current = more base; above = more luxury.
        is_passed = current_index >= 0 and i > current_index
        steps_out.append(
            {
                "name": name,
                "index": i,
                "is_current": is_current,
                "is_passed": is_passed,
                "is_ahead": current_index >= 0 and i < current_index,
                "adds": adds,
                "side": "left" if i % 2 == 0 else "right",
            }
        )

    source = ladder_def.get("source") or ladder_def.get("id") or ""
    result = {
        "id": ladder_def.get("id"),
        "label": ladder_def.get("label") or f"{make} {model} trim lineup",
        "make": make,
        "model": model,
        "year": year,
        "listing_trim": listing_trim,
        "current_index": current_index if best_score > 0 else None,
        "matched": best_score > 0,
        "source": source,
        "steps": steps_out,
    }
    result["quality"] = _trim_ladder_quality(result, make, model)
    return result


def _trim_ladder_quality(result: dict[str, Any], make: str, model: str) -> str:
    """high | medium | low — used for display gating and UI confidence."""
    source = str(result.get("source") or "").lower()
    matched = bool(result.get("matched"))
    names = {str(s.get("name") or "").lower() for s in result.get("steps") or [] if s.get("name")}
    generic_cross_make = frozenset({"platinum", "limited", "premium", "touring", "xle", "sport", "base"})
    if _norm_make(make) not in {"toyota", "lexus", "scion"} and names and names <= generic_cross_make:
        return "low"
    if not matched:
        if source == "curated":
            return "medium"
        if "complete_options" in source or "epa" in source or source.endswith(".csv"):
            return "medium"
        return "low"
    if source == "curated":
        return "high"
    if source in {"inventory", "oem_knowledge"}:
        return "medium"
    if "epa" in source or source.endswith("_epa.csv"):
        return "medium"
    return "medium"


def _trim_ladder_should_display(result: dict[str, Any], make: str, model: str) -> bool:
    steps = result.get("steps") or []
    if len(steps) < 2:
        return False
    step_dicts = [{"name": s.get("name")} for s in steps]
    if not _ladder_steps_plausible_for_model(step_dicts, make, model):
        return False
    if _trim_ladder_quality(result, make, model) == "low":
        return False
    if not result.get("matched"):
        names = {str(s.get("name") or "").lower() for s in steps}
        if names <= {"sport", "base"} or names <= {"sport", "base", "s3 sportback"}:
            return False
    return True


def _read_ladders_file(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    ladders = data.get("ladders")
    return ladders if isinstance(ladders, list) else []


@lru_cache(maxsize=1)
def _load_json_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_JSON)


@lru_cache(maxsize=1)
def _load_generated_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_GENERATED_JSON)


def _steps_from_csv_rows(
    rows: list[dict[str, str]],
    make: str,
    model: str,
) -> list[dict[str, Any]]:
    from backend.enrichment.trim_ladder_knowledge import extract_trims_from_text, merge_trim_names

    blob_parts: list[str] = []
    for row in rows:
        for key in ("Trim", "Packages", "packageDetails", "Options", "optionDetails"):
            val = (row.get(key) or "").strip()
            if val:
                blob_parts.append(val)
    text_trims = extract_trims_from_text(make, "\n".join(blob_parts), model=model)

    steps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        step_year_min = 0
        step_year_max = 9999
        m_min = _YEAR_MIN_SUFFIX_RE.search(trim_raw)
        if m_min:
            step_year_min = int(m_min.group(1))
        m_range = _YEAR_RANGE_SUFFIX_RE.search(trim_raw)
        if m_range:
            step_year_min = max(step_year_min, int(m_range.group(1)))
            step_year_max = int(m_range.group(2))
        name = _extract_trim_from_cell(trim_raw, make, model)
        if not name:
            continue
        key = _norm_token(name)
        if key in seen:
            continue
        seen.add(key)

        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": _row_trim_adds(row) if isinstance(row, dict) else [],
                **({"year_min": step_year_min} if step_year_min else {}),
                **({"year_max": step_year_max} if step_year_max < 9999 else {}),
            }
        )

    if len(steps) < 2 and text_trims:
        for name in merge_trim_names([s["name"] for s in steps], text_trims, make=make, model=model):
            if not _is_valid_trim_name(name, make=make):
                continue
            key = _norm_token(name)
            if key in seen:
                continue
            seen.add(key)
            steps.append(
                {
                    "name": name,
                    "aliases": [],
                    "adds": [],
                }
            )
    from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps

    return normalize_ladder_steps(steps, make, model=model)


def _epa_models_match(car_model: str, row_model: str) -> bool:
    """True when an EPA CSV row model corresponds to the listing model."""
    cm = _norm_model(car_model)
    rm = _norm_model(row_model)
    if not cm or not rm:
        return False
    if cm == rm:
        return True
    if cm.startswith(rm) or rm.startswith(cm):
        return True
    if cm.endswith("class") and cm[:-5] == rm:
        return True
    if rm.endswith("class") and rm[:-5] == cm:
        return True
    if cm.replace("hybrid", "") == rm.replace("hybrid", ""):
        return True
    if cm.startswith("r8") and rm == "r8":
        return True
    if cm.startswith("r8spyder") and rm in {"r8", "r8spyder"}:
        return True
    if cm.startswith("rs6") and rm in {"rs6", "rs"}:
        return True
    return False


def _normalize_epa_trim(trim_raw: str, make: str, model: str) -> str | None:
    """Extract a display trim from EPA CSV cells (often verbose)."""
    from backend.enrichment.trim_ladder_knowledge import preserve_trim_label

    raw = str(trim_raw or "").strip()
    if not raw:
        return None

    preserved = preserve_trim_label(raw, make, model)
    if preserved:
        return preserved

    t = re.sub(r"\s*\([^)]*\)\s*", " ", raw).strip()
    t = re.sub(r"\s+", " ", t)
    if not t or t.startswith("("):
        return None

    if _norm_model(t) == _norm_model(model):
        preserved = preserve_trim_label(raw, make, model)
        if not preserved:
            return None
        t = preserved

    if _norm_make(make) == "jeep" and t.upper() in {"2WD", "4WD"}:
        return None
    if re.fullmatch(r"L\s*\dWD", t, re.I):
        return None

    preserved = preserve_trim_label(t, make, model)
    if preserved:
        return preserved

    name = _extract_trim_from_cell(t, make, model)
    return name


def _find_epa_csv(make: str, model: str, year: Any) -> Path | None:
    """Best-match ``{year}_{Make}_{Model}_EPA.csv`` file (fuzzy model aliases)."""
    from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

    model_label = epa_model_search_name(make, model)
    make_t = _filename_token(make)
    model_t = _filename_token(model_label)
    if not make_t or not model_t:
        return None

    found: list[Path] = []
    patterns = [
        f"*_{make_t}_{model_t}_EPA.csv",
        f"*_{make_t}_{model_t.replace('_', '')}_EPA.csv",
        f"*_{make_t}_{model.replace(' ', ' ')}*_EPA.csv",
    ]
    for pat in patterns:
        found.extend(_DICTIONARY_DIR.glob(pat))

    try:
        target_year = int(year)
    except (TypeError, ValueError):
        target_year = None

    def year_from_path(p: Path) -> int | None:
        m = re.match(r"^(\d{4})_", p.name)
        return int(m.group(1)) if m else None

    if found:
        if target_year is not None:
            found.sort(
                key=lambda p: (
                    abs((year_from_path(p) or target_year) - target_year),
                    p.name,
                )
            )
        else:
            found.sort(key=lambda p: p.name, reverse=True)
        return found[0]

    car_model_norm = _norm_model(model_label)
    make_norm = _norm_make(make)
    fuzzy: list[tuple[int, Path]] = []
    for path in _DICTIONARY_DIR.glob("*_EPA.csv"):
        m = re.match(r"^(\d{4})_(.+)_EPA$", path.stem)
        if not m:
            continue
        file_year = int(m.group(1))
        rest = m.group(2)
        if make_norm not in _norm_make(rest.replace("_", " ")):
            continue
        file_model = rest
        for prefix in (make, make.replace("-", " ")):
            for sep in ("_", " "):
                head = prefix.replace(" ", sep)
                if rest.lower().startswith(head.lower() + sep):
                    file_model = rest[len(head) + 1 :]
                    break
        file_model_norm = _norm_model(file_model.replace("_", " "))
        if not _epa_models_match(model, file_model.replace("_", " ")):
            continue
        year_dist = abs(file_year - target_year) if target_year is not None else 0
        name_dist = 0 if car_model_norm == file_model_norm else 1
        fuzzy.append((year_dist + name_dist, path))

    if not fuzzy:
        return None
    fuzzy.sort(key=lambda item: (item[0], item[1].name))
    return fuzzy[0][1]


def _ladder_from_epa_csv(path: Path, make: str, model: str, year: Any) -> dict[str, Any] | None:
    from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

    model_label = epa_model_search_name(make, model)
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None

    steps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        trim_raw = (row.get("Trim") or "").strip()
        if not trim_raw:
            continue
        row_make = (row.get("Make") or make or "").strip()
        row_model = (row.get("Model") or model or "").strip()
        if _norm_make(row_make) != _norm_make(make):
            continue
        if not _epa_models_match(model_label, row_model):
            continue
        name = _normalize_epa_trim(trim_raw, make, model)
        if not name:
            continue
        if not _is_valid_trim_name(name, make=make, model=model):
            continue
        key = _norm_token(name)
        if key in seen:
            continue
        seen.add(key)
        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": [],
            }
        )

    if len(steps) < 2:
        return None

    y_match = re.match(r"^(\d{4})_", path.name)
    yr = int(y_match.group(1)) if y_match else None
    ladder = {
        "id": path.stem.lower(),
        "make": make,
        "models": [model],
        "year_min": (yr - 2) if yr else 0,
        "year_max": (yr + 2) if yr else 9999,
        "label": f"{make} {model} trim lineup",
        "source": path.name,
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, make, model)


def _ladder_from_dt_csv(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None
    if not rows:
        return None

    make = ""
    model = ""
    year_min: int | None = None
    year_max: int | None = None
    for row in rows:
        if row.get("Make"):
            make = str(row["Make"]).strip()
        if row.get("Model"):
            model = str(row["Model"]).strip()
        yr_raw = (row.get("Year") or "").strip()
        if yr_raw.isdigit():
            yr = int(yr_raw)
            year_min = yr if year_min is None else min(year_min, yr)
            year_max = yr if year_max is None else max(year_max, yr)

    steps = _steps_from_csv_rows(rows, make, model)
    if not make or not model or len(steps) < 2:
        return None

    stem = path.stem.replace("_Complete_Options", "")
    ladder = {
        "id": stem.lower(),
        "make": make,
        "models": [model, re.sub(r"\s*\(dt\)\s*", "", model, flags=re.I).strip()],
        "year_min": year_min or 0,
        "year_max": year_max or 9999,
        "label": f"{make} {model} trim lineup",
        "source": path.name,
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, make, model)


def _find_complete_options_csv(make: str, model: str, year: Any) -> Path | None:
    """Best-match ``{year}_{Make}_{Model}_Complete_Options.csv`` file."""
    make_t = _filename_token(make)
    model_t = _filename_token(model)
    if not make_t or not model_t:
        return None

    patterns = [
        f"*_{make_t}_{model_t}_Complete_Options.csv",
        f"*_{make_t}_{model_t.replace('_', '')}_Complete_Options.csv",
    ]
    found: list[Path] = []
    for pat in patterns:
        found.extend(_DICTIONARY_DIR.glob(pat))
    if not found:
        loose = _filename_token(model.replace("-", " "))
        if loose and loose != model_t:
            found.extend(_DICTIONARY_DIR.glob(f"*_{make_t}_{loose}_Complete_Options.csv"))
    if not found:
        return None

    try:
        target_year = int(year)
    except (TypeError, ValueError):
        target_year = None

    def year_from_path(p: Path) -> int | None:
        m = re.match(r"^(\d{4})_", p.name)
        return int(m.group(1)) if m else None

    if target_year is not None:
        found.sort(
            key=lambda p: (
                abs((year_from_path(p) or target_year) - target_year),
                p.name,
            )
        )
    else:
        found.sort(key=lambda p: p.name, reverse=True)
    return found[0]


def _ladder_from_complete_options_csv(path: Path, make: str, model: str, year: Any) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None
    csv_make = make
    csv_model = model
    for row in rows:
        if row.get("Make"):
            csv_make = str(row["Make"]).strip() or csv_make
        if row.get("Model"):
            csv_model = str(row["Model"]).strip() or csv_model
        if csv_make and csv_model:
            break

    steps = _steps_from_csv_rows(rows, csv_make, csv_model)
    if not _ladder_steps_usable(steps, csv_make):
        return None
    try:
        blob = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        blob = ""
    if _complete_options_ladder_is_junk(steps, csv_make, csv_model, source_blob=blob):
        return None

    y_match = re.match(r"^(\d{4})_", path.name)
    yr = int(y_match.group(1)) if y_match else None
    ladder = {
        "id": path.stem.lower(),
        "make": csv_make,
        "models": [csv_model, model],
        "year_min": (yr - 2) if yr else 0,
        "year_max": (yr + 2) if yr else 9999,
        "label": f"{csv_make} {csv_model} trim lineup",
        "source": path.name,
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, csv_make, model)


def _inventory_trim_rows(
    make_norm: str,
    model_label: str,
    year_lo: int,
    year_hi: int,
) -> list[Any]:
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT trim, AVG(price) AS avg_price, COUNT(*) AS n
            FROM cars
            WHERE (COALESCE(listing_active, 1) = 1)
              AND LOWER(TRIM(make)) = LOWER(?)
              AND LOWER(TRIM(model)) = LOWER(?)
              AND year BETWEEN ? AND ?
              AND trim IS NOT NULL
              AND TRIM(trim) != ''
              AND price IS NOT NULL
              AND price > 0
            GROUP BY LOWER(TRIM(trim))
            ORDER BY avg_price ASC
            LIMIT 24
            """,
            (make_norm, model_label, year_lo, year_hi),
        )
        return cur.fetchall()


def _ladder_from_inventory(make: str, model: str, year: Any) -> dict[str, Any] | None:
    """Build a price-ordered trim ladder from active inventory."""
    try:
        y = int(year)
        year_ranges = [(y - 3, y + 1), (y - 6, y + 2), (0, 9999)]
    except (TypeError, ValueError):
        year_ranges = [(0, 9999)]

    make_norm = (make or "").strip()
    model_norm = _norm_model(model)
    model_label = (model or "").strip()
    if not make_norm or not model_norm:
        return None

    rows: list[Any] = []
    for year_lo, year_hi in year_ranges:
        try:
            rows = _inventory_trim_rows(make_norm, model_label, year_lo, year_hi)
        except Exception as e:
            logger.debug("inventory trim ladder query failed: %s", e)
            return None
        if len(rows) >= 2:
            break

    steps: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            trim_raw = row.get("trim")
            avg_price = row.get("avg_price")
            count = row.get("n")
        else:
            trim_raw = row[0]
            avg_price = row[1]
            count = row[2]
        from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

        name = (
            preserve_trim_label(str(trim_raw or ""), make_norm, model_label)
            or canonical_trim_name(str(trim_raw or ""), make_norm, model_label)
            or _clean_trim_label(str(trim_raw or ""))
        )
        if not _is_valid_trim_name(name, make=make_norm, model=model_label):
            continue
        if _norm_model(name) == model_norm:
            continue
        try:
            price_note = f"Typical listing price near ${int(float(avg_price)):,} in our inventory ({int(count)} listings)."
        except (TypeError, ValueError):
            price_note = "Seen on similar listings in our inventory."
        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": [],
                "inventory_price_note": price_note,
            }
        )

    if len(steps) < 2:
        return None

    ymin = year_ranges[0][0] if year_ranges else 0
    ymax = year_ranges[0][1] if year_ranges else 9999
    for lo, hi in year_ranges:
        if rows:
            ymin, ymax = lo, hi
            break

    ladder = {
        "id": f"inventory_{_norm_make(make)}_{model_norm}",
        "make": make_norm,
        "models": [model, model_norm],
        "year_min": ymin,
        "year_max": ymax,
        "label": f"{make_norm} {model} trim lineup",
        "source": "inventory",
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, make_norm, model_label)


@lru_cache(maxsize=1)
def _all_ladder_defs() -> tuple[dict[str, Any], ...]:
    seen_ids: set[str] = set()
    merged: list[dict[str, Any]] = []
    for loader in (_load_json_ladders, _load_generated_ladders):
        for ladder in loader():
            if not isinstance(ladder, dict):
                continue
            lid = str(ladder.get("id") or "")
            if lid and lid not in seen_ids:
                seen_ids.add(lid)
                merged.append(ladder)
    for ladder in _load_dt_csv_ladders():
        if not isinstance(ladder, dict):
            continue
        lid = str(ladder.get("id") or "")
        if lid and lid not in seen_ids:
            seen_ids.add(lid)
            merged.append(ladder)
    return tuple(merged)


def _finalize_ladder_steps(ladder: dict[str, Any], make: str, model: str | None = None) -> dict[str, Any]:
    """Canonicalize, dedupe, and sort steps (luxury at top)."""
    from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps

    model_label = model or ((ladder.get("models") or [None])[0])
    steps = normalize_ladder_steps(ladder.get("steps") or [], make, model=str(model_label or ""))
    return {**ladder, "steps": steps}


def _ladder_steps_plausible_for_model(
    steps: list[dict[str, Any]],
    make: str,
    model: str,
) -> bool:
    """Reject ladders whose rungs clearly belong to a different vehicle line."""
    if len(steps) < 2:
        return False
    names = [str(s.get("name") or "").strip() for s in steps if str(s.get("name") or "").strip()]
    if not names:
        return False

    mk = _norm_make(make)
    mod = _norm_model(model)
    names_lower = {n.lower() for n in names}

    if mk == "bmw" and re.match(r"^x[1-7]$", mod):
        has_motor_id = any(
            re.search(r"[xs]Drive\d{2}[ie]|M\d{2,3}i|M60i|Alpina", n, re.I) or re.fullmatch(r"M", n.strip(), re.I)
            for n in names
        )
        package_lines = {"m sport", "xline", "luxury line", "sport line", "modern line", "executive", "premium"}
        if (names_lower & package_lines) and not has_motor_id:
            return False
        if has_motor_id:
            return True
        package_only = package_lines | {"m competition", "competition", "luxury", "base", "standard"}
        if names_lower and names_lower <= package_only:
            return False

    if mk == "bmw" and mod in {"i4", "i5", "ix", "i7"}:
        if any(re.search(r"edrive|xdrive|m60", n, re.I) for n in names):
            return True
        package_lines = {"m competition", "sport line", "luxury line", "modern line", "xline", "competition", "m sport"}
        if all(n.lower() in package_lines for n in names):
            return False

    if mk == "jeep" and mod == "wagoneer":
        if any(re.search(r"\bseries\b|\bcarbide\b|\bobsidian\b|\blaunch edition\b", n, re.I) for n in names):
            return True
        gc_only = {"summit reserve", "summit", "limited", "altitude", "premium", "base", "laredo"}
        if all(n.lower() in gc_only for n in names):
            return False

    if mk == "jeep" and mod == "grandwagoneer":
        has_series = any(
            re.search(r"\bseries\b|\bobsidian\b|\bcarbide\b|\bupland\b", n, re.I) for n in names
        )
        gc_bleed = {"limited", "altitude", "premium", "base", "laredo"}
        if (names_lower & gc_bleed) and not has_series:
            return False

    if mk == "toyota":
        truck_trims = {"capstone", "1794", "trd pro", "sr5", "sr", "platinum", "limited"}
        model_is_truck = any(tok in mod for tok in ("tundra", "tacoma", "sequoia", "highlander"))
        if not model_is_truck and truck_trims.issuperset(names_lower) and "1794" in names_lower:
            return False
        if not model_is_truck and names_lower & {"capstone", "1794"} and not names_lower & {"le", "se", "xle", "xse", "hybrid"}:
            if any(tok in names_lower for tok in ("capstone", "1794")):
                return False
        if mod in ("crownsignia", "crown") and names_lower & {"touring", "sport", "base"} and "xle" not in names_lower:
            return False
        if "corolla" in mod and "hybrid" in mod:
            if names_lower & {"hatchback", "hatchback xse", "hatchback fx", "gr corolla"}:
                return False
            hatch_only = {"hatchback", "hatchback xse", "hatchback fx", "gr corolla", "hybrid se", "hybrid"}
            if names_lower and names_lower <= hatch_only:
                return False

    if mk == "mercedesbenz":
        has_model_trim = any(re.search(r"\b\d{3}\b|AMG", n, re.I) for n in names)
        generic_only = {"maybach", "amg", "amg line", "premium plus", "premium", "night edition", "exclusive", "base"}
        if not has_model_trim and names_lower <= generic_only:
            return False

    generic_cross_make = {"platinum", "limited", "premium", "touring", "xle", "sport", "base"}
    if mk not in {"toyota", "lexus", "scion"} and names_lower and names_lower <= generic_cross_make:
        return False

    return True


def _complete_options_ladder_is_junk(
    steps: list[dict[str, Any]],
    make: str,
    model: str,
    *,
    source_blob: str = "",
) -> bool:
    """Reject Wikipedia scrapes and cross-make generic trim ladders from Complete_Options CSVs."""
    if len(steps) < 2:
        return True
    if not _ladder_steps_plausible_for_model(steps, make, model):
        return True

    names = [str(s.get("name") or "").strip() for s in steps if str(s.get("name") or "").strip()]
    if not names:
        return True

    junk_markers = (
        "predecessor ",
        "wikipedia",
        "internet movie",
        "wheelbase swb",
        "transporter film",
        "imdb",
        "authority control",
    )
    blob = (source_blob or "").lower()
    if any(marker in blob for marker in junk_markers):
        return True

    long_names = sum(1 for n in names if len(n) > 55)
    if long_names >= max(1, len(names) // 2):
        return True

    from backend.enrichment.trim_ladder_knowledge import trim_name_is_acceptable

    bad_names = sum(1 for n in names if not trim_name_is_acceptable(n, make, model))
    if bad_names >= max(1, len(names) // 2):
        return True

    tesla_junk = {"performance", "standard range", "base"}
    names_lower = {n.lower() for n in names}
    if names_lower and names_lower <= tesla_junk:
        return True

    return False


def _ladder_steps_usable(
    steps: list[dict[str, Any]],
    make: str,
    *,
    model: str | None = None,
    min_steps: int = 2,
) -> bool:
    from backend.enrichment.trim_ladder_knowledge import luxury_rank

    if len(steps) < min_steps:
        return False
    known = sum(
        1 for s in steps if luxury_rank(str(s.get("name") or ""), make, model) < 8_000
    )
    if known < min_steps:
        return False
    if any(luxury_rank(str(s.get("name") or ""), make, model) >= 8_000 for s in steps):
        return False
    return True


def _load_dt_csv_ladders() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(_DICTIONARY_DIR.glob("*_DT_Complete_Options.csv")):
        ladder = _ladder_from_dt_csv(path)
        if ladder:
            out.append(ladder)
    return out


def _year_in_range(year: Any, ladder: dict[str, Any]) -> bool:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return True
    ymin = int(ladder.get("year_min") or 0)
    ymax = int(ladder.get("year_max") or 9999)
    return ymin <= y <= ymax


def _ladder_matches_car(ladder: dict[str, Any], make: str, model: str, year: Any) -> bool:
    if _norm_make(ladder.get("make") or "") != _norm_make(make):
        return False
    if not _year_in_range(year, ladder):
        return False
    car_model = _norm_model(model)
    if not car_model:
        return False
    for m in ladder.get("models") or []:
        lm = _norm_model(str(m))
        if not lm:
            continue
        if car_model == lm:
            return True
        # Listing model may be more specific than ladder key (e.g. "1500" vs "1500 Classic").
        if len(lm) >= 3 and car_model.startswith(lm):
            return True
    return False


def _pick_curated_ladder(make: str, model: str, year: Any) -> dict[str, Any] | None:
    matches = [lad for lad in _all_ladder_defs() if _ladder_matches_car(lad, make, model, year)]
    if not matches:
        return None
    try:
        y = int(year)
    except (TypeError, ValueError):
        return matches[0]

    def year_dist(lad: dict[str, Any]) -> int:
        ymin = int(lad.get("year_min") or 0)
        ymax = int(lad.get("year_max") or 9999)
        if ymin <= y <= ymax:
            return 0
        return min(abs(y - ymin), abs(y - ymax))

    def source_rank(lad: dict[str, Any]) -> int:
        src = str(lad.get("source") or "").lower()
        if src == "curated":
            return 0
        if src == "inventory":
            return 3
        return 2

    matches.sort(key=lambda lad: (year_dist(lad), source_rank(lad)))
    return matches[0]


def _generic_trim_ladder_def(make: str, model: str, year: Any) -> dict[str, Any]:
    from backend.enrichment.trim_ladder_knowledge import generic_fallback_steps

    try:
        y = int(year)
        ymin, ymax = y - 5, y + 2
    except (TypeError, ValueError):
        ymin, ymax = 0, 9999
    return {
        "id": f"generic_{_norm_make(make)}_{_norm_model(model)}",
        "make": make,
        "models": [model, _norm_model(model)],
        "year_min": ymin,
        "year_max": ymax,
        "label": f"{make} {model} trim lineup",
        "source": "oem_knowledge",
        "steps": generic_fallback_steps(make, model),
    }


def _pick_ladder_def(make: str, model: str, year: Any) -> dict[str, Any] | None:
    curated = _pick_curated_ladder(make, model, year)
    if curated:
        finalized = _finalize_ladder_steps(curated, make, model)
        if _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model):
            return finalized

    epa_path = _find_epa_csv(make, model, year)
    if epa_path:
        epa_ladder = _ladder_from_epa_csv(epa_path, make, model, year)
        if epa_ladder and _ladder_steps_plausible_for_model(epa_ladder.get("steps") or [], make, model):
            return epa_ladder

    csv_path = _find_complete_options_csv(make, model, year)
    if csv_path:
        try:
            csv_blob = csv_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            csv_blob = ""
        ladder = _ladder_from_complete_options_csv(csv_path, make, model, year)
        if ladder and _ladder_matches_car(ladder, make, model, year):
            finalized = _finalize_ladder_steps(ladder, make, model)
            if (
                _ladder_steps_usable(finalized.get("steps") or [], make, model=model)
                and _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model)
                and not _complete_options_ladder_is_junk(
                    finalized.get("steps") or [], make, model, source_blob=csv_blob
                )
            ):
                return finalized
        blob = csv_blob
        from backend.enrichment.trim_ladder_knowledge import extract_trims_from_text, merge_trim_names

        col_trims = []
        try:
            with csv_path.open(encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    raw = (row.get("Trim") or "").strip()
                    if not raw:
                        continue
                    name = _extract_trim_from_cell(raw, make, model)
                    if name and _is_valid_trim_name(name, make=make, model=model):
                        col_trims.append(name)
        except OSError:
            pass
        merged = merge_trim_names(
            col_trims,
            extract_trims_from_text(make, blob, model=model),
            make=make,
            model=model,
        )
        if len(merged) >= 3:
            ladder = {
                "id": csv_path.stem.lower(),
                "make": make,
                "models": [model],
                "year_min": int(year) - 2 if str(year).isdigit() else 0,
                "year_max": int(year) + 2 if str(year).isdigit() else 9999,
                "label": f"{make} {model} trim lineup",
                "source": csv_path.name,
                "steps": [
                    {
                        "name": n,
                        "aliases": [],
                        "adds": [],
                    }
                    for n in merged
                ],
            }
            finalized = _finalize_ladder_steps(ladder, make, model)
            if (
                _ladder_steps_usable(finalized.get("steps") or [], make, model=model)
                and _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model)
                and not _complete_options_ladder_is_junk(
                    finalized.get("steps") or [], make, model, source_blob=blob
                )
            ):
                return finalized

    inventory = _ladder_from_inventory(make, model, year)
    if inventory and _ladder_steps_usable(inventory.get("steps") or [], make, model=model):
        if _ladder_steps_plausible_for_model(inventory.get("steps") or [], make, model):
            return inventory

    return _generic_trim_ladder_def(make, model, year)


def resolve_trim_ladder(
    *,
    make: str | None,
    model: str | None,
    year: Any = None,
    trim: str | None,
) -> dict[str, Any] | None:
    """
    Return trim ladder context for a listing, or None if no ladder applies.
    """
    if not make or not model:
        return None
    model = _normalize_listing_model(make, model)
    ladder_def = _pick_ladder_def(make, model, year)
    if not ladder_def:
        ladder_def = _generic_trim_ladder_def(make, model, year)

    steps_def = ladder_def.get("steps") or []
    if len(steps_def) < 2:
        ladder_def = _generic_trim_ladder_def(make, model, year)
        steps_def = ladder_def.get("steps") or []

    steps_def = _filter_ladder_steps_for_year(steps_def, year, make=make, model=model)
    if len(steps_def) < 2:
        ladder_def = _generic_trim_ladder_def(make, model, year)
        steps_def = _filter_ladder_steps_for_year(
            ladder_def.get("steps") or [], year, make=make, model=model
        )

    result = _build_ladder_result(
        {**ladder_def, "steps": steps_def},
        make=make,
        model=model,
        year=year,
        trim=trim,
    )
    if not _trim_ladder_should_display(result, make, model):
        return None
    return result
