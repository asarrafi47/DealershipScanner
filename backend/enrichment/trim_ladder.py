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

from backend.enrichment.dictionary_catalog import find_complete_options_csv as _catalog_find_co
from backend.enrichment.dictionary_catalog import find_epa_csv as _catalog_find_epa
from backend.enrichment.dictionary_catalog import iter_dt_options_paths
from backend.enrichment.dictionary_paths import (
    DICTIONARY_ROOT as _DICTIONARY_DIR,
    trim_ladders_curated_path,
    trim_ladders_epa_path,
    trim_ladders_generated_path,
    trim_ladders_merged_path,
)

_LADDERS_JSON = trim_ladders_curated_path()
_LADDERS_GENERATED_JSON = trim_ladders_generated_path()
_LADDERS_EPA_JSON = trim_ladders_epa_path()
_LADDERS_MERGED_JSON = trim_ladders_merged_path()

_MIN_TRIM_LADDER_YEAR = 2010

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
    skip_trim_year_windows: bool = False,
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
    if skip_trim_year_windows:
        return True
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
    skip_trim_year_windows: bool = False,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for step in steps or []:
        if not _step_applies_to_year(
            step,
            year,
            make=make,
            model=model,
            skip_trim_year_windows=skip_trim_year_windows,
        ):
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


def _trim_identity_keys(label: str, make: str, model: str | None = None) -> set[str]:
    """Normalized keys for matching trim labels across inventory, brochure, and CSV sources."""
    from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

    keys: set[str] = set()
    raw = str(label or "").strip()
    if not raw:
        return keys
    candidates = [raw]
    if make:
        candidates.extend(
            [
                canonical_trim_name(raw, make, model),
                preserve_trim_label(raw, make, model),
                _clean_trim_label(raw),
            ]
        )
    for candidate in candidates:
        c = str(candidate or "").strip()
        if not c:
            continue
        keys.add(_norm_token(c))
        if make:
            canon = canonical_trim_name(c, make, model)
            if canon:
                keys.add(_norm_token(canon))
    return {k for k in keys if k}


def _lookup_brochure_adds_key(
    step_name: str,
    aliases: list[str],
    brochure_adds_by_trim: dict[str, list[str]],
    *,
    make: str,
    model: str,
) -> str | None:
    """Map a ladder step name to the matching ``adds_by_trim`` brochure key (fuzzy)."""
    if not brochure_adds_by_trim:
        return None
    name = str(step_name or "").strip()
    if not name:
        return None

    if name in brochure_adds_by_trim:
        return name

    lower_map = {str(k).strip().lower(): str(k).strip() for k in brochure_adds_by_trim}
    if name.lower() in lower_map:
        return lower_map[name.lower()]

    step_keys = _trim_identity_keys(name, make, model)
    for alias in aliases:
        step_keys |= _trim_identity_keys(alias, make, model)

    best_key: str | None = None
    best_score = 0
    for bkey in brochure_adds_by_trim:
        overlap = step_keys & _trim_identity_keys(bkey, make, model)
        if not overlap:
            continue
        score = max(len(k) for k in overlap)
        if score > best_score:
            best_score = score
            best_key = bkey
    if best_key:
        return best_key

    # Inventory labels like "XLE Premium" — match when every brochure-key token is a whole
    # token in the step name (avoids matching "LE" inside "XLE").
    step_tokens = set(re.findall(r"[a-z0-9]+", name.lower()))
    token_best: str | None = None
    token_best_len = 0
    for bkey in brochure_adds_by_trim:
        b_tokens = [t for t in re.findall(r"[a-z0-9]+", str(bkey).lower()) if t]
        if not b_tokens or not all(t in step_tokens for t in b_tokens):
            continue
        if len(b_tokens) > token_best_len:
            token_best_len = len(b_tokens)
            token_best = bkey
    return token_best


def _index_ladder_steps_by_trim(
    steps: list[dict[str, Any]],
    *,
    make: str,
    model: str,
) -> dict[str, dict[str, Any]]:
    """Index ladder steps by display name, alias, and normalized trim keys."""
    index: dict[str, dict[str, Any]] = {}
    for step in steps or []:
        name = str(step.get("name") or "").strip()
        if not name:
            continue
        index[name] = step
        index[name.lower()] = step
        for alias in step.get("aliases") or []:
            alias_s = str(alias).strip()
            if alias_s:
                index[alias_s] = step
                index[alias_s.lower()] = step
        for key in _trim_identity_keys(name, make, model):
            index[key] = step
    return index


def _find_ladder_step(
    index: dict[str, dict[str, Any]],
    trim_label: str,
    *,
    make: str,
    model: str,
) -> dict[str, Any] | None:
    label = str(trim_label or "").strip()
    if not label:
        return None
    if label in index:
        return index[label]
    if label.lower() in index:
        return index[label.lower()]
    for key in _trim_identity_keys(label, make, model):
        if key in index:
            return index[key]
    return None


def _step_trim_keys(step: dict[str, Any], *, make: str, model: str) -> set[str]:
    keys: set[str] = set()
    name = str(step.get("name") or "").strip()
    if name:
        keys |= _trim_identity_keys(name, make, model)
    for alias in step.get("aliases") or []:
        keys |= _trim_identity_keys(str(alias), make, model)
    return keys


def _inventory_listing_price_fallback(step: dict[str, Any]) -> list[str]:
    """Deprecated for trim panels — listing price is not shown as a trim 'add'."""
    return []


def _strip_inventory_price_adds(adds: list[str]) -> list[str]:
    return [
        a
        for a in adds
        if a and not re.search(r"\btypical listing price near \$", str(a), re.I)
    ]


def _is_mechanical_trim_bullet(text: str) -> bool:
    """Powertrain, chassis, and performance hardware (vs cabin/infotainment)."""
    from backend.enrichment.trim_spec_extractor import _classify_text

    s = str(text or "").strip()
    if not s:
        return False
    label = _classify_text(s)
    if label in {"Engine Options", "Maximum Towing Capacity"}:
        return True
    low = s.lower()
    mechanical_kw = (
        "engine",
        "hemi",
        "pentastar",
        "hurricane",
        "ecoboost",
        "powerboost",
        "v6",
        "v8",
        "i4",
        "i6",
        "turbo",
        "supercharged",
        "horsepower",
        " lb-ft",
        " hp",
        "torque",
        "transmission",
        "4wd",
        "4x4",
        "awd",
        "etorque",
        "transfer case",
        "differential",
        "bilstein",
        "monotube",
        "suspension",
        "shock",
        "damp",
        "brake",
        "rotor",
        "brembo",
        "skid plate",
        "tow hook",
        "towing capacity",
        " payload",
        "wide-body",
        "wide body",
        "flare",
        "desert-rated",
        "off-road suspension",
        "rock mode",
        "launch control",
        "quadra-lift",
        "quadra-trac",
        "air suspension",
        "exhaust",
        "cooling",
        "intercooler",
        "limited-slip",
        "locking rear",
        "trail rated",
    )
    return any(kw in low for kw in mechanical_kw)


def _comfort_bullet_priority(text: str) -> int:
    """Lower = show first in cabin/tech slots (deprioritize generic option packages)."""
    low = text.lower()
    if any(
        tok in low
        for tok in (
            "safety group",
            "parksense",
            "surround-view",
            "surround view",
            "blind spot",
            "remote start",
            "park assist",
            "driver assist",
            "forward collision",
        )
    ):
        return 2
    if any(tok in low for tok in ("equipment package", "package capabilities", "package includes")):
        return 1
    return 0


def _merge_trim_display_bullets(
    candidates: list[str],
    *,
    trim_name: str,
    make: str,
    model: str,
    year: Any,
    max_items: int = 8,
) -> list[str]:
    """Prefer engine/chassis bullets, then cabin tech — deduped."""
    from backend.enrichment.trim_ladder_knowledge import is_generic_trim_add, sanitize_trim_adds
    from backend.enrichment.trim_spec_extractor import (
        compact_trim_bullet,
        is_junk_spec_text,
        is_stale_listing_trim_prose,
        split_spec_value_parts,
    )

    mechanical: list[str] = []
    comfort: list[str] = []
    seen: set[str] = set()

    def _bullet_core(text: str) -> str:
        return re.sub(r"^[^:]+:\s*", "", text.strip(), count=1).lower()

    def _cores_too_similar(a: str, b: str) -> bool:
        if a == b:
            return True
        shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
        if shorter not in longer:
            return False
        if len(shorter) < 10:
            return False
        return len(shorter) / max(len(longer), 1) >= 0.72

    def _try_add(part: str) -> None:
        if not part:
            return
        if (
            is_stale_listing_trim_prose(part, make=make, model=model, year=year)
            or is_generic_trim_add(part, trim_name)
            or is_junk_spec_text(part)
        ):
            return
        if re.fullmatch(r"four[- ]wheel drive", part.strip(), re.I) and any(
            re.search(r"\b4wd\b|four[- ]wheel|awd\b|transfer case", s, re.I) for s in seen
        ):
            return
        core = _bullet_core(part)
        if part.lower() in seen or core in seen:
            return
        if any(_cores_too_similar(core, _bullet_core(existing)) for existing in seen):
            return
        seen.add(part.lower())
        seen.add(core)
        bucket = mechanical if _is_mechanical_trim_bullet(part) else comfort
        bucket.append(part)

    for bullet in candidates:
        compact = compact_trim_bullet(bullet, trim_name=trim_name, make=make, model=model, year=year)
        parts = split_spec_value_parts(compact or bullet, trim_name=trim_name) or (
            [compact] if compact else [str(bullet).strip()] if str(bullet).strip() else []
        )
        for part in parts:
            _try_add(part)

    mech = sanitize_trim_adds(mechanical, trim_name, max_items=6)
    comfort = sanitize_trim_adds(comfort, trim_name, max_items=6)
    comfort = [
        b
        for _, b in sorted(
            enumerate(comfort),
            key=lambda pair: (_comfort_bullet_priority(pair[1]), pair[0]),
        )
    ]
    if mech and comfort:
        # Reserve cabin/tech bullets when both powertrain and interior exist.
        mech_take = min(len(mech), max(4, max_items - 3))
        comfort_take = max(0, max_items - mech_take)
        return (mech[:mech_take] + comfort[:comfort_take])[:max_items]
    return (mech or comfort)[:max_items]


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
        if lt_norm.startswith(cn) and len(cn) >= 3:
            best = max(best, 80)
        elif cn.startswith(lt_norm) and len(lt_norm) >= 3:
            best = max(best, 80)
        elif len(lt_norm) <= 3 and len(cn) <= 3:
            if lt_norm.startswith(cn) or cn.startswith(lt_norm):
                suffix = cn[len(lt_norm) :] if cn.startswith(lt_norm) else lt_norm[len(cn) :]
                if not suffix or not suffix[0].isalnum():
                    best = max(best, 80)
        if cn in lt_norm or lt_norm in cn:
            if len(lt_norm) > 3 or len(cn) > 3 or lt_norm == cn:
                best = max(best, 60)
        if re.search(rf"\b{re.escape(c)}\b", lt, re.I):
            best = max(best, 70)
    if best < 100 and _norm_make(make) == "bmw":
        from backend.enrichment.trim_ladder_knowledge import drivetrain_merge_key

        lt_key = drivetrain_merge_key(lt, make, model)
        if lt_key:
            for cand in candidates:
                c = (
                    canonical_trim_name(cand, make, model)
                    or preserve_trim_label(cand, make, model)
                    or _clean_trim_label(cand)
                )
                if not c:
                    continue
                cn_key = drivetrain_merge_key(c, make, model)
                if cn_key and lt_key == cn_key:
                    return 100
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


def _merge_trim_spec_values(left: str, right: str) -> str:
    """Combine two spec values for the same label (e.g. RWD + xDrive engine options)."""
    a = (left or "").strip()
    b = (right or "").strip()
    if not a:
        return b
    if not b or a.lower() == b.lower():
        return a
    if b.lower() in a.lower():
        return a
    if a.lower() in b.lower():
        return b
    return f"{a}; {b}"


def _lookup_merged_trim_specs(
    step_name: str,
    aliases: list[str],
    *,
    make: str,
    model: str,
    year: Any,
    ladder_id: str,
    adds: list[str],
    dict_adds: dict[str, list[str]],
) -> list[dict[str, str]]:
    from backend.enrichment.trim_spec_sheets import lookup_trim_specs

    variant_names = [step_name] + [str(a).strip() for a in aliases if str(a).strip()]
    seen_labels: dict[str, dict[str, str]] = {}
    for i, variant in enumerate(variant_names):
        variant_adds = list(adds) if i == 0 else list(dict_adds.get(_norm_token(variant), []))
        part = lookup_trim_specs(
            variant,
            make=make,
            model=model,
            year=year,
            ladder_id=ladder_id,
            adds=variant_adds,
        )
        for row in part:
            label = str(row.get("label") or "").strip()
            value = str(row.get("value") or "").strip()
            if not label or not value:
                continue
            if label not in seen_labels:
                seen_labels[label] = {"label": label, "value": value}
            else:
                seen_labels[label]["value"] = _merge_trim_spec_values(
                    seen_labels[label]["value"],
                    value,
                )
    return list(seen_labels.values())


def _curated_adds_for_step(step: dict[str, Any], year: Any) -> list[str]:
    """Return ladder-step adds, optionally overridden by ``adds_from_year`` thresholds."""
    base = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
    by_year = step.get("adds_from_year")
    if not isinstance(by_year, dict):
        return base
    try:
        y = int(year)
    except (TypeError, ValueError):
        return base
    thresholds: list[int] = []
    for key in by_year:
        try:
            thresholds.append(int(key))
        except (TypeError, ValueError):
            continue
    for threshold in sorted(thresholds, reverse=True):
        if y >= threshold:
            vals = by_year.get(str(threshold), by_year.get(threshold))
            if isinstance(vals, list):
                picked = [str(a).strip() for a in vals if str(a).strip()]
                if picked:
                    return picked
    return base


def _build_ladder_result(
    ladder_def: dict[str, Any],
    *,
    make: str,
    model: str,
    year: Any,
    trim: str | None,
    brochure_overlay: dict[str, Any] | None = None,
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
    brochure_adds_by_trim: dict[str, list[str]] = {}
    if brochure_overlay:
        raw_b = brochure_overlay.get("adds_by_trim")
        if isinstance(raw_b, dict):
            brochure_adds_by_trim = {
                str(k).strip(): [str(a).strip() for a in v if str(a).strip()]
                for k, v in raw_b.items()
                if isinstance(v, list)
            }
    from backend.enrichment.trim_ladder_knowledge import (
        fallback_trim_step_adds,
        sanitize_brochure_trim_adds,
        sanitize_trim_adds,
    )

    for i, step in enumerate(steps_def):
        name = str(step.get("name") or "").strip()
        aliases = [str(a).strip() for a in (step.get("aliases") or []) if str(a).strip()]
        step_curated_adds = sanitize_trim_adds(
            _curated_adds_for_step(step, year),
            name,
            max_items=12,
        )
        brochure_key = _lookup_brochure_adds_key(
            name,
            aliases,
            brochure_adds_by_trim,
            make=make,
            model=model,
        )
        from_brochure = brochure_key is not None
        neighbor_brochure_adds: list[str] = []
        # Neighbor-year overlay backfill only when this step has thin curated content —
        # avoids copying another trim's option-package lines onto performance trims (e.g. TRX).
        if not from_brochure and brochure_overlay and len(step_curated_adds) < 3:
            try:
                y_int = int(year)
            except (TypeError, ValueError):
                y_int = int(brochure_overlay.get("year") or 0)
            if y_int:
                from backend.enrichment.brochure_overlay_backfill import _neighbor_adds

                neighbor_brochure_adds = _neighbor_adds(y_int, make, model, name)
                if neighbor_brochure_adds:
                    from_brochure = True
        ladder_source_lower = str(ladder_def.get("source") or "").lower()
        brochure_lines: list[str] = []
        if from_brochure:
            brochure_lines = list(
                brochure_adds_by_trim.get(brochure_key or "", []) or neighbor_brochure_adds
            )
            if brochure_overlay and len(sanitize_brochure_trim_adds(brochure_lines, name)) < 3:
                try:
                    y_int = int(year)
                except (TypeError, ValueError):
                    y_int = int(brochure_overlay.get("year") or 0)
                if y_int:
                    from backend.enrichment.brochure_overlay_backfill import _neighbor_adds

                    richer = _neighbor_adds(y_int, make, model, name)
                    if len(sanitize_brochure_trim_adds(richer, name)) >= 3:
                        brochure_lines = richer

        candidates: list[str] = []
        candidates.extend(step_curated_adds)
        candidates.extend(sanitize_brochure_trim_adds(brochure_lines, name))
        if not step_curated_adds and not brochure_lines:
            for alias in aliases:
                candidates.extend(dict_adds.get(_norm_token(alias), []))
            candidates.extend(dict_adds.get(_norm_token(name), []))
        elif brochure_overlay and not from_brochure and ladder_source_lower != "curated":
            # Overlay exists for this YMM but not this trim — avoid cross-trim CSV/wiki noise.
            candidates = list(step_curated_adds)

        from backend.enrichment.trim_spec_extractor import (
            sanitize_trim_specs,
            trim_specs_to_bullets,
        )

        spec_rows = sanitize_trim_specs(
            _lookup_merged_trim_specs(
                name,
                aliases,
                make=make,
                model=model,
                year=year,
                ladder_id=str(ladder_def.get("id") or ""),
                adds=candidates,
                dict_adds=dict_adds,
            )
        )
        candidates.extend(trim_specs_to_bullets(spec_rows, trim_name=name))

        adds = _merge_trim_display_bullets(
            candidates,
            trim_name=name,
            make=make,
            model=model,
            year=year,
        )
        adds = _strip_inventory_price_adds(adds)
        specs: list[Any] = []

        if not adds and not specs and not brochure_overlay:
            less_equipped = (
                str(steps_def[i + 1].get("name") or "").strip() if i + 1 < len(steps_def) else ""
            )
            more_equipped = str(steps_def[i - 1].get("name") or "").strip() if i > 0 else ""
            ladder_source_lower = str(ladder_def.get("source") or "").lower()
            if ladder_source_lower == "inventory":
                from backend.enrichment.trim_ladder_knowledge import infer_trim_step_adds

                adds = infer_trim_step_adds(
                    name,
                    index=i,
                    total=len(steps_def),
                    make=make,
                    model=model,
                    lower_trim=less_equipped,
                    higher_trim=more_equipped,
                )
            if not adds:
                adds = fallback_trim_step_adds(
                    name,
                    index=i,
                    total=len(steps_def),
                    make=make,
                    model=model,
                    lower_trim=less_equipped,
                    higher_trim=more_equipped,
                )
        adds = _strip_inventory_price_adds(adds)

        is_current = i == current_index and best_score > 0
        # Steps are luxury-first (index 0 = top). Below current = more base; above = more luxury.
        is_passed = current_index >= 0 and i > current_index
        steps_out.append(
            {
                "name": name,
                "aliases": aliases,
                "index": i,
                "is_current": is_current,
                "is_passed": is_passed,
                "is_ahead": current_index >= 0 and i < current_index,
                "adds": adds,
                "specs": specs,
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


def _only_marketing_brochure_placeholders(result: dict[str, Any]) -> bool:
    steps = result.get("steps") or []
    saw_add = False
    for step in steps:
        for line in step.get("adds") or []:
            saw_add = True
            if not re.search(
                r"\bmarketing trim level from oem brochure\b",
                str(line),
                re.I,
            ):
                return False
    return saw_add


def _trim_ladder_should_display(result: dict[str, Any], make: str, model: str) -> bool:
    steps = result.get("steps") or []
    if len(steps) < 2:
        return False
    step_dicts = [{"name": s.get("name")} for s in steps]
    if not _ladder_steps_plausible_for_model(step_dicts, make, model):
        return False
    if _trim_ladder_quality(result, make, model) == "low":
        return False
    if _only_marketing_brochure_placeholders(result):
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
def _load_generated_ladders_raw() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_GENERATED_JSON)


@lru_cache(maxsize=1)
def _load_generated_ladders() -> list[dict[str, Any]]:
    merged = _load_merged_ladders()
    if merged:
        return [
            lad
            for lad in merged
            if str(lad.get("source") or "").lower()
            not in {
                "epa",
                "epa_fueleconomy",
                "epa csv",
                "epa_fueleconomy.gov",
                "brochure",
            }
        ]
    return _load_generated_ladders_raw()


@lru_cache(maxsize=1)
def _load_merged_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_MERGED_JSON)


@lru_cache(maxsize=1)
def _load_epa_ladders() -> list[dict[str, Any]]:
    return _read_ladders_file(_LADDERS_EPA_JSON)


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
    if _norm_make(make) == "cadillac" and _norm_model(model) in {"escalade", "xt4"} and t.upper() in {
        "2WD",
        "4WD",
        "FWD",
        "AWD",
    }:
        return None
    if re.fullmatch(r"V\s*AWD", t, re.I) and _norm_make(make) == "cadillac":
        return "V-Series"
    if re.fullmatch(r"L\s*\dWD", t, re.I):
        return None

    preserved = preserve_trim_label(t, make, model)
    if preserved:
        return preserved

    name = _extract_trim_from_cell(t, make, model)
    return name


def _find_epa_csv(make: str, model: str, year: Any) -> Path | None:
    """Best-match EPA CSV via dictionary catalog (fallback: legacy glob)."""
    return _catalog_find_epa(make, model, year)


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
        adds: list[str] = []
        engine = (row.get("engineDisplay") or row.get("engineOptions") or "").strip()
        if engine:
            adds.append(engine)
        drive = (row.get("drivetrainOptions") or "").strip()
        if drive:
            adds.append(drive)
        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": adds[:3],
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
    """Best-match Complete_Options CSV via dictionary catalog (skips stubs)."""
    return _catalog_find_co(make, model, year)


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
    loaders = [_load_json_ladders, _load_generated_ladders]
    for loader in loaders:
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


def _preserve_curated_ladder_steps(
    steps: list[dict[str, Any]],
    make: str,
    model: str | None = None,
) -> list[dict[str, Any]]:
    """Keep curated OEM trim order and distinct trim names from the ladder JSON."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for step in steps or []:
        name = _clean_trim_step_display_name(str(step.get("name") or ""))
        if not name:
            continue
        key = _norm_token(name)
        if key in seen:
            continue
        seen.add(key)
        aliases = [
            _clean_trim_step_display_name(str(a))
            for a in (step.get("aliases") or [])
            if str(a).strip()
        ]
        adds = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
        cleaned: dict[str, Any] = {
            "name": name,
            "aliases": list(dict.fromkeys(aliases))[:6],
            # Keep full curated mechanical + cabin lists; display merge caps bullets later.
            "adds": adds[:12],
        }
        by_year = step.get("adds_from_year")
        if isinstance(by_year, dict) and by_year:
            cleaned["adds_from_year"] = by_year
        ymin = int(step.get("year_min") or 0)
        ymax = int(step.get("year_max") or 9999)
        if ymin:
            cleaned["year_min"] = ymin
        if ymax < 9999:
            cleaned["year_max"] = ymax
        out.append(cleaned)
    if len(out) >= 2:
        from backend.enrichment.trim_ladder_knowledge import luxury_rank

        first_rank = luxury_rank(out[0]["name"], make, model)
        last_rank = luxury_rank(out[-1]["name"], make, model)
        if first_rank > last_rank:
            out.reverse()
    return out if len(out) >= 2 else []


def _finalize_ladder_steps(ladder: dict[str, Any], make: str, model: str | None = None) -> dict[str, Any]:
    """Canonicalize, dedupe, and sort steps (luxury at top)."""
    from backend.enrichment.trim_ladder_knowledge import merge_drivetrain_ladder_steps, normalize_ladder_steps

    model_label = model or ((ladder.get("models") or [None])[0])
    if str(ladder.get("source") or "").lower() == "curated":
        steps = _preserve_curated_ladder_steps(ladder.get("steps") or [], make, model=str(model_label or ""))
    else:
        steps = normalize_ladder_steps(ladder.get("steps") or [], make, model=str(model_label or ""))
    steps = merge_drivetrain_ladder_steps(steps, make, model=str(model_label or ""))
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

    if mk == "chevrolet":
        sport_models = (
            "corvette",
            "camaro",
            "silverado",
            "tahoe",
            "suburban",
            "colorado",
            "blazer",
        )
        performance_trims = {
            "zr1",
            "z06",
            "stingray",
            "zr2",
            "ss",
            "trail boss",
            "lt trail boss",
            "custom trail boss",
            "high country",
        }
        if not any(tok in mod for tok in sport_models) and names_lower & performance_trims:
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


def options_trim_cell_is_plausible(raw: str, make: str, model: str) -> bool:
    """Reject Wikipedia-style Complete_Options trim cells before ladder generation."""
    text = (raw or "").strip()
    if not text or len(text) > 52:
        return False
    low = text.lower()
    junk = (
        "predecessor ",
        "wikipedia",
        "wheelbase",
        "sourced from",
        "unveiled",
        "launched in",
        "internet movie",
    )
    if any(j in low for j in junk):
        return False
    name = _extract_trim_from_cell(text, make, model)
    if not name or not _is_valid_trim_name(name, make=make, model=model):
        return False
    return True


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
    for path in sorted(iter_dt_options_paths()):
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
    from backend.enrichment.dictionary_catalog import catalog_key as _catalog_key_for_car

    try:
        y = int(year)
        ladder_ck = ladder.get("catalog_key")
        if ladder_ck and _catalog_key_for_car(y, make, model) == str(ladder_ck):
            return True
    except (TypeError, ValueError):
        pass
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
        if src == "brochure":
            return 9
        if src in {"epa", "epa_fueleconomy", "epa csv", "epa_fueleconomy.gov"}:
            return 2
        if src == "inventory":
            return 4
        return 3

    def year_mid_dist(lad: dict[str, Any]) -> int:
        """Among equal-rank ladders covering the query year, prefer the narrowest/closest range."""
        ymin = int(lad.get("year_min") or 0)
        ymax = int(lad.get("year_max") or 9999)
        mid = (ymin + ymax) // 2 if ymin and ymax < 9999 else y
        return abs(y - mid)

    # Prefer curated OEM ladders over per-year brochure auto-ladders when both match.
    # Secondary tiebreaker: prefer year-range midpoint closest to query year so the
    # most-specific ladder (e.g. 2015 EPA vs 2013 EPA for year=2015) wins.
    matches.sort(key=lambda lad: (source_rank(lad), year_dist(lad), year_mid_dist(lad)))
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


def _pick_brochure_ladder(make: str, model: str, year: Any) -> dict[str, Any] | None:
    from backend.enrichment.brochure_extract import load_brochure_trim_overlay
    from backend.enrichment.dictionary_catalog import catalog_key as _ck

    # Hand-reviewed overlay drives steps; avoid auto brochure ladder superseding EPA/options base.
    if load_brochure_trim_overlay(year, make, model):
        return None

    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    target = _ck(y, make, model)
    for lad in _load_merged_ladders():
        if str(lad.get("catalog_key") or "") != target:
            continue
        if str(lad.get("source") or "").lower() != "brochure":
            continue
        from backend.enrichment.brochure_trim_candidates import filter_spurious_brochure_trims
        from backend.enrichment.trim_ladder_knowledge import normalize_ladder_steps

        finalized = _finalize_ladder_steps(lad, make, model)
        names = filter_spurious_brochure_trims(
            [str(s.get("name") or "").strip() for s in finalized.get("steps") or []],
            make=make,
            model=model or "",
        )
        if len(names) < 2:
            return None
        seen: set[str] = set()
        ordered: list[dict[str, Any]] = []
        for n in names:
            key = n.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append({"name": n, "aliases": [], "adds": []})
        finalized = {
            **finalized,
            "source": "brochure",
            "steps": normalize_ladder_steps(ordered, make, model=model or ""),
        }
        if _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model):
            return finalized
    return None


def _pick_ladder_def(make: str, model: str, year: Any) -> dict[str, Any] | None:
    from backend.enrichment.brochure_extract import load_brochure_trim_overlay

    has_overlay = bool(load_brochure_trim_overlay(year, make, model))
    curated = _pick_curated_ladder(make, model, year)
    if curated:
        src = str(curated.get("source") or "").lower()
        if src == "brochure" and has_overlay:
            curated = None
        else:
            finalized = _finalize_ladder_steps(curated, make, model)
            if _ladder_steps_plausible_for_model(finalized.get("steps") or [], make, model):
                return finalized

    epa_path = _find_epa_csv(make, model, year)
    if epa_path:
        epa_ladder = _ladder_from_epa_csv(epa_path, make, model, year)
        if epa_ladder and _ladder_steps_plausible_for_model(epa_ladder.get("steps") or [], make, model):
            return epa_ladder

    brochure = _pick_brochure_ladder(make, model, year)
    if brochure and len(brochure.get("steps") or []) >= 3:
        return brochure

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

    generic = _generic_trim_ladder_def(make, model, year)
    generic_steps = generic.get("steps") or []

    inventory = _ladder_from_inventory(make, model, year)
    inv_steps = inventory.get("steps") or [] if inventory else []
    if (
        len(inv_steps) >= 3
        and inventory
        and _ladder_steps_usable(inv_steps, make, model=model)
        and _ladder_steps_plausible_for_model(inv_steps, make, model)
    ):
        return inventory

    if len(generic_steps) >= 2:
        return generic

    if (
        inventory
        and len(inv_steps) >= 2
        and _ladder_steps_usable(inv_steps, make, model=model)
        and _ladder_steps_plausible_for_model(inv_steps, make, model)
    ):
        return inventory

    return generic if generic_steps else None


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
    try:
        model_year = int(year)
    except (TypeError, ValueError):
        model_year = 0
    if model_year and model_year < _MIN_TRIM_LADDER_YEAR:
        return None
    model = _normalize_listing_model(make, model)
    ladder_def = _pick_ladder_def(make, model, year)
    if not ladder_def:
        ladder_def = _generic_trim_ladder_def(make, model, year)

    steps_def = ladder_def.get("steps") or []
    if len(steps_def) < 2:
        ladder_def = _generic_trim_ladder_def(make, model, year)
        steps_def = ladder_def.get("steps") or []

    from backend.enrichment.brochure_extract import load_brochure_trim_overlay

    brochure_overlay = load_brochure_trim_overlay(year, make, model)
    curated_ladder = str(ladder_def.get("source") or "").lower() == "curated"
    preserve_all_steps = bool(ladder_def.get("preserve_all_steps"))
    skip_trim_year_windows = preserve_all_steps
    if brochure_overlay:
        from backend.enrichment.trim_ladder_knowledge import luxury_rank

        preserve_all_steps = False
        brochure_trims = set(brochure_overlay.get("trims_available") or [])
        brochure_trims.update((brochure_overlay.get("adds_by_trim") or {}).keys())
        steps_before_brochure = list(steps_def)

        if curated_ladder and brochure_trims:
            # Curated ladders: keep curated step list but apply year-window filtering.
            # Trims confirmed by the overlay's trims_available are force-included even if
            # their knowledge-base year window disagrees (overlay is authoritative for the year).
            overlay_confirmed = {str(t).strip() for t in (brochure_overlay.get("trims_available") or [])}
            filtered_steps: list[dict[str, Any]] = []
            for step in steps_def:
                name = str(step.get("name") or "").strip()
                if name in overlay_confirmed:
                    filtered_steps.append(step)
                elif _step_applies_to_year(step, year, make=make, model=model, skip_trim_year_windows=False):
                    filtered_steps.append(step)
            steps_def = filtered_steps
            skip_trim_year_windows = True  # year filtering already applied above
        else:
            skip_trim_year_windows = True
            if len(brochure_trims) >= 2:
                order = list(brochure_overlay.get("trims_available") or [])
                if not order:
                    order = list((brochure_overlay.get("adds_by_trim") or {}).keys())
                order = sorted(
                    [str(t).strip() for t in order if str(t).strip()],
                    key=lambda t: luxury_rank(t, make, model),
                )
                existing_index = _index_ladder_steps_by_trim(steps_def, make=make, model=model)
                steps_def = []
                seen_step_keys: set[str] = set()
                for t in order:
                    if t not in brochure_trims:
                        continue
                    step = _find_ladder_step(existing_index, t, make=make, model=model) or {
                        "name": t,
                        "aliases": [],
                        "adds": [],
                    }
                    steps_def.append(step)
                    seen_step_keys |= _step_trim_keys(step, make=make, model=model)
                if str(ladder_def.get("source") or "").lower() == "inventory":
                    for step in steps_before_brochure:
                        step_keys = _step_trim_keys(step, make=make, model=model)
                        if step_keys & seen_step_keys:
                            continue
                        steps_def.append(step)
                        seen_step_keys |= step_keys
                steps_def.sort(
                    key=lambda s: luxury_rank(str(s.get("name") or ""), make, model),
                )
    steps_def = _filter_ladder_steps_for_year(
        steps_def,
        year,
        make=make,
        model=model,
        skip_trim_year_windows=skip_trim_year_windows,
    )
    if len(steps_def) < 2:
        ladder_def = _generic_trim_ladder_def(make, model, year)
        steps_def = _filter_ladder_steps_for_year(
            ladder_def.get("steps") or [],
            year,
            make=make,
            model=model,
            skip_trim_year_windows=bool(ladder_def.get("preserve_all_steps")),
        )

    result = _build_ladder_result(
        {**ladder_def, "steps": steps_def},
        make=make,
        model=model,
        year=year,
        trim=trim,
        brochure_overlay=brochure_overlay,
    )
    if len(result.get("steps") or []) < 2:
        fallback_def = _generic_trim_ladder_def(make, model, year)
        fallback_steps = _filter_ladder_steps_for_year(
            fallback_def.get("steps") or [],
            year,
            make=make,
            model=model,
        )
        if len(fallback_steps) >= 2:
            result = _build_ladder_result(
                {**fallback_def, "steps": fallback_steps},
                make=make,
                model=model,
                year=year,
                trim=trim,
                brochure_overlay=None,
            )

    if len(result.get("steps") or []) < 2:
        return None
    if _only_marketing_brochure_placeholders(result):
        return None
    step_dicts = [{"name": s.get("name")} for s in result.get("steps") or []]
    source = str(result.get("source") or "").lower()
    plausible_ok = _ladder_steps_plausible_for_model(step_dicts, make, model)
    is_generic_oem = str(result.get("id") or "").startswith("generic_")
    if not plausible_ok and source not in {"oem_knowledge", "inventory", "brochure"}:
        return None
    if not plausible_ok and is_generic_oem:
        return None
    if not _trim_ladder_should_display(result, make, model):
        # Premium VDP: still show OEM fallback ladder when quality heuristics are conservative.
        if str(result.get("source") or "").lower() in {
            "oem_knowledge",
            "inventory",
            "brochure",
        } and not is_generic_oem:
            result["quality"] = "medium"
            return result
        fallback_def = _generic_trim_ladder_def(make, model, year)
        # Skip trim-specific year windows so generic steps aren't over-filtered.
        fallback_steps = _filter_ladder_steps_for_year(
            fallback_def.get("steps") or [],
            year,
            make=make,
            model=model,
            skip_trim_year_windows=True,
        )
        if len(fallback_steps) >= 2:
            result = _build_ladder_result(
                {**fallback_def, "steps": fallback_steps},
                make=make,
                model=model,
                year=year,
                trim=trim,
                brochure_overlay=None,
            )
            if len(result.get("steps") or []) >= 2:
                result["quality"] = "medium"
                return result
        # Absolute last resort: return unfiltered generic steps so premium VDP always
        # shows a trim ladder even for unusual makes/years.
        all_steps = fallback_def.get("steps") or []
        if len(all_steps) >= 2:
            result = _build_ladder_result(
                {**fallback_def, "steps": all_steps},
                make=make,
                model=model,
                year=year,
                trim=trim,
                brochure_overlay=None,
            )
            if len(result.get("steps") or []) >= 2:
                result["quality"] = "low"
                return result
        return None
    return result
