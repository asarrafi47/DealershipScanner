"""Shared normalisers, ladder-file paths and catalog-file lookups."""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any
from backend.utils.spec_field_normalize import normalize_trim_text

logger = logging.getLogger(__name__)
from backend.enrichment.dictionary_catalog import find_complete_options_csv as _catalog_find_co
from backend.enrichment.dictionary_catalog import find_epa_csv as _catalog_find_epa
from backend.enrichment.dictionary_paths import (
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


def _provenance_gate_on() -> bool:
    """
    True while brochure-overlay bullets must carry a citation to render.

    Read through :func:`backend.enrichment.brochure_extract.trim_adds_provenance_required`
    on every call rather than cached, so ``TRIM_ADDS_REQUIRE_PROVENANCE`` can be
    flipped without a restart and tests can set it per case.
    """
    from backend.enrichment.brochure_extract import trim_adds_provenance_required

    return trim_adds_provenance_required()


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


def _find_epa_csv(make: str, model: str, year: Any) -> Path | None:
    """Best-match EPA CSV via dictionary catalog (fallback: legacy glob)."""
    return _catalog_find_epa(make, model, year)


def _find_complete_options_csv(make: str, model: str, year: Any) -> Path | None:
    """Best-match Complete_Options CSV via dictionary catalog (skips stubs)."""
    return _catalog_find_co(make, model, year)
