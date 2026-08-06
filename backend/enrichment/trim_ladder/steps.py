"""Ladder steps: year windows, identity keys, indexing and lookup."""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _YEAR_MIN_SUFFIX_RE,
    _YEAR_RANGE_SUFFIX_RE,
    _clean_trim_label,
    _norm_token,
)
from .claims import (
    _rung_claim_forms,
)

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
    """The one ``adds_by_trim`` heading that NAMES this rung, or ``None``.

    This is the second hop of the bullet path: ``_exact_rung_match`` decides
    which rung the car is, and this decides which section of the brochure that
    rung may read its bullets out of. Both hops carry the same claim — the
    bullets printed under a heading are that heading's trim's bullets — so both
    are held to the same rule, spelling equality under ``_rung_claim_forms``.

    TWO FUZZY ROUTES WERE REMOVED HERE (2026-08-02):

      * a WHOLE-TOKEN CONTAINMENT fallback that took the heading with the most
        tokens all of which appear in the step name. It is what let the Dodge
        Charger rung "SRT Hellcat Redeye Widebody" read the section filed under
        "SRT HELLCAT WIDEBODY" — {srt, hellcat, widebody} ⊂ {srt, hellcat,
        redeye, widebody} — i.e. a 797 hp car's rung printing the 717 hp car's
        equipment. Measured on the live fleet on the day it was removed: 12
        rendered ladder steps bound to a differently-named heading, all of them
        that one 2022 Charger rung, seen by 33 active cars' ladders, 1 of them
        as its own "This vehicle" rung.
      * a ``_trim_identity_keys`` overlap score. That helper runs a label
        through ``canonical_trim_name``, which TRUNCATES one trim onto another's
        name ("Sport Prestige" → "Sport", "SX-Prestige" → "SX"), so a heading
        and a step could be declared the same trim by our own reduction of both.
        It scored 0 cross-name bindings in the live fleet today, but it is the
        identical defect the rung-claim path had its fuzzy scorer revoked for,
        and the corpus is about to grow ~40x.

    ``aliases`` IS ACCEPTED AND IGNORED, for the reason set out in
    ``_exact_rung_match``: the alias table is our own mapping, it is not checked
    against any document, and it demonstrably contains neighbouring trims. The
    parameter stays only so existing positional callers keep working.

    TWO HEADINGS ANSWERING IS A REFUSAL, NOT A CHOICE. If more than one heading
    names this rung (e.g. a book with separate "Limited" and "Limited 4WD"
    sections, which reduce to the same claim form), the document cannot tell us
    which section is this rung's, so nothing is returned. The two exact-string
    checks below run first and are exempt: a heading that IS the step name
    character-for-character is not ambiguous with anything.
    """
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

    step_forms = _rung_claim_forms(name, make, model)
    if not step_forms:
        return None
    matched = [
        str(bkey)
        for bkey in brochure_adds_by_trim
        if _rung_claim_forms(str(bkey), make, model) & step_forms
    ]
    return matched[0] if len(matched) == 1 else None


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
