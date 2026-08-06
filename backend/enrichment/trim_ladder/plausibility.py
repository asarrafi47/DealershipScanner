"""Is a set of ladder steps usable for this model, or is it junk?"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _extract_trim_from_cell,
    _is_valid_trim_name,
    _norm_make,
    _norm_model,
    _norm_token,
)
from .steps import (
    _clean_trim_step_display_name,
)

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
