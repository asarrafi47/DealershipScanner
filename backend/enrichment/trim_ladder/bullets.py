"""Trim-add bullet display: mechanical detection, parts, merge."""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


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
    """Powertrain, chassis, and performance hardware (vs cabin/infotainment).

    Kept as a floor under the category table in ``trim_ladder_knowledge``: these
    keywords are hardware by definition, so anything they match is pinned into
    the top band even if the category patterns did not recognise the wording.

    It deliberately does NOT consult ``trim_spec_extractor._classify_text``.
    That classifier labels "Power folding outside mirrors with reverse tilt-down
    feature" as ``Engine Options`` and "Tow hitch receiver with 7-pin wiring
    harness" as ``Maximum Towing Capacity``; trusting it floored both into the
    hardware band and pushed the panoramic moonroof and the leather seats below
    a door mirror on the 2026 Pathfinder Platinum rung. The explicit keyword
    list below is the only evidence used, because every entry in it names
    hardware outright.
    """
    s = str(text or "").strip()
    if not s:
        return False
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


def _bullet_display_parts(
    raw: str,
    *,
    trim_name: str,
    make: str = "",
    model: str = "",
    year: Any = None,
) -> list[str]:
    """One source line → the bullets we are willing to render from it.

    Replaces ``trim_spec_extractor.split_spec_value_parts`` at this call site:
    that helper splits on every ``;``, including the ones inside a parenthesis,
    which is how "Hybrid 2.5L I4 (SIDI & PFI; Hybrid)" reached the page as the
    two orphans "Hybrid 2.5L I4 (SIDI & PFI" and "Hybrid)". Here the split only
    happens at bracket depth 0, a doubled label prefix is collapsed rather than
    shown, and a line we cannot split cleanly yields nothing at all.

    A cell is also judged as a whole: if any one of its depth-0 pieces is a
    severed fragment, a sentence, or an encyclopedia infobox field, the cell is
    scraped prose and NONE of it is quotable — keeping the surviving halves is
    what published a 1975 Bronco engine on 2026 Bronco Sport rungs.
    """
    from backend.enrichment.trim_ladder_knowledge import (
        collapse_repeated_label_prefix,
        expand_long_enumeration,
        is_wellformed_trim_bullet,
        part_is_contaminated,
        split_outside_brackets,
        strip_lower_rung_reference,
        strip_source_tag,
    )
    from backend.enrichment.trim_spec_extractor import compact_trim_bullet

    line = collapse_repeated_label_prefix(strip_source_tag(str(raw or "").strip()))
    if not line:
        return []

    out: list[str] = []
    # "|" is how the brochure importer joins several values into one cell.
    pieces = split_outside_brackets(line, separators=";|")
    candidates: list[str] = []
    for piece in pieces:
        compact = compact_trim_bullet(
            piece, trim_name=trim_name, make=make, model=model, year=year
        )
        candidate = strip_lower_rung_reference(
            collapse_repeated_label_prefix(compact or piece)
        )
        if not candidate:
            continue
        if part_is_contaminated(candidate):
            return []
        candidates.append(candidate)
    for candidate in candidates:
        if is_wellformed_trim_bullet(candidate):
            out.append(candidate)
            continue
        # Too long to render, but a clean comma list: keep the items, not a cut.
        out.extend(expand_long_enumeration(candidate))
    return out


def _merge_trim_display_bullets(
    candidates: list[str],
    *,
    trim_name: str,
    make: str,
    model: str,
    year: Any,
    max_items: int = 8,
) -> list[str]:
    """Rank 'what this trim adds' by what actually changes the car — deduped.

    Source lists arrive in brochure order, which buries the engine under trivia.
    The category table in ``trim_ladder_knowledge`` scores each bullet so
    powertrain/chassis/capability lead and commodity equipment drops out.
    """
    from backend.enrichment.trim_ladder_knowledge import (
        bullet_year_window_excludes,
        is_generic_trim_add,
        is_wellformed_trim_bullet,
        rank_trim_adds,
        sanitize_trim_adds,
    )
    from backend.enrichment.trim_spec_extractor import (
        is_derived_comparison_bullet,
        is_displayable_trim_bullet,
        is_junk_spec_text,
        is_stale_listing_trim_prose,
    )

    try:
        model_year: int | None = int(year)
    except (TypeError, ValueError):
        model_year = None

    ordered: list[str] = []
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
        if not is_displayable_trim_bullet(part):
            return
        if not is_wellformed_trim_bullet(part):
            return
        # The source stated which model years its claim covers. Honour it: a
        # curated Gladiator rung carried "Uconnect 3 with 5-inch display
        # (2020–2021)" onto 2022–2026 cars, and an X5 rung carried "456 hp
        # twin-turbo V8 with xDrive AWD (2019–2020)" onto 2021–2026 cars.
        if bullet_year_window_excludes(part, model_year):
            return
        core = _bullet_core(part)
        if part.lower() in seen or core in seen:
            return
        if any(_cores_too_similar(core, _bullet_core(existing)) for existing in seen):
            return
        seen.add(part.lower())
        seen.add(core)
        ordered.append(part)

    for bullet in candidates:
        # Reject the WHOLE line before splitting: "Upgrades Engine Options: A; B
        # (was C; D)" otherwise survives as the orphan fragments "B" and "D)".
        if is_derived_comparison_bullet(bullet):
            continue
        for part in _bullet_display_parts(
            bullet, trim_name=trim_name, make=make, model=model, year=year
        ):
            _try_add(part)

    try:
        year_int: int | None = int(year)
    except (TypeError, ValueError):
        year_int = None
    cleaned = sanitize_trim_adds(ordered, trim_name, max_items=0)
    return rank_trim_adds(
        cleaned,
        year=year_int,
        max_items=max_items,
        hardware_hint=_is_mechanical_trim_bullet,
    )
