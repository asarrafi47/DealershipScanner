"""Infer or fall back to a rung's "adds" bullets when no document supplies them."""
from __future__ import annotations


from .bullets import (
    sanitize_trim_adds,
)

_TRIM_NAME_HINTS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("calligraphy",), "Flagship trim with premium materials, advanced driver assists, and exclusive design details."),
    (("pinnacle", "ultimate"), "Highest trim level in this lineup, with maximum luxury and technology content."),
    (("limited", "platinum", "prestige", "premium plus"), "Upper trim with upgraded interior, expanded tech, and additional comfort features."),
    (("n line", "n-line"), "Sport-styled trim with unique exterior accents, wheels, and interior touches."),
    (("hellcat", "scat pack", "srt", "trd pro", "m sport", "m package"), "High-performance trim with upgraded power, brakes, and sport suspension."),
    # No "over the <X> trim" tail here: the baseline would be our own guess at
    # the ladder order, not something an OEM printed.
    (("sel premium", "xle premium", "premium plus"), "Upper trim with premium audio, a larger display, and comfort upgrades."),
    (("sel", "xle", "ex", "sv", "touring"), "Mid-level trim with added convenience features and nicer interior finishes."),
    (("xrt", "trail", "off-road", "wilderness", "rubicon"), "Rugged or adventure-oriented trim with protective styling and capability-focused equipment."),
    (("big horn",), "Well-equipped work-oriented trim with added convenience and appearance upgrades."),
    (("laramie", "lariat", "ltz", "limited"), "Upper trim with leather-appointed seating, upgraded tech, and premium comfort features."),
    (("sport", "rs", "r/t", "gt", "ss"), "Sport trim with performance styling, handling upgrades, or stronger engine options."),
    (("se", "s ", " s", "lx", "ls", "base", "essential", "preferred"), "Entry-level trim with core standard equipment."),
)


def _trim_name_hint(trim_name: str) -> str | None:
    raw = (trim_name or "").strip()
    if not raw:
        return None
    if raw.lower() == "n":
        return "Performance-oriented trim with sport tuning and stronger powertrain options."
    low = f" {raw.lower()} "
    for tokens, line in _TRIM_NAME_HINTS:
        for tok in tokens:
            t = tok.strip().lower()
            if not t:
                continue
            if t == raw.lower() or t in raw.lower() or (tok.startswith(" ") and tok in low):
                return line
    return None


def infer_trim_step_adds(
    trim_name: str,
    *,
    index: int,
    total: int,
    lower_trim: str = "",
    higher_trim: str = "",
    make: str = "",
    model: str = "",
) -> list[str]:
    """
    Educational fallback when CSV/curated data has no feature bullets.
    Describes trim tier position and common naming patterns — not model-specific OEM specs.
    """
    name = (trim_name or "").strip()
    if not name or total < 2:
        return []

    out: list[str] = []
    hint = _trim_name_hint(name)
    if hint:
        out.append(hint)

    if index == 0:
        out.append("Top of this trim lineup — typically the most equipment and premium content.")
    elif index >= total - 1:
        out.append("Entry rung on this ladder — fewer optional upgrades than higher trims.")

    lower = (lower_trim or "").strip()
    higher = (higher_trim or "").strip()
    if lower and index < total - 1:
        out.append(f"Builds on {lower} with additional comfort, technology, or appearance upgrades.")
    elif higher and index > 0:
        out.append(f"Sits below {higher} with fewer premium features but a lower typical price point.")

    return sanitize_trim_adds(out, name)[:4]


def fallback_trim_step_adds(
    trim_name: str,
    *,
    index: int,
    total: int,
    make: str,
    model: str | None = None,
    lower_trim: str = "",
    higher_trim: str = "",
) -> list[str]:
    """
    Last-resort copy when OEM/brochure/inventory bullets are unavailable.
    Keeps the trim ladder panel useful without implying specific factory equipment.
    """
    name = (trim_name or "").strip()
    if not name or total < 2:
        return []

    out: list[str] = []
    hint = _trim_name_hint(name)
    if hint:
        out.append(hint)

    if not out:
        mk = (make or "").strip()
        mdl = (model or "").strip()
        vehicle = f"{mk} {mdl}".strip() or "this model"
        if index == 0:
            out.append(f"Highest trim level offered on the {vehicle}.")
        elif index >= total - 1:
            out.append(f"Base trim level on the {vehicle}.")
        elif higher_trim and lower_trim:
            out.append(f"Mid-level trim between {lower_trim} and {higher_trim}.")
        elif higher_trim:
            out.append(f"Positioned below {higher_trim} on the {vehicle} trim ladder.")
        elif lower_trim:
            out.append(f"Positioned above {lower_trim} on the {vehicle} trim ladder.")

    return sanitize_trim_adds(out, name)[:3]
