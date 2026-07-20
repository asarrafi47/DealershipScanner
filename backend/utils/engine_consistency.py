"""
Cross-field sanity for engine data: a proposed ``cylinders`` value must not
contradict the layout token already present in ``engine_description``
(e.g. "3.5L V6" → 6). Heuristic/snippet-derived cylinder values (EPA trim
matching, search snippets, page-text parsing) must pass this check before
being written; authoritative VIN-decode values (vPIC) may override the
description instead, since the description itself can be enrichment-written
and wrong.

Motivating incident (2026-07-19): ~1,200 rows carried cylinders=4 on cars
whose own engine_description said V6/V8 — year-collisions where "E 350"-style
model names resolved to the modern (turbo-four) generation.
"""
from __future__ import annotations

import re

# "V6" / "V-8" / "I4" / "H6" / "W12" — but not "24V" (valve counts)
_LAYOUT_TOKEN = re.compile(r"\b([VIHW])[-\s]?(\d{1,2})\b", re.I)
# "6-cyl", "6 cylinder", "6cyl"
_CYL_PHRASE = re.compile(r"\b(\d{1,2})\s*[-\s]?cyl(?:inder)?s?\b", re.I)


def cylinders_from_engine_text(text: str | None) -> int | None:
    """Cylinder count implied by an engine description, or None if unclear."""
    if not text:
        return None
    m = _CYL_PHRASE.search(text)
    if m:
        try:
            n = int(m.group(1))
            return n if 2 <= n <= 16 else None
        except ValueError:
            return None
    m = _LAYOUT_TOKEN.search(text)
    if m:
        try:
            n = int(m.group(2))
            return n if 2 <= n <= 16 else None
        except ValueError:
            return None
    return None


def cylinders_conflicts_with_engine_text(
    cylinders: int | None,
    engine_text: str | None,
) -> bool:
    """True when *cylinders* contradicts a clear layout token in *engine_text*."""
    if cylinders is None:
        return False
    implied = cylinders_from_engine_text(engine_text)
    if implied is None:
        return False
    try:
        return int(cylinders) != implied
    except (TypeError, ValueError):
        return False
