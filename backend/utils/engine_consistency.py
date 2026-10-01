"""
Cross-field sanity for engine data: a proposed ``cylinders`` value must not
contradict the layout token already present in ``engine_description``
(e.g. "3.5L V6" → 6). Heuristic/snippet-derived cylinder values (EPA trim
matching, search snippets, page-text parsing) must pass this check before
being written; authoritative VIN-decode values (vPIC) may override the
description instead, since the description itself can be enrichment-written
and wrong.

Motivating incidents:
- 2026-07-19: ~1,200 rows carried cylinders=4 on cars whose own
  engine_description said V6/V8 — year-collisions where "E 350"-style model
  names resolved to the modern (turbo-four) generation.
- 2026-07-20: ~380 battery-electric vehicles (BMW i4, Mercedes EQE, Chevy
  Bolt/Blazer EV) carried a positive cylinder count from the trim decoder — a
  BEV has zero cylinders, so any count > 0 is wrong.
"""
from __future__ import annotations

import re

# "V6" / "V-8" / "I4" / "H6" / "W12" — but not "24V" (valve counts)
_LAYOUT_TOKEN = re.compile(r"\b([VIHW])[-\s]?(\d{1,2})\b", re.I)
# "6-cyl", "6 cylinder", "6cyl"
_CYL_PHRASE = re.compile(r"\b(\d{1,2})\s*[-\s]?cyl(?:inder)?s?\b", re.I)


def is_bev_fuel(fuel_type: str | None) -> bool:
    """
    True for a pure battery-electric fuel type (has no engine → 0 cylinders).
    Plug-in hybrids and "Electric/Gas" hybrids keep a combustion engine and
    are deliberately excluded.
    """
    if not fuel_type:
        return False
    from backend.vehicle_facts.electrification import BEV, fuel_label_electrification

    return fuel_label_electrification(fuel_type) == BEV


# "2.7L", "2.7-Liter", "2.7 L", "3.5-L" — a displacement in dealer engine copy.
_LITERS_RE = re.compile(r"\b(\d{1,2}(?:\.\d)?)\s*-?\s*l(?:iter|itre)?s?\b", re.I)


def liters_from_engine_text(text: str | None) -> float | None:
    """Displacement in liters implied by an engine description, or None."""
    if not text:
        return None
    m = _LITERS_RE.search(text)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    return v if 0.5 <= v <= 9.0 else None


def catalog_row_conflicts_with_engine_text(
    engine_text: str | None,
    row_displacement: float | int | str | None,
    row_cylinders: int | str | None,
    *,
    liters_tolerance: float = 0.15,
) -> str | None:
    """Why a catalog row (epa_master) cannot describe the car whose dealer
    engine text this is — or None when nothing contradicts.

    Dealer engine copy is observed on the car; the catalog row was picked by a
    resolver. When the resolver had no parsed ``engine_l`` it scored on
    cylinders + drivetrain alone and linked 2.7L I4 Silverados to the 5.3L V8
    row and 3.0L X5 40i to the 4.4L M50i row (2,811 active cars, 2026-09-21).
    Reading hp / MPG / cylinders off such a row is worse than showing nothing.
    """
    text_l = liters_from_engine_text(engine_text)
    try:
        row_l = float(row_displacement) if row_displacement not in (None, "") else None
    except (TypeError, ValueError):
        row_l = None
    if text_l is not None and row_l is not None and abs(text_l - row_l) > liters_tolerance:
        return f"displacement {text_l:.1f}L vs catalog {row_l:.1f}L"
    text_c = cylinders_from_engine_text(engine_text)
    try:
        row_c = int(row_cylinders) if row_cylinders not in (None, "") else None
    except (TypeError, ValueError):
        row_c = None
    if text_c is not None and row_c is not None and text_c != row_c:
        return f"cylinders {text_c} vs catalog {row_c}"
    return None


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
    fuel_type: str | None = None,
) -> bool:
    """
    True when *cylinders* is inconsistent with the car:
      - a positive count on a battery-electric fuel type (BEV has 0 cylinders), or
      - a count that contradicts a clear layout token in *engine_text*.
    """
    if cylinders is None:
        return False
    try:
        c = int(cylinders)
    except (TypeError, ValueError):
        return False
    if c > 0 and is_bev_fuel(fuel_type):
        # The BEV label only outranks the cylinder count when the engine text
        # shows nothing combustion-shaped. Feeds have stored bare "Electric" on
        # gas cars (Lexus GX 550 "3.4L V6" ×25); treating their cylinders as the
        # error kept erasing the one field that disproved the bad label — the
        # defect self-sealed. With combustion evidence present, the FUEL LABEL
        # is the suspect (handled by fuel_label_plausibility at read/write
        # time), and the count is judged against the text alone below.
        from backend.utils.fuel_label_plausibility import combustion_evidence_from_text

        if not combustion_evidence_from_text(engine_text):
            return True
    implied = cylinders_from_engine_text(engine_text)
    if implied is None:
        return False
    return c != implied
