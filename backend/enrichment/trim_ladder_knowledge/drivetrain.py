"""Collapse drivetrain spellings of one rung ("Limited AWD"/"Limited FWD") into one step."""
from __future__ import annotations

import re


_TRIM_LIST_IN_PROSE_RE = re.compile(
    r"(?:trim levels?|grades?|trims?)(?:\s+(?:include|are|were|is))?\s*[:—-]?\s*"
    r"([A-Za-z0-9][A-Za-z0-9\s,&+/®™\-]{4,120})",
    re.I,
)
_DRIVETRAIN_INLINE_RE = re.compile(
    r"\b(?:2wd|4wd|awd|fwd|rwd|4x4|4x2)\b",
    re.I,
)
_DRIVETRAIN_SUFFIX_RE = re.compile(
    r"\s+(?:xDrive|sDrive|eDrive|Quattro|4MATIC|AWD|RWD|2WD|4WD|FWD)\s*$",
    re.I,
)
_BMW_SEDAN_MOTOR_DV_RE = re.compile(
    r"^(M?\d{3}[ie])(?:\s+(?:xDrive|sDrive))?$",
    re.I,
)
_BMW_SAV_DV_PREFIX_RE = re.compile(
    r"^(?:xDrive|sDrive|eDrive)(\d{2}[ie])$",
    re.I,
)


def drivetrain_merge_key(name: str, make: str, model: str | None = None) -> str:
    """Normalize trim labels that differ only by drivetrain into one merge bucket."""
    n = (name or "").strip()
    if not n:
        return ""

    m = _BMW_SEDAN_MOTOR_DV_RE.match(n)
    if m:
        return re.sub(r"[^a-z0-9]+", "", m.group(1).lower())

    compact = re.sub(r"[^a-z0-9]+", "", n.lower())
    sm = _BMW_SAV_DV_PREFIX_RE.match(compact)
    if sm:
        return sm.group(1)

    base = _DRIVETRAIN_SUFFIX_RE.sub("", n).strip()
    for _ in range(4):
        base = _DRIVETRAIN_INLINE_RE.sub(" ", base).strip()
    base = re.sub(r"\s+", " ", base).strip()
    return re.sub(r"[^a-z0-9]+", "", (base or n).lower())


def drivetrain_merge_display_name(name: str, make: str, model: str | None = None) -> str:
    """Preferred ladder label after merging xDrive / sDrive / 2WD variants."""
    n = (name or "").strip()
    if not n:
        return n

    m = _BMW_SEDAN_MOTOR_DV_RE.match(n)
    if m:
        motor = m.group(1)
        if motor.upper().startswith("M"):
            return motor[0].upper() + motor[1:]
        return motor

    compact = re.sub(r"[^a-z0-9]+", "", n.lower())
    sm = _BMW_SAV_DV_PREFIX_RE.match(compact)
    if sm:
        return sm.group(1)

    base = _DRIVETRAIN_SUFFIX_RE.sub("", n).strip()
    for _ in range(4):
        base = _DRIVETRAIN_INLINE_RE.sub(" ", base).strip()
    base = re.sub(r"\s+", " ", base).strip()
    return base or n


def _prefer_merged_display_name(names: list[str], make: str, model: str | None = None) -> str:
    """Pick the cleanest label when several drivetrain variants merge."""
    if not names:
        return ""
    for n in names:
        low = n.lower()
        if not any(tok in low for tok in ("xdrive", "sdrive", "edrive", "4wd", "2wd", "4matic", "quattro")):
            return drivetrain_merge_display_name(n, make, model)
    return drivetrain_merge_display_name(names[0], make, model)


def merge_drivetrain_ladder_steps(
    steps: list[dict],
    make: str,
    *,
    model: str | None = None,
    adds_limit: int = 10,
    alias_limit: int = 12,
) -> list[dict]:
    """Merge ladder rungs that differ only by drivetrain; union aliases and feature adds."""
    if not steps:
        return []

    buckets: dict[str, dict] = {}
    order: list[str] = []
    raw_names: dict[str, list[str]] = {}

    for step in steps or []:
        name = str(step.get("name") or "").strip()
        if not name:
            continue
        key = drivetrain_merge_key(name, make, model) or re.sub(r"[^a-z0-9]+", "", name.lower())
        if key not in buckets:
            buckets[key] = {"aliases": [], "adds": [], "year_min": 0, "year_max": 9999, "inventory_price_note": ""}
            order.append(key)
            raw_names[key] = []
        raw_names[key].append(name)
        entry = buckets[key]
        price_note = str(step.get("inventory_price_note") or "").strip()
        if price_note and not entry.get("inventory_price_note"):
            entry["inventory_price_note"] = price_note

        by_year = step.get("adds_from_year")
        if isinstance(by_year, dict) and by_year and "adds_from_year" not in entry:
            entry["adds_from_year"] = by_year

        ymin = int(step.get("year_min") or 0)
        ymax = int(step.get("year_max") or 9999)
        if ymin:
            entry["year_min"] = ymin if not entry["year_min"] else min(entry["year_min"], ymin)
        if ymax >= 9999:
            entry["year_max"] = 9999
        elif entry["year_max"] < 9999:
            entry["year_max"] = max(entry["year_max"], ymax)

        for alias in step.get("aliases") or []:
            astr = str(alias).strip()
            if astr and astr not in entry["aliases"]:
                entry["aliases"].append(astr)
        seen_adds = {a.lower() for a in entry["adds"]}
        for line in step.get("adds") or []:
            s = str(line).strip()
            if s and s.lower() not in seen_adds:
                seen_adds.add(s.lower())
                entry["adds"].append(s)

    out: list[dict] = []
    for key in order:
        entry = buckets[key]
        names = raw_names[key]
        display = _prefer_merged_display_name(names, make, model)
        aliases: list[str] = []
        for variant in names + entry["aliases"]:
            if variant != display and variant not in aliases:
                aliases.append(variant)
        aliases = list(dict.fromkeys(aliases))[:alias_limit]

        row: dict = {
            "name": display,
            "aliases": aliases,
            "adds": entry["adds"][:adds_limit],
        }
        if entry["year_min"]:
            row["year_min"] = entry["year_min"]
        if entry["year_max"] < 9999:
            row["year_max"] = entry["year_max"]
        if entry.get("adds_from_year"):
            row["adds_from_year"] = entry["adds_from_year"]
        if str(entry.get("inventory_price_note") or "").strip():
            row["inventory_price_note"] = str(entry["inventory_price_note"]).strip()
        out.append(row)
    return out
