"""
Trim-name knowledge and text extraction for OEM trim ladders.

Used by ``build_trim_ladders.py`` (offline generation) and ``trim_ladder.py`` (runtime fallback).
"""
from __future__ import annotations

import re
from typing import Iterable

# Longest / most specific names first within each make list.
MAKE_TRIM_ORDER: dict[str, tuple[str, ...]] = {
    "ford": (
        "Limited",
        "Raptor R",
        "Raptor",
        "Platinum",
        "King Ranch",
        "Lariat",
        "Timberline",
        "Tremor",
        "Titanium",
        "GT",
        "SEL",
        "ST-Line",
        "STX",
        "Sport",
        "FX4",
        "XLT",
        "SE",
        "XL",
        "Active",
        "Base",
    ),
    "chevrolet": (
        "ZR1",
        "Z06",
        "Stingray",
        "High Country",
        "Premier",
        "LTZ",
        "ZR2",
        "SS",
        "Trail Boss",
        "LT Trail Boss",
        "RST",
        "Z71",
        "RS",
        "LT",
        "Custom Trail Boss",
        "Custom",
        "LS",
        "WT",
        "L",
    ),
    "gmc": (
        "Denali Ultimate",
        "AT4X",
        "Denali",
        "AT4",
        "SLT",
        "Elevation",
        "SLE",
        "Pro",
    ),
    "ram": (
        "TRX",
        "Tungsten",
        "Limited",
        "Laramie Longhorn",
        "Laramie",
        "Rebel",
        "Night Edition",
        "Big Horn",
        "Lone Star",
        "Tradesman HFE",
        "Tradesman",
    ),
    "jeep": (
        "Summit Reserve",
        "Summit",
        "Overland",
        "High Altitude",
        "Limited",
        "Rubicon",
        "Trailhawk",
        "Mojave",
        "Sahara",
        "Willys Sport",
        "Willys",
        "Latitude Plus",
        "Altitude",
        "Latitude",
        "Sport S",
        "Sport",
        "Laredo",
    ),
    "toyota": (
        "Capstone",
        "1794",
        "Platinum",
        "Limited",
        "XLE Premium",
        "TRD Pro",
        "XSE",
        "XLE",
        "TRD Off-Road",
        "TRD Sport",
        "TRD",
        "Adventure",
        "SE",
        "LE",
        "SR5",
        "SR",
        "Base",
    ),
    "honda": (
        "Type R",
        "Black Edition",
        "Sport Touring",
        "Elite",
        "Touring",
        "EX-L",
        "TrailSport",
        "Sport",
        "EX",
        "LX",
        "SE",
    ),
    "nissan": (
        "Platinum Reserve",
        "Platinum",
        "SL",
        "Nismo",
        "Pro-4X",
        "SV",
        "SR",
        "Rock Creek",
        "Pro-X",
        "Midnight Edition",
        "S",
    ),
    "hyundai": (
        "Calligraphy",
        "Limited",
        "N",
        "N Line",
        "SEL Premium",
        "SEL",
        "XRT",
        "SE",
        "Blue",
        "Essential",
        "Preferred",
    ),
    "kia": (
        "SX Prestige",
        "SX",
        "GT",
        "GT-Line",
        "EX",
        "X-Line",
        "S",
        "LX",
    ),
    "subaru": (
        "Touring XT",
        "Touring",
        "Wilderness",
        "Limited XT",
        "Limited",
        "Onyx Edition",
        "Sport",
        "Premium",
        "Base",
    ),
    "mazda": (
        "Turbo Premium Plus",
        "Turbo Premium",
        "Signature",
        "Premium Plus",
        "Premium",
        "Carbon Turbo",
        "Carbon Edition",
        "Preferred",
        "Select",
        "Sport",
    ),
    "volkswagen": (
        "R",
        "GTI",
        "GLI",
        "SEL Premium",
        "SEL",
        "R-Line",
        "SE",
        "S",
    ),
    "bmw": (
        "M Competition",
        "Competition",
        "M Sport",
        "Sport Line",
        "Luxury Line",
        "Modern Line",
        "xLine",
        "Executive",
        "Premium",
        "Base",
    ),
    "mercedesbenz": (
        "Maybach",
        "AMG",
        "AMG Line",
        "Premium Plus",
        "Premium",
        "Night Edition",
        "Exclusive",
        "Base",
    ),
    "audi": (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
        "Progressiv",
        "Technik",
        "Komfort",
    ),
    "lexus": (
        "Ultra Luxury",
        "Luxury",
        "F Sport Performance",
        "F Sport Handling",
        "F Sport",
        "Premium",
        "Base",
    ),
    "acura": (
        "Type S Advance",
        "Type S",
        "Advance",
        "Technology",
        "A-Spec",
        "Base",
    ),
    "cadillac": (
        "V-Series Blackwing",
        "V-Series",
        "Platinum",
        "Sport Platinum",
        "Premium Luxury",
        "Luxury",
        "Sport",
        "Preferred",
    ),
    "lincoln": (
        "Black Label",
        "Reserve",
        "Select",
        "Standard",
    ),
    "buick": (
        "Avenir",
        "Essence",
        "Sport Touring",
        "Preferred",
        "Select",
        "Base",
    ),
    "chrysler": (
        "Pinnacle",
        "Limited",
        "Touring L",
        "Touring Plus",
        "Touring",
    ),
    "dodge": (
        "SRT Hellcat Redeye Widebody",
        "SRT Hellcat Redeye",
        "SRT Hellcat",
        "Hellcat",
        "Scat Pack Widebody",
        "Scat Pack",
        "R/T Scat Pack",
        "Daytona Scat Pack",
        "Daytona R/T Scat Pack",
        "Daytona",
        "Citadel",
        "R/T Plus",
        "R/T",
        "GT",
        "Limited",
        "Premium",
        "SXT Plus",
        "SXT",
        "Pursuit",
        "Crew",
        "Sport",
        "SE Plus",
        "SE",
        "Express",
    ),
    "tesla": (
        "Plaid",
        "Performance",
        "Long Range",
        "Standard Range",
        "Base",
    ),
    "rivian": (
        "Ascend",
        "Adventure",
        "Explore",
    ),
    "porsche": (
        "GT3 RS",
        "GT3",
        "Turbo S",
        "Turbo",
        "GTS",
        "Carrera S",
        "S",
        "Carrera",
        "Base",
    ),
    "volvo": (
        "Ultra",
        "Ultimate",
        "Plus",
        "Core",
    ),
    "genesis": (
        "Sport Prestige",
        "Prestige",
        "Advanced",
        "Base",
    ),
    "landrover": (
        "Autobiography",
        "SV",
        "HSE",
        "HST",
        "SE",
        "Standard",
    ),
    "infiniti": (
        "Autograph",
        "Sensory",
        "Luxe",
        "Pure",
    ),
}

# Model-specific trim ladders (luxury-first). Used before make-wide lists.
MODEL_TRIM_ORDER: dict[tuple[str, str], tuple[str, ...]] = {
    ("bmw", "x2"): (
        "M35i",
        "xDrive28i",
        "sDrive28i",
    ),
    ("bmw", "i4"): (
        "M50 Gran Coupe",
        "M50",
        "M60 xDrive",
        "xDrive40",
        "eDrive40",
        "eDrive35",
    ),
    ("bmw", "x3"): (
        "M Competition",
        "M40i",
        "xDrive30e",
        "xDrive30i",
        "sDrive30i",
        "xDrive28i",
    ),
    ("bmw", "x5"): (
        "M Competition",
        "M60i xDrive",
        "M60i",
        "xDrive50i",
        "M50i",
        "xDrive45e",
        "xDrive40i",
        "sDrive40i",
    ),
    ("bmw", "x6"): (
        "M Competition",
        "M60i xDrive",
        "M60i",
        "xDrive40i",
    ),
    ("bmw", "i5"): (
        "M60 xDrive",
        "xDrive40",
        "eDrive40",
    ),
    ("bmw", "5series"): (
        "M550i",
        "550e",
        "540i",
        "530i",
        "530e",
    ),
    ("bmw", "3series"): (
        "M340i",
        "340i",
        "330e",
        "330i",
        "328i",
        "335i",
        "320i",
    ),
    ("bmw", "ix"): (
        "xDrive50",
        "xDrive40",
        "M60",
    ),
    ("bmw", "i7"): (
        "xDrive60",
        "eDrive50",
    ),
    ("jeep", "wagoneer"): (
        "L Series III",
        "L Series II",
        "L Series I",
        "Series III",
        "Series II",
        "Series I",
        "Carbide",
        "Obsidian",
        "Launch Edition",
    ),
    ("jeep", "grandcherokee"): (
        "Trackhawk",
        "SRT",
        "Summit Reserve",
        "Summit",
        "High Altitude",
        "Overland",
        "Trailhawk",
        "Limited X",
        "Limited",
        "Laredo",
    ),
    ("honda", "accord"): (
        "Touring Hybrid",
        "Sport-L Hybrid",
        "EX-L Hybrid",
        "Sport Hybrid",
        "SE",
        "LX",
    ),
    ("jeep", "renegade"): (
        "Trailhawk",
        "Limited",
        "Latitude",
        "Sport",
        "Altitude",
    ),
    ("chevrolet", "captivasport"): (
        "LTZ",
        "LT",
        "LS",
    ),
    ("volvo", "ex90"): (
        "Ultra",
        "Ultimate",
        "Plus",
        "Core",
    ),
    ("volvo", "ex30"): (
        "Ultra",
        "Plus",
        "Core",
    ),
    ("toyota", "bz"): (
        "Limited",
        "Woodland",
        "XLE",
        "AWD Limited",
        "AWD",
    ),
    ("toyota", "crownsignia"): (
        "Limited",
        "XLE",
    ),
    ("toyota", "grsupra"): (
        "3.0 Premium",
        "3.0",
        "2.0",
    ),
    ("toyota", "corolla"): (
        "XSE",
        "SE",
        "XLE",
        "LE",
        "Hybrid",
    ),
    ("chrysler", "pacifica"): (
        "Pinnacle",
        "Limited",
        "Touring L",
        "Touring Plus",
        "Touring",
        "Select",
        "Hybrid",
    ),
    ("mini", "cooper"): (
        "Iconic",
        "Signature Plus",
        "Classic",
        "Cooper S",
        "Cooper",
    ),
    ("bmw", "2series"): (
        "M240i",
        "M235",
        "230i",
        "228",
    ),
    ("bmw", "4series"): (
        "M440i",
        "430i",
        "428i",
    ),
    ("bmw", "8series"): (
        "M850i",
        "840i",
        "840",
    ),
    ("audi", "a8"): (
        "L",
        "60 TFSI",
        "55 TFSI",
        "Premium Plus",
        "Premium",
    ),
    ("audi", "r8"): (
        "V10 performance",
        "V10 Plus",
        "V10 RWD",
        "V10 quattro",
        "RWD",
        "AWD",
    ),
    ("audi", "r8spyder"): (
        "V10 performance",
        "V10 Plus",
        "V10 quattro",
        "RWD",
        "AWD",
    ),
    ("audi", "q6etron"): (
        "Premium Plus",
        "Premium",
        "Prestige",
        "Ultra",
        "quattro",
    ),
    ("audi", "q8etron"): (
        "Premium Plus",
        "Premium",
        "Prestige",
        "S line",
    ),
    ("audi", "rs6"): (
        "RS 6 Avant",
        "RS 6",
        "RS6",
    ),
    ("audi", "q5"): (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
        "Sport",
    ),
    ("audi", "a4"): (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
        "Sport",
    ),
    ("audi", "a6"): (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
    ),
    ("audi", "a3"): (
        "Premium Plus",
        "Premium",
        "S line",
    ),
    ("audi", "q7"): (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
    ),
    ("audi", "q8"): (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
    ),
    ("audi", "q8"): (
        "Prestige",
        "Premium Plus",
        "Premium",
        "S line",
    ),
    ("jeep", "compass"): (
        "Limited",
        "Trailhawk",
        "High Altitude",
        "Latitude Lux",
        "Latitude",
        "Altitude",
        "Sport",
    ),
    ("landrover", "rangerover"): (
        "Autobiography",
        "SV",
        "HSE",
        "HST",
        "SE",
    ),
    ("tesla", "model3"): (
        "Plaid",
        "Performance",
        "Long Range",
        "Standard Range",
    ),
    ("tesla", "modely"): (
        "Plaid",
        "Performance",
        "Long Range",
        "Standard Range",
    ),
    ("volvo", "xc90"): (
        "T8",
        "B6",
        "B5",
    ),
    ("gmc", "yukon"): (
        "Denali Ultimate",
        "Denali",
        "AT4",
        "SLT",
        "SLE",
        "Elevation",
    ),
    ("toyota", "tacoma"): (
        "TRD Pro",
        "TRD Off-Road",
        "TRD Sport",
        "Limited",
        "SR5",
        "SR",
    ),
    ("toyota", "landcruiser"): (
        "First Edition",
        "1958",
        "Land Cruiser",
    ),
    ("toyota", "rav4prime"): (
        "XSE",
        "SE",
    ),
    ("nissan", "pathfinder"): (
        "Platinum",
        "SL",
        "SV",
        "S",
    ),
    ("porsche", "taycan"): (
        "Turbo S",
        "Turbo",
        "GTS",
        "4S",
        "Base",
    ),
    ("bmw", "z4"): (
        "M40i",
        "sDrive30i",
    ),
    ("bmw", "m2"): (
        "CS",
        "Competition",
        "Base",
    ),
    ("kia", "k5"): (
        "GT",
        "GT-Line",
        "EX",
        "LXS",
        "LX",
    ),
    ("mazda", "mazda3"): (
        "Turbo Premium Plus",
        "Turbo",
        "Premium Plus",
        "Preferred",
        "Select",
    ),
    ("cadillac", "escalade"): (
        "V-Series",
        "Platinum",
        "Premium Luxury",
        "Luxury",
        "Sport",
    ),
    ("cadillac", "xt4"): (
        "Premium Luxury",
        "Sport",
        "Luxury",
    ),
    ("jeep", "grandwagoneer"): (
        "Summit Reserve",
        "Summit",
        "Obsidian",
        "Carbide",
        "Upland",
        "Series III",
        "Series II",
        "Series I",
    ),
}

_BMW_MOTOR_TRIM_RE = re.compile(
    r"^(?:[xs]Drive\d{2}[ie]|M\d{2,3}i|M60i(?:xDrive)?|AlpinaXB7|M Competition|M Sport)$",
    re.I,
)
_BMW_EV_TRIM_RE = re.compile(r"\b(M60\s*xDrive|xDrive\d{2}|eDrive\d{2})\b", re.I)
_JEEP_WAGONEER_SERIES_TRIM_RE = re.compile(
    r"^(?:L Series [IVX]+|Series [IVX]+|Carbide|Obsidian|Launch Edition)$",
    re.I,
)

# Universal fallback when make-specific extraction finds nothing.
GENERIC_TRIM_ORDER: tuple[str, ...] = (
    "Platinum",
    "Limited",
    "Premium",
    "Touring",
    "XLE",
    "SEL",
    "LT",
    "Sport",
    "EX",
    "SE",
    "LE",
    "S",
    "Base",
)

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
_DIMENSION_TAIL_RE = re.compile(
    r"\s*:?\s*\d+(?:\.\d+)?\s*(?:in|mm|inches)\b.*$",
    re.I,
)
_PAREN_DIMENSION_RE = re.compile(
    r"\(\s*\d+(?:\.\d+)?\s*(?:in|mm|inches)?[^)]*\)",
    re.I,
)
_COLON_SUFFIX_RE = re.compile(r"\s*:\s*.+$")
_NON_TRIM_NAME_RE = re.compile(
    r"^(?:wheelbase|trim levels?|engine|transmission|varies\b|model\b|standard\b|optional\b|and\b|or\b|the\b)",
    re.I,
)
_ENGINE_TRIM_RE = re.compile(
    r"\b(?:\d+\.\d+\s*l\b|\d+\s*l\b|v\d|v-\d|turbo|diesel|cummins|hemi|pentastar|"
    r"ecoboost|hybrid|electric|i\d|tfsi|tsi|dohc|sohc|ohv|hp\b|kw\b|nm\b|"
    r"cylinder|liter|litre)\b",
    re.I,
)
_TRANSMISSION_TRIM_RE = re.compile(
    r"\b(?:\d+[\s-]*speed|automatic|manual|cvt|torqueflite|aisin|zf\b|"
    r"transmission|dual[\s-]*clutch|dct|e-cvt)\b",
    re.I,
)
_FEATURE_FRAGMENT_RE = re.compile(
    r"\b(?:grille|headlamp|head\s*lamp|taillamp|tail\s*lamps?|bumper|fender|"
    r"seat|seats|vinyl|cloth|decal|mirror|wiper|exhaust|muffler|spoiler|"
    r"running\s*boards?|step\s*bars?|plastic|chrome|aluminum|steel\s*wheel|"
    r"incandescent|halogen|projector)\b",
    re.I,
)
_SENTENCE_FRAGMENT_RE = re.compile(
    r"^(?:a|an|and|or|the|with|for|from|includes?|featuring)\s+",
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
        "hybrid",
        "electric",
        "gasoline",
        "diesel",
    }
)
# EPA / wiki CSV section headers and prose fragments — never trim levels.
_JUNK_TRIM_EXACT = frozenset(
    {
        "an",
        "and",
        "or",
        "the",
        "na",
        "n/a",
        "bmw",
        "none",
        "unknown",
        "package",
        "packages",
        "options",
        "option",
        "badges",
        "badge",
        "wheels",
        "wheel",
        "tires",
        "tire",
        "engines",
        "engine",
        "transmission",
        "transmissions",
        "layout",
        "body",
        "bodym",
        "body-m",
        "specialinteriors",
        "specialinterior",
        "interiors",
        "exterior",
        "upholstery",
        "paint",
        "accessories",
        "medseats",
        "seats",
        "note",
        "includes",
        "offered",
        "available",
        "discontinued",
    }
)
_JUNK_TRIM_SUBSTR_RE = re.compile(
    r"\b(?:internet\s+movie|imcdb|movie\s+cars\s+database|authority\s+control|"
    r"wikipedia|wikidata|carbuzz|discontinued|database|databases|"
    r"bronze\s+package|co-?pilot|assist\+|sunroof|innovative\s+technologies|"
    r"mazda-?based|served\s+as|accessories\s+were|and\s+options|"
    r"now\s+available|had\s+an\s+available|med\s+seats|special\s+interiors?|"
    r"body-?m\b|trim\s+levels?)\b",
    re.I,
)
_SHORT_PERFORMANCE_TRIM_RE = re.compile(
    r"^(?:[A-Z]{1,4}\d{0,2}|[A-Z]/[A-Z]|GT\d*|ST\d*|RS|SS|SE|LE|LX|EX|LT|LS|XL|XLT|"
    r"SRT|R/T|TRX|TRD|FX\d*|STX|GTD|HFE)(?:\s+[A-Z][a-z]+){0,2}$"
)


def _norm_make_key(make: str) -> str:
    t = re.sub(r"[^a-z0-9]+", "", (make or "").lower())
    if t in ("chevrolet", "chevy"):
        return "chevrolet"
    if t in ("mercedesbenz", "mercedes"):
        return "mercedesbenz"
    return t


def _norm_model_key(model: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (model or "").lower())


def _trim_year_window_key(trim_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (trim_name or "").lower())


# Listing year must fall in one window for the trim to appear on the ladder.
# Single (year_min, year_max) or multiple eras: ((y0, y1), (y2, y3)).
TRIM_YEAR_WINDOWS: dict[tuple[str, str, str], tuple[int, ...]] = {
    # --- Dodge Charger LD (2015–2023) + FR (2024+) ---
    ("dodge", "charger", "srthellcatredeyewidebody"): (2020, 2023),
    ("dodge", "charger", "srthellcatredeye"): (2019, 2023),
    ("dodge", "charger", "srthellcat"): (2015, 2023),
    ("dodge", "charger", "hellcat"): (2015, 2023),
    ("dodge", "charger", "scatpackwidebody"): (2019, 2023),
    ("dodge", "charger", "scatpack"): (2015, 2023, 2024, 2030),
    ("dodge", "charger", "rtscatpack"): (2024, 2030),
    ("dodge", "charger", "daytonascatpack"): (2023, 2030),
    ("dodge", "charger", "daytona"): (2017, 2018, 2024, 2030),
    ("dodge", "charger", "gt"): (2017, 2023),
    ("dodge", "charger", "sxt"): (2015, 2023),
    ("dodge", "charger", "se"): (2015, 2019),
    # --- Dodge Challenger (shared performance trims) ---
    ("dodge", "challenger", "srthellcatredeyewidebody"): (2019, 2023),
    ("dodge", "challenger", "srthellcatredeye"): (2019, 2023),
    ("dodge", "challenger", "srthellcat"): (2015, 2023),
    ("dodge", "challenger", "hellcat"): (2015, 2023),
    ("dodge", "challenger", "scatpackwidebody"): (2019, 2023),
    ("dodge", "challenger", "scatpack"): (2015, 2023),
    ("dodge", "challenger", "rtscatpack"): (2015, 2016),
    ("dodge", "challenger", "srt392"): (2015, 2023),
    ("dodge", "challenger", "t/a"): (2017, 2018),
    ("dodge", "challenger", "gt"): (2017, 2023),
    ("dodge", "challenger", "sxt"): (2015, 2023),
    # --- Ram 1500 ---
    ("ram", "1500", "tungsten"): (2025, 2030),
    ("ram", "1500", "trx"): (2021, 2023),
    ("gmc", "yukon", "at4"): (2021, 2030),
    ("gmc", "yukon", "denaliultimate"): (2022, 2030),
    # --- Jeep Grand Cherokee WK2 / WL ---
    ("jeep", "grandcherokee", "trackhawk"): (2018, 2021),
    ("jeep", "grandcherokee", "srt"): (2012, 2021),
    ("jeep", "grandcherokee", "limitedx"): (2020, 2021),
    ("jeep", "grandcherokee", "summitreserve"): (2022, 2030),
    # --- BMW X5 (G05) ---
    ("bmw", "x5", "m60ixdrive"): (2024, 2030),
    ("bmw", "x5", "m60i"): (2024, 2030),
    ("bmw", "x5", "xdrive50i"): (2019, 2023),
    ("bmw", "x5", "m50i"): (2019, 2023),
    ("bmw", "x5", "xdrive45e"): (2021, 2030),
    ("bmw", "x5", "45e"): (2021, 2030),
    # --- BMW X6 (G06) ---
    ("bmw", "x6", "m60ixdrive"): (2024, 2030),
    ("bmw", "x6", "m60i"): (2024, 2030),
    ("bmw", "x6", "xdrive50i"): (2020, 2023),
    # --- Cadillac Escalade (5th gen) ---
    ("cadillac", "escalade", "vseries"): (2022, 2030),
}


def trim_step_year_windows(
    make: str,
    model: str | None,
    trim_name: str,
) -> list[tuple[int, int]]:
    """Return applicable model-year windows for a trim, or [] if unconstrained."""
    mk, mod = _resolve_trim_model_key(make, model)
    tok = _trim_year_window_key(trim_name)
    if not tok:
        return []
    raw = TRIM_YEAR_WINDOWS.get((mk, mod, tok))
    if not raw:
        return []
    if len(raw) == 2:
        return [(int(raw[0]), int(raw[1]))]
    out: list[tuple[int, int]] = []
    for i in range(0, len(raw) - 1, 2):
        out.append((int(raw[i]), int(raw[i + 1])))
    return out


_GENERIC_TRIM_ADD_RE = re.compile(
    r"(?:equipment and features|factory equipment and features|equipment and packaging)"
    r".*(?:typical of|typical .+ trim\b)",
    re.I,
)


_LADDER_PLACEHOLDER_PROSE_RE = re.compile(
    r"(?:"
    r"mid-level trim between\b"
    r"|rugged or adventure-oriented\b"
    r"|builds on .+ with additional comfort, technology, or appearance upgrades"
    r"|top of this trim lineup\b"
    r"|entry rung on this ladder\b"
    r"|typically the most equipment\b"
    r"|fewer optional upgrades than higher trims\b"
    r"|positioned below .+ on the .+ trim ladder\b"
    r"|positioned above .+ on the .+ trim ladder\b"
    r"|highest trim level offered on the\b"
    r"|base trim level on the\b"
    r"|adds premium audio, larger display, and comfort upgrades over the mid trim\b"
    r"|sits below .+ with fewer premium features\b"
    r")",
    re.I,
)


def is_generic_trim_add(text: str, trim_name: str | None = None) -> bool:
    """True for tautological placeholder lines that restate the trim name."""
    s = (text or "").strip()
    if not s:
        return True
    if _LADDER_PLACEHOLDER_PROSE_RE.search(s):
        return True
    if re.search(r"\bmarketing trim level from oem brochure\b", s, re.I):
        return True
    if re.search(r"\bauthorized\b.*\bcenter\b|\bbmwusa\.com\b", s, re.I):
        return True
    if _GENERIC_TRIM_ADD_RE.search(s):
        return True
    if len(s) < 20:
        return False
    if trim_name:
        tn = trim_name.strip()
        if tn and re.fullmatch(
            rf"(?:Equipment and features|Factory equipment and features) typical of the {re.escape(tn)} trim\.?",
            s,
            re.I,
        ):
            return True
        if re.fullmatch(rf"Typical .+ {re.escape(tn)} equipment and packaging\.?", s, re.I):
            return True
    if re.search(r"\btypical listing price near \$", s, re.I):
        return True
    return False


def sanitize_trim_adds(
    adds: list[str] | None,
    trim_name: str | None = None,
    *,
    max_items: int = 6,
) -> list[str]:
    """Drop placeholder / junk add lines; keep substantive feature bullets only."""
    from backend.enrichment.trim_spec_extractor import is_junk_spec_text

    out: list[str] = []
    seen: set[str] = set()
    for raw in adds or []:
        line = str(raw or "").strip()
        if not line or is_generic_trim_add(line, trim_name) or is_junk_spec_text(line):
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line[:240])
    if max_items <= 0:
        return out
    return out[:max_items]


def sanitize_brochure_trim_adds(adds: list[str] | None, trim_name: str | None = None) -> list[str]:
    """Brochure-sourced bullets: keep full OEM equipment list (minimal filtering)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in adds or []:
        line = str(raw or "").strip()
        if not line or is_generic_trim_add(line, trim_name):
            continue
        if len(line) < 8:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line[:280])
    return out[:80]


_TRIM_NAME_HINTS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("calligraphy",), "Flagship trim with premium materials, advanced driver assists, and exclusive design details."),
    (("pinnacle", "ultimate"), "Highest trim level with the most luxury features and technology."),
    (("limited", "platinum", "prestige", "premium plus"), "Upper trim with upgraded interior, expanded tech, and additional comfort features."),
    (("n line", "n-line"), "Sport-styled trim with unique exterior accents, wheels, and interior touches."),
    (("hellcat", "scat pack", "srt", "trd pro", "m sport", "m package"), "High-performance trim with upgraded power, brakes, and sport suspension."),
    (("sel premium", "xle premium", "premium plus"), "Adds premium audio, larger display, and comfort upgrades over the mid trim."),
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


def _resolve_trim_model_key(make: str, model: str | None) -> tuple[str, str]:
    """Map listing model strings to trim-ladder model keys."""
    mk = _norm_make_key(make)
    mod = _norm_model_key(model)
    if mk == "jeep":
        if mod.startswith("wagoneer"):
            return mk, "wagoneer"
        if mod.startswith("grandwagoneer"):
            return mk, "grandwagoneer"
        if "grandcherokee" in mod:
            return mk, "grandcherokee"
    if mk == "mini":
        if mod in {"2door", "4door", "convertible", "cooper", "coopers", "hardtop"}:
            return mk, "cooper"
        if "countryman" in mod:
            return mk, "countryman"
    if mk == "toyota":
        if "crownsignia" in mod or mod == "crownsignia":
            return mk, "crownsignia"
        if mod.startswith("toyotacrown"):
            return mk, "crown"
        if "grsupra" in mod or mod == "supra":
            return mk, "grsupra"
        if mod.startswith("corollahybrid") or mod.startswith("corollahatchback"):
            return mk, "corolla"
        if mod.startswith("bz"):
            return mk, "bz"
        if mod.startswith("tacoma"):
            return mk, "tacoma"
        if mod.startswith("landcruiser"):
            return mk, "landcruiser"
        if mod.startswith("rav4prime"):
            return mk, "rav4prime"
    if mk == "bmw":
        if mod.startswith("2series"):
            return mk, "2series"
        if mod.startswith("4series"):
            return mk, "4series"
        if mod.startswith("8series"):
            return mk, "8series"
        if mod.startswith("z4"):
            return mk, "z4"
        if mod == "m2" or mod.startswith("m2"):
            return mk, "m2"
    if mk == "chevrolet" and "captiva" in mod:
        return mk, "captivasport"
    if mk == "volvo":
        if mod.startswith("ex90"):
            return mk, "ex90"
        if mod.startswith("ex30"):
            return mk, "ex30"
    if mk == "chrysler" and mod == "pacifica":
        return mk, "pacifica"
    if mk == "audi":
        if mod.startswith("r8spyder"):
            return mk, "r8spyder"
        if mod.startswith("r8"):
            return mk, "r8"
        if "q6etron" in mod or (mod.startswith("q6") and "etron" in mod):
            return mk, "q6etron"
        if "q8etron" in mod or (mod.startswith("q8") and "etron" in mod):
            return mk, "q8etron"
        if mod.startswith("a8"):
            return mk, "a8"
        if mod.startswith("q5") or mod.startswith("sq5"):
            return mk, "q5"
        if mod.startswith("a4") or mod.startswith("a5") or mod.startswith("s4") or mod.startswith("s5"):
            return mk, "a4"
        if mod.startswith("a6") or mod.startswith("s6"):
            return mk, "a6"
        if mod.startswith("a3") or mod.startswith("s3"):
            return mk, "a3"
        if mod.startswith("q7"):
            return mk, "q7"
        if mod.startswith("q8"):
            return mk, "q8"
        if mod.startswith("rs6"):
            return mk, "rs6"
        if "etron" in mod:
            return mk, mod
    if mk == "landrover" and "rangerover" in mod:
        return mk, "rangerover"
    if mk == "tesla":
        if mod.startswith("model3"):
            return mk, "model3"
        if mod.startswith("modely"):
            return mk, "modely"
        if mod.startswith("models"):
            return mk, "models"
        if mod.startswith("modelx"):
            return mk, "modelx"
    if mk == "volvo" and mod.startswith("xc90"):
        return mk, "xc90"
    if mk == "gmc" and mod.startswith("yukon"):
        return mk, "yukon"
    if mk == "nissan" and mod.startswith("pathfinder"):
        return mk, "pathfinder"
    if mk == "porsche" and mod.startswith("taycan"):
        return mk, "taycan"
    if mk == "kia" and mod.startswith("k5"):
        return mk, "k5"
    if mk == "mazda" and mod.startswith("mazda3"):
        return mk, "mazda3"
    if mk == "cadillac" and mod.startswith("escalade"):
        return mk, "escalade"
    if mk == "cadillac" and mod.startswith("xt4"):
        return mk, "xt4"
    if mk == "dodge" and mod.startswith("charger"):
        return mk, "charger"
    if mk == "dodge" and mod.startswith("challenger"):
        return mk, "challenger"
    return mk, mod


_EPA_MODEL_SEARCH_NAMES: dict[tuple[str, str], str] = {
    ("mini", "cooper"): "Cooper",
    ("mini", "countryman"): "Countryman",
    ("toyota", "crownsignia"): "Crown Signia",
    ("toyota", "grsupra"): "GR Supra",
    ("toyota", "corolla"): "Corolla",
    ("toyota", "bz"): "bZ",
    ("bmw", "2series"): "2 Series",
    ("bmw", "4series"): "4 Series",
    ("bmw", "8series"): "8 Series",
    ("jeep", "wagoneer"): "Wagoneer",
    ("jeep", "grandwagoneer"): "Grand Wagoneer",
    ("audi", "a8"): "A8",
    ("audi", "r8"): "R8",
    ("audi", "r8spyder"): "R8 Spyder",
    ("audi", "q6etron"): "Q6 e-tron",
    ("audi", "q8etron"): "Q8 e-tron",
    ("audi", "q5"): "Q5",
    ("audi", "a4"): "A4",
    ("audi", "a6"): "A6",
    ("audi", "a3"): "A3",
    ("audi", "q7"): "Q7",
    ("audi", "q8"): "Q8",
    ("audi", "rs6"): "RS 6",
    ("mercedesbenz", "glc"): "GLC-Class",
    ("mercedesbenz", "gle"): "GLE-Class",
    ("mercedesbenz", "gla"): "GLA-Class",
    ("mercedesbenz", "glb"): "GLB-Class",
    ("mercedesbenz", "gls"): "GLS-Class",
    ("mercedesbenz", "sl"): "SL-Class",
    ("mercedesbenz", "slclass"): "SL-Class",
    ("mercedesbenz", "slc"): "SLC-Class",
    ("mercedesbenz", "slk"): "SLK-Class",
    ("mercedesbenz", "cclass"): "C-Class",
    ("mercedesbenz", "eclass"): "E-Class",
    ("mercedesbenz", "cle"): "CLE-Class",
}


AUDI_DEALER_TRIM_TOKENS: tuple[str, ...] = (
    "Premium Plus",
    "Premium",
    "Prestige",
    "S line",
    "Ultra",
    "Progressiv",
    "Technik",
    "Komfort",
    "60 TFSI",
    "55 TFSI",
    "50 TFSI",
    "L",
)


def audi_dealer_trim_tokens(raw: str) -> list[str]:
    """Extract known Audi dealer trim tokens from a compound listing trim string."""
    text = str(raw or "").strip()
    if not text:
        return []
    found: list[str] = []
    for token in sorted(AUDI_DEALER_TRIM_TOKENS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(token)}\b", text, re.I):
            label = "S line" if token.lower() == "s line" else token
            if label not in found:
                found.append(label)
    return found


def epa_model_search_name(make: str, model: str | None) -> str:
    """Model label used when locating EPA CSV files."""
    mk, mod = _resolve_trim_model_key(make, model)
    return _EPA_MODEL_SEARCH_NAMES.get((mk, mod), (model or "").strip())


def _model_trim_order(make: str, model: str | None) -> tuple[str, ...]:
    key = _resolve_trim_model_key(make, model or "")
    return MODEL_TRIM_ORDER.get(key, ())


def _mercedes_motor_trim_label(raw: str) -> str | None:
    """Normalize Mercedes motor badges (SL400, SL63 AMG, GLC300, etc.)."""
    t = re.sub(r"\s+", " ", (raw or "").strip())
    if not t:
        return None

    m = re.match(r"^(?:AMG\s+)?SL([CK]?)\s*(\d{2,3})\s+AMG\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)} AMG"
    m = re.match(r"^SL([CK]?)(\d{2,3})\s+AMG\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)} AMG"
    m = re.match(r"^SL([CK]?)\s*(\d{2,3})\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)}"
    m = re.match(r"^SL([CK]?)(\d{2,3})\b", t, re.I)
    if m:
        body = f"SL{m.group(1).upper()}" if m.group(1) else "SL"
        return f"{body} {m.group(2)}"

    m = re.search(
        r"\b(AMG\s+[A-Z]{1,3}\s?\d{2,3}[a-z]?|GL[CESABLK]\s?\d{2,3}[a-z]?|C\s?\d{3}|E\s?\d{3}|S\s?\d{3})\b",
        t,
        re.I,
    )
    if m:
        return re.sub(r"\s+", " ", m.group(1).strip())
    return None


def preserve_trim_label(raw: str, make: str, model: str | None = None) -> str:
    """Keep motor/series trim tokens that generic canonicalization would drop."""
    t = str(raw or "").strip()
    if not t:
        return ""

    mk_early = _norm_make_key(make)
    if mk_early == "audi":
        if t.upper() in {"AWD", "RWD"}:
            return t.upper()
        spyder_drv = re.match(r"^Spyder\s+(AWD|RWD)$", t, re.I)
        if spyder_drv:
            return f"Spyder {spyder_drv.group(1).upper()}"

    t = _COLON_SUFFIX_RE.sub("", t).strip()
    t = _PAREN_DIMENSION_RE.sub("", t).strip()
    t = _DIMENSION_TAIL_RE.sub("", t).strip()
    for _ in range(4):
        t = _DRIVETRAIN_INLINE_RE.sub(" ", t).strip()
    t = re.sub(r"\s+", " ", t).strip(" -–—")
    if not t:
        return ""

    mk, mod = _resolve_trim_model_key(make, model)
    if mk == "mercedesbenz":
        mb_label = _mercedes_motor_trim_label(t)
        if mb_label:
            return mb_label

    model_trims = _model_trim_order(make, model)
    if model_trims:
        for known in sorted(model_trims, key=len, reverse=True):
            if t.lower() == known.lower():
                return known
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known

    if mk == "bmw" and _BMW_MOTOR_TRIM_RE.match(t.replace(" ", "")):
        if re.match(r"^M60i(?:\s+xDrive)?$", t, re.I):
            return "M60i xDrive" if re.search(r"xDrive", t, re.I) else "M60i"
        if re.match(r"^Alpina\s+XB7$", t, re.I):
            return "Alpina XB7"
        return t
    if mk == "bmw" and mod in {"x1", "x2", "x3", "x4", "x5", "x6", "x7"}:
        sav = re.search(r"\bX\d\s+(\d{2}[ie])\b", t, re.I)
        if sav:
            return sav.group(1).lower()
        sav_e = re.search(r"\bX\d\s+(\d{2}e)\b", t, re.I)
        if sav_e:
            return sav_e.group(1).lower()
    if mk == "bmw" and re.match(r"^[xs]Drive\d{2}[ie]$", t.replace(" ", ""), re.I):
        return t[0].lower() + t[1:] if t[0].isupper() else t
    if mk == "bmw" and re.match(r"^M\d{2,3}i$", t, re.I):
        return t.upper() if t.islower() else t
    if mk == "bmw":
        ev = _BMW_EV_TRIM_RE.search(t)
        if ev:
            label = ev.group(1).replace(" ", "")
            low = label.lower()
            if low.startswith("edrive"):
                return "eDrive" + label[6:]
            if low.startswith("xdrive"):
                return "xDrive" + label[6:]
            if low.startswith("m60xdrive"):
                return "M60 xDrive"
        motor = re.search(r"\b(M?\d{3}[ie]?)\b", t, re.I)
        if motor and (
            mod in {"3series", "5series", "7series"}
            or re.match(r"^\d{3}[ie]?$", mod)
        ):
            s = motor.group(1)
            if s.upper().startswith("M"):
                base = s[0].upper() + s[1:].lower()
            else:
                base = s.lower()
            if re.search(r"\bxDrive\b", t, re.I):
                return f"{base} xDrive"
            return base
        motor = re.search(r"\b(M?\d{3}i)\b", t, re.I)
        if motor:
            s = motor.group(1)
            if s.upper().startswith("M"):
                return s[0].upper() + s[1:].lower()
            return s.lower()
        plain = re.search(r"\b(M?\d{3})\b", t, re.I)
        if plain and not re.search(r"\b\d{4}\b", t):
            s = plain.group(1)
            if s.upper().startswith("M"):
                return s[0].upper() + s[1:].lower()
            return s.lower()

    if mk == "mini" and mod == "cooper":
        m = re.match(r"^C\s+(\d\s+Door)", t, re.I)
        if m:
            return "Cooper " + m.group(1).replace(" ", "")
        m = re.match(r"^S\s+(\d\s+Door)", t, re.I)
        if m:
            return "Cooper S " + m.group(1).replace(" ", "")

    if mk == "mercedesbenz":
        mb_label = _mercedes_motor_trim_label(t)
        if mb_label:
            return mb_label

    if mk == "toyota" and mod in ("bz", "bzwoodland", "corolla", "corollahybrid", "corollahatchback"):
        m = re.match(r"^(AWD\s*)?(Woodland|LIMITED|Hybrid(?:\s+SE|\s+AWD|\s+SE\s+AWD)?|GR\s+Corolla|Hatchback(?:\s+XSE|\s+FX)?|SE|LE|XLE|XSE)\b", t, re.I)
        if m:
            label = re.sub(r"\s+", " ", m.group(0).strip())
            return label.title() if label.isupper() else label

    if mk == "audi":
        for known in ("Premium Plus", "Premium", "Prestige", "Progressiv", "Technik", "Komfort", "S line", "S Line"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return "S line" if known.lower().replace(" ", "") == "sline" else known
        if re.fullmatch(r"L", t, re.I):
            return "L"
        if t.lower() in {"ultra", "quattro"}:
            return t.title()
        if t.upper() in {"AWD", "RWD"}:
            return t.upper()
        m = re.match(r"^(Spyder\s+)?(AWD|RWD)$", t, re.I)
        if m:
            return ("Spyder " if m.group(1) else "") + m.group(2).upper()
        m = re.search(r"\b(RS\s?6|RS6)\b", t, re.I)
        if m:
            return "RS 6" if " " in m.group(1) else "RS6"
        m = re.search(r"\b(\d{2})\s*TFSI\b", t, re.I)
        if m:
            return f"{m.group(1)} TFSI"
        m = re.search(r"\bV10(?:\s+(?:Plus|performance|RWD|quattro))?\b", t, re.I)
        if m:
            return m.group(0).title().replace("Rwd", "RWD").replace("Quattro", "quattro")

    if mk == "jeep" and mod in ("wagoneer", "grandwagoneer"):
        m = re.match(r"^(L Series [IVX]+|Series [IVX]+|Carbide|Obsidian|Launch Edition)", t, re.I)
        if m:
            label = m.group(1)
            parts: list[str] = []
            for part in label.split():
                up = part.upper()
                if up in {"I", "II", "III", "IV", "V", "VI"}:
                    parts.append(up)
                elif part.lower() == "series":
                    parts.append("Series")
                elif part.lower() == "l":
                    parts.append("L")
                else:
                    parts.append(part.title())
            return " ".join(parts)

    if mk == "volvo":
        m = re.match(r"^(T\d+|B\d+)", t, re.I)
        if m:
            return m.group(1).upper()
    if mk == "gmc" and mod == "yukon":
        for known in ("Denali Ultimate", "AT4X", "AT4", "Denali", "SLT", "Elevation", "SLE"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "tesla":
        for known in ("Plaid", "Performance", "Long Range", "Standard Range"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "porsche" and mod == "taycan":
        for known in ("Turbo S", "Turbo", "GTS", "4S"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "toyota" and mod == "tacoma":
        for known in ("TRD Pro", "TRD Off-Road", "TRD Sport", "Limited", "SR5", "SR"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "landrover" and mod == "rangerover":
        for known in ("Autobiography", "SV", "HSE", "HST", "SE"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "cadillac" and mod == "escalade":
        if re.fullmatch(r"V\s*AWD", t, re.I):
            return "V-Series"
        for known in ("V-Series", "Platinum", "Premium Luxury", "Luxury", "Sport"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known
    if mk == "cadillac" and mod == "xt4":
        for known in ("Premium Luxury", "Sport", "Luxury"):
            if re.search(rf"\b{re.escape(known)}\b", t, re.I):
                return known

    return ""


def ordered_trim_candidates(make: str, model: str | None = None) -> tuple[str, ...]:
    """Luxury-first order: index 0 = top of ladder (most luxurious)."""
    key = _norm_make_key(make)
    seen: set[str] = set()
    out: list[str] = []
    for name in (*_model_trim_order(make, model), *MAKE_TRIM_ORDER.get(key, ()), *GENERIC_TRIM_ORDER):
        tok = re.sub(r"[^a-z0-9]+", "", name.lower())
        if tok and tok not in seen:
            seen.add(tok)
            out.append(name)
    return tuple(out)


def ordered_trim_candidates_longest_first(make: str, model: str | None = None) -> tuple[str, ...]:
    return tuple(sorted(ordered_trim_candidates(make, model), key=len, reverse=True))


def known_trim_names_for_make(make: str, model: str | None = None) -> frozenset[str]:
    names = {n.lower() for n in (*ordered_trim_candidates(make, model), *GENERIC_TRIM_ORDER)}
    return frozenset(names)


def trim_name_is_acceptable(name: str, make: str, model: str | None = None) -> bool:
    """True when *name* is a known OEM trim or a short plausible performance label."""
    low = (name or "").strip().lower()
    if low in known_trim_names_for_make(make, model):
        return True
    if preserve_trim_label(name, make, model):
        return True
    if not _is_valid_trim_label(name):
        return False
    tok = re.sub(r"[^a-z0-9]+", "", low)
    if tok in _JUNK_TRIM_EXACT:
        return False
    if _JUNK_TRIM_SUBSTR_RE.search(name):
        return False
    if _SHORT_PERFORMANCE_TRIM_RE.match(name.strip()):
        return True
    # Multi-word only when every word is a known trim token (e.g. "Sport S", "TRD Pro").
    words = low.split()
    if 1 < len(words) <= 4:
        known = known_trim_names_for_make(make, model)
        if all(w in known or re.sub(r"[^a-z0-9]+", "", w) in known for w in words):
            return True
    return False


def _is_valid_trim_label(name: str) -> bool:
    if not name or len(name) < 2 or len(name) > 48:
        return False
    if _NON_TRIM_NAME_RE.match(name):
        return False
    if _SENTENCE_FRAGMENT_RE.match(name):
        return False
    if _ENGINE_TRIM_RE.search(name):
        return False
    if _TRANSMISSION_TRIM_RE.search(name):
        return False
    if _FEATURE_FRAGMENT_RE.search(name):
        return False
    words = name.split()
    if len(words) > 6:
        return False
    if name.isupper() and len(name) > 28:
        return False
    if re.search(r"\b(?:in|on|with|for|from|at)\s+the\b", name, re.I):
        return False
    if re.search(r"\b\d{4}\b", name):
        return False
    if _JUNK_TRIM_SUBSTR_RE.search(name):
        return False
    tok = re.sub(r"[^a-z0-9]+", "", name.lower())
    if tok in _JUNK_TRIM_TOKENS or tok in _JUNK_TRIM_EXACT or tok.isdigit():
        return False
    if re.match(r"^[A-Za-z]{1,4}\d{1,2}$", name.strip()):
        return True
    if _SHORT_PERFORMANCE_TRIM_RE.match(name.strip()):
        return True
    alpha = sum(1 for ch in name if ch.isalpha())
    if alpha < max(3, len(name) // 4):
        return False
    return True


def canonical_trim_name(raw: str, make: str, model: str | None = None) -> str:
    """
    Normalize a trim label: drop drivetrain/wheelbase noise and map to a known OEM trim name.
    """
    preserved = preserve_trim_label(raw, make, model)
    if preserved:
        return preserved

    t = str(raw or "").strip()
    if not t:
        return ""
    if _norm_make_key(make) == "bmw":
        m = re.match(r"^(\d{2})i(\s|/|$)", t, re.I)
        if m and 18 <= int(m.group(1)) <= 49:
            t = re.sub(r"^(\d{2})i", rf"3{m.group(1)}i", t, count=1, flags=re.I)
    t = _COLON_SUFFIX_RE.sub("", t).strip()
    t = _PAREN_DIMENSION_RE.sub("", t).strip()
    t = _DIMENSION_TAIL_RE.sub("", t).strip()
    for _ in range(4):
        t = _DRIVETRAIN_INLINE_RE.sub(" ", t).strip()
    t = re.sub(r"\s+", " ", t).strip(" -–—")
    if not t:
        return ""

    for known in ordered_trim_candidates_longest_first(make, model):
        if re.search(rf"\b{re.escape(known)}\b", t, re.I):
            return known

    # Title-case short unknown tokens (e.g. "sr5" from prose)
    if t.isupper() and len(t) <= 12:
        t = t.title()
    elif t == t.lower():
        t = t.title()

    if not _is_valid_trim_label(t):
        return ""
    return t if trim_name_is_acceptable(t, make, model) else ""


def _bmw_luxury_sort_key(trim_name: str) -> tuple[int, int, int, int] | None:
    """
    BMW motor-trim ordering (lower tuple = more luxurious, top of ladder).

    Rules:
    - M / Competition above numeric motor trims
    - Higher motor number above lower (40i above 30i above 35 eDrive)
    - xDrive above sDrive above eDrive when motor tier matches
    - PHEV (30e) above gas (30i) at same motor tier and drivetrain
    """
    t = (trim_name or "").lower()
    compact = re.sub(r"[^a-z0-9]+", "", t)
    if not compact:
        return None

    drive = 0 if "xdrive" in compact else (1 if "sdrive" in compact else (2 if "edrive" in compact else 1))

    if "competition" in t and re.search(r"\bm\b", t):
        return (0, 0, drive, 0)

    m = re.search(r"m(\d{2,3})", compact)
    if m and compact.startswith("m"):
        return (1, -int(m.group(1)), drive, 0)

    core = compact.replace("xdrive", "").replace("sdrive", "")
    sm = re.match(r"^(\d{3})([ie])?$", core)
    if sm:
        num = int(sm.group(1))
        suffix = 0 if sm.group(2) == "e" else (1 if sm.group(2) == "i" else 2)
        return (2, -num, drive, suffix)

    dm = re.search(r"(?:x|s|e)?drive(\d{2})([ie])?", compact)
    if dm:
        num = int(dm.group(1))
        suffix = 0 if dm.group(2) == "e" else (1 if dm.group(2) == "i" else 2)
        return (2, -num, drive, suffix)

    plain = re.search(r"(\d{2})i", compact)
    if plain:
        return (2, -int(plain.group(1)), drive, 1)

    return None


def _mercedes_luxury_sort_key(trim_name: str) -> tuple[int, int, int] | None:
    """Sort Mercedes motor trims luxury-first (Maybach/600 → AMG → higher model numbers)."""
    t = re.sub(r"\s+", " ", (trim_name or "").strip())
    if not t:
        return None
    low = t.lower()

    model_num: int | None = None
    m = re.search(r"\b(?:GL[CESABLK]|CLS?|EQ[ESCB]|SL|G)\s*(\d{2,3})\b", t, re.I)
    if m:
        model_num = int(m.group(1))
    if model_num is None:
        m = re.search(r"\b(?:AMG\s+)?(?:\w+\s+)?(\d{2,3})\b", t, re.I)
        if m:
            model_num = int(m.group(1))

    if "maybach" in low or (model_num is not None and model_num >= 600 and re.search(r"\bGLS\s*600\b", t, re.I)):
        return (0, -(model_num or 600), 0)

    if "amg" in low:
        amg_num = model_num
        if amg_num is None:
            m2 = re.search(r"\bamg\s+(?:\w+\s+)?(\d{2,3})\b", t, re.I)
            amg_num = int(m2.group(1)) if m2 else 63
        return (1, -(amg_num or 63), 0)

    if model_num is not None:
        return (2, -model_num, 0)

    return None


def _mercedes_luxury_rank(trim_name: str) -> int | None:
    key = _mercedes_luxury_sort_key(trim_name)
    if key is None:
        return None
    tier, neg_num, suffix = key
    return tier * 10_000 + (-neg_num) * 100 + suffix


def _bmw_luxury_rank(trim_name: str) -> int | None:
    key = _bmw_luxury_sort_key(trim_name)
    if key is None:
        return None
    tier, neg_motor, drive, suffix = key
    return tier * 10_000 + (-neg_motor) * 100 + drive * 10 + suffix


def luxury_rank(trim_name: str, make: str, model: str | None = None) -> int:
    """Lower rank = more luxurious (top of ladder)."""
    if _norm_make_key(make) == "bmw":
        bmw_rank = _bmw_luxury_rank(trim_name)
        if bmw_rank is not None:
            return bmw_rank
    if _norm_make_key(make) == "mercedesbenz":
        mb_rank = _mercedes_luxury_rank(trim_name)
        if mb_rank is not None:
            return mb_rank
    order = ordered_trim_candidates(make, model)
    rank_map = {n.lower(): i for i, n in enumerate(order)}
    direct = rank_map.get((trim_name or "").lower())
    if direct is not None:
        return direct
    best = 9_999
    for known in ordered_trim_candidates_longest_first(make, model):
        if re.search(rf"\b{re.escape(known)}\b", trim_name or "", re.I):
            best = min(best, rank_map.get(known.lower(), 9_999))
    return best


def normalize_ladder_steps(
    steps: list[dict],
    make: str,
    *,
    model: str | None = None,
    limit: int = 14,
) -> list[dict]:
    """
    Canonicalize trim names, merge drivetrain duplicates, sort luxury → base (top → bottom).
    """
    by_key: dict[str, dict] = {}

    for step in steps or []:
        raw_name = str(step.get("name") or "")
        name = canonical_trim_name(raw_name, make, model) or preserve_trim_label(raw_name, make, model)
        if not name or not trim_name_is_acceptable(name, make, model):
            continue
        key = drivetrain_merge_key(name, make, model) or re.sub(r"[^a-z0-9]+", "", name.lower())
        aliases: list[str] = []
        for a in step.get("aliases") or []:
            raw = str(a).strip()
            if not raw:
                continue
            canonical = canonical_trim_name(raw, make, model)
            aliases.append(canonical or raw)
        aliases = [a for a in aliases if a.lower() != name.lower()]
        adds = [str(a).strip() for a in (step.get("adds") or []) if str(a).strip()]
        step_ymin = int(step.get("year_min") or 0)
        step_ymax = int(step.get("year_max") or 9999)
        price_note = str(step.get("inventory_price_note") or "").strip()

        if key not in by_key:
            by_key[key] = {
                "name": name,
                "aliases": list(dict.fromkeys(aliases)),
                "adds": [],
                "year_min": step_ymin,
                "year_max": step_ymax,
                "inventory_price_note": price_note,
            }
        entry = by_key[key]
        if price_note and not entry.get("inventory_price_note"):
            entry["inventory_price_note"] = price_note
        if step_ymin:
            entry["year_min"] = max(int(entry.get("year_min") or 0), step_ymin)
        if step_ymax < 9999:
            entry["year_max"] = min(int(entry.get("year_max") or 9999), step_ymax)
        for alias in aliases:
            if alias not in entry["aliases"]:
                entry["aliases"].append(alias)
        seen_adds = {a.lower() for a in entry["adds"]}
        for line in adds:
            if line.lower() not in seen_adds:
                seen_adds.add(line.lower())
                entry["adds"].append(line)

    ordered_keys = sorted(
        by_key.keys(),
        key=lambda k: (
            (0, _bmw_luxury_sort_key(by_key[k]["name"]))
            if _norm_make_key(make) == "bmw" and _bmw_luxury_sort_key(by_key[k]["name"]) is not None
            else (
                (0, _mercedes_luxury_sort_key(by_key[k]["name"]))
                if _norm_make_key(make) == "mercedesbenz"
                and _mercedes_luxury_sort_key(by_key[k]["name"]) is not None
                else (1, luxury_rank(by_key[k]["name"], make, model), by_key[k]["name"].lower())
            )
        ),
    )
    out: list[dict] = []
    for key in ordered_keys[:limit]:
        entry = by_key[key]
        if not entry["adds"]:
            pass
        out.append(
            {
                "name": entry["name"],
                "aliases": entry["aliases"][:6],
                "adds": entry["adds"][:6],
                **({"year_min": int(entry["year_min"])} if int(entry.get("year_min") or 0) else {}),
                **({"year_max": int(entry["year_max"])} if int(entry.get("year_max") or 9999) < 9999 else {}),
                **(
                    {"inventory_price_note": str(entry.get("inventory_price_note") or "").strip()}
                    if str(entry.get("inventory_price_note") or "").strip()
                    else {}
                ),
            }
        )
    return out if len(out) >= 2 else []


def extract_trims_from_text(
    make: str,
    text: str,
    *,
    model: str | None = None,
    limit: int = 12,
) -> list[str]:
    """Find known trim names in a blob of dictionary / listing text."""
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for name in ordered_trim_candidates(make, model):
        if len(name) < 2:
            continue
        if re.search(rf"\b{re.escape(name)}\b", text, re.I):
            tok = re.sub(r"[^a-z0-9]+", "", name.lower())
            if tok not in seen:
                seen.add(tok)
                found.append(name)
        if len(found) >= limit:
            break
  # Do not split arbitrary prose (wiki CSV blobs); only known OEM trim tokens.
    return found[:limit]


def merge_trim_names(
    *groups: Iterable[str],
    make: str = "",
    model: str | None = None,
    limit: int = 12,
) -> list[str]:
    """Merge trim name lists; order by OEM knowledge when possible."""
    order = ordered_trim_candidates(make, model)
    rank = {n.lower(): i for i, n in enumerate(order)}
    seen: set[str] = set()
    merged: list[str] = []

    def add(name: str) -> None:
        n = canonical_trim_name(name, make, model) or preserve_trim_label(name, make, model)
        if not n:
            return
        tok = re.sub(r"[^a-z0-9]+", "", n.lower())
        if tok in seen:
            return
        seen.add(tok)
        merged.append(n)

    for g in groups:
        for name in g:
            add(name)

    merged.sort(key=lambda n: rank.get(n.lower(), 10_000 + len(merged)))
    return merged[:limit]


_TRUCK_MODEL_TOKENS = frozenset(
    {
        "tundra",
        "tacoma",
        "sequoia",
        "4runner",
        "highlander",
        "sienna",
        "landcruiser",
    }
)

# Chevrolet make-wide order is truck/sports-only (ZR1, High Country, etc.).
_CHEVROLET_MAKE_FALLBACK_MODEL_TOKENS = frozenset(
    {
        "silverado",
        "tahoe",
        "suburban",
        "corvette",
        "camaro",
        "colorado",
        "blazer",
        "express",
    }
)

def _make_fallback_trim_order(make: str, model: str) -> tuple[str, ...]:
    """Make-wide trim order when model-specific data is unavailable."""
    mk, mod = _resolve_trim_model_key(make, model)
    if mk == "bmw":
        return ()
    if mk == "toyota" and not any(tok in mod for tok in _TRUCK_MODEL_TOKENS):
        return ()
    if mk == "chevrolet" and not any(tok in mod for tok in _CHEVROLET_MAKE_FALLBACK_MODEL_TOKENS):
        return ()
    return MAKE_TRIM_ORDER.get(mk, ())


def generic_fallback_steps(make: str, model: str) -> list[dict[str, object]]:
    """Last-resort ladder so the VDP never shows an empty trim panel."""
    names = list(_model_trim_order(make, model)[:8])
    if len(names) < 2:
        names = list(_make_fallback_trim_order(make, model)[:8])
    if len(names) < 2:
        names = list(GENERIC_TRIM_ORDER[:5])
    return [
        {
            "name": name,
            "aliases": [],
            "adds": [],
        }
        for name in names
    ]
