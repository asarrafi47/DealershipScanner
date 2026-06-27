"""
MPG for EPA-exempt heavy-duty trucks (Ram/Ford/Chevy/GMC 2500+).

fueleconomy.gov does not rate HD pickups. This module supplies manufacturer-published
estimates and parses dealer listing / VDP text when MPG appears there.
"""
from __future__ import annotations

import re
from typing import Any

# Manufacturer / dealer published estimates (non-EPA). Key: (fuel_key, drive_key).
_RAM_HD_MPG: dict[tuple[str, str], tuple[int, int]] = {
    ("gas", "2wd"): (14, 18),
    ("gas", "4wd"): (13, 17),
    ("diesel", "2wd"): (16, 22),
    ("diesel", "4wd"): (16, 21),
}

_FORD_SD_MPG: dict[tuple[str, str], tuple[int, int]] = {
    ("gas", "2wd"): (12, 18),
    ("gas", "4wd"): (11, 17),
    ("diesel", "2wd"): (15, 21),
    ("diesel", "4wd"): (15, 20),
}

_GM_HD_MPG: dict[tuple[str, str], tuple[int, int]] = {
    ("gas", "2wd"): (13, 18),
    ("gas", "4wd"): (12, 17),
    ("diesel", "2wd"): (16, 22),
    ("diesel", "4wd"): (16, 21),
}

_MPG_SLASH = re.compile(
    r"(?:mpg|fuel\s+economy|city[/\s]+highway)[^\d]{0,24}(\d{1,2})\s*[/|–-]\s*(\d{1,2})",
    re.I,
)
_MPG_SLASH_TRAILING = re.compile(
    r"(\d{1,2})\s*[/|–-]\s*(\d{1,2})\s*(?:mpg|city\s*/\s*hwy|city\s*/\s*highway)\b",
    re.I,
)
_MPG_CITY_HWY = re.compile(
    r"(\d{1,2})\s*(?:city|cty)\b[^\d]{0,20}(\d{1,2})\s*(?:hwy|highway|hw)\b",
    re.I,
)


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip()).lower()


def _hd_model_token(model: str) -> str | None:
    mo = _norm(model)
    m = re.search(r"\b(2500|3500|4500|5500|6500)\b", mo)
    if m:
        return m.group(1)
    m = re.search(r"\bf[-\s]?([2345]\d{2})\b", mo)
    if m:
        return m.group(1)
    if re.search(r"\bsuper\s+duty\b", mo):
        return "super_duty"
    if re.search(r"\bsilverado\b|\bsierra\b", mo) and re.search(r"\b25|\b35", mo):
        return "hd_pickup"
    return None


def is_epa_exempt_hd_truck(make: str | None, model: str | None) -> bool:
    """True for Ram/Ford/Chevy/GMC 2500+ class pickups EPA does not rate."""
    mk = _norm(make)
    if not mk:
        return False
    tok = _hd_model_token(str(model or ""))
    if not tok:
        return False
    if mk in ("ram", "dodge"):
        return tok in ("2500", "3500", "4500", "5500", "6500")
    if mk == "ford":
        return tok in ("250", "350", "450", "550", "super_duty") or bool(
            re.search(r"\bf[-\s]?[2345]\d{2}\b", _norm(model))
        )
    if mk in ("chevrolet", "chevy", "gmc"):
        if tok in ("2500", "3500", "4500", "5500", "6500", "hd_pickup"):
            return True
        return bool(re.search(r"\b(2500|3500)\s*hd\b", _norm(model)))
    return False


def _fuel_key(*blobs: str, fuel_type: str | None = None) -> str:
    ft = _norm(fuel_type)
    blob = " ".join(_norm(b) for b in blobs if b)
    if "diesel" in ft or "cummins" in blob or re.search(r"\b6\.7\b", blob):
        return "diesel"
    return "gas"


def _drive_key(*blobs: str, drivetrain: str | None = None) -> str:
    d = _norm(drivetrain)
    blob = " ".join(_norm(b) for b in blobs if b)
    combined = f"{d} {blob}"
    if re.search(r"\b(4wd|4x4|awd|four[-\s]?wheel)\b", combined):
        return "4wd"
    if re.search(r"\b(2wd|rwd|rear[-\s]?wheel)\b", combined):
        return "2wd"
    if re.search(r"\b4wd\b|\b4x4\b", combined):
        return "4wd"
    return "2wd"


def _make_mpg_table(make: str) -> dict[tuple[str, str], tuple[int, int]]:
    mk = _norm(make)
    if mk in ("ram", "dodge"):
        return _RAM_HD_MPG
    if mk == "ford":
        return _FORD_SD_MPG
    if mk in ("chevrolet", "chevy", "gmc"):
        return _GM_HD_MPG
    return _RAM_HD_MPG


def parse_mpg_from_listing_text(*texts: str | None) -> tuple[int, int] | None:
    """Extract city/highway MPG from description, title, or HTML snippet."""
    blob = " ".join(str(t or "") for t in texts if t and str(t).strip())
    if not blob:
        return None
    m = _MPG_SLASH.search(blob)
    if m:
        try:
            c, h = int(m.group(1)), int(m.group(2))
            if 5 <= c <= 45 and 5 <= h <= 45 and c <= h + 8:
                return c, h
        except ValueError:
            pass
    m = _MPG_SLASH_TRAILING.search(blob)
    if m:
        try:
            c, h = int(m.group(1)), int(m.group(2))
            if 5 <= c <= 45 and 5 <= h <= 45 and c <= h + 8:
                return c, h
        except ValueError:
            pass
    m = _MPG_CITY_HWY.search(blob)
    if m:
        try:
            c, h = int(m.group(1)), int(m.group(2))
            if 5 <= c <= 45 and 5 <= h <= 45:
                return c, h
        except ValueError:
            pass
    return None


def lookup_hd_truck_mpg_reference(
    *,
    make: str | None,
    model: str | None,
    trim: str | None = None,
    title: str | None = None,
    description: str | None = None,
    drivetrain: str | None = None,
    fuel_type: str | None = None,
    engine_description: str | None = None,
) -> tuple[int, int] | None:
    """
    Manufacturer-estimate MPG for EPA-exempt HD trucks.

    Uses engine (6.4 gas vs 6.7 diesel) and drive (2WD vs 4WD) inferred from trim/title.
    """
    if not is_epa_exempt_hd_truck(make, model):
        return None
    fk = _fuel_key(trim, title, description, engine_description, fuel_type=fuel_type)
    dk = _drive_key(trim, title, description, drivetrain=drivetrain)
    table = _make_mpg_table(str(make or ""))
    return table.get((fk, dk)) or table.get((fk, "4wd" if dk == "4wd" else "2wd"))


def mpg_for_epa_master_trim(trim: str) -> tuple[int, int] | None:
    """Map ``epa_master`` Ram HD seed trim labels to city/highway MPG."""
    t = _norm(trim)
    if not t:
        return None
    fk = _fuel_key(t)
    dk = _drive_key(t)
    return _RAM_HD_MPG.get((fk, dk))


def resolve_hd_truck_mpg(
    car: dict[str, Any],
    *,
    html: str | None = None,
) -> tuple[int, int, str] | None:
    """
    Best-effort HD truck MPG: listing text → VDP HTML → reference table.

    Returns (city, highway, source_tag) or None.
    """
    if not is_epa_exempt_hd_truck(car.get("make"), car.get("model")):
        return None

    texts = (
        car.get("description"),
        car.get("title"),
        car.get("trim"),
        html,
    )
    parsed = parse_mpg_from_listing_text(*texts)
    if parsed:
        return parsed[0], parsed[1], "listing_text"

    if html:
        try:
            from backend.utils.vdp_spec_parse import parse_html_for_vehicle_specs

            specs = parse_html_for_vehicle_specs(html)
            c, h = specs.get("mpg_city"), specs.get("mpg_highway")
            if c is not None and h is not None:
                return int(c), int(h), "vdp_html"
        except ImportError:
            pass

    ref = lookup_hd_truck_mpg_reference(
        make=car.get("make"),
        model=car.get("model"),
        trim=car.get("trim"),
        title=car.get("title"),
        description=car.get("description"),
        drivetrain=car.get("drivetrain"),
        fuel_type=car.get("fuel_type"),
        engine_description=car.get("engine_description"),
    )
    if ref:
        return ref[0], ref[1], "hd_truck_reference"
    return None
