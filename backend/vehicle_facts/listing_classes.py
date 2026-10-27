"""Vehicle classes the completeness rules judge differently (N21.8).

Three small, data-driven rules, each with the finding it answers in
``docs/INCOMPLETE_LISTINGS_FINDINGS_2026_10.md``:

* F1: a Mercedes model that carries the designation ("C 300", "GLC 300") already shows
  the buyer the trim; a blank trim there is not a gap.
* M1: a blank trim is exempt only for a model on an explicit single-trim list. The
  catalog's "only trim equals the model name" no longer decides (it hid about 201 real
  gaps: Land Cruiser, Odyssey, WRX).
* F3: trailers, motorcycles, carts and boats are judged against the field set that
  exists for them, not the passenger-car one.

Pure functions plus one memoised read of the cached vPIC ``VehicleType``.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

# -- F1 Mercedes designation -------------------------------------------------------

# Letters plus digits ("C 300", "GLC 300", "E 450", "CLA 220", "EQS 580", "AMG GT 53",
# "S 580"), optionally prefixed "Mercedes-AMG" / "Maybach", optionally with a power-train
# suffix ("e", "d", "+"). A bare family ("GLC", "CLA") and Sprinter/Metris vans do not match.
_MB_DESIGNATION_RE = re.compile(
    r"^(?:(?:mercedes-)?amg\s+|(?:mercedes-)?maybach\s+)?"
    r"(?P<family>AMG\s+GT|EQ[A-Z]|[A-Z]{1,3})\s?-?(?P<digits>\d{2,3})(?P<suffix>[a-z+]{0,3})$",
    re.I,
)
_MB_NOT_DESIGNATION = re.compile(r"sprinter|metris|class", re.I)


def mercedes_model_is_designation(make: Any, model: Any) -> bool:
    """True when *model* is a Mercedes designation (letters plus digits), not a family."""
    if str(make or "").strip().lower() not in ("mercedes-benz", "mercedes", "mercedes benz"):
        return False
    text = str(model or "").strip()
    if not text or _MB_NOT_DESIGNATION.search(text):
        return False
    return bool(_MB_DESIGNATION_RE.match(text))


# -- M1 explicit single-trim list --------------------------------------------------

# (make, model, first model year, last model year); None = open. Only models whose
# trim ladder is a single rung. Anything not listed with a blank trim is a real gap,
# whatever ``epa_master`` says about the model name.
SINGLE_TRIM_MODELS: tuple[tuple[str, str, int | None, int | None], ...] = (
    ("honda", "prelude", 2026, None),
    ("toyota", "mirai", None, None),
    ("bmw", "xm", 2023, 2023),   # 2024+ XM has label/trim variants
    ("bmw", "i8", None, None),
    ("bmw", "m6", None, None),
    ("audi", "rs q8", None, None),
    ("audi", "r8", None, None),
    ("chevrolet", "corvette", None, 2004),  # one body/engine rung before the C6 trim ladder
)


def _norm(text: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def model_is_single_trim(year: Any, make: Any, model: Any) -> bool:
    """True when (year, make, model) is on the explicit single-trim list."""
    mk, md = _norm(make), _norm(model)
    try:
        yr = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        yr = None
    for lm, lmd, lo, hi in SINGLE_TRIM_MODELS:
        if mk != lm or md != lmd:
            continue
        if lo is None and hi is None:
            return True
        if yr is None:
            continue
        if (lo is None or yr >= lo) and (hi is None or yr <= hi):
            return True
    return False


# -- F3 non-road vehicles ----------------------------------------------------------

# Field codes that do not exist for the class, so a blank is not a gap.
NON_ROAD_EXEMPT: dict[str, frozenset[str]] = {
    "trailer": frozenset({"engine", "transmission", "drivetrain", "fuel_type", "cylinders"}),
    "motorcycle": frozenset({"drivetrain"}),
    "cart": frozenset({"engine", "transmission", "drivetrain", "cylinders"}),
    "watercraft": frozenset({"engine", "transmission", "drivetrain", "fuel_type", "cylinders"}),
}

_VPIC_TYPE_CLASS = (
    ("trailer", "trailer"),
    ("motorcycle", "motorcycle"),
    ("low speed vehicle", "cart"),
    ("lsv", "cart"),
)
_BODY_CLASS = (
    (re.compile(r"\btrailer\b", re.I), "trailer"),
    (re.compile(r"motorcycle|motorbike|scooter", re.I), "motorcycle"),
    (re.compile(r"golf\s*cart|\bcart\b|low[- ]speed", re.I), "cart"),
    (re.compile(r"\bboat\b|watercraft|jet\s*ski|\bpwc\b", re.I), "watercraft"),
)


@lru_cache(maxsize=8192)
def vpic_vehicle_type(vin: str) -> str:
    """Cached vPIC ``VehicleType`` for *vin* ("" when not cached or not stated)."""
    if not vin:
        return ""
    conn = None
    try:
        from backend.db.inventory_db import get_conn

        conn = get_conn()
        row = conn.execute("SELECT response_json FROM nhtsa_vpic_cache WHERE vin=?", (vin,)).fetchone()
        if not row:
            return ""
        raw = row[0]
        payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        r0 = (payload.get("Results") or [{}])[0]
        text = str(r0.get("VehicleType") or "").strip()
        return "" if text.lower() in ("not applicable", "n/a") else text
    except Exception:
        return ""
    finally:
        if conn is not None:
            conn.close()


def non_road_class(car_raw: dict[str, Any], *, vehicle_type: str | None = None) -> str | None:
    """"trailer" / "motorcycle" / "cart" / "watercraft", or None for a road car.

    vPIC's VehicleType wins; the stored body style is the fallback for rows with no decode
    (boats and carts often have no real VIN). *vehicle_type* skips the cache read."""
    vt = vehicle_type
    if vt is None:
        vin = str(car_raw.get("vin") or "").strip().upper()
        vt = vpic_vehicle_type(vin) if len(vin) == 17 else ""
    low = (vt or "").lower()
    for needle, cls in _VPIC_TYPE_CLASS:
        if needle in low:
            return cls
    body = str(car_raw.get("body_style") or "")
    for pat, cls in _BODY_CLASS:
        if pat.search(body):
            return cls
    return None
