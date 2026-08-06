"""
TCO helpers: resolve a car's US state code and premium/regular fuel requirement.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import re
from typing import Any

_TCO_DEFAULT_STATE = "NC"
_STATE_ZIP_IN_TEXT_RE = re.compile(r"\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b")
_COMMA_STATE_ZIP_RE = re.compile(r",\s*([A-Za-z]{2})\s+(\d{5})(?:-\d{4})?\b")
_TRAILING_COMMA_STATE_RE = re.compile(r",\s*([A-Za-z]{2})\s*$")

_TCO_PREMIUM_ENGINE_KEYWORDS = (
    "turbo",
    "twin turbo",
    "twin-turbo",
    "supercharged",
    "supercharger",
    "v8",
    "v12",
    "v-8",
    "v-12",
)

_TCO_LUXURY_PREMIUM_MAKES = frozenset(
    {
        "bmw",
        "mercedes-benz",
        "mercedes",
        "porsche",
        "audi",
    }
)


def _valid_us_state_code(raw: Any) -> str:
    from backend.discovery.normalize import normalize_us_state_to_code

    code = normalize_us_state_to_code(str(raw).strip() if raw is not None else "")
    return code if len(code) == 2 else ""


def _extract_state_from_location_text(text: str | None) -> str:
    if not text or not str(text).strip():
        return ""
    raw = str(text).strip()
    m = _STATE_ZIP_IN_TEXT_RE.search(raw.upper())
    if m:
        return m.group(1)
    m = _COMMA_STATE_ZIP_RE.search(raw)
    if m:
        code = _valid_us_state_code(m.group(1))
        if code:
            return code
    m = _TRAILING_COMMA_STATE_RE.search(raw)
    if m:
        code = _valid_us_state_code(m.group(1))
        if code:
            return code
    from backend.discovery.normalize import normalize_us_state_to_code

    for segment in reversed([p.strip() for p in raw.split(",") if p.strip()]):
        code = normalize_us_state_to_code(segment)
        if code:
            return code
    return ""


def _state_from_dealership_registry(registry_id: Any) -> str:
    try:
        rid = int(registry_id)
    except (TypeError, ValueError):
        return ""
    if rid <= 0:
        return ""
    try:
        from backend.db.dealerships_db import get_dealership_by_id

        row = get_dealership_by_id(rid)
        if not row:
            return ""
        for key in ("state", "state_code"):
            code = _valid_us_state_code(row.get(key))
            if code:
                return code
        parts = [
            row.get("street_address"),
            row.get("city"),
            row.get("state"),
            row.get("zip_code"),
        ]
        location = ", ".join(str(p).strip() for p in parts if p and str(p).strip())
        return _extract_state_from_location_text(location)
    except Exception:
        return ""


def resolve_car_state_code(car: dict[str, Any]) -> str:
    """
    Two-letter US state for TCO fuel lookup.

    Uses row state, dealership registry/location text, listing ZIP, then ``NC``.
    """
    if not car:
        return _TCO_DEFAULT_STATE
    c = car
    for key in ("state", "state_code", "dealer_state"):
        code = _valid_us_state_code(c.get(key))
        if code:
            return code
    for key in (
        "dealer_location",
        "dealer_address",
        "location",
        "address",
        "dealer_city_state",
    ):
        code = _extract_state_from_location_text(c.get(key))
        if code:
            return code
    code = _state_from_dealership_registry(c.get("dealership_registry_id"))
    if code:
        return code
    zip_raw = c.get("zip_code")
    if zip_raw and str(zip_raw).strip():
        try:
            from backend.db.geo import us_postal_meta_for_zip

            meta = us_postal_meta_for_zip(str(zip_raw).strip())
            if meta and meta.get("state_code"):
                code = _valid_us_state_code(meta["state_code"])
                if code:
                    return code
        except Exception:
            pass
    return _TCO_DEFAULT_STATE


def _normalize_tco_make_key(raw: Any) -> str:
    return re.sub(r"\s+", " ", str(raw or "").strip().lower())


def _engine_spec_text_for_fuel_requirement(
    car: dict[str, Any],
    *,
    engine_display: str | None = None,
    verified_specs: dict[str, Any] | None = None,
) -> str:
    vs = verified_specs or {}
    chunks = [
        car.get("engine_description"),
        car.get("engine"),
        car.get("title"),
        car.get("trim"),
        engine_display,
        vs.get("master_engine_string"),
        vs.get("epa_engine_description"),
        car.get("fuel_type"),
        vs.get("epa_fuel_type"),
    ]
    return " ".join(str(x).strip() for x in chunks if x and str(x).strip()).lower()


def resolve_car_fuel_requirement(
    car: dict[str, Any],
    *,
    engine_display: str | None = None,
    verified_specs: dict[str, Any] | None = None,
) -> str:
    """
    TCO fuel tier: ``premium`` when engine or make signals premium gasoline; else ``regular``.
    """
    if not car:
        return "regular"
    blob = _engine_spec_text_for_fuel_requirement(
        car, engine_display=engine_display, verified_specs=verified_specs
    )
    if "premium" in blob or "premium gasoline" in blob:
        return "premium"
    for kw in _TCO_PREMIUM_ENGINE_KEYWORDS:
        if kw in blob:
            return "premium"
    if re.search(r"\bv\s*[-]?\s*8\b", blob):
        return "premium"
    if re.search(r"\bv\s*[-]?\s*12\b", blob):
        return "premium"
    make_key = _normalize_tco_make_key(car.get("make"))
    if make_key in _TCO_LUXURY_PREMIUM_MAKES:
        return "premium"
    compact = make_key.replace(" ", "").replace("-", "")
    for luxury in _TCO_LUXURY_PREMIUM_MAKES:
        if compact == luxury.replace(" ", "").replace("-", ""):
            return "premium"
    return "regular"
