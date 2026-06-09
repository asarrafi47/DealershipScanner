"""TCO fuel helpers: city/highway MPG average and fuel-tank capacity lookup."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR

_TCO_DEFAULT_MPG = 25.0
_TCO_DEFAULT_TANK_GAL = 15.5
_EPA_KWH_PER_GALLON = 33.7
_TCO_DEFAULT_EV_KWH_PER_100 = 33.7

_TANK_GALLON_RE = re.compile(
    r"(\d{1,2}(?:\.\d{1,2})?)\s*-?\s*(?:us\s+)?gallon(?:s)?(?:\s+(?:useable\s+)?fuel\s+tank)?",
    re.IGNORECASE,
)
_TANK_LITRE_RE = re.compile(
    r"(\d{2,3})\s*-?\s*(?:litre|liter)s?\s+(?:useable\s+)?fuel\s+tank",
    re.IGNORECASE,
)
_LITRES_TO_US_GAL = 0.264172

_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "fuel_tank_defaults.json"


def average_mpg_city_highway(mpg_city: Any, mpg_highway: Any) -> float | None:
    """
    Arithmetic mean of EPA city and highway MPG when both are present.

    If only one is available, returns that value. Returns ``None`` when neither is usable.
    """
    city = _positive_float(mpg_city)
    highway = _positive_float(mpg_highway)
    if city is not None and highway is not None:
        return (city + highway) / 2.0
    if city is not None:
        return city
    if highway is not None:
        return highway
    return None


def resolve_tco_avg_mpg(car: dict[str, Any] | None, *, default: float = _TCO_DEFAULT_MPG) -> float:
    """MPG for TCO and fill-up range copy; falls back to ``default`` when EPA values are missing."""
    if not car:
        return default
    avg = average_mpg_city_highway(car.get("mpg_city"), car.get("mpg_highway"))
    if avg is not None and avg > 0:
        return round(avg, 1)
    combined = _positive_float(car.get("mpg_combined"))
    if combined is not None:
        return round(combined, 1)
    return default


def resolve_tco_ev_efficiency(
    car: dict[str, Any] | None,
    *,
    default: float = _TCO_DEFAULT_EV_KWH_PER_100,
) -> float:
    """
    Estimated residential charging intensity in kWh per 100 miles for pure EVs.

    Uses EPA MPGe (stored in ``mpg_city`` / ``mpg_highway`` for electric listings)
    converted via 33.7 kWh per gasoline-gallon equivalent.
    """
    if not car:
        return default
    avg_mpge = average_mpg_city_highway(car.get("mpg_city"), car.get("mpg_highway"))
    if avg_mpge is None:
        avg_mpge = _positive_float(car.get("mpg_combined"))
    if avg_mpge is None or avg_mpge <= 0:
        return default
    kwh_per_100 = (_EPA_KWH_PER_GALLON * 100.0) / float(avg_mpge)
    return round(kwh_per_100, 1)


def resolve_fuel_tank_gallons(car: dict[str, Any] | None) -> float:
    """
    Estimated US fuel-tank capacity in gallons.

    Resolution order: trim/brochure overlay text → curated make/model table → body-style
    heuristic → class default (15.5 gal).
    """
    if not car:
        return _TCO_DEFAULT_TANK_GAL
    from_trim = _tank_gallons_from_trim_adds(car)
    if from_trim is not None:
        return from_trim
    make_key = _norm_make_model_key(car.get("make"))
    model_key = _norm_make_model_key(car.get("model"))
    year = _parse_year(car.get("year"))
    data = _load_fuel_tank_defaults()
    models = data.get("models") or {}
    if make_key and model_key:
        exact = _lookup_model_tank(models, make_key, model_key, year)
        if exact is not None:
            return exact
    body_gal = _tank_from_body_style(str(car.get("body_style") or ""), data)
    if body_gal is not None:
        return body_gal
    return float(data.get("default_gallons") or _TCO_DEFAULT_TANK_GAL)


def compute_fill_up_cost_usd(
    fuel_price_per_gallon: float,
    tank_gallons: float,
) -> float | None:
    """Full-tank cost at the given price per gallon."""
    if not _positive_float(fuel_price_per_gallon) or not _positive_float(tank_gallons):
        return None
    return round(float(tank_gallons) * float(fuel_price_per_gallon), 2)


def miles_per_tank(avg_mpg: float, tank_gallons: float) -> int | None:
    if not _positive_float(avg_mpg) or not _positive_float(tank_gallons):
        return None
    return int(round(float(avg_mpg) * float(tank_gallons)))


def _positive_float(value: Any) -> float | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _parse_year(value: Any) -> int | None:
    try:
        y = int(value)
    except (TypeError, ValueError):
        return None
    return y if 1900 <= y <= 2100 else None


def _norm_make_model_key(raw: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(raw or "").strip().lower())


def _lookup_model_tank(
    models: dict[str, Any],
    make_key: str,
    model_key: str,
    year: int | None,
) -> float | None:
    entry = models.get(f"{make_key}|{model_key}")
    if entry is None:
        return None
    if isinstance(entry, (int, float)):
        return float(entry)
    if not isinstance(entry, dict):
        return None
    if year is not None:
        by_year = entry.get("by_year") or {}
        year_key = str(year)
        if year_key in by_year:
            g = _positive_float(by_year[year_key])
            if g is not None:
                return g
        for span, gallons in sorted((by_year or {}).items(), key=lambda x: x[0], reverse=True):
            if not str(span).isdigit() or len(str(span)) != 4:
                continue
            try:
                if int(span) <= year:
                    g = _positive_float(gallons)
                    if g is not None:
                        return g
            except ValueError:
                continue
    g = _positive_float(entry.get("default"))
    return g


def _tank_from_body_style(body_style: str, data: dict[str, Any]) -> float | None:
    blob = body_style.lower()
    if not blob.strip():
        return None
    keywords: dict[str, Any] = data.get("body_keywords") or {}
    best: tuple[int, float] | None = None
    for keyword, gallons in keywords.items():
        kw = str(keyword).lower().strip()
        if kw and kw in blob:
            g = _positive_float(gallons)
            if g is None:
                continue
            rank = len(kw)
            if best is None or rank > best[0]:
                best = (rank, g)
    return best[1] if best else None


def _tank_gallons_from_trim_adds(car: dict[str, Any]) -> float | None:
    year = _parse_year(car.get("year"))
    make = str(car.get("make") or "").strip()
    model = str(car.get("model") or "").strip()
    if year is None or not make or not model:
        return None
    ck = catalog_key(year, make, model)
    path = TRIM_ADDS_BY_YEAR_DIR / f"{ck.replace('|', '__')}.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    texts: list[str] = []
    adds = payload.get("adds_by_trim")
    if isinstance(adds, dict):
        for bullets in adds.values():
            if isinstance(bullets, list):
                texts.extend(str(b) for b in bullets if b)
    return _max_tank_gallons_from_texts(texts)


def _max_tank_gallons_from_texts(texts: list[str]) -> float | None:
    found: list[float] = []
    for text in texts:
        for m in _TANK_GALLON_RE.finditer(text):
            g = _positive_float(m.group(1))
            if g is not None and 5 <= g <= 60:
                found.append(g)
        for m in _TANK_LITRE_RE.finditer(text):
            litres = _positive_float(m.group(1))
            if litres is not None:
                g = litres * _LITRES_TO_US_GAL
                if 5 <= g <= 60:
                    found.append(round(g, 1))
    if not found:
        return None
    return max(found)


@lru_cache(maxsize=1)
def _load_fuel_tank_defaults() -> dict[str, Any]:
    if not _DATA_PATH.is_file():
        return {"default_gallons": _TCO_DEFAULT_TANK_GAL, "body_keywords": {}, "models": {}}
    try:
        return json.loads(_DATA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"default_gallons": _TCO_DEFAULT_TANK_GAL, "body_keywords": {}, "models": {}}
