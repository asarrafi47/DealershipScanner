"""Resolve original EPA all-electric range (miles) for EV / PHEV listings."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.enrichment.dictionary_catalog import catalog_key
from backend.enrichment.dictionary_paths import TRIM_ADDS_BY_YEAR_DIR
from backend.enrichment.knowledge_engine import lookup_epa_by_trim

logger = logging.getLogger(__name__)

_RANGE_TOKEN_STOP = frozenset(
    {
        "awd",
        "rwd",
        "fwd",
        "with",
        "the",
        "and",
        "for",
        "suv",
        "sedan",
        "hatchback",
        "coupe",
    }
)

_EPA_VEHICLES_CSV_URL = "https://fueleconomy.gov/feg/epadata/vehicles.csv"
_EPA_RANGE_CACHE_PATH = (
    Path(__file__).resolve().parents[1] / "dictionary" / "derived" / "epa_ev_range_miles.json"
)
_ELECTRIFIED_FUEL_TYPES = frozenset({"electric", "ev", "electricity", "plug-in hybrid", "plug in hybrid", "phev"})

_RANGE_TEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(\d{2,3})\s*(?:mile|mi\.?)\s*(?:EPA[- ]?)?(?:estimated\s+)?range\b", re.I),
    re.compile(r"\bEPA[- ]?(?:estimated\s+)?range\s*(?:of\s*)?(\d{2,3})\s*(?:mile|mi\.?)?\b", re.I),
    re.compile(r"\b(\d{2,3})\s*miles?\s*(?:EPA[- ]?)?(?:estimated\s+)?range\b", re.I),
    re.compile(r"\b(\d{2,3})\s*(?:mile|mi\.?)\s*(?:all[- ]?electric|electric)\s+range\b", re.I),
    re.compile(r"\b(?:up to|about|approx\.?|at least)\s*(\d{2,3})\s*(?:mile|mi\.?)\s*(?:of\s*)?(?:EPA\s*)?range\b", re.I),
    re.compile(
        r"\b(?:manufacturer[- ]?estimated|estimated|advertised)\s*(?:all[- ]?electric\s*)?"
        r"(?:range\s*(?:of\s*)?)?(\d{2,3})\s*(?:mile|mi\.?)\b",
        re.I,
    ),
    re.compile(r"\b(\d{2,3})\s*(?:mile|mi\.?)\s*(?:manufacturer[- ]?estimated|advertised)\s*range\b", re.I),
)


def _normalize_token(raw: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(raw or "").strip().lower())


def _positive_int(value: Any) -> int | None:
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _positive_range_miles(value: Any) -> int | None:
    miles = _positive_int(value)
    return miles if miles is not None and 40 <= miles <= 600 else None


def _parse_year(value: Any) -> int | None:
    year = _positive_int(value)
    return year if year is not None and 1900 <= year <= 2100 else None


def _is_electrified_car(car: dict[str, Any] | None) -> bool:
    if not car:
        return False
    fuel = str(car.get("fuel_type") or "").strip().lower()
    if fuel in _ELECTRIFIED_FUEL_TYPES:
        return True
    if "electric" in fuel or "plug" in fuel:
        return True
    return False


def parse_epa_range_from_text(text: Any) -> int | None:
    blob = str(text or "").strip()
    if not blob:
        return None
    for pattern in _RANGE_TEXT_PATTERNS:
        match = pattern.search(blob)
        if not match:
            continue
        miles = _positive_range_miles(match.group(1))
        if miles is not None:
            return miles
    return None


def _listing_range_tokens(*parts: Any) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        for raw in re.split(r"[\s/+-]+", str(part or "").lower()):
            tok = re.sub(r"[^a-z0-9]", "", raw)
            if not tok:
                continue
            if tok.isdigit():
                tokens.add(tok)
                continue
            if len(tok) < 3 or tok in _RANGE_TOKEN_STOP:
                continue
            tokens.add(tok)
    return tokens


def _listing_text_blobs(car: dict[str, Any]) -> list[str]:
    chunks: list[str] = []
    for key in ("description", "title", "trim", "model", "packages"):
        val = car.get(key)
        if val is None:
            continue
        chunks.append(str(val))
    return chunks


def _score_epa_model_match(
    *,
    listing_model: str,
    listing_trim: str,
    epa_model: str,
    epa_trim: str | None = None,
) -> int:
    model_n = _normalize_token(listing_model)
    trim_n = _normalize_token(listing_trim)
    epa_model_n = _normalize_token(epa_model)
    epa_trim_n = _normalize_token(epa_trim or "")
    blob = f"{listing_model} {listing_trim}".strip().lower()
    epa_blob = f"{epa_model} {epa_trim or ''}".strip().lower()
    score = 0
    if model_n and model_n in epa_model_n:
        score += 20
    if trim_n and trim_n in epa_model_n:
        score += 30
    if trim_n and epa_trim_n and (trim_n in epa_trim_n or epa_trim_n in trim_n):
        score += 25
    tokens = [t for t in re.split(r"[\s/+-]+", listing_trim.lower()) if len(t) > 2]
    if tokens and all(tok in epa_blob for tok in tokens):
        score += 15
    if blob and blob in epa_blob:
        score += 40

    listing_tokens = _listing_range_tokens(listing_model, listing_trim)
    epa_tokens = _listing_range_tokens(epa_model, epa_trim or "")
    overlap = listing_tokens & epa_tokens
    for tok in overlap:
        if tok.isdigit():
            score += 18
        else:
            score += 10
    if len(overlap) >= 3:
        score += 20
    elif len(overlap) >= 2:
        score += 10
    return score


def _score_trim_name_match(listing_trim: str, trim_name: str) -> int:
    trim_raw = str(listing_trim or "").strip()
    name_raw = str(trim_name or "").strip()
    if not trim_raw or not name_raw:
        return 0
    trim_n = _normalize_token(trim_raw)
    name_n = _normalize_token(name_raw)
    if name_n in trim_n or trim_n in name_n:
        return 40
    overlap = _listing_range_tokens(trim_raw) & _listing_range_tokens(name_raw)
    if not overlap:
        return 0
    return min(35, 10 + 8 * len(overlap))


def _max_range_miles_from_texts(texts: list[str]) -> int | None:
    found: list[int] = []
    for text in texts:
        miles = parse_epa_range_from_text(text)
        if miles is not None:
            found.append(miles)
    return max(found) if found else None


def _range_miles_from_trim_adds(car: dict[str, Any]) -> int | None:
    year = _parse_year(car.get("year"))
    make = str(car.get("make") or "").strip()
    model = str(car.get("model") or "").strip()
    trim_raw = str(car.get("trim") or "").strip()
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

    adds = payload.get("adds_by_trim")
    if not isinstance(adds, dict):
        return None

    best_range: int | None = None
    best_score = 0
    for trim_name, bullets in adds.items():
        if not isinstance(bullets, list):
            continue
        score = _score_trim_name_match(trim_raw, str(trim_name))
        if score <= 0 and len(adds) == 1:
            score = 10
        if score <= 0:
            continue
        miles = _max_range_miles_from_texts([str(b) for b in bullets if b])
        if miles is None:
            continue
        if score > best_score:
            best_score = score
            best_range = miles

    if best_range is not None:
        return best_range

    all_texts: list[str] = []
    for bullets in adds.values():
        if isinstance(bullets, list):
            all_texts.extend(str(b) for b in bullets if b)
    return _max_range_miles_from_texts(all_texts)


def _fetch_epa_vehicle_rows() -> list[dict[str, Any]]:
    request = urllib.request.Request(
        _EPA_VEHICLES_CSV_URL,
        headers={"User-Agent": "DealershipScanner/1.0"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = response.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(payload))
    rows: list[dict[str, Any]] = []
    for row in reader:
        atv = str(row.get("atvType") or "").strip().upper()
        fuel = str(row.get("fuelType1") or "").strip().lower()
        if atv not in {"EV", "PHEV"} and "electric" not in fuel and "plug" not in fuel:
            continue
        year = _parse_year(row.get("year"))
        make = str(row.get("make") or "").strip()
        model = str(row.get("model") or "").strip()
        if year is None or not make or not model:
            continue
        range_miles = _positive_range_miles(row.get("range"))
        if range_miles is None:
            range_miles = _positive_range_miles(row.get("rangeA"))
        if range_miles is None:
            continue
        rows.append(
            {
                "year": year,
                "make": make,
                "make_n": _normalize_token(make),
                "model": model,
                "range_miles": range_miles,
                "atv_type": atv or None,
            }
        )
    return rows


def build_epa_ev_range_cache() -> dict[str, Any]:
    rows = _fetch_epa_vehicle_rows()
    return {
        "source": "fueleconomy.gov/vehicles.csv",
        "rows": rows,
    }


def write_epa_ev_range_cache(payload: dict[str, Any] | None = None) -> Path:
    data = payload or build_epa_ev_range_cache()
    path = _EPA_RANGE_CACHE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


@lru_cache(maxsize=1)
def _load_epa_ev_range_rows() -> tuple[dict[str, Any], ...]:
    path = _EPA_RANGE_CACHE_PATH
    if path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = payload.get("rows")
            if isinstance(rows, list) and rows:
                return tuple(rows)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("epa_ev_range_miles.json unreadable: %s", exc)
    try:
        write_epa_ev_range_cache()
        payload = json.loads(_EPA_RANGE_CACHE_PATH.read_text(encoding="utf-8"))
        rows = payload.get("rows") or []
        return tuple(rows if isinstance(rows, list) else [])
    except Exception as exc:
        logger.warning("Unable to build EPA EV range cache: %s", exc)
        return ()


def lookup_epa_range_miles(
    year: Any,
    make: Any,
    model: Any,
    trim: Any = None,
) -> int | None:
    year_val = _parse_year(year)
    make_raw = str(make or "").strip()
    model_raw = str(model or "").strip()
    trim_raw = str(trim or "").strip()
    if year_val is None or not make_raw or not model_raw:
        return None

    make_n = _normalize_token(make_raw)
    rows = _load_epa_ev_range_rows()

    for year_try in (year_val, year_val - 1, year_val + 1):
        candidates = [
            row for row in rows if row.get("year") == year_try and row.get("make_n") == make_n
        ]
        if not candidates:
            continue

        epa_trim = lookup_epa_by_trim(year_try, make_raw, model_raw, trim_raw or None)
        epa_trim_name = str(epa_trim.get("trim") or "").strip() or None

        best_range: int | None = None
        best_score = 0
        for row in candidates:
            score = _score_epa_model_match(
                listing_model=model_raw,
                listing_trim=trim_raw,
                epa_model=str(row.get("model") or ""),
                epa_trim=epa_trim_name,
            )
            if score > best_score:
                best_score = score
                best_range = _positive_range_miles(row.get("range_miles"))
        if best_range is not None and best_score >= 15:
            return best_range
    return None


def resolve_factory_epa_range(car: dict[str, Any] | None) -> int | None:
    """
    Original manufacturer / EPA all-electric range in miles for EV / PHEV listings.

    Resolution order: explicit row value → listing text → brochure trim adds →
    fueleconomy.gov cache (all automakers, not Tesla-only).
    """
    if not car or not _is_electrified_car(car):
        return None

    for key in ("factory_range", "epa_range", "epa_range_miles"):
        explicit = _positive_range_miles(car.get(key))
        if explicit is not None:
            return explicit

    for blob in _listing_text_blobs(car):
        parsed = parse_epa_range_from_text(blob)
        if parsed is not None:
            return parsed

    from_trim_adds = _range_miles_from_trim_adds(car)
    if from_trim_adds is not None:
        return from_trim_adds

    return lookup_epa_range_miles(
        car.get("year"),
        car.get("make"),
        car.get("model"),
        car.get("trim"),
    )
