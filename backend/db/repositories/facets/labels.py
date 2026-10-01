"""Facet value hygiene and display labels (shared with the dealership page facets)."""
from __future__ import annotations

import re

from backend.db.repositories.search.countries import _lookup_make_country
from backend.utils.field_clean import is_effectively_empty


def _normalize_make_capitalization(make: str) -> str:
    """Normalize make name capitalization: title case for most, handle special cases."""
    if not make:
        return make

    # Special cases: handle multi-word makes and known variations
    special_cases = {
        "land rover": "Land Rover",
        "rolls royce": "Rolls Royce",
        "aston martin": "Aston Martin",
        "mclaren": "McLaren",
        "mclaughlin": "McLaughlin",
        "ram": "RAM",
        "gmc": "GMC",
        "bmw": "BMW",
        "tesla": "Tesla",
        "vw": "Volkswagen",
        "mercedes-benz": "Mercedes-Benz",
        "alfa romeo": "Alfa Romeo",
        "mini": "MINI",
        "infiniti": "INFINITI",
        "lexus": "LEXUS",
    }

    m = str(make).strip()
    m_lower = m.lower()

    # Check special cases
    if m_lower in special_cases:
        return special_cases[m_lower]

    # Default: capitalize first letter, lowercase the rest
    return m[0].upper() + m[1:].lower() if m else m


def _normalize_facet_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def _canonical_facet_label(value: str, *, variants: list[str]) -> str:
    """Pick one display label for case/spacing variants (``LARIAT`` vs ``Lariat``)."""
    pool = []
    for v in variants:
        s = re.sub(r"\s+", " ", (v or "").strip())
        if s and s not in pool:
            pool.append(s)
    if not pool:
        return (value or "").strip()
    if len(pool) == 1:
        return pool[0]

    def _rank(v: str) -> tuple:
        letters = sum(c.isalpha() for c in v)
        all_caps = letters > 0 and v == v.upper()
        has_lower = any(c.islower() for c in v)
        has_upper = any(c.isupper() for c in v)
        mixed = has_lower and has_upper
        return (mixed, not all_caps, v == v.title(), len(v))

    return max(pool, key=_rank)


def _facet_make_valid(make) -> bool:
    """Reject polluted ``make`` values (numeric trims, model names) for facet lists."""
    if is_effectively_empty(make):
        return False
    m = str(make).strip()
    if m.isdigit():
        return False
    if len(m) <= 4 and re.match(r"^\d{3,4}$", m):
        return False
    return _lookup_make_country(m) is not None


def _facet_transmission_sane(val) -> bool:
    """Drop values that are clearly cylinder counts or garbage, not transmissions."""
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    if s.isdigit() and len(s) <= 2:
        return False
    if s in frozenset({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}):
        return False
    # Dict/list reprs leaked from a structured feed field, e.g.
    # "{'label': '10-Speed Automatic', 'type': 'Automatic'}"
    if s[0] in "{[" or ("'label'" in s or "'type'" in s or "'value'" in s):
        return False
    # Truncated/malformed fragments missing their leading gear count, e.g. "-Speed"
    if s.startswith("-"):
        return False
    return True


def make_model_trim_labels(car_rows: list[tuple]) -> tuple[list[str], list[tuple], list[tuple]]:
    """One UI option per logical make / (make, model) / (make, model, trim).

    Case and spacing variants share a key; each key shows its
    :func:`_canonical_facet_label`. Returns ``(makes, model_rows, trim_rows)``,
    each sorted by key.
    """
    make_variants: dict[str, list[str]] = {}
    model_variants: dict[tuple[str, str], list[str]] = {}
    trim_variants: dict[tuple[str, str, str], list[str]] = {}
    for row in car_rows:
        make, model, trim = row[0], row[1], row[2]
        make_normalized = _normalize_make_capitalization(make)
        make_key = _normalize_facet_key(make_normalized)
        make_variants.setdefault(make_key, [])
        if make_normalized not in make_variants[make_key]:
            make_variants[make_key].append(make_normalized)

        model_key = _normalize_facet_key(model)
        mk = (make_key, model_key)
        model_variants.setdefault(mk, [])
        if model not in model_variants[mk]:
            model_variants[mk].append(model)

        if trim is not None and str(trim).strip():
            trim_key = _normalize_facet_key(trim)
            tk = (make_key, model_key, trim_key)
            trim_variants.setdefault(tk, [])
            if trim not in trim_variants[tk]:
                trim_variants[tk].append(trim)

    seen_makes: list[str] = []
    for make_key in sorted(make_variants.keys()):
        seen_makes.append(_canonical_facet_label("", variants=make_variants[make_key]))

    seen_models: list[tuple[str, str]] = []
    for (make_key, model_key) in sorted(model_variants.keys()):
        make_label = _canonical_facet_label("", variants=make_variants[make_key])
        model_label = _canonical_facet_label("", variants=model_variants[(make_key, model_key)])
        seen_models.append((make_label, model_label))

    seen_trims: list[tuple[str, str, str | None]] = []
    for (make_key, model_key, trim_key) in sorted(trim_variants.keys()):
        make_label = _canonical_facet_label("", variants=make_variants[make_key])
        model_label = _canonical_facet_label("", variants=model_variants[(make_key, model_key)])
        trim_label = _canonical_facet_label("", variants=trim_variants[(make_key, model_key, trim_key)])
        seen_trims.append((make_label, model_label, trim_label))

    return seen_makes, seen_models, seen_trims


def country_facets(makes: list[str]) -> tuple[list[str], dict[str, list[str]]]:
    """Countries that have at least one make in our DB, and each one's makes (sorted)."""
    country_set = set()
    country_to_makes = {}
    for make in makes:
        # Try normalized make first, fall back to original for legacy compatibility
        c = _lookup_make_country(make) or _lookup_make_country(make.lower())
        if c:
            country_set.add(c)
            country_to_makes.setdefault(c, []).append(make)
    for lst in country_to_makes.values():
        lst.sort()
    return sorted(country_set), country_to_makes
