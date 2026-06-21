"""EPA master table lookups (replaces runtime *_EPA.csv reads)."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from backend.enrichment.dictionary_catalog import canonical_make, catalog_key
from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

_BMW_SERIES_MAP: dict[str, str] = {
    "2": "2 Series",
    "3": "3 Series",
    "4": "4 Series",
    "5": "5 Series",
    "6": "6 Series",
    "7": "7 Series",
    "8": "8 Series",
    "M2": "M",
    "M3": "M",
    "M4": "M",
    "M5": "M",
    "M6": "M",
    "M8": "M",
}

_MINI_MODEL_MAP: dict[str, str] = {
    "2 door": "Cooper",
    "4 door": "Cooper",
    "hardtop 2 door": "Cooper",
    "hardtop 4 door": "Cooper",
    "hardtop": "Cooper",
    "cooper hardtop": "Cooper",
    "convertible": "Cooper",
    "cooper roadster": "Roadster",
}


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _bmw_epa_model(model: str) -> str | None:
    m = model.strip()
    if m in _BMW_SERIES_MAP:
        return _BMW_SERIES_MAP[m]
    m_perf = re.match(r"^(M\d)(\d{2}[a-z]?)$", m, re.IGNORECASE)
    if m_perf:
        return _BMW_SERIES_MAP.get(m_perf.group(1)[1])
    digit_match = re.match(r"^([2-9])(\d{2}[a-z]?)", m, re.IGNORECASE)
    if digit_match:
        return _BMW_SERIES_MAP.get(digit_match.group(1))
    return None


def _mini_epa_model(model: str) -> str | None:
    return _MINI_MODEL_MAP.get(model.strip().lower())


def _model_search_variants(make: str, model: str) -> list[str]:
    mk = canonical_make(make)
    md = (model or "").strip()
    if not md:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(m: str) -> None:
        key = m.strip().lower()
        if m and key not in seen:
            seen.add(key)
            out.append(m.strip())

    add(md)
    add(epa_model_search_name(mk, md))
    if mk.upper() == "BMW":
        series = _bmw_epa_model(md)
        if series:
            add(series)
    if mk.upper() == "MINI":
        mini = _mini_epa_model(md)
        if mini:
            add(mini)
    base = md
    for variant_sep in [
        " 3500 HD Chassis Cab",
        " 3500 HD",
        " 2500 HD",
        " 1500",
        " i-FORCE MAX",
        " PHEV",
        " Plug-In Hybrid",
        " Hybrid",
        " Energi",
        " GT",
        " GTS",
        " N Line",
        " L",
        " XL",
        " LS",
        " LT",
        " Limited",
        " Pro",
        " Sport",
    ]:
        if variant_sep.lower() in base.lower():
            add(base[: base.lower().index(variant_sep.lower())].strip())
    return out


def _row_to_csv_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        get = row.get
    elif isinstance(row, (tuple, list)):
        (
            year,
            make,
            model,
            trim,
            trany,
            drive,
            fuel_type,
            cylinders,
            displacement,
            city08,
            highway08,
            body_style,
            engine_description,
            engine_display,
            forced_induction,
        ) = row
        return {
            "Year": year,
            "Make": make,
            "Model": model,
            "Trim": trim or "",
            "transmissionOptions": trany or "",
            "drivetrainOptions": drive or "",
            "fuelType": fuel_type or "",
            "cylinders": cylinders,
            "displacement": displacement,
            "mpg_city": city08,
            "mpg_highway": highway08,
            "bodyStyle": body_style or "",
            "engineOptions": engine_description or "",
            "engineDisplay": engine_display or "",
            "forcedInduction": forced_induction or "",
        }
    else:
        get = lambda k, d=None: row[k] if k in row.keys() else d  # type: ignore[attr-defined]

    out: dict[str, str | int | float | None] = {
        "Year": get("year"),
        "Make": get("make"),
        "Model": get("model"),
        "Trim": get("trim") or "",
        "transmissionOptions": get("trany") or "",
        "drivetrainOptions": get("drive") or "",
        "fuelType": get("fuel_type") or "",
        "cylinders": get("cylinders"),
        "displacement": get("displacement"),
        "mpg_city": get("city08"),
        "mpg_highway": get("highway08"),
        "bodyStyle": get("body_style") or "",
        "engineOptions": get("engine_description") or "",
        "engineDisplay": get("engine_display") or "",
        "forcedInduction": get("forced_induction") or "",
    }
    return {k: ("" if v is None else v) for k, v in out.items()}


def _query_epa_rows(year: int, make: str, model: str) -> list[dict[str, Any]]:
    from backend.db.inventory_db import get_conn

    mk = canonical_make(make)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT year, make, model, trim, trany, drive, fuel_type, cylinders, displacement,
                   city08, highway08, body_style, engine_description, engine_display, forced_induction
            FROM epa_master
            WHERE year = ? AND lower(make) = lower(?) AND lower(model) = lower(?)
            ORDER BY trim
            """,
            (year, mk, model),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [_row_to_csv_dict(r) for r in rows]


@lru_cache(maxsize=4096)
def fetch_epa_rows(year: int, make: str, model: str) -> tuple[dict[str, Any], ...]:
    """Return EPA rows as CSV-shaped dicts (immutable tuple for caching)."""
    try:
        y = int(year)
    except (TypeError, ValueError):
        return ()
    mk = (make or "").strip()
    md = (model or "").strip()
    if not y or not mk or not md:
        return ()

    for variant in _model_search_variants(mk, md):
        rows = _query_epa_rows(y, mk, variant)
        if rows:
            return tuple(rows)
    return ()


def has_epa_rows(year: int, make: str, model: str) -> bool:
    return bool(fetch_epa_rows(year, make, model))


@lru_cache(maxsize=1)
def epa_index_by_make() -> dict[str, list[tuple[int, str]]]:
    """make_norm -> [(year, model_norm)] for fuzzy model matching."""
    from backend.db.inventory_db import get_conn

    out: dict[str, list[tuple[int, str]]] = {}
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT year, make, model
            FROM epa_master
            WHERE year IS NOT NULL AND make IS NOT NULL AND model IS NOT NULL
            """
        )
        for row in cur.fetchall():
            if isinstance(row, dict):
                yr, make, model = row["year"], row["make"], row["model"]
            else:
                yr, make, model = row[0], row[1], row[2]
            make_norm = _norm_token(canonical_make(str(make)))
            model_norm = _norm_token(str(model))
            try:
                file_year = int(yr)
            except (TypeError, ValueError):
                continue
            out.setdefault(make_norm, []).append((file_year, model_norm))
    finally:
        conn.close()
    return out


def epa_trim_names_for_catalog_key(catalog_key_str: str) -> list[str]:
    parts = (catalog_key_str or "").split("|")
    if len(parts) < 3:
        return []
    try:
        year = int(parts[0])
    except (TypeError, ValueError):
        return []
    make = parts[1].replace("_", " ").title()
    model = parts[2]
    names: list[str] = []
    for row in fetch_epa_rows(year, make, model):
        t = (row.get("Trim") or "").strip()
        if t and t not in names:
            names.append(t)
    return names[:24]


def epa_manifest_buckets() -> dict[str, dict[str, Any]]:
    """catalog_key -> {epa_row_count, year, make, model} from epa_master."""
    from backend.db.inventory_db import get_conn

    grouped: dict[str, dict[str, Any]] = {}
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT year, make, model, COUNT(*) AS n
            FROM epa_master
            WHERE year IS NOT NULL AND make IS NOT NULL AND model IS NOT NULL
            GROUP BY year, make, model
            """
        )
        for row in cur.fetchall():
            if isinstance(row, dict):
                year, make, model, n = row["year"], row["make"], row["model"], row["n"]
            else:
                year, make, model, n = row[0], row[1], row[2], row[3]
            key = catalog_key(int(year), str(make), str(model))
            grouped[key] = {
                "catalog_key": key,
                "year": int(year),
                "make": canonical_make(str(make)),
                "model": str(model),
                "epa_path": None,
                "epa_row_count": int(n),
            }
    finally:
        conn.close()
    return grouped


def clear_epa_caches() -> None:
    fetch_epa_rows.cache_clear()
    epa_index_by_make.cache_clear()
