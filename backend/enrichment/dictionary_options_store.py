"""Complete_Options table lookups (replaces runtime *_Complete_Options.csv reads)."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from backend.enrichment.dictionary_catalog import canonical_make, catalog_key
from backend.enrichment.epa_master_store import _model_search_variants


def _norm_token(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _row_to_csv_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        get = row.get
    elif isinstance(row, (tuple, list)):
        (
            year,
            make,
            model,
            trim,
            engine_options,
            engine_display,
            forced_induction,
            transmission,
            drivetrain,
            fuel_type,
            body_style,
            cylinders,
            displacement,
            mpg_city,
            mpg_highway,
            mpg_combined,
            exterior_colors,
            packages,
            package_details,
            options,
            option_details,
        ) = row
        return {
            "Year": year,
            "Make": make,
            "Model": model,
            "Trim": trim or "",
            "engineOptions": engine_options or "",
            "engineDisplay": engine_display or "",
            "forcedInduction": forced_induction or "",
            "transmissionOptions": transmission or "",
            "drivetrainOptions": drivetrain or "",
            "fuelType": fuel_type or "",
            "bodyStyle": body_style or "",
            "cylinders": cylinders,
            "displacement": displacement,
            "mpg_city": mpg_city,
            "mpg_highway": mpg_highway,
            "mpg_combined": mpg_combined,
            "exteriorColors": exterior_colors or "",
            "Packages": packages or "",
            "packageDetails": package_details or "",
            "Options": options or "",
            "optionDetails": option_details or "",
        }
    else:
        get = lambda k, d=None: row[k] if k in row.keys() else d  # type: ignore[attr-defined]

    out: dict[str, Any] = {
        "Year": get("year"),
        "Make": get("make"),
        "Model": get("model"),
        "Trim": get("trim") or "",
        "engineOptions": get("engine_options") or "",
        "engineDisplay": get("engine_display") or "",
        "forcedInduction": get("forced_induction") or "",
        "transmissionOptions": get("transmission") or "",
        "drivetrainOptions": get("drivetrain") or "",
        "fuelType": get("fuel_type") or "",
        "bodyStyle": get("body_style") or "",
        "cylinders": get("cylinders"),
        "displacement": get("displacement"),
        "mpg_city": get("mpg_city"),
        "mpg_highway": get("mpg_highway"),
        "mpg_combined": get("mpg_combined"),
        "exteriorColors": get("exterior_colors") or "",
        "Packages": get("packages") or "",
        "packageDetails": get("package_details") or "",
        "Options": get("options") or "",
        "optionDetails": get("option_details") or "",
    }
    return {k: ("" if v is None else v) for k, v in out.items()}


def _query_options_rows(year: int, make: str, model: str) -> list[dict[str, Any]]:
    from backend.db.inventory_db import get_conn

    mk = canonical_make(make)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT year, make, model, trim, engine_options, engine_display, forced_induction,
                   transmission, drivetrain, fuel_type, body_style, cylinders, displacement,
                   mpg_city, mpg_highway, mpg_combined, exterior_colors,
                   packages, package_details, options, option_details
            FROM dictionary_options
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
def fetch_options_rows(year: int, make: str, model: str) -> tuple[dict[str, Any], ...]:
    try:
        y = int(year)
    except (TypeError, ValueError):
        return ()
    mk = (make or "").strip()
    md = (model or "").strip()
    if not y or not mk or not md:
        return ()

    for variant in _model_search_variants(mk, md):
        rows = _query_options_rows(y, mk, variant)
        if rows:
            return tuple(rows)
    return ()


def has_options_rows(year: int, make: str, model: str) -> bool:
    return bool(fetch_options_rows(year, make, model))


def options_status_for_rows(rows: tuple[dict[str, Any], ...] | list[dict[str, Any]]) -> str:
    if not rows:
        return "missing"
    non_empty = 0
    for row in rows:
        trim = (row.get("Trim") or "").strip()
        if not trim or trim == "[Brochure Summary]":
            continue
        if any(
            (row.get(k) or "").strip()
            for k in ("Packages", "packageDetails", "Options", "optionDetails", "engineDisplay")
        ):
            non_empty += 1
    if non_empty >= 2:
        return "rich"
    if non_empty == 1:
        return "sparse"
    return "stub"


def options_manifest_buckets() -> dict[str, dict[str, Any]]:
    from backend.db.inventory_db import get_conn

    grouped: dict[str, dict[str, Any]] = {}
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT year, make, model, COUNT(*) AS n
            FROM dictionary_options
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
                "options_path": None,
                "options_row_count": int(n),
            }
    finally:
        conn.close()
    return grouped


def clear_options_caches() -> None:
    fetch_options_rows.cache_clear()
