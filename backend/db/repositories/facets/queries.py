"""The facet reads: each takes the open cursor and issues its own SQL.

``listings_repo._build_filter_options_uncached`` calls these in a fixed order
(scalars, paint families, body styles, packages, relationship rows); the facet
golden pins that order and every statement's text.
"""
from __future__ import annotations

import json
from typing import Any

from backend.db.repositories.facets.labels import (
    _facet_make_valid,
    _facet_transmission_sane,
)
from backend.utils.field_clean import (
    coerce_body_style_stored,
    coerce_fuel_type_stored,
    is_effectively_empty,
    sort_fuel_type_presets,
)
from backend.utils.fuel_type_normalize import normalize_fuel_type_for_display
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    sort_paint_family_ids,
)

ACTIVE_SQL = "(COALESCE(listing_active, 1) = 1)"


def distinct_values(cursor: Any, col: str) -> list:
    """Sorted non-empty ``DISTINCT col`` over active listings."""
    cursor.execute(
        f"SELECT DISTINCT {col} FROM cars WHERE {ACTIVE_SQL} AND {col} IS NOT NULL ORDER BY {col}"
    )
    return [r[0] for r in cursor.fetchall() if not is_effectively_empty(r[0])]


def scalar_facets(cursor: Any) -> dict[str, list]:
    """Fuel types (preset order), cylinders, sane transmissions, drivetrains, induction."""
    return {
        "fuel_types": sort_fuel_type_presets(distinct_values(cursor, "fuel_type")),
        "cylinders": distinct_values(cursor, "cylinders"),
        "transmissions": [
            t for t in distinct_values(cursor, "transmission") if _facet_transmission_sane(t)
        ],
        "drivetrains": distinct_values(cursor, "drivetrain"),
        "forced_inductions": distinct_values(cursor, "forced_induction"),
    }


def paint_family_facets(cursor: Any) -> tuple[list[str], list[str]]:
    """Exterior and interior paint-family ids present on active listings.

    Interior prefers the stored ``interior_color_buckets`` and infers from the raw
    color only when none are stored.
    """
    cursor.execute(
        f"""
            SELECT exterior_color, interior_color, interior_color_buckets
            FROM cars
            WHERE {ACTIVE_SQL}
            """
    )
    ext_facet_ids: set[str] = set()
    int_facet_ids: set[str] = set()
    for ext_raw, int_raw, int_bucks in cursor.fetchall():
        if not is_effectively_empty(ext_raw):
            ext_facet_ids.update(infer_paint_color_buckets(ext_raw, None))
        ib = parse_stored_buckets(int_bucks)
        if ib:
            int_facet_ids.update(ib)
        elif not is_effectively_empty(int_raw):
            int_facet_ids.update(infer_paint_color_buckets(int_raw, None))
    return sort_paint_family_ids(ext_facet_ids), sort_paint_family_ids(int_facet_ids)


def _package_names(pkg: dict) -> list[str]:
    names: list[str] = []
    for entry in (pkg.get("packages_normalized") or []):
        if isinstance(entry, dict):
            n = (entry.get("canonical_name") or entry.get("name") or "").strip()
            if n:
                names.append(n)
    for n in (pkg.get("possible_packages") or []):
        if isinstance(n, str) and n.strip():
            names.append(n.strip())
    return names


def package_facets(cursor: Any) -> tuple[list[dict[str, str]], list[str]]:
    """Packages per make/model (for the filter cascade) and all package names, sorted."""
    package_rows: list[dict[str, str]] = []
    all_package_names: list[str] = []
    seen_pkg_keys: set[tuple] = set()
    seen_pkg_names: set[str] = set()
    cursor.execute(
        f"SELECT make, model, packages FROM cars "
        f"WHERE {ACTIVE_SQL} AND packages IS NOT NULL "
        f"AND packages NOT IN ('{{}}', '[]', 'null', '')"
    )
    for make, model, pkg_raw in cursor.fetchall():
        if not make or not model:
            continue
        try:
            pkg = json.loads(pkg_raw)
        except Exception:
            continue
        for n in _package_names(pkg):
            key = (make.lower(), model.lower(), n.lower())
            if key not in seen_pkg_keys:
                seen_pkg_keys.add(key)
                package_rows.append({"make": make, "model": model, "name": n})
            if n.lower() not in seen_pkg_names:
                seen_pkg_names.add(n.lower())
                all_package_names.append(n)
    all_package_names.sort()
    return package_rows, all_package_names


def _clean_relationship_row(row) -> tuple | None:
    """One DISTINCT row -> the emitted 8-tuple, or None when make/model is unusable."""
    make, model, trim, fuel_type, cyl, drive, body_st, induction = (
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
    )
    year, engine_description, engine_l = row[8], row[9], row[10]
    if is_effectively_empty(make) or is_effectively_empty(model):
        return None
    if not _facet_make_valid(make):
        return None
    if is_effectively_empty(trim):
        trim = None
    if is_effectively_empty(fuel_type):
        fuel_type = None
    else:
        fuel_type = coerce_fuel_type_stored(fuel_type)
        # Same rule the card serializer applies, so the cascade never
        # advertises a fuel the grid cannot show: a Ram 1500 eTorque is
        # fed to us as "Hybrid" but renders (and must filter) as gas.
        # The client card filter matches the SERIALIZED value, so an
        # unnormalized facet here yields an empty grid when picked.
        corrected = normalize_fuel_type_for_display(
            {
                "make": make,
                "model": model,
                "trim": trim,
                "year": year,
                "engine_description": engine_description,
                "engine_l": engine_l,
            },
            fuel_type=fuel_type,
            # No epa_master_id in this DISTINCT projection, so skip the
            # catalog read rather than firing one per facet row.
            catalog_fuel_type=None,
        )
        if corrected:
            fuel_type = corrected
    if is_effectively_empty(drive):
        drive = None
    if is_effectively_empty(body_st):
        body_st = None
    else:
        body_st = coerce_body_style_stored(body_st)
    if is_effectively_empty(induction):
        induction = None
    return (make, model, trim, fuel_type, cyl, drive, body_st, induction)


def relationship_rows(cursor: Any) -> list[tuple]:
    """Every unique combo of the filterable dims, cleaned and deduped.

    The frontend embeds these as data-* on each checkbox so it can filter any
    dropdown based on any combination of other active filters. ``year`` /
    ``engine_description`` / ``engine_l`` are selected but NOT emitted: the
    mild-hybrid fuel correction is a per-row rule that needs them, and they are
    the same engine evidence the card serializer sees (a Ram 1500 whose
    engine_description is feed junk still matches on engine_l -- one such row
    today). They roughly double the DISTINCT row count (12.8k -> 26.6k, +0.02s on
    71k active rows) and are deduped back out here, so the payload the frontend
    embeds is unchanged.
    """
    cursor.execute(f"""
            SELECT DISTINCT make, model, trim, fuel_type, cylinders, drivetrain, body_style,
                   forced_induction, year, engine_description, engine_l
            FROM cars
            WHERE {ACTIVE_SQL}
              AND make IS NOT NULL AND TRIM(make) != ''
            ORDER BY make, model, trim
        """)
    raw_car_rows = cursor.fetchall()
    car_rows: list = []
    seen: set[tuple] = set()
    for row in raw_car_rows:
        entry = _clean_relationship_row(row)
        if entry is None or entry in seen:
            continue
        seen.add(entry)
        car_rows.append(entry)
    return car_rows


def car_rows_payload(car_rows: list[tuple]) -> list[dict[str, Any]]:
    """The ``car_rows`` facet: one dict per relationship tuple."""
    return [
        {
            "make": r[0],
            "model": r[1],
            "trim": r[2],
            "fuel": r[3],
            "cyl": r[4],
            "drive": r[5],
            "body_style": r[6] if len(r) > 6 else None,
            "induction": r[7] if len(r) > 7 else None,
        }
        for r in car_rows
    ]
