"""
Persist knowledge-engine enrichment results back to the cars table.

merge_verified_specs() fills gaps at render time but doesn't write them to the DB.
This module bridges that gap: it runs the engine on a car, then calls
update_car_row_partial() for every field that is currently NULL/empty in the DB
but resolvable from the knowledge engine or EPA master data.

Only fills fields that are genuinely blank — never overwrites dealer-provided data.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.db.inventory_db import get_car_by_id, update_car_row_partial
from backend.enrichment.knowledge_engine import merge_verified_specs
from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.field_clean import is_effectively_empty

log = logging.getLogger(__name__)

# (db_column, primary_vs_key, fallback_vs_key)
# All keys refer to the dict returned by merge_verified_specs().
_FILLABLE: list[tuple[str, str, str | None]] = [
    ("cylinders",          "cylinders",             None),
    ("drivetrain",         "drivetrain",             None),
    ("transmission",       "transmission_display",   None),
    ("fuel_type",          "fuel_type_hint",         "epa_fuel_type"),
    ("body_style",         "body_style_display",     None),
    ("engine_l",           "epa_displacement",       None),
    ("engine_description", "epa_engine_description", "master_engine_string"),
    ("mpg_city",           "epa_city08",             None),
    ("mpg_highway",        "epa_highway08",          None),
]


def _is_blank(car: dict, field: str) -> bool:
    v = car.get(field)
    if v is None:
        return True
    if isinstance(v, (int, float)):
        return False
    return is_effectively_empty(str(v))


def _coerce(field: str, value: Any) -> Any:
    if value is None:
        return None
    if field == "cylinders":
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if field in ("engine_l", "mpg_city", "mpg_highway"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return str(value).strip() if isinstance(value, str) else value


def enrich_car_and_persist(car_id: int) -> dict[str, Any]:
    """
    Run the knowledge engine on *car_id* and write back any fields currently
    blank in the DB.

    Returns {field: new_value} for every field updated, or {} if nothing changed.
    """
    car = get_car_by_id(car_id)
    if not car:
        return {}

    vs = merge_verified_specs(car)
    # Serialize with verified specs to get display-derived fields (e.g. condition from title)
    serialized = serialize_car_for_api(car, include_verified=False, verified_specs=vs)
    updates: dict[str, Any] = {}

    for db_col, primary_key, fallback_key in _FILLABLE:
        if not _is_blank(car, db_col):
            continue

        value = vs.get(primary_key)
        if (value is None or (isinstance(value, str) and is_effectively_empty(value))) and fallback_key:
            value = vs.get(fallback_key)

        value = _coerce(db_col, value)
        if value is not None and not (isinstance(value, str) and is_effectively_empty(value)):
            if db_col == "cylinders":
                from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text

                if cylinders_conflicts_with_engine_text(value, car.get("engine_description")):
                    continue
            updates[db_col] = value

    # Persist condition inferred from title/mileage/URL (fill_derived_condition_for_display)
    if _is_blank(car, "condition"):
        derived_cond = serialized.get("condition")
        if derived_cond and derived_cond not in ("—", "-") and not is_effectively_empty(derived_cond):
            updates["condition"] = derived_cond

    if updates:
        try:
            update_car_row_partial(car_id, updates)
            log.debug("car %d: filled %s", car_id, list(updates.keys()))
        except Exception:
            log.exception("car %d: failed to persist enrichment", car_id)
            return {}

    return updates


def enrich_all_cars(car_ids: list[int] | None = None) -> dict[str, int]:
    """
    Run enrichment over *car_ids* (or all active cars when None).

    Returns aggregate stats: {field_name: count_of_cars_where_that_field_was_filled}.
    """
    if car_ids is None:
        from backend.db.inventory_db import db_conn
        with db_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM cars WHERE COALESCE(listing_active, 1) = 1"
            ).fetchall()
        car_ids = [r[0] for r in rows]

    stats: dict[str, int] = {}
    for car_id in car_ids:
        for field in enrich_car_and_persist(car_id):
            stats[field] = stats.get(field, 0) + 1

    return stats
