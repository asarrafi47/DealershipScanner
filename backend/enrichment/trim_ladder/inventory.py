"""Build a ladder from trims observed in live inventory."""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _clean_trim_label,
    _is_valid_trim_name,
    _norm_make,
    _norm_model,
)
from .plausibility import (
    _finalize_ladder_steps,
)

# ---------------------------------------------------------------------------
# _ENGINE_COMPARISON_REMOVED
#
# A derived "this trim steps up the engine — up from the X on the trims below"
# bullet used to live here. It was deleted, not repaired, because every premise
# a comparative engine claim needs is false in the data we actually hold:
#
#  1. No trustworthy induction / hybrid signal. cars.forced_induction is wrong on
#     real rows: 2025 F-150 "Raptor R" (supercharged 5.2 Predator) is stored as
#     'Twin Turbocharged', and the naturally aspirated 5.0 Coyote appears as
#     'Twin Turbocharged' on Tremor/XL/XLT rows. Without induction and hybrid
#     drive, the 3.5 EcoBoost twin-turbo and the 3.5 PowerBoost full hybrid
#     collapse into one (displacement, cylinders) bucket.
#  2. No power signal, so no direction. epa_extended_specs.horsepower is NULL for
#     every 2025 F150 row and a degenerate 293 for every Grand Cherokee row
#     including the 4xe. Nothing in our schema orders two engines.
#  3. No trim authority. epa_master.trim holds body/drive descriptors
#     ("Pickup 4WD", "RAPTOR R 4WD"), not marketing trims, and inventory trim
#     strings alias across genuinely different variants ("Raptor" vs "Raptor R").
#
# Per the standing instruction that a confident wrong bullet is worse than no
# bullet, the ladder now emits nothing about engines unless a source document
# (brochure / curated ladder / trim spec sheet) says it. Do not reintroduce a
# comparison off cars.engine_l + cars.cylinders — that is exactly what shipped
# "2.0L Hurricane I4 engine — up from the 3.6L V6" onto a 2.0L car.
# ---------------------------------------------------------------------------


def _inventory_trim_rows(
    make_norm: str,
    model_label: str,
    year_lo: int,
    year_hi: int,
) -> list[Any]:
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT trim, AVG(price) AS avg_price, COUNT(*) AS n
            FROM cars
            WHERE (COALESCE(listing_active, 1) = 1)
              AND LOWER(TRIM(make)) = LOWER(?)
              AND LOWER(TRIM(model)) = LOWER(?)
              AND year BETWEEN ? AND ?
              AND trim IS NOT NULL
              AND TRIM(trim) != ''
              AND price IS NOT NULL
              AND price > 0
            GROUP BY LOWER(TRIM(trim))
            ORDER BY avg_price ASC
            LIMIT 24
            """,
            (make_norm, model_label, year_lo, year_hi),
        )
        return cur.fetchall()


def _ladder_from_inventory(make: str, model: str, year: Any) -> dict[str, Any] | None:
    """Build a price-ordered trim ladder from active inventory."""
    try:
        y = int(year)
        year_ranges = [(y - 3, y + 1), (y - 6, y + 2), (0, 9999)]
    except (TypeError, ValueError):
        year_ranges = [(0, 9999)]

    make_norm = (make or "").strip()
    model_norm = _norm_model(model)
    model_label = (model or "").strip()
    if not make_norm or not model_norm:
        return None

    rows: list[Any] = []
    for year_lo, year_hi in year_ranges:
        try:
            rows = _inventory_trim_rows(make_norm, model_label, year_lo, year_hi)
        except Exception as e:
            logger.debug("inventory trim ladder query failed: %s", e)
            return None
        if len(rows) >= 2:
            break

    steps: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            trim_raw = row.get("trim")
            avg_price = row.get("avg_price")
            count = row.get("n")
        else:
            trim_raw = row[0]
            avg_price = row[1]
            count = row[2]
        from backend.enrichment.trim_ladder_knowledge import canonical_trim_name, preserve_trim_label

        name = (
            preserve_trim_label(str(trim_raw or ""), make_norm, model_label)
            or canonical_trim_name(str(trim_raw or ""), make_norm, model_label)
            or _clean_trim_label(str(trim_raw or ""))
        )
        if not _is_valid_trim_name(name, make=make_norm, model=model_label):
            continue
        if _norm_model(name) == model_norm:
            continue
        try:
            price_note = f"Typical listing price near ${int(float(avg_price)):,} in our inventory ({int(count)} listings)."
        except (TypeError, ValueError):
            price_note = "Seen on similar listings in our inventory."
        steps.append(
            {
                "name": name,
                "aliases": [],
                "adds": [],
                "inventory_price_note": price_note,
            }
        )

    if len(steps) < 2:
        return None

    ymin = year_ranges[0][0] if year_ranges else 0
    ymax = year_ranges[0][1] if year_ranges else 9999
    for lo, hi in year_ranges:
        if rows:
            ymin, ymax = lo, hi
            break

    ladder = {
        "id": f"inventory_{_norm_make(make)}_{model_norm}",
        "make": make_norm,
        "models": [model, model_norm],
        "year_min": ymin,
        "year_max": ymax,
        "label": f"{make_norm} {model} trim lineup",
        "source": "inventory",
        "steps": steps,
    }
    return _finalize_ladder_steps(ladder, make_norm, model_label)
