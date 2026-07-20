"""
Incremental catalog linking — called from the scanner's post-upsert hook so
newly scanned (or re-scanned) cars get their ``epa_master_id`` immediately,
not on the next batch run of ``link_cars_to_catalog.py``.

Re-resolves every VIN it is given: if a rescan changed the dealer-observed
engine data, the link follows it, and a row that no longer resolves has its
link CLEARED (an unlinked car shows dealer data only — always safer than a
stale mislink).
"""
from __future__ import annotations

import logging

from backend.catalog.resolver import candidates_for, resolve_from_candidates
from backend.db.inventory_db import get_conn

log = logging.getLogger(__name__)

_CAR_COLS = (
    "id", "year", "make", "model", "trim", "cylinders", "engine_l",
    "engine_description", "drivetrain", "fuel_type", "title", "epa_master_id",
)


def link_cars_by_vins(vins: list[str]) -> int:
    """Resolve + persist catalog links for *vins*; returns rows updated."""
    clean = [v.strip() for v in vins if v and str(v).strip()]
    if not clean:
        return 0
    conn = get_conn()
    updated = 0
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(clean))
        cur.execute(
            f"SELECT {', '.join(_CAR_COLS)} FROM cars WHERE vin IN ({placeholders})",
            clean,
        )
        rows = [dict(zip(_CAR_COLS, r)) for r in cur.fetchall()]
        cand_cache: dict[tuple, list] = {}
        for car in rows:
            try:
                y = int(car.get("year"))
            except (TypeError, ValueError):
                continue
            mk = (car.get("make") or "").strip()
            md = (car.get("model") or "").strip()
            if not mk or not md:
                continue
            key = (y, mk.lower(), md.lower())
            if key not in cand_cache:
                cand_cache[key] = candidates_for(cur, y, mk, md)
            match = resolve_from_candidates(car, cand_cache[key])
            if match is None:
                if car.get("epa_master_id") is not None:
                    cur.execute(
                        "UPDATE cars SET epa_master_id=NULL, epa_match_confidence=NULL, "
                        "epa_match_method=NULL WHERE id=?",
                        (car["id"],),
                    )
                    updated += 1
                continue
            if car.get("epa_master_id") != match.epa_master_id:
                updated += 1
            cur.execute(
                "UPDATE cars SET epa_master_id=?, epa_match_confidence=?, epa_match_method=? WHERE id=?",
                (match.epa_master_id, match.confidence, match.method, car["id"]),
            )
        conn.commit()
    except Exception:
        log.exception("incremental catalog linking failed")
    finally:
        conn.close()
    return updated
