"""
Incremental catalog linking — called from the scanner's post-upsert hook so
newly scanned (or re-scanned) cars get their ``epa_master_id`` immediately,
not on the next batch run of ``link_cars_to_catalog.py``.

Re-resolves every VIN it is given: if a rescan changed the dealer-observed
engine data, the link follows it, and a row that no longer resolves has its
link CLEARED (an unlinked car shows dealer data only — always safer than a
stale mislink).

A catalog that cannot be read is not a car that no longer resolves: on
:class:`~backend.catalog.resolver.CatalogUnavailableError` the whole batch is
abandoned before any UPDATE runs, so a dropped connection, an aborted
transaction or a catalog mid-reload never wipes good links.
"""
from __future__ import annotations

import logging

from backend.catalog.resolver import CatalogUnavailableError, candidates_for, resolve_from_candidates
from backend.db.inventory_db import get_conn

log = logging.getLogger(__name__)

_CAR_COLS = (
    "id", "year", "make", "model", "trim", "cylinders", "engine_l",
    "engine_description", "drivetrain", "fuel_type", "title", "epa_master_id", "vin",
)

_CLEAR_LINK_SQL = (
    "UPDATE cars SET epa_master_id=NULL, epa_match_confidence=NULL, "
    "epa_match_method=NULL WHERE id=?"
)
_SET_LINK_SQL = "UPDATE cars SET epa_master_id=?, epa_match_confidence=?, epa_match_method=? WHERE id=?"


def _rollback_quietly(conn) -> None:
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001 - the connection is closed right after
        pass


def link_cars_by_vins(vins: list[str]) -> int:
    """Resolve + persist catalog links for *vins*; returns rows updated.

    Every car is resolved before the first UPDATE runs, and the writes commit
    together. Any failure (a catalog read error above all) leaves every link as
    it was and returns 0."""
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
        try:
            from backend.enrichment.knowledge_engine import prime_vpic_cache

            prime_vpic_cache([r.get("vin") for r in rows])
        except Exception:  # noqa: BLE001
            pass
        cand_cache: dict[tuple, list] = {}
        writes: list[tuple[str, tuple]] = []
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
                # Raises CatalogUnavailableError on a read failure: nothing has
                # been written yet, so the batch is abandoned whole.
                cand_cache[key] = candidates_for(cur, y, mk, md)
            match = resolve_from_candidates(car, cand_cache[key])
            if match is None:
                if car.get("epa_master_id") is not None:
                    writes.append((_CLEAR_LINK_SQL, (car["id"],)))
                    updated += 1
                continue
            if car.get("epa_master_id") != match.epa_master_id:
                updated += 1
            writes.append(
                (_SET_LINK_SQL, (match.epa_master_id, match.confidence, match.method, car["id"]))
            )
        for sql, params in writes:
            cur.execute(sql, params)
        conn.commit()
    except CatalogUnavailableError:
        log.warning(
            "catalog unavailable; linking abandoned for %d VIN(s), no links changed",
            len(clean),
            exc_info=True,
        )
        _rollback_quietly(conn)
        updated = 0
    except Exception:
        log.exception("incremental catalog linking failed")
        _rollback_quietly(conn)
        updated = 0
    finally:
        conn.close()
    return updated
