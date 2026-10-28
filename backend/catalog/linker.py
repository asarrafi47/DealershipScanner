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


_FI_COLS = (
    "id", "vin", "year", "make", "model", "trim", "title", "cylinders", "engine_l",
    "engine_description", "fuel_type", "description", "forced_induction", "spec_source_json",
)


def _liters(value) -> float | None:
    try:
        return float(str(value).lower().split("l")[0].strip())
    except (TypeError, ValueError):
        return None


def _family_labels(car: dict, candidates: list[dict], twin: bool) -> list[str]:
    """Induction labels of the catalog rows sharing the car's displacement and
    cylinder count. Built from the EPA tCharger/sCharger flags and engine text,
    not the stored epa_master label. Empty when the car's engine is unknown."""
    from backend.utils.forced_induction import forced_induction_from_epa

    liters = _liters(car.get("engine_l"))
    if liters is None:
        return []
    try:
        cyl = int(car.get("cylinders"))
    except (TypeError, ValueError):
        cyl = None
    labels: list[str] = []
    for row in candidates:
        disp = _liters(row.get("displacement"))
        if disp is None or abs(disp - liters) > 0.05:
            continue
        if cyl and row.get("cylinders") not in (None, "") and str(row["cylinders"]) != str(cyl):
            continue
        label = forced_induction_from_epa(
            row.get("t_charger"), row.get("s_charger"), row.get("engine_description"),
            row.get("model"), row.get("atv_type"), twin_hint=twin,
        )
        if label:
            labels.append(label)
    return labels


def _reconcile_forced_induction(conn, vins: list[str], cand_cache: dict[tuple, list]) -> int:
    """P10C.3: re-resolve forced_induction for *vins* once their links are committed.

    Runs in its own transaction and never raises: a forced-induction bug (or an
    unreadable catalog) must not undo the links already committed. A stored value
    is replaced only through ``precedence.decide`` (blank, or weaker evidence),
    so dealer-text and VIN-decode labels survive. Returns rows changed."""
    try:
        from backend.enrichment.knowledge_engine import lookup_vpic_from_cache
        from backend.utils.forced_induction import (
            TWIN_TURBO, classify_forced_induction_from_car_row, resolve_car_forced_induction,
        )
        from backend.vehicle_facts.apply import apply_fact_proposals

        cur = conn.cursor()
        cur.execute(
            f"SELECT {', '.join(_FI_COLS)} FROM cars WHERE vin IN ({','.join('?' * len(vins))})",
            vins,
        )
        changed = 0
        for car in (dict(zip(_FI_COLS, r)) for r in cur.fetchall()):
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
            twin = (classify_forced_induction_from_car_row(car) or "") == TWIN_TURBO
            try:
                boosted = (lookup_vpic_from_cache(car.get("vin")) or {}).get("boosted")
            except Exception:  # noqa: BLE001 - a missing decode is "vPIC did not say"
                boosted = None
            label, src = resolve_car_forced_induction(
                car, vpic_boosted=boosted, family_flags=_family_labels(car, cand_cache[key], twin)
            )
            if not label:
                continue
            # Rank check, provenance merge and the write all go through the one
            # fact-write path; this connection only reads, so the links stay committed.
            res = apply_fact_proposals(None, car, [("forced_induction", label, src)])
            if not res.written:
                continue
            changed += 1
        conn.commit()
        return changed
    except Exception:  # noqa: BLE001 - never undo the committed links
        log.warning("forced-induction reconcile failed; links are committed", exc_info=True)
        _rollback_quietly(conn)
        return 0


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
        _reconcile_forced_induction(conn, clean, cand_cache)
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
