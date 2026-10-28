"""Post-write enrichment of :func:`backend.scanner.database.upsert_vehicles`.

Three independent steps run after the car writes committed, each isolated so a
failure is logged and never fails the upsert (behavior kept from before the
split): incomplete-listing sync + structured spec backfill, the model_specs
dictionary correction, and the catalog link.

``get_conn`` and ``apply_model_specs_corrections`` are passed in by the
orchestrator (looked up on ``backend.scanner.database`` at call time) so tests
that monkeypatch them there keep working.
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from backend.scanner.upsert.timing import lap

# Same logger name as before the split so log routing and captures are unchanged.
logger = logging.getLogger("backend.scanner.database")

_ID_CHUNK = 500

# The ``post_write`` timing block of the run in progress on this thread (P11A.7). A
# thread-local so the step functions keep their signatures (tests patch them).
_sink = threading.local()


def _lap(name: str):
    return lap(getattr(_sink, "block", None), name)


def resolve_car_ids(get_conn: Callable[[], Any], vins: list[str]) -> list[int]:
    """VIN -> cars.id, on a connection that is closed before returning.

    The per-car work that follows does network I/O (NHTSA vPIC) and opens its
    own connections; holding this one open across that loop is the
    ``idle in transaction`` session that parked the web app behind a queued
    CREATE INDEX on ``cars``.
    """
    car_ids: list[int] = []
    conn2 = get_conn()
    try:
        cur2 = conn2.cursor()
        for i in range(0, len(vins), _ID_CHUNK):
            chunk = vins[i : i + _ID_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            cur2.execute(f"SELECT id FROM cars WHERE vin IN ({placeholders}) ORDER BY id", chunk)
            for row_id in cur2.fetchall():
                try:
                    car_ids.append(int(row_id[0]))
                except (TypeError, ValueError):
                    continue
        conn2.commit()
    finally:
        conn2.close()
    return car_ids


def sync_incomplete_and_backfill_specs(vins: list[str], get_conn: Callable[[], Any]) -> None:
    """Step 1: refresh the incomplete-listings index row of each car, then run the
    EPA/trim merge (tier 1) + NHTSA vPIC (tier 2) backfill where transmission /
    cylinders (and other vPIC-fillable fields) are still open."""
    try:
        from backend.db import incomplete_listings_db as ild
        from backend.db.inventory_db import get_car_by_id
        from backend.enrichment.spec_structured_backfill import (
            apply_structured_spec_backfill_for_car,
            car_needs_transmission_or_cylinders_backfill,
        )

        car_ids = resolve_car_ids(get_conn, vins)
        block = getattr(_sink, "block", None)
        if block is not None:
            block["cars"] = len(car_ids)
        for cid in car_ids:
            with _lap("spec_backfill"):
                car = get_car_by_id(cid, include_inactive=True)
                if car and car_needs_transmission_or_cylinders_backfill(car):
                    apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
            # Sync after the backfill (N6.2), not before: the backfill fills the
            # fields the index flags, so a sync first left complete cars flagged.
            with _lap("incomplete_sync"):
                ild.sync_incomplete_listing_for_car_id(cid)
    except Exception:
        logger.exception("incomplete_listings / spec backfill after upsert failed")


def correct_from_model_specs(vins: list[str], apply_model_specs_corrections: Callable[..., Any]) -> None:
    """Step 2: model_specs dictionary correction (cylinders + transmission ...)."""
    try:
        apply_model_specs_corrections(vins=vins)
    except Exception:
        logger.exception("model_specs correction after upsert failed")


def link_catalog(vins: list[str]) -> None:
    """Step 3: catalog link (cars.epa_master_id) so new scans join the catalog
    immediately instead of waiting for the batch linker."""
    try:
        from backend.catalog.linker import link_cars_by_vins

        link_cars_by_vins(vins)
    except Exception:
        logger.exception("catalog linking after upsert failed")


def resync_after_late_writers(vins: list[str], get_conn: Callable[[], Any]) -> None:
    """Step 4 (N6.2): model_specs corrections and the catalog link write after
    step 1's sync; refresh the index rows once more so no flag goes stale."""
    try:
        from backend.db import incomplete_listings_db as ild

        for cid in resolve_car_ids(get_conn, vins):
            with _lap("incomplete_sync"):
                ild.sync_incomplete_listing_for_car_id(cid)
    except Exception:
        logger.exception("incomplete_listings re-sync after late writers failed")


def run_post_write_enrichment(
    vins: list[str],
    *,
    get_conn: Callable[[], Any],
    apply_model_specs_corrections: Callable[..., Any],
    timing: dict | None = None,
) -> None:
    """Run the four steps. ``timing`` (optional) receives the ``post_write`` block:
    wall/CPU per step plus ``cars``, ``get_conn_calls`` (through the orchestrator's
    ``get_conn``) and ``vpic_network_fetches`` (live vPIC decodes on this thread)."""
    if timing is None:
        _sink.block = None
        sync_incomplete_and_backfill_specs(list(vins), get_conn)
        correct_from_model_specs(list(vins), apply_model_specs_corrections)
        link_catalog(list(vins))
        resync_after_late_writers(list(vins), get_conn)
        return
    from backend.enrichment.spec_structured_backfill import vpic_network_fetch_count

    timing.update({"cars": 0, "get_conn_calls": 0, "vpic_network_fetches": 0})

    def counted_conn():
        timing["get_conn_calls"] += 1
        return get_conn()

    _sink.block = timing
    vpic0 = vpic_network_fetch_count()
    try:
        with lap(timing, "total"):
            sync_incomplete_and_backfill_specs(list(vins), counted_conn)
            with lap(timing, "model_specs"):
                correct_from_model_specs(list(vins), apply_model_specs_corrections)
            with lap(timing, "catalog_link"):
                link_catalog(list(vins))
            resync_after_late_writers(list(vins), counted_conn)
    finally:
        _sink.block = None
        timing["vpic_network_fetches"] = vpic_network_fetch_count() - vpic0
