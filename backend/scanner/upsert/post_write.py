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
from typing import Any, Callable

# Same logger name as before the split so log routing and captures are unchanged.
logger = logging.getLogger("backend.scanner.database")

_ID_CHUNK = 500


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
            cur2.execute(f"SELECT id FROM cars WHERE vin IN ({placeholders})", chunk)
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

        for cid in resolve_car_ids(get_conn, vins):
            ild.sync_incomplete_listing_for_car_id(cid)
            car = get_car_by_id(cid, include_inactive=True)
            if not car or not car_needs_transmission_or_cylinders_backfill(car):
                continue
            apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
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


def run_post_write_enrichment(
    vins: list[str],
    *,
    get_conn: Callable[[], Any],
    apply_model_specs_corrections: Callable[..., Any],
) -> None:
    sync_incomplete_and_backfill_specs(list(vins), get_conn)
    correct_from_model_specs(list(vins), apply_model_specs_corrections)
    link_catalog(list(vins))
