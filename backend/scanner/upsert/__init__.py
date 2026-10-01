"""Named steps of :func:`backend.scanner.database.upsert_vehicles`.

``upsert_vehicles`` itself stays in ``backend.scanner.database`` (same import
path and signature) as a short orchestrator over these modules:

* :mod:`.rows`       -- row cleanup rules (dedupe/sort by VIN, normalization,
                        price / mileage / msrp / image / pano coercion)
* :mod:`.serialize`  -- JSON column serialization (gallery, spin frames,
                        highlights, packages, spec provenance, price history)
* :mod:`.sql`        -- the INSERT ... ON CONFLICT statement and its binder
* :mod:`.guard`      -- VIN ownership guard prefetch / backstop / reporting
* :mod:`.write`      -- the per-vehicle write loop
* :mod:`.post_write` -- post-write enrichment steps
"""
from backend.scanner.upsert.guard import (
    GuardWindow,
    backstop_owner,
    guard_window,
    prefetch_existing,
    report_conflicts,
    reset_stats,
)
from backend.scanner.upsert.post_write import (
    correct_from_model_specs,
    link_catalog,
    resolve_car_ids,
    run_post_write_enrichment,
    sync_incomplete_and_backfill_specs,
)
from backend.scanner.upsert.rows import (
    coerce_mileage,
    coerce_msrp,
    coerce_price,
    dedupe_sorted_by_vin,
    image_url_or_placeholder,
    interior_pano_url,
    normalize_vehicle_row,
    row_title,
)
from backend.scanner.upsert.serialize import (
    PRICE_HISTORY_MAX_ENTRIES,
    gallery_json,
    history_highlights_json,
    packages_json,
    price_history_json,
    spec_source_json,
    spin_frames_json,
)
from backend.scanner.upsert.sql import (
    UPSERT_CARS_SQL,
    PreparedRow,
    fetch_prev_price_row,
    prepare_row,
    trace_vin_readback,
    upsert_params,
)
from backend.scanner.upsert.write import write_rows

__all__ = [
    "GuardWindow",
    "PRICE_HISTORY_MAX_ENTRIES",
    "PreparedRow",
    "UPSERT_CARS_SQL",
    "backstop_owner",
    "coerce_mileage",
    "coerce_msrp",
    "coerce_price",
    "correct_from_model_specs",
    "dedupe_sorted_by_vin",
    "fetch_prev_price_row",
    "gallery_json",
    "guard_window",
    "history_highlights_json",
    "image_url_or_placeholder",
    "interior_pano_url",
    "link_catalog",
    "normalize_vehicle_row",
    "packages_json",
    "prefetch_existing",
    "prepare_row",
    "price_history_json",
    "report_conflicts",
    "reset_stats",
    "resolve_car_ids",
    "row_title",
    "run_post_write_enrichment",
    "spec_source_json",
    "spin_frames_json",
    "sync_incomplete_and_backfill_specs",
    "trace_vin_readback",
    "upsert_params",
    "write_rows",
]
