"""
Backward-compatible facade over ``backend.db.repositories``.

The implementation moved to per-entity repository modules
(``backend/db/repositories/*_repo.py``); every public and private name that was
importable from this module is re-exported here explicitly (no wildcard).

This module also OWNS two pieces of mutable state that tests patch directly:

* ``DB_PATH`` — repository readers resolve it dynamically through
  ``base_repo._resolve_db_path()`` so ``monkeypatch.setattr(inventory_db, "DB_PATH", …)``
  keeps working.
* ``_registry_backfill_ran`` — ``dealers_repo.ensure_dealership_registry_backfill``
  reads/writes it via ``sys.modules["backend.db.inventory_db"]``.
"""

from backend.db.inventory_pg import (
    assert_inventory_backend_configured,
    inventory_sqlite_tests_allowed,
    is_inventory_postgres,
)
from backend.db.repositories.base_repo import (
    DB_PATH,
    _REPO_ROOT,
    _default_inventory_db_path,
    _inventory_sqlite_lock_wait_sec,
    _placeholders,
    _resolve_db_path,
    _sqlite_connect_raw,
    db_conn,
    get_conn,
)
from backend.db.repositories.schema_repo import (
    LISTINGS_GRID_CAR_COLUMNS,
    SEED_DATA,
    ensure_cars_listings_indexes,
    ensure_cars_table_columns,
    ensure_nhtsa_vpic_cache_table,
    ensure_scan_runs_table,
    init_inventory_db,
    seed_cars,
)
from backend.db.repositories.cars_repo import (
    _UPDATABLE_CAR_COLUMNS,
    _parse_car_gallery,
    _parse_car_history_highlights,
    _parse_car_spin_frames,
    delete_cars_with_dummy_placeholder_vins,
    get_car_by_id,
    get_car_by_vin,
    get_cars_by_ids,
    is_dummy_placeholder_vin,
    update_car_row_partial,
)
from backend.db.repositories.saved_cars_repo import (
    get_saved_car_ids,
    is_car_saved,
    save_car,
    unsave_car,
)
from backend.db.repositories.search_repo import (
    MAKE_TO_COUNTRY,
    _EQUIPMENT_SEARCH_COLUMNS,
    _equipment_needle_params,
    _equipment_needle_sql_clause,
    _lookup_make_country,
    _makes_for_countries,
    _normalize_equipment_needles,
    _normalized_interior_bucket_filters,
    _sort_cars_by_price,
    search_cars,
    search_cars_by_make_model_pairs,
)
from backend.db.repositories.dealers_repo import (
    backfill_dealership_registry_ids,
    ensure_dealership_registry_backfill,
    link_cars_to_dealership_registry,
    list_scan_runs,
    record_scan_outcomes,
)
from backend.db.repositories.data_quality_repo import (
    _IncompleteIndexSnapshot,
    _car_id_int,
    _car_is_publicly_incomplete,
    _filter_public_listings_cars,
    _incomplete_car_ids_for_listings,
    _incomplete_index_snapshot_for_listings,
    _listings_cache_token,
    get_dealership_issue_stats,
    get_incomplete_cars,
    is_car_incomplete,
    listings_include_incomplete_cars,
    refresh_car_data_quality_score,
)
from backend.db.repositories.listings_repo import (
    _LISTINGS_GRID_CACHE_REV,
    _canonical_facet_label,
    _facet_make_valid,
    _facet_transmission_sane,
    _inventory_listings_cache_token,
    _normalize_facet_key,
    _normalize_make_capitalization,
    clear_inventory_listings_cache,
    get_filter_options,
    listings_geo_coords_maps,
    listings_grid_bootstrap_cars,
    listings_grid_cache_etag,
    listings_grid_serialized_cars,
    public_listings_count,
    serialize_car_for_listings_grid,
)

# Process-once flag for the dealership-registry backfill. Kept HERE (not in
# dealers_repo) because tests set ``inventory_db._registry_backfill_ran = True``.
_registry_backfill_ran = False
