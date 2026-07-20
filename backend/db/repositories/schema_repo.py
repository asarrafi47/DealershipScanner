"""Inventory DDL: table/index creation, init, and seeding."""
import logging
import sqlite3

from backend.db.inventory_pg import (
    assert_inventory_backend_configured,
    inventory_sqlite_tests_allowed,
    is_inventory_postgres,
)
from backend.db.repositories.base_repo import get_conn

_log = logging.getLogger(__name__)


def ensure_cars_table_columns(cursor) -> None:
    """Add optional listing / quality columns (idempotent ALTERs)."""
    if is_inventory_postgres():
        return
    cursor.execute("PRAGMA table_info(cars)")
    existing = {row[1] for row in cursor.fetchall()}
    for col, ctype in [
        ("source_url", "TEXT"),
        ("body_style", "TEXT"),
        ("engine_description", "TEXT"),
        ("transmission_type", "TEXT"),
        ("condition", "TEXT"),
        ("description", "TEXT"),
        ("data_quality_score", "REAL"),
        ("is_cpo", "INTEGER"),
        ("model_full_raw", "TEXT"),
        ("mpg_city", "INTEGER"),
        ("mpg_highway", "INTEGER"),
        ("engine_l", "TEXT"),
        ("recovery_status", "TEXT"),
        ("recovery_attempted_at", "TEXT"),
        ("recovery_source", "TEXT"),
        ("recovery_notes", "TEXT"),
        ("missing_field_count", "INTEGER"),
        ("recoverability_score", "REAL"),
        ("spec_source_json", "TEXT"),
        ("packages", "TEXT"),
        ("listing_active", "INTEGER"),
        ("listing_removed_at", "TEXT"),
        ("interior_color_buckets", "TEXT"),
        ("first_seen_at", "TEXT"),
        ("last_price_change_at", "TEXT"),
        ("internal_notes", "TEXT"),
        ("marked_for_review", "INTEGER"),
        ("price_provenance_json", "TEXT"),
        ("forced_induction", "TEXT"),
        ("spin_frames", "TEXT"),
        ("interior_pano", "TEXT"),
        ("zip_code", "TEXT"),
        ("epa_master_id", "INTEGER"),
        ("epa_match_confidence", "REAL"),
        ("epa_match_method", "TEXT"),
    ]:
        if col not in existing:
            cursor.execute(f"ALTER TABLE cars ADD COLUMN {col} {ctype}")


# Columns needed for listings grid JSON (excludes multi-MB enrichment blobs).
LISTINGS_GRID_CAR_COLUMNS: tuple[str, ...] = (
    "id",
    "vin",
    "title",
    "year",
    "make",
    "model",
    "trim",
    "price",
    "mileage",
    "fuel_type",
    "cylinders",
    "transmission",
    "transmission_type",
    "drivetrain",
    "body_style",
    "exterior_color",
    "interior_color",
    "interior_color_buckets",
    "image_url",
    "dealer_name",
    "dealer_url",
    "dealer_id",
    "dealership_registry_id",
    "stock_number",
    "gallery",
    "packages",
    "engine_l",
    "engine_description",
    "condition",
    "data_quality_score",
    "listing_active",
    "mpg_city",
    "mpg_highway",
    "msrp",
    "carfax_url",
    "source_url",
    "scraped_at",
    "first_seen_at",
    "window_sticker_url",
    "history_highlights",
)


def ensure_cars_listings_indexes(cursor) -> None:
    """
    Partial indexes for active listings: facet DISTINCTs, price sort, dealer/geo filters.

    Idempotent (``IF NOT EXISTS``). Safe on SQLite and PostgreSQL.
    """
    active = "COALESCE(listing_active, 1) = 1"
    stmts = [
        f"CREATE INDEX IF NOT EXISTS idx_cars_active_price "
        f"ON cars(price) WHERE {active}",
        f"CREATE INDEX IF NOT EXISTS idx_cars_active_make "
        f"ON cars(make) WHERE {active} AND make IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS idx_cars_active_facet_combo "
        f"ON cars(make, model, trim, fuel_type, cylinders, drivetrain, body_style) "
        f"WHERE {active}",
        f"CREATE INDEX IF NOT EXISTS idx_cars_active_registry "
        f"ON cars(dealership_registry_id) WHERE {active} "
        f"AND dealership_registry_id IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS idx_cars_active_packages "
        f"ON cars(make, model) WHERE {active} AND packages IS NOT NULL "
        f"AND packages NOT IN ('{{}}', '[]', 'null', '')",
    ]
    for sql in stmts:
        cursor.execute(sql)
    if not is_inventory_postgres():
        try:
            cursor.execute("ANALYZE cars")
        except sqlite3.Error:
            pass


def ensure_scan_runs_table(cursor: sqlite3.Cursor) -> None:
    """Append-only scanner run summaries for store admin sync reliability (inventory.db)."""
    if is_inventory_postgres():
        return
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dealer_id TEXT NOT NULL,
            dealer_name TEXT,
            finished_at TEXT NOT NULL,
            duration_seconds REAL,
            upserted INTEGER,
            inventory_rows INTEGER,
            deduped_rows INTEGER,
            vdps_visited INTEGER,
            vehicles_vdp_enriched INTEGER,
            error TEXT,
            provider TEXT,
            summary_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_scan_runs_dealer_time ON scan_runs(dealer_id, finished_at DESC)"
    )


def ensure_nhtsa_vpic_cache_table(conn: sqlite3.Connection) -> None:
    """
    Optional SQLite cache for NHTSA vPIC ``DecodeVinValuesExtended`` JSON bodies.

    Full API document is stored in ``response_json`` (same shape as HTTP ``format=json``);
    provenance for row patches stays on ``cars.spec_source_json`` only.
    """
    if is_inventory_postgres():
        return
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nhtsa_vpic_cache (
            vin TEXT PRIMARY KEY NOT NULL,
            response_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def init_inventory_db():
    if is_inventory_postgres():
        from backend.db.inventory_pg import init_postgres_inventory, pg_connect

        conn = pg_connect()
        try:
            init_postgres_inventory(conn)
        finally:
            conn.close()
        seed_cars()
        try:
            from backend.db import incomplete_listings_db as ild

            ild.ensure_incomplete_index_built()
        except Exception:
            _log.exception("incomplete_listings index bootstrap failed")
        return

    assert_inventory_backend_configured()
    if not inventory_sqlite_tests_allowed():
        raise RuntimeError(
            "SQLite inventory init is disabled; set INVENTORY_DATABASE_URL to Postgres."
        )

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            vin              TEXT UNIQUE NOT NULL,
            title            TEXT,
            year             INTEGER,
            make             TEXT,
            model            TEXT,
            trim             TEXT,
            price            REAL,
            mileage          INTEGER,
            fuel_type        TEXT,
            cylinders        INTEGER,
            transmission     TEXT,
            drivetrain       TEXT,
            exterior_color   TEXT,
            interior_color   TEXT,
            image_url        TEXT,
            dealer_name      TEXT,
            dealer_url       TEXT,
            scraped_at       TEXT,
            dealer_id        TEXT,
            stock_number     TEXT,
            gallery          TEXT,
            carfax_url       TEXT,
            window_sticker_url TEXT,
            history_highlights TEXT,
            msrp             REAL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS epa_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            epa_vehicle_id INTEGER,
            year INTEGER,
            make TEXT,
            model TEXT,
            cylinders INTEGER,
            displacement REAL,
            trany TEXT,
            drive TEXT,
            fuel_type TEXT
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_epa_master_lookup ON epa_master(year, make, model)"
    )
    cursor.execute("PRAGMA table_info(epa_master)")
    epa_cols = [row[1] for row in cursor.fetchall()]
    for col, ctype in [
        ("city08", "REAL"),
        ("highway08", "REAL"),
        ("city_e", "REAL"),
        ("highway_e", "REAL"),
        ("atv_type", "TEXT"),
    ]:
        if col not in epa_cols:
            cursor.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {ctype}")
    cursor.execute("PRAGMA table_info(cars)")
    car_cols = [row[1] for row in cursor.fetchall()]
    if "msrp" not in car_cols:
        cursor.execute("ALTER TABLE cars ADD COLUMN msrp REAL")
    if "dealership_registry_id" not in car_cols:
        cursor.execute("ALTER TABLE cars ADD COLUMN dealership_registry_id INTEGER")
    ensure_cars_table_columns(cursor)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cars_dealer_listing ON cars(dealer_id, listing_active)"
    )
    ensure_cars_listings_indexes(cursor)
    ensure_nhtsa_vpic_cache_table(conn)
    ensure_scan_runs_table(cursor)
    try:
        cursor.execute(
            """
            UPDATE cars SET first_seen_at = scraped_at
            WHERE first_seen_at IS NULL AND scraped_at IS NOT NULL
            """
        )
        cursor.execute(
            """
            UPDATE cars SET last_price_change_at = scraped_at
            WHERE last_price_change_at IS NULL AND scraped_at IS NOT NULL
            """
        )
    except sqlite3.Error:
        _log.debug("first_seen / last_price backfill skipped")
    conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saved_cars (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            car_id    INTEGER NOT NULL,
            saved_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, car_id)
        )
    """)
    from backend.db.dealerships_db import ensure_dealerships_table

    ensure_dealerships_table(cursor)
    conn.commit()
    conn.close()
    seed_cars()
    try:
        from backend.db import incomplete_listings_db as ild

        ild.ensure_incomplete_index_built()
    except Exception:
        _log.exception("incomplete_listings index bootstrap failed")


# No bundled demo inventory; rows come from the scanner or tests.
SEED_DATA: list[tuple] = []


def seed_cars() -> None:
    if not SEED_DATA:
        return
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT OR IGNORE INTO cars
            (vin, title, year, make, model, trim, price, mileage,
             fuel_type, cylinders, transmission, drivetrain,
             exterior_color, interior_color, image_url, dealer_name, dealer_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        SEED_DATA,
    )
    conn.commit()
    conn.close()
