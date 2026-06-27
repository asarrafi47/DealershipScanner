"""Normalized vehicle catalog: trim spine (OEM-only) + packages/options/colors.

Mechanical / EPA specs (MPG, transmission, forced induction, cylinders, etc.) live in
``epa_master``. Join on ``(year, make, model, trim)`` when both are needed.
"""

from __future__ import annotations

from typing import Any

# Duplicated by epa_master — dropped from catalog_trims on migrate.
CATALOG_TRIMS_EPA_DUPLICATE_COLUMNS: tuple[str, ...] = (
    "body_style",
    "engine_l",
    "engine_desc",
    "cylinders",
    "fuel_type",
    "forced_induction",
    "transmission",
    "trans_speeds",
    "drivetrain",
    "mpg_city",
    "mpg_highway",
    "mpg_combined",
    "range_miles",
)

_CATALOG_TRIMS_DDL_POSTGRES = """
            CREATE TABLE IF NOT EXISTS catalog_trims (
                id BIGSERIAL PRIMARY KEY,
                year INTEGER NOT NULL,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                trim TEXT,
                trim_level TEXT,
                horsepower INTEGER,
                torque_lb_ft INTEGER,
                base_msrp DOUBLE PRECISION,
                source TEXT,
                notes TEXT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                search_vector TSVECTOR,
                embedding TEXT
            )
            """

_CATALOG_TRIMS_DDL_SQLITE = """
        CREATE TABLE IF NOT EXISTS catalog_trims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            trim TEXT,
            trim_level TEXT,
            horsepower INTEGER,
            torque_lb_ft INTEGER,
            base_msrp REAL,
            source TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            search_vector TEXT,
            embedding TEXT
        )
        """


def migrate_catalog_trims_drop_epa_duplicate_columns(cur: Any, *, postgres: bool) -> list[str]:
    """
    Drop EPA-duplicated columns from an existing ``catalog_trims`` table.

    Idempotent (``DROP COLUMN IF EXISTS`` on Postgres; no-op skip on SQLite when missing).
    Returns column names actually dropped.
    """
    dropped: list[str] = []
    if not postgres:
        return dropped
    for col in CATALOG_TRIMS_EPA_DUPLICATE_COLUMNS:
        cur.execute(f"ALTER TABLE catalog_trims DROP COLUMN IF EXISTS {col}")
        # psycopg has no easy "did drop" flag; IF EXISTS is enough for idempotency
        dropped.append(col)
    return dropped


def ensure_catalog_tables(cur: Any, *, postgres: bool = False) -> None:
    """Create catalog_* tables idempotently; slim legacy catalog_trims when on Postgres."""
    if postgres:
        cur.execute(_CATALOG_TRIMS_DDL_POSTGRES)
        migrate_catalog_trims_drop_epa_duplicate_columns(cur, postgres=True)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_packages (
                id BIGSERIAL PRIMARY KEY,
                vehicle_id BIGINT NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
                package_code TEXT,
                package_name TEXT NOT NULL,
                package_msrp DOUBLE PRECISION,
                is_required BOOLEAN NOT NULL DEFAULT FALSE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                notes TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_package_features (
                id BIGSERIAL PRIMARY KEY,
                package_id BIGINT NOT NULL REFERENCES catalog_packages(id) ON DELETE CASCADE,
                feature_name TEXT NOT NULL,
                feature_category TEXT,
                sort_order INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_options (
                id BIGSERIAL PRIMARY KEY,
                vehicle_id BIGINT NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
                option_code TEXT,
                option_name TEXT NOT NULL,
                option_msrp DOUBLE PRECISION,
                category TEXT,
                description TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_exterior_colors (
                id BIGSERIAL PRIMARY KEY,
                vehicle_id BIGINT NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
                color_name TEXT NOT NULL,
                color_code TEXT,
                finish_type TEXT,
                hex_code TEXT,
                extra_cost DOUBLE PRECISION
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_interior_colors (
                id BIGSERIAL PRIMARY KEY,
                vehicle_id BIGINT NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
                color_name TEXT NOT NULL,
                color_code TEXT,
                material TEXT,
                hex_code TEXT,
                extra_cost DOUBLE PRECISION
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_catalog_trims_ymm "
            "ON catalog_trims(year, make, model)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_catalog_trims_search "
            "ON catalog_trims USING GIN (search_vector)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_catalog_packages_vehicle "
            "ON catalog_packages(vehicle_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_catalog_options_vehicle "
            "ON catalog_options(vehicle_id)"
        )
        return

    cur.execute(_CATALOG_TRIMS_DDL_SQLITE)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_packages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id INTEGER NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
            package_code TEXT,
            package_name TEXT NOT NULL,
            package_msrp REAL,
            is_required INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_package_features (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER NOT NULL REFERENCES catalog_packages(id) ON DELETE CASCADE,
            feature_name TEXT NOT NULL,
            feature_category TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id INTEGER NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
            option_code TEXT,
            option_name TEXT NOT NULL,
            option_msrp REAL,
            category TEXT,
            description TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_exterior_colors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id INTEGER NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
            color_name TEXT NOT NULL,
            color_code TEXT,
            finish_type TEXT,
            hex_code TEXT,
            extra_cost REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_interior_colors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id INTEGER NOT NULL REFERENCES catalog_trims(id) ON DELETE CASCADE,
            color_name TEXT NOT NULL,
            color_code TEXT,
            material TEXT,
            hex_code TEXT,
            extra_cost REAL
        )
        """
    )
