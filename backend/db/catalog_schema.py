"""Normalized vehicle catalog tables (trim spine + packages/options/colors)."""

from __future__ import annotations

from typing import Any


def ensure_catalog_tables(cur: Any, *, postgres: bool = False) -> None:
    """Create catalog_* tables idempotently."""
    if postgres:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_trims (
                id BIGSERIAL PRIMARY KEY,
                year INTEGER NOT NULL,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                trim TEXT,
                body_style TEXT,
                trim_level TEXT,
                engine_l TEXT,
                engine_desc TEXT,
                cylinders INTEGER,
                horsepower INTEGER,
                torque_lb_ft INTEGER,
                fuel_type TEXT,
                forced_induction TEXT,
                transmission TEXT,
                trans_speeds INTEGER,
                drivetrain TEXT,
                mpg_city INTEGER,
                mpg_highway INTEGER,
                mpg_combined INTEGER,
                range_miles INTEGER,
                base_msrp DOUBLE PRECISION,
                source TEXT,
                notes TEXT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                search_vector TSVECTOR,
                embedding TEXT
            )
            """
        )
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog_trims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            trim TEXT,
            body_style TEXT,
            trim_level TEXT,
            engine_l TEXT,
            engine_desc TEXT,
            cylinders INTEGER,
            horsepower INTEGER,
            torque_lb_ft INTEGER,
            fuel_type TEXT,
            forced_induction TEXT,
            transmission TEXT,
            trans_speeds INTEGER,
            drivetrain TEXT,
            mpg_city INTEGER,
            mpg_highway INTEGER,
            mpg_combined INTEGER,
            range_miles INTEGER,
            base_msrp REAL,
            source TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            search_vector TEXT,
            embedding TEXT
        )
        """
    )
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
