"""
PostgreSQL inventory backend when DATABASE_URL / INVENTORY_DATABASE_URL points at Postgres.

Scanner + Flask share this connection via :func:`backend.db.inventory_db.get_conn`.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

_log = logging.getLogger(__name__)

_PG_INV_SCHEMA_OK = False


def reset_postgres_inventory_schema_cache() -> None:
    """Tests / tooling: next ``init_postgres_inventory`` runs full DDL again."""
    global _PG_INV_SCHEMA_OK
    _PG_INV_SCHEMA_OK = False


def inventory_postgres_dsn() -> str | None:
    raw = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        return None
    if raw.startswith(("postgresql://", "postgres://")):
        return raw
    return None


def is_inventory_postgres() -> bool:
    return inventory_postgres_dsn() is not None


def inventory_sqlite_tests_allowed() -> bool:
    """Pytest-only escape hatch; never set in production or local dev."""
    return (os.environ.get("INVENTORY_SQLITE_TESTS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


_INVENTORY_POSTGRES_REQUIRED_MSG = (
    "INVENTORY_DATABASE_URL must be set to a postgresql:// or postgres:// URL. "
    "SQLite inventory is disabled. For local dev run: WEB_PORT=8000 ./deploy/up.sh "
    "(Postgres + web + scanner workers)."
)


def assert_inventory_backend_configured() -> None:
    """Fail fast when inventory would fall back to SQLite (SEC-102)."""
    if is_inventory_postgres() or inventory_sqlite_tests_allowed():
        return
    raise RuntimeError(_INVENTORY_POSTGRES_REQUIRED_MSG)


def pg_connect():
    import psycopg

    return psycopg.connect(inventory_postgres_dsn() or "", autocommit=False)


def qmarks_to_percent_s(sql: str) -> str:
    """Convert SQLite ``?`` placeholders to psycopg ``%s`` (quote-aware).

    Also escapes bare ``%`` inside single-quoted string literals to ``%%``
    so psycopg3 does not misinterpret LIKE patterns such as ``LIKE 'http%'``
    as invalid format-string placeholders.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    while i < n:
        c = sql[i]
        if not in_single:
            if c == "'":
                in_single = True
                out.append(c)
                i += 1
                continue
            if c == "?":
                out.append("%s")
                i += 1
                continue
            out.append(c)
            i += 1
            continue
        # Inside single-quoted string literal
        if c == "'":
            if i + 1 < n and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_single = False
            out.append(c)
            i += 1
            continue
        if c == "%":
            # Escape % inside string literals: psycopg3 would otherwise
            # interpret e.g. LIKE 'http%' as an invalid placeholder.
            out.append("%%")
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def adapt_sqlite_functions_to_pg(sql: str) -> str:
    s = sql
    s = re.sub(r"\bIFNULL\s*\(", "COALESCE(", s, flags=re.IGNORECASE)
    # SQLite UPSERT pseudo-table vs PostgreSQL EXCLUDED
    s = re.sub(r"\bexcluded\.", "EXCLUDED.", s, flags=re.IGNORECASE)
    s = re.sub(r"\bdatetime\s*\(\s*['\"]now['\"]\s*\)", "CURRENT_TIMESTAMP", s, flags=re.IGNORECASE)
    s = re.sub(r"\bINSTR\s*\(", "strpos(", s, flags=re.IGNORECASE)
    # SQLite ORDER BY datetime(col) — ISO text sorts lexicographically; drop wrapper for PG compatibility.
    s = re.sub(r"\bdatetime\s*\(\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\)", r"\1", s)
    return s


def adapt_insert_or_ignore_pg(sql: str) -> str | None:
    """Map SQLite ``INSERT OR IGNORE`` to Postgres ``ON CONFLICT … DO NOTHING``."""
    m = re.match(
        r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*$",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    table = m.group(1)
    cols = [c.strip() for c in m.group(2).split(",")]
    vals = m.group(3).strip()
    if table.lower() == "saved_cars" and "user_id" in cols and "car_id" in cols:
        return (
            f"INSERT INTO saved_cars ({', '.join(cols)}) VALUES ({vals}) "
            f"ON CONFLICT (user_id, car_id) DO NOTHING"
        )
    if table.lower() == "cars" and "vin" in cols:
        return f"INSERT INTO cars ({', '.join(cols)}) VALUES ({vals}) ON CONFLICT (vin) DO NOTHING"
    return None


def adapt_insert_or_replace_pg(sql: str) -> str | None:
    """Map SQLite ``INSERT OR REPLACE`` to PostgreSQL ``ON CONFLICT … DO UPDATE``."""
    m = re.match(
        r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*$",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    table = m.group(1).lower()
    cols = [c.strip() for c in m.group(2).split(",")]
    vals = m.group(3).strip()
    if table == "incomplete_listings_meta" and cols == ["k", "v"]:
        return (
            f"INSERT INTO incomplete_listings_meta (k, v) VALUES ({vals}) "
            "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
        )
    if table == "dealer_geopoints" and "dealer_url" in cols:
        update_cols = [c for c in cols if c != "dealer_url"]
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        return (
            f"INSERT INTO dealer_geopoints ({', '.join(cols)}) VALUES ({vals}) "
            f"ON CONFLICT (dealer_url) DO UPDATE SET {set_clause}"
        )
    return None


def adapt_sql_for_postgres_execute(sql: str) -> str | None:
    """
    Return SQL suitable for psycopg3, or None if the statement should be skipped (no-op).
    """
    stripped = sql.strip()
    if stripped.upper().startswith("PRAGMA"):
        return None
    low = stripped.upper()
    if low.startswith("INSERT OR REPLACE"):
        alt = adapt_insert_or_replace_pg(sql)
        if alt:
            sql = alt
        else:
            _log.warning("INSERT OR REPLACE without Postgres mapping; statement may fail: %s", stripped[:200])
    elif low.startswith("INSERT OR IGNORE"):
        alt = adapt_insert_or_ignore_pg(sql)
        if alt:
            sql = alt
        else:
            sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, count=1, flags=re.IGNORECASE)
            sql += " ON CONFLICT DO NOTHING"
    sql = adapt_sqlite_functions_to_pg(sql)
    sql = qmarks_to_percent_s(sql)
    return sql


def pg_table_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {str(r[0]) for r in cur.fetchall()}


def pg_add_columns(cur, table: str, additive: list[tuple[str, str]]) -> None:
    existing = pg_table_columns(cur, table)
    for col, typ in additive:
        if col not in existing:
            cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {typ}')


def init_postgres_inventory(conn: Any) -> None:
    """Create inventory tables and indexes on PostgreSQL (idempotent)."""
    global _PG_INV_SCHEMA_OK
    from backend.db.inventory_db import drop_kbb_columns_from_cars, drop_model_full_raw_column

    cur = conn.cursor()
    drop_kbb_columns_from_cars(cur, postgres=True)
    drop_model_full_raw_column(cur, postgres=True)
    conn.commit()
    if _PG_INV_SCHEMA_OK:
        cur.close()
        return
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cars (
            id BIGSERIAL PRIMARY KEY,
            vin TEXT NOT NULL UNIQUE,
            title TEXT,
            year INTEGER,
            make TEXT,
            model TEXT,
            trim TEXT,
            price DOUBLE PRECISION,
            mileage INTEGER,
            zip_code TEXT,
            fuel_type TEXT,
            cylinders INTEGER,
            transmission TEXT,
            drivetrain TEXT,
            exterior_color TEXT,
            interior_color TEXT,
            image_url TEXT,
            dealer_name TEXT,
            dealer_url TEXT,
            scraped_at TEXT,
            dealer_id TEXT,
            stock_number TEXT,
            gallery TEXT,
            carfax_url TEXT,
            history_highlights TEXT,
            msrp DOUBLE PRECISION,
            dealership_registry_id INTEGER,
            source_url TEXT,
            body_style TEXT,
            engine_description TEXT,
            transmission_type TEXT,
            condition TEXT,
            description TEXT,
            data_quality_score DOUBLE PRECISION,
            is_cpo INTEGER,
            mpg_city INTEGER,
            mpg_highway INTEGER,
            engine_l TEXT,
            recovery_status TEXT,
            recovery_attempted_at TEXT,
            recovery_source TEXT,
            recovery_notes TEXT,
            missing_field_count INTEGER,
            recoverability_score DOUBLE PRECISION,
            spec_source_json TEXT,
            packages TEXT,
            listing_active INTEGER DEFAULT 1,
            listing_removed_at TEXT,
            interior_color_buckets TEXT,
            first_seen_at TEXT,
            last_price_change_at TEXT,
            internal_notes TEXT,
            marked_for_review INTEGER,
            price_provenance_json TEXT
        )
        """
    )
    pg_add_columns(
        cur,
        "cars",
        [
            ("transmission_type", "TEXT"),
            ("recovery_status", "TEXT"),
            ("recovery_attempted_at", "TEXT"),
            ("recovery_source", "TEXT"),
            ("recovery_notes", "TEXT"),
            ("missing_field_count", "INTEGER"),
            ("recoverability_score", "DOUBLE PRECISION"),
            ("price_provenance_json", "TEXT"),
            ("window_sticker_url", "TEXT"),
        ],
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_cars_dealer_listing ON cars(dealer_id, listing_active)"
    )
    from backend.db.inventory_db import ensure_cars_listings_indexes

    ensure_cars_listings_indexes(cur)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS epa_master (
            id BIGSERIAL PRIMARY KEY,
            epa_vehicle_id INTEGER,
            year INTEGER,
            make TEXT,
            model TEXT,
            cylinders INTEGER,
            displacement DOUBLE PRECISION,
            trany TEXT,
            drive TEXT,
            fuel_type TEXT,
            city08 DOUBLE PRECISION,
            highway08 DOUBLE PRECISION,
            city_e DOUBLE PRECISION,
            highway_e DOUBLE PRECISION,
            atv_type TEXT
        )
        """
    )
    pg_add_columns(
        cur,
        "epa_master",
        [
            ("city08", "DOUBLE PRECISION"),
            ("highway08", "DOUBLE PRECISION"),
            ("city_e", "DOUBLE PRECISION"),
            ("highway_e", "DOUBLE PRECISION"),
            ("atv_type", "TEXT"),
        ],
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_epa_master_lookup ON epa_master(year, make, model)"
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS model_specs (
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            cylinders INTEGER,
            gears INTEGER,
            transmission TEXT,
            drivetrain TEXT,
            body_style TEXT,
            fuel_type TEXT,
            PRIMARY KEY (make, model)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_cars (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            car_id BIGINT NOT NULL,
            saved_at TEXT DEFAULT (CURRENT_TIMESTAMP::text),
            UNIQUE(user_id, car_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id BIGSERIAL PRIMARY KEY,
            dealer_id TEXT NOT NULL,
            dealer_name TEXT,
            finished_at TEXT NOT NULL,
            duration_seconds DOUBLE PRECISION,
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
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_scan_runs_dealer_time ON scan_runs(dealer_id, finished_at DESC)"
    )
    pg_add_columns(cur, "scan_runs", [("provider", "TEXT")])

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_scan_profile (
            dealer_id TEXT PRIMARY KEY,
            last_winning_strategy TEXT,
            platform_hints_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nhtsa_vpic_cache (
            vin TEXT PRIMARY KEY NOT NULL,
            response_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dealer_geopoints (
            dealer_url TEXT PRIMARY KEY,
            dealer_name TEXT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            zip_code TEXT,
            city TEXT,
            state TEXT,
            geocode_source TEXT,
            geocoded_at TEXT DEFAULT (CURRENT_TIMESTAMP::text)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dgp_dealer_url ON dealer_geopoints(dealer_url)"
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dealerships (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            website_url TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP::text),
            duplicate_of_id BIGINT REFERENCES dealerships(id),
            duplicate_score DOUBLE PRECISION,
            is_active INTEGER NOT NULL DEFAULT 1,
            street_address TEXT,
            zip_code TEXT,
            dealer_website_url TEXT,
            source_dmv INTEGER NOT NULL DEFAULT 0,
            source_osm INTEGER NOT NULL DEFAULT 0,
            source_web INTEGER NOT NULL DEFAULT 0,
            osm_id TEXT
        )
        """
    )
    pg_add_columns(
        cur,
        "dealerships",
        [
            ("is_active", "INTEGER NOT NULL DEFAULT 1"),
            ("street_address", "TEXT"),
            ("zip_code", "TEXT"),
            ("dealer_website_url", "TEXT"),
            ("source_dmv", "INTEGER NOT NULL DEFAULT 0"),
            ("source_osm", "INTEGER NOT NULL DEFAULT 0"),
            ("source_web", "INTEGER NOT NULL DEFAULT 0"),
            ("osm_id", "TEXT"),
            ("sticker_provider", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("sticker_ipacket_fail_count", "INTEGER NOT NULL DEFAULT 0"),
            ("sticker_provider_updated_at", "TEXT"),
            ("google_place_id", "TEXT"),
            ("google_rating", "DOUBLE PRECISION"),
            ("google_review_count", "INTEGER"),
            ("google_rating_fetched_at", "TEXT"),
        ],
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealerships_created ON dealerships(created_at DESC)"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dealerships_zip ON dealerships(zip_code)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dealerships_google_place ON dealerships(google_place_id)"
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS incomplete_listings (
            car_id BIGINT PRIMARY KEY NOT NULL,
            vin TEXT NOT NULL,
            missing_fields_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS incomplete_listings_meta (
            k TEXT PRIMARY KEY NOT NULL,
            v TEXT NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_incomplete_listings_updated ON incomplete_listings(updated_at DESC)"
    )

    conn.commit()
    cur.close()
    _PG_INV_SCHEMA_OK = True
    _log.info("PostgreSQL inventory schema ensured.")
