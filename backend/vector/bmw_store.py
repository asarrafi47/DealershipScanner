"""Read side of the BMW dealer-intake SQLite store, used by pgvector_service.

The intake pipeline that wrote this database (backend/oem/intake, backend/oem/scraper)
was deleted on 2026-10-01 (monolith audit, enrich.md #12): its BMW locator fetch timed
out, its Playwright fallback and enrichment used imports that no longer resolved, and
its deep locator imported a package that never existed. The pgvector reindex still
embeds an existing ``data/oem/bmw/bmw_intake.db`` when one is present, so the path,
connection helper and schema live here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.scraping.paths import ROOT

BMW_DB_PATH = ROOT / "data" / "oem" / "bmw" / "bmw_intake.db"


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or BMW_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS bmw_raw_intake (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT NOT NULL,
            source_locator_url TEXT NOT NULL,
            intake_method TEXT NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE,
            raw_payload_json TEXT NOT NULL,
            extracted_fields_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bmw_normalized_dealer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dealer_name TEXT NOT NULL,
            normalized_dealer_name TEXT NOT NULL,
            brand TEXT NOT NULL,
            street TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            latitude REAL,
            longitude REAL,
            phone TEXT,
            root_website TEXT,
            normalized_root_domain TEXT,
            map_reference_url TEXT,
            new_inventory_url TEXT,
            used_inventory_url TEXT,
            dealer_group_canonical TEXT,
            confidence_score REAL,
            row_quality TEXT,
            row_rejection_reasons_json TEXT,
            enrichment_ready INTEGER NOT NULL DEFAULT 0,
            source_oem TEXT NOT NULL,
            source_locator_url TEXT,
            last_verified_at TEXT,
            dedupe_key TEXT NOT NULL UNIQUE,
            merged_raw_intake_ids TEXT NOT NULL,
            enrichment_status TEXT,
            enrichment_run_id TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_bmw_norm_domain ON bmw_normalized_dealer(normalized_root_domain);
        CREATE INDEX IF NOT EXISTS idx_bmw_norm_zip ON bmw_normalized_dealer(zip);

        CREATE TABLE IF NOT EXISTS bmw_partial_staging (
            partial_group_key TEXT PRIMARY KEY,
            merged_raw_intake_ids_json TEXT NOT NULL DEFAULT '[]',
            dealer_name TEXT,
            normalized_dealer_name TEXT,
            brand TEXT,
            street TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            phone TEXT,
            root_website TEXT,
            map_reference_url TEXT,
            dedupe_key TEXT,
            row_quality TEXT,
            row_rejection_reasons_json TEXT,
            source_of_each_field_json TEXT,
            zip_seed_hint TEXT,
            source_locator_url TEXT,
            last_verified_at TEXT
        );
        """
    )
    _ensure_column(
        conn, "bmw_partial_staging", "partial_group_key", "TEXT"
    )
    _ensure_column(
        conn, "bmw_partial_staging", "merged_raw_intake_ids_json", "TEXT NOT NULL DEFAULT '[]'"
    )
    _ensure_column(
        conn, "bmw_partial_staging", "source_of_each_field_json", "TEXT"
    )
    _ensure_column(conn, "bmw_partial_staging", "zip_seed_hint", "TEXT")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bmw_partial_group_key ON bmw_partial_staging(partial_group_key)"
    )
    _ensure_column(conn, "bmw_normalized_dealer", "map_reference_url", "TEXT")
    _ensure_column(conn, "bmw_normalized_dealer", "row_quality", "TEXT")
    _ensure_column(conn, "bmw_normalized_dealer", "row_rejection_reasons_json", "TEXT")
    _ensure_column(
        conn, "bmw_normalized_dealer", "enrichment_ready", "INTEGER NOT NULL DEFAULT 0"
    )
    conn.commit()


def _ensure_column(
    conn: sqlite3.Connection, table: str, column: str, column_sql_type: str
) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_sql_type}")
