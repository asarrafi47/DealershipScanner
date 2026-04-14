from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from oem_intake.models import NormalizedDealer
from oem_intake.paths import BMW_DB_PATH, ensure_bmw_dirs


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    ensure_bmw_dirs()
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


def insert_raw_intake(
    conn: sqlite3.Connection,
    *,
    scraped_at: str,
    source_locator_url: str,
    intake_method: str,
    fingerprint: str,
    raw_payload: dict[str, Any],
    extracted_fields: dict[str, Any],
) -> int:
    conn.execute(
        """
        INSERT INTO bmw_raw_intake
        (scraped_at, source_locator_url, intake_method, fingerprint, raw_payload_json, extracted_fields_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            scraped_at,
            source_locator_url,
            intake_method,
            fingerprint,
            json.dumps(raw_payload, ensure_ascii=False),
            json.dumps(extracted_fields, ensure_ascii=False),
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def delete_all_normalized(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM bmw_normalized_dealer")
    conn.commit()


def clear_partial_staging(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM bmw_partial_staging")
    conn.commit()


def load_all_raw_extracted(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, scraped_at, extracted_fields_json FROM bmw_raw_intake ORDER BY id"
    ).fetchall()


def upsert_normalized(
    conn: sqlite3.Connection,
    d: NormalizedDealer,
    raw_ids: list[int],
) -> tuple[int, bool]:
    """Insert or merge normalized dealer. Returns (id, merged_duplicate)."""
    cur = conn.execute(
        "SELECT id, merged_raw_intake_ids FROM bmw_normalized_dealer WHERE dedupe_key = ?",
        (d.dedupe_key,),
    )
    row = cur.fetchone()
    ids_json = json.dumps(sorted(set(raw_ids)), ensure_ascii=False)

    if row:
        existing_ids = set(json.loads(row["merged_raw_intake_ids"] or "[]"))
        existing_ids.update(raw_ids)
        merged = json.dumps(sorted(existing_ids), ensure_ascii=False)
        conn.execute(
            """
            UPDATE bmw_normalized_dealer SET
                dealer_name = COALESCE(NULLIF(?, ''), dealer_name),
                phone = CASE WHEN length(?) > length(phone) THEN ? ELSE phone END,
                root_website = CASE WHEN length(?) > length(COALESCE(root_website,'')) THEN ? ELSE root_website END,
                normalized_root_domain = CASE WHEN length(?) > length(COALESCE(normalized_root_domain,'')) THEN ? ELSE normalized_root_domain END,
                map_reference_url = CASE WHEN length(?) > length(COALESCE(map_reference_url,'')) THEN ? ELSE map_reference_url END,
                row_quality = CASE
                    WHEN ? = 'usable' THEN 'usable'
                    WHEN row_quality = 'usable' THEN row_quality
                    WHEN ? = 'partial' THEN 'partial'
                    ELSE COALESCE(row_quality, ?)
                END,
                row_rejection_reasons_json = ?,
                enrichment_ready = CASE WHEN ? = 1 THEN 1 ELSE COALESCE(enrichment_ready, 0) END,
                merged_raw_intake_ids = ?,
                last_verified_at = ?
            WHERE id = ?
            """,
            (
                d.dealer_name,
                d.phone,
                d.phone,
                d.root_website,
                d.root_website,
                d.normalized_root_domain,
                d.normalized_root_domain,
                d.map_reference_url,
                d.map_reference_url,
                d.row_quality,
                d.row_quality,
                d.row_quality,
                json.dumps(d.row_rejection_reasons, ensure_ascii=False),
                1 if d.enrichment_ready else 0,
                merged,
                d.last_verified_at,
                row["id"],
            ),
        )
        conn.commit()
        return int(row["id"]), True

    conn.execute(
        """
        INSERT INTO bmw_normalized_dealer (
            dealer_name, normalized_dealer_name, brand, street, city, state, zip,
            latitude, longitude, phone, root_website, normalized_root_domain,
            map_reference_url, new_inventory_url, used_inventory_url, dealer_group_canonical, confidence_score,
            row_quality, row_rejection_reasons_json, enrichment_ready,
            source_oem, source_locator_url, last_verified_at, dedupe_key, merged_raw_intake_ids
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            d.dealer_name,
            d.normalized_dealer_name,
            d.brand,
            d.street,
            d.city,
            d.state,
            d.zip,
            d.latitude,
            d.longitude,
            d.phone,
            d.root_website,
            d.normalized_root_domain,
            d.map_reference_url,
            d.new_inventory_url,
            d.used_inventory_url,
            d.dealer_group_canonical,
            d.confidence_score,
            d.row_quality,
            json.dumps(d.row_rejection_reasons, ensure_ascii=False),
            1 if d.enrichment_ready else 0,
            d.source_oem,
            d.source_locator_url,
            d.last_verified_at,
            d.dedupe_key,
            ids_json,
        ),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0]), False


def upsert_partial_staging(conn: sqlite3.Connection, raw_id: int, d: NormalizedDealer) -> None:
    key = d.partial_group_key or d.dedupe_key
    existing = conn.execute(
        "SELECT merged_raw_intake_ids_json FROM bmw_partial_staging WHERE partial_group_key = ?",
        (key,),
    ).fetchone()
    merged_ids: set[int] = {raw_id}
    if existing:
        try:
            merged_ids.update(json.loads(existing["merged_raw_intake_ids_json"] or "[]"))
        except Exception:
            pass
    conn.execute(
        """
        INSERT INTO bmw_partial_staging (
            partial_group_key, merged_raw_intake_ids_json, dealer_name, normalized_dealer_name, brand, street, city, state, zip,
            phone, root_website, map_reference_url, dedupe_key, row_quality, row_rejection_reasons_json,
            source_of_each_field_json, zip_seed_hint,
            source_locator_url, last_verified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(partial_group_key) DO UPDATE SET
            merged_raw_intake_ids_json = excluded.merged_raw_intake_ids_json,
            dealer_name = CASE WHEN length(excluded.dealer_name) > length(COALESCE(dealer_name,'')) THEN excluded.dealer_name ELSE dealer_name END,
            normalized_dealer_name = CASE WHEN length(excluded.normalized_dealer_name) > length(COALESCE(normalized_dealer_name,'')) THEN excluded.normalized_dealer_name ELSE normalized_dealer_name END,
            brand = COALESCE(excluded.brand, brand),
            street = CASE WHEN length(excluded.street) > length(COALESCE(street,'')) THEN excluded.street ELSE street END,
            city = CASE WHEN length(excluded.city) > length(COALESCE(city,'')) THEN excluded.city ELSE city END,
            state = CASE WHEN length(excluded.state) > length(COALESCE(state,'')) THEN excluded.state ELSE state END,
            zip = CASE WHEN length(excluded.zip) > length(COALESCE(zip,'')) THEN excluded.zip ELSE zip END,
            phone = CASE WHEN length(excluded.phone) > length(COALESCE(phone,'')) THEN excluded.phone ELSE phone END,
            root_website = CASE WHEN length(excluded.root_website) > length(COALESCE(root_website,'')) THEN excluded.root_website ELSE root_website END,
            map_reference_url = CASE WHEN length(excluded.map_reference_url) > length(COALESCE(map_reference_url,'')) THEN excluded.map_reference_url ELSE map_reference_url END,
            dedupe_key = excluded.dedupe_key,
            row_quality = excluded.row_quality,
            row_rejection_reasons_json = excluded.row_rejection_reasons_json,
            source_of_each_field_json = COALESCE(excluded.source_of_each_field_json, source_of_each_field_json),
            zip_seed_hint = CASE WHEN length(excluded.zip_seed_hint) > length(COALESCE(zip_seed_hint,'')) THEN excluded.zip_seed_hint ELSE zip_seed_hint END,
            source_locator_url = COALESCE(excluded.source_locator_url, source_locator_url),
            last_verified_at = excluded.last_verified_at
        """,
        (
            key,
            json.dumps(sorted(merged_ids), ensure_ascii=False),
            d.dealer_name,
            d.normalized_dealer_name,
            d.brand,
            d.street,
            d.city,
            d.state,
            d.zip,
            d.phone,
            d.root_website,
            d.map_reference_url,
            d.dedupe_key,
            d.row_quality,
            json.dumps(d.row_rejection_reasons, ensure_ascii=False),
            json.dumps(d.extra.get("source_of_each_field") or {}, ensure_ascii=False),
            str(d.extra.get("zip_seed_hint") or ""),
            d.source_locator_url,
            d.last_verified_at,
        ),
    )
    conn.commit()


def list_partial_staging(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM bmw_partial_staging ORDER BY partial_group_key"
    ).fetchall()
    return [dict(r) for r in rows]


def list_normalized_for_enrichment(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM bmw_normalized_dealer
        WHERE TRIM(COALESCE(root_website,'')) != '' AND COALESCE(enrichment_ready,0) = 1
        ORDER BY id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def update_enrichment_fields(
    conn: sqlite3.Connection,
    dealer_id: int,
    *,
    canonical: str | None,
    confidence: float | None,
    status: str,
    run_id: str,
) -> None:
    conn.execute(
        """
        UPDATE bmw_normalized_dealer SET
            dealer_group_canonical = COALESCE(?, dealer_group_canonical),
            confidence_score = COALESCE(?, confidence_score),
            enrichment_status = ?,
            enrichment_run_id = ?
        WHERE id = ?
        """,
        (canonical, confidence, status, run_id, dealer_id),
    )
    conn.commit()


def count_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    n_raw = conn.execute("SELECT COUNT(*) FROM bmw_raw_intake").fetchone()[0]
    n_norm = conn.execute("SELECT COUNT(*) FROM bmw_normalized_dealer").fetchone()[0]
    n_no_web = conn.execute(
        "SELECT COUNT(*) FROM bmw_normalized_dealer WHERE TRIM(COALESCE(root_website,'')) = ''"
    ).fetchone()[0]
    n_bad_addr = conn.execute(
        """
        SELECT COUNT(*) FROM bmw_normalized_dealer WHERE
            TRIM(COALESCE(street,'')) = '' OR TRIM(COALESCE(city,'')) = ''
            OR TRIM(COALESCE(state,'')) = '' OR TRIM(COALESCE(zip,'')) = ''
        """
    ).fetchone()[0]
    n_usable = conn.execute(
        "SELECT COUNT(*) FROM bmw_normalized_dealer WHERE row_quality = 'usable'"
    ).fetchone()[0]
    n_partial = conn.execute(
        "SELECT COUNT(*) FROM bmw_normalized_dealer WHERE row_quality = 'partial'"
    ).fetchone()[0]
    n_insufficient = conn.execute(
        "SELECT COUNT(*) FROM bmw_normalized_dealer WHERE row_quality = 'insufficient'"
    ).fetchone()[0]
    n_ready = conn.execute(
        "SELECT COUNT(*) FROM bmw_normalized_dealer WHERE COALESCE(enrichment_ready,0) = 1"
    ).fetchone()[0]
    n_partial_staging = conn.execute(
        "SELECT COUNT(*) FROM bmw_partial_staging"
    ).fetchone()[0]
    return {
        "bmw_raw_records": n_raw,
        "bmw_normalized_dealers": n_norm,
        "bmw_missing_website": n_no_web,
        "bmw_missing_address_fields": n_bad_addr,
        "bmw_row_quality_usable": n_usable,
        "bmw_row_quality_partial": n_partial + n_partial_staging,
        "bmw_row_quality_insufficient": n_insufficient,
        "bmw_enrichment_ready": n_ready,
        "bmw_partial_staging_rows": n_partial_staging,
    }
