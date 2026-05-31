import json
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator
from urllib.parse import urlparse

from backend.db.inventory_pg import is_inventory_postgres
from backend.utils.car_serialize import serialize_car_for_listings_grid as _serialize_car_for_listings_grid

# Default SQLite location for the public scanned inventory. Prefer ``backend/inventory.db``
# when that file exists (common dev layout next to ``backend/incomplete_listings.db``); otherwise
# ``<repo>/inventory.db``. Always set ``INVENTORY_DB_PATH`` in production if ambiguous.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))


def _default_inventory_db_path() -> str:
    backend_p = os.path.join(_REPO_ROOT, "backend", "inventory.db")
    root_p = os.path.join(_REPO_ROOT, "inventory.db")
    try:
        if os.path.isfile(backend_p):
            import sqlite3 as _sqlite3
            _conn = _sqlite3.connect(backend_p)
            _has_cars = bool(_conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cars'"
            ).fetchone())
            _conn.close()
            if _has_cars:
                return backend_p
    except OSError:
        pass
    return root_p

from backend.utils.car_serialize import car_matches_engine_displacement_l_range, serialize_car_for_api
from backend.utils.field_clean import (
    coerce_body_style_stored,
    coerce_fuel_type_stored,
    compute_data_quality_score,
    is_effectively_empty,
    sort_body_style_presets,
    sort_fuel_type_presets,
)
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    row_matches_interior_bucket_filter,
    sort_paint_family_ids,
)

DB_PATH = os.environ.get("INVENTORY_DB_PATH", _default_inventory_db_path())
_log = logging.getLogger(__name__)


def _inventory_sqlite_lock_wait_sec() -> float:
    """Connect ``timeout=`` and basis for ``busy_timeout``; default 60s, min 5s."""
    raw = (os.environ.get("INVENTORY_SQLITE_LOCK_TIMEOUT_SEC") or "60").strip() or "60"
    try:
        return max(5.0, float(raw.split()[0]))
    except (ValueError, IndexError):
        return 60.0


def is_dummy_placeholder_vin(vin: str | None) -> bool:
    """
    True for legacy dev / seed rows such as ``VIN001``, ``VINXXX``, ``VINXXXX`` (not real VINs).

    Matches: ``VIN`` + digits only with total length ``< 17``; ``VIN`` + ``X`` only; or any
    value containing the literal substring ``VINXXX`` (case-insensitive).
    """
    v = str(vin or "").strip().upper()
    if not v:
        return False
    if "VINXXX" in v:
        return True
    if len(v) >= 17:
        return False
    if not v.startswith("VIN"):
        return False
    suf = v[3:]
    if suf.isdigit():
        return True
    if suf and re.fullmatch(r"X+", suf):
        return True
    return False


def delete_cars_with_dummy_placeholder_vins() -> dict[str, Any]:
    """
    Delete ``cars`` rows whose VIN matches :func:`is_dummy_placeholder_vin`, remove matching
    ``incomplete_listings`` and ``saved_cars`` rows, and drop ``nhtsa_vpic_cache`` entries for those VINs.
    """
    from backend.db.incomplete_listings_db import delete_incomplete_record

    conn = get_conn()
    try:
        ensure_nhtsa_vpic_cache_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT id, vin FROM cars")
        rows: list[tuple[int, str]] = []
        for r in cur.fetchall():
            rid, rv = int(r[0]), str(r[1] or "")
            if is_dummy_placeholder_vin(rv):
                rows.append((rid, rv))
        if not rows:
            return {"deleted": 0, "vins": [], "nhtsa_cache_deleted": 0}
        ids = [r[0] for r in rows]
        vins = [r[1] for r in rows]
        n_cache = 0
        for cid in ids:
            try:
                delete_incomplete_record(cid)
            except Exception as exc:
                _log.debug("incomplete_listings delete car_id=%s: %s", cid, exc)
        for vin in vins:
            cur.execute("DELETE FROM nhtsa_vpic_cache WHERE UPPER(TRIM(vin)) = ?", (vin.upper().strip(),))
            n_cache += cur.rowcount
        ph = ",".join("?" * len(ids))
        try:
            cur.execute(f"DELETE FROM saved_cars WHERE car_id IN ({ph})", ids)
        except Exception:
            _log.debug("saved_cars delete for dummy VINs skipped (table missing?)")
        cur.execute(f"DELETE FROM cars WHERE id IN ({ph})", ids)
        conn.commit()
        return {"deleted": len(ids), "vins": vins, "nhtsa_cache_deleted": n_cache}
    finally:
        conn.close()


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
        ("kbb_fetched_at", "TEXT"),
        ("kbb_snapshot_json", "TEXT"),
        ("kbb_fair_purchase", "REAL"),
        ("kbb_range_low", "REAL"),
        ("kbb_range_high", "REAL"),
        ("kbb_private_party", "REAL"),
        ("kbb_trade_in", "REAL"),
        ("first_seen_at", "TEXT"),
        ("last_price_change_at", "TEXT"),
        ("internal_notes", "TEXT"),
        ("marked_for_review", "INTEGER"),
        ("price_provenance_json", "TEXT"),
        ("forced_induction", "TEXT"),
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
    "zip_code",
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
    "kbb_fetched_at",
    "kbb_fair_purchase",
    "kbb_range_low",
    "kbb_range_high",
    "kbb_private_party",
    "kbb_trade_in",
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
        f"CREATE INDEX IF NOT EXISTS idx_cars_active_zip "
        f"ON cars(zip_code) WHERE {active} AND zip_code IS NOT NULL",
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
            summary_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_scan_runs_dealer_time ON scan_runs(dealer_id, finished_at DESC)"
    )


def record_scan_outcomes(outcomes: list[Any], *, finished_at: str) -> int:
    """
    Persist per-dealer scanner ``run_dealer`` result dicts into ``scan_runs``.

    Safe to call with empty list; ignores non-dict entries. Returns rows inserted.
    """
    if not outcomes:
        return 0
    n = 0
    with db_conn() as conn:
        cur = conn.cursor()
        ensure_scan_runs_table(cur)
        for o in outcomes:
            if not isinstance(o, dict):
                continue
            did = str(o.get("dealer_id") or "").strip()
            if not did:
                continue
            summary = {
                "inventory_rows": o.get("inventory_rows"),
                "deduped_rows": o.get("deduped_rows"),
                "vdps_visited": o.get("vdps_visited"),
                "vehicles_vdp_enriched": o.get("vehicles_vdp_enriched"),
                "gallery_vdp_urls_added": o.get("gallery_vdp_urls_added"),
                "gallery_vision": o.get("gallery_vision"),
                "monroney_vision": o.get("monroney_vision"),
                "reconcile": o.get("reconcile"),
                "phase_secs": o.get("phase_secs"),
                "vins_count": len(o.get("vins") or []) if isinstance(o.get("vins"), list) else None,
            }
            err = o.get("error")
            err_s = str(err)[:2000] if err else None
            cur.execute(
                """
                INSERT INTO scan_runs (
                    dealer_id, dealer_name, finished_at, duration_seconds,
                    upserted, inventory_rows, deduped_rows, vdps_visited,
                    vehicles_vdp_enriched, error, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    did,
                    (o.get("dealer_name") or "")[:500] or None,
                    finished_at,
                    float(o.get("seconds") or 0.0),
                    int(o.get("upserted") or 0),
                    int(o.get("inventory_rows") or 0),
                    int(o.get("deduped_rows") or 0),
                    int(o.get("vdps_visited") or 0),
                    int(o.get("vehicles_vdp_enriched") or 0),
                    err_s,
                    json.dumps(summary, ensure_ascii=False, default=str),
                ),
            )
            n += 1
        conn.commit()
    return n


def list_scan_runs(*, dealer_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Recent scan rows (newest first), optionally filtered by ``dealer_id``."""
    lim = max(1, min(200, int(limit)))
    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        ensure_scan_runs_table(cur)
        if dealer_id and str(dealer_id).strip():
            cur.execute(
                f"""
                SELECT * FROM scan_runs
                WHERE dealer_id = ?
                ORDER BY datetime(finished_at) DESC, id DESC
                LIMIT ?
                """,
                (str(dealer_id).strip(), lim),
            )
        else:
            cur.execute(
                f"""
                SELECT * FROM scan_runs
                ORDER BY datetime(finished_at) DESC, id DESC
                LIMIT ?
                """,
                (lim,),
            )
        rows = [dict(r) for r in cur.fetchall()]
    return rows


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


def _parse_car_gallery(car_dict):
    """Ensure car_dict['gallery'] is a list (parse from JSON string if needed)."""
    if not car_dict:
        return
    g = car_dict.get("gallery")
    if isinstance(g, list):
        return
    if g is None or g == "":
        car_dict["gallery"] = []
        return
    try:
        car_dict["gallery"] = json.loads(g) if isinstance(g, str) else []
    except (TypeError, ValueError):
        car_dict["gallery"] = []


def _parse_car_history_highlights(car_dict):
    """Ensure car_dict['history_highlights'] is a list (parse from JSON string if needed)."""
    if not car_dict:
        return
    h = car_dict.get("history_highlights")
    if isinstance(h, list):
        return
    if h is None or h == "":
        car_dict["history_highlights"] = []
        return
    try:
        car_dict["history_highlights"] = json.loads(h) if isinstance(h, str) else []
    except (TypeError, ValueError):
        car_dict["history_highlights"] = []


# Major automakers by country of origin (for country filter)
MAKE_TO_COUNTRY = {
    "BMW": "Germany", "Mercedes-Benz": "Germany", "Audi": "Germany",
    "Porsche": "Germany", "Volkswagen": "Germany", "VW": "Germany",
    "Toyota": "Japan", "Honda": "Japan", "Nissan": "Japan", "Lexus": "Japan",
    "Mazda": "Japan", "Subaru": "Japan", "Mitsubishi": "Japan",
    "Acura": "Japan", "Infiniti": "Japan",
    "Ford": "USA", "Chevrolet": "USA", "GM": "USA", "Ram": "USA",
    "Tesla": "USA", "Jeep": "USA", "Dodge": "USA", "Cadillac": "USA",
    "Buick": "USA", "GMC": "USA", "Chrysler": "USA", "Lincoln": "USA",
    "Hyundai": "South Korea", "Kia": "South Korea", "Genesis": "South Korea",
    "Jaguar": "UK", "Land Rover": "UK", "Bentley": "UK", "Mini": "UK",
    "Ferrari": "Italy", "Lamborghini": "Italy", "Fiat": "Italy", "Maserati": "Italy",
    "Renault": "France", "Peugeot": "France", "Citroën": "France",
    "Volvo": "Sweden", "Alfa Romeo": "Italy",
}


def _sqlite_connect_raw() -> sqlite3.Connection:
    """SQLite inventory connection (WAL + busy_timeout); used only when not on PostgreSQL."""
    lock_s = _inventory_sqlite_lock_wait_sec()
    conn = sqlite3.connect(DB_PATH, timeout=lock_s)
    try:
        conn.execute("PRAGMA busy_timeout=?", (int(max(5000, round(lock_s * 1000))),))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def get_conn():
    """
    Inventory DB connection: PostgreSQL (``DATABASE_URL`` / ``INVENTORY_DATABASE_URL``) via
    psycopg3 when configured; otherwise SQLite with WAL and extended lock wait.
    """
    from backend.db.inventory_compat import open_inventory_connection

    return open_inventory_connection()


@contextmanager
def db_conn(*, row_factory: Any = None) -> Iterator[Any]:
    """
    Open an inventory connection and always close it (avoids leaks on error paths).
    When *row_factory* is set, assign ``conn.row_factory = row_factory`` before *yield*.
    """
    conn = get_conn()
    if row_factory is not None:
        conn.row_factory = row_factory
    try:
        yield conn
    finally:
        conn.close()


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
            zip_code         TEXT,
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
            (vin, title, year, make, model, trim, price, mileage, zip_code,
             fuel_type, cylinders, transmission, drivetrain,
             exterior_color, interior_color, image_url, dealer_name, dealer_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        SEED_DATA,
    )
    conn.commit()
    conn.close()


def _placeholders(lst):
    return ", ".join("?" * len(lst))


def _makes_for_countries(countries):
    """Return set of makes whose country of origin is in the given list."""
    if not countries:
        return None
    countries_set = set(c.strip() for c in countries if c and c.strip())
    return {make for make, country in MAKE_TO_COUNTRY.items() if country in countries_set}


def _lookup_make_country(make: str):
    """Resolve country for a DB make string; case-insensitive vs MAKE_TO_COUNTRY keys."""
    if make is None:
        return None
    m = str(make).strip()
    if not m:
        return None
    if m in MAKE_TO_COUNTRY:
        return MAKE_TO_COUNTRY[m]
    ml = m.lower()
    for k, v in MAKE_TO_COUNTRY.items():
        if k.lower() == ml:
            return v
    return None


_incomplete_listings_check_fn = None
_incomplete_car_id_set_cache: tuple[float, set[int]] | None = None


def _incomplete_car_ids_for_listings() -> set[int]:
    """Cached incomplete car ids for grid filter + ``public_incomplete`` pill (O(1) per row)."""
    global _incomplete_car_id_set_cache
    token = _inventory_listings_cache_token()
    if _incomplete_car_id_set_cache is not None and _incomplete_car_id_set_cache[0] == token:
        return _incomplete_car_id_set_cache[1]
    try:
        from backend.db.incomplete_listings_db import get_incomplete_car_id_set

        ids = get_incomplete_car_id_set()
    except Exception:
        ids = set()
    _incomplete_car_id_set_cache = (token, ids)
    return ids


def is_car_incomplete(car: dict) -> bool:
    """True when the row should be hidden from public listings (subset of spec sheet)."""
    global _incomplete_listings_check_fn
    if _incomplete_listings_check_fn is None:
        from backend.utils.listing_completeness import is_car_incomplete_for_public_listings

        _incomplete_listings_check_fn = is_car_incomplete_for_public_listings
    return _incomplete_listings_check_fn(car)


def listings_include_incomplete_cars() -> bool:
    """
    When True, listings JSON and ``search_cars`` include rows that fail public completeness
    (e.g. missing transmission until VDP/repair). Override with env:

    * ``LISTINGS_INCLUDE_INCOMPLETE_CARS=0`` — hide incomplete (strict) in any environment
    * ``LISTINGS_INCLUDE_INCOMPLETE_CARS=1`` — show incomplete everywhere

    Default: include incomplete in non-production, exclude in production (keeps public prod tidy).
    """
    from backend.utils.runtime_env import is_production_env

    raw = (os.environ.get("LISTINGS_INCLUDE_INCOMPLETE_CARS") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return not is_production_env()


def serialize_car_for_listings_grid(
    car: dict,
    *,
    incomplete_ids: set[int] | None = None,
) -> dict[str, Any]:
    """
    Lightweight grid JSON for listings (see ``car_serialize.serialize_car_for_listings_grid``).
    """
    out = _serialize_car_for_listings_grid(car)
    if listings_include_incomplete_cars():
        ids = incomplete_ids if incomplete_ids is not None else _incomplete_car_ids_for_listings()
        try:
            cid = int(car.get("id") or 0)
        except (TypeError, ValueError):
            cid = 0
        if cid and cid in ids:
            out["public_incomplete"] = True
    return out


def listings_grid_bootstrap_cars(limit: int = 48) -> list[dict[str, Any]]:
    """First page of cached grid cars for SSR (instant paint while full fleet loads)."""
    try:
        lim = max(1, min(int(limit), 200))
    except (TypeError, ValueError):
        lim = 48
    cars = listings_grid_serialized_cars()
    return cars[:lim] if cars else []


def refresh_car_data_quality_score(car_id: int) -> None:
    """Recompute data_quality_score from current row."""
    car = get_car_by_id(car_id)
    if not car:
        return
    score = compute_data_quality_score(car)
    with db_conn() as conn:
        conn.execute("UPDATE cars SET data_quality_score = ? WHERE id = ?", (score, car_id))
        conn.commit()
    try:
        from backend.db import incomplete_listings_db as ild

        ild.sync_incomplete_listing_for_car_id(car_id)
    except Exception:
        _log.exception("incomplete_listings sync after data_quality_score update failed")


def get_incomplete_cars() -> list[dict]:
    """Cars indexed in ``incomplete_listings.db`` (regular listing spec gaps for dev tools)."""
    from backend.db.incomplete_listings_db import get_incomplete_cars_for_dev

    return get_incomplete_cars_for_dev()


def get_dealership_issue_stats(limit: int = 10) -> list[dict[str, Any]]:
    """Get dealerships ranked by number of incomplete/problematic listings."""
    with db_conn() as conn:
        cursor = conn.cursor()
        # Query dealerships with the most incomplete or low-quality cars
        cursor.execute(f"""
            SELECT
                dealer_name,
                dealer_id,
                COUNT(*) as total_cars,
                SUM(CASE WHEN missing_field_count > 0 THEN 1 ELSE 0 END) as incomplete_count,
                SUM(CASE WHEN marked_for_review = 1 THEN 1 ELSE 0 END) as flagged_count,
                ROUND(AVG(COALESCE(data_quality_score, 0)), 2) as avg_quality_score,
                SUM(CASE WHEN price IS NULL OR price = 0 THEN 1 ELSE 0 END) as no_price_count
            FROM cars
            WHERE dealer_name IS NOT NULL AND TRIM(dealer_name) != ''
            GROUP BY dealer_id, dealer_name
            HAVING incomplete_count > 0 OR flagged_count > 0
            ORDER BY incomplete_count DESC, flagged_count DESC
            LIMIT ?
        """, (limit,))

        stats = []
        for row in cursor.fetchall():
            stats.append({
                "dealer_name": row[0],
                "dealer_id": row[1],
                "total_cars": row[2],
                "incomplete_count": row[3] or 0,
                "flagged_count": row[4] or 0,
                "avg_quality_score": row[5] or 0.0,
                "no_price_count": row[6] or 0,
            })
        return stats


def _sort_cars_by_price(cars: list) -> list:
    """Stable sort: priced vehicles first, unknown/NULL last (avoids TypeError vs None)."""
    def key(c):
        p = c.get("price")
        if p is None:
            return (1, 0.0)
        try:
            return (0, float(p))
        except (TypeError, ValueError):
            return (1, 0.0)

    return sorted(cars, key=key)


def link_cars_to_dealership_registry(
    registry_id: int,
    website_url: str,
    *,
    dealer_id_slug: str | None = None,
) -> int:
    """
    Attach scraped cars to a Smart Import dealership row.

    Matches on ``dealer_url`` (normalized host / URL variants) and, when given, on
    ``dealer_id`` (same slug as ``dealers.json`` / ``scanner.js``), so links succeed even if
    URL text differs between Node insert and the registry row.
    """
    if not website_url or not registry_id:
        return 0
    w = (website_url or "").strip()
    base = w.rstrip("/")
    w_lower = w.lower()
    base_lower = base.lower()
    base_slash_lower = (base_lower + "/") if not base_lower.endswith("/") else base_lower
    host = ""
    try:
        host = (urlparse(w).netloc or "").lower().replace("www.", "")
    except ValueError:
        pass
    did = (dealer_id_slug or "").strip()
    total = 0
    with db_conn() as conn:
        cursor = conn.cursor()
        if host:
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE dealership_registry_id IS NULL
                  AND (
                    LOWER(TRIM(dealer_url)) IN (?, ?, ?)
                    OR LOWER(dealer_url) LIKE ?
                  )
                """,
                (
                    registry_id,
                    w_lower,
                    base_lower,
                    base_slash_lower,
                    f"%{host}%",
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE dealership_registry_id IS NULL
                  AND LOWER(TRIM(dealer_url)) IN (?, ?)
                """,
                (registry_id, w_lower, base_lower),
            )
        total += cursor.rowcount
        if did:
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE dealership_registry_id IS NULL
                  AND TRIM(dealer_id) = ?
                """,
                (registry_id, did),
            )
            total += cursor.rowcount
        conn.commit()
    return int(total)


_registry_backfill_ran = False


def backfill_dealership_registry_ids(*, conn=None) -> int:
    """
    Set ``dealership_registry_id`` on active cars where host matches registry URLs.

    Idempotent; safe to run after scans or before listings geo load.
    """
    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    total = 0
    if conn is not None:
        host_to_reg = registry_id_by_dealer_host(conn)
        cursor = conn.cursor()
        for host, reg_id in host_to_reg.items():
            cursor.execute(
                """
                UPDATE cars
                SET dealership_registry_id = ?
                WHERE (COALESCE(listing_active, 1) = 1)
                  AND (dealership_registry_id IS NULL
                       OR CAST(dealership_registry_id AS INTEGER) <= 0)
                  AND LOWER(IFNULL(dealer_url, '')) LIKE ?
                """,
                (reg_id, f"%{host.lower()}%"),
            )
            total += int(cursor.rowcount or 0)
        conn.commit()
        return total

    with db_conn() as c:
        return backfill_dealership_registry_ids(conn=c)


def ensure_dealership_registry_backfill() -> int:
    """Run host→registry backfill once per process (listings geo / first search)."""
    global _registry_backfill_ran
    if _registry_backfill_ran:
        return 0
    _registry_backfill_ran = True
    try:
        n = backfill_dealership_registry_ids()
        if n:
            _log.info("Backfilled dealership_registry_id on %s listing(s)", n)
        return n
    except Exception as e:
        _log.warning("dealership_registry backfill skipped: %s", e)
        return 0


def _normalized_interior_bucket_filters(raw) -> set[str] | None:
    if not raw:
        return None
    from backend.utils.interior_color_buckets import ALLOWED_BUCKETS

    sel = {str(x).strip().lower() for x in raw if str(x).strip()}
    sel &= ALLOWED_BUCKETS
    return sel or None


def search_cars(makes=None, models=None, trims=None, fuel_types=None,
                cylinders=None, transmissions=None, drivetrains=None,
                body_styles=None,
                exterior_colors=None, interior_colors=None,
                interior_color_bucket_filters=None,
                engine_displacement_l_min=None,
                engine_displacement_l_max=None,
                countries=None,
                min_year=None, max_year=None,
                max_price=None, max_mileage=None,
                zip_code=None, radius_miles=None,
                dealership_registry_id=None,
                dealer_registry_ids=None,
                candidate_ids=None,
                packages_json_contains=None,
                packages_json_contains_list=None,
                trim_contains=None,
                trim_contains_list=None,
                vehicle_or=None,
                vin=None,
                include_incomplete: bool | None = None):
    """
    ``candidate_ids``: optional list of SQLite ``cars.id`` values (e.g. pgvector semantic recall).
    When set, results are restricted to ``id IN (candidate_ids)`` in addition to other filters.

    ``vin``: optional full 17-character VIN (normalized: spaces stripped, case-insensitive). When
    set, only that VIN row is considered (with other filters AND).

    ``packages_json_contains``: optional **literal** substring (case-insensitive) matched against
    the raw ``cars.packages`` TEXT (uses ``INSTR``, not ``LIKE``, so ``%``/``_`` in the needle are
    not SQL wildcards). Hybrid search: ``backend.utils.hybrid_search`` kwargs builder.

    ``packages_json_contains_list``: optional list of substrings; a row matches if **any** needle
    appears in ``cars.packages`` (OR). Sidebar ``package`` checkboxes map here via GET ``/listings``.

    ``max_price`` / ``max_mileage`` when set to ``0`` are applied; they are not treated as
    "unset." ``dealership_registry_id`` must be a positive int; invalid values are ignored.

    ``interior_color_bucket_filters``: optional list of bucket ids (e.g. ``black``, ``tan``);
    a row matches if its ``interior_color_buckets`` JSON array intersects the selection (OR).
    When combined with ``interior_colors``, both constraints apply (AND).

    ``exterior_colors`` / ``interior_colors``: each value is a **paint-family bucket id**
    (e.g. ``red``, ``black``), not the raw dealer string. A row matches if any inferred family
    for that side intersects the selection (OR within one column). Raw ``exterior_color`` /
    ``interior_color`` on the row is unchanged for detail pages.

    ``engine_displacement_l_min`` / ``engine_displacement_l_max``: optional inclusive range in
    liters, matched using ``engine_l`` (numeric) or a leading ``N.NL`` / ``NL`` token in
    ``engine_description``. Rows with no parseable displacement are excluded when either bound
    is set. Combined with ``cylinders`` as AND when both are provided.

    ``include_incomplete``: when False, rows that fail :func:`is_car_incomplete` are omitted
    (public listings). When None, use :func:`listings_include_incomplete_cars`.
    """
    if include_incomplete is None:
        inc = listings_include_incomplete_cars()
    else:
        inc = bool(include_incomplete)

    from backend.db.geo import zip_to_coords, haversine

    query = "SELECT * FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
    params = []

    if candidate_ids:
        ids = []
        for x in candidate_ids:
            try:
                i = int(x)
                if i > 0:
                    ids.append(i)
            except (TypeError, ValueError):
                continue
        if ids:
            query += f" AND id IN ({_placeholders(ids)})"
            params.extend(ids)

    dealer_registry_filter_ids: list[int] = []
    if dealership_registry_id is not None:
        try:
            dr = int(dealership_registry_id)
        except (TypeError, ValueError):
            dr = 0
        if dr > 0:
            dealer_registry_filter_ids = [dr]

    if dealer_registry_ids:
        for x in dealer_registry_ids:
            try:
                i = int(x)
                if i > 0 and i not in dealer_registry_filter_ids:
                    dealer_registry_filter_ids.append(i)
            except (TypeError, ValueError):
                continue

    if vin and str(vin).strip():
        vnorm = re.sub(r"\s+", "", str(vin).strip().upper())[:20]
        if len(vnorm) == 17:
            query += " AND REPLACE(UPPER(TRIM(IFNULL(vin, ''))), ' ', '') = ?"
            params.append(vnorm)

    def add_multi(col, values):
        nonlocal query
        if values:
            query += f" AND {col} IN ({_placeholders(values)})"
            params.extend(values)

    def add_multi_ci(col, values):
        """Case-insensitive match for scraped text fields (e.g. DODGE vs Dodge)."""
        nonlocal query
        if values:
            lowered = [str(v).lower().strip() for v in values]
            query += (
                f" AND LOWER(TRIM(IFNULL({col}, ''))) IN ({_placeholders(lowered)})"
            )
            params.extend(lowered)

    # Country of origin filter: resolve countries to makes, combine with explicit makes
    makes_for_countries = _makes_for_countries(countries)
    if makes_for_countries is not None:
        if makes:
            allow_lower = {k.lower(): k for k in makes_for_countries}
            normalized = []
            for m in makes:
                hit = allow_lower.get(str(m).lower().strip())
                if hit is not None:
                    normalized.append(hit)
            makes = list(dict.fromkeys(normalized))
        else:
            makes = list(makes_for_countries)

    vehicle_or_clauses: list[str] = []
    if vehicle_or and isinstance(vehicle_or, list) and len(vehicle_or) >= 2:
        for branch in vehicle_or[:8]:
            if not isinstance(branch, dict):
                continue
            sub_parts: list[str] = []
            mk = branch.get("make")
            if mk and str(mk).strip():
                sub_parts.append("LOWER(TRIM(IFNULL(make, ''))) = ?")
                params.append(str(mk).lower().strip())
            branch_models = branch.get("models") or branch.get("model")
            if branch_models:
                if isinstance(branch_models, str):
                    branch_models = [branch_models]
                lowered_models = [str(v).lower().strip() for v in branch_models if str(v).strip()]
                if lowered_models:
                    sub_parts.append(
                        f"LOWER(TRIM(IFNULL(model, ''))) IN ({_placeholders(lowered_models)})"
                    )
                    params.extend(lowered_models)
            branch_trim = branch.get("trim_contains")
            if branch_trim and str(branch_trim).strip():
                sub_parts.append("INSTR(LOWER(IFNULL(trim, '')), ?) > 0")
                params.append(str(branch_trim).strip().lower()[:100])
            if sub_parts:
                vehicle_or_clauses.append("(" + " AND ".join(sub_parts) + ")")
        if vehicle_or_clauses:
            query += " AND (" + " OR ".join(vehicle_or_clauses) + ")"
    else:
        add_multi_ci("make", makes)
        add_multi_ci("model", models)
    add_multi_ci("trim", trims)
    add_multi("fuel_type", fuel_types)
    add_multi("cylinders", [int(c) for c in cylinders] if cylinders else None)
    add_multi("transmission", transmissions)
    add_multi("drivetrain", drivetrains)
    add_multi_ci("body_style", body_styles)

    pkg_needles: list[str] = []
    seen_pkg: set[str] = set()
    if packages_json_contains and str(packages_json_contains).strip():
        needle = str(packages_json_contains).strip().lower()
        if len(needle) > 200:
            needle = needle[:200]
        if needle not in seen_pkg:
            seen_pkg.add(needle)
            pkg_needles.append(needle)
    if packages_json_contains_list:
        for raw_needle in packages_json_contains_list:
            needle = str(raw_needle or "").strip().lower()
            if not needle or needle in seen_pkg:
                continue
            if len(needle) > 200:
                needle = needle[:200]
            seen_pkg.add(needle)
            pkg_needles.append(needle)
    if pkg_needles:
        query += " AND (" + " OR ".join(
            ["INSTR(LOWER(IFNULL(packages, '')), ?) > 0"] * len(pkg_needles)
        ) + ")"
        params.extend(pkg_needles)

    if trim_contains_list:
        needles = [str(t).strip().lower()[:100] for t in trim_contains_list if str(t).strip()]
        if needles:
            query += " AND (" + " OR ".join(
                ["INSTR(LOWER(IFNULL(trim, '')), ?) > 0"] * len(needles)
            ) + ")"
            params.extend(needles)
    elif trim_contains and str(trim_contains).strip():
        needle = str(trim_contains).strip().lower()
        if len(needle) > 100:
            needle = needle[:100]
        query += " AND INSTR(LOWER(IFNULL(trim, '')), ?) > 0"
        params.append(needle)

    if min_year is not None:
        query += " AND year >= ?"
        params.append(int(min_year))
    if max_year is not None:
        query += " AND year <= ?"
        params.append(int(max_year))

    if max_price is not None:
        try:
            mp = float(max_price)
        except (TypeError, ValueError):
            pass
        else:
            query += " AND (price IS NULL OR price <= ? OR price = 0)"
            params.append(mp)
    if max_mileage is not None:
        try:
            mm = int(float(max_mileage))
        except (TypeError, ValueError):
            pass
        else:
            query += " AND (mileage IS NULL OR mileage <= ? OR mileage = 0)"
            params.append(mm)

    with db_conn(row_factory=sqlite3.Row) as conn:
        if dealer_registry_filter_ids:
            from backend.listings.dealer_registry_match import (
                dealer_registry_sql_filter,
                registry_id_by_dealer_host,
            )

            host_map = registry_id_by_dealer_host(conn)
            clause, extra = dealer_registry_sql_filter(
                dealer_registry_filter_ids,
                host_map,
                placeholders_fn=_placeholders,
            )
            query += clause
            params.extend(extra)
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]

    bucket_sel = _normalized_interior_bucket_filters(interior_color_bucket_filters)
    ext_family_sel = _normalized_interior_bucket_filters(exterior_colors)
    int_family_sel = _normalized_interior_bucket_filters(interior_colors)

    def _row_exterior_families(car: dict) -> set[str]:
        return set(infer_paint_color_buckets(car.get("exterior_color"), car.get("make")))

    def _row_interior_families(car: dict) -> set[str]:
        stored = set(parse_stored_buckets(car.get("interior_color_buckets")))
        if stored:
            return stored
        return set(infer_paint_color_buckets(car.get("interior_color"), car.get("make")))

    eng_lo = eng_hi = None
    if engine_displacement_l_min is not None:
        try:
            eng_lo = float(engine_displacement_l_min)
        except (TypeError, ValueError):
            eng_lo = None
    if engine_displacement_l_max is not None:
        try:
            eng_hi = float(engine_displacement_l_max)
        except (TypeError, ValueError):
            eng_hi = None

    def _post_sql_filters(cars: list[dict]) -> list[dict]:
        out = cars
        if ext_family_sel:
            out = [c for c in out if _row_exterior_families(c) & ext_family_sel]
        if int_family_sel:
            out = [c for c in out if _row_interior_families(c) & int_family_sel]
        if bucket_sel:
            out = [c for c in out if row_matches_interior_bucket_filter(c, bucket_sel)]
        if eng_lo is not None or eng_hi is not None:
            out = [c for c in out if car_matches_engine_displacement_l_range(c, eng_lo, eng_hi)]
        return out

    if zip_code and radius_miles:
        origin = zip_to_coords(zip_code)
        if origin is None:
            return []
        from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords

        with db_conn() as _gc:
            dealer_geo = load_dealer_geo_index(_gc)
        filtered = []
        for car in results:
            dest = lookup_dealer_coords(str(car.get("dealer_url") or ""), dealer_geo)
            if not dest:
                dest = zip_to_coords(car.get("zip_code", "") or "")
            if dest:
                dist = haversine(origin[0], origin[1], dest[0], dest[1])
                if dist <= radius_miles:
                    car["distance_miles"] = round(dist, 1)
                    filtered.append(car)
        for c in filtered:
            _parse_car_gallery(c)
            _parse_car_history_highlights(c)
        base = filtered if inc else [c for c in filtered if not is_car_incomplete(c)]
        complete = _post_sql_filters(base)
        return sorted(complete, key=lambda c: c["distance_miles"])

    for c in results:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)
    base = results if inc else [c for c in results if not is_car_incomplete(c)]
    complete = _post_sql_filters(base)
    return _sort_cars_by_price(complete)


def search_cars_by_make_model_pairs(
    pairs: list[tuple[str, str]],
    *,
    zip_code: str | None = None,
    radius_miles: float | None = None,
    include_incomplete: bool | None = None,
) -> list[dict]:
    """Fetch active cars matching any (make, model) pair in one SQL round-trip."""
    if not pairs:
        return []
    if include_incomplete is None:
        inc = listings_include_incomplete_cars()
    else:
        inc = bool(include_incomplete)

    clauses: list[str] = []
    params: list[Any] = []
    seen: set[tuple[str, str]] = set()
    for make, model in pairs[:8]:
        mk = str(make or "").strip().lower()
        mo = str(model or "").strip().lower()
        if not mk or not mo or (mk, mo) in seen:
            continue
        seen.add((mk, mo))
        clauses.append(
            "(LOWER(TRIM(IFNULL(make, ''))) = ? AND LOWER(TRIM(IFNULL(model, ''))) = ?)"
        )
        params.extend([mk, mo])
    if not clauses:
        return []

    from backend.db.geo import haversine, zip_to_coords

    query = (
        "SELECT * FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
        f" AND ({' OR '.join(clauses)})"
    )
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]

    if zip_code and radius_miles:
        origin = zip_to_coords(zip_code)
        if origin is None:
            return []
        from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords

        with db_conn() as _gc:
            dealer_geo = load_dealer_geo_index(_gc)
        filtered = []
        for car in results:
            dest = lookup_dealer_coords(str(car.get("dealer_url") or ""), dealer_geo)
            if not dest:
                dest = zip_to_coords(car.get("zip_code", "") or "")
            if dest:
                dist = haversine(origin[0], origin[1], dest[0], dest[1])
                if dist <= radius_miles:
                    car["distance_miles"] = round(dist, 1)
                    filtered.append(car)
        for c in filtered:
            _parse_car_gallery(c)
            _parse_car_history_highlights(c)
        base = filtered if inc else [c for c in filtered if not is_car_incomplete(c)]
        return sorted(base, key=lambda c: c.get("distance_miles", 0))

    for c in results:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)
    base = results if inc else [c for c in results if not is_car_incomplete(c)]
    return _sort_cars_by_price(base)


def save_car(user_id: int, car_id: int) -> None:
    with db_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO saved_cars (user_id, car_id) VALUES (?, ?)",
            (int(user_id), int(car_id)),
        )
        conn.commit()


def unsave_car(user_id: int, car_id: int) -> None:
    with db_conn() as conn:
        conn.execute(
            "DELETE FROM saved_cars WHERE user_id = ? AND car_id = ?",
            (int(user_id), int(car_id)),
        )
        conn.commit()


def get_saved_car_ids(user_id: int) -> list[int]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT car_id FROM saved_cars WHERE user_id = ? ORDER BY saved_at DESC",
            (int(user_id),),
        ).fetchall()
    return [int(r[0]) for r in rows]


def is_car_saved(user_id: int, car_id: int) -> bool:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM saved_cars WHERE user_id = ? AND car_id = ? LIMIT 1",
            (int(user_id), int(car_id)),
        ).fetchone()
    return row is not None


def get_car_by_id(car_id, *, include_inactive: bool = True):
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        if include_inactive:
            cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
        else:
            cursor.execute(
                "SELECT * FROM cars WHERE id = ? AND (COALESCE(listing_active, 1) = 1)",
                (car_id,),
            )
        row = cursor.fetchone()
    car = dict(row) if row else None
    if car:
        _parse_car_gallery(car)
        _parse_car_history_highlights(car)
    return car


def get_cars_by_ids(car_ids: list[int]) -> list[dict]:
    """Fetch full car rows by primary key; order matches ``car_ids`` (skips missing)."""
    if not car_ids:
        return []
    ordered_unique: list[int] = []
    seen: set[int] = set()
    for raw in car_ids:
        try:
            i = int(raw)
        except (TypeError, ValueError):
            continue
        if i <= 0 or i in seen:
            continue
        seen.add(i)
        ordered_unique.append(i)
    if not ordered_unique:
        return []
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        ph = _placeholders(ordered_unique)
        cursor.execute(f"SELECT * FROM cars WHERE id IN ({ph})", ordered_unique)
        by_id = {dict(row)["id"]: dict(row) for row in cursor.fetchall()}
    out: list[dict] = []
    for cid in ordered_unique:
        row = by_id.get(cid)
        if not row:
            continue
        _parse_car_gallery(row)
        _parse_car_history_highlights(row)
        out.append(row)
    return out


def get_car_by_vin(vin):
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cars WHERE vin = ?", (vin,))
        row = cursor.fetchone()
    car = dict(row) if row else None
    if car:
        _parse_car_gallery(car)
        _parse_car_history_highlights(car)
    return car


_UPDATABLE_CAR_COLUMNS = frozenset(
    {
        "title",
        "year",
        "make",
        "model",
        "trim",
        "price",
        "mileage",
        "zip_code",
        "fuel_type",
        "cylinders",
        "transmission",
        "transmission_type",
        "drivetrain",
        "exterior_color",
        "interior_color",
        "image_url",
        "dealer_name",
        "dealer_url",
        "dealer_id",
        "stock_number",
        "gallery",
        "carfax_url",
        "window_sticker_url",
        "history_highlights",
        "msrp",
        "dealership_registry_id",
        "source_url",
        "body_style",
        "engine_description",
        "engine_l",
        "condition",
        "description",
        "data_quality_score",
        "mpg_city",
        "mpg_highway",
        "is_cpo",
        "model_full_raw",
        "recovery_status",
        "recovery_attempted_at",
        "recovery_source",
        "recovery_notes",
        "missing_field_count",
        "recoverability_score",
        "spec_source_json",
        "packages",
        "listing_active",
        "listing_removed_at",
        "interior_color_buckets",
        "kbb_fetched_at",
        "kbb_snapshot_json",
        "kbb_fair_purchase",
        "kbb_range_low",
        "kbb_range_high",
        "kbb_private_party",
        "kbb_trade_in",
        "first_seen_at",
        "last_price_change_at",
        "internal_notes",
        "marked_for_review",
        "price_provenance_json",
    }
)


def update_car_row_partial(car_id: int, fields: dict) -> None:
    """Persist only provided keys (used by incomplete listing recovery)."""
    if not fields:
        return
    sets: list[str] = []
    vals: list = []
    for k, raw in fields.items():
        if k not in _UPDATABLE_CAR_COLUMNS:
            continue
        if k == "gallery" and isinstance(raw, list):
            raw = json.dumps(raw)
        sets.append(f"{k} = ?")
        vals.append(raw)
    if not sets:
        return
    vals.append(car_id)
    with db_conn() as conn:
        conn.execute(f"UPDATE cars SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
    try:
        from backend.db import incomplete_listings_db as ild

        ild.sync_incomplete_listing_for_car_id(car_id)
    except Exception:
        _log.exception("incomplete_listings sync after partial update failed")


def _normalize_make_capitalization(make: str) -> str:
    """Normalize make name capitalization: title case for most, handle special cases."""
    if not make:
        return make

    # Special cases: handle multi-word makes and known variations
    special_cases = {
        "land rover": "Land Rover",
        "rolls royce": "Rolls Royce",
        "aston martin": "Aston Martin",
        "mclaren": "McLaren",
        "mclaughlin": "McLaughlin",
        "ram": "RAM",
        "gmc": "GMC",
        "bmw": "BMW",
        "tesla": "Tesla",
        "vw": "Volkswagen",
        "mercedes-benz": "Mercedes-Benz",
        "alfa romeo": "Alfa Romeo",
        "mini": "MINI",
        "infiniti": "INFINITI",
        "lexus": "LEXUS",
    }

    m = str(make).strip()
    m_lower = m.lower()

    # Check special cases
    if m_lower in special_cases:
        return special_cases[m_lower]

    # Default: capitalize first letter, lowercase the rest
    return m[0].upper() + m[1:].lower() if m else m


def _normalize_facet_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def _canonical_facet_label(value: str, *, variants: list[str]) -> str:
    """Pick one display label for case/spacing variants (``LARIAT`` vs ``Lariat``)."""
    pool = []
    for v in variants:
        s = re.sub(r"\s+", " ", (v or "").strip())
        if s and s not in pool:
            pool.append(s)
    if not pool:
        return (value or "").strip()
    if len(pool) == 1:
        return pool[0]

    def _rank(v: str) -> tuple:
        letters = sum(c.isalpha() for c in v)
        all_caps = letters > 0 and v == v.upper()
        has_lower = any(c.islower() for c in v)
        has_upper = any(c.isupper() for c in v)
        mixed = has_lower and has_upper
        return (mixed, not all_caps, v == v.title(), len(v))

    return max(pool, key=_rank)


def _facet_make_valid(make) -> bool:
    """Reject polluted ``make`` values (numeric trims, model names) for facet lists."""
    if is_effectively_empty(make):
        return False
    m = str(make).strip()
    if m.isdigit():
        return False
    if len(m) <= 4 and re.match(r"^\d{3,4}$", m):
        return False
    return _lookup_make_country(m) is not None


def _facet_transmission_sane(val) -> bool:
    """Drop values that are clearly cylinder counts or garbage, not transmissions."""
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    if s.isdigit() and len(s) <= 2:
        return False
    if s in frozenset({"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}):
        return False
    return True


_facet_options_cache_token: float | None = None
_facet_options_cache_value: dict[str, Any] | None = None
_geo_coords_cache_token: float | None = None
_geo_coords_cache_value: dict[str, Any] | None = None
_grid_cars_cache_token: float | None = None
_grid_cars_cache_value: list[dict[str, Any]] | None = None
_LISTINGS_GRID_CACHE_REV = 4


def _inventory_listings_cache_token() -> float:
    """Invalidate listings caches when SQLite inventory mtime changes (60s bucket on Postgres)."""
    if is_inventory_postgres():
        import time

        return float(int(time.time()) // 60)
    try:
        return os.path.getmtime(DB_PATH)
    except OSError:
        return 0.0


def clear_inventory_listings_cache() -> None:
    """Drop facet/grid caches (tests or admin tools after bulk inventory writes)."""
    global _facet_options_cache_token, _facet_options_cache_value
    global _geo_coords_cache_token, _geo_coords_cache_value
    global _grid_cars_cache_token, _grid_cars_cache_value
    global _incomplete_car_id_set_cache
    _facet_options_cache_token = None
    _facet_options_cache_value = None
    _geo_coords_cache_token = None
    _geo_coords_cache_value = None
    _grid_cars_cache_token = None
    _grid_cars_cache_value = None
    _incomplete_car_id_set_cache = None


def public_listings_count() -> int:
    """Approximate count of active inventory rows for marketing/stats (cheap COUNT)."""
    active = "(COALESCE(listing_active, 1) = 1)"
    with db_conn() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM cars WHERE {active}").fetchone()
    try:
        return max(0, int(row[0] if row else 0))
    except (TypeError, ValueError, IndexError):
        return 0


def listings_grid_serialized_cars() -> list[dict[str, Any]]:
    """
    Per-car JSON for the listings grid (``options.all_cars`` and ``GET /api/listings/cars``).
    Honors :func:`listings_include_incomplete_cars` and :func:`serialize_car_for_listings_grid`.
    """
    global _grid_cars_cache_token, _grid_cars_cache_value
    token = _inventory_listings_cache_token()
    if _grid_cars_cache_value is not None and _grid_cars_cache_token == token:
        return _grid_cars_cache_value

    active = "(COALESCE(listing_active, 1) = 1)"
    inc = listings_include_incomplete_cars()
    cols = ", ".join(LISTINGS_GRID_CAR_COLUMNS)
    with db_conn(row_factory=sqlite3.Row) as conn2:
        cur = conn2.cursor()
        cur.execute(
            f"SELECT {cols} FROM cars WHERE {active} ORDER BY price ASC"
        )
        all_cars_raw = [dict(r) for r in cur.fetchall()]
    incomplete_ids = _incomplete_car_ids_for_listings()
    for c in all_cars_raw:
        _parse_car_gallery(c)
    out: list[dict[str, Any]] = []
    for c in all_cars_raw:
        if not inc:
            cid = c.get("id")
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                cid_int = 0
            if cid_int in incomplete_ids:
                continue
        out.append(serialize_car_for_listings_grid(c, incomplete_ids=incomplete_ids))
    # Cars with images float to the top; no-image cars sink to the bottom.
    out.sort(key=lambda c: (0 if c.get("image_url") or c.get("gallery") else 1, c.get("price") or 0))
    _grid_cars_cache_token = token
    _grid_cars_cache_value = out
    return out


def listings_grid_cache_etag() -> str:
    """Cheap cache validator for ``GET /api/listings/cars`` (If-None-Match / 304)."""
    token = _inventory_listings_cache_token()
    if _grid_cars_cache_value is not None and _grid_cars_cache_token == token:
        n = len(_grid_cars_cache_value)
    else:
        with db_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM cars WHERE (COALESCE(listing_active, 1) = 1)"
            ).fetchone()
            n = int(row[0] if row else 0)
    return f'W/"{_LISTINGS_GRID_CACHE_REV}-{token}-{n}"'


def listings_geo_coords_maps() -> dict[str, Any]:
    """
    ZIP + dealer coordinate maps for client-side radius filtering.
    Loaded lazily via ``GET /api/listings/geo-coords`` (not embedded in HTML).
    """
    global _geo_coords_cache_token, _geo_coords_cache_value
    token = _inventory_listings_cache_token()
    if _geo_coords_cache_value is not None and _geo_coords_cache_token == token:
        return _geo_coords_cache_value

    ensure_dealership_registry_backfill()

    active = "(COALESCE(listing_active, 1) = 1)"
    from backend.db.dealer_geo import (
        dealer_coords_client_map,
        load_dealer_geo_index,
        load_registry_coords_map,
    )
    from backend.db.geo import zip_to_coords
    from backend.listings.dealer_registry_match import registry_id_by_dealer_host

    zip_coords: dict[str, list[float]] = {}
    registry_id_by_host: dict[str, int] = {}
    registry_coords: dict[str, list[float]] = {}
    with db_conn() as conn:
        dealer_coords = dealer_coords_client_map(load_dealer_geo_index(conn))
        registry_coords = load_registry_coords_map(conn)
        raw_host_map = registry_id_by_dealer_host(conn)
        registry_id_by_host = {h: rid for h, rid in raw_host_map.items()}
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT zip_code FROM cars WHERE {active} AND zip_code IS NOT NULL")
        unique_zips = {row[0] for row in cursor.fetchall()}
    for zip_code in unique_zips:
        if zip_code and str(zip_code).strip():
            coords = zip_to_coords(str(zip_code).strip())
            if coords:
                zip_coords[str(zip_code).strip()] = [float(coords[0]), float(coords[1])]
    out = {
        "zip_coords": zip_coords,
        "dealer_coords": dealer_coords,
        "registry_coords": registry_coords,
        "registry_id_by_host": registry_id_by_host,
    }
    _geo_coords_cache_token = token
    _geo_coords_cache_value = out
    return out


def get_filter_options(*, include_all_cars: bool = False) -> dict[str, Any]:
    """
    Returns all filter option data with full relationship maps so the
    frontend can do bidirectional cascading across every dimension.

    ``include_all_cars`` embeds the full grid payload (~12MB); listings HTML loads
    cars via ``GET /api/listings/cars`` instead (``include_all_cars=False``, default).
    Facet metadata is cached until inventory.db changes.
    """
    global _facet_options_cache_token, _facet_options_cache_value
    token = _inventory_listings_cache_token()
    if _facet_options_cache_value is not None and _facet_options_cache_token == token:
        out = dict(_facet_options_cache_value)
        out["all_cars"] = listings_grid_serialized_cars() if include_all_cars else []
        return out

    active = "(COALESCE(listing_active, 1) = 1)"

    with db_conn() as conn:
        cursor = conn.cursor()

        def distinct(col):
            cursor.execute(
                f"SELECT DISTINCT {col} FROM cars WHERE {active} AND {col} IS NOT NULL ORDER BY {col}"
            )
            return [r[0] for r in cursor.fetchall() if not is_effectively_empty(r[0])]

        def distinct_title_cased(col):
            """Get distinct values, deduplicated with title-case normalization."""
            values = distinct(col)
            seen = {}
            result = []
            for v in values:
                if v:
                    # Normalize to title case, but keep as-is for short acronyms
                    normalized = v if len(v) <= 3 and v.isupper() else v.title()
                    key = normalized.lower()
                    if key not in seen:
                        seen[key] = normalized
                        result.append(normalized)
            return result

        fuel_types      = sort_fuel_type_presets(distinct("fuel_type"))
        cylinders       = distinct("cylinders")
        transmissions   = [t for t in distinct("transmission") if _facet_transmission_sane(t)]
        drivetrains     = distinct("drivetrain")
        cursor.execute(
            f"""
            SELECT exterior_color, interior_color, interior_color_buckets
            FROM cars
            WHERE {active}
            """
        )
        ext_facet_ids: set[str] = set()
        int_facet_ids: set[str] = set()
        for ext_raw, int_raw, int_bucks in cursor.fetchall():
            if not is_effectively_empty(ext_raw):
                ext_facet_ids.update(infer_paint_color_buckets(ext_raw, None))
            ib = parse_stored_buckets(int_bucks)
            if ib:
                int_facet_ids.update(ib)
            elif not is_effectively_empty(int_raw):
                int_facet_ids.update(infer_paint_color_buckets(int_raw, None))
        exterior_colors = sort_paint_family_ids(ext_facet_ids)
        interior_colors = sort_paint_family_ids(int_facet_ids)
        body_styles_list = sort_body_style_presets(distinct("body_style"))

        # Extract packages per make/model for filter cascade
        package_rows: list[dict[str, str]] = []
        all_package_names: list[str] = []
        _seen_pkg_keys: set[tuple] = set()
        _seen_pkg_names: set[str] = set()
        cursor.execute(
            f"SELECT make, model, packages FROM cars "
            f"WHERE {active} AND packages IS NOT NULL "
            f"AND packages NOT IN ('{{}}', '[]', 'null', '')"
        )
        for _make, _model, _pkg_raw in cursor.fetchall():
            if not _make or not _model:
                continue
            try:
                _p = json.loads(_pkg_raw)
            except Exception:
                continue
            _names: list[str] = []
            for _entry in (_p.get("packages_normalized") or []):
                if isinstance(_entry, dict):
                    _n = (_entry.get("canonical_name") or _entry.get("name") or "").strip()
                    if _n:
                        _names.append(_n)
            for _n in (_p.get("possible_packages") or []):
                if isinstance(_n, str) and _n.strip():
                    _names.append(_n.strip())
            for _n in _names:
                _key = (_make.lower(), _model.lower(), _n.lower())
                if _key not in _seen_pkg_keys:
                    _seen_pkg_keys.add(_key)
                    package_rows.append({"make": _make, "model": _model, "name": _n})
                if _n.lower() not in _seen_pkg_names:
                    _seen_pkg_names.add(_n.lower())
                    all_package_names.append(_n)
        all_package_names.sort()

        # Full relationship rows — every unique combo of all filterable dims.
        # The frontend embeds these as data-* on each checkbox so it can filter
        # any dropdown based on any combination of other active filters.
        cursor.execute(f"""
            SELECT DISTINCT make, model, trim, fuel_type, cylinders, drivetrain, body_style
            FROM cars
            WHERE {active}
              AND make IS NOT NULL AND TRIM(make) != ''
            ORDER BY make, model, trim
        """)
        raw_car_rows = cursor.fetchall()
        car_rows: list = []
        for row in raw_car_rows:
            make, model, trim, fuel_type, cyl, drive, body_st = (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
            )
            if is_effectively_empty(make) or is_effectively_empty(model):
                continue
            if not _facet_make_valid(make):
                continue
            if is_effectively_empty(trim):
                trim = None
            if is_effectively_empty(fuel_type):
                fuel_type = None
            else:
                fuel_type = coerce_fuel_type_stored(fuel_type)
            if is_effectively_empty(drive):
                drive = None
            if is_effectively_empty(body_st):
                body_st = None
            else:
                body_st = coerce_body_style_stored(body_st)
            car_rows.append((make, model, trim, fuel_type, cyl, drive, body_st))

    # Derive distinct makes/models/trims with normalized keys (one UI option per logical value).
    make_variants: dict[str, list[str]] = {}
    model_variants: dict[tuple[str, str], list[str]] = {}
    trim_variants: dict[tuple[str, str, str], list[str]] = {}
    for row in car_rows:
        make, model, trim = row[0], row[1], row[2]
        make_normalized = _normalize_make_capitalization(make)
        make_key = _normalize_facet_key(make_normalized)
        make_variants.setdefault(make_key, [])
        if make_normalized not in make_variants[make_key]:
            make_variants[make_key].append(make_normalized)

        model_key = _normalize_facet_key(model)
        mk = (make_key, model_key)
        model_variants.setdefault(mk, [])
        if model not in model_variants[mk]:
            model_variants[mk].append(model)

        if trim is not None and str(trim).strip():
            trim_key = _normalize_facet_key(trim)
            tk = (make_key, model_key, trim_key)
            trim_variants.setdefault(tk, [])
            if trim not in trim_variants[tk]:
                trim_variants[tk].append(trim)

    seen_makes: list[str] = []
    for make_key in sorted(make_variants.keys()):
        seen_makes.append(_canonical_facet_label("", variants=make_variants[make_key]))

    seen_models: list[tuple[str, str]] = []
    for (make_key, model_key) in sorted(model_variants.keys()):
        make_label = _canonical_facet_label("", variants=make_variants[make_key])
        model_label = _canonical_facet_label("", variants=model_variants[(make_key, model_key)])
        seen_models.append((make_label, model_label))

    seen_trims: list[tuple[str, str, str | None]] = []
    for (make_key, model_key, trim_key) in sorted(trim_variants.keys()):
        make_label = _canonical_facet_label("", variants=make_variants[make_key])
        model_label = _canonical_facet_label("", variants=model_variants[(make_key, model_key)])
        trim_label = _canonical_facet_label("", variants=trim_variants[(make_key, model_key, trim_key)])
        seen_trims.append((make_label, model_label, trim_label))

    # Countries that have at least one make in our DB
    country_set = set()
    country_to_makes = {}
    for make in seen_makes:
        # Try normalized make first, fall back to original for legacy compatibility
        c = _lookup_make_country(make) or _lookup_make_country(make.lower())
        if c:
            country_set.add(c)
            country_to_makes.setdefault(c, []).append(make)
    for lst in country_to_makes.values():
        lst.sort()
    countries = sorted(country_set)

    facets: dict[str, Any] = {
        "makes":           seen_makes,
        "model_rows":      seen_models,
        "trim_rows":       seen_trims,
        "fuel_types":      fuel_types,
        "cylinders":       cylinders,
        "transmissions":   transmissions,
        "drivetrains":     drivetrains,
        "body_styles":     body_styles_list,
        "exterior_colors": exterior_colors,
        "interior_colors": interior_colors,
        "package_rows":    package_rows,
        "all_package_names": all_package_names,
        "countries":       countries,
        "country_to_makes": country_to_makes,
        # Full relationship table for cascade engine
        "car_rows":        [
            {
                "make": r[0],
                "model": r[1],
                "trim": r[2],
                "fuel": r[3],
                "cyl": r[4],
                "drive": r[5],
                "body_style": r[6] if len(r) > 6 else None,
            }
            for r in car_rows
        ],
        # Geo maps are lazy-loaded via GET /api/listings/geo-coords (keeps HTML fast).
        "zip_coords":      {},
        "dealer_coords":   {},
    }
    _facet_options_cache_token = token
    _facet_options_cache_value = dict(facets)
    out = dict(facets)
    out["all_cars"] = listings_grid_serialized_cars() if include_all_cars else []
    return out
