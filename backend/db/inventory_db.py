import json
import logging
import os
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse

from backend.utils.car_serialize import car_matches_engine_displacement_l_range, serialize_car_for_api
from backend.utils.field_clean import compute_data_quality_score, is_effectively_empty
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    row_matches_interior_bucket_filter,
    sort_paint_family_ids,
)

DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
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
    ``incomplete_listings`` rows, and drop ``nhtsa_vpic_cache`` entries for those VINs.
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
        cur.execute(f"DELETE FROM cars WHERE id IN ({ph})", ids)
        conn.commit()
        return {"deleted": len(ids), "vins": vins, "nhtsa_cache_deleted": n_cache}
    finally:
        conn.close()


def ensure_cars_table_columns(cursor) -> None:
    """Add optional listing / quality columns (idempotent ALTERs)."""
    cursor.execute("PRAGMA table_info(cars)")
    existing = {row[1] for row in cursor.fetchall()}
    for col, ctype in [
        ("source_url", "TEXT"),
        ("body_style", "TEXT"),
        ("engine_description", "TEXT"),
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
    ]:
        if col not in existing:
            cursor.execute(f"ALTER TABLE cars ADD COLUMN {col} {ctype}")


def ensure_scan_runs_table(cursor: sqlite3.Cursor) -> None:
    """Append-only scanner run summaries for store admin sync reliability (inventory.db)."""
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
    conn = get_conn()
    cur = conn.cursor()
    ensure_scan_runs_table(cur)
    n = 0
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
    conn.close()
    return n


def list_scan_runs(*, dealer_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Recent scan rows (newest first), optionally filtered by ``dealer_id``."""
    lim = max(1, min(200, int(limit)))
    conn = get_conn()
    conn.row_factory = sqlite3.Row
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
    conn.close()
    return rows


def ensure_nhtsa_vpic_cache_table(conn: sqlite3.Connection) -> None:
    """
    Optional SQLite cache for NHTSA vPIC ``DecodeVinValuesExtended`` JSON bodies.

    Full API document is stored in ``response_json`` (same shape as HTTP ``format=json``);
    provenance for row patches stays on ``cars.spec_source_json`` only.
    """
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


def get_conn():
    """
    New connection with WAL, extended lock wait (``INVENTORY_SQLITE_LOCK_TIMEOUT_SEC``, default 60s)
    to avoid spurious ``database is locked`` when the scraper, Flask, or a CLI tool overlaps
    the same ``inventory.db``.
    """
    lock_s = _inventory_sqlite_lock_wait_sec()
    conn = sqlite3.connect(DB_PATH, timeout=lock_s)
    try:
        conn.execute("PRAGMA busy_timeout=?", (int(max(5000, round(lock_s * 1000))),))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def init_inventory_db():
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


# (vin, title, year, make, model, trim, price, mileage, zip_code,
#  fuel_type, cylinders, transmission, drivetrain,
#  exterior_color, interior_color, image_url, dealer_name, dealer_url)
SEED_DATA = [
    ("VIN001", "2022 BMW M3 Competition", 2022, "BMW", "M3", "Competition",
     72995, 8200, "28202", "Gasoline", 6, "Automatic", "RWD",
     "Frozen Portimao Blue", "Silverstone Merino",
     "https://images.unsplash.com/photo-1555215695-3004980ad54e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN002", "2023 BMW M5 Competition", 2023, "BMW", "M5", "Competition",
     115900, 3100, "28203", "Gasoline", 8, "Automatic", "AWD",
     "Isle of Man Green", "Black Merino",
     "https://images.unsplash.com/photo-1607853202273-797f1c22a38e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN003", "2022 BMW 550i xDrive", 2022, "BMW", "5 Series", "M550i",
     78400, 14500, "28202", "Gasoline", 8, "Automatic", "AWD",
     "Carbon Black", "Cognac",
     "https://images.unsplash.com/photo-1556189250-72ba954cfc2b?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN004", "2021 BMW 740i xDrive", 2021, "BMW", "7 Series", "740i",
     82000, 22000, "28205", "Gasoline", 6, "Automatic", "AWD",
     "Black Sapphire", "Ivory White",
     "https://images.unsplash.com/photo-1619767886558-efdc259b6e09?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN005", "2023 Mercedes-Benz C300", 2023, "Mercedes-Benz", "C-Class", "C300",
     48500, 5600, "28203", "Gasoline", 4, "Automatic", "RWD",
     "Polar White", "Black",
     "https://images.unsplash.com/photo-1618843479313-40f8afb4b4d8?w=800",
     "Fletcher Jones Mercedes", "https://www.fletcherjonesmercedes.com"),

    ("VIN006", "2022 Audi Q5 Premium Plus", 2022, "Audi", "Q5", "Premium Plus",
     41200, 18700, "28205", "Gasoline", 4, "Automatic", "AWD",
     "Mythos Black", "Rock Gray",
     "https://images.unsplash.com/photo-1606664515524-ed2f786a0bd6?w=800",
     "Hendrick Audi", "https://www.hendrickaudi.com"),

    ("VIN007", "2023 Porsche 911 Carrera S", 2023, "Porsche", "911", "Carrera S",
     138000, 1200, "28207", "Gasoline", 6, "Automatic", "RWD",
     "GT Silver", "Black",
     "https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=800",
     "Porsche Charlotte", "https://www.porschecharlotte.com"),

    ("VIN008", "2022 Toyota Camry XSE", 2022, "Toyota", "Camry", "XSE",
     31450, 27000, "28208", "Gasoline", 4, "Automatic", "FWD",
     "Midnight Black", "Black",
     "https://images.unsplash.com/photo-1621007947382-bb3c3994e3fb?w=800",
     "Toyota of Charlotte", "https://www.toyotaofcharlotte.com"),

    ("VIN009", "2023 Tesla Model 3 Long Range", 2023, "Tesla", "Model 3", "Long Range",
     47990, 4300, "28212", "Electric", 0, "Automatic", "AWD",
     "Pearl White", "Black",
     "https://images.unsplash.com/photo-1560958089-b8a1929cea89?w=800",
     "Tesla Charlotte", "https://www.tesla.com/findus/location/store/charlotte"),

    ("VIN010", "2022 Lexus RX 350 F Sport", 2022, "Lexus", "RX 350", "F Sport",
     55600, 16400, "28213", "Gasoline", 6, "Automatic", "AWD",
     "Atomic Silver", "Black",
     "https://images.unsplash.com/photo-1519641471654-76ce0107ad1b?w=800",
     "Lexus of Charlotte", "https://www.lexusofcharlotte.com"),

    ("VIN011", "2021 Ford F-150 Lariat", 2021, "Ford", "F-150", "Lariat",
     54900, 31000, "28209", "Gasoline", 8, "Automatic", "4WD",
     "Oxford White", "Medium Dark Slate",
     "https://images.unsplash.com/photo-1558618666-fcd25c85cd64?w=800",
     "Ford of Charlotte", "https://www.fordofcharlotte.com"),

    ("VIN012", "2021 Chevrolet Corvette Stingray", 2021, "Chevrolet", "Corvette", "Stingray",
     67800, 9800, "28210", "Gasoline", 8, "Manual", "RWD",
     "Rapid Blue", "Jet Black",
     "https://images.unsplash.com/photo-1552519507-da3b142c6e3d?w=800",
     "Hendrick Chevrolet", "https://www.hendrickchevrolet.com"),

    ("VIN013", "2022 Honda Accord Sport", 2022, "Honda", "Accord", "Sport",
     29900, 19200, "28211", "Gasoline", 4, "CVT", "FWD",
     "Sonic Gray Pearl", "Black",
     "https://images.unsplash.com/photo-1609521263047-f8f205293f24?w=800",
     "Honda of Charlotte", "https://www.hondaofcharlotte.com"),

    ("VIN014", "2023 BMW 640i Gran Coupe", 2023, "BMW", "6 Series", "640i Gran Coupe",
     89500, 2900, "28202", "Gasoline", 6, "Automatic", "RWD",
     "Mineral White", "Oyster",
     "https://images.unsplash.com/photo-1556189250-72ba954cfc2b?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    ("VIN015", "2022 BMW M550i xDrive", 2022, "BMW", "5 Series", "M550i",
     91200, 11000, "28202", "Gasoline", 8, "Automatic", "AWD",
     "Bernina Grey", "Tartufo",
     "https://images.unsplash.com/photo-1555215695-3004980ad54e?w=800",
     "Hendrick BMW Charlotte", "https://www.hendrickbmwcharlotte.com"),

    # Diesel example — Ram 1500
    ("VIN016", "2022 Ram 1500 Laramie", 2022, "Ram", "1500", "Laramie",
     58900, 21000, "28209", "Diesel", 6, "Automatic", "4WD",
     "Granite Crystal", "Black",
     "https://images.unsplash.com/photo-1622038085247-bbe4a0e98adb?w=800",
     "Charlotte Ram", "https://www.charlotteram.com"),
]


def seed_cars():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT OR IGNORE INTO cars
            (vin, title, year, make, model, trim, price, mileage, zip_code,
             fuel_type, cylinders, transmission, drivetrain,
             exterior_color, interior_color, image_url, dealer_name, dealer_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, SEED_DATA)
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


def is_car_incomplete(car: dict) -> bool:
    """True when the row should be hidden from public listings (subset of spec sheet)."""
    from backend.utils.listing_completeness import is_car_incomplete_for_public_listings

    return is_car_incomplete_for_public_listings(car)


def refresh_car_data_quality_score(car_id: int) -> None:
    """Recompute data_quality_score from current row."""
    car = get_car_by_id(car_id)
    if not car:
        return
    score = compute_data_quality_score(car)
    conn = get_conn()
    conn.execute("UPDATE cars SET data_quality_score = ? WHERE id = ?", (score, car_id))
    conn.commit()
    conn.close()
    try:
        from backend.db import incomplete_listings_db as ild

        ild.sync_incomplete_listing_for_car_id(car_id)
    except Exception:
        _log.exception("incomplete_listings sync after data_quality_score update failed")


def get_incomplete_cars() -> list[dict]:
    """Cars indexed in ``incomplete_listings.db`` (regular listing spec gaps for dev tools)."""
    from backend.db.incomplete_listings_db import get_incomplete_cars_for_dev

    return get_incomplete_cars_for_dev()


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


def link_cars_to_dealership_registry(registry_id: int, website_url: str) -> int:
    """Attach scraped cars to a Smart Import dealership row (match on dealer_url)."""
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
    conn = get_conn()
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
    n = cursor.rowcount
    conn.commit()
    conn.close()
    return n


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
                candidate_ids=None,
                packages_json_contains=None):
    """
    ``candidate_ids``: optional list of SQLite ``cars.id`` values (e.g. pgvector semantic recall).
    When set, results are restricted to ``id IN (candidate_ids)`` in addition to other filters.

    ``packages_json_contains``: optional substring matched case-insensitively against the raw
    ``cars.packages`` TEXT (JSON). Intended for ``packages_normalized`` / catalog names populated
    by ``scripts/parse_listing_descriptions.py``. Hybrid search wiring: see
    ``backend.hybrid_inventory_search.flask_request_to_search_cars_kwargs`` (no separate public
    filter UI contract yet — pass through kwargs or extend query params when exposing).

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
    """

    from backend.db.geo import zip_to_coords, haversine

    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

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

    if dealership_registry_id is not None:
        query += " AND dealership_registry_id = ?"
        params.append(int(dealership_registry_id))

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
    add_multi_ci("make", makes)
    add_multi_ci("model", models)
    add_multi_ci("trim", trims)
    add_multi("fuel_type", fuel_types)
    add_multi("cylinders", [int(c) for c in cylinders] if cylinders else None)
    add_multi("transmission", transmissions)
    add_multi("drivetrain", drivetrains)
    add_multi_ci("body_style", body_styles)

    if packages_json_contains and str(packages_json_contains).strip():
        needle = str(packages_json_contains).strip().lower()
        if len(needle) > 200:
            needle = needle[:200]
        query += " AND LOWER(IFNULL(packages, '')) LIKE ?"
        params.append(f"%{needle}%")

    if min_year is not None:
        query += " AND year >= ?"
        params.append(int(min_year))
    if max_year is not None:
        query += " AND year <= ?"
        params.append(int(max_year))

    if max_price is not None and max_price:
        query += " AND (price IS NULL OR price <= ? OR price = 0)"
        params.append(max_price)
    if max_mileage is not None and max_mileage:
        query += " AND (mileage IS NULL OR mileage <= ? OR mileage = 0)"
        params.append(max_mileage)

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

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
        if origin:
            filtered = []
            for car in results:
                dest = zip_to_coords(car.get("zip_code", ""))
                if dest:
                    dist = haversine(origin[0], origin[1], dest[0], dest[1])
                    if dist <= radius_miles:
                        car["distance_miles"] = round(dist, 1)
                        filtered.append(car)
            for c in filtered:
                _parse_car_gallery(c)
                _parse_car_history_highlights(c)
            complete = [c for c in filtered if not is_car_incomplete(c)]
            complete = _post_sql_filters(complete)
            return sorted(complete, key=lambda c: c["distance_miles"])

    for c in results:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)
    complete = [c for c in results if not is_car_incomplete(c)]
    complete = _post_sql_filters(complete)
    return _sort_cars_by_price(complete)


def get_car_by_id(car_id, *, include_inactive: bool = True):
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    if include_inactive:
        cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
    else:
        cursor.execute(
            "SELECT * FROM cars WHERE id = ? AND (COALESCE(listing_active, 1) = 1)",
            (car_id,),
        )
    row = cursor.fetchone()
    conn.close()
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
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ph = _placeholders(ordered_unique)
    cursor.execute(f"SELECT * FROM cars WHERE id IN ({ph})", ordered_unique)
    by_id = {dict(row)["id"]: dict(row) for row in cursor.fetchall()}
    conn.close()
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
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cars WHERE vin = ?", (vin,))
    row = cursor.fetchone()
    conn.close()
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
    conn = get_conn()
    conn.execute(f"UPDATE cars SET {', '.join(sets)} WHERE id = ?", vals)
    conn.commit()
    conn.close()
    try:
        from backend.db import incomplete_listings_db as ild

        ild.sync_incomplete_listing_for_car_id(car_id)
    except Exception:
        _log.exception("incomplete_listings sync after partial update failed")


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


def get_filter_options():
    """
    Returns all filter option data with full relationship maps so the
    frontend can do bidirectional cascading across every dimension.
    """
    conn = get_conn()
    cursor = conn.cursor()

    active = "(COALESCE(listing_active, 1) = 1)"

    def distinct(col):
        cursor.execute(
            f"SELECT DISTINCT {col} FROM cars WHERE {active} AND {col} IS NOT NULL ORDER BY {col}"
        )
        return [r[0] for r in cursor.fetchall() if not is_effectively_empty(r[0])]

    fuel_types      = distinct("fuel_type")
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
    body_styles_list = distinct("body_style")

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
    car_rows = []
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
        if is_effectively_empty(drive):
            drive = None
        if is_effectively_empty(body_st):
            body_st = None
        car_rows.append((make, model, trim, fuel_type, cyl, drive, body_st))

    conn.close()

    # Derive distinct makes/models/trims preserving order
    seen_makes  = []
    seen_models = []  # (make, model)
    seen_trims  = []  # (make, model, trim)
    for row in car_rows:
        make, model, trim = row[0], row[1], row[2]
        if make not in seen_makes:
            seen_makes.append(make)
        if (make, model) not in seen_models:
            seen_models.append((make, model))
        if (make, model, trim) not in seen_trims:
            seen_trims.append((make, model, trim))

    # Full per-car data for client-side live filtering and rendering
    conn2 = get_conn()
    conn2.row_factory = sqlite3.Row
    all_cars_cursor = conn2.cursor()
    all_cars_cursor.execute(
        f"SELECT * FROM cars WHERE {active} ORDER BY price ASC"
    )
    all_cars_raw = [dict(r) for r in all_cars_cursor.fetchall()]
    for c in all_cars_raw:
        _parse_car_gallery(c)
        _parse_car_history_highlights(c)
    conn2.close()
    all_cars = [
        serialize_car_for_api(c, include_verified=False)
        for c in all_cars_raw
        if not is_car_incomplete(c)
    ]

    # Countries that have at least one make in our DB
    country_set = set()
    country_to_makes = {}
    for make in seen_makes:
        c = _lookup_make_country(make)
        if c:
            country_set.add(c)
            country_to_makes.setdefault(c, []).append(make)
    for lst in country_to_makes.values():
        lst.sort()
    countries = sorted(country_set)

    return {
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
        # Complete car objects for client-side live rendering
        "all_cars":        all_cars,
    }
