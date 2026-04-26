"""
Database layer for scanner: connection to inventory.db and vehicle upsert.
"""
import json
import logging
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from backend.db.inventory_db import ensure_cars_table_columns
from backend.utils.analytics_ep import apply_ep_from_scanner_dict
from backend.utils.field_clean import clean_car_row_dict, compute_data_quality_score
from backend.utils.interior_color_buckets import interior_color_buckets_json

# Use same DB path as inventory_db / dealerships_db when running from project root
DB_PATH = os.environ.get("INVENTORY_DB_PATH", "inventory.db")
logger = logging.getLogger(__name__)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def _ensure_schema(conn):
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
            dealer_id        TEXT,
            scraped_at       TEXT
        )
    """)
    conn.commit()
    cursor.execute("PRAGMA table_info(cars)")
    cols = [row[1] for row in cursor.fetchall()]
    for col, ctype in [
        ("dealer_id", "TEXT"),
        ("stock_number", "TEXT"),
        ("gallery", "TEXT"),
        ("carfax_url", "TEXT"),
        ("history_highlights", "TEXT"),
        ("msrp", "REAL"),
        ("dealership_registry_id", "INTEGER"),
        ("is_cpo", "INTEGER"),
        ("model_full_raw", "TEXT"),
        ("mpg_city", "INTEGER"),
        ("mpg_highway", "INTEGER"),
    ]:
        if col not in cols:
            logger.info("Adding %s column to cars table", col)
            cursor.execute(f"ALTER TABLE cars ADD COLUMN {col} {ctype}")
            conn.commit()
    ensure_cars_table_columns(cursor)
    conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_specs (
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            cylinders INTEGER,
            gears INTEGER,
            transmission TEXT,
            PRIMARY KEY (make, model)
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
            conn.commit()
    conn.commit()
    conn.close()


def upsert_vehicles(vehicles: list[dict]) -> int:
    """
    Insert or replace vehicles by vin. Strict de-duplication: one row per VIN
    (same car in 'New' and 'Used' counts once). Uses ON CONFLICT(vin) DO UPDATE.
    """
    if not vehicles:
        return 0
    by_vin = {}
    for v in vehicles:
        vin = (v.get("vin") or "").strip()
        if vin:
            by_vin[vin] = v
    vehicles = list(by_vin.values())
    conn = get_conn()
    _ensure_schema(conn)
    conn = get_conn()
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    count = 0
    for raw in vehicles:
        merged = apply_ep_from_scanner_dict(dict(raw))
        v = clean_car_row_dict(merged)
        vin = (v.get("vin") or "").strip()
        if not vin:
            continue
        title = (
            v.get("title")
            or f"{v.get('year') or ''} {v.get('make') or ''} {v.get('model') or ''} {v.get('trim') or ''}".strip()
            or "Unknown vehicle"
        )
        # Price: ensure number (strip $ and , already done in parser); store as int/float
        try:
            price = v.get("price")
            price = int(round(float(price))) if price is not None and str(price).strip() != "" else 0
        except (TypeError, ValueError):
            price = 0
        # Mileage: ensure integer
        try:
            mileage = v.get("mileage")
            mileage = int(mileage) if mileage is not None and str(mileage).strip() != "" else 0
        except (TypeError, ValueError):
            mileage = 0
        try:
            msrp_val = v.get("msrp")
            msrp = int(round(float(msrp_val))) if msrp_val is not None and str(msrp_val).strip() != "" else None
            if msrp is not None and msrp <= 0:
                msrp = None
        except (TypeError, ValueError):
            msrp = None
        # Gallery: SQLite stores arrays as JSON string; always use json.dumps(list)
        gallery = v.get("gallery")
        if isinstance(gallery, list):
            gallery_json = json.dumps(gallery)
        elif gallery is not None and isinstance(gallery, str):
            try:
                json.loads(gallery)
                gallery_json = gallery
            except (TypeError, ValueError):
                gallery_json = "[]"
        else:
            gallery_json = "[]"
        highlights = v.get("history_highlights")
        highlights_json = json.dumps(highlights) if isinstance(highlights, list) else (highlights if isinstance(highlights, str) else "[]")
        img = v.get("image_url")
        if not img or not str(img).strip().startswith("http"):
            img = "/static/placeholder.svg"
        preview = {
            **v,
            "vin": vin,
            "title": title,
            "price": price,
            "mileage": mileage,
            "image_url": img,
        }
        dq = compute_data_quality_score(preview)
        interior_buckets_json = interior_color_buckets_json(v.get("interior_color"), v.get("make"))
        spec_src = v.get("spec_source_json")
        if isinstance(spec_src, dict):
            spec_src = json.dumps(spec_src, ensure_ascii=False)
        elif spec_src is not None and not isinstance(spec_src, str):
            spec_src = str(spec_src)
        pkg_raw = v.get("packages")
        if isinstance(pkg_raw, dict):
            packages_json = json.dumps(pkg_raw, ensure_ascii=False)
        elif isinstance(pkg_raw, str) and pkg_raw.strip() not in ("", "{}", "[]", "null"):
            packages_json = pkg_raw.strip()
        else:
            packages_json = None
        cursor.execute(
            """
            INSERT INTO cars (
                vin, title, year, make, model, trim, price, mileage,
                image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                zip_code, fuel_type, cylinders, transmission, drivetrain,
                exterior_color, interior_color, interior_color_buckets, stock_number, gallery, carfax_url, history_highlights, msrp,
                dealership_registry_id,
                source_url, body_style, engine_description, condition, description, data_quality_score,
                mpg_city, mpg_highway, is_cpo, model_full_raw,
                packages,
                listing_active, listing_removed_at, spec_source_json,
                first_seen_at, last_price_change_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vin) DO UPDATE SET
                title=excluded.title, year=excluded.year, make=excluded.make,
                model=excluded.model, trim=COALESCE(excluded.trim, trim), price=excluded.price,
                mileage=excluded.mileage,
                image_url=CASE
                    WHEN excluded.image_url LIKE 'http%' THEN excluded.image_url
                    WHEN cars.image_url LIKE 'http%' THEN cars.image_url
                    ELSE excluded.image_url
                END,
                dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
                dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
                zip_code=excluded.zip_code,
                fuel_type=COALESCE(excluded.fuel_type, fuel_type),
                cylinders=COALESCE(excluded.cylinders, cylinders),
                transmission=COALESCE(excluded.transmission, transmission),
                drivetrain=COALESCE(excluded.drivetrain, drivetrain),
                exterior_color=COALESCE(NULLIF(TRIM(excluded.exterior_color), ''), exterior_color),
                interior_color=COALESCE(NULLIF(TRIM(excluded.interior_color), ''), interior_color),
                interior_color_buckets=CASE
                    WHEN NULLIF(TRIM(excluded.interior_color), '') IS NOT NULL THEN excluded.interior_color_buckets
                    ELSE cars.interior_color_buckets
                END,
                stock_number=COALESCE(NULLIF(excluded.stock_number, ''), stock_number),
                gallery=COALESCE(
                    NULLIF(NULLIF(TRIM(excluded.gallery), ''), '[]'),
                    cars.gallery
                ),
                carfax_url=excluded.carfax_url, history_highlights=excluded.history_highlights,
                msrp=excluded.msrp,
                dealership_registry_id=COALESCE(excluded.dealership_registry_id, dealership_registry_id),
                source_url=COALESCE(excluded.source_url, source_url),
                body_style=COALESCE(excluded.body_style, body_style),
                engine_description=COALESCE(excluded.engine_description, engine_description),
                condition=COALESCE(NULLIF(TRIM(excluded.condition), ''), condition),
                description=COALESCE(excluded.description, description),
                data_quality_score=excluded.data_quality_score,
                mpg_city=COALESCE(excluded.mpg_city, mpg_city),
                mpg_highway=COALESCE(excluded.mpg_highway, mpg_highway),
                is_cpo=COALESCE(excluded.is_cpo, is_cpo),
                model_full_raw=COALESCE(excluded.model_full_raw, model_full_raw),
                packages=COALESCE(NULLIF(TRIM(excluded.packages), ''), cars.packages),
                listing_active=1,
                listing_removed_at=NULL,
                spec_source_json=CASE
                    WHEN excluded.spec_source_json IS NOT NULL AND length(trim(excluded.spec_source_json)) > 0
                    THEN excluded.spec_source_json
                    ELSE cars.spec_source_json
                END,
                first_seen_at=COALESCE(cars.first_seen_at, excluded.scraped_at),
                last_price_change_at=CASE
                    WHEN IFNULL(cars.price, -1e12) != IFNULL(excluded.price, -1e12) THEN excluded.scraped_at
                    ELSE COALESCE(cars.last_price_change_at, cars.first_seen_at, excluded.scraped_at)
                END,
                internal_notes=cars.internal_notes,
                marked_for_review=cars.marked_for_review
            """,
            (
                vin,
                title,
                v.get("year"),
                v.get("make") or "",
                v.get("model") or "",
                v.get("trim"),
                price,
                mileage,
                img,
                v.get("dealer_name") or "",
                v.get("dealer_url"),
                v.get("dealer_id") or "",
                now,
                v.get("zip_code"),
                v.get("fuel_type"),
                v.get("cylinders"),
                v.get("transmission"),
                v.get("drivetrain"),
                v.get("exterior_color"),
                v.get("interior_color"),
                interior_buckets_json,
                v.get("stock_number") or "",
                gallery_json,
                v.get("carfax_url"),
                highlights_json,
                msrp,
                v.get("dealership_registry_id"),
                v.get("source_url"),
                v.get("body_style"),
                v.get("engine_description"),
                v.get("condition"),
                v.get("description"),
                dq,
                v.get("mpg_city"),
                v.get("mpg_highway"),
                v.get("is_cpo"),
                v.get("model_full_raw"),
                packages_json,
                1,
                None,
                spec_src,
                now,
                now,
            ),
        )
        count += 1
        trace_vin = (os.environ.get("SCANNER_TRACE_VIN") or "").strip().upper()
        if trace_vin and vin.upper() == trace_vin[:17]:
            cursor.execute(
                "SELECT transmission, drivetrain, interior_color, exterior_color, fuel_type, "
                "body_style, engine_description, cylinders, mpg_city, mpg_highway, trim "
                "FROM cars WHERE vin = ?",
                (vin,),
            )
            rb = cursor.fetchone()
            logger.info(
                "UPSERT VERIFY VIN %s mem: tr=%r drv=%r int=%r ext=%r fuel=%r | DB: %s",
                vin[:17],
                v.get("transmission"),
                v.get("drivetrain"),
                v.get("interior_color"),
                v.get("exterior_color"),
                v.get("fuel_type"),
                rb,
            )
    conn.commit()
    conn.close()
    logger.info("Upserted %d vehicles", count)
    if count > 0:
        try:
            from backend.db import incomplete_listings_db as ild
            from backend.db.inventory_db import get_car_by_id, refresh_car_data_quality_score, update_car_row_partial
            from backend.nhtsa_vpic import looks_like_decode_vin
            from backend.spec_structured_backfill import apply_structured_spec_backfill_for_car
            from backend.utils.field_clean import is_effectively_empty
            from backend.utils.inventory_repair import collect_row_storage_repairs

            conn2 = get_conn()
            cur2 = conn2.cursor()
            for vin_key in by_vin:
                cur2.execute("SELECT id FROM cars WHERE vin = ?", (vin_key,))
                row_id = cur2.fetchone()
                if not row_id:
                    continue
                cid = int(row_id[0])
                ild.sync_incomplete_listing_for_car_id(cid)
                car = get_car_by_id(cid, include_inactive=True)
                if not car or not is_effectively_empty(car.get("transmission")):
                    continue
                if not looks_like_decode_vin(vin_key):
                    continue
                tier1 = collect_row_storage_repairs(car)
                if tier1.get("transmission"):
                    update_car_row_partial(cid, tier1)
                    refresh_car_data_quality_score(cid)
                    ild.sync_incomplete_listing_for_car_id(cid)
                else:
                    apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
            conn2.close()
        except Exception:
            logger.exception("incomplete_listings / transmission backfill after upsert failed")

        # --- model_specs dictionary correction (cylinders + transmission) ---
        try:
            apply_model_specs_corrections(vins=list(by_vin.keys()))
        except Exception:
            logger.exception("model_specs correction after upsert failed")

    return count


# ---------------------------------------------------------------------------
# Canonical make resolution for model_specs lookups
# Handles scraper-introduced make/model swaps (e.g. make='Wrangler' model='Wrangler')
# ---------------------------------------------------------------------------
_MAKE_FIX_MAP: dict[tuple[str, str], str] = {
    ("wrangler",        "wrangler"):          "Jeep",
    ("gladiator",       "gladiator"):         "Jeep",
    ("grand",           "grand cherokee"):    "Jeep",
    ("grand",           "grand cherokee l"):  "Jeep",
    ("grand",           "grand wagoneer"):    "Jeep",
    ("grand",           "grand wagoneer l"):  "Jeep",
    ("cherokee",        "cherokee"):          "Jeep",
    ("compass",         "compass"):           "Jeep",
    ("1500",            "1500"):              "RAM",
    ("2500",            "2500"):              "RAM",
    ("3500",            "3500"):              "RAM",
    ("classic",         "1500 classic"):      "RAM",
    ("promaster",       "promaster 1500"):    "RAM",
    ("promaster",       "promaster 2500"):    "RAM",
    ("5500hd",          "5500hd"):            "RAM",
    ("durango",         "durango"):           "Dodge",
    ("charger",         "charger"):           "Dodge",
    ("challenger",      "challenger"):        "Dodge",
    ("journey",         "journey"):           "Dodge",
    ("pacifica",        "pacifica"):          "Dodge",
    ("sierra",          "sierra 1500"):       "GMC",
    ("silverado",       "silverado 1500"):    "Chevrolet",
    ("silverado",       "silverado 1500 ltd"):"Chevrolet",
    ("tacoma",          "tacoma"):            "Toyota",
    ("telluride",       "telluride"):         "Kia",
    ("santa",           "santa fe"):          "Hyundai",
    ("santa",           "santa fe sport"):    "Hyundai",
    ("ioniq",           "ioniq 5"):           "Hyundai",
    ("hyundai",         "ioniq 5"):           "Hyundai",
    ("hyundai",         "ioniq 6"):           "Hyundai",
    ("hyundai",         "kona"):              "Hyundai",
    ("f-150",           "f-150"):             "Ford",
    ("explorer",        "explorer"):          "Ford",
    ("mustang",         "mustang"):           "Ford",
    ("escape",          "escape"):            "Ford",
    ("camaro",          "camaro"):            "Chevrolet",
    ("escalade",        "escalade esv"):      "Cadillac",
    ("tahoe",           "tahoe"):             "Chevrolet",
    ("yukon",           "yukon"):             "GMC",
    ("mdx",             "mdx"):               "Acura",
    ("rdx",             "rdx"):               "Acura",
    ("gx",              "gx"):                "Lexus",
    ("cx-50",           "cx-50"):             "Mazda",
    ("rav4",            "rav4"):              "Toyota",
    ("lacrosse",        "lacrosse"):          "Buick",
    ("tiguan",          "tiguan"):            "VW",
    ("forte",           "forte"):             "Kia",
    ("cooper",          "cooper s countryman"):"Mini",
    ("golf",            "golf gti"):          "VW",
    ("accord",          "accord sedan"):      "Honda",
    ("lincoln",         "aviator"):           "Lincoln",
    ("x5",              "x5"):                "BMW",
    ("4",               "4 series"):          "BMW",
}


def _resolve_canonical_make(make: str, model: str) -> str:
    """Return the canonical make for a (make, model) pair, handling scraper swaps."""
    key = (make.strip().lower(), model.strip().lower())
    return _MAKE_FIX_MAP.get(key, make)


def _infer_drivetrain_from_trim(trim: str | None, title: str | None) -> str | None:
    """
    Return AWD/RWD/FWD/4WD based on known drivetrain keywords in trim or title.
    Returns None when nothing conclusive is found (fall back to model_specs default).
    """
    blob = f"{trim or ''} {title or ''}".upper()
    # AWD signals
    if re.search(r"\b(XDRIVE|4MATIC|QUATTRO|SH-AWD|AWD|4X4|4WD|ALL[\s-]WHEEL)\b", blob):
        return "AWD"
    # 4WD truck signals
    if re.search(r"\b(4X4|4WD)\b", blob):
        return "4WD"
    # RWD signals
    if re.search(r"\b(SDRIVE|RWD|REAR[\s-]WHEEL)\b", blob):
        return "RWD"
    # FWD signals
    if re.search(r"\b(FWD|FRONT[\s-]WHEEL)\b", blob):
        return "FWD"
    return None


def apply_model_specs_corrections(
    vins: list[str] | None = None,
    conn: sqlite3.Connection | None = None,
) -> int:
    """
    Fill NULL/empty cylinders, transmission, drivetrain, body_style, and fuel_type
    from the model_specs dictionary.

    For drivetrain: also applies trim-level overrides (xDrive → AWD, sDrive → RWD,
    4MATIC → AWD, Quattro → AWD) which take precedence over the model default.

    Called after every upsert (for the just-inserted VINs) and by the backfill
    script (vins=None to scan all rows). Never overwrites a value the dealer
    already provided.

    Returns the number of rows updated.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()
    cur = conn.cursor()

    if vins is not None:
        placeholders = ",".join("?" * len(vins))
        cur.execute(
            f"SELECT vin, make, model, trim, title, cylinders, transmission, drivetrain, body_style, fuel_type "
            f"FROM cars WHERE vin IN ({placeholders})",
            vins,
        )
    else:
        cur.execute(
            "SELECT vin, make, model, trim, title, cylinders, transmission, drivetrain, body_style, fuel_type "
            "FROM cars"
        )

    rows = cur.fetchall()
    updated = 0

    for vin, raw_make, raw_model, trim, title, cylinders, transmission, drivetrain, body_style, fuel_type in rows:
        needs_cyl = cylinders is None or (
            isinstance(cylinders, (int, float)) and int(cylinders) == 0
            and not _is_electric_make_model(raw_make, raw_model)
        )
        needs_trans = not transmission or str(transmission).strip() == ""
        needs_drive = not drivetrain or str(drivetrain).strip() == ""
        needs_body = not body_style or str(body_style).strip() == ""
        needs_fuel = not fuel_type or str(fuel_type).strip() == ""

        if not any([needs_cyl, needs_trans, needs_drive, needs_body, needs_fuel]):
            continue

        canonical_make = _resolve_canonical_make(raw_make or "", raw_model or "")
        model = (raw_model or "").strip()

        cur.execute(
            "SELECT cylinders, transmission, drivetrain, body_style, fuel_type FROM model_specs "
            "WHERE lower(make)=lower(?) AND lower(model)=lower(?) LIMIT 1",
            (canonical_make, model),
        )
        spec = cur.fetchone()
        if not spec:
            continue

        spec_cyl, spec_trans, spec_drive, spec_body, spec_fuel = spec
        patch: dict[str, object] = {}

        if needs_cyl and spec_cyl is not None:
            patch["cylinders"] = int(spec_cyl)
        if needs_trans and spec_trans and str(spec_trans).strip():
            patch["transmission"] = str(spec_trans).strip()
        if needs_drive and spec_drive:
            # Trim/title overrides take precedence over model default
            drive = _infer_drivetrain_from_trim(trim, title) or spec_drive
            patch["drivetrain"] = drive
        if needs_body and spec_body:
            patch["body_style"] = str(spec_body).strip()
        if needs_fuel and spec_fuel:
            patch["fuel_type"] = str(spec_fuel).strip()

        if patch:
            sets = ", ".join(f"{k}=?" for k in patch)
            cur.execute(
                f"UPDATE cars SET {sets} WHERE vin=?",
                (*patch.values(), vin),
            )
            updated += 1

    conn.commit()
    if owns_conn:
        conn.close()

    if updated:
        logger.info("model_specs corrections: %d rows updated", updated)
    return updated


def _is_electric_make_model(make: str, model: str) -> bool:
    """True for known BEV makes/models where 0 cylinders is correct."""
    make_u = (make or "").strip().upper()
    model_u = (model or "").strip().upper()
    if make_u == "TESLA":
        return True
    if make_u == "BMW" and re.search(r"\bI[0-9X]\b", model_u):
        return True
    if make_u == "HYUNDAI" and "IONIQ" in model_u:
        return True
    if make_u in ("RIVIAN", "LUCID", "POLESTAR", "NIO", "FISKER"):
        return True
    return False
