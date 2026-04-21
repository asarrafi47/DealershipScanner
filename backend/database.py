"""
Database layer for scanner: connection to inventory.db and vehicle upsert.
"""
import json
import logging
import os
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
    return count
