"""The ``INSERT ... ON CONFLICT(vin) DO UPDATE`` that writes one ``cars`` row.

``UPSERT_CARS_SQL`` is the statement; :func:`upsert_params` binds a prepared
row (:func:`prepare_row`) in column order. The two trailing parameters drive
the VIN ownership guard's atomic backstop (the ``WHERE`` on the DO UPDATE).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from backend.utils.field_clean import compute_data_quality_score
from backend.utils.forced_induction import classify_forced_induction_from_car_row
from backend.utils.interior_color_buckets import interior_color_buckets_json

from backend.scanner.upsert import rows as _rows
from backend.scanner.upsert import serialize as _ser

# Same logger name as before the split so log routing and captures are unchanged.
logger = logging.getLogger("backend.scanner.database")

UPSERT_CARS_SQL = """
                INSERT INTO cars (
                    vin, title, year, make, model, trim, price, mileage,
                    image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                    fuel_type, cylinders, transmission, transmission_type, drivetrain,
                    exterior_color, interior_color, interior_color_buckets, stock_number, gallery, carfax_url, history_highlights, msrp,
                    dealership_registry_id,
                    source_url, body_style, engine_description, engine_l, condition, description, data_quality_score,
                    mpg_city, mpg_highway, is_cpo, model_full_raw,
                    packages,
                    listing_active, listing_removed_at, spec_source_json,
                    first_seen_at, last_price_change_at, forced_induction, price_provenance_json,
                    spin_frames, interior_pano
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vin) DO UPDATE SET
                    title=CASE
                        WHEN NULLIF(TRIM(excluded.title),'') IS NOT NULL AND excluded.title != 'Unknown vehicle'
                        THEN excluded.title
                        ELSE COALESCE(NULLIF(TRIM(cars.title),''), excluded.title)
                    END,
                    year=CASE WHEN IFNULL(excluded.year,0)!=0 THEN excluded.year ELSE COALESCE(cars.year,excluded.year) END,
                    make=COALESCE(NULLIF(TRIM(excluded.make),''), cars.make),
                    model=COALESCE(NULLIF(TRIM(excluded.model),''), cars.model),
                    trim=COALESCE(excluded.trim, cars.trim),
                    price=CASE WHEN COALESCE(excluded.price, 0) > 0 THEN excluded.price ELSE cars.price END,
                    -- feed odometer > 0 wins; an explicit 0 keeps a prior real reading
                    -- (else 0: new cars are 0 mi); NULL (no odometer) keeps a prior
                    -- real reading but replaces a stale default 0 with NULL (F12)
                    mileage=CASE WHEN IFNULL(excluded.mileage,0) > 0 THEN excluded.mileage
                                 WHEN excluded.mileage IS NOT NULL THEN COALESCE(NULLIF(cars.mileage,0), 0)
                                 ELSE NULLIF(cars.mileage,0) END,
                    image_url=CASE
                        WHEN excluded.image_url LIKE 'http%' THEN excluded.image_url
                        WHEN cars.image_url LIKE 'http%' THEN cars.image_url
                        ELSE excluded.image_url
                    END,
                    dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
                    dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
                    fuel_type=COALESCE(excluded.fuel_type, cars.fuel_type),
                    cylinders=COALESCE(excluded.cylinders, cars.cylinders),
                    transmission=COALESCE(excluded.transmission, cars.transmission),
                    transmission_type=COALESCE(excluded.transmission_type, cars.transmission_type),
                    drivetrain=COALESCE(excluded.drivetrain, cars.drivetrain),
                    exterior_color=COALESCE(NULLIF(TRIM(excluded.exterior_color), ''), cars.exterior_color),
                    interior_color=COALESCE(NULLIF(TRIM(excluded.interior_color), ''), cars.interior_color),
                    interior_color_buckets=CASE
                        WHEN NULLIF(TRIM(excluded.interior_color), '') IS NOT NULL THEN excluded.interior_color_buckets
                        ELSE cars.interior_color_buckets
                    END,
                    stock_number=COALESCE(NULLIF(excluded.stock_number, ''), cars.stock_number),
                    gallery=COALESCE(
                        NULLIF(NULLIF(TRIM(excluded.gallery), ''), '[]'),
                        cars.gallery
                    ),
                    -- Carfax link, history highlights and MSRP come from the VDP (or a
                    -- window sticker), never from the SRP card. An SRP-only refresh of
                    -- the same VIN therefore arrives with carfax_url=NULL,
                    -- history_highlights='[]' and msrp=NULL. Assigning excluded.* here
                    -- wiped all three on every such rescan, against this statement's
                    -- stated contract that an empty incoming value never overwrites a
                    -- stored one.
                    carfax_url=COALESCE(NULLIF(TRIM(excluded.carfax_url), ''), cars.carfax_url),
                    history_highlights=COALESCE(
                        NULLIF(NULLIF(TRIM(excluded.history_highlights), ''), '[]'),
                        cars.history_highlights
                    ),
                    msrp=COALESCE(excluded.msrp, cars.msrp),
                    dealership_registry_id=COALESCE(excluded.dealership_registry_id, cars.dealership_registry_id),
                    source_url=COALESCE(excluded.source_url, cars.source_url),
                    body_style=COALESCE(excluded.body_style, cars.body_style),
                    engine_description=COALESCE(excluded.engine_description, cars.engine_description),
                    engine_l=COALESCE(NULLIF(TRIM(excluded.engine_l), ''), cars.engine_l),
                    condition=COALESCE(NULLIF(TRIM(excluded.condition), ''), cars.condition),
                    description=COALESCE(excluded.description, cars.description),
                    -- Scored off the INCOMING payload, but every column this score
                    -- reads (title/year/make/model/trim/price/mileage/transmission/
                    -- drivetrain/fuel_type/colors/image/engine/mpg -- see
                    -- ``compute_data_quality_score``) is keep-if-nonempty above, so the
                    -- stored row's field set never shrinks through this statement.
                    -- Assigning excluded.* therefore let an SRP-only rescan drop the
                    -- score of a row that still holds every field it was scored on
                    -- (measured 95.37 -> 54.63), and ``hybrid_search`` ranks on this
                    -- column. Take the better of the two; a path that genuinely CLEARS
                    -- fields re-derives the score via refresh_car_data_quality_score.
                    data_quality_score=CASE
                        WHEN COALESCE(excluded.data_quality_score, 0) > COALESCE(cars.data_quality_score, 0)
                        THEN excluded.data_quality_score
                        ELSE cars.data_quality_score
                    END,
                    mpg_city=COALESCE(excluded.mpg_city, cars.mpg_city),
                    mpg_highway=COALESCE(excluded.mpg_highway, cars.mpg_highway),
                    is_cpo=COALESCE(excluded.is_cpo, cars.is_cpo),
                    model_full_raw=COALESCE(excluded.model_full_raw, cars.model_full_raw),
                    packages=COALESCE(NULLIF(TRIM(excluded.packages), ''), cars.packages),
                    listing_active=1,
                    listing_removed_at=NULL,
                    spec_source_json=CASE
                        WHEN excluded.spec_source_json IS NOT NULL AND length(trim(excluded.spec_source_json)) > 0
                        THEN excluded.spec_source_json
                        ELSE cars.spec_source_json
                    END,
                    first_seen_at=COALESCE(cars.first_seen_at, excluded.scraped_at),
                    -- Stamp only when the stored price ACTUALLY moves. The price
                    -- column above keeps its stored value when the incoming price is
                    -- missing (hidden price / "call for price" / a feed that dropped
                    -- the field), but the old condition compared
                    -- COALESCE(excluded.price, 0) and so read a missing price as a
                    -- change to 0 -- stamping "repriced today" on a row whose price
                    -- it had just decided not to touch. ``merchandising.py`` anchors
                    -- price aging on this column, so those rows read as permanently
                    -- just-repriced.
                    last_price_change_at=CASE
                        WHEN COALESCE(excluded.price, 0) > 0
                             AND COALESCE(cars.price, 0) != excluded.price
                        THEN excluded.scraped_at
                        ELSE COALESCE(cars.last_price_change_at, cars.first_seen_at, excluded.scraped_at)
                    END,
                    internal_notes=cars.internal_notes,
                    marked_for_review=cars.marked_for_review,
                    forced_induction=COALESCE(excluded.forced_induction, cars.forced_induction),
                    price_provenance_json=COALESCE(excluded.price_provenance_json, cars.price_provenance_json),
                    spin_frames=COALESCE(NULLIF(NULLIF(TRIM(excluded.spin_frames), ''), '[]'), cars.spin_frames),
                    interior_pano=COALESCE(NULLIF(TRIM(excluded.interior_pano), ''), cars.interior_pano)
                -- VIN ownership guard, atomic backstop for the prefetch check above
                -- (a concurrent shard may have claimed the VIN since): never move an
                -- active, fresh row to a different dealer_id.
                WHERE ? = 0
                   OR COALESCE(cars.dealer_id, '') = ''
                   OR cars.dealer_id = excluded.dealer_id
                   OR COALESCE(cars.listing_active, 1) != 1
                   OR cars.scraped_at IS NULL
                   OR cars.scraped_at < ?
                """


@dataclass
class PreparedRow:
    """One normalized vehicle with every derived value the INSERT binds."""

    v: dict
    vin: str
    title: str
    price: int | None
    mileage: int | None
    msrp: int | None
    image_url: Any
    gallery_json: str
    spin_frames_json: str | None
    interior_pano: str | None
    highlights_json: str
    data_quality_score: Any
    interior_buckets_json: Any
    spec_source_json: Any
    packages_json: str | None
    forced_induction: Any


def prepare_row(v: dict, prior_spec_src: str | None) -> PreparedRow | None:
    """Derive the bound values from a normalized row; None when it has no VIN."""
    vin = (v.get("vin") or "").strip()
    if not vin:
        return None
    title = _rows.row_title(v)
    price = _rows.coerce_price(v.get("price"))
    mileage = _rows.coerce_mileage(v.get("mileage"))
    msrp = _rows.coerce_msrp(v.get("msrp"))
    gallery_json = _ser.gallery_json(v.get("gallery"))
    spin_frames_json = _ser.spin_frames_json(v.get("spin_frames"))
    interior_pano = _rows.interior_pano_url(v.get("interior_pano"))
    highlights_json = _ser.history_highlights_json(v.get("history_highlights"))
    img = _rows.image_url_or_placeholder(v.get("image_url"))
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
    spec_src = _ser.spec_source_json(v, prior_spec_src)
    packages_json = _ser.packages_json(v.get("packages"))
    fi = v.get("forced_induction") or classify_forced_induction_from_car_row(v)
    return PreparedRow(
        v=v,
        vin=vin,
        title=title,
        price=price,
        mileage=mileage,
        msrp=msrp,
        image_url=img,
        gallery_json=gallery_json,
        spin_frames_json=spin_frames_json,
        interior_pano=interior_pano,
        highlights_json=highlights_json,
        data_quality_score=dq,
        interior_buckets_json=interior_buckets_json,
        spec_source_json=spec_src,
        packages_json=packages_json,
        forced_induction=fi,
    )


def fetch_prev_price_row(cursor, vin: str):
    """``(price, price_provenance_json)`` of the stored row, or None."""
    try:
        cursor.execute("SELECT price, price_provenance_json FROM cars WHERE vin = ?", (vin,))
        return cursor.fetchone()
    except Exception:
        logger.debug("upsert step fetch_prev_price_row failed for %s", vin, exc_info=True)
        return None


def upsert_params(
    row: PreparedRow,
    *,
    now: str,
    price_history_json: str | None,
    guard_on: bool,
    guard_cutoff_iso: str,
) -> tuple:
    """Bind parameters for :data:`UPSERT_CARS_SQL`, in column order."""
    v = row.v
    return (
        row.vin,
        row.title,
        v.get("year"),
        v.get("make") or "",
        v.get("model") or "",
        v.get("trim"),
        row.price,
        row.mileage,
        row.image_url,
        v.get("dealer_name") or "",
        v.get("dealer_url"),
        v.get("dealer_id") or "",
        now,
        v.get("fuel_type"),
        v.get("cylinders"),
        v.get("transmission"),
        v.get("transmission_type"),
        v.get("drivetrain"),
        v.get("exterior_color"),
        v.get("interior_color"),
        row.interior_buckets_json,
        v.get("stock_number") or "",
        row.gallery_json,
        v.get("carfax_url"),
        row.highlights_json,
        row.msrp,
        v.get("dealership_registry_id"),
        v.get("source_url"),
        v.get("body_style"),
        v.get("engine_description"),
        v.get("engine_l"),
        v.get("condition"),
        v.get("description"),
        row.data_quality_score,
        v.get("mpg_city"),
        v.get("mpg_highway"),
        v.get("is_cpo"),
        v.get("model_full_raw"),
        row.packages_json,
        1,
        None,
        row.spec_source_json,
        now,
        now,
        row.forced_induction,
        price_history_json,
        row.spin_frames_json,
        row.interior_pano,
        1 if guard_on else 0,
        guard_cutoff_iso,
    )


def trace_vin_readback(cursor, vin: str, v: dict) -> None:
    """``SCANNER_TRACE_VIN``: log the in-memory vs stored values of one VIN."""
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
