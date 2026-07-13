"""Single-car reads/writes and car-row parse helpers."""
import json
import logging
import re
import sqlite3
from typing import Any

from backend.db.repositories.base_repo import _placeholders, db_conn, get_conn
from backend.db.repositories.schema_repo import ensure_nhtsa_vpic_cache_table

_log = logging.getLogger(__name__)


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


def _parse_car_spin_frames(car_dict):
    """Ensure car_dict['spin_frames'] is a list (parse from JSON string if needed)."""
    if not car_dict:
        return
    s = car_dict.get("spin_frames")
    if isinstance(s, list):
        return
    if s is None or s == "":
        car_dict["spin_frames"] = []
        return
    try:
        parsed = json.loads(s) if isinstance(s, str) else []
        car_dict["spin_frames"] = parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        car_dict["spin_frames"] = []


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


def get_car_by_id(car_id, *, include_inactive: bool = True):
    try:
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
    except sqlite3.OperationalError as ex:
        # Uninitialized SQLite file (e.g. tests pointing DB_PATH at an empty db):
        # no cars table means no car. Anything else is a real error.
        if "no such table" not in str(ex).lower():
            raise
        row = None
    car = dict(row) if row else None
    if car:
        _parse_car_gallery(car)
        _parse_car_spin_frames(car)
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
        _parse_car_spin_frames(row)
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
        _parse_car_spin_frames(car)
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
