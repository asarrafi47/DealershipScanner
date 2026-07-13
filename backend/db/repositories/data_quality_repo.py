"""Listing completeness / data quality + the incomplete-index snapshot cache.

Also owns :func:`_listings_cache_token` (used by the snapshot cache here and by the
listings caches in ``listings_repo``), keeping the dependency graph acyclic.
"""
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any

from backend.db.inventory_pg import is_inventory_postgres
from backend.db.repositories.base_repo import _resolve_db_path, db_conn
from backend.db.repositories.cars_repo import get_car_by_id
from backend.utils.field_clean import compute_data_quality_score

_log = logging.getLogger(__name__)


def _facade_db_conn():
    """Honor tests that monkeypatch ``backend.db.inventory_db.db_conn`` (the facade)."""
    facade = sys.modules.get("backend.db.inventory_db")
    if facade is not None:
        fn = getattr(facade, "db_conn", None)
        if fn is not None:
            return fn
    return db_conn


_incomplete_listings_check_fn = None
_incomplete_index_snapshot_cache: tuple[tuple[float, float], "_IncompleteIndexSnapshot"] | None = None


@dataclass(frozen=True)
class _IncompleteIndexSnapshot:
    """Incomplete car ids for listings filters; ``per_row_fallback`` when the index is unavailable."""

    ids: frozenset[int]
    per_row_fallback: bool = False


def _car_id_int(car: dict) -> int:
    try:
        return int(car.get("id") or 0)
    except (TypeError, ValueError):
        return 0


def _car_is_publicly_incomplete(car: dict, snapshot: _IncompleteIndexSnapshot) -> bool:
    """True when a row should be treated as incomplete for listings (fail closed on bad ids)."""
    if snapshot.per_row_fallback:
        return is_car_incomplete(car)
    cid = _car_id_int(car)
    if cid <= 0:
        return is_car_incomplete(car)
    return cid in snapshot.ids


def _incomplete_index_snapshot_for_listings() -> _IncompleteIndexSnapshot:
    """Cached incomplete index for grid filter + ``public_incomplete`` pill."""
    global _incomplete_index_snapshot_cache
    token = _listings_cache_token()
    if _incomplete_index_snapshot_cache is not None and _incomplete_index_snapshot_cache[0] == token:
        return _incomplete_index_snapshot_cache[1]
    try:
        from backend.db.incomplete_listings_db import get_incomplete_car_id_set

        snap = _IncompleteIndexSnapshot(ids=frozenset(get_incomplete_car_id_set()))
    except Exception:
        _log.exception("incomplete_listings index unavailable; using per-row completeness fallback")
        snap = _IncompleteIndexSnapshot(ids=frozenset(), per_row_fallback=True)
    _incomplete_index_snapshot_cache = (token, snap)
    return snap


def _incomplete_car_ids_for_listings() -> frozenset[int]:
    """Legacy helper: id set only (empty when per-row fallback is active)."""
    return _incomplete_index_snapshot_for_listings().ids


def is_car_incomplete(car: dict) -> bool:
    """True when the row should be hidden from public listings (subset of spec sheet)."""
    global _incomplete_listings_check_fn
    if _incomplete_listings_check_fn is None:
        from backend.utils.listing_completeness import is_car_incomplete_for_public_listings

        _incomplete_listings_check_fn = is_car_incomplete_for_public_listings
    return _incomplete_listings_check_fn(car)


def _filter_public_listings_cars(cars: list[dict], *, include_incomplete: bool) -> list[dict]:
    """Drop incomplete rows using the cached id set (O(n), same semantics as listings grid)."""
    if include_incomplete or not cars:
        return cars
    snapshot = _incomplete_index_snapshot_for_listings()
    out: list[dict] = []
    for c in cars:
        if _car_is_publicly_incomplete(c, snapshot):
            continue
        out.append(c)
    return out


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
    from backend.db.incomplete_listings_db import DB_PATH as _INC_DB_PATH

    attach_inc = not is_inventory_postgres()
    sql = """
            SELECT
                c.dealer_name,
                c.dealer_id,
                COUNT(*) as total_cars,
                SUM(CASE WHEN il.car_id IS NOT NULL THEN 1 ELSE 0 END) as incomplete_count,
                SUM(CASE WHEN c.marked_for_review = 1 THEN 1 ELSE 0 END) as flagged_count,
                ROUND(CAST(AVG(COALESCE(c.data_quality_score, 0)) AS numeric), 2) as avg_quality_score,
                SUM(CASE WHEN c.price IS NULL OR c.price = 0 THEN 1 ELSE 0 END) as no_price_count
            FROM cars c
            LEFT JOIN {inc_table} il ON il.car_id = c.id
            WHERE c.dealer_name IS NOT NULL AND TRIM(c.dealer_name) != ''
            GROUP BY c.dealer_id, c.dealer_name
            HAVING SUM(CASE WHEN il.car_id IS NOT NULL THEN 1 ELSE 0 END) > 0
                OR SUM(CASE WHEN c.marked_for_review = 1 THEN 1 ELSE 0 END) > 0
            ORDER BY incomplete_count DESC, flagged_count DESC
            LIMIT ?
        """
    inc_table = "incomplete_listings" if is_inventory_postgres() else "inc_idx.incomplete_listings"
    with _facade_db_conn()() as conn:
        cursor = conn.cursor()
        if attach_inc:
            cursor.execute("ATTACH DATABASE ? AS inc_idx", (_INC_DB_PATH,))
        try:
            cursor.execute(sql.format(inc_table=inc_table), (limit,))
            rows = cursor.fetchall()
        finally:
            if attach_inc:
                try:
                    cursor.execute("DETACH DATABASE inc_idx")
                except sqlite3.Error:
                    pass

        stats = []
        for row in rows:
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


def _listings_cache_token() -> tuple[float, float]:
    """Invalidate listings caches when inventory or incomplete index mtimes change."""
    if is_inventory_postgres():
        import time

        bucket = float(int(time.time()) // 60)
        return (bucket, bucket)
    inv_mtime = 0.0
    inc_mtime = 0.0
    try:
        inv_mtime = os.path.getmtime(_resolve_db_path())
    except OSError:
        pass
    try:
        from backend.db.incomplete_listings_db import incomplete_index_db_mtime

        inc_mtime = incomplete_index_db_mtime()
    except Exception:
        pass
    return (inv_mtime, inc_mtime)


def clear_incomplete_snapshot_cache() -> None:
    """Reset the cached incomplete-index snapshot (listings cache invalidation)."""
    global _incomplete_index_snapshot_cache
    _incomplete_index_snapshot_cache = None
