"""
Sidecar SQLite DB listing cars with gaps in the regular listing spec sheet (car detail col 2).

Rows are keyed by ``cars.id`` in the main inventory DB. The index is kept in sync on writes;
``ensure_incomplete_index_built`` bootstraps from a full scan when the meta row is absent.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from backend.db.inventory_db import DB_PATH as _INVENTORY_DB_PATH
from backend.db import inventory_pg
from backend.utils.listing_completeness import (
    PENDING_WINDOW_DAYS,
    is_pending_arrival,
    listing_missing_field_codes,
)

logger = logging.getLogger(__name__)

# Keep incomplete index beside the active inventory DB (same directory as ``DB_PATH`` targets).
DB_PATH = os.environ.get(
    "INCOMPLETE_LISTINGS_DB_PATH",
    os.path.join(os.path.dirname(_INVENTORY_DB_PATH), "incomplete_listings.db"),
)
_META_BOOTSTRAP_KEY = "index_bootstrap_v1"
_bootstrap_lock = threading.Lock()


def incomplete_index_db_mtime() -> float:
    """Modification time for cache invalidation (sidecar SQLite or 60s bucket on Postgres)."""
    if inventory_pg.is_inventory_postgres():
        return float(int(time.time()) // 60)
    try:
        return os.path.getmtime(DB_PATH)
    except OSError:
        return 0.0


def get_conn():
    """Incomplete-listings index: same PostgreSQL DB as inventory when configured; else sidecar SQLite."""
    if inventory_pg.is_inventory_postgres():
        from backend.db.inventory_db import get_conn as inv_get_conn

        return inv_get_conn()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


_PG_INC_SCHEMA_OK = False


def reset_incomplete_listings_schema_cache() -> None:
    """Tests / tooling: next ``_ensure_schema`` call runs full DDL again on Postgres."""
    global _PG_INC_SCHEMA_OK
    _PG_INC_SCHEMA_OK = False


def _ensure_schema(conn) -> None:
    """Idempotent DDL, mirroring ``inventory_pg.init_postgres_inventory``'s guard.

    On Postgres this used to re-run CREATE TABLE/INDEX on every call -- including
    the car-detail read path and every scanner write -- with no per-process skip
    flag and no ``lock_timeout``. ``CREATE INDEX`` takes a SHARE lock, so a running
    scan holding open transactions on this table could park every later reader and
    writer behind it (the same failure mode fixed for ``inventory_pg`` after the
    2026-07-30 incident). Skip after the first successful run per process, and set
    a short ``lock_timeout`` so a blocked call fails fast instead of queuing.
    """
    global _PG_INC_SCHEMA_OK
    is_pg = inventory_pg.is_inventory_postgres()
    if is_pg and _PG_INC_SCHEMA_OK:
        return
    if is_pg:
        from backend.db.schema_version import schema_is_current

        if schema_is_current():  # migrations/ owns these tables
            _PG_INC_SCHEMA_OK = True
            return
    cur = conn.cursor()
    if is_pg:
        try:
            cur.execute("SET lock_timeout = '3s'")
        except Exception:
            pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS incomplete_listings (
            car_id INTEGER PRIMARY KEY NOT NULL,
            vin TEXT NOT NULL,
            missing_fields_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            first_seen_at TEXT,
            last_scraped_at TEXT,
            scans_survived INTEGER NOT NULL DEFAULT 1,
            pending INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    _ensure_window_columns(cur, is_pg)
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
    if is_pg:
        _PG_INC_SCHEMA_OK = True


# N6.4: per-row new-arrival state (V026 owns these on Postgres).
_WINDOW_COLUMNS = (
    ("first_seen_at", "TEXT"),
    ("last_scraped_at", "TEXT"),
    ("scans_survived", "INTEGER NOT NULL DEFAULT 1"),
    ("pending", "INTEGER NOT NULL DEFAULT 0"),
)


def _ensure_window_columns(cur, is_pg: bool) -> None:
    """Add the N6.4 columns to an index table created before them (idempotent)."""
    if is_pg:
        for name, decl in _WINDOW_COLUMNS:
            cur.execute(f"ALTER TABLE incomplete_listings ADD COLUMN IF NOT EXISTS {name} {decl}")
        return
    cur.execute("PRAGMA table_info(incomplete_listings)")
    have = {r[1] for r in cur.fetchall()}
    for name, decl in _WINDOW_COLUMNS:
        if name not in have:
            cur.execute(f"ALTER TABLE incomplete_listings ADD COLUMN {name} {decl}")


def _clear_listings_caches_after_index_write() -> None:
    try:
        from backend.db.inventory_db import clear_inventory_listings_cache

        clear_inventory_listings_cache()
    except Exception:
        logger.debug("Could not clear listings cache after incomplete index write", exc_info=True)


def delete_incomplete_record(car_id: int) -> None:
    conn = get_conn()
    _ensure_schema(conn)
    conn.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
    conn.commit()
    conn.close()


def get_missing_field_codes_for_car_id(car_id: int) -> list[str]:
    """Fast read from incomplete index; empty list when the listing is complete."""
    conn = get_conn()
    _ensure_schema(conn)
    row = conn.execute(
        "SELECT missing_fields_json, COALESCE(pending, 0) FROM incomplete_listings WHERE car_id = ?",
        (int(car_id),),
    ).fetchone()
    conn.close()
    # A pending new arrival is waiting on the dealer, not incomplete (N6.4).
    if not row or row[1]:
        return []
    try:
        raw = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(c) for c in raw if c] if isinstance(raw, list) else []


def _ts_text(value) -> str | None:
    """A timestamp column as text for the index (``None`` stays ``None``)."""
    if value is None:
        return None
    s = value.isoformat() if hasattr(value, "isoformat") else str(value)
    return s.strip() or None


def _next_scans(prev, scraped: str | None) -> int:
    """Scans a gap has survived: +1 when the car was scraped again since the stored row."""
    if not prev:
        return 1
    n = int(prev[0] or 1)
    return n + 1 if scraped and scraped != prev[1] else n


def sync_incomplete_listing_for_car_id(car_id: int) -> None:
    from backend.db.inventory_db import get_car_by_id

    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    car = get_car_by_id(car_id, include_inactive=True)
    # Delisted cars (listing_active = 0) are invisible to every public query,
    # so they don't belong in the incomplete queue either.
    if not car or not int(car.get("listing_active") if car.get("listing_active") is not None else 1):
        cur.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
        conn.commit()
        conn.close()
        return
    missing = listing_missing_field_codes(car, for_public_filter=False)
    vin = str(car.get("vin") or "").strip() or "?"
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    if missing:
        cur.execute(
            "SELECT scans_survived, last_scraped_at FROM incomplete_listings WHERE car_id = ?",
            (car_id,),
        )
        prev = cur.fetchone()
        first_seen, scraped = _ts_text(car.get("first_seen_at")), _ts_text(car.get("scraped_at"))
        scans = _next_scans(prev, scraped)
        pending = 1 if is_pending_arrival(missing, first_seen, scraped, PENDING_WINDOW_DAYS) else 0
        cur.execute(
            """
            INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at,
                                             first_seen_at, last_scraped_at, scans_survived, pending)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(car_id) DO UPDATE SET
                vin = excluded.vin,
                missing_fields_json = excluded.missing_fields_json,
                updated_at = excluded.updated_at,
                first_seen_at = excluded.first_seen_at,
                last_scraped_at = excluded.last_scraped_at,
                scans_survived = excluded.scans_survived,
                pending = excluded.pending
            """,
            (car_id, vin, json.dumps(missing), now, first_seen, scraped, scans, pending),
        )
    else:
        cur.execute("DELETE FROM incomplete_listings WHERE car_id = ?", (car_id,))
    conn.commit()
    conn.close()


def rebuild_incomplete_listings_index() -> int:
    """Full rescan of inventory ``cars``; returns count of rows left in the incomplete index."""
    return fast_rebuild_incomplete_listings_index()


# Bump when listing_completeness / merge_verified_specs rules change: the fleet post step
# rebuilds the index whenever the stored value differs (N6.1).
INDEX_RULES_VERSION = "2026-10-n6.4"
_META_RULES_KEY = "rules_version"
_META_REBUILT_KEY = "last_rebuilt_at"
REBUILD_CHUNK = 20000
_REBUILD_LOCK_KEY = 7306001  # pg_try_advisory_lock key for the index rebuild
_STAGING = "incomplete_listings_staging"


def get_index_meta(key: str) -> str | None:
    conn = get_conn()
    try:
        _ensure_schema(conn)
        cur = conn.cursor()
        cur.execute("SELECT v FROM incomplete_listings_meta WHERE k = ?", (key,))
        row = cur.fetchone()
        return str(row[0]) if row else None
    finally:
        conn.close()


def _set_meta(cur, key: str, value: str) -> None:
    cur.execute(
        "INSERT INTO incomplete_listings_meta (k, v) VALUES (?, ?) "
        "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        (key, value),
    )


def index_needs_rebuild() -> bool:
    """True when the stored rules version differs from this code's."""
    return get_index_meta(_META_RULES_KEY) != INDEX_RULES_VERSION


def _try_rebuild_lock(conn) -> bool:
    """Session advisory lock on Postgres; SQLite is single-writer so always granted."""
    if not inventory_pg.is_inventory_postgres():
        return True
    cur = conn.cursor()
    cur.execute("SELECT pg_try_advisory_lock(?)", (_REBUILD_LOCK_KEY,))
    return bool(cur.fetchone()[0])


def _release_rebuild_lock(conn) -> None:
    if inventory_pg.is_inventory_postgres():
        conn.cursor().execute("SELECT pg_advisory_unlock(?)", (_REBUILD_LOCK_KEY,))


def _missing_rows(cars: list[dict], now: str, prior: dict[int, tuple] | None = None) -> list[tuple]:
    from backend.enrichment.knowledge_engine import merge_verified_specs, prime_vpic_cache

    rows = []
    # Reuse ONE Postgres connection for the read-only reference lookups fired per car
    # (EPA / catalog / vPIC / model-specs); per-car connects dominated 70k+ rebuilds.
    with inventory_pg.borrow_read_connection():
        # vPIC is keyed by VIN, so warm it per chunk with batched IN queries.
        prime_vpic_cache([c.get("vin") for c in cars])
        for car in cars:
            # The index only needs verified_specs; skip the heavy per-car enrichment.
            # include_extended_specs=False: display-only fields never feed gap detection.
            detail_ctx = {"verified_specs": merge_verified_specs(car, include_extended_specs=False)}
            missing = listing_missing_field_codes(car, for_public_filter=False, detail_ctx=detail_ctx)
            if missing:
                first_seen, scraped = _ts_text(car.get("first_seen_at")), _ts_text(car.get("scraped_at"))
                scans = _next_scans((prior or {}).get(int(car["id"])), scraped)
                pending = 1 if is_pending_arrival(missing, first_seen, scraped, PENDING_WINDOW_DAYS) else 0
                rows.append((car["id"], str(car.get("vin") or "?").strip(), json.dumps(missing), now,
                             first_seen, scraped, scans, pending))
    return rows


def rebuild_index_chunked(chunk: int = REBUILD_CHUNK, *, force: bool = False) -> dict:
    """Rebuild the index in id-ordered chunks of at most ``chunk`` cars.

    Each chunk is computed and written to a staging table in its own short transaction;
    one final transaction swaps staging into ``incomplete_listings`` so readers never see
    a half-built index (X5). A Postgres advisory lock keeps two rebuilds from running at
    once: the loser returns ``{"status": "locked"}`` without touching anything.
    Per-car syncs that write the live index during a rebuild are overwritten by the swap
    (the next rebuild or sync restores them). With ``force=False`` the rebuild is skipped when ``rules_version`` already matches.
    """
    from backend.db.inventory_db import get_conn as inv_get_conn

    if not force and not index_needs_rebuild():
        return {"status": "current", "rows": 0, "chunks": 0}
    conn_inc = get_conn()
    try:
        _ensure_schema(conn_inc)
        if not _try_rebuild_lock(conn_inc):
            return {"status": "locked", "rows": 0, "chunks": 0}
        try:
            cur = conn_inc.cursor()
            # Dropped and recreated so a staging table left by an older code version
            # (4 columns) cannot be reused.
            cur.execute(f"DROP TABLE IF EXISTS {_STAGING}")
            cur.execute(f"CREATE TABLE {_STAGING} (LIKE incomplete_listings)"
                        if inventory_pg.is_inventory_postgres() else
                        f"CREATE TABLE {_STAGING} (car_id INTEGER PRIMARY KEY NOT NULL, "
                        "vin TEXT NOT NULL, missing_fields_json TEXT NOT NULL, updated_at TEXT NOT NULL, "
                        "first_seen_at TEXT, last_scraped_at TEXT, "
                        "scans_survived INTEGER NOT NULL DEFAULT 1, pending INTEGER NOT NULL DEFAULT 0)")
            cur.execute(f"DELETE FROM {_STAGING}")
            conn_inc.commit()
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            last_id, n_chunks, n_rows, n_cars = 0, 0, 0, 0
            while True:
                t_chunk = time.time()
                inv = inv_get_conn()
                inv.row_factory = sqlite3.Row
                icur = inv.cursor()
                # Match the public queries: delisted rows never render, so never queue them.
                icur.execute(
                    "SELECT * FROM cars WHERE id > ? AND COALESCE(listing_active, 1) = 1 "
                    "ORDER BY id LIMIT ?",
                    (last_id, chunk),
                )
                cars = [dict(r) for r in icur.fetchall()]
                inv.close()
                if not cars:
                    break
                ids = [int(c["id"]) for c in cars]
                prior: dict[int, tuple] = {}
                for i in range(0, len(ids), 500):
                    part = ids[i : i + 500]
                    cur.execute(
                        "SELECT car_id, scans_survived, last_scraped_at FROM incomplete_listings "
                        f"WHERE car_id IN ({','.join('?' * len(part))})",
                        part,
                    )
                    prior.update({int(r[0]): (r[1], r[2]) for r in cur.fetchall()})
                rows = _missing_rows(cars, now, prior)
                if rows:
                    cur.executemany(
                        f"INSERT INTO {_STAGING} (car_id, vin, missing_fields_json, updated_at, "
                        "first_seen_at, last_scraped_at, scans_survived, pending) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        rows,
                    )
                conn_inc.commit()
                last_id = cars[-1]["id"]
                n_chunks += 1
                n_rows += len(rows)
                n_cars += len(cars)
                logger.info("rebuild_index_chunked: chunk %d (%d cars) in %.1fs",
                            n_chunks, len(cars), time.time() - t_chunk)
                if len(cars) < chunk:
                    break
            # One transaction: the swap.
            cur.execute(
                "DELETE FROM incomplete_listings"
            )
            cur.execute(
                "INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at, "
                "first_seen_at, last_scraped_at, scans_survived, pending) "
                "SELECT car_id, vin, missing_fields_json, updated_at, "
                f"first_seen_at, last_scraped_at, scans_survived, pending FROM {_STAGING}"
            )
            _set_meta(cur, _META_RULES_KEY, INDEX_RULES_VERSION)
            _set_meta(cur, _META_REBUILT_KEY, now)
            _set_meta(cur, _META_BOOTSTRAP_KEY, "1")
            cur.execute(f"DELETE FROM {_STAGING}")
            conn_inc.commit()
        except Exception:
            conn_inc.rollback()
            raise
        finally:
            _release_rebuild_lock(conn_inc)
    finally:
        conn_inc.close()
    logger.info("rebuild_index_chunked: %d incomplete / %d total in %d chunk(s)", n_rows, n_cars, n_chunks)
    _clear_listings_caches_after_index_write()
    return {"status": "rebuilt", "rows": n_rows, "cars": n_cars, "chunks": n_chunks}


def fast_rebuild_incomplete_listings_index() -> int:
    """
    Full resync of the incomplete_listings index using the same rules as
    :func:`sync_incomplete_listing_for_car_id` / the car detail page
    (:func:`listing_missing_field_codes`). Chunked, staged and locked
    (:func:`rebuild_index_chunked`).

    Returns the number of incomplete rows (the current count when another rebuild holds the lock).
    """
    res = rebuild_index_chunked(force=True)
    if res["status"] == "locked":
        conn = get_conn()
        try:
            _ensure_schema(conn)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM incomplete_listings")
            return int(cur.fetchone()[0])
        finally:
            conn.close()
    return int(res["rows"])


def _mark_bootstrapped() -> None:
    conn = get_conn()
    _ensure_schema(conn)
    conn.execute(
        "INSERT OR REPLACE INTO incomplete_listings_meta (k, v) VALUES (?, ?)",
        (_META_BOOTSTRAP_KEY, "1"),
    )
    conn.commit()
    conn.close()


def ensure_incomplete_index_built() -> None:
    with _bootstrap_lock:
        conn = get_conn()
        _ensure_schema(conn)
        cur = conn.cursor()
        cur.execute("SELECT v FROM incomplete_listings_meta WHERE k = ?", (_META_BOOTSTRAP_KEY,))
        row = cur.fetchone()
        conn.close()
        if row:
            return
        try:
            n = fast_rebuild_incomplete_listings_index()
            _mark_bootstrapped()
            logger.info("Built incomplete_listings index (%d row(s) flagged).", n)
        except Exception:
            logger.exception("Failed to build incomplete_listings index")


def get_incomplete_listings_count() -> int:
    """Count of ACTIVE cars in the incomplete index (cheap query for dev SSR).

    The index can still hold rows for delisted cars until the retirement-time
    delete lands (P5A.4), so a bare ``COUNT(*)`` over-counts; join to ``cars``
    and keep ``listing_active`` rows only.
    """
    ensure_incomplete_index_built()
    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    try:
        if inventory_pg.is_inventory_postgres():
            cur.execute(
                "SELECT COUNT(*) FROM incomplete_listings il "
                "JOIN cars c ON c.id = il.car_id WHERE COALESCE(c.listing_active, 1) = 1 "
                "AND COALESCE(il.pending, 0) = 0"
            )
            row = cur.fetchone()
            return max(0, int(row[0] if row else 0))
        cur.execute("SELECT car_id FROM incomplete_listings WHERE COALESCE(pending, 0) = 0")
        ids = [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    except (TypeError, ValueError, IndexError):
        return 0
    finally:
        conn.close()
    if not ids:
        return 0
    from backend.db.repositories.base_repo import db_conn

    total = 0
    with db_conn() as inv:
        for i in range(0, len(ids), 500):
            chunk = ids[i : i + 500]
            ph = ",".join("?" * len(chunk))
            r = inv.execute(
                f"SELECT COUNT(*) FROM cars WHERE id IN ({ph}) AND COALESCE(listing_active, 1) = 1",
                chunk,
            ).fetchone()
            total += int(r[0] if r else 0)
    return max(0, total)


def get_incomplete_car_id_set() -> set[int]:
    """Car ids flagged incomplete (O(1) membership for listings grid filtering)."""
    ensure_incomplete_index_built()
    conn = get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT car_id FROM incomplete_listings WHERE COALESCE(pending, 0) = 0")
    out = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    conn.close()
    return out


def get_pending_car_id_set() -> set[int]:
    """Car ids in the new-arrival window whose only gap is price/photos (N6.4, D-N13).

    Not incomplete: the grid shows them with a plain "details coming" line.
    """
    ensure_incomplete_index_built()
    conn = get_conn()
    try:
        _ensure_schema(conn)
        cur = conn.cursor()
        cur.execute("SELECT car_id FROM incomplete_listings WHERE COALESCE(pending, 0) = 1")
        return {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    finally:
        conn.close()


def is_car_pending(car_id: int) -> bool:
    """One-row read of the pending flag (car page)."""
    conn = get_conn()
    try:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT pending FROM incomplete_listings WHERE car_id = ?", (int(car_id),)
        ).fetchone()
        return bool(row and row[0])
    finally:
        conn.close()


def get_incomplete_cars_for_dev() -> list[dict]:
    """Cars referenced in the incomplete index, newest first, with ``incomplete_missing_fields``."""
    from backend.db.inventory_db import get_cars_by_ids

    ensure_incomplete_index_built()
    conn = get_conn()
    _ensure_schema(conn)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT car_id, missing_fields_json FROM incomplete_listings "
        "WHERE COALESCE(pending, 0) = 0 ORDER BY datetime(updated_at) DESC"
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return []
    ids = [int(r["car_id"]) for r in rows]
    fields_by_id = {int(r["car_id"]): json.loads(r["missing_fields_json"]) for r in rows}
    cars = get_cars_by_ids(ids, active_only=True)
    for c in cars:
        c["incomplete_missing_fields"] = fields_by_id.get(int(c["id"]), [])
    return cars
