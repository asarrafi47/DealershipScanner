"""Per-user car view history, compare history, and recommendation queries (users.db)."""
from __future__ import annotations

import logging

from backend.db.users_sqlite import get_users_conn

_log = logging.getLogger(__name__)


def _bootstrap() -> None:
    try:
        conn = get_users_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS car_view_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER NOT NULL,
                car_id    INTEGER NOT NULL,
                viewed_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(user_id, car_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cvh_user_time "
            "ON car_view_history(user_id, viewed_at DESC)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS car_compare_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                car_id      INTEGER NOT NULL,
                compared_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(user_id, car_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cch_user_time "
            "ON car_compare_history(user_id, compared_at DESC)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS compare_sessions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                car_ids     TEXT NOT NULL,
                compared_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_compare_sessions_user_time "
            "ON compare_sessions(user_id, compared_at DESC)"
        )
        conn.commit()
        conn.close()
    except Exception:
        _log.debug("user history bootstrap failed", exc_info=True)


_bootstrap()


def ensure_car_history_table() -> None:
    _bootstrap()


def record_car_view(user_id: int, car_id: int) -> None:
    conn = get_users_conn()
    try:
        conn.execute(
            """
            INSERT INTO car_view_history (user_id, car_id, viewed_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(user_id, car_id) DO UPDATE SET viewed_at = excluded.viewed_at
            """,
            (int(user_id), int(car_id)),
        )
        conn.commit()
    except Exception:
        _log.debug("record_car_view failed", exc_info=True)
    finally:
        conn.close()


def get_recent_viewed_car_ids(user_id: int, limit: int = 30) -> list[int]:
    conn = get_users_conn()
    try:
        rows = conn.execute(
            "SELECT car_id FROM car_view_history "
            "WHERE user_id = ? ORDER BY viewed_at DESC LIMIT ?",
            (int(user_id), int(limit)),
        ).fetchall()
        return [int(r[0]) for r in rows]
    finally:
        conn.close()


def count_viewed_cars(user_id: int) -> int:
    conn = get_users_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM car_view_history WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def _normalize_car_ids(car_ids: list[int]) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for raw in car_ids:
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            continue
        if cid <= 0 or cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
    return out


def record_compare_session(user_id: int, car_ids: list[int]) -> None:
    """Persist a compare visit and bump per-car compare timestamps."""
    ids = _normalize_car_ids(car_ids)
    if not ids:
        return
    uid = int(user_id)
    conn = get_users_conn()
    try:
        for cid in ids:
            conn.execute(
                """
                INSERT INTO car_compare_history (user_id, car_id, compared_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(user_id, car_id) DO UPDATE SET compared_at = excluded.compared_at
                """,
                (uid, cid),
            )
        if len(ids) >= 2:
            conn.execute(
                """
                INSERT INTO compare_sessions (user_id, car_ids, compared_at)
                VALUES (?, ?, datetime('now'))
                """,
                (uid, ",".join(str(i) for i in ids)),
            )
        conn.commit()
    except Exception:
        _log.debug("record_compare_session failed", exc_info=True)
    finally:
        conn.close()


def get_recent_compared_car_ids(user_id: int, limit: int = 30) -> list[int]:
    conn = get_users_conn()
    try:
        rows = conn.execute(
            "SELECT car_id FROM car_compare_history "
            "WHERE user_id = ? ORDER BY compared_at DESC LIMIT ?",
            (int(user_id), int(limit)),
        ).fetchall()
        return [int(r[0]) for r in rows]
    finally:
        conn.close()


def count_compared_cars(user_id: int) -> int:
    conn = get_users_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM car_compare_history WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()
