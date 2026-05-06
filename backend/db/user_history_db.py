"""Per-user car view history and simple recommendation queries (stored in users.db)."""
from __future__ import annotations

import logging

from backend.db.users_sqlite import get_users_conn

_log = logging.getLogger(__name__)

# Create table on first import so it exists even before init_users_db runs.
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
        conn.commit()
        conn.close()
    except Exception:
        _log.debug("car_view_history bootstrap failed", exc_info=True)

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
