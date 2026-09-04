"""Saved searches (FEATURE_SAVED_SEARCHES) — persisted listings filter sets.

Mirrors saved_cars_repo.py: a thin CRUD layer over the inventory DB connection
(SQLite in tests, Postgres in prod via backend.db.inventory_compat). No
notification/alerting cron reads last_notified_at yet (see migrations/
V018__saved_searches.sql) — this module only supports create/list/delete.
"""
import json

from backend.db.repositories.base_repo import db_conn

# Filters are stored as opaque JSON (whatever query params the listings page
# had at save time) but bounded so one saved search can't bloat the table.
_MAX_FILTERS_JSON_LEN = 4000


def create_saved_search(user_id: int, filters: dict) -> int:
    """Store a saved search; returns its new id."""
    filters_json = json.dumps(filters or {}, separators=(",", ":"))
    if len(filters_json) > _MAX_FILTERS_JSON_LEN:
        raise ValueError("filters payload too large")
    with db_conn() as conn:
        cur = conn.execute(
            "INSERT INTO saved_searches (user_id, filters_json) VALUES (?, ?)",
            (int(user_id), filters_json),
        )
        conn.commit()
        row_id = getattr(cur, "lastrowid", None)
        if row_id:
            return int(row_id)
        # Postgres path (psycopg cursors don't populate lastrowid) — fall back
        # to the most recently created row for this user.
        row = conn.execute(
            "SELECT id FROM saved_searches WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (int(user_id),),
        ).fetchone()
    if not row:
        raise RuntimeError("saved search insert did not return an id")
    return int(row[0] if not isinstance(row, dict) else row["id"])


def list_saved_searches(user_id: int) -> list[dict]:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, filters_json, created_at, last_notified_at "
            "FROM saved_searches WHERE user_id = ? ORDER BY created_at DESC, id DESC",
            (int(user_id),),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        if isinstance(r, dict):
            rid, filters_json, created_at, last_notified_at = (
                r["id"], r["filters_json"], r["created_at"], r["last_notified_at"]
            )
        else:
            rid, filters_json, created_at, last_notified_at = r
        try:
            filters = json.loads(filters_json) if filters_json else {}
        except (TypeError, ValueError):
            filters = {}
        out.append(
            {
                "id": int(rid),
                "filters": filters,
                "created_at": created_at,
                "last_notified_at": last_notified_at,
            }
        )
    return out


def delete_saved_search(user_id: int, search_id: int) -> bool:
    """Delete a saved search owned by ``user_id``. Returns True if a row was removed."""
    with db_conn() as conn:
        cur = conn.execute(
            "DELETE FROM saved_searches WHERE id = ? AND user_id = ?",
            (int(search_id), int(user_id)),
        )
        conn.commit()
        deleted = getattr(cur, "rowcount", None)
    if deleted is not None:
        return deleted > 0
    # Backends whose cursor doesn't report rowcount reliably: re-check existence.
    return not _saved_search_exists(user_id, search_id)


def _saved_search_exists(user_id: int, search_id: int) -> bool:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM saved_searches WHERE id = ? AND user_id = ? LIMIT 1",
            (int(search_id), int(user_id)),
        ).fetchone()
    return row is not None
