"""Per-user search history (account profile -> "Recent searches").

Mirrors saved_searches_repo.py: a thin CRUD layer over the inventory DB
connection (SQLite in tests, Postgres in prod via backend.db.inventory_compat).
Rows are written by the listings search endpoints for signed-in users only;
``filters`` is the same cleaned listings query-param object saved searches
store, so a row can be re-run (``/listings?...``) or promoted to a saved search
as-is. Schema: migrations/V022__user_search_history.sql (+ the runtime CREATE
TABLE copies in inventory_pg.py / schema_repo.py).

Bounds: identical filters within DEDUPE_WINDOW_SECONDS of the user's newest row
are not re-recorded, and each user is pruned to MAX_HISTORY_PER_USER rows after
every insert.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from backend.db.repositories.base_repo import db_conn

DEDUPE_WINDOW_SECONDS = 10 * 60
MAX_HISTORY_PER_USER = 100
DEFAULT_LIST_LIMIT = 20
_MAX_FILTERS_JSON_LEN = 4000
_MAX_QUERY_TEXT_LEN = 300


def canonical_filters_json(filters) -> str:
    """Stable JSON for ``filters`` (sorted keys) so equal filter sets compare equal."""
    return json.dumps(filters or {}, sort_keys=True, separators=(",", ":"), default=str)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def format_created_at(dt: datetime) -> str:
    """ISO-8601 UTC string the table stores (``2026-09-28T14:03:22+00:00``)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_created_at(value) -> datetime | None:
    """Parse a stored created_at back to an aware UTC datetime.

    Accepts the application's ISO form, SQLite's ``datetime('now')``
    (``YYYY-MM-DD HH:MM:SS``, naive UTC) and Postgres' ``CURRENT_TIMESTAMP::text``
    (``... +00`` offsets, optional fractional seconds). Returns None when unparseable.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        # Postgres emits "+00" / "-05" style offsets that fromisoformat rejects on
        # older interpreters; pad to "+00:00".
        if len(text) >= 3 and text[-3] in "+-" and text[-2:].isdigit():
            text = text + ":00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            try:
                dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _clean_query_text(query_text) -> str | None:
    text = str(query_text or "").strip()
    if not text:
        return None
    return text[:_MAX_QUERY_TEXT_LEN]


def _clean_result_count(result_count) -> int | None:
    if result_count is None:
        return None
    try:
        n = int(result_count)
    except (TypeError, ValueError):
        return None
    return max(0, n)


def _row_get(row, key: str, idx: int):
    return row[key] if isinstance(row, dict) else row[idx]


def record_search_history(
    user_id: int,
    filters: dict | None,
    *,
    query_text: str | None = None,
    result_count: int | None = None,
    now: datetime | None = None,
) -> int | None:
    """Append a history row for ``user_id``.

    Returns the row id. When the user's most recent row has identical filters and
    is younger than DEDUPE_WINDOW_SECONDS, nothing is written and that row's id is
    returned. Raises ValueError when the filters payload is too large. Callers on
    the request path wrap this in try/except: a history write must never fail a
    search.
    """
    filters_json = canonical_filters_json(filters)
    if len(filters_json) > _MAX_FILTERS_JSON_LEN:
        raise ValueError("filters payload too large")
    if filters_json == "{}" and not _clean_query_text(query_text):
        # Nothing to remember: a bare /listings visit is browsing, not a search.
        return None
    now_dt = now or utc_now()
    q_text = _clean_query_text(query_text)
    count = _clean_result_count(result_count)
    uid = int(user_id)

    with db_conn() as conn:
        latest = conn.execute(
            "SELECT id, filters_json, created_at FROM user_search_history "
            "WHERE user_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
            (uid,),
        ).fetchone()
        if latest is not None and _row_get(latest, "filters_json", 1) == filters_json:
            prev_at = parse_created_at(_row_get(latest, "created_at", 2))
            if prev_at is not None and abs((now_dt - prev_at).total_seconds()) < DEDUPE_WINDOW_SECONDS:
                return int(_row_get(latest, "id", 0))

        cur = conn.execute(
            "INSERT INTO user_search_history "
            "(user_id, filters_json, query_text, result_count, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (uid, filters_json, q_text, count, format_created_at(now_dt)),
        )
        row_id = getattr(cur, "lastrowid", None)
        if not row_id:
            # Postgres path (psycopg cursors don't populate lastrowid).
            row = conn.execute(
                "SELECT id FROM user_search_history WHERE user_id = ? ORDER BY id DESC LIMIT 1",
                (uid,),
            ).fetchone()
            row_id = _row_get(row, "id", 0) if row is not None else None
        _prune_conn(conn, uid, MAX_HISTORY_PER_USER)
        conn.commit()
    return int(row_id) if row_id else None


def list_search_history(user_id: int, limit: int = DEFAULT_LIST_LIMIT) -> list[dict]:
    """Newest-first history rows: [{id, filters, query_text, result_count, created_at}, ...]."""
    try:
        lim = int(limit)
    except (TypeError, ValueError):
        lim = DEFAULT_LIST_LIMIT
    lim = max(1, min(lim, MAX_HISTORY_PER_USER))
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, filters_json, query_text, result_count, created_at "
            "FROM user_search_history WHERE user_id = ? "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            (int(user_id), lim),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        filters_json = _row_get(r, "filters_json", 1)
        try:
            filters = json.loads(filters_json) if filters_json else {}
        except (TypeError, ValueError):
            filters = {}
        if not isinstance(filters, dict):
            filters = {}
        count = _row_get(r, "result_count", 3)
        out.append(
            {
                "id": int(_row_get(r, "id", 0)),
                "filters": filters,
                "query_text": _row_get(r, "query_text", 2) or None,
                "result_count": int(count) if count is not None else None,
                "created_at": _row_get(r, "created_at", 4),
            }
        )
    return out


def delete_search_history_entry(user_id: int, entry_id: int) -> bool:
    """Delete one history row owned by ``user_id``. Returns True if a row was removed."""
    with db_conn() as conn:
        cur = conn.execute(
            "DELETE FROM user_search_history WHERE id = ? AND user_id = ?",
            (int(entry_id), int(user_id)),
        )
        conn.commit()
        deleted = getattr(cur, "rowcount", None)
    if deleted is not None and deleted >= 0:
        return deleted > 0
    return not _entry_exists(user_id, entry_id)


def clear_search_history(user_id: int) -> int:
    """Delete every history row for ``user_id``. Returns the number removed (best effort)."""
    with db_conn() as conn:
        before = conn.execute(
            "SELECT COUNT(*) FROM user_search_history WHERE user_id = ?", (int(user_id),)
        ).fetchone()
        n = int(_first(before) or 0)
        conn.execute("DELETE FROM user_search_history WHERE user_id = ?", (int(user_id),))
        conn.commit()
    return n


def prune_search_history(user_id: int, keep: int = MAX_HISTORY_PER_USER) -> int:
    """Drop everything but the newest ``keep`` rows for ``user_id``. Returns rows removed."""
    with db_conn() as conn:
        removed = _prune_conn(conn, int(user_id), int(keep))
        conn.commit()
    return removed


def _prune_conn(conn, uid: int, keep: int) -> int:
    keep = max(0, int(keep))
    before = conn.execute(
        "SELECT COUNT(*) FROM user_search_history WHERE user_id = ?", (uid,)
    ).fetchone()
    n = int(_first(before) or 0)
    if n <= keep:
        return 0
    # Same SQL on both engines: keep the newest ``keep`` ids, delete the rest.
    conn.execute(
        "DELETE FROM user_search_history WHERE user_id = ? AND id NOT IN ("
        "SELECT id FROM user_search_history WHERE user_id = ? "
        "ORDER BY created_at DESC, id DESC LIMIT ?)",
        (uid, uid, keep),
    )
    return n - keep


def _entry_exists(user_id: int, entry_id: int) -> bool:
    with db_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM user_search_history WHERE id = ? AND user_id = ? LIMIT 1",
            (int(entry_id), int(user_id)),
        ).fetchone()
    return row is not None


def _first(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]
