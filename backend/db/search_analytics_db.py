"""Search and usage analytics (users.db) — aggregates for site-admin dashboard."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db.users_sqlite import get_users_conn

_log = logging.getLogger(__name__)

_AUDIENCE_ALL = "all"
_AUDIENCE_AUTH = "authenticated"
_AUDIENCE_ANON = "anonymous"


def _bootstrap() -> None:
    try:
        conn = get_users_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS search_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_at   TEXT NOT NULL DEFAULT (datetime('now')),
                user_id       INTEGER,
                session_key   TEXT,
                source        TEXT NOT NULL,
                query_text    TEXT,
                filters_json  TEXT NOT NULL DEFAULT '{}',
                result_count  INTEGER NOT NULL DEFAULT 0,
                geo_zip       TEXT,
                geo_state     TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_search_events_time "
            "ON search_events(occurred_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_search_events_user "
            "ON search_events(user_id, occurred_at DESC)"
        )
        conn.commit()
        conn.close()
    except Exception:
        _log.debug("search analytics bootstrap failed", exc_info=True)


_bootstrap()


def ensure_search_analytics_table() -> None:
    _bootstrap()


def _geo_state_for_zip(zip_code: str | None) -> str | None:
    z = (zip_code or "").strip()
    if not z or len(z) < 3:
        return None
    try:
        from backend.db.geo import us_postal_meta_for_zip

        meta = us_postal_meta_for_zip(z[:5])
        if meta and meta.get("state_code"):
            return str(meta["state_code"]).strip().upper() or None
    except Exception:
        pass
    return None


def _normalize_filters(filters: dict[str, Any] | None) -> dict[str, Any]:
    if not filters:
        return {}
    out: dict[str, Any] = {}
    for k, v in filters.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            vals = [str(x).strip() for x in v if str(x).strip()]
            if vals:
                out[k] = vals if len(vals) > 1 else vals[0]
        elif isinstance(v, (int, float, bool)):
            out[k] = v
        else:
            s = str(v).strip()
            if s:
                out[k] = s
    return out


def analytics_session_key(session_obj: object) -> str:
    """Stable per-browser key stored in Flask session (no PII)."""
    key = "analytics_session_key"
    existing = getattr(session_obj, "get", lambda _k, _d=None: None)(key)
    if existing:
        return str(existing)
    new_key = uuid.uuid4().hex
    if hasattr(session_obj, "__setitem__"):
        session_obj[key] = new_key
    return new_key


def record_search_event(
    *,
    source: str,
    query_text: str | None = None,
    filters: dict[str, Any] | None = None,
    result_count: int = 0,
    user_id: int | None = None,
    session_key: str | None = None,
    geo_zip: str | None = None,
) -> None:
    """Best-effort insert; never raises to callers."""
    src = (source or "unknown").strip().lower()[:32]
    if not src:
        return
    q = (query_text or "").strip()
    if len(q) > 500:
        q = q[:500]
    filt = _normalize_filters(filters)
    z = (geo_zip or filt.get("zip_code") or "")
    if isinstance(z, str):
        z = z.strip()[:10] or None
    else:
        z = str(z).strip()[:10] if z else None
    state = _geo_state_for_zip(z) if z else None
    try:
        conn = get_users_conn()
        conn.execute(
            """
            INSERT INTO search_events (
                user_id, session_key, source, query_text, filters_json,
                result_count, geo_zip, geo_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id) if user_id else None,
                (session_key or "")[:64] or None,
                src,
                q or None,
                json.dumps(filt, separators=(",", ":"), default=str),
                max(0, int(result_count)),
                z,
                state,
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        _log.debug("record_search_event failed", exc_info=True)


def _audience_clause(audience: str) -> tuple[str, list[Any]]:
    a = (audience or _AUDIENCE_ALL).strip().lower()
    if a == _AUDIENCE_AUTH:
        return "user_id IS NOT NULL", []
    if a == _AUDIENCE_ANON:
        return "user_id IS NULL", []
    return "1", []


def _since_sql(days: int) -> tuple[str, list[Any]]:
    d = max(1, min(int(days), 365))
    since = (datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
    return "occurred_at >= ?", [since]


def usage_summary(*, days: int = 30, audience: str = _AUDIENCE_ALL) -> dict[str, Any]:
    """Rollups for site-admin dashboard (aggregates only)."""
    aud_sql, aud_params = _audience_clause(audience)
    since_sql, since_params = _since_sql(days)
    base_where = f"WHERE {since_sql} AND ({aud_sql})"
    params = since_params + aud_params
    out: dict[str, Any] = {
        "days": days,
        "audience": audience,
        "total_searches": 0,
        "zero_result_searches": 0,
        "by_source": {},
        "top_makes": [],
        "top_states": [],
        "top_queries": [],
        "logged_in_searches": 0,
        "anonymous_searches": 0,
    }
    try:
        conn = get_users_conn()
        row = conn.execute(
            f"SELECT COUNT(*) FROM search_events {base_where}",
            tuple(params),
        ).fetchone()
        out["total_searches"] = int(row[0] if row else 0)

        row_z = conn.execute(
            f"SELECT COUNT(*) FROM search_events {base_where} AND result_count = 0",
            tuple(params),
        ).fetchone()
        out["zero_result_searches"] = int(row_z[0] if row_z else 0)

        if audience == _AUDIENCE_ALL:
            row_in = conn.execute(
                f"SELECT COUNT(*) FROM search_events {base_where} AND user_id IS NOT NULL",
                tuple(params),
            ).fetchone()
            row_an = conn.execute(
                f"SELECT COUNT(*) FROM search_events {base_where} AND user_id IS NULL",
                tuple(params),
            ).fetchone()
            out["logged_in_searches"] = int(row_in[0] if row_in else 0)
            out["anonymous_searches"] = int(row_an[0] if row_an else 0)

        src_rows = conn.execute(
            f"""
            SELECT source, COUNT(*) AS n FROM search_events
            {base_where}
            GROUP BY source ORDER BY n DESC LIMIT 8
            """,
            tuple(params),
        ).fetchall()
        out["by_source"] = {str(r[0]): int(r[1]) for r in src_rows}

        st_rows = conn.execute(
            f"""
            SELECT geo_state, COUNT(*) AS n FROM search_events
            {base_where} AND geo_state IS NOT NULL AND trim(geo_state) != ''
            GROUP BY geo_state ORDER BY n DESC LIMIT 10
            """,
            tuple(params),
        ).fetchall()
        out["top_states"] = [{"state": str(r[0]), "count": int(r[1])} for r in st_rows]

        # Top makes / queries from stored JSON (SQLite json_extract)
        make_rows = conn.execute(
            f"""
            SELECT json_extract(filters_json, '$.make') AS mk, COUNT(*) AS n
            FROM search_events
            {base_where}
              AND json_extract(filters_json, '$.make') IS NOT NULL
              AND trim(json_extract(filters_json, '$.make')) != ''
            GROUP BY mk ORDER BY n DESC LIMIT 10
            """,
            tuple(params),
        ).fetchall()
        out["top_makes"] = [{"make": str(r[0]), "count": int(r[1])} for r in make_rows]

        q_rows = conn.execute(
            f"""
            SELECT query_text, COUNT(*) AS n FROM search_events
            {base_where}
              AND query_text IS NOT NULL AND trim(query_text) != ''
            GROUP BY lower(trim(query_text)) ORDER BY n DESC LIMIT 10
            """,
            tuple(params),
        ).fetchall()
        out["top_queries"] = [{"query": str(r[0]), "count": int(r[1])} for r in q_rows]

        conn.close()
    except Exception:
        _log.debug("usage_summary failed", exc_info=True)
    return out


def engagement_counts(*, days: int = 30) -> dict[str, int]:
    """VDP views / compares from user history tables."""
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, min(days, 365)))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    out = {"vdp_views": 0, "compare_events": 0, "saved_cars": 0}
    try:
        conn = get_users_conn()
        rv = conn.execute(
            "SELECT COUNT(*) FROM car_view_history WHERE viewed_at >= ?",
            (since,),
        ).fetchone()
        out["vdp_views"] = int(rv[0] if rv else 0)
        rc = conn.execute(
            "SELECT COUNT(*) FROM car_compare_history WHERE compared_at >= ?",
            (since,),
        ).fetchone()
        out["compare_events"] = int(rc[0] if rc else 0)
        try:
            from backend.db.inventory_db import get_conn as inv_conn

            ic = inv_conn()
            rs = ic.execute("SELECT COUNT(*) FROM saved_cars").fetchone()
            out["saved_cars"] = int(rs[0] if rs else 0)
            ic.close()
        except Exception:
            pass
        conn.close()
    except Exception:
        _log.debug("engagement_counts failed", exc_info=True)
    return out
