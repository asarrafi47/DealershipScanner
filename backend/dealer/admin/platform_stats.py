"""Platform-wide metrics for site-admin dashboard."""

from __future__ import annotations

import logging
from typing import Any

from backend.db.search_analytics_db import engagement_counts, usage_summary
from backend.dealer.admin import inventory_queries as invq

_log = logging.getLogger(__name__)


def _fetch_scalar(row: Any) -> Any:
    if row is None:
        return 0
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def inventory_scale() -> dict[str, int]:
    """Active/inactive cars and distinct dealers with scraped inventory."""
    from backend.db.inventory_db import get_conn

    out = {"active_cars": 0, "inactive_cars": 0, "dealers_in_inventory": 0}
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM cars WHERE COALESCE(listing_active, 1) = 1"
        )
        out["active_cars"] = int(_fetch_scalar(cur.fetchone()))
        cur.execute(
            "SELECT COUNT(*) FROM cars WHERE COALESCE(listing_active, 1) != 1"
        )
        out["inactive_cars"] = int(_fetch_scalar(cur.fetchone()))
        cur.execute(
            """
            SELECT COUNT(DISTINCT dealer_id) FROM cars
            WHERE dealer_id IS NOT NULL AND trim(dealer_id) != ''
              AND COALESCE(listing_active, 1) = 1
            """
        )
        out["dealers_in_inventory"] = int(_fetch_scalar(cur.fetchone()))
        conn.close()
    except Exception:
        _log.debug("inventory_scale failed", exc_info=True)
    return out


def registry_dealership_count() -> int:
    from backend.db.inventory_db import get_conn

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM dealerships
            WHERE COALESCE(is_active, 1) = 1
            """
        )
        n = int(_fetch_scalar(cur.fetchone()))
        conn.close()
        return n
    except Exception:
        _log.debug("registry_dealership_count failed", exc_info=True)
        return 0


def users_scale() -> dict[str, int]:
    from backend.db.users_db import get_conn

    out = {
        "total": 0,
        "active": 0,
        "suspended": 0,
        "verified": 0,
        "premium": 0,
        "new_7d": 0,
    }
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(users)")
        cols = {r[1] for r in cur.fetchall()}
        cur.execute("SELECT COUNT(*) FROM users")
        out["total"] = int(_fetch_scalar(cur.fetchone()))
        if "is_active" in cols:
            cur.execute("SELECT COUNT(*) FROM users WHERE COALESCE(is_active, 1) = 1")
            out["active"] = int(_fetch_scalar(cur.fetchone()))
            cur.execute("SELECT COUNT(*) FROM users WHERE COALESCE(is_active, 1) = 0")
            out["suspended"] = int(_fetch_scalar(cur.fetchone()))
        else:
            out["active"] = out["total"]
        if "email_verified_at" in cols:
            cur.execute(
                "SELECT COUNT(*) FROM users WHERE email_verified_at IS NOT NULL "
                "AND trim(email_verified_at) != ''"
            )
            out["verified"] = int(_fetch_scalar(cur.fetchone()))
        if "is_premium" in cols:
            cur.execute("SELECT COUNT(*) FROM users WHERE is_premium = 1")
            out["premium"] = int(_fetch_scalar(cur.fetchone()))
        if "created_at" in cols:
            cur.execute(
                "SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', '-7 days')"
            )
            out["new_7d"] = int(_fetch_scalar(cur.fetchone()))
        conn.close()
    except Exception:
        _log.debug("users_scale failed", exc_info=True)
    return out


def platform_health() -> dict[str, Any]:
    from backend.db.inventory_pg import is_inventory_postgres

    health: dict[str, Any] = {
        "web": "ok",
        "postgres": "offline",
        "active_jobs": 0,
        "failed_jobs_24h": 0,
        "catalog_dealers": 0,
        "last_scrape_at": None,
    }
    if not is_inventory_postgres():
        health["postgres"] = "misconfigured"
        return health
    try:
        from backend.scanner.job_queue import list_dealer_catalog, list_recent_jobs

        health["postgres"] = "ok"
        jobs = list_recent_jobs(limit=50)
        health["active_jobs"] = sum(
            1 for j in jobs if (j.get("status") or "") in ("queued", "running")
        )
        health["failed_jobs_24h"] = sum(
            1
            for j in jobs
            if (j.get("status") or "") == "failed"
            and (j.get("finished_at") or "") >= _iso_day_ago()
        )
        catalog = list_dealer_catalog(limit=500)
        health["catalog_dealers"] = len(catalog)
        dash = invq.dashboard_summary({"role": "admin"})
        if dash.get("top_dealers"):
            times = [
                d.get("last_scraped_at")
                for d in dash["top_dealers"]
                if d.get("last_scraped_at")
            ]
            if times:
                health["last_scrape_at"] = max(times)
    except Exception:
        _log.debug("platform_health jobs failed", exc_info=True)
        health["postgres"] = "error"
    return health


def _iso_day_ago() -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d")


def build_platform_dashboard(*, usage_audience: str = "all", usage_days: int = 30) -> dict[str, Any]:
    inv = inventory_scale()
    users = users_scale()
    health = platform_health()
    return {
        "health": health,
        "inventory": inv,
        "registry_dealerships": registry_dealership_count(),
        "catalog_dealerships": health.get("catalog_dealers", 0),
        "users": users,
        "engagement": engagement_counts(days=usage_days),
        "usage": usage_summary(days=usage_days, audience=usage_audience),
        "usage_audience": usage_audience,
        "usage_days": usage_days,
        "merchandising": invq.dashboard_summary({"role": "admin"}),
    }
