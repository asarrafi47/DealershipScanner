"""Health + readiness checks for DealershipScanner.

GET /api/health — liveness: cheap, no dependency access, always 200.
GET /api/ready  — readiness: per-dependency checks, each wrapped in its own
                  try/except so one broken dep never masks the others.
                  Returns 200 only when every *required* check is "ok",
                  else 503. Optional deps (pgvector, redis) that are not
                  configured report "skipped" and never gate readiness;
                  configured-but-broken optional deps report
                  "error: <ExceptionClassName>" (class name only — no
                  message text, so no DSNs/secrets leak) and stay non-gating.

The legacy bare ``/health`` route in backend/routes/site_misc.py is kept
untouched; ``/api/health`` is additive and returns the same shape.
"""

from __future__ import annotations

import os

from flask import Blueprint, jsonify

from backend.config import Config
from backend.routes.site_misc import _app_version

bp = Blueprint("health_api", __name__, url_prefix="/api")

# Only the inventory DB gates readiness; pgvector and redis are optional.
REQUIRED_CHECKS = ("db",)

# Lazily-created module-level redis client (created on first /api/ready call
# that has REDIS_URL configured, reused after). Env is fixed per process, so
# a stale URL in the cache is acceptable.
_redis = None


def _redis_client():
    global _redis
    if _redis is None:
        import redis

        _redis = redis.Redis.from_url(
            Config.redis_url(),
            socket_timeout=2,
            socket_connect_timeout=2,
        )
    return _redis


def _check_inventory_db() -> str:
    """Required: inventory DB (SQLite or Postgres behind inventory_compat)."""
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    return "ok"


def _check_pgvector() -> str | None:
    """Optional pgvector probe; ``None`` => unconfigured (caller records "skipped").

    Gated on ``PGVECTOR_URL`` specifically — *not* on
    ``pgvector_service.pgvector_configured()``, which also treats a bare
    ``DATABASE_URL`` (this project's plain inventory-Postgres variable, whose
    server may not ship the ``vector`` extension) as configured. Probing that
    server on every readiness poll would perpetually report an error for a
    dependency the operator never opted into.
    """
    from backend.vector import pgvector_service

    if not (os.environ.get("PGVECTOR_URL") or "").strip():
        return None
    conn = pgvector_service._connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
    finally:
        conn.close()
    return "ok"


@bp.get("/health")
def api_health():
    """Liveness: never touches a dependency; must not flap when a DB is down."""
    return jsonify(status="ok", version=_app_version())


@bp.get("/ready")
def api_ready():
    checks: dict[str, str] = {}

    # Required: inventory DB.
    try:
        checks["db"] = _check_inventory_db()
    except Exception as e:  # noqa: BLE001
        checks["db"] = f"error: {e.__class__.__name__}"

    # Optional: pgvector (PGVECTOR_URL). Skipped when unset.
    try:
        checks["pgvector"] = _check_pgvector() or "skipped"
    except Exception as e:  # noqa: BLE001
        checks["pgvector"] = f"error: {e.__class__.__name__}"

    # Optional: redis (REDIS_URL). Skipped when unset.
    if not Config.redis_url():
        checks["redis"] = "skipped"
    else:
        try:
            _redis_client().ping()
            checks["redis"] = "ok"
        except Exception as e:  # noqa: BLE001
            checks["redis"] = f"error: {e.__class__.__name__}"

    ok = all(checks.get(k) == "ok" for k in REQUIRED_CHECKS)
    return jsonify(ready=ok, checks=checks), (200 if ok else 503)
