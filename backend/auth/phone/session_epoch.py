"""Sign out everywhere: the per-user ``valid_after`` mark (card S10, DESIGN.md section 9).

Every session whose ``auth_at`` is older than the user's ``valid_after`` is void. The row lives in
``public.auth_session_epochs`` (migration V031, repository ``auth_phone_repo``).

Two doors only, so a later shared cache (card R4) has one place to read and one place to forget:

* :func:`get_valid_after` -- the one read. An in-process cache keeps each answer 5 seconds for
  admin and dealer sessions and 30 seconds for shoppers.
* :func:`revoke_user_sessions` -- the one write. It drops this process's cached answer for the user.

When the lookup fails, :func:`get_valid_after` raises :class:`EpochLookupFailed`; the session hook
refuses admin and dealer sessions (fail closed) and lets shoppers through (fail open). Two cases
are not failures because no row can exist: Postgres is not configured (offline tests, local
SQLite-only runs) and the table is not there yet (the owner has not applied V031; every write
fails the same way, so nothing has ever been revoked).

Rate-limit keys never use ``valid_after``.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

_log = logging.getLogger("auth_session")

CACHE_SECONDS_STRICT = 5.0  # admin, /dev and dealer or staff sessions
CACHE_SECONDS_SHOPPER = 30.0
_UNDEFINED_TABLE = "42P01"

# Reasons written to the row (plain words, no personal data).
REASON_SIGN_OUT_EVERYWHERE = "sign_out_everywhere"
REASON_PASSWORD_CHANGE = "password_change"
REASON_PASSWORD_RESET = "password_reset"
REASON_SUSPEND = "suspend"
REASON_DELETE = "delete"

_lock = threading.Lock()
# user_id -> (stored_at monotonic, valid_after epoch seconds or None)
_cache: dict[int, tuple[float, float | None]] = {}
# Bumped on every forget, so a read that started before a write cannot cache the old answer.
_generation = 0


class EpochLookupFailed(Exception):
    """The valid_after lookup could not be answered (database down, timeout, bad row)."""


def _monotonic() -> float:
    return time.monotonic()


def _store_configured() -> bool:
    from backend.db.inventory_pg import is_inventory_postgres

    return is_inventory_postgres()


def _is_missing_table(exc: BaseException) -> bool:
    return getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE


def _is_dsn_missing(exc: BaseException) -> bool:
    """No inventory address at all: the same as "Postgres is not configured", not an outage."""
    from backend.db.connect import InventoryDsnMissing

    return isinstance(exc, InventoryDsnMissing)


def _read_row(user_id: int) -> float | None:
    from backend.db.repositories import auth_phone_repo

    value = auth_phone_repo.sessions_valid_after(int(user_id))
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def get_valid_after(user_id: int, *, strict: bool) -> float | None:
    """``valid_after`` for ``user_id`` as epoch seconds, or None when no row exists.

    ``strict`` picks the cache window: True for admin, /dev and dealer sessions (5 s), False for
    shoppers (30 s). Raises :class:`EpochLookupFailed` when the database cannot answer.
    """
    uid = int(user_id)
    ttl = CACHE_SECONDS_STRICT if strict else CACHE_SECONDS_SHOPPER
    now = _monotonic()
    with _lock:
        hit = _cache.get(uid)
        gen = _generation
    if hit is not None and now - hit[0] < ttl:
        return hit[1]
    if not _store_configured():
        return None
    try:
        value = _read_row(uid)
    except Exception as exc:
        if _is_missing_table(exc) or _is_dsn_missing(exc):
            value = None
        else:
            _log.warning("event=session_epoch_lookup_failed err=%s", type(exc).__name__)
            raise EpochLookupFailed() from None
    with _lock:
        if gen == _generation:
            _cache[uid] = (now, value)
    return value


def forget_cached(user_id: int) -> None:
    """Drop this process's cached answer for one user (the write path calls it)."""
    global _generation
    with _lock:
        _generation += 1
        _cache.pop(int(user_id), None)


def clear_cache() -> None:
    """Tests only: start from an empty cache."""
    with _lock:
        _cache.clear()


def revoke_user_sessions(user_id: int, *, reason: str, now: datetime | None = None) -> float | None:
    """Void every session of ``user_id`` issued before now. The ONE write of ``valid_after``.

    Returns the stored ``valid_after`` as epoch seconds, or None when it could not be written
    (no Postgres configured, table not applied yet, database error). Never raises: callers are
    admin actions and account forms that must finish; a suspended or deleted user is refused by
    the session hook anyway because the users row says so. A caller that must not go on without
    the write (sign out everywhere) checks for None.
    """
    uid = int(user_id)
    when = now or datetime.now(timezone.utc)
    forget_cached(uid)
    if not _store_configured():
        return None
    from backend.db.repositories import auth_phone_repo

    try:
        stored = auth_phone_repo.set_sessions_valid_after(uid, now=when, reason=reason)
    except Exception as exc:
        _log.warning(
            "event=session_epoch_write_failed reason=%s err=%s missing_table=%d",
            reason,
            type(exc).__name__,
            int(_is_missing_table(exc)),
        )
        return None
    finally:
        forget_cached(uid)
    if stored.tzinfo is None:
        stored = stored.replace(tzinfo=timezone.utc)
    return stored.timestamp()
