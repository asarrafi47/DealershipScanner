"""Sign out everywhere: the per-user ``valid_after`` mark (card S10, DESIGN.md section 9).

Every session whose ``auth_at`` is older than the user's ``valid_after`` is void. The row lives in
``public.auth_session_epochs`` (migration V031, repository ``auth_phone_repo``).

Two doors only, so the shared cache (card R4) has one place to read and one place to forget:

* :func:`get_valid_after` -- the one read. Lookup order: Redis (when ``REDIS_URL`` is set), the
  in-process cache, Postgres. Each answer is kept 5 seconds for admin and dealer sessions and
  30 seconds for shoppers, in both caches. While Redis answers, a Redis miss goes straight to
  Postgres (a revoke in another process deletes the shared key, and this process's own copy
  could still hold the old answer); the in-process cache is used when Redis is unset, skipped by
  its breaker or failing, exactly as before card R4. A Redis error is only a cache miss.
* :func:`revoke_user_sessions` -- the one write. It deletes the user's Redis key before and after
  the write (after a successful write the key is set to the new value, so a read racing the
  write cannot put the old answer back) and drops this process's cached answer. When Redis
  fails during a write, this process stops trusting Redis for that user for 30 seconds, the
  longest any key can live.

Redis key: ``sc:sva:<user id>`` (no phone number, code or secret), value ``<valid_after or ->|
<stored at>``, TTL 5 or 30 seconds (31 after a write). A read stamps its entry with the time
before it asked Postgres and does not share an answer that took 5 seconds or more. Postgres stays the only authority: a flush or an eviction
only means the next request asks Postgres.

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

from backend.utils import redis_client

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
REASON_LOGOUT = "logout"


# Row keys in ``auth_session_epochs`` (BIGINT, no foreign key, so no migration):
#   app users            id                         > 0
#   /dev operators       -id                        [-2**62, -1]
#   /dev/manifest console sessions (by nonce)       [-2**63, -2**62 - 1]
# The ranges are disjoint by construction, so an operator key never equals a console key.
_OPERATOR_ID_MAX = 2**62
_CONSOLE_KEY_SPAN = 2**62


def dev_operator_epoch_id(admin_user_id: int) -> int:
    """The row key of a /dev operator (``admin_users`` id): the NEGATIVE of its id."""
    aid = abs(int(admin_user_id))
    if not 0 < aid <= _OPERATOR_ID_MAX:
        raise ValueError("operator id out of range")
    return -aid


def console_epoch_id(nonce: str) -> int:
    """The row key of one /dev/manifest console session (a random nonce in its cookie), below every
    operator key."""
    import hashlib

    digest = int(hashlib.sha256(("dev_console:" + str(nonce)).encode("utf-8")).hexdigest()[:16], 16)
    return -_OPERATOR_ID_MAX - 1 - (digest % _CONSOLE_KEY_SPAN)


_lock = threading.Lock()
# user_id -> (stored_at monotonic, valid_after epoch seconds or None)
_cache: dict[int, tuple[float, float | None]] = {}
# Bumped on every forget, so a read that started before a write cannot cache the old answer.
_generation = 0
# user_id -> monotonic deadline: a write whose Redis delete failed; skip Redis for that user.
_redis_skip_until: dict[int, float] = {}

_REDIS_HIT = "hit"
_REDIS_MISS = "miss"  # Redis answered and has no fresh value: ask Postgres
_REDIS_OFF = "off"  # unset, breaker open or an error: the pre-R4 path (in-process cache, Postgres)
_NONE_MARK = "-"


class EpochLookupFailed(Exception):
    """The valid_after lookup could not be answered (database down, timeout, bad row)."""


def _monotonic() -> float:
    return time.monotonic()


def _wall() -> float:
    return time.time()


def _redis_key(uid: int) -> str:
    return redis_client.key("sva", str(int(uid)))


def _redis_skipped(uid: int, now: float) -> bool:
    with _lock:
        until = _redis_skip_until.get(uid)
        if until is None:
            return False
        if until <= now:
            _redis_skip_until.pop(uid, None)
            return False
        return True


def _redis_read(uid: int, ttl: float, now: float) -> tuple[str, float | None]:
    """(state, value). Never raises: any Redis trouble is ``_REDIS_OFF``."""
    if _redis_skipped(uid, now):
        return _REDIS_OFF, None
    client = redis_client.get_client()
    if client is None:
        return _REDIS_OFF, None
    try:
        raw = client.get(_redis_key(uid))
    except Exception as exc:
        redis_client.record_failure(exc)
        return _REDIS_OFF, None
    redis_client.record_success()
    if raw is None:
        return _REDIS_MISS, None
    try:
        text = raw.decode("ascii") if isinstance(raw, bytes) else str(raw)
        value_text, stored_text = text.split("|", 1)
        stored = float(stored_text)
        value = None if value_text == _NONE_MARK else float(value_text)
    except (ValueError, UnicodeDecodeError):
        return _REDIS_MISS, None  # a value we did not write: ask Postgres
    age = _wall() - stored
    if age < 0 or age >= ttl:
        return _REDIS_MISS, None  # a shopper-length entry read by a strict session, or clock skew
    return _REDIS_HIT, value


def _encode(value: float | None, stamped_at: float) -> str:
    return "%s|%.3f" % (_NONE_MARK if value is None else repr(float(value)), stamped_at)


def _redis_store(
    uid: int, value: float | None, ttl: float, *, overwrite: bool, stamped_at: float | None = None
) -> bool:
    """Put an answer in Redis. ``overwrite=False`` (a read) never replaces a newer write, and a
    read stamps its entry with the time BEFORE it asked Postgres, so its age is never understated.
    Returns False when Redis was configured but failed."""
    client = redis_client.get_client()
    if client is None:
        return not redis_client.configured()  # unset: nothing to do; breaker open: not stored
    stamp = _wall() if stamped_at is None else stamped_at
    try:
        if overwrite:
            client.set(_redis_key(uid), _encode(value, stamp), ex=max(1, int(ttl)))
        else:
            client.set(_redis_key(uid), _encode(value, stamp), ex=max(1, int(ttl)), nx=True)
    except Exception as exc:
        redis_client.record_failure(exc)
        return False
    redis_client.record_success()
    return True


def _redis_forget(uid: int) -> bool:
    """Delete the shared key. True when deleted or Redis is unset; False when it could not be."""
    if not redis_client.configured():
        return True
    client = redis_client.get_client()
    if client is None:  # breaker open: the key may still be there
        return False
    try:
        client.delete(_redis_key(uid))
    except Exception as exc:
        redis_client.record_failure(exc)
        return False
    redis_client.record_success()
    return True


def _distrust_redis(uid: int) -> None:
    with _lock:
        _redis_skip_until[uid] = _monotonic() + CACHE_SECONDS_SHOPPER + 1.0


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
    state, shared = _REDIS_OFF, None
    if redis_client.configured():
        if not _store_configured():
            return None
        state, shared = _redis_read(uid, ttl, now)
        if state == _REDIS_HIT:
            return shared
    if state == _REDIS_OFF and hit is not None and now - hit[0] < ttl:
        return hit[1]
    if not _store_configured():
        return None
    read_started = _wall()
    try:
        value = _read_row(uid)
    except Exception as exc:
        if _is_missing_table(exc) or _is_dsn_missing(exc):
            value = None
        else:
            _log.warning("event=session_epoch_lookup_failed err=%s", type(exc).__name__)
            raise EpochLookupFailed() from None
    with _lock:
        fresh = gen == _generation
        if fresh:
            _cache[uid] = (now, value)
    # A read slower than the strict window is not shared: a revoke may have landed (and its
    # protective key expired) while it ran, and another process's write is invisible here.
    if fresh and state == _REDIS_MISS and _wall() - read_started < CACHE_SECONDS_STRICT:
        _redis_store(uid, value, ttl, overwrite=False, stamped_at=read_started)
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
        _redis_skip_until.clear()


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
    redis_ok = _redis_forget(uid)
    if not _store_configured():
        if not redis_ok:
            _distrust_redis(uid)
        return None
    from backend.db.repositories import auth_phone_repo

    stored = None
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
        if stored is None:
            redis_ok = _redis_forget(uid) and redis_ok
        if not redis_ok:
            _distrust_redis(uid)
    if stored.tzinfo is None:
        stored = stored.replace(tzinfo=timezone.utc)
    value = stored.timestamp()
    # Write the new answer over anything a racing read put back (a read only ever adds with NX).
    # Kept for the longest read window (31 s) so no read that began before the write can add its
    # older answer with NX; strict readers still ignore it after 5 s by its stored time.
    if redis_ok and not _redis_store(uid, value, CACHE_SECONDS_SHOPPER + 1, overwrite=True):
        _redis_forget(uid)
        _distrust_redis(uid)
    return value
