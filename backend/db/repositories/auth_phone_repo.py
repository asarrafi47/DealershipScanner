"""Phone sign-in tables in Postgres (card S5, docs/auth_sms/DESIGN.md section 4.2, migration V031).

Plain values in, plain values out: no Flask, no vendor calls, no logging of any number, code or
hash. Time is always passed in (``now``) so tests use a fake clock, and every write that must be
single-use is decided by the database (``UPDATE ... WHERE status = 'open' RETURNING``), never by
a read followed by a write.

Every function takes an optional ``conn``. Without one it opens its own connection, commits or
rolls back, and closes it. With one it still commits (or rolls back on error) so a caller's
connection never stays inside an aborted transaction.

What is NOT here (card S6 owns it): the order of checks, the limit numbers, the pools, and the
decision to send. ``reserve_send`` gives S6 the one transaction those rules run in.
"""

from __future__ import annotations

import functools
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.auth.phone.provider import LineInfo

CHALLENGE_PURPOSES = ("login", "link", "claim", "step_up", "change", "seller_verify")
CHALLENGE_ENDS = ("approved", "failed", "expired", "cancelled")
POOLS = ("known", "new", "resend", "seller")
CHANNELS = ("sms", "call")
SEND_OUTCOMES = (
    "pending",
    "sent",
    "unavailable",
    "provider_error",
    "blocked_turnstile",
    "blocked_voip",
    "blocked_country",
    "blocked_limit",
    "blocked_cap",
)
# Outcomes that never count against a person's per-phone, per-IP or per-device limits (card S6 (e)).
LIMIT_FREE_OUTCOMES = ("unavailable", "blocked_cap")

# Retention (DESIGN.md section 16)
CHALLENGE_RETENTION_DAYS = 7
SEND_RETENTION_DAYS = 30
WRONG_TRY_RETENTION_HOURS = 24
LOOKUP_CACHE_DAYS = 30

SEND_CAP_LOCK = "auth_phone_send_cap"  # pg_advisory_xact_lock(hashtext(...)) key of the send-cap check

_SEND_FILTERS = (
    "phone_hash",
    "ip_hash",
    "device_hash",
    "ip_prefix_hash",
    "pool",
    "purpose",
    "channel",
    "has_device_cookie",
    "country",
)


class IdentityTaken(Exception):
    """The number or the account already has an identity (the unique index refused the row)."""


class AuthPhoneDbError(Exception):
    """A database error with every value removed (rule 8 of TASKS.md).

    psycopg's own message repeats the failing row ("Failing row contains (...)"), which would put a
    number and its hash into a log or an error report. This carries only the SQLSTATE code and the
    constraint name.
    """

    def __init__(self, sqlstate: str | None, constraint: str | None = None) -> None:
        super().__init__(sqlstate, constraint)
        self.sqlstate = sqlstate
        self.constraint = constraint

    def __str__(self) -> str:
        return f"auth_phone database error {self.sqlstate} ({self.constraint or 'no constraint'})"


def _is_db_error(exc: BaseException) -> bool:
    return type(exc).__module__.split(".")[0] == "psycopg" and hasattr(exc, "sqlstate")


def _clean_errors(*, unique_is_taken: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Turn a driver error into :class:`AuthPhoneDbError` (or :class:`IdentityTaken` for a unique
    violation when ``unique_is_taken``). The new error is raised after the handler has ended, so
    the driver's error is not even chained to it as context."""

    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def inner(*args: Any, **kwargs: Any) -> Any:
            clean: Exception | None = None
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                if not _is_db_error(exc):
                    raise
                if unique_is_taken and exc.sqlstate == "23505":
                    clean = IdentityTaken()
                else:
                    diag = getattr(exc, "diag", None)
                    clean = AuthPhoneDbError(exc.sqlstate, getattr(diag, "constraint_name", None))
            raise clean

        return inner

    return wrap


# ---------------------------------------------------------------- data types


@dataclass(frozen=True)
class Challenge:
    id: int
    public_id: str
    phone_hash: str
    phone_e164: str | None = field(repr=False)
    purpose: str
    user_id: int | None
    session_nonce: str
    provider: str
    provider_ref: str | None
    attempts: int
    sends: int
    status: str
    created_at: datetime
    expires_at: datetime
    last_send_at: datetime
    finished_at: datetime | None


_CHALLENGE_COLS = (
    "id, public_id, phone_hash, phone_e164, purpose, user_id, session_nonce, provider, provider_ref, "
    "attempts, sends, status, created_at, expires_at, last_send_at, finished_at"
)


@dataclass(frozen=True)
class Identity:
    id: int
    user_id: int | None
    phone_e164: str = field(repr=False)
    phone_hash: str
    country: str
    line_type: str | None
    verified_at: datetime
    last_login_at: datetime | None
    state: str
    dormant_user_id: int | None
    detached_at: datetime | None
    consent_at: datetime
    consent_text_version: str
    created_at: datetime


_IDENTITY_COLS = (
    "id, user_id, phone_e164, phone_hash, country, line_type, verified_at, last_login_at, state, "
    "dormant_user_id, detached_at, consent_at, consent_text_version, created_at"
)


@dataclass(frozen=True)
class SendRow:
    """One send row to insert. Hashes only: the raw number never reaches this table."""

    phone_hash: str
    ip_hash: str
    ip_prefix_hash: str
    country: str
    purpose: str
    pool: str
    has_device_cookie: bool
    device_hash: str | None = None
    channel: str = "sms"
    est_cost_cents: Decimal | int = 0
    lookup_cost_cents: Decimal | int = 0
    outcome: str = "pending"


@dataclass(frozen=True)
class WrongTries:
    tries: int
    window_start: datetime | None
    locked_until: datetime | None
    locked: bool


@dataclass(frozen=True)
class SendReservation:
    send_id: int
    outcome: str


# ---------------------------------------------------------------- connection


def _connect():
    from backend.db.connect import connect

    return connect()


@contextmanager
def _tx(conn: Any) -> Iterator[Any]:
    """One transaction: commit on success, rollback on error, close only a connection we opened."""
    own = conn is None
    c = conn or _connect()
    try:
        yield c
        c.commit()
    except BaseException:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


def _utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _check(value: str, allowed: tuple[str, ...], what: str) -> None:
    if value not in allowed:
        raise ValueError(f"unknown {what}: {value!r}")


# ---------------------------------------------------------------- challenges


def new_public_id() -> str:
    return secrets.token_urlsafe(18)


@_clean_errors()
def create_challenge(
    *,
    phone_e164: str,
    phone_hash: str,
    purpose: str,
    session_nonce: str,
    provider: str,
    now: datetime,
    ttl_s: int,
    user_id: int | None = None,
    provider_ref: str | None = None,
    conn: Any = None,
) -> Challenge:
    """A new open challenge with a random ``public_id``. No code is stored (the provider holds it)."""
    _check(purpose, CHALLENGE_PURPOSES, "purpose")
    if not session_nonce:
        raise ValueError("session_nonce is required")
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "INSERT INTO public.auth_phone_challenges (public_id, phone_hash, phone_e164, purpose, user_id, "
            "session_nonce, provider, provider_ref, created_at, expires_at, last_send_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING " + _CHALLENGE_COLS,
            (
                new_public_id(),
                phone_hash,
                phone_e164,
                purpose,
                user_id,
                session_nonce,
                provider,
                provider_ref,
                now,
                now + timedelta(seconds=int(ttl_s)),
                now,
            ),
        ).fetchone()
    return Challenge(*row)


@_clean_errors()
def fetch_challenge(public_id: str, session_nonce: str, *, conn: Any = None) -> Challenge | None:
    """The challenge when both ``public_id`` and this browser's nonce match; otherwise None."""
    if not public_id or not session_nonce:
        return None
    with _tx(conn) as c:
        row = c.execute(
            "SELECT " + _CHALLENGE_COLS + " FROM public.auth_phone_challenges WHERE public_id = %s",
            (public_id,),
        ).fetchone()
    if row is None:
        return None
    ch = Challenge(*row)
    if not secrets.compare_digest(ch.session_nonce.encode(), session_nonce.encode()):
        return None
    return ch


@_clean_errors()
def reserve_attempt(
    public_id: str, session_nonce: str, *, now: datetime, max_attempts: int = 5, conn: Any = None
) -> int | None:
    """Take one check attempt BEFORE the vendor call; the new count, or None when none is left
    (closed, expired, other browser, or ``max_attempts`` already used)."""
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_challenges SET attempts = attempts + 1 "
            "WHERE public_id = %s AND session_nonce = %s AND status = 'open' AND attempts < %s "
            "AND expires_at > %s RETURNING attempts",
            (public_id, session_nonce, int(max_attempts), _utc(now)),
        ).fetchone()
    return None if row is None else int(row[0])


@_clean_errors()
def refund_attempt(public_id: str, session_nonce: str, *, conn: Any = None) -> bool:
    """Give an attempt back (only for a network error or a throttle; card S6 (c))."""
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_challenges SET attempts = attempts - 1 "
            "WHERE public_id = %s AND session_nonce = %s AND status = 'open' AND attempts > 0 RETURNING id",
            (public_id, session_nonce),
        ).fetchone()
    return row is not None


@_clean_errors()
def close_approved(public_id: str, session_nonce: str, *, now: datetime, conn: Any = None) -> Challenge | None:
    """Close an open, unexpired challenge as approved, exactly once.

    The returned row carries the number as it was (the stored copy is cleared by the same
    statement). Two concurrent callers: one gets the row, the other gets None.
    """
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_challenges AS ch SET status = 'approved', finished_at = %s, phone_e164 = NULL "
            "FROM (SELECT id, phone_e164 AS old_phone FROM public.auth_phone_challenges "
            "      WHERE public_id = %s FOR UPDATE) AS o "
            "WHERE ch.id = o.id AND ch.session_nonce = %s AND ch.status = 'open' AND ch.expires_at > %s "
            "RETURNING ch.id, ch.public_id, ch.phone_hash, o.old_phone, ch.purpose, ch.user_id, ch.session_nonce, "
            "ch.provider, ch.provider_ref, ch.attempts, ch.sends, ch.status, ch.created_at, ch.expires_at, "
            "ch.last_send_at, ch.finished_at",
            (now, public_id, session_nonce, now),
        ).fetchone()
    return None if row is None else Challenge(*row)


@_clean_errors()
def finish_challenge(
    public_id: str, status: str, *, now: datetime, session_nonce: str | None = None, conn: Any = None
) -> bool:
    """End an open challenge as failed, expired or cancelled and clear its number. False if not open."""
    _check(status, ("failed", "expired", "cancelled"), "end status")
    sql = (
        "UPDATE public.auth_phone_challenges SET status = %s, finished_at = %s, phone_e164 = NULL "
        "WHERE public_id = %s AND status = 'open'"
    )
    params: list[Any] = [status, _utc(now), public_id]
    if session_nonce is not None:
        sql += " AND session_nonce = %s"
        params.append(session_nonce)
    with _tx(conn) as c:
        row = c.execute(sql + " RETURNING id", params).fetchone()
    return row is not None


@_clean_errors()
def note_resend(
    public_id: str,
    session_nonce: str,
    *,
    now: datetime,
    min_wait_s: int,
    ttl_s: int,
    provider_ref: str | None = None,
    conn: Any = None,
) -> int | None:
    """Count one more send on an open challenge if ``min_wait_s`` has passed since the last one;
    the code life restarts. The new send count, or None (too soon, closed or other browser)."""
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_challenges SET sends = sends + 1, last_send_at = %s, expires_at = %s, "
            "provider_ref = COALESCE(%s, provider_ref) "
            "WHERE public_id = %s AND session_nonce = %s AND status = 'open' AND last_send_at <= %s "
            "RETURNING sends",
            (
                now,
                now + timedelta(seconds=int(ttl_s)),
                provider_ref,
                public_id,
                session_nonce,
                now - timedelta(seconds=int(min_wait_s)),
            ),
        ).fetchone()
    return None if row is None else int(row[0])



@_clean_errors()
def take_resend_gate(
    public_id: str, session_nonce: str, *, now: datetime, min_wait_s: int, conn: Any = None
) -> bool:
    """Card S6 resend: claim the 60-second gate atomically (``last_send_at`` moves to ``now``) without
    touching the code's life, so a resend that is then refused never stretches the old code's expiry.
    False when it is too soon, the challenge is closed, or the browser does not match."""
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_challenges SET last_send_at = %s "
            "WHERE public_id = %s AND session_nonce = %s AND status = 'open' AND last_send_at <= %s RETURNING id",
            (now, public_id, session_nonce, now - timedelta(seconds=int(min_wait_s))),
        ).fetchone()
    return row is not None


@_clean_errors()
def restart_code_life(
    public_id: str,
    session_nonce: str,
    *,
    now: datetime,
    ttl_s: int,
    provider_ref: str | None = None,
    conn: Any = None,
) -> int | None:
    """After a resent text really went out: one more send, a fresh code life, the vendor's new id.
    The new send count, or None when the challenge is no longer open."""
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_challenges SET sends = sends + 1, expires_at = %s, "
            "provider_ref = COALESCE(%s, provider_ref) "
            "WHERE public_id = %s AND session_nonce = %s AND status = 'open' RETURNING sends",
            (now + timedelta(seconds=int(ttl_s)), provider_ref, public_id, session_nonce),
        ).fetchone()
    return None if row is None else int(row[0])


# ---------------------------------------------------------------- sends


def _validate_send(row: SendRow) -> None:
    _check(row.purpose, CHALLENGE_PURPOSES, "purpose")
    _check(row.pool, POOLS, "pool")
    _check(row.channel, CHANNELS, "channel")
    _check(row.outcome, SEND_OUTCOMES, "outcome")


_INSERT_SEND = (
    "INSERT INTO public.auth_phone_sends (at, phone_hash, ip_hash, device_hash, country, est_cost_cents, "
    "lookup_cost_cents, purpose, pool, ip_prefix_hash, has_device_cookie, channel, outcome) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id"
)


def _insert_send(c: Any, row: SendRow, now: datetime) -> int:
    return int(
        c.execute(
            _INSERT_SEND,
            (
                _utc(now),
                row.phone_hash,
                row.ip_hash,
                row.device_hash,
                row.country,
                Decimal(str(row.est_cost_cents)),
                Decimal(str(row.lookup_cost_cents)),
                row.purpose,
                row.pool,
                row.ip_prefix_hash,
                bool(row.has_device_cookie),
                row.channel,
                row.outcome,
            ),
        ).fetchone()[0]
    )


@_clean_errors()
def record_send(row: SendRow, *, now: datetime, conn: Any = None) -> int:
    """Insert one send row (a text or a refusal); its id."""
    _validate_send(row)
    with _tx(conn) as c:
        return _insert_send(c, row, now)


@_clean_errors()
def set_send_outcome(
    send_id: int,
    outcome: str,
    *,
    est_cost_cents: Decimal | int | None = None,
    conn: Any = None,
) -> bool:
    """Move a ``pending`` row to its result (``sent``, ``provider_error``, ``unavailable``, ``blocked_voip``...).

    ``est_cost_cents`` replaces the text estimate (0 when the vendor refused). The lookup cost
    is never changed here: it was spent at reserve time. False if the row is not pending.
    """
    _check(outcome, SEND_OUTCOMES, "outcome")
    sql = "UPDATE public.auth_phone_sends SET outcome = %s"
    params: list[Any] = [outcome]
    if est_cost_cents is not None:
        sql += ", est_cost_cents = %s"
        params.append(Decimal(str(est_cost_cents)))
    sql += " WHERE id = %s AND outcome = 'pending' RETURNING id"
    params.append(int(send_id))
    with _tx(conn) as c:
        return c.execute(sql, params).fetchone() is not None


def _send_where(
    since: datetime,
    until: datetime | None,
    filters: dict[str, Any],
    outcomes: tuple[str, ...] | None,
    exclude_outcomes: tuple[str, ...] | None,
) -> tuple[str, list[Any]]:
    clauses = ["at >= %s"]
    params: list[Any] = [_utc(since)]
    if until is not None:
        clauses.append("at < %s")
        params.append(_utc(until))
    for col, value in filters.items():
        if col not in _SEND_FILTERS:
            raise ValueError(f"unknown send filter: {col}")
        if value is None:  # None means "no device", never "any device"
            clauses.append(f"{col} IS NULL")
            continue
        clauses.append(f"{col} = %s")
        params.append(value)
    if outcomes:
        clauses.append("outcome = ANY(%s)")
        params.append(list(outcomes))
    if exclude_outcomes:
        clauses.append("NOT (outcome = ANY(%s))")
        params.append(list(exclude_outcomes))
    return " AND ".join(clauses), params


class SendWindow:
    """Counts over ``auth_phone_sends`` on one connection (inside ``reserve_send`` it sees the
    rows committed before the lock was taken, so every decision is serialised)."""

    def __init__(self, c: Any) -> None:
        self._c = c

    def count(
        self,
        *,
        since: datetime,
        until: datetime | None = None,
        outcomes: tuple[str, ...] | None = None,
        exclude_outcomes: tuple[str, ...] | None = None,
        **filters: Any,
    ) -> int:
        where, params = _send_where(since, until, filters, outcomes, exclude_outcomes)
        return int(self._c.execute("SELECT count(*) FROM public.auth_phone_sends WHERE " + where, params).fetchone()[0])

    def seller_challenges(self, *, user_id: int, since: datetime) -> int:
        """Seller verification codes one account was sent since ``since`` (the reserved pool's
        one-per-account rule of card S6 (d); send rows carry no account id)."""
        return int(
            self._c.execute(
                "SELECT count(*) FROM public.auth_phone_challenges "
                "WHERE purpose = 'seller_verify' AND user_id = %s AND created_at >= %s",
                (int(user_id), _utc(since)),
            ).fetchone()[0]
        )

    def cost_cents(self, *, since: datetime, until: datetime | None = None, **filters: Any) -> Decimal:
        """Text plus lookup cost of every row in the window (refusals included)."""
        where, params = _send_where(since, until, filters, None, None)
        value = self._c.execute(
            "SELECT COALESCE(SUM(est_cost_cents + lookup_cost_cents), 0) FROM public.auth_phone_sends WHERE " + where,
            params,
        ).fetchone()[0]
        return Decimal(value)


@_clean_errors()
def count_sends(
    *,
    since: datetime,
    until: datetime | None = None,
    outcomes: tuple[str, ...] | None = None,
    exclude_outcomes: tuple[str, ...] | None = None,
    conn: Any = None,
    **filters: Any,
) -> int:
    """Rows in ``[since, until)`` matching the filters (``phone_hash``, ``ip_hash``, ``device_hash``,
    ``ip_prefix_hash``, ``pool``, ``purpose``, ``channel``, ``has_device_cookie``, ``country``).
    A filter given as None matches rows where that column IS NULL (``device_hash=None`` counts
    sends without a device); leave a filter out to not filter on it."""
    with _tx(conn) as c:
        return SendWindow(c).count(
            since=since, until=until, outcomes=outcomes, exclude_outcomes=exclude_outcomes, **filters
        )


@_clean_errors()
def sum_cost_cents(*, since: datetime, until: datetime | None = None, conn: Any = None, **filters: Any) -> Decimal:
    """Estimated spend (text plus lookup) in ``[since, until)``."""
    with _tx(conn) as c:
        return SendWindow(c).cost_cents(since=since, until=until, **filters)


@_clean_errors()
def reserve_send(
    row: SendRow,
    *,
    now: datetime,
    decide: Callable[[SendWindow], str | None],
    conn: Any = None,
) -> SendReservation:
    """The send-cap check as ONE transaction (card S5 review (d), S6 (a)).

    Takes ``pg_advisory_xact_lock(hashtext('auth_phone_send_cap'))``, so every reserve in the
    site runs one after the other; calls ``decide(window)`` which counts what it needs and returns
    None to allow or a ``blocked_*`` outcome to refuse; inserts the row (``pending`` with its
    estimated costs when allowed, the refusal with zero cost otherwise) and commits, which releases
    the lock. A database error rolls everything back and raises: the caller fails closed.
    """
    _validate_send(row)
    with _tx(conn) as c:
        c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (SEND_CAP_LOCK,))
        verdict = decide(SendWindow(c))
        if verdict is None:
            final = row
        else:
            _check(verdict, SEND_OUTCOMES, "outcome")
            if not verdict.startswith("blocked_"):
                raise ValueError("decide() must return None or a blocked_* outcome")
            final = replace(row, outcome=verdict, est_cost_cents=0, lookup_cost_cents=0)
        send_id = _insert_send(c, final, now)
    return SendReservation(send_id=send_id, outcome=final.outcome)


# ``pending`` is included so a send whose vendor outcome is unknown (kept pending after a timeout
# that may have sent) counts for the breaker; the service reads it as a failure.
VENDOR_OUTCOMES = ("sent", "provider_error", "unavailable", "pending")


@_clean_errors()
def recent_vendor_outcomes(*, limit: int = 5, conn: Any = None) -> list[tuple[str, datetime]]:
    """(outcome, at) of the newest rows where the vendor was asked, newest first (the S6 breaker)."""
    with _tx(conn) as c:
        rows = c.execute(
            "SELECT outcome, at FROM public.auth_phone_sends WHERE outcome = ANY(%s) ORDER BY at DESC, id DESC LIMIT %s",
            (list(VENDOR_OUTCOMES), int(limit)),
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------- wrong tries


@_clean_errors()
def wrong_try_state(key: str, *, now: datetime, conn: Any = None) -> WrongTries:
    """The counter for ``key`` (a phone hash, or a phone-and-browser key chosen by S6)."""
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "SELECT tries, window_start, locked_until FROM public.auth_phone_wrong_tries WHERE phone_hash = %s",
            (key,),
        ).fetchone()
    if row is None:
        return WrongTries(0, None, None, False)
    tries, window_start, locked_until = row
    return WrongTries(int(tries), window_start, locked_until, locked_until is not None and locked_until > now)


@_clean_errors()
def add_wrong_try(
    key: str,
    *,
    now: datetime,
    max_tries: int = 5,
    lock_minutes: int = 15,
    window_hours: int = 24,
    conn: Any = None,
) -> WrongTries:
    """Count one wrong code. At ``max_tries`` inside the window the key is locked for ``lock_minutes``.

    A new challenge does not touch this row, so a fresh code never resets the count. After a lock
    ends, or when the window has passed, the count starts again at 1. Row-locked, so concurrent
    wrong codes are all counted.
    """
    now = _utc(now)
    with _tx(conn) as c:
        c.execute(
            "INSERT INTO public.auth_phone_wrong_tries (phone_hash, tries, window_start) VALUES (%s, 0, %s) "
            "ON CONFLICT (phone_hash) DO NOTHING",
            (key, now),
        )
        tries, window_start, locked_until = c.execute(
            "SELECT tries, window_start, locked_until FROM public.auth_phone_wrong_tries "
            "WHERE phone_hash = %s FOR UPDATE",
            (key,),
        ).fetchone()
        lock_over = locked_until is not None and locked_until <= now
        window_over = locked_until is None and window_start <= now - timedelta(hours=int(window_hours))
        if lock_over or window_over:
            tries, window_start, locked_until = 0, now, None
        tries = int(tries) + 1
        if locked_until is None and tries >= int(max_tries):
            locked_until = now + timedelta(minutes=int(lock_minutes))
        c.execute(
            "UPDATE public.auth_phone_wrong_tries SET tries = %s, window_start = %s, locked_until = %s "
            "WHERE phone_hash = %s",
            (tries, window_start, locked_until, key),
        )
    return WrongTries(tries, window_start, locked_until, locked_until is not None and locked_until > now)


@_clean_errors()
def clear_wrong_tries(key: str, *, conn: Any = None) -> None:
    """Forget the counter (S6 decides when, for example after a successful sign-in)."""
    with _tx(conn) as c:
        c.execute("DELETE FROM public.auth_phone_wrong_tries WHERE phone_hash = %s", (key,))


# ---------------------------------------------------------------- identities


@_clean_errors()
def find_identity_by_phone(phone_e164: str, *, conn: Any = None) -> Identity | None:
    """Identities are found by the number itself, never by its hash (DESIGN.md section 16)."""
    with _tx(conn) as c:
        row = c.execute(
            "SELECT " + _IDENTITY_COLS + " FROM public.auth_phone_identities WHERE phone_e164 = %s", (phone_e164,)
        ).fetchone()
    return None if row is None else Identity(*row)


@_clean_errors()
def find_identity_by_user(user_id: int, *, conn: Any = None) -> Identity | None:
    with _tx(conn) as c:
        row = c.execute(
            "SELECT " + _IDENTITY_COLS + " FROM public.auth_phone_identities WHERE user_id = %s", (int(user_id),)
        ).fetchone()
    return None if row is None else Identity(*row)


@_clean_errors(unique_is_taken=True)
def insert_identity(
    *,
    phone_e164: str,
    phone_hash: str,
    country: str,
    now: datetime,
    consent_at: datetime,
    consent_text_version: str,
    line_type: str | None = None,
    user_id: int | None = None,
    state: str = "reserving",
    conn: Any = None,
) -> Identity:
    """Insert an identity (``reserving`` with no user, or ``active`` with one).

    Raises :class:`IdentityTaken` when the number or the account already has one: the unique
    index decides, so two tabs racing for one number get one row (the loser signs in as the winner).
    """
    _check(state, ("reserving", "active"), "state")
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "INSERT INTO public.auth_phone_identities (user_id, phone_e164, phone_hash, country, line_type, "
            "verified_at, state, consent_at, consent_text_version, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING " + _IDENTITY_COLS,
            (
                user_id,
                phone_e164,
                phone_hash,
                country,
                line_type,
                now,
                state,
                _utc(consent_at),
                consent_text_version,
                now,
            ),
        ).fetchone()
    return Identity(*row)


@_clean_errors(unique_is_taken=True)
def activate_identity(identity_id: int, user_id: int, *, now: datetime, conn: Any = None) -> bool:
    """``reserving`` to ``active`` with its account (creation order step 3). Raises
    :class:`IdentityTaken` if that account already holds another number."""
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_identities SET user_id = %s, state = 'active', last_login_at = %s "
            "WHERE id = %s AND state = 'reserving' RETURNING id",
            (int(user_id), _utc(now), int(identity_id)),
        ).fetchone()
    return row is not None


@_clean_errors()
def touch_login(identity_id: int, *, now: datetime, conn: Any = None) -> None:
    with _tx(conn) as c:
        c.execute(
            "UPDATE public.auth_phone_identities SET last_login_at = %s WHERE id = %s",
            (_utc(now), int(identity_id)),
        )


@_clean_errors(unique_is_taken=True)
def change_number(
    user_id: int,
    *,
    phone_e164: str,
    phone_hash: str,
    country: str,
    now: datetime,
    line_type: str | None = None,
    conn: Any = None,
) -> Identity | None:
    """Move an account's identity to a new verified number. None if the account has no active
    identity; :class:`IdentityTaken` if another account holds the new number."""
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_identities SET phone_e164 = %s, phone_hash = %s, country = %s, "
            "line_type = %s, verified_at = %s WHERE user_id = %s AND state = 'active' RETURNING "
            + _IDENTITY_COLS,
            (phone_e164, phone_hash, country, line_type, now, int(user_id)),
        ).fetchone()
    return None if row is None else Identity(*row)


@_clean_errors()
def detach_dormant(identity_id: int, *, now: datetime, conn: Any = None) -> bool:
    """Dormant-number rule (card S6 (g)): the old account keeps its data but loses the number.

    The row goes back to ``reserving`` with no user, remembering the old account in
    ``dormant_user_id``, so the next holder's new account can take it with :func:`activate_identity`.
    """
    with _tx(conn) as c:
        row = c.execute(
            "UPDATE public.auth_phone_identities SET dormant_user_id = user_id, user_id = NULL, "
            "state = 'reserving', detached_at = %s WHERE id = %s AND state = 'active' RETURNING id",
            (_utc(now), int(identity_id)),
        ).fetchone()
    return row is not None


@_clean_errors()
def delete_identity_for_user(user_id: int, *, now: datetime | None = None, conn: Any = None) -> bool:
    """Erase the phone identity of a deleted account (card S7: deleting an account frees its number).

    In one transaction: end the open code challenges of that number (the challenge keeps no number
    once it has ended), delete the identity row that holds the account, and clear the old-account
    link of a dormant row. A number that was handed to a newer account after going dormant is not
    touched: only the row whose ``user_id`` is this account goes. ``auth_phone_sends`` hash rows stay
    on purpose (they expire at 30 days; deleting them would let a person dodge the caps).
    Returns True when an identity row was removed.
    """
    when = _utc(now or datetime.now(timezone.utc))
    uid = int(user_id)
    with _tx(conn) as c:
        row = c.execute(
            "SELECT phone_hash FROM public.auth_phone_identities WHERE user_id = %s", (uid,)
        ).fetchone()
        if row is not None:
            c.execute(
                "UPDATE public.auth_phone_challenges SET status = 'cancelled', phone_e164 = NULL, "
                "finished_at = %s WHERE phone_hash = %s AND status = 'open'",
                (when, row[0]),
            )
        deleted = c.execute(
            "DELETE FROM public.auth_phone_identities WHERE user_id = %s RETURNING id", (uid,)
        ).fetchone()
        c.execute(
            "UPDATE public.auth_phone_identities SET dormant_user_id = NULL WHERE dormant_user_id = %s",
            (uid,),
        )
    return deleted is not None


@_clean_errors()
def max_identity_user_id(*, conn: Any = None) -> int:
    """Highest account id any identity points at (active or dormant); 0 when there is none."""
    with _tx(conn) as c:
        row = c.execute(
            "SELECT GREATEST(COALESCE(MAX(user_id), 0), COALESCE(MAX(dormant_user_id), 0)) "
            "FROM public.auth_phone_identities"
        ).fetchone()
    return int(row[0] or 0)


# ---------------------------------------------------------------- session epochs (card S10)


@_clean_errors()
def set_sessions_valid_after(user_id: int, *, now: datetime, reason: str, conn: Any = None) -> datetime:
    """Void every session of ``user_id`` issued before ``now``. Never moves backwards."""
    now = _utc(now)
    with _tx(conn) as c:
        row = c.execute(
            "INSERT INTO public.auth_session_epochs (user_id, valid_after, reason, set_at) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "valid_after = GREATEST(auth_session_epochs.valid_after, EXCLUDED.valid_after), "
            "reason = EXCLUDED.reason, set_at = EXCLUDED.set_at RETURNING valid_after",
            (int(user_id), now, reason, now),
        ).fetchone()
    return row[0]


@_clean_errors()
def sessions_valid_after(user_id: int, *, conn: Any = None) -> datetime | None:
    with _tx(conn) as c:
        row = c.execute(
            "SELECT valid_after FROM public.auth_session_epochs WHERE user_id = %s", (int(user_id),)
        ).fetchone()
    return None if row is None else row[0]


# ---------------------------------------------------------------- line-type cache


class PgLookupCache:
    """The durable :class:`backend.auth.phone.provider.LookupCache` (card S3 asked for it; S6 wires it).

    Keeps a classification only, by phone hash: no number, no vendor text.
    """

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    @_clean_errors()
    def get(self, phone_hash: str) -> LineInfo | None:
        with _tx(None) as c:
            row = c.execute(
                "SELECT ok, line_type, country, reason FROM public.auth_phone_lookup_cache "
                "WHERE phone_hash = %s AND expires_at > %s",
                (phone_hash, _utc(self._clock())),
            ).fetchone()
        return None if row is None else LineInfo(ok=bool(row[0]), line_type=row[1], country=row[2], reason=row[3])

    @_clean_errors()
    def put(self, phone_hash: str, info: LineInfo, ttl_s: int) -> None:
        now = _utc(self._clock())
        with _tx(None) as c:
            c.execute(
                "INSERT INTO public.auth_phone_lookup_cache (phone_hash, ok, line_type, country, reason, checked_at, "
                "expires_at) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (phone_hash) DO UPDATE SET "
                "ok = EXCLUDED.ok, line_type = EXCLUDED.line_type, country = EXCLUDED.country, "
                "reason = EXCLUDED.reason, checked_at = EXCLUDED.checked_at, expires_at = EXCLUDED.expires_at",
                (
                    phone_hash,
                    bool(info.ok),
                    info.line_type,
                    info.country,
                    info.reason,
                    now,
                    now + timedelta(seconds=int(ttl_s)),
                ),
            )


# ---------------------------------------------------------------- retention


@_clean_errors()
def cleanup(*, now: datetime, conn: Any = None) -> dict[str, int]:
    """Delete by age (DESIGN.md section 16); the nightly job calls it. Counts per table.

    Open challenges past their expiry are ended first (status ``expired``, number cleared), so no
    raw number outlives its code. Identities and session epochs are never deleted by age.
    """
    now = _utc(now)
    out: dict[str, int] = {}
    with _tx(conn) as c:
        out["challenges_expired"] = c.execute(
            "UPDATE public.auth_phone_challenges SET status = 'expired', finished_at = expires_at, phone_e164 = NULL "
            "WHERE status = 'open' AND expires_at <= %s",
            (now,),
        ).rowcount
        out["challenges"] = c.execute(
            "DELETE FROM public.auth_phone_challenges WHERE status <> 'open' AND finished_at < %s",
            (now - timedelta(days=CHALLENGE_RETENTION_DAYS),),
        ).rowcount
        out["sends"] = c.execute(
            "DELETE FROM public.auth_phone_sends WHERE at < %s",
            (now - timedelta(days=SEND_RETENTION_DAYS),),
        ).rowcount
        out["wrong_tries"] = c.execute(
            "DELETE FROM public.auth_phone_wrong_tries WHERE GREATEST(window_start, COALESCE(locked_until, window_start)) < %s",
            (now - timedelta(hours=WRONG_TRY_RETENTION_HOURS),),
        ).rowcount
        out["lookup_cache"] = c.execute(
            "DELETE FROM public.auth_phone_lookup_cache WHERE expires_at <= %s", (now,)
        ).rowcount
    return out
