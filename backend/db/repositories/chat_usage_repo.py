"""Chat usage counters and app settings in Postgres (cards C1 and C2, CHAT_LIMITS.md).

One small row per (period, bucket_start, subject_kind, subject_key). Nothing here stores
message text. Plain values in, plain values out: no Flask. Time is always passed in
(``now``) so tests use a fake clock.

Concurrency: ``reserve`` upserts every row it needs in a fixed order inside ONE
transaction. The upsert takes the row lock, so two requests racing for the last message
of a limit are serialised by Postgres: the second sees the first one's count and rolls
back. Fixed order avoids deadlocks between requests that share some rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

_log = logging.getLogger(__name__)

DEFAULT_TZ = "America/New_York"
RETENTION_DAYS = 35
SUBJECT_KINDS = ("user", "phone", "device", "ip", "listing", "global")
GLOBAL_KEY = "all"


# ---------------------------------------------------------------- time buckets


def _zone(tz_name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name or DEFAULT_TZ)
    except Exception:  # unknown zone name: use the default
        return ZoneInfo(DEFAULT_TZ)


def _aware_utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def hour_start(now: datetime, tz_name: str | None = None) -> datetime:
    """Start of the clock hour containing ``now`` (UTC datetime)."""
    local = _aware_utc(now).astimezone(_zone(tz_name))
    start_local = local.replace(minute=0, second=0, microsecond=0, fold=local.fold)
    return start_local.astimezone(timezone.utc)


def day_start(now: datetime, tz_name: str | None = None) -> datetime:
    """Start (local midnight in the reset time zone) of the day containing ``now``, in UTC.

    On the days clocks change the day is 23 or 25 hours long; local midnight is computed
    from the wall-clock date, so the bucket still starts at midnight.
    """
    zone = _zone(tz_name)
    local = _aware_utc(now).astimezone(zone)
    midnight = datetime(local.year, local.month, local.day, tzinfo=zone)
    return midnight.astimezone(timezone.utc)


def next_hour(now: datetime, tz_name: str | None = None) -> datetime:
    """Next top of the hour after ``now`` (UTC)."""
    return hour_start(now, tz_name) + timedelta(hours=1)


def next_day(now: datetime, tz_name: str | None = None) -> datetime:
    """Next local midnight after ``now`` in the reset time zone (UTC)."""
    zone = _zone(tz_name)
    local = _aware_utc(now).astimezone(zone)
    nxt = datetime(local.year, local.month, local.day, tzinfo=zone) + timedelta(days=1)
    nxt = datetime(nxt.year, nxt.month, nxt.day, tzinfo=zone)
    return nxt.astimezone(timezone.utc)


def bucket_starts(now: datetime, tz_name: str | None = None) -> dict[str, datetime]:
    return {"hour": hour_start(now, tz_name), "day": day_start(now, tz_name)}


# ---------------------------------------------------------------- data types


@dataclass(frozen=True)
class Charge:
    """One subject a message is counted against, with its limits (0 = no limit).

    ``tokens_limit`` and ``cost_limit_micro`` are day totals (used by the global row).
    """

    kind: str
    key: str
    hour_limit: int = 0
    day_limit: int = 0
    tokens_limit: int = 0
    cost_limit_micro: int = 0


@dataclass(frozen=True)
class Violation:
    kind: str
    key: str
    period: str  # 'hour' | 'day'
    what: str  # 'messages' | 'tokens' | 'cost'
    limit: int
    current: int


@dataclass
class Reservation:
    """What a successful reserve counted, so true-up and give-back undo exactly that."""

    rows: list[tuple[str, datetime, str, str]] = field(default_factory=list)
    weight: int = 1
    est_tokens_in: int = 0
    est_tokens_out: int = 0
    est_cost_micro: int = 0


@dataclass
class ReserveResult:
    ok: bool
    reservation: Reservation | None = None
    violations: list[Violation] = field(default_factory=list)


# ---------------------------------------------------------------- connection


def _connect():
    from backend.db.connect import connect

    return connect()


_UPSERT_ADD = """
INSERT INTO public.chat_usage_buckets
    (period, bucket_start, subject_kind, subject_key, messages, tokens_in, tokens_out, cost_micro)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (period, bucket_start, subject_kind, subject_key) DO UPDATE SET
    messages   = chat_usage_buckets.messages   + EXCLUDED.messages,
    tokens_in  = chat_usage_buckets.tokens_in  + EXCLUDED.tokens_in,
    tokens_out = chat_usage_buckets.tokens_out + EXCLUDED.tokens_out,
    cost_micro = chat_usage_buckets.cost_micro + EXCLUDED.cost_micro
RETURNING messages, tokens_in + tokens_out, cost_micro
"""


def _ordered(charges: list[Charge]) -> list[Charge]:
    return sorted(charges, key=lambda c: (c.kind, c.key))


def reserve(
    charges: list[Charge],
    *,
    now: datetime,
    tz_name: str | None = None,
    weight: int = 1,
    est_tokens_in: int = 0,
    est_tokens_out: int = 0,
    est_cost_micro: int = 0,
    conn: Any = None,
) -> ReserveResult:
    """Count one message (``weight`` messages plus the estimate) against every charge.

    All-or-nothing: the whole thing is one transaction, and if any hour or day row would
    pass its limit the transaction rolls back and no counter changes. The result lists
    every limit that would have been passed (not only the first). A limit of 0 means no
    limit. Raises on database errors (the gate fails closed on those).
    """
    weight = max(1, int(weight))
    starts = bucket_starts(now, tz_name)
    own = conn is None
    c = conn or _connect()
    violations: list[Violation] = []
    reservation = Reservation(
        weight=weight,
        est_tokens_in=max(0, int(est_tokens_in)),
        est_tokens_out=max(0, int(est_tokens_out)),
        est_cost_micro=max(0, int(est_cost_micro)),
    )
    try:
        for ch in _ordered(charges):
            if ch.kind not in SUBJECT_KINDS:
                raise ValueError(f"unknown subject kind: {ch.kind}")
            for period in ("hour", "day"):
                start = starts[period]
                msgs, tokens, cost = c.execute(
                    _UPSERT_ADD,
                    (
                        period,
                        start,
                        ch.kind,
                        ch.key,
                        weight,
                        reservation.est_tokens_in,
                        reservation.est_tokens_out,
                        reservation.est_cost_micro,
                    ),
                ).fetchone()
                reservation.rows.append((period, start, ch.kind, ch.key))
                limit = ch.hour_limit if period == "hour" else ch.day_limit
                if limit > 0 and msgs > limit:
                    violations.append(Violation(ch.kind, ch.key, period, "messages", limit, msgs - weight))
                if period == "day":
                    if ch.tokens_limit > 0 and tokens > ch.tokens_limit:
                        violations.append(
                            Violation(ch.kind, ch.key, period, "tokens", ch.tokens_limit, int(tokens))
                        )
                    if ch.cost_limit_micro > 0 and cost > ch.cost_limit_micro:
                        violations.append(
                            Violation(ch.kind, ch.key, period, "cost", ch.cost_limit_micro, int(cost))
                        )
        if violations:
            c.rollback()
            return ReserveResult(ok=False, violations=violations)
        c.commit()
        return ReserveResult(ok=True, reservation=reservation)
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


_ADJUST = """
UPDATE public.chat_usage_buckets SET
    messages   = GREATEST(0, messages   + %s),
    tokens_in  = GREATEST(0, tokens_in  + %s),
    tokens_out = GREATEST(0, tokens_out + %s),
    cost_micro = GREATEST(0, cost_micro + %s)
WHERE period = %s AND bucket_start = %s AND subject_kind = %s AND subject_key = %s
"""


def _adjust(res: Reservation, d_msgs: int, d_in: int, d_out: int, d_cost: int, conn: Any) -> None:
    own = conn is None
    c = conn or _connect()
    try:
        for period, start, kind, key in sorted(res.rows, key=lambda r: (r[2], r[3], 0 if r[0] == "hour" else 1)):
            c.execute(_ADJUST, (d_msgs, d_in, d_out, d_cost, period, start, kind, key))
        c.commit()
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


def true_up(
    res: Reservation,
    *,
    tokens_in: int,
    tokens_out: int,
    cost_micro: int,
    messages_back: bool = False,
    conn: Any = None,
) -> None:
    """Replace the estimate with the real numbers on every row the reserve counted.

    ``messages_back=True`` also gives the message(s) back (the model failed or refused),
    while still recording whatever tokens were really spent.
    """
    _adjust(
        res,
        -res.weight if messages_back else 0,
        max(0, int(tokens_in)) - res.est_tokens_in,
        max(0, int(tokens_out)) - res.est_tokens_out,
        max(0, int(cost_micro)) - res.est_cost_micro,
        conn,
    )


def give_back(res: Reservation, *, conn: Any = None) -> None:
    """Undo a reserve completely (the model failed or refused: nobody pays for our errors)."""
    _adjust(res, -res.weight, -res.est_tokens_in, -res.est_tokens_out, -res.est_cost_micro, conn)


_CLAIM_GIVE_BACK = """
INSERT INTO public.chat_usage_buckets (period, bucket_start, subject_kind, subject_key, messages)
VALUES ('hour', %s, 'user', %s, 1)
ON CONFLICT (period, bucket_start, subject_kind, subject_key) DO UPDATE SET
    messages = chat_usage_buckets.messages + 1
    WHERE chat_usage_buckets.messages < %s
RETURNING messages
"""


def claim_give_back(
    user_id: str,
    *,
    now: datetime,
    cap: int,
    tz_name: str | None = None,
    conn: Any = None,
) -> bool:
    """Take one of an account's ``cap`` message give-backs for this clock hour (atomic).

    True when the give-back may be made; False once the hour's cap is used up (the row stays at
    ``cap``). Kept on a row of its own (``user`` kind, key ``giveback:<id>``) so it never mixes with
    the account's message counters. A cap of 0 or less never gives back.
    """
    if cap <= 0:
        return False
    own = conn is None
    c = conn or _connect()
    try:
        row = c.execute(_CLAIM_GIVE_BACK, (hour_start(now, tz_name), f"giveback:{user_id}", int(cap))).fetchone()
        c.commit()
        return row is not None
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


def add_denied(
    kind: str,
    key: str,
    period: str,
    *,
    now: datetime,
    tz_name: str | None = None,
    conn: Any = None,
) -> None:
    """Count one denial on the subject's row for ``period`` (its own small write)."""
    start = bucket_starts(now, tz_name)[period]
    own = conn is None
    c = conn or _connect()
    try:
        c.execute(
            "INSERT INTO public.chat_usage_buckets (period, bucket_start, subject_kind, subject_key, denied) "
            "VALUES (%s, %s, %s, %s, 1) "
            "ON CONFLICT (period, bucket_start, subject_kind, subject_key) "
            "DO UPDATE SET denied = chat_usage_buckets.denied + 1",
            (period, start, kind, key),
        )
        c.commit()
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


_COLS = ("period", "bucket_start", "subject_kind", "subject_key", "messages", "tokens_in", "tokens_out", "cost_micro", "denied")


def read_totals(
    period: str,
    *,
    now: datetime,
    tz_name: str | None = None,
    kind: str | None = None,
    key: str | None = None,
    bucket_start: datetime | None = None,
    limit: int = 200,
    conn: Any = None,
) -> list[dict]:
    """Rows of one bucket (the current hour or day, or ``bucket_start``), busiest first."""
    start = bucket_start or bucket_starts(now, tz_name)[period]
    sql = "SELECT " + ", ".join(_COLS) + " FROM public.chat_usage_buckets WHERE period = %s AND bucket_start = %s"
    params: list[Any] = [period, start]
    if kind:
        sql += " AND subject_kind = %s"
        params.append(kind)
    if key is not None:
        sql += " AND subject_key = %s"
        params.append(key)
    sql += " ORDER BY messages DESC, subject_kind, subject_key LIMIT %s"
    params.append(int(limit))
    own = conn is None
    c = conn or _connect()
    try:
        rows = c.execute(sql, params).fetchall()
        c.rollback()
    finally:
        if own:
            c.close()
    return [dict(zip(_COLS, r)) for r in rows]


def read_day_totals(**kwargs: Any) -> list[dict]:
    return read_totals("day", **kwargs)


def read_hour_totals(**kwargs: Any) -> list[dict]:
    return read_totals("hour", **kwargs)


def delete_older_than(*, now: datetime, days: int = RETENTION_DAYS, conn: Any = None) -> int:
    """Delete rows whose bucket started more than ``days`` days ago. Returns the row count."""
    cutoff = _aware_utc(now) - timedelta(days=int(days))
    own = conn is None
    c = conn or _connect()
    try:
        cur = c.execute("DELETE FROM public.chat_usage_buckets WHERE bucket_start < %s", (cutoff,))
        n = cur.rowcount
        c.commit()
        return int(n or 0)
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


# ---------------------------------------------------------------- app_settings


def load_app_settings(*, conn: Any = None) -> dict[str, str]:
    """All rows of app_settings as {key: value}. Raises when the table cannot be read."""
    own = conn is None
    c = conn or _connect()
    try:
        rows = c.execute("SELECT key, value FROM public.app_settings").fetchall()
        c.rollback()
    finally:
        if own:
            c.close()
    return {str(k): str(v) for k, v in rows}


def set_app_setting(key: str, value: str, updated_by: int | None = None, *, conn: Any = None) -> None:
    own = conn is None
    c = conn or _connect()
    try:
        c.execute(
            "INSERT INTO public.app_settings (key, value, updated_at, updated_by) "
            "VALUES (%s, %s, NOW(), %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW(), updated_by = EXCLUDED.updated_by",
            (key, value, updated_by),
        )
        c.commit()
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            c.close()


def delete_app_setting(key: str, *, conn: Any = None) -> None:
    own = conn is None
    c = conn or _connect()
    try:
        c.execute("DELETE FROM public.app_settings WHERE key = %s", (key,))
        c.commit()
    finally:
        if own:
            c.close()
