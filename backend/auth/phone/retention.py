"""Retention purge and day totals for phone sign-in (card S16, DESIGN.md sections 16 and 17).

What is deleted by age (identities and session epochs never are):

- ``auth_phone_challenges``: 7 days after the challenge ended (``finished_at``). An open challenge
  whose code expired (``expires_at``) more than 7 days ago counts as ended then; so does an ended
  row that has no ``finished_at``.
- ``auth_phone_sends``: 30 days after ``at``.
- ``auth_phone_wrong_tries``: 24 hours after the lock ended (``locked_until``); a row that never
  locked goes 24 hours after its window started (``window_start``), when its 24-hour window is over.
- ``auth_phone_lookup_cache``: once ``expires_at`` (30 days after the lookup) has passed.

Chat counters (35 days) are purged by ``backend/scripts/chat_budget_nightly.py`` (card C6), not here.

Each table is purged in its own transaction, in batches (``DELETE ... WHERE key IN (SELECT ...
LIMIT n)``), so a re-run deletes nothing new and a failure on one table leaves the others done.
``dry_run`` counts the same rows and deletes nothing. Logs carry counts only: no number, no hash.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

_log = logging.getLogger("auth_phone")

CHALLENGE_RETENTION_DAYS = 7
SEND_RETENTION_DAYS = 30
WRONG_TRY_RETENTION_HOURS = 24
RESERVING_STALE_HOURS = 1
DEFAULT_BATCH = 500

REAL_SEND_OUTCOMES = ("pending", "sent")


@dataclass(frozen=True)
class _Rule:
    name: str
    table: str
    key: str
    where: str  # uses %(cut)s (the age cut-off) and %(now)s


def _rules() -> tuple[_Rule, ...]:
    return (
        _Rule(
            "challenges",
            "public.auth_phone_challenges",
            "id",
            "(status <> 'open' AND COALESCE(finished_at, expires_at) < %(cut_challenge)s) "
            "OR (status = 'open' AND expires_at < %(cut_challenge)s)",
        ),
        _Rule("sends", "public.auth_phone_sends", "id", "at < %(cut_send)s"),
        _Rule(
            "wrong_tries",
            "public.auth_phone_wrong_tries",
            "phone_hash",
            "COALESCE(locked_until, window_start) < %(cut_wrong)s",
        ),
        _Rule("lookup_cache", "public.auth_phone_lookup_cache", "phone_hash", "expires_at <= %(now)s"),
    )


TABLES = tuple(r.name for r in _rules())


def _utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _params(now: datetime) -> dict[str, Any]:
    return {
        "now": now,
        "cut_challenge": now - timedelta(days=CHALLENGE_RETENTION_DAYS),
        "cut_send": now - timedelta(days=SEND_RETENTION_DAYS),
        "cut_wrong": now - timedelta(hours=WRONG_TRY_RETENTION_HOURS),
    }


@contextmanager
def _connection(conn: Any) -> Iterator[Any]:
    """The caller's connection, or one we open and close (``backend.db.connect``)."""
    if conn is not None:
        yield conn
        return
    from backend.db.connect import connect

    c = connect()
    try:
        yield c
    finally:
        c.close()


def _purge_one(c: Any, rule: _Rule, params: dict[str, Any], *, dry_run: bool, batch: int) -> int:
    try:
        if dry_run:
            row = c.execute(f"SELECT count(*) FROM {rule.table} WHERE {rule.where}", params).fetchone()
            c.rollback()
            return int(row[0])
        total = 0
        sql = (
            f"DELETE FROM {rule.table} WHERE {rule.key} IN ("
            f"SELECT {rule.key} FROM {rule.table} WHERE {rule.where} LIMIT %(batch)s FOR UPDATE SKIP LOCKED)"
        )
        while True:
            n = c.execute(sql, {**params, "batch": batch}).rowcount
            total += max(int(n or 0), 0)
            if not n or n < batch:
                break
        c.commit()
        return total
    except BaseException:
        try:
            c.rollback()
        except Exception:
            pass
        raise


def purge(*, now: datetime | None = None, dry_run: bool = False, batch: int = DEFAULT_BATCH,
          conn: Any = None) -> dict[str, int]:
    """Delete (or with ``dry_run`` count) every row past retention. Returns ``{table: rows}``."""
    if batch < 1:
        raise ValueError("batch must be at least 1")
    now = _utc(now or datetime.now(timezone.utc))
    params = _params(now)
    out: dict[str, int] = {}
    with _connection(conn) as c:
        for rule in _rules():
            out[rule.name] = _purge_one(c, rule, params, dry_run=dry_run, batch=batch)
    _log.info(
        "event=retention_purge dry_run=%s %s",
        "yes" if dry_run else "no",
        " ".join(f"{k}={v}" for k, v in out.items()),
    )
    return out


# ---------------------------------------------------------------- totals


@dataclass
class DayTotals:
    """Counts for one window of send rows and challenges. Counts and cents only."""

    sends: int = 0  # real texts (pending or sent)
    approvals: int = 0  # challenges approved (check_ok) inside the window
    cost_cents: Decimal = Decimal(0)  # text plus lookup estimates of every row in the window
    outcomes: dict[str, int] = field(default_factory=dict)  # every outcome, refusals included
    pools: dict[str, int] = field(default_factory=dict)  # real texts by pool

    @property
    def blocked(self) -> dict[str, int]:
        return {k: v for k, v in sorted(self.outcomes.items()) if k.startswith("blocked_")}

    @property
    def approval_pct(self) -> int | None:
        """Approvals per real text, in percent; None when nothing was sent."""
        if self.sends <= 0:
            return None
        return int(self.approvals * 100 // self.sends)


def day_totals(*, since: datetime, until: datetime, conn: Any = None) -> DayTotals:
    """Totals for ``since <= t < until`` from the app's own rows (never from vendor callbacks)."""
    since, until = _utc(since), _utc(until)
    t = DayTotals()
    with _connection(conn) as c:
        try:
            for outcome, pool, n, cost in c.execute(
                "SELECT outcome, pool, count(*), COALESCE(SUM(est_cost_cents + lookup_cost_cents), 0) "
                "FROM public.auth_phone_sends WHERE at >= %s AND at < %s GROUP BY outcome, pool",
                (since, until),
            ).fetchall():
                t.outcomes[outcome] = t.outcomes.get(outcome, 0) + int(n)
                t.cost_cents += Decimal(cost)
                if outcome in REAL_SEND_OUTCOMES:
                    t.sends += int(n)
                    t.pools[pool] = t.pools.get(pool, 0) + int(n)
            row = c.execute(
                "SELECT count(*) FROM public.auth_phone_challenges "
                "WHERE status = 'approved' AND finished_at >= %s AND finished_at < %s",
                (since, until),
            ).fetchone()
            t.approvals = int(row[0])
        finally:
            c.rollback()
    return t


def spend_since(since: datetime, *, conn: Any = None) -> Decimal:
    """Estimated cents of every send row since ``since`` (the prepaid guard's figure)."""
    with _connection(conn) as c:
        try:
            row = c.execute(
                "SELECT COALESCE(SUM(est_cost_cents + lookup_cost_cents), 0) FROM public.auth_phone_sends WHERE at >= %s",
                (_utc(since),),
            ).fetchone()
        finally:
            c.rollback()
    return Decimal(row[0])


def stale_reserving(*, now: datetime, conn: Any = None) -> int:
    """Identities still ``reserving`` more than an hour after they were created (account creation
    never finished: card S6 order)."""
    cut = _utc(now) - timedelta(hours=RESERVING_STALE_HOURS)
    with _connection(conn) as c:
        try:
            row = c.execute(
                "SELECT count(*) FROM public.auth_phone_identities WHERE state = 'reserving' AND created_at < %s",
                (cut,),
            ).fetchone()
        finally:
            c.rollback()
    return int(row[0])
