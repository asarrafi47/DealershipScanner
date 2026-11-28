"""S16: retention purge and the nightly job on real Postgres rows (DESIGN.md section 16).

Every table gets rows on both sides of its retention boundary; the purge must delete exactly the
rows past it and keep the rest, a second run deletes nothing, and ``--dry-run`` deletes nothing.
Test numbers are in the fictional +1 202 555 01xx range; hashes are made-up hex.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import pytest

from backend import settings_store as ss
from backend.auth.phone import retention
from backend.scripts import auth_phone_nightly as nightly

pg = pytest.mark.pg
UTC = timezone.utc
# 3:30 AM Eastern on 15 November: "yesterday" is 14 November in New York.
NOW = datetime(2026, 11, 15, 8, 30, tzinfo=UTC)
YESTERDAY_NOON = datetime(2026, 11, 14, 17, 0, tzinfo=UTC)
MIN = timedelta(minutes=1)
PHONE = "+12025550142"
DIGITS7 = re.compile(r"\d{7,}")
HEX8 = re.compile(r"\b[0-9a-f]{8,}\b")


def _h(i: int) -> str:
    return f"{i:02x}" * 32


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    for name in ("OWNER_ALERT_EMAIL", "SMS_ALERT_SEND_THRESHOLD"):
        monkeypatch.delenv(name, raising=False)
    rows: dict[str, str] = {}
    ss.set_loader(lambda: dict(rows))
    yield rows
    ss.set_loader(None)


class _Db:
    def __init__(self, pg_db):
        self.pg_db = pg_db
        self.n = 0

    def run(self, sql, params=()):
        with self.pg_db.connect() as c:
            c.execute(sql, params)

    def count(self, table, where="TRUE", params=()):
        with self.pg_db.connect() as c:
            return c.execute(f"SELECT count(*) FROM public.{table} WHERE {where}", params).fetchone()[0]

    def challenge(self, *, status, expires_at, finished_at=None, created_at=None, tag=""):
        self.n += 1
        self.run(
            "INSERT INTO public.auth_phone_challenges (public_id, phone_hash, phone_e164, purpose, session_nonce, "
            "provider, status, created_at, expires_at, last_send_at, finished_at) "
            "VALUES (%s, %s, %s, 'login', 'nonce', 'fake', %s, %s, %s, %s, %s)",
            (f"pid-{tag}-{self.n}", _h(1), PHONE if status == "open" else None, status,
             created_at or expires_at - timedelta(minutes=5), expires_at, expires_at - timedelta(minutes=5),
             finished_at),
        )

    def send(self, *, at, outcome="sent", pool="known", cost=6):
        self.run(
            "INSERT INTO public.auth_phone_sends (at, phone_hash, ip_hash, ip_prefix_hash, country, est_cost_cents, "
            "purpose, pool, has_device_cookie, outcome) VALUES (%s, %s, %s, %s, 'US', %s, 'login', %s, TRUE, %s)",
            (at, _h(1), _h(2), _h(3), cost if outcome in ("sent", "pending") else 0, pool, outcome),
        )

    def wrong(self, key, *, window_start, locked_until=None):
        self.run(
            "INSERT INTO public.auth_phone_wrong_tries (phone_hash, tries, window_start, locked_until) "
            "VALUES (%s, 5, %s, %s)",
            (key, window_start, locked_until),
        )

    def lookup(self, key, *, expires_at):
        self.run(
            "INSERT INTO public.auth_phone_lookup_cache (phone_hash, ok, reason, checked_at, expires_at) "
            "VALUES (%s, TRUE, 'ok', %s, %s)",
            (key, expires_at - timedelta(days=30), expires_at),
        )

    def identity(self, *, phone, user_id, state, created_at):
        self.run(
            "INSERT INTO public.auth_phone_identities (user_id, phone_e164, phone_hash, country, verified_at, state, "
            "consent_at, consent_text_version, created_at) VALUES (%s, %s, %s, 'US', %s, %s, %s, 'v1', %s)",
            (user_id, phone, _h(9), created_at, state, created_at, created_at),
        )


@pytest.fixture
def db(pg_db):
    return _Db(pg_db)


def _seed_boundaries(db: _Db) -> dict[str, int]:
    """Rows on both sides of every boundary. Returns how many of each table must go."""
    edge7 = NOW - timedelta(days=7)
    # challenges: ended 7 days ago give or take a minute; open ones whose code expired then
    db.challenge(status="approved", expires_at=edge7 - 2 * MIN, finished_at=edge7 - MIN, tag="old")  # go
    db.challenge(status="failed", expires_at=edge7, finished_at=edge7 + MIN, tag="new")  # keep
    db.challenge(status="open", expires_at=edge7 - MIN, tag="stale")  # go (ended at expiry)
    db.challenge(status="open", expires_at=edge7 + MIN, tag="recent")  # keep
    db.challenge(status="cancelled", expires_at=edge7 - timedelta(days=1), finished_at=None, tag="nofin")  # go
    db.challenge(status="open", expires_at=NOW + timedelta(minutes=4), tag="live")  # keep
    # sends: 30 days
    edge30 = NOW - timedelta(days=30)
    db.send(at=edge30 - MIN)  # go
    db.send(at=edge30 - timedelta(days=3), outcome="blocked_cap")  # go
    db.send(at=edge30 + MIN)  # keep
    db.send(at=NOW - timedelta(hours=1))  # keep
    # wrong tries: 24 hours after unlock, or after the window of a row that never locked
    edge24 = NOW - timedelta(hours=24)
    db.wrong(_h(10), window_start=edge24 - timedelta(hours=1), locked_until=edge24 - MIN)  # go
    db.wrong(_h(11), window_start=edge24 - timedelta(hours=3), locked_until=edge24 + MIN)  # keep: unlocked < 24h ago
    db.wrong(_h(12), window_start=edge24 - MIN)  # go: never locked, window over a day ago
    db.wrong(_h(13), window_start=edge24 + MIN)  # keep
    # lookup cache: past its 30-day life
    db.lookup(_h(20), expires_at=NOW - MIN)  # go
    db.lookup(_h(21), expires_at=NOW + MIN)  # keep
    # identities are never purged by age
    db.identity(phone="+12025550143", user_id=7, state="active", created_at=NOW - timedelta(days=400))
    return {"challenges": 3, "sends": 2, "wrong_tries": 2, "lookup_cache": 1}


def _counts(db: _Db) -> dict[str, int]:
    return {
        "challenges": db.count("auth_phone_challenges"),
        "sends": db.count("auth_phone_sends"),
        "wrong_tries": db.count("auth_phone_wrong_tries"),
        "lookup_cache": db.count("auth_phone_lookup_cache"),
        "identities": db.count("auth_phone_identities"),
    }


@pg
def test_purge_deletes_only_rows_past_retention(db):
    gone = _seed_boundaries(db)
    before = _counts(db)
    out = retention.purge(now=NOW)
    assert out == gone
    after = _counts(db)
    for table, n in gone.items():
        assert after[table] == before[table] - n, table
    assert after["identities"] == before["identities"] == 1
    # the survivors are the ones on the young side of each boundary
    assert db.count("auth_phone_challenges", "public_id LIKE 'pid-old-%%' OR public_id LIKE 'pid-stale-%%'") == 0
    assert db.count("auth_phone_challenges", "public_id LIKE 'pid-nofin-%%'") == 0
    assert db.count("auth_phone_wrong_tries", "phone_hash = ANY(%s)", ([_h(11), _h(13)],)) == 2
    assert db.count("auth_phone_lookup_cache", "phone_hash = %s", (_h(21),)) == 1


@pg
def test_purge_is_idempotent(db):
    _seed_boundaries(db)
    retention.purge(now=NOW)
    assert retention.purge(now=NOW) == {"challenges": 0, "sends": 0, "wrong_tries": 0, "lookup_cache": 0}


@pg
def test_dry_run_counts_and_deletes_nothing(db):
    gone = _seed_boundaries(db)
    before = _counts(db)
    assert retention.purge(now=NOW, dry_run=True) == gone
    assert _counts(db) == before


@pg
def test_small_batches_delete_everything_past_retention(db):
    for i in range(7):
        db.send(at=NOW - timedelta(days=31, minutes=i))
    db.send(at=NOW - timedelta(days=29))
    assert retention.purge(now=NOW, batch=2)["sends"] == 7
    assert db.count("auth_phone_sends") == 1


@pg
def test_purge_logs_counts_only(db, caplog):
    _seed_boundaries(db)
    with caplog.at_level(logging.INFO, logger="auth_phone"):
        retention.purge(now=NOW)
    text = caplog.text
    assert "event=retention_purge" in text and "sends=2" in text
    assert PHONE not in text and not DIGITS7.search(text) and not HEX8.search(text)


@pg
def test_day_totals_count_sends_approvals_cost_and_refusals(db):
    for _ in range(4):
        db.send(at=YESTERDAY_NOON)
    db.send(at=YESTERDAY_NOON, pool="new")
    db.send(at=YESTERDAY_NOON, outcome="blocked_limit")
    db.send(at=NOW - timedelta(days=2))  # the day before: not counted
    for _ in range(3):
        db.challenge(status="approved", expires_at=YESTERDAY_NOON + MIN, finished_at=YESTERDAY_NOON)
    since, until = datetime(2026, 11, 14, 5, tzinfo=UTC), datetime(2026, 11, 15, 5, tzinfo=UTC)
    t = retention.day_totals(since=since, until=until)
    assert (t.sends, t.approvals, t.approval_pct) == (5, 3, 60)
    assert t.cost_cents == 30
    assert t.blocked == {"blocked_limit": 1}
    assert t.pools == {"known": 4, "new": 1}


@pg
def test_stale_reserving_counts_identities_older_than_an_hour(db):
    db.identity(phone="+12025550144", user_id=None, state="reserving", created_at=NOW - timedelta(minutes=61))
    db.identity(phone="+12025550145", user_id=None, state="reserving", created_at=NOW - timedelta(minutes=59))
    db.identity(phone="+12025550146", user_id=8, state="active", created_at=NOW - timedelta(days=3))
    assert retention.stale_reserving(now=NOW) == 1


# ---------------------------------------------------------------- nightly job, end to end


def _run(capsys, **kw):
    code = nightly.run(now=NOW, **kw)
    return code, capsys.readouterr().out


@pg
def test_nightly_quiet_day_exits_0_and_purges(db, capsys):
    gone = _seed_boundaries(db)
    for _ in range(3):
        db.send(at=YESTERDAY_NOON)
        db.challenge(status="approved", expires_at=YESTERDAY_NOON + MIN, finished_at=YESTERDAY_NOON)
    code, out = _run(capsys)
    assert code == nightly.EXIT_OK
    assert "day 2026-11-14 (America/New_York)" in out
    assert "sends 3 of 30 (alert at 15)" in out
    assert "approvals 3 (100% of texts)" in out
    assert f"deleted challenges {gone['challenges']}, sends {gone['sends']}" in out
    assert PHONE not in out and not DIGITS7.search(out) and not HEX8.search(out)


@pg
def test_nightly_dry_run_deletes_nothing(db, capsys):
    _seed_boundaries(db)
    before = _counts(db)
    code, out = _run(capsys, dry_run=True)
    assert code == nightly.EXIT_OK
    assert "would delete challenges 3, sends 2, wrong_tries 2, lookup_cache 1" in out
    assert _counts(db) == before


@pg
def test_nightly_exits_2_at_half_the_send_cap(db, capsys):
    for _ in range(15):
        db.send(at=YESTERDAY_NOON)
        db.challenge(status="approved", expires_at=YESTERDAY_NOON + MIN, finished_at=YESTERDAY_NOON)
    code, out = _run(capsys)
    assert code == nightly.EXIT_ALERT
    assert "result: cap_50pct" in out


@pg
def test_nightly_exits_3_on_a_cap_refusal(db, capsys):
    db.send(at=YESTERDAY_NOON)
    db.send(at=YESTERDAY_NOON, outcome="blocked_cap")
    code, out = _run(capsys)
    assert code == nightly.EXIT_CAP
    assert "blocked blocked_cap 1" in out and "result: cap_hit" in out


@pg
def test_nightly_reports_stale_reserving_identities(db, capsys):
    db.identity(phone="+12025550147", user_id=None, state="reserving", created_at=NOW - timedelta(hours=2))
    code, out = _run(capsys, purge=False)
    assert code == nightly.EXIT_OK
    assert "reserving identities older than one hour 1" in out
