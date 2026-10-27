"""P6A.1: advisory scan leases (backend/scanner/scan_leases.py).

Offline: with no Postgres DSN every lease succeeds as a no-op. ``pg`` tier: a second
holder is refused and named, and the lock is released when its session ends. The lock
connection is autocommit (never idle in transaction) and its ``idle_session_timeout``
is 0, since a server that drops an idle session drops the lock with it."""
from __future__ import annotations

import pytest

from backend.scanner import scan_leases as sl


@pytest.fixture(autouse=True)
def _fresh_shared():
    sl._shared.clear()
    yield
    for lc in list(sl._shared.values()):
        lc.close()
    sl._shared.clear()


def test_no_dsn_is_a_noop(monkeypatch):
    monkeypatch.setattr("backend.db.connect.inventory_dsn", lambda **kw: None)
    acquired, refused = sl.try_lock_dealers(["a-com", "b-com"])
    assert acquired == ["a-com", "b-com"] and refused == {}
    with sl.fleet_lock("nightly") as lease:
        assert lease.acquired and lease.holder is None and lease.degraded == "no postgres dsn"
    with sl.fleet_lock("nightly") as again:  # nothing is held, so nothing contends
        assert again.acquired


def test_unreachable_server_degrades_with_a_warning(monkeypatch, caplog):
    monkeypatch.setattr("backend.db.connect.inventory_dsn",
                        lambda **kw: "postgresql://nobody@127.0.0.1:1/none?connect_timeout=1")
    with caplog.at_level("WARNING", logger="scanner"):
        acquired, refused = sl.try_lock_dealers(["a-com"])
    assert acquired == ["a-com"] and not refused
    assert "without cross-host locks" in caplog.text


@pytest.mark.pg
def test_second_holder_refused_and_named(pg_db):
    first = sl.LeaseConnection("nightly", pg_db.dsn)
    second = sl.LeaseConnection("manual", pg_db.dsn)
    try:
        assert first.try_lock("dealer:a-com") == (True, None)
        ok, holder = second.try_lock("dealer:a-com")
        assert ok is False and holder.startswith("scanner:nightly:")
        assert second.try_lock("dealer:b-com") == (True, None)  # other keys are free
        # re-entrant within the holding session
        assert first.try_lock("dealer:a-com") == (True, None)
    finally:
        first.close()
        second.close()


@pytest.mark.pg
def test_released_when_the_holder_session_ends(pg_db):
    first = sl.LeaseConnection("nightly", pg_db.dsn)
    second = sl.LeaseConnection("manual", pg_db.dsn)
    try:
        assert first.try_lock("fleet:nightly")[0]
        assert not second.try_lock("fleet:nightly")[0]
        first.close()
        assert second.try_lock("fleet:nightly") == (True, None)
    finally:
        first.close()
        second.close()


@pytest.mark.pg
def test_unlock_frees_a_dealer(pg_db):
    first = sl.LeaseConnection("nightly", pg_db.dsn)
    second = sl.LeaseConnection("manual", pg_db.dsn)
    try:
        first.try_lock("dealer:a-com")
        first.unlock("dealer:a-com")
        assert second.try_lock("dealer:a-com")[0]
    finally:
        first.close()
        second.close()


@pytest.mark.pg
def test_try_lock_dealers_and_fleet_lock_over_the_shared_dsn(pg_db):
    acquired, refused = sl.try_lock_dealers(["a-com", "b-com"])
    assert acquired == ["a-com", "b-com"] and refused == {}
    other = sl.LeaseConnection("other", pg_db.dsn)
    try:
        assert not other.try_lock("dealer:b-com")[0]
        sl.release_dealers(["b-com"])
        assert other.try_lock("dealer:b-com")[0]
    finally:
        other.close()
    with sl.fleet_lock("nightly") as a:
        assert a.acquired and a.degraded is None
        with sl.fleet_lock("nightly") as b:
            assert not b.acquired and b.holder.startswith("scanner:nightly:")
        with sl.fleet_lock("other-lane") as c:  # lanes are independent
            assert c.acquired
    with sl.fleet_lock("nightly") as again:
        assert again.acquired


@pytest.mark.pg
def test_lock_connection_is_not_idle_in_transaction_and_keeps_its_session(pg_db):
    lc = sl.LeaseConnection("nightly", pg_db.dsn)
    try:
        lc.try_lock("dealer:a-com")
        assert lc.conn.autocommit is True
        assert lc.conn.execute("SHOW idle_session_timeout").fetchone()[0] in ("0", "0ms")
        state = lc.conn.execute(
            "SELECT state FROM pg_stat_activity WHERE pid = pg_backend_pid()").fetchone()[0]
        assert state != "idle in transaction"
        app = lc.conn.execute("SHOW application_name").fetchone()[0]
        assert app.startswith("scanner:nightly:")
    finally:
        lc.close()
