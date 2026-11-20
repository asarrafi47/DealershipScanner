"""One inventory connection per web request (inventory_pg request scope), offline.

``backend.web.request_scope`` enters ``inventory_pg.begin_request_scope()`` for every
request; inside it, SEQUENTIAL ``get_conn()`` calls share one raw psycopg connection.
These tests pin the safety rules with fake raw connections:

* each wrapper close rolls back and restores autocommit=False, a second close is a no-op;
* a get_conn() while another wrapper is open gets a FRESH connection;
* a broken raw connection, or one whose rollback fails, is dropped and replaced;
* scope end closes the raw connection, unless a wrapper still holds it (then that
  wrapper's close closes it);
* outside the scope (scanner, jobs, other threads) every call connects, as before;
* ``WEB_PG_STATEMENT_TIMEOUT_MS`` reaches libpq ``options`` only inside the scope.

The real-Postgres behaviour (rollback isolation, the timeout firing) is in
``test_request_conn_reuse_pg.py``.
"""

from __future__ import annotations

import threading

import pytest

from backend.db import inventory_compat, inventory_pg


class FakeRaw:
    def __init__(self, n: int):
        self.n = n
        self.closed = False
        self.broken = False
        self.autocommit = False
        self.rollbacks = 0
        self.commits = 0
        self.fail_rollback = False

    def rollback(self):
        if self.fail_rollback:
            raise RuntimeError("server closed the connection")
        self.rollbacks += 1

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True

    def cursor(self, row_factory=None):  # pragma: no cover - not used here
        raise AssertionError("no SQL in these tests")


@pytest.fixture()
def fake_pg(monkeypatch):
    made: list[FakeRaw] = []

    def _connect():
        raw = FakeRaw(len(made))
        made.append(raw)
        return raw

    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(inventory_pg, "pg_connect", _connect)
    monkeypatch.delenv("INVENTORY_REQUEST_CONN_REUSE", raising=False)
    yield made
    inventory_pg.end_request_scope()


def _get():
    return inventory_compat.open_inventory_connection()


def test_sequential_calls_share_one_connection_and_reset_it(fake_pg):
    inventory_pg.begin_request_scope()
    for _ in range(5):
        c = _get()
        c.commit()
        c.close()
    assert len(fake_pg) == 1
    raw = fake_pg[0]
    assert raw.rollbacks == 5 and raw.commits == 5 and not raw.closed
    inventory_pg.end_request_scope()
    assert raw.closed


def test_outside_the_scope_every_call_connects(fake_pg):
    for _ in range(3):
        _get().close()
    assert len(fake_pg) == 3 and all(r.closed for r in fake_pg)


def test_reuse_off_by_env_connects_per_call(fake_pg, monkeypatch):
    monkeypatch.setenv("INVENTORY_REQUEST_CONN_REUSE", "0")
    inventory_pg.begin_request_scope()
    for _ in range(3):
        _get().close()
    assert len(fake_pg) == 3 and all(r.closed for r in fake_pg)


def test_nested_open_gets_a_fresh_connection(fake_pg):
    inventory_pg.begin_request_scope()
    outer = _get()
    inner = _get()
    assert outer._raw is not inner._raw
    inner.close()
    assert fake_pg[1].closed  # the fresh one is really closed
    assert not fake_pg[0].closed and fake_pg[0].rollbacks == 0  # outer's transaction untouched
    outer.close()
    again = _get()
    assert again._raw is fake_pg[0]  # the kept one is free again
    again.close()


def test_second_close_does_not_touch_the_next_borrower(fake_pg):
    inventory_pg.begin_request_scope()
    a = _get()
    a.close()
    b = _get()
    assert b._raw is a._raw
    before = fake_pg[0].rollbacks
    a.close()  # stale double close (e.g. explicit close inside a db_conn block)
    assert fake_pg[0].rollbacks == before and not fake_pg[0].closed
    b.close()


def test_autocommit_is_restored_on_release(fake_pg):
    inventory_pg.begin_request_scope()
    c = _get()
    c._raw.autocommit = True  # as scanner/pipeline/db.py::_assess_conn does
    c.close()
    assert fake_pg[0].autocommit is False


def test_broken_connection_is_replaced(fake_pg):
    inventory_pg.begin_request_scope()
    c = _get()
    c.close()
    fake_pg[0].broken = True
    d = _get()
    assert d._raw is fake_pg[1] and fake_pg[0].closed
    d.close()


def test_failed_rollback_drops_the_connection(fake_pg):
    inventory_pg.begin_request_scope()
    c = _get()
    fake_pg[0].fail_rollback = True
    c.close()
    assert fake_pg[0].closed
    d = _get()
    assert d._raw is fake_pg[1]
    d.close()


def test_wrapper_open_at_scope_end_keeps_ownership(fake_pg):
    inventory_pg.begin_request_scope()
    leaked = _get()
    inventory_pg.end_request_scope()
    assert not fake_pg[0].closed  # still in use by the wrapper
    leaked.close()
    assert fake_pg[0].closed
    # and the next request starts clean
    inventory_pg.begin_request_scope()
    _get().close()
    assert len(fake_pg) == 2


def test_other_threads_are_not_in_the_scope(fake_pg):
    inventory_pg.begin_request_scope()
    first = _get()
    first.close()
    seen = []

    def _bg():
        seen.append(inventory_pg.in_request_scope())
        c = _get()
        seen.append(c._raw)
        c.close()

    t = threading.Thread(target=_bg)
    t.start()
    t.join(5)
    assert seen[0] is False
    assert seen[1] is not fake_pg[0] and seen[1].closed


def test_borrow_read_connection_still_wins(fake_pg):
    inventory_pg.begin_request_scope()
    shared = FakeRaw(99)
    inventory_pg._shared_read.raw = shared
    try:
        c = _get()
        assert c._raw is shared
        c.close()
        assert not shared.closed
    finally:
        inventory_pg._shared_read.raw = None


# ── WEB_PG_STATEMENT_TIMEOUT_MS ──────────────────────────────────────────────────────


@pytest.fixture()
def captured_connect(monkeypatch):
    import psycopg

    calls: list[dict] = []
    monkeypatch.setattr(psycopg, "connect", lambda dsn, **kw: calls.append(kw) or FakeRaw(0))
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://u:p@127.0.0.1:5/db")
    yield calls
    inventory_pg.end_request_scope()


def test_statement_timeout_only_inside_the_scope(captured_connect, monkeypatch):
    monkeypatch.setenv("WEB_PG_STATEMENT_TIMEOUT_MS", "8000")
    inventory_pg.pg_connect()
    assert "options" not in captured_connect[-1]  # scanner / jobs / scripts
    inventory_pg.begin_request_scope()
    inventory_pg.pg_connect()
    assert captured_connect[-1]["options"] == "-c statement_timeout=8000"
    inventory_pg.end_request_scope()
    inventory_pg.pg_connect()
    assert "options" not in captured_connect[-1]


@pytest.mark.parametrize("value", ["", "0", "-5", "junk"])
def test_statement_timeout_is_opt_in(captured_connect, monkeypatch, value):
    monkeypatch.setenv("WEB_PG_STATEMENT_TIMEOUT_MS", value)
    inventory_pg.begin_request_scope()
    inventory_pg.pg_connect()
    assert "options" not in captured_connect[-1]


def test_statement_timeout_keeps_dsn_options(captured_connect, monkeypatch):
    monkeypatch.setenv(
        "INVENTORY_DATABASE_URL", "postgresql://u:p@127.0.0.1:5/db?options=-c%20search_path%3Dpublic"
    )
    monkeypatch.setenv("WEB_PG_STATEMENT_TIMEOUT_MS", "500")
    inventory_pg.begin_request_scope()
    inventory_pg.pg_connect()
    assert captured_connect[-1]["options"] == "-c search_path=public -c statement_timeout=500"


def test_scanner_session_set_skips_the_request_connection(fake_pg, monkeypatch):
    """scanner/database.py sets idle_in_transaction_session_timeout per SESSION; on the
    request's reused connection that would outlive the call, so it is skipped there."""
    from backend.scanner import database as sdb

    executed = []
    monkeypatch.setattr(FakeRaw, "execute", lambda self, sql, *a: executed.append(sql), raising=False)
    monkeypatch.setattr(sdb, "_scanner_idle_in_txn_timeout_ms", lambda: 300000)
    inventory_pg.begin_request_scope()
    c = _get()
    sdb._apply_idle_in_txn_guard(c)
    c.close()
    assert executed == []
    inventory_pg.end_request_scope()
    fresh = _get()  # outside the scope: a scanner-owned session still gets it
    sdb._apply_idle_in_txn_guard(fresh)
    fresh.close()
    assert executed == ["SET SESSION idle_in_transaction_session_timeout = 300000"]
