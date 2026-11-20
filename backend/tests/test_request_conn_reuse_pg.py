"""The web request's reused inventory connection on a real Postgres (``pg`` tier).

Proves, against libpq, that the reuse in ``inventory_pg``'s request scope behaves like
connect-per-call: committed work persists, uncommitted work is discarded, an aborted
transaction does not leak into the next borrower, and the opt-in
``WEB_PG_STATEMENT_TIMEOUT_MS`` is set on the request's connections only.
"""

from __future__ import annotations

import pytest

from backend.db import inventory_pg
from backend.db.inventory_db import db_conn

pytestmark = pytest.mark.pg


@pytest.fixture()
def scope(pg_db):
    with db_conn() as conn:
        conn.execute("CREATE TABLE reuse_probe (v INTEGER)")
        conn.commit()
    inventory_pg.begin_request_scope()
    yield pg_db
    inventory_pg.end_request_scope()


def _pid(conn) -> int:
    return conn.execute("SELECT pg_backend_pid()").fetchone()[0]


def test_one_backend_serves_sequential_calls(scope):
    pids = set()
    for _ in range(4):
        with db_conn() as conn:
            pids.add(_pid(conn))
    assert len(pids) == 1


def test_uncommitted_work_is_discarded_and_committed_work_kept(scope):
    with db_conn() as conn:
        conn.execute("INSERT INTO reuse_probe VALUES (1)")
        conn.commit()
        conn.execute("INSERT INTO reuse_probe VALUES (2)")  # never committed
    with db_conn() as conn:
        assert [r[0] for r in conn.execute("SELECT v FROM reuse_probe ORDER BY v").fetchall()] == [1]


def test_aborted_transaction_does_not_reach_the_next_borrower(scope):
    with pytest.raises(Exception):
        with db_conn() as conn:
            conn.execute("SELECT 1/0")
    with db_conn() as conn:
        assert conn.execute("SELECT 41 + 1").fetchone()[0] == 42


def test_nested_connections_are_separate_backends(scope):
    with db_conn() as outer:
        outer.execute("INSERT INTO reuse_probe VALUES (7)")
        with db_conn() as inner:
            assert _pid(inner) != _pid(outer)
            # the outer transaction is not visible to, nor ended by, the inner one
            assert inner.execute("SELECT COUNT(*) FROM reuse_probe").fetchone()[0] == 0
        outer.commit()
    with db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM reuse_probe").fetchone()[0] == 1


def test_statement_timeout_is_web_request_only(scope, monkeypatch):
    monkeypatch.setenv("WEB_PG_STATEMENT_TIMEOUT_MS", "250")
    inventory_pg.end_request_scope()
    inventory_pg.begin_request_scope()  # a new request: the env is read on connect
    with db_conn() as conn:
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "250ms"
        with pytest.raises(Exception) as exc:
            conn.execute("SELECT pg_sleep(2)")
        assert "statement timeout" in str(exc.value)
    inventory_pg.end_request_scope()
    with db_conn() as conn:  # outside the scope: the scanner's and jobs' connections
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "0"
    inventory_pg.begin_request_scope()
