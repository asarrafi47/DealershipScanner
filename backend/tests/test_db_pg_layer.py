"""backend/db/pg.py (N37.4): pooled dict_row access with ``%s`` placeholders.

The un-marked tests need no server. The ``pg`` tests run on a throwaway clone
(``pg_db``; set TEST_PG_ADMIN_DSN to a LOCAL server) and need psycopg_pool.
"""

from __future__ import annotations

import pytest

from backend.db import pg


def test_in_list_is_one_placeholder_whatever_the_length():
    sql, params = pg.in_list("c.id", (1, 2, 3))
    assert sql == "c.id = ANY(%s)"
    assert params == [[1, 2, 3]]
    assert pg.in_list("c.id", [])[1] == [[]]


def test_returning_id_refuses_a_statement_without_returning():
    with pytest.raises(ValueError):
        pg.execute_returning_id("INSERT INTO t (a) VALUES (%s)", (1,))


def test_module_imports_no_sqlite():
    import ast
    from pathlib import Path

    tree = ast.parse(Path(pg.__file__).read_text(encoding="utf-8"))
    names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    names |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert "sqlite3" not in names


@pytest.fixture
def pgl(pg_db):
    pytest.importorskip("psycopg_pool")
    pg.close_pool()
    with pg.tx() as conn:
        conn.execute("CREATE TABLE pgl_t (id serial PRIMARY KEY, name text UNIQUE, n integer)")
    try:
        yield pg
    finally:
        pg.close_pool()


@pytest.mark.pg
def test_rows_are_dicts_and_scalar_reads_by_position_of_first_column(pgl):
    pgl.execute("INSERT INTO pgl_t (name, n) VALUES (%s, %s), (%s, %s)", ("a", 1, "b", 2))
    assert pgl.one("SELECT name, n FROM pgl_t WHERE name = %s", ("a",)) == {"name": "a", "n": 1}
    assert pgl.one("SELECT 1 FROM pgl_t WHERE name = %s", ("zzz",)) is None
    assert [r["name"] for r in pgl.all("SELECT name FROM pgl_t ORDER BY id")] == ["a", "b"]
    assert pgl.scalar("SELECT count(*) FROM pgl_t") == 2
    assert pgl.scalar("SELECT n FROM pgl_t WHERE name = %s", ("zzz",), default=-1) == -1
    assert pgl.scalar("SELECT NULL::int") is None


@pytest.mark.pg
def test_percent_literals_and_named_params(pgl):
    pgl.execute("INSERT INTO pgl_t (name, n) VALUES (%(name)s, %(n)s)", {"name": "50% off", "n": 5})
    assert pgl.scalar("SELECT count(*) FROM pgl_t WHERE name LIKE '%%off'") == 1


@pytest.mark.pg
def test_in_list_matches_and_empty_matches_nothing(pgl):
    pgl.execute_many("INSERT INTO pgl_t (name, n) VALUES (%s, %s)", [("a", 1), ("b", 2), ("c", 3)])
    frag, params = pgl.in_list("n", [1, 3])
    assert [r["name"] for r in pgl.all(f"SELECT name FROM pgl_t WHERE {frag} ORDER BY n", params)] == ["a", "c"]
    frag, params = pgl.in_list("n", [])
    assert pgl.all(f"SELECT name FROM pgl_t WHERE {frag}", params) == []
    assert pgl.execute_many("INSERT INTO pgl_t (name, n) VALUES (%s, %s)", []) == 0


@pytest.mark.pg
def test_execute_returning_id_and_rowcount(pgl):
    first = pgl.execute_returning_id("INSERT INTO pgl_t (name, n) VALUES (%s, %s) RETURNING id", ("a", 1))
    second = pgl.execute_returning_id("INSERT INTO pgl_t (name, n) VALUES (%s, %s) RETURNING id", ("b", 2))
    assert second == first + 1
    assert pgl.execute("UPDATE pgl_t SET n = n + 1") == 2


@pytest.mark.pg
def test_tx_commits_on_success_and_rolls_back_on_error(pgl):
    with pgl.tx() as conn:
        conn.execute("INSERT INTO pgl_t (name, n) VALUES ('kept', 1)")
    with pytest.raises(RuntimeError):
        with pgl.tx() as conn:
            conn.execute("INSERT INTO pgl_t (name, n) VALUES ('lost', 2)")
            raise RuntimeError("boom")
    assert [r["name"] for r in pgl.all("SELECT name FROM pgl_t")] == ["kept"]


@pytest.mark.pg
def test_helpers_on_a_caller_connection_leave_the_transaction_to_the_caller(pgl):
    with pytest.raises(RuntimeError):
        with pgl.tx() as conn:
            pgl.execute("INSERT INTO pgl_t (name, n) VALUES (%s, %s)", ("x", 1), conn=conn)
            assert pgl.scalar("SELECT count(*) FROM pgl_t", conn=conn) == 1
            raise RuntimeError("undo")
    assert pgl.scalar("SELECT count(*) FROM pgl_t") == 0


@pytest.mark.pg
def test_unique_violation_surfaces_the_psycopg_error(pgl):
    import psycopg

    pgl.execute("INSERT INTO pgl_t (name, n) VALUES ('dup', 1)")
    with pytest.raises(psycopg.errors.UniqueViolation):
        pgl.execute("INSERT INTO pgl_t (name, n) VALUES ('dup', 2)")
    # The failed statement rolled its own transaction back; the pool still works.
    assert pgl.scalar("SELECT count(*) FROM pgl_t") == 1


@pytest.mark.pg
def test_readonly_refuses_writes(pgl):
    import psycopg

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        with pgl.readonly() as conn:
            conn.execute("INSERT INTO pgl_t (name, n) VALUES ('no', 0)")
    with pgl.readonly() as conn:
        assert conn.execute("SELECT count(*) AS c FROM pgl_t").fetchone()["c"] == 0


@pytest.mark.pg
def test_session_settings_are_applied(pgl, monkeypatch):
    pgl.close_pool()
    monkeypatch.setenv("PG_STATEMENT_TIMEOUT_MS", "12345")
    monkeypatch.setenv("PG_LOCK_TIMEOUT_MS", "2345")
    assert pgl.scalar("SELECT current_setting('statement_timeout', true)") in ("12345ms", "12s")
    assert pgl.scalar("SELECT current_setting('lock_timeout', true)") == "2345ms"
    assert pgl.scalar("SHOW timezone") == "UTC"


@pytest.mark.pg
def test_pool_follows_the_dsn_and_is_reused(pgl, pg_template, monkeypatch):
    from backend.tests import pg_harness as pgh

    first = pgl.get_pool()
    assert pgl.get_pool() is first
    name, dsn = pgh.clone(pg_template)
    try:
        monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
        second = pgl.get_pool()
        assert second is not first
        assert pgl.scalar("SELECT current_database()") == name
    finally:
        pgl.close_pool()
        pgh.drop(pg_template.admin, name)
