"""Reuse of keyed SQLCipher users.db connections (backend/db/keyed_conn_pool.py)."""

from __future__ import annotations

import os
import threading

import pytest

sc = pytest.importorskip("sqlcipher3")

from backend.db import users_sqlite  # noqa: E402

KEY = "pytest-users-db-pool-key-0123456789abcdef"


@pytest.fixture
def enc_db(tmp_path, monkeypatch):
    """An encrypted users.db at a tmp path, reached through get_users_conn()."""
    path = tmp_path / "users_pool.db"
    monkeypatch.setenv("USERS_DB_PATH", str(path))
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", KEY)
    monkeypatch.setenv("ALLOW_UNENCRYPTED_USER_DB", "")
    monkeypatch.delenv("USERS_DB_POOL_SIZE", raising=False)
    monkeypatch.delenv("USERS_DB_POOL_MAX_IDLE_S", raising=False)
    users_sqlite.reset_users_conn_pool()
    conn = users_sqlite.get_users_conn()
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()

    opens: list[str] = []
    real = users_sqlite._connect_sqlcipher

    def counting(key):
        opens.append(users_sqlite.users_db_path())
        return real(key)

    monkeypatch.setattr(users_sqlite, "_connect_sqlcipher", counting)
    users_sqlite.reset_users_conn_pool()
    yield path, opens
    users_sqlite.reset_users_conn_pool()


def test_file_is_really_encrypted(enc_db):
    path, _ = enc_db
    with open(path, "rb") as fh:
        assert not fh.read(16).startswith(b"SQLite format 3")


def test_sequential_gets_derive_key_once(enc_db):
    _, opens = enc_db
    raws = set()
    for i in range(10):
        conn = users_sqlite.get_users_conn()
        raws.add(id(conn.raw_connection))
        conn.execute("INSERT INTO t (v) VALUES (?)", (str(i),))
        conn.commit()
        conn.close()
    assert len(opens) == 1
    assert len(raws) == 1
    conn = users_sqlite.get_users_conn()
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 10
    conn.close()


def test_returned_connection_is_clean(enc_db):
    conn = users_sqlite.get_users_conn()
    raw = conn.raw_connection
    default_rf = raw.row_factory
    conn.row_factory = sc.Row
    conn.execute("INSERT INTO t (v) VALUES ('uncommitted')")
    assert raw.in_transaction
    conn.close()

    again = users_sqlite.get_users_conn()
    assert again.raw_connection is raw
    assert not raw.in_transaction
    assert again.row_factory is default_rf
    assert again.execute("SELECT count(*) FROM t WHERE v='uncommitted'").fetchone()[0] == 0
    again.close()


def test_pragmas_are_restored_for_the_next_borrower(enc_db):
    conn = users_sqlite.get_users_conn()
    raw = conn.raw_connection
    before = raw.execute("PRAGMA busy_timeout").fetchone()[0]
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.close()

    again = users_sqlite.get_users_conn()
    assert again.raw_connection is raw
    assert again.execute("PRAGMA busy_timeout").fetchone()[0] == before
    assert again.execute("PRAGMA foreign_keys").fetchone()[0] == 0
    again.close()


def test_handle_with_temp_objects_is_never_pooled(enc_db):
    _, opens = enc_db
    conn = users_sqlite.get_users_conn()
    raw = conn.raw_connection
    conn.execute("CREATE TEMP TABLE leak (v TEXT)")
    conn.execute("INSERT INTO leak VALUES ('user A secret')")
    conn.commit()
    conn.close()

    again = users_sqlite.get_users_conn()
    assert again.raw_connection is not raw
    assert again.execute("SELECT count(*) FROM sqlite_temp_master").fetchone()[0] == 0
    again.close()
    assert len(opens) == 2


def test_use_after_close_raises(enc_db):
    conn = users_sqlite.get_users_conn()
    cur = conn.cursor()
    conn.close()
    with pytest.raises(sc.ProgrammingError):
        conn.execute("SELECT 1")
    with pytest.raises(sc.ProgrammingError):
        cur.execute("SELECT 1")
    cur.close()  # closing a cursor after its connection stays harmless
    conn.close()  # double close is a no-op


def test_with_block_commits_and_returns_wrapper(enc_db):
    conn = users_sqlite.get_users_conn()
    with conn as c:
        assert c is conn
        c.execute("INSERT INTO t (v) VALUES ('ctx')")
    conn.close()
    other = users_sqlite.get_users_conn()
    assert other.execute("SELECT count(*) FROM t WHERE v='ctx'").fetchone()[0] == 1
    other.close()


def test_different_path_gets_different_connection(enc_db, tmp_path, monkeypatch):
    _, opens = enc_db
    a = users_sqlite.get_users_conn()
    raw_a = a.raw_connection
    a.close()
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "other.db"))
    b = users_sqlite.get_users_conn()
    assert b.raw_connection is not raw_a
    assert b.execute("SELECT count(*) FROM sqlite_master WHERE name='t'").fetchone()[0] == 0
    b.close()
    assert len(opens) == 2


def test_replaced_file_is_not_reused(enc_db):
    path, opens = enc_db
    a = users_sqlite.get_users_conn()
    raw_a = a.raw_connection
    a.close()
    # An ops restore that renames a new file into place: new inode.
    os.replace(path, str(path) + ".old")
    import shutil

    shutil.copyfile(str(path) + ".old", path)
    b = users_sqlite.get_users_conn()
    assert b.raw_connection is not raw_a
    b.close()
    assert len(opens) == 2


def test_pid_change_discards_pool(enc_db, monkeypatch):
    _, opens = enc_db
    a = users_sqlite.get_users_conn()
    raw_a = a.raw_connection
    a.close()
    pool = users_sqlite._POOL
    assert pool.idle_count() == 1
    real_pid = os.getpid()
    monkeypatch.setattr(os, "getpid", lambda: real_pid + 1)
    b = users_sqlite.get_users_conn()
    assert b.raw_connection is not raw_a
    assert raw_a in pool._orphans  # inherited handle parked, never closed or reused
    b.close()
    assert len(opens) == 2
    monkeypatch.setattr(os, "getpid", lambda: real_pid)
    pool._orphans.remove(raw_a)
    raw_a.close()


def test_borrowed_before_fork_not_returned_in_child(enc_db, monkeypatch):
    a = users_sqlite.get_users_conn()
    raw_a = a.raw_connection
    real_pid = os.getpid()
    monkeypatch.setattr(os, "getpid", lambda: real_pid + 1)
    a.close()
    pool = users_sqlite._POOL
    assert raw_a in pool._orphans
    assert pool.idle_count() == 0
    monkeypatch.setattr(os, "getpid", lambda: real_pid)
    pool._orphans.remove(raw_a)
    raw_a.close()


def test_error_discards_connection(enc_db):
    _, opens = enc_db
    a = users_sqlite.get_users_conn()
    raw_a = a.raw_connection
    with pytest.raises(sc.OperationalError):
        a.execute("SELECT * FROM no_such_table")
    a.close()
    b = users_sqlite.get_users_conn()
    assert b.raw_connection is not raw_a
    b.close()
    assert len(opens) == 2


def test_cursor_error_discards_connection(enc_db):
    _, opens = enc_db
    a = users_sqlite.get_users_conn()
    raw_a = a.raw_connection
    cur = a.cursor()
    with pytest.raises(sc.OperationalError):
        cur.execute("SELECT * FROM no_such_table")
    a.close()
    b = users_sqlite.get_users_conn()
    assert b.raw_connection is not raw_a
    b.close()


def test_idle_age_and_size_bounds(enc_db, monkeypatch):
    _, opens = enc_db
    monkeypatch.setenv("USERS_DB_POOL_SIZE", "2")
    conns = [users_sqlite.get_users_conn() for _ in range(4)]
    for c in conns:
        c.close()
    assert users_sqlite._POOL.idle_count() == 2
    monkeypatch.setenv("USERS_DB_POOL_MAX_IDLE_S", "0")
    before = len(opens)
    c = users_sqlite.get_users_conn()
    c.close()
    assert len(opens) == before + 1  # every idle handle was too old


def test_pool_size_zero_disables_reuse(enc_db, monkeypatch):
    _, opens = enc_db
    monkeypatch.setenv("USERS_DB_POOL_SIZE", "0")
    for _ in range(3):
        c = users_sqlite.get_users_conn()
        c.execute("SELECT 1")
        c.close()
    assert len(opens) == 3


def test_threads_share_pool_without_overlap(enc_db):
    _, opens = enc_db
    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def work(n: int) -> None:
        try:
            barrier.wait()
            for i in range(10):
                c = users_sqlite.get_users_conn()
                c.execute("INSERT INTO t (v) VALUES (?)", (f"{n}-{i}",))
                c.commit()
                c.close()
        except BaseException as e:  # pragma: no cover - surfaced below
            errors.append(e)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(opens) <= 4
    c = users_sqlite.get_users_conn()
    assert c.execute("SELECT count(*) FROM t").fetchone()[0] == 40
    c.close()


def test_get_org_works_under_sqlcipher(enc_db):
    from backend.db.users_db import accounts

    conn = users_sqlite.get_users_conn()
    conn.execute(
        "CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT, stripe_customer_id TEXT, "
        "stripe_subscription_id TEXT, stripe_subscription_status TEXT, "
        "stripe_current_period_end TEXT)"
    )
    conn.execute("INSERT INTO orgs (id, name) VALUES (7, 'Acme')")
    conn.commit()
    conn.close()
    org = accounts.get_org(7)
    assert org["name"] == "Acme" and org["id"] == 7
