"""Hot-path runtime DDL is gated on the migration chain (REMEDIATION_PLAN P4.6).

Two helpers still carry their own DDL and run on hot paths (``reviews.store``
dropped its runtime DDL in N37.8; a dealership page request must still issue no
CREATE, pinned below):

* ``scanner.recipe_store._ensure_table`` (first recipe-store touch per process;
  its ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` took ACCESS EXCLUSIVE);
* ``scanner.job_queue.ensure_job_tables`` (every web, worker and scheduler start).

On Postgres at the ``migrations/`` version they must issue no statement at all.
Behind the chain (warn mode) they create only what ``to_regclass`` cannot find,
with ``SET LOCAL lock_timeout`` before the first DDL statement. SQLite (pytest)
keeps its DDL. Separately, ``inventory_pg._legacy_postgres_inventory_ddl`` used a
session-level ``SET lock_timeout`` that outlived its commit on the scanner's raw
connection; it is ``SET LOCAL`` now.

A fake psycopg connection stands in for Postgres. The opt-in live test at the end
(``TEST_PG_ADMIN_DSN``, a local server; D-TC9 ``pytest_*`` scratch DBs only) checks
the lock_timeout reset and the behind-the-chain creates on a real server.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import uuid
from pathlib import Path

import pytest

from backend.db import inventory_pg, runtime_ddl, schema_version
from backend.db.inventory_compat import InventoryConnection
from backend.db.schema_version import SchemaNotMigratedError
from backend.scanner import job_queue, recipe_store

_REPO = Path(__file__).resolve().parents[2]
_DDL_RE = re.compile(r"^\s*(CREATE|ALTER|DROP)\b", re.IGNORECASE)
_SET_LOCAL = "SET LOCAL lock_timeout = '3s'"


# ---------------------------------------------------------------------------
# Fake Postgres
# ---------------------------------------------------------------------------


class _LockNotAvailable(Exception):
    """Stands in for psycopg.errors.LockNotAvailable (lock_timeout expired)."""


def _norm(sql: str) -> str:
    return " ".join(sql.split())


class FakePgCursor:
    def __init__(self, conn: "FakePg", *, dict_rows: bool = False) -> None:
        self._conn = conn
        self._dict_rows = dict_rows
        self._one = None
        self._all: list = []
        self.rowcount = 0
        self.description = None

    def execute(self, sql: str, params=None):
        text = _norm(sql)
        self._conn.events.append(("sql", text))
        self._one, self._all = None, []
        if self._conn.fail_on and _DDL_RE.match(text) and self._conn.fail_on in text:
            raise _LockNotAvailable("canceling statement due to lock timeout")
        if text.startswith("SELECT to_regclass("):
            names = re.findall(r"to_regclass\('public\.(\w+)'\)", text)
            vals = tuple(n if n in self._conn.present else None for n in names)
            self._one = {f"r{i}": v for i, v in enumerate(vals)} if self._dict_rows else vals
        elif text.startswith("SELECT version FROM public.schema_migrations"):
            self._all = [(v,) for v in sorted(self._conn.versions or ())]
        elif "FROM information_schema.columns" in text:
            m = re.search(r"table_name = '(\w+)' AND column_name = '(\w+)'", text)
            if m and (m.group(1), m.group(2)) in self._conn.columns:
                self._one = (1,)
        return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._all)

    def __iter__(self):
        return iter(self._all)

    def close(self) -> None:
        pass


class FakePg:
    """A psycopg-like raw connection: records every statement, commit and rollback."""

    def __init__(self, present=(), *, versions=None, columns=(), fail_on: str | None = None) -> None:
        self.present = set(present)
        self.versions = versions
        if versions is not None:
            self.present.add("schema_migrations")
        self.columns = set(columns)
        self.fail_on = fail_on
        self.events: list[tuple] = []
        self.closed = False

    def cursor(self, row_factory=None) -> FakePgCursor:
        return FakePgCursor(self, dict_rows=row_factory is not None)

    def execute(self, sql: str, params=None) -> FakePgCursor:
        return self.cursor().execute(sql, params)

    def commit(self) -> None:
        self.events.append(("commit",))

    def rollback(self) -> None:
        self.events.append(("rollback",))

    def close(self) -> None:
        self.closed = True

    @property
    def statements(self) -> list[str]:
        return [e[1] for e in self.events if e[0] == "sql"]

    @property
    def ddl(self) -> list[str]:
        return [s for s in self.statements if _DDL_RE.match(s)]


def _compat(raw: FakePg) -> InventoryConnection:
    return InventoryConnection(raw, backend="postgres")


def _assert_lock_timeout_precedes_ddl(raw: FakePg) -> None:
    stmts = raw.statements
    ddl_at = [i for i, s in enumerate(stmts) if _DDL_RE.match(s)]
    assert ddl_at, "expected DDL"
    set_at = stmts.index(_SET_LOCAL)
    assert set_at < ddl_at[0], stmts
    assert stmts[set_at + 1] == stmts[ddl_at[0]], "SET LOCAL must come right before the first DDL"
    # SET LOCAL and every DDL statement share one transaction.
    i_set = raw.events.index(("sql", _SET_LOCAL))
    i_last = raw.events.index(("sql", stmts[ddl_at[-1]]))
    assert ("commit",) not in raw.events[i_set:i_last]
    assert ("rollback",) not in raw.events[i_set:i_last]
    assert not any(re.match(r"SET\s+lock_timeout", s, re.IGNORECASE) for s in stmts), "session-level SET"


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch):
    """Per-process flags start clean; the schema answer is set per test."""
    monkeypatch.setattr(recipe_store, "_table_ready", False)
    monkeypatch.setattr(inventory_pg, "_PG_INV_SCHEMA_OK", False)
    monkeypatch.setattr(schema_version, "_current", None)
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "")
    monkeypatch.setenv("INVENTORY_AUTO_MIGRATE", "")


def _no_connect():
    raise AssertionError("the schema answer is cached; no connection expected")


@pytest.fixture
def pg_current(monkeypatch):
    """Inventory is Postgres and this process already confirmed the chain."""
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(inventory_pg, "pg_connect", _no_connect)
    monkeypatch.setattr(schema_version, "_current", True)


@pytest.fixture
def pg_behind(monkeypatch):
    """Inventory is Postgres, behind the chain, warn mode (answer cached)."""
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(inventory_pg, "pg_connect", _no_connect)
    monkeypatch.setattr(schema_version, "_current", False)


@pytest.fixture
def pg_strict_behind(monkeypatch):
    """Postgres one migration behind, INVENTORY_SCHEMA_CHECK=strict, not yet checked."""
    expected = schema_version.expected_schema_version()
    checker = FakePg(versions=set(range(1, expected)))
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(inventory_pg, "pg_connect", lambda: checker)
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "strict")
    return checker


# ---------------------------------------------------------------------------
# runtime_ddl
# ---------------------------------------------------------------------------


def test_gate_is_open_on_sqlite_without_asking_postgres(monkeypatch):
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: False)
    monkeypatch.setattr(schema_version, "schema_is_current", lambda: pytest.fail("asked on SQLite"))
    assert runtime_ddl.pg_legacy_ddl_needed() is True


def test_gate_is_shut_on_a_current_postgres(pg_current):
    assert runtime_ddl.pg_legacy_ddl_needed() is False


def test_gate_is_open_on_a_behind_postgres_in_warn_mode(pg_behind):
    assert runtime_ddl.pg_legacy_ddl_needed() is True


def test_gate_raises_on_a_behind_postgres_in_strict_mode(pg_strict_behind):
    with pytest.raises(SchemaNotMigratedError):
        runtime_ddl.pg_legacy_ddl_needed()
    assert pg_strict_behind.ddl == []


def test_gate_checks_the_chain_once_per_process(monkeypatch):
    expected = schema_version.expected_schema_version()
    opened: list[FakePg] = []

    def connect():
        opened.append(FakePg(versions=set(range(1, expected + 1))))
        return opened[-1]

    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(inventory_pg, "pg_connect", connect)
    for _ in range(5):
        assert runtime_ddl.pg_legacy_ddl_needed() is False
    assert len(opened) == 1 and opened[0].ddl == []


def test_missing_relations_is_one_catalog_query_in_the_given_order():
    raw = FakePg(present={"b"})
    cur = raw.cursor()
    assert runtime_ddl.pg_missing_relations(cur, ["c", "b", "a"]) == ["c", "a"]
    assert raw.statements == [
        "SELECT to_regclass('public.c') AS r0, to_regclass('public.b') AS r1, "
        "to_regclass('public.a') AS r2"
    ]
    dict_cur = raw.cursor(row_factory=object())
    assert runtime_ddl.pg_missing_relations(dict_cur, ["b", "z"]) == ["z"]
    assert runtime_ddl.pg_missing_relations(cur, []) == []


@pytest.mark.parametrize("bad", ["x; DROP TABLE cars", "Cars", "a.b", "", "x'"])
def test_catalog_helpers_refuse_anything_but_plain_identifiers(bad):
    cur = FakePg().cursor()
    with pytest.raises(ValueError):
        runtime_ddl.pg_missing_relations(cur, [bad])
    with pytest.raises(ValueError):
        runtime_ddl.pg_column_exists(cur, bad, "c")


def test_run_legacy_ddl_sets_local_lock_timeout_first():
    raw = FakePg()
    runtime_ddl.run_pg_legacy_ddl(raw.cursor(), ["CREATE TABLE IF NOT EXISTS t (a int)", " ", ""])
    assert raw.statements == [_SET_LOCAL, "CREATE TABLE IF NOT EXISTS t (a int)"]
    raw2 = FakePg()
    runtime_ddl.run_pg_legacy_ddl(raw2.cursor(), [])
    assert raw2.events == []


def test_sqlite_connection_detection(tmp_path):
    conn = sqlite3.connect(tmp_path / "x.db")
    try:
        assert runtime_ddl.is_sqlite_connection(conn)
        assert runtime_ddl.is_sqlite_connection(InventoryConnection(conn, backend="sqlite"))
    finally:
        conn.close()
    assert not runtime_ddl.is_sqlite_connection(_compat(FakePg()))
    assert not runtime_ddl.is_sqlite_connection(FakePg())


# ---------------------------------------------------------------------------
# recipe_store
# ---------------------------------------------------------------------------


def test_recipe_store_issues_no_statement_when_the_schema_is_current(pg_current):
    raw = FakePg()
    recipe_store._ensure_table(_compat(raw))
    assert raw.events == []
    assert recipe_store._table_ready is True


def test_recipe_store_behind_checks_the_catalog_and_never_alters(pg_behind):
    raw = FakePg(present={"dealer_recipes"}, columns={("dealer_recipes", "scan_hints")})
    recipe_store._ensure_table(_compat(raw))
    assert raw.ddl == []
    assert raw.statements[0] == "SELECT to_regclass('public.dealer_recipes') AS r0"
    assert "FROM information_schema.columns" in raw.statements[1]
    assert raw.events[-1] == ("commit",)


def test_recipe_store_missing_scan_hints_warns_instead_of_altering(pg_behind, caplog):
    caplog.set_level(logging.WARNING, logger="scanner")
    raw = FakePg(present={"dealer_recipes"})
    recipe_store._ensure_table(_compat(raw))
    assert raw.ddl == []
    assert "no scan_hints column" in caplog.text


def test_recipe_store_behind_and_missing_creates_under_lock_timeout(pg_behind):
    raw = FakePg()
    recipe_store._ensure_table(_compat(raw))
    _assert_lock_timeout_precedes_ddl(raw)
    assert raw.ddl == [_norm(recipe_store._DDL_PG)]
    assert "scan_hints TEXT" in raw.ddl[0]
    assert raw.events[-1] == ("commit",)


def test_recipe_store_failed_create_rolls_back_and_stays_unready(pg_behind):
    raw = FakePg(fail_on="dealer_recipes")
    with pytest.raises(_LockNotAvailable):
        recipe_store._ensure_table(_compat(raw))
    assert raw.events[-1] == ("rollback",)
    assert recipe_store._table_ready is False


def test_recipe_store_strict_refusal_is_best_effort_for_callers(pg_strict_behind, monkeypatch):
    raw = FakePg()
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(recipe_store, "_conn", lambda: _compat(raw))
    monkeypatch.setattr(recipe_store, "_alias_read_slug", lambda _d: None)
    assert recipe_store.db_load_recipes("x-com") is None
    assert raw.ddl == []


def test_recipe_store_writes_issue_no_ddl_on_a_current_postgres(pg_current, monkeypatch):
    raw = FakePg()
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(recipe_store, "_conn", lambda: _compat(raw))
    assert recipe_store.db_save_recipes("x-com", [{"url": "https://x.com/api", "saved_at": 1.0}])
    assert recipe_store.set_scan_hints("x-com", {"notes": "n"})
    assert raw.ddl == []
    assert any(s.startswith("INSERT INTO dealer_recipes") for s in raw.statements)


def test_recipe_store_sqlite_creates_the_table_and_adds_scan_hints(tmp_path, monkeypatch):
    monkeypatch.setattr(schema_version, "schema_is_current", lambda: pytest.fail("asked on SQLite"))
    db = tmp_path / "store.db"
    conn = sqlite3.connect(db)
    try:
        recipe_store._ensure_table(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(dealer_recipes)")}
        assert {"dealer_id", "recipes_json", "max_saved_at", "scan_hints"} <= cols
    finally:
        conn.close()

    # A SQLite store created before scan_hints existed gets the column (ALTER ladder kept).
    old = tmp_path / "old.db"
    conn = sqlite3.connect(old)
    try:
        conn.execute("CREATE TABLE dealer_recipes (dealer_id TEXT PRIMARY KEY, recipes_json TEXT NOT NULL)")
        conn.commit()
        monkeypatch.setattr(recipe_store, "_table_ready", False)
        recipe_store._ensure_table(conn)
        assert "scan_hints" in {r[1] for r in conn.execute("PRAGMA table_info(dealer_recipes)")}
    finally:
        conn.close()


def test_recipe_store_sqlite_path_wins_even_with_a_postgres_url(tmp_path, monkeypatch):
    """test_recipe_cache_identity names stores by Postgres URL while the store is SQLite:
    the dialect comes from the connection, not the environment."""
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(schema_version, "schema_is_current", lambda: pytest.fail("asked for a SQLite conn"))
    conn = sqlite3.connect(tmp_path / "s.db")
    try:
        recipe_store._ensure_table(conn)
        assert "scan_hints" in {r[1] for r in conn.execute("PRAGMA table_info(dealer_recipes)")}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# job_queue
# ---------------------------------------------------------------------------

_JOB_OBJECTS = {"dealer_catalog", "dealer_jobs", "idx_dealer_jobs_status", "idx_dealer_catalog_next_scan"}


def test_job_tables_issue_no_statement_when_the_schema_is_current(pg_current):
    raw = FakePg()
    job_queue.ensure_job_tables(raw)
    assert raw.events == []


def test_init_job_queue_schema_issues_no_ddl_when_current(pg_current, monkeypatch):
    raw = FakePg()
    monkeypatch.setattr(job_queue, "pg_connect", lambda: raw)
    job_queue.init_job_queue_schema()
    assert raw.events == [] and raw.closed


def test_job_tables_behind_but_present_only_probe(pg_behind):
    raw = FakePg(present=_JOB_OBJECTS)
    job_queue.ensure_job_tables(raw)
    assert raw.ddl == []
    assert len(raw.statements) == 1 and raw.events[-1] == ("commit",)


def test_job_tables_behind_and_missing_create_under_lock_timeout(pg_behind):
    raw = FakePg(present={"dealer_catalog", "idx_dealer_catalog_next_scan"})
    job_queue.ensure_job_tables(raw)
    _assert_lock_timeout_precedes_ddl(raw)
    assert [s.split("(")[0].strip() for s in raw.ddl] == [
        "CREATE TABLE IF NOT EXISTS dealer_jobs",
        "CREATE INDEX IF NOT EXISTS idx_dealer_jobs_status ON dealer_jobs",
    ]
    assert raw.events[-1] == ("commit",)


def test_job_tables_failed_create_rolls_back_and_raises(pg_behind):
    raw = FakePg(fail_on="idx_dealer_jobs_status")
    with pytest.raises(_LockNotAvailable):
        job_queue.ensure_job_tables(raw)
    assert raw.events[-1] == ("rollback",)
    assert ("commit",) not in raw.events


def test_job_tables_refuse_in_strict_mode(pg_strict_behind):
    raw = FakePg()
    with pytest.raises(SchemaNotMigratedError):
        job_queue.ensure_job_tables(raw)
    assert raw.events == []


# ---------------------------------------------------------------------------
# inventory_pg: the scanner's raw connection must not keep a 3 s lock_timeout
# ---------------------------------------------------------------------------


def test_legacy_inventory_ddl_uses_set_local_inside_its_transaction():
    raw = FakePg()
    inventory_pg._legacy_postgres_inventory_ddl(raw)
    assert raw.statements[0] == _SET_LOCAL
    _assert_lock_timeout_precedes_ddl(raw)
    assert raw.events.count(("commit",)) == 1 and raw.events[-1] == ("commit",)


def test_no_session_level_lock_timeout_left_in_the_hot_path_modules():
    session_set = re.compile(r"""SET\s+(SESSION\s+)?lock_timeout""", re.IGNORECASE)
    for rel in ("backend/db/inventory_pg.py", "backend/db/runtime_ddl.py",
                "backend/scanner/recipe_store.py", "backend/scanner/job_queue.py"):
        text = (_REPO / rel).read_text(encoding="utf-8")
        assert not session_set.search(text), rel


def test_no_add_column_if_not_exists_left_in_recipe_store():
    text = (_REPO / "backend/scanner/recipe_store.py").read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS" not in text


# ---------------------------------------------------------------------------
# A dealership page request issues no CREATE
# ---------------------------------------------------------------------------


_CARD = {
    "id": 1, "title": "2022 RAM 1500 Limited", "make": "RAM", "model": "1500", "trim": "Limited",
    "price": 50000, "mileage": 20000, "fuel_type": "Gasoline", "cylinders": 8,
    "transmission": "8-Speed Automatic", "drivetrain": "4WD", "body_style": "Truck",
    "exterior_color_families": ["black"], "interior_color_families": ["black"],
    "package_names": [], "condition": "Used", "dealer_id": "beamancdjr-com",
    "dealer_name": "Beaman CDJR", "dealer_url": "https://www.beamancdjr.com",
}


@pytest.fixture
def flask_app():
    """Imported under the SQLite test env, before any fixture fakes Postgres
    (backend.main runs init_inventory_db and init_job_queue_schema at import)."""
    from backend.main import app

    return app


def test_dealership_page_request_issues_no_create_on_a_current_postgres(flask_app, pg_current, monkeypatch):
    import backend.db.inventory_db as inv
    import backend.db.repositories.listings_repo as lr
    from backend.routes import dealership_page as dp

    # Anything that bypassed pg_connect would hit a closed loopback port, never a real DB.
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://pytest@127.0.0.1:9/pytest_never")
    opened: list[FakePg] = []

    def connect():
        opened.append(FakePg())
        return opened[-1]

    monkeypatch.setattr(inventory_pg, "pg_connect", connect)
    monkeypatch.setattr(dp, "_dealer_grid_cards_json", lambda _id: [json.dumps(_CARD)])
    monkeypatch.setattr(inv, "listings_grid_serialized_cars", lambda *a, **k: pytest.fail("whole fleet"))
    monkeypatch.setattr(lr, "listings_grid_serialized_cars", lambda *a, **k: pytest.fail("whole fleet"))
    monkeypatch.setattr(dp, "_known_catalog_makes", lambda: frozenset())
    monkeypatch.setattr(dp, "_find_dealership_by_dealer_id", lambda _id: None)
    monkeypatch.setattr(dp, "_dealer_specials", lambda _id: {"total": 0, "groups": [], "scraped_at": None})
    # _dealer_reviews is NOT stubbed: its reads must run without any DDL.
    with flask_app.test_client() as client:
        for _ in range(2):
            r = client.get("/dealership/beamancdjr-com")
            assert r.status_code == 200
    statements = [s for c in opened for s in c.statements]
    assert sum("FROM dealer_reviews" in s for s in statements) >= 2, "the reviews read did not run"
    assert [s for s in statements if _DDL_RE.match(s)] == []


# ---------------------------------------------------------------------------
# Live Postgres (opt-in): TEST_PG_ADMIN_DSN names a LOCAL server where the test
# may create and drop pytest_* databases (D-TC9). Never cars or cars_cleanroom,
# never a remote host, never the mini tunnel (port 15432).
# ---------------------------------------------------------------------------

_ADMIN_DSN_ENV = "TEST_PG_ADMIN_DSN"
_LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1"}


def _guarded_admin_dsn() -> str:
    dsn = (os.environ.get(_ADMIN_DSN_ENV) or "").strip()
    if not dsn:
        pytest.skip(f"{_ADMIN_DSN_ENV} not set (a local server where pytest_* DBs may be created)")
    psycopg = pytest.importorskip("psycopg")
    params = psycopg.conninfo.conninfo_to_dict(dsn)
    host = str(params.get("host") or "")
    if not (host in _LOCAL_HOSTS or host.startswith("/")):
        pytest.fail(f"{_ADMIN_DSN_ENV} must name a local server, not {host!r}")
    if str(params.get("port") or "5432") == "15432":
        pytest.fail(f"{_ADMIN_DSN_ENV} points at the mini tunnel (port 15432)")
    if params.get("dbname") in ("cars", "cars_cleanroom"):
        pytest.fail(f"{_ADMIN_DSN_ENV} must not name an inventory database")
    return dsn


@pytest.fixture
def scratch_pg(monkeypatch):
    from urllib.parse import urlencode

    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    admin_dsn = _guarded_admin_dsn()
    name = f"pytest_runtime_ddl_{uuid.uuid4().hex[:10]}"
    assert name.startswith("pytest_") and name not in ("cars", "cars_cleanroom")
    admin = psycopg.connect(admin_dsn, autocommit=True)
    try:
        admin.execute(f'CREATE DATABASE "{name}"')
        params = conninfo_to_dict(admin_dsn)
        params["dbname"] = name
        dsn = "postgresql://?" + urlencode({k: str(v) for k, v in params.items()})
        monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
        yield dsn
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


@pytest.mark.integration
def test_live_lock_timeout_ends_with_the_transaction_and_behind_creates_only_missing(scratch_pg, monkeypatch):
    import psycopg

    assert inventory_pg.is_inventory_postgres()
    monkeypatch.setattr(schema_version, "_current", False)  # empty DB: behind, warn mode

    raw = psycopg.connect(scratch_pg)
    try:
        before = raw.execute("SHOW lock_timeout").fetchone()[0]
        raw.rollback()
        assert before != "3s"
        inventory_pg._legacy_postgres_inventory_ddl(raw)  # creates the legacy tables, commits
        assert raw.execute("SHOW lock_timeout").fetchone()[0] == before
        raw.rollback()

        # The hot-path helpers on a behind DB: first call creates, the session keeps
        # its own lock_timeout afterwards, and a second call issues no DDL.
        job_queue.ensure_job_tables(raw)
        assert raw.execute("SHOW lock_timeout").fetchone()[0] == before
        raw.rollback()

        conn = InventoryConnection(raw, backend="postgres")
        recipe_store._ensure_table(conn)
        assert raw.execute("SHOW lock_timeout").fetchone()[0] == before
        raw.rollback()
        present = raw.execute(
            "SELECT to_regclass('public.dealer_recipes'), "
            "to_regclass('public.dealer_jobs'), to_regclass('public.idx_dealer_catalog_next_scan')"
        ).fetchone()
        raw.rollback()
        assert all(present), present
    finally:
        raw.close()

    # Second pass in a "new process": the catalog shows everything, so no DDL.
    monkeypatch.setattr(recipe_store, "_table_ready", False)
    seen: list[str] = []
    raw = psycopg.connect(scratch_pg)
    real_cursor = raw.cursor

    class _Spy:
        def __init__(self, c):
            self._c = c

        def execute(self, sql, params=None):
            seen.append(_norm(sql))
            return self._c.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._c, name)

    monkeypatch.setattr(raw, "cursor", lambda *a, **k: _Spy(real_cursor(*a, **k)))
    try:
        job_queue.ensure_job_tables(raw)
        conn = InventoryConnection(raw, backend="postgres")
        recipe_store._ensure_table(conn)
    finally:
        raw.close()
    assert seen and [s for s in seen if _DDL_RE.match(s) or s.startswith("SET")] == []
