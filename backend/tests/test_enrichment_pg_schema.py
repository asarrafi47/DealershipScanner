"""InventoryEnricher issues no DDL on ``cars`` when the inventory is Postgres
(remediation plan 2026-10, unit P1A.4).

Before the fix, ``ensure_enrichment_columns`` probed columns with ``PRAGMA
table_info(cars)``. The Postgres adapter makes PRAGMA a no-op, so the probe saw
no columns and sent ``ALTER TABLE cars ADD COLUMN engine_l``: an ACCESS EXCLUSIVE
lock request on ``cars`` (queued behind every reader, blocking every reader that
arrived after it) that then failed with DuplicateColumn, because migration V001
already owns engine_l / mpg_city / mpg_highway / packages.

The Postgres side here is a fake raw psycopg connection behind the real
``InventoryConnection`` compat wrapper (``open_inventory_connection`` with a
postgres URL and a patched ``pg_connect``), so every statement goes through the
same adapter it does in prod. The fake raises like Postgres would on the ALTER,
which is how the old code failed.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

import pytest

from backend.db import inventory_pg
from backend.db.inventory_compat import InventoryConnection
from backend.enrichment import service

_DDL_ON_CARS = re.compile(r"^\s*(ALTER|CREATE|DROP)\b[^;]*\bcars\b", re.IGNORECASE)
_ANY_DDL = re.compile(r"^\s*(ALTER|CREATE|DROP|TRUNCATE)\b", re.IGNORECASE)
_ENRICHMENT_COLS = {"engine_l", "mpg_city", "mpg_highway", "packages"}


class _DuplicateColumn(Exception):
    """Stands in for psycopg.errors.DuplicateColumn."""


class _LockNotAvailable(Exception):
    """Stands in for psycopg.errors.LockNotAvailable (lock_timeout expired)."""


class _FakePgCursor:
    def __init__(self, conn: _FakePgRaw) -> None:
        self._conn = conn
        self._row: Any = None

    def execute(self, sql: str, params: Any = ()) -> _FakePgCursor:
        text = " ".join(sql.split())
        self._conn.events.append(("sql", text))
        upper = text.upper()
        self._row = None
        if upper.startswith("PRAGMA"):
            raise AssertionError(f"PRAGMA reached Postgres: {text}")
        if re.match(r"ALTER TABLE \"?CARS\"? ADD COLUMN", upper):
            col = text.split()[5].strip('"')
            raise _DuplicateColumn(f'column "{col}" of relation "cars" already exists')
        if "TO_REGCLASS" in upper:
            self._row = (self._conn.regclass,)
        elif upper.startswith("CREATE TABLE IF NOT EXISTS HAIKU_SPEC_CACHE"):
            if self._conn.create_error is not None:
                raise self._conn.create_error
            self._conn.regclass = "haiku_spec_cache"
        return self

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list:
        return [self._row] if self._row is not None else []

    def close(self) -> None:
        pass


class _FakePgRaw:
    """Raw psycopg-shaped connection: records statements and txn boundaries."""

    def __init__(self, *, regclass: str | None, create_error: Exception | None = None) -> None:
        self.regclass = regclass
        self.create_error = create_error
        self.events: list[tuple[str, ...]] = []
        self.closed = False

    def cursor(self, *a: Any, **k: Any) -> _FakePgCursor:
        return _FakePgCursor(self)

    def execute(self, sql: str, params: Any = ()) -> _FakePgCursor:
        # psycopg3 Connection.execute: used when a raw connection is passed in.
        return _FakePgCursor(self).execute(sql, params)

    def commit(self) -> None:
        self.events.append(("commit",))

    def rollback(self) -> None:
        self.events.append(("rollback",))

    def close(self) -> None:
        self.closed = True

    @property
    def statements(self) -> list[str]:
        return [e[1] for e in self.events if e[0] == "sql"]


class _StubCatalog:
    """Stands in for MasterCatalog (no sentence-transformer load, no pgvector)."""

    def collection_exists(self) -> bool:
        return False


@pytest.fixture
def fake_pg(monkeypatch: pytest.MonkeyPatch):
    """Route ``get_conn()`` to the real compat wrapper over fake raw PG connections."""
    raws: list[_FakePgRaw] = []
    state = {"regclass": "haiku_spec_cache", "create_error": None}

    def _connect() -> _FakePgRaw:
        raw = _FakePgRaw(regclass=state["regclass"], create_error=state["create_error"])
        raws.append(raw)
        return raw

    monkeypatch.setenv("INVENTORY_DATABASE_URL", "postgresql://fake-host/fake-db")
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "")
    monkeypatch.setattr(inventory_pg, "pg_connect", _connect)
    monkeypatch.setattr(inventory_pg, "current_shared_read_raw", lambda: None)
    monkeypatch.setattr(service, "MasterCatalog", _StubCatalog)
    # run_all reads candidates after ensure_enrichment_columns; keep it empty so
    # only the schema path is exercised.
    monkeypatch.setattr(service, "fetch_enrichment_candidate_ids", lambda conn, **kw: [])
    monkeypatch.setattr(service, "_fetch_vision_urls_for_ids", lambda conn, ids: {})
    return {"raws": raws, "state": state}


def _all_statements(raws: list[_FakePgRaw]) -> list[str]:
    return [s for raw in raws for s in raw.statements]


def _assert_no_ddl_on_cars(raws: list[_FakePgRaw]) -> None:
    stmts = _all_statements(raws)
    assert stmts, "expected the enricher to open at least one Postgres connection"
    bad = [s for s in stmts if _DDL_ON_CARS.match(s)]
    assert bad == [], f"DDL on cars reached Postgres: {bad}"


# ---------------------------------------------------------------------------
# Postgres: no DDL on cars
# ---------------------------------------------------------------------------


def test_get_conn_is_the_postgres_compat_connection(fake_pg) -> None:
    conn = service.get_conn()
    try:
        assert isinstance(conn, InventoryConnection)
        assert getattr(conn, "_backend", None) == "postgres"
    finally:
        conn.close()


def test_ensure_enrichment_columns_pg_issues_no_ddl_when_cache_exists(fake_pg) -> None:
    conn = service.get_conn()
    try:
        service.ensure_enrichment_columns(conn)  # must not raise
    finally:
        conn.close()
    raws = fake_pg["raws"]
    _assert_no_ddl_on_cars(raws)
    stmts = _all_statements(raws)
    assert [s for s in stmts if _ANY_DDL.match(s)] == [], stmts
    assert stmts == ["SELECT to_regclass('haiku_spec_cache')"]
    # The probe's read transaction is ended, not left idle.
    assert raws[0].events[-1] == ("commit",)


def test_inventory_enricher_init_on_pg_issues_no_alter_table_cars(fake_pg) -> None:
    enricher = service.InventoryEnricher()  # must not raise
    assert isinstance(enricher.catalog, _StubCatalog)
    _assert_no_ddl_on_cars(fake_pg["raws"])
    assert all(raw.closed for raw in fake_pg["raws"])


def test_post_scan_post_enrich_entry_issues_no_alter_table_cars(fake_pg) -> None:
    """post_scan --post-enrich -> run_enrichment_for_car_ids -> InventoryEnricher()."""
    from backend.scanner.post_scan.pipeline import run_enrichment_for_car_ids

    # Catalog not indexed: constructs InventoryEnricher() and stops.
    out = run_enrichment_for_car_ids([1, 2], vision_only=False)
    assert out.get("reason") == "catalog_missing"
    # Vision-only: constructs it and runs run_all (ensure_enrichment_columns again).
    out = run_enrichment_for_car_ids([1, 2], vision_only=True, max_workers=1)
    assert out["processed"] == 0
    assert len(fake_pg["raws"]) >= 3
    _assert_no_ddl_on_cars(fake_pg["raws"])


def test_dev_route_enrich_job_issues_no_alter_table_cars(fake_pg, caplog) -> None:
    """dev/routes.py _spawn_inventory_enrich -> InventoryEnricher().run_all()."""
    from backend.dev import routes

    caplog.set_level("ERROR", logger="dev_routes")
    routes._spawn_inventory_enrich(limit=5, vision_only=False, max_workers=1)
    # The job swallows exceptions into a log line; it must not have needed to.
    assert not [r for r in caplog.records if "enrichment background job failed" in r.getMessage()]
    assert len(fake_pg["raws"]) >= 2
    _assert_no_ddl_on_cars(fake_pg["raws"])


def test_haiku_cache_created_only_when_absent_under_lock_timeout(fake_pg) -> None:
    fake_pg["state"]["regclass"] = None
    conn = service.get_conn()
    try:
        service.ensure_enrichment_columns(conn)
    finally:
        conn.close()
    raws = fake_pg["raws"]
    _assert_no_ddl_on_cars(raws)
    events = raws[0].events
    stmts = raws[0].statements
    assert stmts[0] == "SELECT to_regclass('haiku_spec_cache')"
    assert stmts[1] == "SET LOCAL lock_timeout = '3s'"
    assert stmts[2].startswith("CREATE TABLE IF NOT EXISTS haiku_spec_cache (")
    assert len(stmts) == 3
    # SET LOCAL and CREATE share one transaction (no commit between them),
    # and the create is committed.
    i_set = events.index(("sql", stmts[1]))
    i_create = events.index(("sql", stmts[2]))
    assert ("commit",) not in events[i_set:i_create]
    assert events[i_create + 1] == ("commit",)
    # The DDL sent is Postgres-native (no SQLite function left in it).
    assert "datetime(" not in stmts[2].lower()


def test_haiku_cache_create_failure_rolls_back_and_raises(fake_pg) -> None:
    fake_pg["state"]["regclass"] = None
    fake_pg["state"]["create_error"] = _LockNotAvailable("canceling statement due to lock timeout")
    with pytest.raises(_LockNotAvailable):
        service.InventoryEnricher()
    raws = fake_pg["raws"]
    _assert_no_ddl_on_cars(raws)
    assert ("rollback",) in raws[0].events
    assert ("commit",) not in raws[0].events


def test_save_haiku_cache_path_probes_instead_of_creating(fake_pg) -> None:
    """``_save_haiku_cache`` re-ensures the cache table on every save; on Postgres
    with the table present that is one probe and no DDL."""
    conn = service.get_conn()
    try:
        service._ensure_haiku_cache_table(conn)
    finally:
        conn.close()
    stmts = _all_statements(fake_pg["raws"])
    assert stmts == ["SELECT to_regclass('haiku_spec_cache')"]


def test_raw_psycopg_shaped_connection_gets_no_ddl_on_cars() -> None:
    """Fail closed: a connection that is not positively SQLite never reaches
    ``ALTER TABLE cars`` (raw psycopg connection, no compat wrapper)."""
    raw = _FakePgRaw(regclass="haiku_spec_cache")
    service.ensure_enrichment_columns(raw)
    assert raw.statements == ["SELECT to_regclass('haiku_spec_cache')"]


def test_dict_row_probe_result_is_read() -> None:
    assert service._first_value({"to_regclass": "haiku_spec_cache"}) == "haiku_spec_cache"
    assert service._first_value({"to_regclass": None}) is None
    assert service._first_value(None) is None
    assert service._first_value((None,)) is None


# ---------------------------------------------------------------------------
# SQLite: unchanged
# ---------------------------------------------------------------------------


def _sqlite_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_sqlite_raw_connection_still_adds_missing_columns() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT)")
        service.ensure_enrichment_columns(conn)
        assert _ENRICHMENT_COLS <= _sqlite_cols(conn, "cars")
        assert "cache_key" in _sqlite_cols(conn, "haiku_spec_cache")
        # Idempotent.
        service.ensure_enrichment_columns(conn)
        assert _ENRICHMENT_COLS <= _sqlite_cols(conn, "cars")
    finally:
        conn.close()


def test_sqlite_compat_connection_still_adds_missing_columns() -> None:
    raw = sqlite3.connect(":memory:")
    conn = InventoryConnection(raw, backend="sqlite")
    try:
        raw.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, engine_l TEXT)")
        service.ensure_enrichment_columns(conn)
        assert _ENRICHMENT_COLS <= _sqlite_cols(raw, "cars")
        assert "created_at" in _sqlite_cols(raw, "haiku_spec_cache")
    finally:
        conn.close()
