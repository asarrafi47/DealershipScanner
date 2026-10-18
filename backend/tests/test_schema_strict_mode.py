"""
P4.5: ``INVENTORY_SCHEMA_CHECK=strict`` propagates out of the two broad catches.

``schema_repo.init_inventory_db`` (web, scanner and worker start-up) and the
scanner's per-upsert ``database._ensure_schema`` both wrap
``inventory_pg.init_postgres_inventory`` in ``except Exception`` so a busy table
(the legacy DDL's 3 s lock_timeout) cannot take the web app or a scan down. Before
P4.5 that catch also swallowed :class:`SchemaNotMigratedError`, so strict mode
logged a warning and carried on: the web failed late in ``init_dealer_portal_db``
and the scanner wrote on every upsert. Strict now refuses at the first call; warn
mode and the busy-table fallback are unchanged.

No database is touched: Postgres is a fake connection that answers the two
catalog reads ``schema_version.ensure_schema_current`` issues, and the migration
chain is a temporary directory of empty ``V<NNN>__*.sql`` files.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from backend.db import incomplete_listings_db, inventory_pg
from backend.db import schema_version as sv
from backend.db.repositories import schema_repo
from backend.scanner import database as scanner_db


class _FakeCursor:
    def __init__(self, db: "_FakeConn"):
        self.db = db
        self._rows: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.db.queries.append(sql)
        if "to_regclass('public.cars')" in sql:
            self._rows = [("cars",)]
        elif "to_regclass" in sql:
            self._rows = [("schema_migrations",)]
        elif "FROM public.schema_migrations" in sql:
            self._rows = [(v,) for v in sorted(self.db.versions)]
        else:
            raise AssertionError(f"unexpected SQL (a DDL or write reached the fake DB): {sql}")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self) -> None:
        pass


class _FakeConn:
    """Inventory Postgres that records ``versions`` in ``schema_migrations``."""

    def __init__(self, versions: set[int]):
        self.versions = versions
        self.queries: list[str] = []
        self.rollbacks = 0
        self.closed = 0
        self.commits = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def rollback(self) -> None:
        self.rollbacks += 1

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed += 1


def _write_chain(directory: Path, top: int) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for v in range(1, top + 1):
        (directory / f"V{v:03d}__step_{v}.sql").write_text("SELECT 1;\n", encoding="utf-8")
    return directory


def _reset_caches() -> None:
    # Clears inventory_pg._PG_INV_SCHEMA_OK and schema_version._current.
    inventory_pg.reset_postgres_inventory_schema_cache()


@pytest.fixture
def pg(tmp_path, monkeypatch):
    """A fake Postgres inventory behind a 3-file chain; returns a hook dict.

    ``legacy`` collects every connection the warn-mode legacy DDL was run on (it is
    replaced by a recorder, so no CREATE is ever issued); ``seeded`` counts
    ``seed_cars`` calls after a successful init.
    """
    monkeypatch.setattr(sv, "MIGRATIONS_DIR", _write_chain(tmp_path / "migrations", 3))
    # setenv(""), not delenv: a deleted key can be refilled from .env by a reload.
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "")
    monkeypatch.setenv("INVENTORY_AUTO_MIGRATE", "")
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(schema_repo, "is_inventory_postgres", lambda: True)

    hooks: dict[str, Any] = {"conn": _FakeConn({1, 2}), "legacy": [], "seeded": 0, "index": 0}

    monkeypatch.setattr(inventory_pg, "pg_connect", lambda *a, **k: hooks["conn"])
    monkeypatch.setattr(
        inventory_pg, "_legacy_postgres_inventory_ddl", lambda c: hooks["legacy"].append(c)
    )

    def _seed() -> None:
        hooks["seeded"] += 1

    def _index() -> None:
        hooks["index"] += 1

    monkeypatch.setattr(schema_repo, "seed_cars", _seed)
    monkeypatch.setattr(incomplete_listings_db, "ensure_incomplete_index_built", _index)
    _reset_caches()
    yield hooks
    _reset_caches()


def _strict(monkeypatch) -> None:
    monkeypatch.setenv("INVENTORY_SCHEMA_CHECK", "strict")


# ---------------------------------------------------------------------------
# schema_repo.init_inventory_db
# ---------------------------------------------------------------------------


def test_init_inventory_db_strict_raises_on_behind_database(pg, monkeypatch):
    _strict(monkeypatch)
    with pytest.raises(sv.SchemaNotMigratedError) as exc:
        schema_repo.init_inventory_db()
    assert "missing V003" in str(exc.value) and "strict" in str(exc.value)
    assert pg["legacy"] == [], "strict must not fall back to the legacy DDL"
    assert pg["seeded"] == 0 and pg["index"] == 0, "nothing runs after the refusal"
    assert pg["conn"].closed == 1, "the connection is still closed"


def test_init_inventory_db_public_entry_point_raises_too(pg, monkeypatch):
    # backend.main, scanner/cli.py, post_scan/job.py and the worker loops import it
    # from backend.db.inventory_db.
    from backend.db import inventory_db

    _strict(monkeypatch)
    with pytest.raises(sv.SchemaNotMigratedError):
        inventory_db.init_inventory_db()


def test_init_inventory_db_strict_passes_current_database(pg, monkeypatch):
    _strict(monkeypatch)
    pg["conn"] = _FakeConn({1, 2, 3})
    schema_repo.init_inventory_db()
    assert pg["legacy"] == []
    assert pg["seeded"] == 1 and pg["index"] == 1


def test_init_inventory_db_warn_mode_unchanged(pg, caplog):
    with caplog.at_level(logging.WARNING):
        schema_repo.init_inventory_db()
    assert pg["legacy"] == [pg["conn"]], "warn mode still runs the legacy DDL"
    assert pg["seeded"] == 1 and pg["index"] == 1
    assert "SCHEMA CHECK" in caplog.text and sv.MIGRATE_COMMAND in caplog.text


def test_init_inventory_db_busy_reassert_is_still_swallowed(pg, monkeypatch, caplog):
    def busy(conn):
        raise RuntimeError("canceling statement due to lock timeout")

    monkeypatch.setattr(inventory_pg, "_legacy_postgres_inventory_ddl", busy)
    with caplog.at_level(logging.WARNING, logger=schema_repo.__name__):
        schema_repo.init_inventory_db()
    assert "inventory schema re-assert failed" in caplog.text
    assert "table busy" not in caplog.text
    assert pg["seeded"] == 1, "boot continues after a failed re-assert"
    assert pg["conn"].closed == 1


# ---------------------------------------------------------------------------
# scanner database._ensure_schema and upsert_vehicles
# ---------------------------------------------------------------------------


def test_scanner_ensure_schema_strict_raises(pg, monkeypatch):
    _strict(monkeypatch)
    with pytest.raises(sv.SchemaNotMigratedError) as exc:
        scanner_db._ensure_schema(pg["conn"])
    assert "missing V003" in str(exc.value)
    assert pg["legacy"] == []
    # ensure_schema_current already ended its read transaction before raising.
    assert pg["conn"].rollbacks == 1


def test_scanner_ensure_schema_unwraps_raw_connection(pg, monkeypatch):
    class _Wrapped:
        _backend = "postgres"

        def __init__(self, raw):
            self._raw = raw

    _strict(monkeypatch)
    with pytest.raises(sv.SchemaNotMigratedError):
        scanner_db._ensure_schema(_Wrapped(pg["conn"]))
    assert pg["conn"].queries, "the check ran on the raw connection"


def test_scanner_ensure_schema_strict_passes_current_database(pg, monkeypatch):
    _strict(monkeypatch)
    conn = _FakeConn({1, 2, 3})
    scanner_db._ensure_schema(conn)
    assert pg["legacy"] == []


def test_scanner_ensure_schema_warn_mode_unchanged(pg):
    scanner_db._ensure_schema(pg["conn"])
    assert pg["legacy"] == [pg["conn"]]


def test_scanner_ensure_schema_busy_reassert_is_still_swallowed(pg, monkeypatch, caplog):
    at_failure: list[int] = []

    def busy(conn):
        at_failure.append(conn.rollbacks)
        raise RuntimeError("canceling statement due to lock timeout")

    monkeypatch.setattr(inventory_pg, "_legacy_postgres_inventory_ddl", busy)
    with caplog.at_level(logging.WARNING, logger=scanner_db.__name__):
        scanner_db._ensure_schema(pg["conn"])
    assert "inventory schema re-assert failed" in caplog.text
    assert "table busy" not in caplog.text
    assert pg["conn"].rollbacks == at_failure[0] + 1, "the aborted transaction is rolled back"


def test_upsert_vehicles_strict_refuses_before_any_write(pg, monkeypatch):
    """The per-upsert check used to swallow the refusal and write anyway (:384)."""
    _strict(monkeypatch)
    conn = pg["conn"]
    monkeypatch.setattr(scanner_db, "get_conn", lambda: conn)
    monkeypatch.setattr(scanner_db, "drop_unattributable_vehicles", lambda v: (v, 0))

    def no_write(*a, **k):
        raise AssertionError("write_rows reached on a refused schema")

    monkeypatch.setattr(scanner_db, "prefetch_existing", no_write)
    monkeypatch.setattr(scanner_db, "write_rows", no_write)
    monkeypatch.setattr(scanner_db, "run_post_write_enrichment", no_write)

    with pytest.raises(sv.SchemaNotMigratedError):
        scanner_db.upsert_vehicles([{"vin": "1HGCM82633A004352", "dealer_id": "x"}])
    assert conn.closed == 1 and conn.commits == 0


# ---------------------------------------------------------------------------
# N5 exit gate: strict refuses a database missing V026
# ---------------------------------------------------------------------------


def test_strict_refuses_database_missing_v026(pg, monkeypatch, tmp_path):
    monkeypatch.setattr(sv, "MIGRATIONS_DIR", _write_chain(tmp_path / "chain26", 26))
    _strict(monkeypatch)
    pg["conn"] = _FakeConn(set(range(1, 26)))  # V001-V025: prod and local per P0A.2
    with pytest.raises(sv.SchemaNotMigratedError) as exc:
        schema_repo.init_inventory_db()
    msg = str(exc.value)
    assert "at V025" in msg and "expected V026" in msg and "missing V026" in msg
    with pytest.raises(sv.SchemaNotMigratedError):
        scanner_db._ensure_schema(_FakeConn(set(range(1, 26))))


def test_strict_refuses_database_one_behind_the_repo_chain(pg, monkeypatch):
    """Against the real ``migrations/``: one version short of the head is refused."""
    monkeypatch.setattr(sv, "MIGRATIONS_DIR", Path(sv.__file__).resolve().parents[2] / "migrations")
    head = sv.expected_schema_version()
    assert head >= 25
    _strict(monkeypatch)
    with pytest.raises(sv.SchemaNotMigratedError) as exc:
        scanner_db._ensure_schema(_FakeConn(set(range(1, head))))
    assert f"missing V{head:03d}" in str(exc.value)
    scanner_db._ensure_schema(_FakeConn(set(range(1, head + 1))))  # at the head: passes


# ---------------------------------------------------------------------------
# P4.5 docstring correction (P0A.2: prod and local record V001-V025)
# ---------------------------------------------------------------------------


def test_no_docstring_claims_prod_at_v018():
    for mod in (sv, inventory_pg):
        text = Path(mod.__file__).read_text(encoding="utf-8")
        assert "V018" not in text, f"{mod.__name__} still names V018"
