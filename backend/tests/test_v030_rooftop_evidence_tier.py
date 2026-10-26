"""V030 (N8.6a): persisted rooftop tier + evidence record on ``cars``.

Static checks on the migration text, plus the SQLite runtime copy in
``schema_repo.ensure_cars_table_columns``. The PG apply path is exercised by the
chain tests (test_schema_from_migrations / test_migrate) and, once N8.6b lands,
by the guard's PG acceptance.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from backend.db.repositories.schema_repo import ensure_cars_table_columns

V030 = Path(__file__).resolve().parents[2] / "migrations" / "V030__rooftop_evidence_tier.sql"


def _sql() -> str:
    return re.sub(r"--[^\n]*", "", V030.read_text(encoding="utf-8"))


def test_v030_adds_exactly_the_two_columns_additively():
    sql = _sql().lower()
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    assert len(stmts) == 2
    assert all(s.startswith("alter table public.cars add column if not exists") for s in stmts)
    assert "rooftop_tier text not null default 'none'" in stmts[0]
    assert "rooftop_evidence text" in stmts[1]
    assert not re.search(r"drop|lock_timeout|create index|update ", sql)


def test_sqlite_runtime_copy_defaults_legacy_rows_to_none(monkeypatch):
    monkeypatch.setattr(
        "backend.db.repositories.schema_repo.is_inventory_postgres", lambda: False
    )
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT)")
    cur.execute("INSERT INTO cars (vin) VALUES ('LEGACY')")
    ensure_cars_table_columns(cur)
    ensure_cars_table_columns(cur)  # idempotent
    cur.execute("SELECT rooftop_tier, rooftop_evidence FROM cars")
    assert cur.fetchall() == [("none", None)]
