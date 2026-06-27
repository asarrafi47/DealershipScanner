"""Tests for cars table column introspection and forced-induction SQL expr."""

from __future__ import annotations

import sqlite3

from backend.db import inventory_db as idb
from backend.db.cars_columns import (
    cars_forced_induction_sql_expr,
    cars_has_column,
    reset_cars_columns_cache,
)


def test_cars_forced_induction_expr_uses_column_when_present(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "inv.db"
    monkeypatch.setattr(idb, "DB_PATH", str(db_path))
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    reset_cars_columns_cache()
    idb.init_inventory_db()
    assert cars_has_column("forced_induction")
    assert cars_forced_induction_sql_expr() == "cars.forced_induction"


def test_cars_forced_induction_expr_epa_fallback_without_column(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "inv.db"
    monkeypatch.setattr(idb, "DB_PATH", str(db_path))
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT UNIQUE, year INTEGER, make TEXT, model TEXT, trim TEXT)"
    )
    conn.execute(
        "CREATE TABLE epa_master (id INTEGER PRIMARY KEY, year INTEGER, make TEXT, model TEXT, trim TEXT, forced_induction TEXT)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    reset_cars_columns_cache()

    def _fake_postgres() -> bool:
        return False

    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", _fake_postgres)
    reset_cars_columns_cache()
    assert not cars_has_column("forced_induction")
    expr = cars_forced_induction_sql_expr()
    assert "epa_master" in expr
    assert "forced_induction" in expr
