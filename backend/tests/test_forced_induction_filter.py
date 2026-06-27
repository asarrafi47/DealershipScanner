"""Listings forced-induction facet: NULL → Naturally Aspirated."""

from __future__ import annotations

import sqlite3

import pytest

from backend.db import inventory_db as idb
from backend.utils.forced_induction import (
    FORCED_INDUCTION_NATURALLY_ASPIRATED,
    forced_induction_filter_label,
    forced_induction_sql_filter_clause,
    sort_forced_induction_filter_options,
)


def test_forced_induction_filter_label_null_is_na() -> None:
    assert forced_induction_filter_label(None) == FORCED_INDUCTION_NATURALLY_ASPIRATED
    assert forced_induction_filter_label("") == FORCED_INDUCTION_NATURALLY_ASPIRATED
    assert forced_induction_filter_label("Turbocharged") == "Turbocharged"


def test_sort_forced_induction_filter_options_na_first() -> None:
    out = sort_forced_induction_filter_options(["Turbocharged", None, "Supercharged", ""])
    assert out[0] == FORCED_INDUCTION_NATURALLY_ASPIRATED
    assert "Turbocharged" in out


def test_search_cars_forced_induction_na_and_turbo(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "inv.db"
    monkeypatch.setattr(idb, "DB_PATH", str(db_path))
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    from backend.db.cars_columns import reset_cars_columns_cache

    reset_cars_columns_cache()
    idb.init_inventory_db()

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO cars (vin, make, model, year, forced_induction, listing_active)
        VALUES ('VINNA000000000001', 'Test', 'Sedan', 2020, NULL, 1),
               ('VINTURBO000000002', 'Test', 'Sedan', 2021, 'Turbocharged', 1)
        """
    )
    conn.commit()
    conn.close()

    na_rows = idb.search_cars(
        makes=["Test"],
        forced_induction_types=[FORCED_INDUCTION_NATURALLY_ASPIRATED],
    )
    assert len(na_rows) == 1
    assert na_rows[0]["vin"] == "VINNA000000000001"

    turbo_rows = idb.search_cars(makes=["Test"], forced_induction_types=["Turbocharged"])
    assert len(turbo_rows) == 1
    assert turbo_rows[0]["vin"] == "VINTURBO000000002"


def test_forced_induction_sql_filter_clause_both() -> None:
    clause, params = forced_induction_sql_filter_clause(
        [FORCED_INDUCTION_NATURALLY_ASPIRATED, "Turbocharged"]
    )
    assert "forced_induction IS NULL" in clause
    assert "Turbocharged" in params
