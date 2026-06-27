"""Tests for slim catalog_trims schema (OEM spine; EPA specs in epa_master)."""

from __future__ import annotations

import sqlite3

from backend.db.catalog_schema import (
    CATALOG_TRIMS_EPA_DUPLICATE_COLUMNS,
    ensure_catalog_tables,
)


def test_catalog_trims_sqlite_has_oem_columns_only() -> None:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    ensure_catalog_tables(cur, postgres=False)
    cur.execute("PRAGMA table_info(catalog_trims)")
    cols = {row[1] for row in cur.fetchall()}
    assert {"year", "make", "model", "trim", "horsepower", "torque_lb_ft", "base_msrp"} <= cols
    for dup in CATALOG_TRIMS_EPA_DUPLICATE_COLUMNS:
        assert dup not in cols
