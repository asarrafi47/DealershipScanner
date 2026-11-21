"""B-003: the inventory trim query must be valid under Postgres GROUP BY rules."""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager

from backend.enrichment.trim_ladder import inventory


def _run(monkeypatch, conn):
    seen: list[str] = []

    class _Cur:
        def __init__(self, c):
            self._c = c

        def execute(self, sql, params=()):
            seen.append(sql)
            self._c.execute(sql.replace("?", "?"), params)
            return self

        def fetchall(self):
            return self._c.fetchall()

    @contextmanager
    def fake_db_conn():
        class _W:
            def cursor(self_inner):
                return _Cur(conn.cursor())

        yield _W()

    monkeypatch.setattr("backend.db.inventory_db.db_conn", fake_db_conn)
    rows = inventory._inventory_trim_rows("Honda", "Civic", 0, 9999)
    return rows, seen[0]


def test_select_trim_is_aggregated_not_bare(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE cars (make TEXT, model TEXT, year INT, trim TEXT, price REAL, listing_active INT)"
    )
    conn.executemany(
        "INSERT INTO cars VALUES ('Honda','Civic',2022,?,?,1)",
        [("LX", 20000), ("lx", 21000), ("EX", 25000)],
    )
    rows, sql = _run(monkeypatch, conn)
    select_list = re.search(r"SELECT(.*?)FROM", sql, re.S).group(1)
    # Postgres rejects a bare ``trim`` when grouping by LOWER(TRIM(trim)).
    assert re.search(r"MIN\(\s*trim\s*\)", select_list)
    assert not re.match(r"\s*trim\s*,", select_list)
    assert len(rows) == 2
    assert sorted(r[2] for r in rows) == [1, 2]
