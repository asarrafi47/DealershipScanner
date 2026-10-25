"""N6.1: chunked, staged, locked incomplete-index rebuild with a rules_version trigger."""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from backend.db import incomplete_listings_db as ild
from backend.db import inventory_db as inv_db


@pytest.fixture()
def dbs(monkeypatch: pytest.MonkeyPatch, tmp_path):
    inv_path, inc_path = tmp_path / "inventory.db", tmp_path / "incomplete_listings.db"
    monkeypatch.setattr(inv_db, "DB_PATH", str(inv_path))
    monkeypatch.setattr(ild, "DB_PATH", str(inc_path))
    conn = sqlite3.connect(inv_path)
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, listing_active INTEGER)")
    # ids 1..10; id 10 is retired; odd ids "incomplete"
    conn.executemany(
        "INSERT INTO cars (id, vin, listing_active) VALUES (?,?,?)",
        [(i, f"VIN{i}", 0 if i == 10 else 1) for i in range(1, 11)],
    )
    conn.commit()
    conn.close()
    # stale row the rebuild must clear
    inc = sqlite3.connect(inc_path)
    ild._ensure_schema(inc)
    inc.execute("INSERT INTO incomplete_listings VALUES (99, 'OLD', '[\"trim\"]', 'x')")
    inc.commit()
    inc.close()

    def fake_rows(cars, now):
        return [(c["id"], c["vin"], '["trim"]', now) for c in cars if c["id"] % 2]

    monkeypatch.setattr(ild, "_missing_rows", fake_rows)
    return inc_path


def _ids(path):
    c = sqlite3.connect(path)
    try:
        return sorted(r[0] for r in c.execute("SELECT car_id FROM incomplete_listings"))
    finally:
        c.close()


def test_chunking_covers_every_active_car_once(dbs) -> None:
    seen: list[list[int]] = []
    real = ild._missing_rows

    def spy(cars, now):
        seen.append([c["id"] for c in cars])
        return real(cars, now)

    with patch.object(ild, "_missing_rows", spy):
        res = ild.rebuild_index_chunked(chunk=4, force=True)
    assert seen == [[1, 2, 3, 4], [5, 6, 7, 8], [9]]  # retired id 10 never read
    assert res["status"] == "rebuilt" and res["chunks"] == 3 and res["rows"] == 5
    assert _ids(dbs) == [1, 3, 5, 7, 9]  # stale 99 gone, swap done


def test_failure_mid_chunk_leaves_live_index_untouched(dbs) -> None:
    calls = {"n": 0}

    def boom(cars, now):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return [(c["id"], c["vin"], "[]", now) for c in cars]

    with patch.object(ild, "_missing_rows", boom):
        with pytest.raises(RuntimeError):
            ild.rebuild_index_chunked(chunk=4, force=True)
    assert _ids(dbs) == [99]
    assert ild.get_index_meta("rules_version") is None


def test_rules_version_trigger(dbs, monkeypatch: pytest.MonkeyPatch) -> None:
    assert ild.index_needs_rebuild()
    assert ild.rebuild_index_chunked(chunk=100)["status"] == "rebuilt"
    assert ild.get_index_meta("rules_version") == ild.INDEX_RULES_VERSION
    assert ild.get_index_meta("last_rebuilt_at")
    assert not ild.index_needs_rebuild()
    assert ild.rebuild_index_chunked(chunk=100)["status"] == "current"  # no-op when current
    monkeypatch.setattr(ild, "INDEX_RULES_VERSION", "bumped")
    assert ild.index_needs_rebuild()
    assert ild.rebuild_index_chunked(chunk=100)["status"] == "rebuilt"
    assert ild.get_index_meta("rules_version") == "bumped"


def test_lock_contention_skips_without_touching_index(dbs) -> None:
    with patch.object(ild, "_try_rebuild_lock", return_value=False):
        res = ild.rebuild_index_chunked(chunk=4, force=True)
        assert res["status"] == "locked"
        assert ild.fast_rebuild_incomplete_listings_index() == 1  # current row count
    assert _ids(dbs) == [99]


def test_lock_released_after_rebuild_and_after_failure(dbs) -> None:
    released = []
    with patch.object(ild, "_release_rebuild_lock", side_effect=lambda c: released.append(1)):
        ild.rebuild_index_chunked(chunk=100, force=True)
        with patch.object(ild, "_missing_rows", side_effect=RuntimeError("x")):
            with pytest.raises(RuntimeError):
                ild.rebuild_index_chunked(chunk=100, force=True)
    assert len(released) == 2


def test_postgres_lock_uses_advisory_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    class Cur:
        def __init__(self):
            self.sql = []

        def execute(self, q, p=()):
            self.sql.append((q, p))

        def fetchone(self):
            return (False,)

    class Conn:
        c = Cur()

        def cursor(self):
            return self.c

    monkeypatch.setattr(ild.inventory_pg, "is_inventory_postgres", lambda: True)
    conn = Conn()
    assert ild._try_rebuild_lock(conn) is False
    assert "pg_try_advisory_lock" in conn.c.sql[0][0]
    ild._release_rebuild_lock(conn)
    assert "pg_advisory_unlock" in conn.c.sql[1][0]
