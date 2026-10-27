"""P13A.7: snapshot helper writes timestamped tables into ``backups`` and never overwrites."""
from __future__ import annotations

import json
import re
from types import SimpleNamespace

import pytest

from backend.utils import db_snapshot as snap


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._next = None

    def execute(self, sql, params=()):
        self.conn.sql.append((sql, params))
        if "to_regclass" in sql:
            self._next = (params[0] if params[0] in self.conn.tables else None,)
        elif sql.startswith("CREATE TABLE"):
            self.conn.tables.add(sql.split()[2])
        elif sql.startswith("SELECT COUNT"):
            self._next = (self.conn.count,)

    def fetchone(self):
        return self._next


class FakeConn:
    def __init__(self, count=2):
        self.sql, self.tables, self.count = [], set(), count

    def cursor(self):
        return FakeCursor(self)


def test_table_lands_in_backups_schema_with_timestamp():
    conn = FakeConn()
    name = snap.create_snapshot_table(conn, "cars_vinrepair", "SELECT 1", ts="20261026_101500")
    assert name == "backups.cars_vinrepair_20261026_101500"
    assert any(s.startswith("CREATE SCHEMA IF NOT EXISTS backups") for s, _ in conn.sql)
    assert not any("IF NOT EXISTS backups." in s for s, _ in conn.sql)


def test_default_name_carries_a_timestamp():
    name = snap.create_snapshot_table(FakeConn(), "x_bak", "SELECT 1")
    assert re.fullmatch(r"backups\.x_bak_\d{8}_\d{6}", name)


def test_second_snapshot_fails_instead_of_skipping():
    conn = FakeConn()
    snap.create_snapshot_table(conn, "t", "SELECT 1", ts="20261026_101500")
    with pytest.raises(snap.SnapshotExistsError):
        snap.create_snapshot_table(conn, "t", "SELECT 1", ts="20261026_101500")
    assert sum(s.startswith("CREATE TABLE") for s, _ in conn.sql) == 1


def test_row_count_mismatch_raises():
    with pytest.raises(RuntimeError, match="expected 5"):
        snap.create_snapshot_table(FakeConn(count=2), "t", "SELECT 1", ts="1", expected_rows=5)


def test_params_pass_through():
    conn = FakeConn()
    snap.create_snapshot_table(conn, "t", "SELECT id FROM cars WHERE id = ANY(%s)", ([1, 2],), ts="1")
    assert conn.sql[-1][1] == ([1, 2],)


@pytest.mark.parametrize("bad", ["a;drop table x", "a.b.c", "Bad Name", "a-b", ""])
def test_unsafe_names_rejected(bad):
    with pytest.raises(ValueError):
        snap.parse_snapshot_name(bad)


def test_parse_accepts_schema_qualified_and_bare():
    assert snap.parse_snapshot_name("public.epa_extended_specs_bak_20260713_1") == (
        "public", "epa_extended_specs_bak_20260713_1")
    assert snap.parse_snapshot_name("epa_extended_specs_bak_1") == ("backups", "epa_extended_specs_bak_1")


def test_json_snapshot_refuses_overwrite(tmp_path):
    p = snap.write_json_snapshot(str(tmp_path), "vinrepair", [{"id": 1}], ts="20261026_101500")
    assert p.endswith("vinrepair_20261026_101500.json")
    with pytest.raises(snap.SnapshotExistsError):
        snap.write_json_snapshot(str(tmp_path), "vinrepair", [{"id": 2}], ts="20261026_101500")
    assert json.load(open(p)) == [{"id": 1}]


def test_heal_script_uses_helper(tmp_path, monkeypatch):
    from backend.scripts import heal_stock_code_contamination as heal

    conn = FakeConn(count=1)
    plan = SimpleNamespace(car_id=7)
    conn.commit = lambda: None
    t = heal.create_backup_table(conn, [plan], "20261026_101500")
    assert t == "backups.cars_stock_contam_20261026_101500"
    with pytest.raises(snap.SnapshotExistsError):
        heal.create_backup_table(conn, [plan], "20261026_101500")


def test_backfill_and_outlier_scripts_use_helper():
    from backend.scripts import backfill_extended_specs as bf, fix_extended_spec_outliers as fx

    conn = FakeConn()
    assert bf._create_backup_table(conn, [1], "20261026_101500") == (
        "backups.epa_extended_specs_bak_20261026_101500")
    with pytest.raises(snap.SnapshotExistsError):
        bf._create_backup_table(conn, [1], "20261026_101500")
    assert fx._backup_table(FakeConn(), [1]).startswith("backups.epa_extended_specs_bak_")
