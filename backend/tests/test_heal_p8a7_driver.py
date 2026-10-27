"""P8A.7: backup check, guarded rollback, dealer log appender, owner gate.
Name / log tests run everywhere; the table tests need the Postgres harness (``pg``)."""
from __future__ import annotations

import json

import pytest

from backend.scripts import heal_p8a7_driver as drv

pg = pytest.mark.pg
T = "cars_precedence_backup_t1"


def test_backup_name_validation():
    assert drv.backup_table_name("20261026") == "cars_precedence_backup_20261026"
    for bad in ("x; drop table cars", "cars", "cars_precedence_backup_A b"):
        with pytest.raises(ValueError):
            drv._check_table(bad)


def test_append_dealer_logs_is_idempotent(tmp_path):
    lines = {"d1": "- 2026-10-26 P8A.7 x"}
    assert drv.append_dealer_logs(lines, root=tmp_path) == 1  # dry run counts, writes nothing
    assert not (tmp_path / "d1").exists()
    assert drv.append_dealer_logs(lines, root=tmp_path, apply=True) == 1
    assert drv.append_dealer_logs(lines, root=tmp_path, apply=True) == 0
    assert (tmp_path / "d1" / "summary.md").read_text().count("P8A.7") == 1


def test_preflight_requires_mini_check_and_locks():
    class C:
        def cursor(self):
            return self

        def execute(self, *a):
            pass

        def fetchall(self):
            return [(7, "scanner-w1")]

    p = drv.preflight(C(), mini_checked=False, locks=["/x/scanner.lock"])
    assert len(p) == 3
    assert drv.preflight(type("E", (C,), {"fetchall": lambda s: []})(), mini_checked=True, locks=[]) == []


@pytest.fixture
def conn(pg_db):
    c = pg_db.get_conn()
    yield c
    c.close()


def _car(conn, i, fuel, dealer="d1", spec=None, active=1):
    conn.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, fuel_type, spec_source_json)"
                 " VALUES (?, ?, ?, 't', ?, ?, ?)", (i, f"VIN{i:014d}", dealer, active, fuel, spec))
    conn.commit()


def _heal(conn, i, new, *, prior_spec=None):
    """What the heal does to one row: the column and a provenance value."""
    spec = json.loads(prior_spec) if prior_spec else {}
    spec["fuel_type"] = {"source": "nhtsa_vpic", "value": new, "previous": None}
    conn.execute("UPDATE cars SET fuel_type = ?, spec_source_json = ? WHERE id = ?", (new, json.dumps(spec), i))
    conn.commit()


@pg
def test_backup_counts_active_and_cleans_up_on_mismatch(conn):
    _car(conn, 1, "Gasoline")
    _car(conn, 2, "Gasoline", active=0)
    out = drv.create_backup(conn, T)
    assert out["rows"] == out["active"] == 1
    conn.execute(f"DELETE FROM {T}")
    conn.commit()
    with pytest.raises(RuntimeError):
        drv.verify_backup(conn, T)


@pg
def test_guarded_rollback_restores_only_heal_written_values(conn):
    for i in (1, 2, 3):
        _car(conn, i, "Gasoline")
    drv.create_backup(conn, T)
    for i in (1, 2, 3):
        _heal(conn, i, "Plug-In Hybrid")
    conn.execute("UPDATE cars SET fuel_type = 'Diesel' WHERE id = 3")  # a later rescan
    conn.commit()
    dry = drv.guarded_rollback(conn, T)
    assert dry["restored"] == 2 and dry["skipped_not_ours"] == 1 and dry["dry_run"]
    assert conn.execute("SELECT fuel_type FROM cars WHERE id = 1").fetchone()[0] == "Plug-In Hybrid"
    drv.guarded_rollback(conn, T, apply=True)
    rows = dict(conn.execute("SELECT id, fuel_type FROM cars ORDER BY id").fetchall())
    assert rows == {1: "Gasoline", 2: "Gasoline", 3: "Diesel"}
    spec = conn.execute("SELECT spec_source_json FROM cars WHERE id = 1").fetchone()[0]
    assert "fuel_type" not in json.loads(spec)


@pg
def test_dealer_lines_group_transitions(conn):
    _car(conn, 1, "Gasoline", dealer="a")
    _car(conn, 2, "Gasoline", dealer="a")
    _car(conn, 3, None, dealer="b")
    drv.create_backup(conn, T)
    _heal(conn, 1, "Plug-In Hybrid")
    _heal(conn, 2, "Plug-In Hybrid")
    _heal(conn, 3, "Gasoline")
    lines = drv.dealer_lines(conn, T, date="2026-10-26")
    assert "fuel_type Gasoline -> Plug-In Hybrid x2" in lines["a"]
    assert "NULL -> Gasoline x1" in lines["b"] and T in lines["b"]


@pg
def test_apply_needs_owner_ok(conn, monkeypatch, capsys):
    _car(conn, 1, "Gasoline")
    drv.create_backup(conn, T)
    assert drv.main(["heal", "--backup", T, "--apply"]) == 2
    assert "--owner-ok" in capsys.readouterr().err
