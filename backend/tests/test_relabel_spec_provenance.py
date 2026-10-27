"""P8C.5: legacy provenance relabel. Pure cases run everywhere; table cases need the Postgres harness (``pg``)."""
from __future__ import annotations

import json

import pytest

from backend.scripts import relabel_spec_provenance as rel
from backend.vehicle_facts import precedence as prec

pg = pytest.mark.pg


def test_relabels_source_and_keeps_the_rest():
    old = json.dumps({
        "drivetrain": {"source": "inventory_repair", "value": "AWD", "detail": "x"},
        "cylinders": {"source": "listing_gap_fill", "detail": "listing_gap_fill", "url": "u"},
        "fuel_type": {"source": "nhtsa_vpic", "value": "Gasoline"},
    })
    new, changed = rel.relabel_spec(old)
    d = json.loads(new)
    assert d["drivetrain"] == {"source": "unknown", "value": "AWD", "detail": "x", "legacy_source": "inventory_repair"}
    assert d["cylinders"]["source"] == "unknown" and d["cylinders"]["legacy_source"] == "listing_gap_fill"
    assert d["cylinders"]["url"] == "u"
    assert d["fuel_type"] == {"source": "nhtsa_vpic", "value": "Gasoline"}
    assert sorted(changed) == [("inventory_repair", "drivetrain"), ("listing_gap_fill", "cylinders")]


def test_idempotent_and_detail_only_entries_untouched():
    spec = json.dumps({"drivetrain": {"source": "dealer_vdp", "detail": "listing_gap_fill", "value": "AWD"}})
    assert rel.relabel_spec(spec) == (None, [])
    new, _ = rel.relabel_spec(json.dumps({"fuel_type": {"source": "inventory_repair"}}))
    assert rel.relabel_spec(new) == (None, [])


@pytest.mark.parametrize("bad", [None, "", "not json", "[1]", "{}", 5])
def test_unparseable_or_empty_is_untouched(bad):
    assert rel.relabel_spec(bad) == (None, [])


def test_bare_string_form():
    new, changed = rel.relabel_spec('{"drivetrain": "inventory_repair"}')
    assert json.loads(new)["drivetrain"] == {"source": "unknown", "legacy_source": "inventory_repair"}
    assert changed == [("inventory_repair", "drivetrain")]


def test_relabel_keeps_precedence_rank():
    # the stored label changes, the rank precedence already gave it does not
    assert prec._ALIASES["inventory_repair"] == prec.UNKNOWN == rel.UNKNOWN
    assert prec._ALIASES["listing_gap_fill"] == prec.UNKNOWN


def test_table_name_validation_and_apply_gate(capsys):
    assert rel.backup_table_name("20261027") == "cars_provenance_backup_20261027"
    for bad in ("cars", "x; drop table cars", "cars_provenance_backup_A b"):
        with pytest.raises(ValueError):
            rel._check_table(bad)
    assert rel.main(["--apply"]) == 2  # refused before any connection is opened
    assert "--owner-ok" in capsys.readouterr().err


@pytest.fixture
def conn(pg_db):
    c = pg_db.get_conn()
    yield c
    c.close()


def _car(conn, i, spec):
    conn.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, spec_source_json)"
                 " VALUES (?, ?, 'd1', 't', 1, ?)", (i, f"VIN{i:014d}", spec))
    conn.commit()


def _seed(conn):
    _car(conn, 1, json.dumps({"drivetrain": {"source": "inventory_repair", "value": "AWD"},
                              "cylinders": {"source": "listing_gap_fill", "value": 6}}))
    _car(conn, 2, json.dumps({"fuel_type": {"source": "inventory_repair", "value": "Gasoline"}}))
    _car(conn, 3, json.dumps({"drivetrain": {"source": "nhtsa_vpic", "value": "AWD"}}))
    _car(conn, 4, None)


@pg
def test_dry_run_counts_and_writes_nothing(conn):
    _seed(conn)
    p = rel.plan(conn)
    assert p["rows"] == 2 and p["entries"] == 3
    assert p["by_label"] == {"inventory_repair": 2, "listing_gap_fill": 1}
    assert p["by_label_field"]["inventory_repair.drivetrain"] == 1
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM cars WHERE spec_source_json LIKE '%inventory_repair%'")
    assert cur.fetchone()[0] == 2


@pg
def test_apply_backs_up_first_and_counts_match(conn):
    _seed(conn)
    p = rel.plan(conn)
    table = rel.backup_table_name("t1")
    info = rel.create_backup(conn, table, [t[0] for t in p["todo"]])
    assert info["rows"] >= p["rows"]
    assert rel.apply_plan(conn, p["todo"]) == p["rows"]
    after = rel.plan(conn)
    assert after["rows"] == 0
    cur = conn.cursor()
    cur.execute(f"SELECT spec_source_json FROM {table} WHERE id = 2")
    assert "inventory_repair" in cur.fetchone()[0]  # the backup keeps the originals
    cur.execute("SELECT spec_source_json FROM cars WHERE id = 3")
    assert "nhtsa_vpic" in cur.fetchone()[0]


@pg
def test_apply_skips_a_row_rewritten_since_read(conn):
    _seed(conn)
    p = rel.plan(conn)
    conn.execute("UPDATE cars SET spec_source_json = ? WHERE id = 2",
                 (json.dumps({"fuel_type": {"source": "nhtsa_vpic", "value": "Gasoline"}}),))
    conn.commit()
    assert rel.apply_plan(conn, p["todo"]) == 1
    cur = conn.cursor()
    cur.execute("SELECT spec_source_json FROM cars WHERE id = 2")
    assert "nhtsa_vpic" in cur.fetchone()[0]
