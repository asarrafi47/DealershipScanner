"""N6.7 / D-N14: price floor, cap, repeated-value rule, completeness on the displayed price."""
from __future__ import annotations

import pytest

from backend.scanner.upsert.rows import coerce_price
from backend.scripts.repair_junk_prices import find_junk, main
from backend.utils.price_floor import classify_price, repeated_value_pairs


@pytest.mark.parametrize(
    "value,expected",
    [(None, None), ("", None), ("abc", None), (0, None), (-5, None), (10, None), (699, None),
     (999, None), (999.4, None), (1000, 1000), ("1,000", None), ("24999.6", 25000),
     (1_000_000, 1_000_000), (1_000_001, None), (6001323, None)],
)
def test_coerce_price_floor_and_cap(value, expected):
    assert coerce_price(value) == expected


def test_classify_price_names_the_rule():
    assert classify_price(750) == (None, "price_below_floor:750<1000")
    assert classify_price(6001323)[1].startswith("price_above_cap")
    assert classify_price(0) == (None, None)
    assert classify_price(25000) == (25000, None)


def test_repeated_value_needs_five_distinct_models():
    rows = [("d1", f"model{i}", 999) for i in range(5)] + [("d1", "m0", 999)]
    rows += [("d2", f"model{i}", 5000) for i in range(4)]  # only 4 models: kept
    rows += [("d3", "same", 7000)] * 9  # one model, many cars: kept
    assert repeated_value_pairs(rows) == {("d1", 999): 6}


def test_find_junk_classifies_and_spares_real_prices():
    rows = [(i, f"V{i}", "d1", f"m{i}", 1500) for i in range(5)]  # repeated pair
    rows += [(10, "V10", "d2", "x", 699), (11, "V11", "d2", "y", 6001323), (12, "V12", "d2", "z", 31990)]
    got = {j["id"]: j["reason"] for j in find_junk(rows)}
    assert got == {0: "repeated", 1: "repeated", 2: "repeated", 3: "repeated", 4: "repeated",
                   10: "below_floor", 11: "above_cap"}


def test_completeness_judges_the_displayed_price():
    from backend.utils import listing_completeness as lc

    base = {"vin": "1HGCM82633A004352", "year": 2020, "make": "Honda", "model": "Accord", "trim": "EX",
            "title": "2020 Honda Accord", "condition": "New", "mileage": 5,
            "image_url": "https://x.test/a.jpg"}
    assert "price" in lc.listing_missing_field_codes({**base, "price": 699}, for_public_filter=False)
    assert "price" not in lc.listing_missing_field_codes({**base, "price": 25000}, for_public_filter=False)


def test_repair_dry_run_writes_nothing(tmp_path, monkeypatch, capsys):
    from backend.db import inventory_db

    monkeypatch.setattr(inventory_db, "DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    conn = inventory_db.get_conn()
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT, "
                 "model TEXT, price INTEGER, listing_active INTEGER)")
    conn.execute("INSERT INTO cars VALUES (1,'V1','d','m',699,1)")
    conn.commit()
    conn.close()
    assert main([]) == 0
    conn = inventory_db.get_conn()
    assert conn.execute("SELECT price FROM cars WHERE id=1").fetchone()[0] == 699


def _seed_db(tmp_path, monkeypatch):
    from backend.db import inventory_db

    monkeypatch.setattr(inventory_db, "DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    conn = inventory_db.get_conn()
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT, "
                 "model TEXT, price INTEGER, listing_active INTEGER)")
    conn.execute("INSERT INTO cars VALUES (1,'V1','d','m',699,1)")
    conn.execute("INSERT INTO cars VALUES (2,'V2','d','m',31990,1)")
    conn.execute("INSERT INTO cars VALUES (3,'V3','d','m',6001323,1)")
    conn.execute("INSERT INTO cars VALUES (4,'V4','d','m',500,0)")  # inactive: untouched
    conn.commit()
    conn.close()
    return inventory_db


def test_repair_apply_writes_backup_and_nulls_only_junk(tmp_path, monkeypatch):
    inventory_db = _seed_db(tmp_path, monkeypatch)
    backup = tmp_path / "backup.csv"
    assert main(["--apply", "--backup-file", str(backup)]) == 0
    lines = backup.read_text().splitlines()
    assert lines[0] == "id,vin,dealer_id,price,reason"
    assert sorted(l.split(",")[0] for l in lines[1:]) == ["1", "3"]
    conn = inventory_db.get_conn()
    got = dict(conn.execute("SELECT id, price FROM cars").fetchall())
    assert got == {1: None, 2: 31990, 3: None, 4: 500}


def test_repair_apply_without_backup_is_refused(tmp_path, monkeypatch):
    inventory_db = _seed_db(tmp_path, monkeypatch)
    assert main(["--apply"]) == 2
    conn = inventory_db.get_conn()
    assert conn.execute("SELECT price FROM cars WHERE id=1").fetchone()[0] == 699
