"""F12 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): used / CPO rows stored
mileage = 0 when the feed had no odometer (parsers defaulted to 0, the upsert
stored 0, _mileage_blank accepted 0), so 1,307 rows hid the gap (crownlexus:
VDP shows 10,914 where 0 was stored). No odometer must be NULL, and 0 on a
used row is blank."""
from __future__ import annotations

import sqlite3

from backend.parsers import dealer_dot_com
from backend.parsers.base import extract_mileage
from backend.parsers.dealer_dot_com import _extract_mileage_dealer_com
from backend.utils.listing_completeness import _mileage_blank


def test_base_extract_mileage_none_when_absent():
    assert extract_mileage({"vin": "X"}) is None
    assert extract_mileage({"odometer": ""}) is None
    assert extract_mileage({"odometer": "10,914"}) == 10914
    assert extract_mileage({"attributes": {"mileage": 5}}) == 5
    assert extract_mileage({"mileage": 0}) == 0  # an explicit 0 is a reading (new car)


def test_dealer_dot_com_mileage_none_when_no_odometer():
    assert _extract_mileage_dealer_com({"vin": "X", "trackingAttributes": [{"name": "engine", "value": "2.5L"}]}) is None
    assert _extract_mileage_dealer_com({"trackingAttributes": [{"name": "odometer", "value": "10914"}]}) == 10914
    assert _extract_mileage_dealer_com({"trackingAttributes": [{"name": "odometer", "value": ""}], "odometer": 0}) == 0
    row = dealer_dot_com._map_vehicle({"vin": "JTHY3JBH2P2000001", "year": 2023, "make": "Lexus", "model": "RX 350",
                                       "internetPrice": 47000, "condition": "Used", "type": "used"},
                                      "https://www.crownlexus.com", "crownlexus-com", "Crown Lexus", "https://www.crownlexus.com")
    assert row["mileage"] is None


def test_mileage_blank_treats_zero_on_used_as_missing():
    assert _mileage_blank({"mileage": 0, "condition": "Used"})
    assert _mileage_blank({"mileage": 0, "condition": "Certified Pre-Owned"})
    assert _mileage_blank({"mileage": 0, "condition": None, "is_cpo": 1})
    assert _mileage_blank({"mileage": None, "condition": "New"})
    assert not _mileage_blank({"mileage": 0, "condition": "New"})
    assert not _mileage_blank({"mileage": 0, "condition": None})
    assert not _mileage_blank({"mileage": 10914, "condition": "Used"})


def _row(vin, mileage, condition="Used"):
    return {"vin": vin, "title": f"2023 Lexus RX 350 {vin[-3:]}", "year": 2023, "make": "Lexus", "model": "RX 350",
            "trim": "Premium", "price": 47000, "mileage": mileage, "dealer_name": "Crown Lexus", "dealer_id": "crownlexus-com",
            "dealer_url": "https://www.crownlexus.com/", "condition": condition, "fuel_type": "Gasoline"}


def test_upsert_stores_null_not_zero_and_keeps_real_readings(tmp_path, monkeypatch):
    import backend.db.inventory_db as inventory_db
    from backend.scanner.database import upsert_vehicles

    db_path = tmp_path / "f12.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db_path))

    upsert_vehicles([_row("JTHY3JBH2P2000001", None), _row("JTHY3JBH2P2000002", 10914), _row("JTHY3JBH2P2000003", 0, "New")])
    conn = sqlite3.connect(str(db_path))
    got = dict(conn.execute("SELECT vin, mileage FROM cars").fetchall())
    assert got["JTHY3JBH2P2000001"] is None
    assert got["JTHY3JBH2P2000002"] == 10914
    assert got["JTHY3JBH2P2000003"] == 0
    conn.close()

    # rescan: no odometer must not wipe a real reading; an explicit 0 on the new car stays 0
    upsert_vehicles([_row("JTHY3JBH2P2000002", None), _row("JTHY3JBH2P2000003", 0, "New")])
    conn = sqlite3.connect(str(db_path))
    got = dict(conn.execute("SELECT vin, mileage FROM cars").fetchall())
    assert got["JTHY3JBH2P2000002"] == 10914 and got["JTHY3JBH2P2000003"] == 0
    # a stale default 0 on a used row is replaced by NULL when the feed still has no odometer
    conn.execute("UPDATE cars SET mileage = 0 WHERE vin = 'JTHY3JBH2P2000002'")
    conn.commit(); conn.close()
    upsert_vehicles([_row("JTHY3JBH2P2000002", None)])
    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT mileage FROM cars WHERE vin='JTHY3JBH2P2000002'").fetchone()[0] is None
    conn.close()
