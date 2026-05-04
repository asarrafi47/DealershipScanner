"""Dealer portal SQLite (isolated file path)."""

from __future__ import annotations

import pytest

from backend.utils.dealer_vin_prefill import build_vehicle_prefill_from_vin


def test_dealer_portal_db_roundtrip(monkeypatch, tmp_path) -> None:
    import backend.db.dealer_portal_db as ddb

    dbf = tmp_path / "dealer_portal_test.db"
    monkeypatch.setattr(ddb, "DB_PATH", str(dbf))
    ddb.init_dealer_portal_db()
    vid = ddb.insert_vehicle(
        42,
        {
            "vin": "1HGBH41JXMN109185",
            "title": "2020 Honda Accord",
            "year": 2020,
            "make": "Honda",
            "model": "Accord",
            "transmission": "CVT",
        },
    )
    rows = ddb.list_vehicles_for_user(42)
    assert len(rows) == 1
    assert rows[0]["vin"] == "1HGBH41JXMN109185"
    assert rows[0]["gallery"] == []
    got = ddb.get_vehicle(42, vid)
    assert got and got["make"] == "Honda"
    ddb.update_vehicle_gallery(42, vid, ["/dealer-uploads/42/1/a.jpg"])
    got2 = ddb.get_vehicle(42, vid)
    assert got2["gallery"] == ["/dealer-uploads/42/1/a.jpg"]
    ddb.update_vehicle_fields(42, vid, {"price": 24000.0, "mileage": 12000})
    got3 = ddb.get_vehicle(42, vid)
    assert got3["price"] == 24000.0
    assert got3["mileage"] == 12000
    assert ddb.delete_vehicle(42, vid)
    assert ddb.list_vehicles_for_user(42) == []


def test_vin_prefill_mocked_vpic() -> None:
    def fake_get(url: str) -> dict:
        assert "decodevinvaluesextended" in url.lower()
        return {
            "Results": [
                {
                    "Make": "HONDA",
                    "Model": "Accord",
                    "ModelYear": "2020",
                    "TransmissionStyle": "CVT",
                    "DriveType": "Front-Wheel Drive (FWD)",
                    "FuelTypePrimary": "Gasoline",
                    "EngineCylinders": "4",
                    "BodyClass": "Sedan/Saloon",
                    "DisplacementL": "1.5",
                    "EngineConfiguration": "In-Line",
                    "ErrorText": "",
                }
            ]
        }

    row, err = build_vehicle_prefill_from_vin("1HGBH41JXMN109185", get_json=fake_get)
    assert err is None
    assert row.get("make") == "Honda"
    assert row.get("vin") == "1HGBH41JXMN109185"
    assert row.get("transmission")


def test_duplicate_vin_same_user_raises(monkeypatch, tmp_path) -> None:
    import backend.db.dealer_portal_db as ddb
    import sqlite3

    dbf = tmp_path / "dp2.db"
    monkeypatch.setattr(ddb, "DB_PATH", str(dbf))
    ddb.init_dealer_portal_db()
    ddb.insert_vehicle(7, {"vin": "1HGBH41JXMN109185", "title": "A"})
    with pytest.raises(sqlite3.IntegrityError):
        ddb.insert_vehicle(7, {"vin": "1HGBH41JXMN109185", "title": "B"})
