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


def test_get_conn_routes_to_inventory_pg(monkeypatch) -> None:
    """In Postgres mode the module must use the shared inventory connection."""
    import backend.db.dealer_portal_db as ddb
    import backend.db.inventory_db as invdb
    from backend.db import inventory_pg

    sentinel = object()
    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(invdb, "get_conn", lambda: sentinel)
    assert ddb.get_conn() is sentinel


class _PgSimCursor:
    """Runs every statement through the real Postgres SQL adapter (qmark -> %s),
    then converts back to qmarks to execute on SQLite. Any statement that would
    reach psycopg with a stray ``?`` or unmapped SQLite-ism fails here."""

    def __init__(self, raw):
        self._c = raw

    def execute(self, sql, params=None):
        from backend.db import inventory_pg

        adapted = inventory_pg.adapt_sql_for_postgres_execute(sql)
        assert adapted is not None, f"statement became a no-op on Postgres: {sql[:100]}"
        assert "?" not in adapted, f"unconverted qmark placeholder: {adapted[:200]}"
        runnable = adapted.replace("%s", "?").replace("%%", "%")
        self._c.execute(runnable, tuple(params or ()))
        return self

    def __getattr__(self, name):
        return getattr(self._c, name)


class _PgSimConn:
    def __init__(self, path):
        import sqlite3

        self._raw = sqlite3.connect(path)
        self.row_factory = None

    def cursor(self):
        c = self._raw.cursor()
        if self.row_factory is not None:
            c.row_factory = self.row_factory
        return _PgSimCursor(c)

    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        self._raw.close()


def test_dealer_portal_dml_survives_postgres_adaptation(monkeypatch, tmp_path) -> None:
    """Full CRUD roundtrip with every statement passed through
    inventory_pg.adapt_sql_for_postgres_execute / qmarks_to_percent_s."""
    import backend.db.dealer_portal_db as ddb
    import backend.db.inventory_db as invdb
    from backend.db import inventory_pg

    dbf = tmp_path / "pg_sim.db"
    # Create the schema via the plain SQLite path first (the PG DDL uses
    # BIGSERIAL, which SQLite cannot execute), then flip to simulated Postgres.
    monkeypatch.setattr(ddb, "DB_PATH", str(dbf))
    ddb.init_dealer_portal_db()

    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(invdb, "get_conn", lambda: _PgSimConn(str(dbf)))

    vid = ddb.insert_vehicle(9, {"vin": "1hgbh41jxmn109185", "title": "T", "make": "Honda"})
    assert isinstance(vid, int) and vid > 0
    assert ddb.count_user_vehicles_with_vin(9, " 1HGBH41JXMN109185 ") == 1
    rows = ddb.list_vehicles_for_user(9)
    assert len(rows) == 1 and rows[0]["vin"] == "1HGBH41JXMN109185"
    assert rows[0]["gallery"] == []
    ddb.update_vehicle_gallery(9, vid, ["/dealer-uploads/9/1/a.jpg"])
    ddb.update_vehicle_fields(9, vid, {"price": 1000.0, "notes": "50% off LIKE 'http%'"})
    got = ddb.get_vehicle(9, vid)
    assert got["gallery"] == ["/dealer-uploads/9/1/a.jpg"]
    assert got["price"] == 1000.0
    assert got["notes"] == "50% off LIKE 'http%'"
    ddb.delete_vehicles_for_user(9)
    assert ddb.list_vehicles_for_user(9) == []
