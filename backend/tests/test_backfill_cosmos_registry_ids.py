"""N21.10: cosmos registry-id backfill plan (Postgres tier; skipped without TEST_PG_ADMIN_DSN)."""
from __future__ import annotations

import json

import pytest

from backend.scripts import backfill_cosmos_registry_ids as bf

COSMOS = json.dumps([{"url": "https://x.example/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/1/2"}])


def _reg(conn, rid, url):
    conn.execute(
        "INSERT INTO dealerships (id, name, city, state, website_url, is_active) "
        "VALUES (%s, %s, 'Testville', 'NC', %s, 1)",
        (rid, f"R{rid}", url),
    )


def _car(conn, vin, dealer_id, url, rid=None):
    conn.execute(
        "INSERT INTO cars (vin, dealer_id, dealer_url, dealership_registry_id, listing_active) "
        "VALUES (%s, %s, %s, %s, 1)",
        (vin, dealer_id, url, rid),
    )


@pytest.mark.pg
def test_plan_fills_single_host_and_refuses_shared_and_unknown(pg_db):
    with pg_db.connect() as c:
        _reg(c, 1, "https://www.solo.com")
        _reg(c, 2, "https://www.shared.com")
        _reg(c, 3, "https://www.shared.com")
        for did in ("solo-com", "shared-com", "ghost-com", "nonc-com"):
            c.execute("INSERT INTO dealer_recipes (dealer_id, recipes_json) VALUES (%s, %s)",
                      (did, COSMOS if did != "nonc-com" else "[]"))
        _car(c, "V1", "solo-com", "https://www.solo.com/x")
        _car(c, "V2", "shared-com", "https://www.shared.com/x")
        _car(c, "V3", "ghost-com", "https://www.ghost.com/x")
        _car(c, "V4", "nonc-com", "https://www.solo.com/x")
        c.commit()
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        plan = bf.build_plan(conn)
        assert plan.cosmos_dealers == 3
        got = {d.dealer_id: (d.registry_id, d.skipped) for d in plan.dealers}
        assert got == {"solo-com": (1, ""), "shared-com": (None, "shared_host"),
                       "ghost-com": (None, "no_registry_row")}
        assert bf.apply_plan(conn, plan) == 1
        assert bf.build_plan(conn).fills == []  # idempotent
        cur = conn.cursor()
        cur.execute("SELECT dealership_registry_id FROM cars WHERE vin = 'V4'")
        assert cur.fetchone()[0] is None  # non-cosmos dealer untouched


@pytest.mark.pg
def test_plan_stamped_siblings_and_ambiguous(pg_db):
    with pg_db.connect() as c:
        for rid in (1, 2, 3):
            _reg(c, rid, f"https://www.r{rid}.com")
        for did in ("sib-com", "amb-com"):
            c.execute("INSERT INTO dealer_recipes (dealer_id, recipes_json) VALUES (%s, %s)", (did, COSMOS))
        _car(c, "S1", "sib-com", "https://www.r1.com/x", 1)
        _car(c, "S2", "sib-com", "https://www.r1.com/x")
        _car(c, "A1", "amb-com", "https://www.r2.com/x", 2)
        _car(c, "A2", "amb-com", "https://www.r3.com/x", 3)
        _car(c, "A3", "amb-com", "https://www.r3.com/x")
        c.commit()
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        got = {d.dealer_id: (d.registry_id, d.source, d.skipped) for d in bf.build_plan(conn).dealers}
        assert got == {"sib-com": (1, "stamped_siblings", ""), "amb-com": (None, "", "ambiguous")}


class _Cur:
    def __init__(self, conn):
        self.c, self.rows = conn, []

    def execute(self, sql, params=()):
        if "dealer_recipes" in sql:
            self.rows = [(d,) for d in self.c.dealers]
        elif "FROM dealerships" in sql:
            self.rows = [(i,) for i in self.c.active]
        else:
            self.rows = self.c.cars.get(params[0], [])

    def fetchall(self):
        return self.rows


class _Conn:
    def __init__(self, dealers, cars, active):
        self.dealers, self.cars, self.active = dealers, cars, active

    def cursor(self):
        return _Cur(self)


def _plan(monkeypatch, stamped, by_host, shared, cars, active=(1, 2, 3)):
    monkeypatch.setattr(bf, "registry_dealer_ids", lambda conn: stamped)
    monkeypatch.setattr(bf, "registry_id_by_dealer_host", lambda conn: by_host)
    monkeypatch.setattr(bf, "shared_registry_hosts", lambda conn: shared)
    conn = _Conn(sorted(cars), cars, active)
    return {d.dealer_id: (d.registry_id, d.source, d.skipped) for d in bf.build_plan(conn).dealers}


def test_build_plan_branching_without_postgres(monkeypatch):
    cars = {
        "sib": [("https://a.com/x", 2)],
        "amb": [("https://b.com/x", 1)],
        "host": [("https://www.solo.com/x", 3)],
        "shr": [("https://www.shared.com/x", 1)],
        "ghost": [("https://www.ghost.com/x", 1)],
        "nourl": [("", 1)],
        "off": [("https://www.off.com/x", 1)],
        "none": [],
    }
    got = _plan(
        monkeypatch,
        stamped={1: "sib", 2: "amb", 3: "amb"},
        by_host={"solo.com": 1, "shared.com": 2, "off.com": 9},
        shared={"shared.com"},
        cars=cars,
    )
    assert got == {
        "sib": (1, "stamped_siblings", ""),
        "amb": (None, "", "ambiguous"),
        "host": (1, "host", ""),
        "shr": (None, "", "shared_host"),
        "ghost": (None, "", "no_registry_row"),
        "nourl": (None, "", "no_url"),
        "off": (None, "", "inactive_registry"),
    }
