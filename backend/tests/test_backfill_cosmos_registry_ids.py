"""N21.10: cosmos registry-id backfill plan (Postgres tier; skipped without TEST_PG_ADMIN_DSN)."""
from __future__ import annotations

import json

import pytest

from backend.scripts import backfill_cosmos_registry_ids as bf

pytestmark = pytest.mark.pg

COSMOS = json.dumps([{"url": "https://x.example/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/1/2"}])


def _reg(conn, rid, url):
    conn.execute(
        "INSERT INTO dealerships (id, name, website_url, is_active) VALUES (%s, %s, %s, 1)",
        (rid, f"R{rid}", url),
    )


def _car(conn, vin, dealer_id, url, rid=None):
    conn.execute(
        "INSERT INTO cars (vin, dealer_id, dealer_url, dealership_registry_id, listing_active) "
        "VALUES (%s, %s, %s, %s, 1)",
        (vin, dealer_id, url, rid),
    )


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
