"""Shared-host dealer identity (N8.14): stores that share one website host are told apart
by dealer_id and their own registry id, never by the URL host.

Fixture: three AutoSavvy-style stores on ``www.autosavvy.com`` (each with its own store
page, street and coordinates) plus one single-store dealer. SQLite only; nothing here
touches a real database.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from backend.db import dealer_geo
from backend.db import inventory_db as inv
from backend.db.dealerships_db import ensure_dealerships_table
from backend.listings import dealer_registry_match as drm
from backend.scanner import database as sdb
from backend.scanner import manifest as mf
from backend.scanner.database import upsert_vehicles

HOST = "https://www.autosavvy.com"
# (id, name, store page, street, city, state, lat, lon)
STORES = [
    (198, "AutoSavvy San Antonio", f"{HOST}/dealers/san-antonio-texas", "15447 Interstate Highway 10 W", "San Antonio", "TX", 29.58, -98.59),
    (301, "AutoSavvy Lindon", f"{HOST}/dealers/lindon-utah/", "9 Lindon Rd", "Lindon", "UT", 40.34, -111.72),
    (302, "AutoSavvy Woods Cross", f"{HOST}/dealers/woods-cross-utah/", "2023 S 625 W", "Woods Cross", "UT", 40.87, -111.89),
]
VIN = "1N4BL4DV0PN3{:05d}"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    path = tmp_path / "shared.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(path))
    monkeypatch.setattr(inv, "DB_PATH", str(path))
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.setattr(inv, "_registry_backfill_ran", True, raising=False)
    inv.init_inventory_db()
    with closing(sqlite3.connect(path)) as c:
        ensure_dealerships_table(c.cursor())
        for rid, name, url, street, city, state, lat, lon in STORES:
            c.execute(
                "INSERT INTO dealerships (id, name, website_url, dealer_website_url, street_address, city, state, "
                "zip_code, latitude, longitude, is_active) VALUES (?,?,?,?,?,?,?,?,?,?,1)",
                (rid, name, url, url, street, city, state, "00000", lat, lon),
            )
        c.execute(
            "INSERT INTO dealerships (id, name, website_url, street_address, city, state, zip_code, latitude, longitude, is_active) "
            "VALUES (400, 'Solo Toyota', 'https://www.solotoyota.com', '1 Solo Way', 'Cary', 'NC', '27511', 35.7, -78.8, 1)"
        )
        c.commit()
    return str(path)


def _car(dealer_id, vin_n, url, rid=None, name=None):
    return {"vin": VIN.format(vin_n), "title": "2020 Nissan Altima", "year": 2020, "make": "Nissan", "model": "Altima",
            "price": 20000, "dealer_id": dealer_id, "dealer_name": name or dealer_id, "dealer_url": url,
            "dealership_registry_id": rid}


def _stored(db_path, vin_n):
    with closing(sqlite3.connect(db_path)) as c:
        return c.execute("SELECT dealer_id, dealership_registry_id, dealer_url FROM cars WHERE vin=?", (VIN.format(vin_n),)).fetchone()


# -- registry helpers --------------------------------------------------------

def test_three_stores_one_host_give_three_registry_rows_and_no_host_answer(db):
    with closing(sqlite3.connect(db)) as c:
        by_host = drm.registry_ids_by_host(c)
        assert by_host["autosavvy.com"] == [198, 301, 302]
        assert drm.shared_registry_hosts(c) == {"autosavvy.com"}
        host_map = drm.registry_id_by_dealer_host(c)
    assert "autosavvy.com" not in host_map and host_map["solotoyota.com"] == 400
    # a host-only car on the shared host resolves to nothing; its own id always wins
    assert drm.resolve_car_dealership_registry_id({"dealer_url": HOST}, host_to_registry=host_map) == 0
    assert drm.resolve_car_dealership_registry_id({"dealer_url": HOST, "dealership_registry_id": 302}, host_to_registry=host_map) == 302
    # no LIKE fallback on a shared host either: the filter is the registry id alone
    clause, params = drm.dealer_registry_sql_filter([302], host_map, placeholders_fn=lambda xs: ",".join("?" * len(xs)))
    assert "LIKE" not in clause and params == [302]


def test_registry_dealer_ids_reads_the_id_most_cars_carry(db):
    upsert_vehicles([_car("autosavvy-com-lindon-ut", 1, HOST, 301), _car("autosavvy-com-lindon-ut", 2, HOST, 301),
                     _car("autosavvy-com", 3, HOST, 301)])
    with closing(sqlite3.connect(db)) as c:
        assert drm.registry_dealer_ids(c) == {301: "autosavvy-com-lindon-ut"}


def test_shared_host_dealer_id_from_town_state_or_registry_id():
    assert drm.shared_host_dealer_id("autosavvy-com", "Woods Cross", "UT", 302) == "autosavvy-com-woods-cross-ut"
    assert drm.shared_host_dealer_id("autosavvy-com", "", "UT", 302) == "autosavvy-com-r302"


# -- the gate's own-store place ----------------------------------------------

def test_gate_place_is_each_stores_own_street_and_never_a_sisters(db):
    from backend.attribution import DealerCtx, registry_place

    assert registry_place(HOST) == {}  # shared host: no store is named by it
    for rid, street in ((198, "15447 Interstate Highway 10 W"), (301, "9 Lindon Rd"), (302, "2023 S 625 W")):
        assert registry_place(HOST, registry_id=rid)["dealer_address"] == street
    ctx = DealerCtx.for_store("autosavvy-com-lindon-ut", "AutoSavvy Lindon", STORES[1][2], 301)
    assert ctx.place["dealer_address"] == "9 Lindon Rd" and ctx.place["dealer_city"] == "Lindon"
    assert DealerCtx.for_store("autosavvy-com-lindon-ut", "AutoSavvy Lindon", HOST).place == {}
    # a single-store dealer still resolves by host
    assert registry_place("https://www.solotoyota.com")["dealer_address"] == "1 Solo Way"


# -- stamping and the upsert -------------------------------------------------

def test_each_stores_scan_stamps_its_own_registry_id_and_keeps_it_on_rescan(db):
    page = {rid: url for rid, _n, url, *_ in STORES}
    for i, (rid, name, url, *_r) in enumerate(STORES):
        did = "autosavvy-com" if rid == 198 else f"autosavvy-com-{i}"
        upsert_vehicles([_car(did, i, url, rid, name)])
    assert [_stored(db, i)[1] for i in range(3)] == [198, 301, 302]
    # a roster-rebuilt rescan: bare host URL and no registry id (nightly slug roster)
    upsert_vehicles([_car("autosavvy-com-1", 1, HOST, None, "AutoSavvy Lindon")])
    did, rid, url = _stored(db, 1)
    assert (did, rid, url) == ("autosavvy-com-1", 301, page[301])  # id and store page both kept
    # the stamped rescan (manifest row with its registry id) may still move the URL
    upsert_vehicles([_car("autosavvy-com-1", 1, HOST, 301, "AutoSavvy Lindon")])
    assert _stored(db, 1)[2] == HOST
    # a single-store dealer's URL still updates exactly as before
    upsert_vehicles([_car("solo", 9, "https://www.solotoyota.com/a", None)])
    upsert_vehicles([_car("solo", 9, "https://www.solotoyota.com", None)])
    assert _stored(db, 9)[2] == "https://www.solotoyota.com/a"  # unstamped bare host never downgrades a deeper URL


def test_enrich_stamp_uses_manifest_id_and_refuses_the_host_fallback(db):
    from types import SimpleNamespace

    from backend.scanner.phases.dealer_run_steps.enrich import stamp_registry_and_source_urls

    def run(dealer, url):
        return SimpleNamespace(dealer=dealer, url=url, all_vehicles=[{"vin": "X", "dealer_url": url}], reg_id=None)

    r = run({"dealership_registry_id": 302}, HOST)
    stamp_registry_and_source_urls(r)
    assert r.reg_id == 302 and r.all_vehicles[0]["dealership_registry_id"] == 302
    r = run({}, HOST)  # shared host, no manifest id: nothing is guessed
    stamp_registry_and_source_urls(r)
    assert not r.reg_id and "dealership_registry_id" not in r.all_vehicles[0]
    r = run({}, "https://www.solotoyota.com")  # one store on the host: resolved as before
    stamp_registry_and_source_urls(r)
    assert r.reg_id == 400


# -- rosters -----------------------------------------------------------------

def test_registry_roster_gives_each_shared_store_its_own_dealer_id(db):
    out = {e["dealership_registry_id"]: e for e in mf._load_dealers_from_db()}
    # lowest id keeps the host id the cars table already uses; the others get town + state
    assert out[198]["dealer_id"] == "autosavvy-com"
    assert out[301]["dealer_id"] == "autosavvy-com-lindon-ut"
    assert out[302]["dealer_id"] == "autosavvy-com-woods-cross-ut"
    assert all(out[r]["shared_host"] == "autosavvy.com" for r in (198, 301, 302))
    assert out[400]["dealer_id"] == "solotoyota-com" and "shared_host" not in out[400]
    assert out[302]["url"] == STORES[2][2]
    # scanned stores keep the id their cars carry, even when it differs from the derivation
    upsert_vehicles([_car("autosavvy-com-south-houston-tx", 5, STORES[2][2], 302)])
    out = {e["dealership_registry_id"]: e for e in mf._load_dealers_from_db()}
    assert out[302]["dealer_id"] == "autosavvy-com-south-houston-tx"
    assert out[198]["dealer_id"] == "autosavvy-com" and out[301]["dealer_id"] == "autosavvy-com-lindon-ut"


def test_cars_roster_carries_registry_id_and_store_page_for_shared_stores(db):
    upsert_vehicles([_car("autosavvy-com", 1, STORES[0][2], 198), _car("autosavvy-com-lindon-ut", 2, STORES[1][2], 301),
                     _car("solotoyota-com", 3, "https://www.solotoyota.com", 400)])
    out = {e["dealer_id"]: e for e in mf._load_active_inventory_dealers()}
    assert out["autosavvy-com-lindon-ut"]["dealership_registry_id"] == 301
    assert out["autosavvy-com-lindon-ut"]["url"] == STORES[1][2]
    assert out["autosavvy-com"]["dealership_registry_id"] == 198
    assert "dealership_registry_id" not in out["solotoyota-com"]


def test_legacy_bulk_stamp_does_not_give_a_row_to_a_second_dealer_id(db):
    # every AutoSavvy car stamped 198 (the scratch DB's state): only the dealer_id with the
    # most cars keeps row 198; the sister's id gets no registry row from the cars
    upsert_vehicles([_car("autosavvy-com", i, HOST, 198) for i in range(5)]
                    + [_car("autosavvy-com-lindon-ut", 7, HOST, 198)])
    out = {e["dealer_id"]: e for e in mf._load_active_inventory_dealers()}
    assert out["autosavvy-com"]["dealership_registry_id"] == 198
    assert "dealership_registry_id" not in out["autosavvy-com-lindon-ut"]


# -- geo ---------------------------------------------------------------------

def test_geo_index_has_no_host_key_for_a_shared_host_and_registry_wins(db):
    with closing(sqlite3.connect(db)) as c:
        geo = dealer_geo.load_dealer_geo_index(c)
        reg = dealer_geo.load_registry_coords_map(c)
    assert "host:autosavvy.com" not in geo and geo["host:solotoyota.com"] == (35.7, -78.8)
    assert dealer_geo.lookup_dealer_coords(HOST, geo) is None
    assert dealer_geo.lookup_dealer_coords(STORES[1][2], geo) == (40.34, -111.72)
    car = {"dealer_url": HOST, "dealership_registry_id": 302}
    assert dealer_geo.lookup_car_coords(car, geo, reg) == (40.87, -111.89)
    assert dealer_geo.lookup_car_coords({"dealer_url": HOST}, geo, reg) is None


def test_zip_search_near_store_b_ranks_b_cars_by_b_coordinates(db):
    from backend.db.repositories.search.ranking import rank_ids_by_distance
    from backend.db.repositories.search.where import SqlWhere

    # every car carries the bare group URL, as the nightly roster wrote them
    upsert_vehicles([_car("a", 1, HOST, 198), _car("b", 2, HOST, 302)])
    origin = (40.87, -111.89)  # Woods Cross, UT
    with closing(sqlite3.connect(db)) as c:
        c.row_factory = sqlite3.Row
        ids, dist = rank_ids_by_distance(c, SqlWhere("1=1"), origin, 25, open_conn=lambda: closing(sqlite3.connect(db)))
        by_vin = {r["id"]: r["vin"] for r in c.execute("SELECT id, vin FROM cars")}
    assert [by_vin[i] for i in ids] == [VIN.format(2)]  # San Antonio's car is 1,100 mi away
    assert dist[ids[0]] == 0.0


def test_market_price_places_a_car_by_its_registry_row_first(db):
    from backend.utils import market_price as mp

    geo = {}
    reg = {"302": [40.87, -111.89]}
    assert mp._car_coords({"dealer_url": HOST, "dealership_registry_id": 302}, geo, reg) == (40.87, -111.89)
    assert mp._car_coords({"dealer_url": HOST}, geo, reg) is None


# -- the dealership page -----------------------------------------------------

def test_each_dealership_id_shows_its_own_address_and_inventory(db):
    from backend.routes import dealership_page as dp

    upsert_vehicles([_car("autosavvy-com", 1, STORES[0][2], 198), _car("autosavvy-com-lindon-ut", 2, STORES[1][2], 301)])
    row, did = dp._resolve_dealer("301")
    assert row["street_address"] == "9 Lindon Rd" and did == "autosavvy-com-lindon-ut"
    row, did = dp._resolve_dealer("198")
    assert row["city"] == "San Antonio" and did == "autosavvy-com"
    # an unscanned sister: lowest id of the host keeps the plain id, the rest are derived
    row, did = dp._resolve_dealer("302")
    assert row["street_address"] == "2023 S 625 W" and did == "autosavvy-com-woods-cross-ut"
    # by dealer_id: the row its cars are stamped with, never the host's first row
    assert dp._find_dealership_by_dealer_id("autosavvy-com-lindon-ut")["id"] == 301
    assert dp._find_dealership_by_dealer_id("autosavvy-com-woods-cross-ut")["id"] == 302
    assert dp._find_dealership_by_dealer_id("autosavvy-com")["id"] == 198
    assert dp._find_dealership_by_dealer_id("autosavvy-com-nowhere-zz") is None
    assert dp._resolve_dealer("400")[1] == "solotoyota-com"


# -- the move script ---------------------------------------------------------

def test_move_destination_on_a_shared_host_uses_the_rows_own_dealer_id():
    from backend.scripts.apply_attribution_moves import destination_dealer_id

    shared, ids = {"autosavvy.com"}, {301: "autosavvy-com-lindon-ut"}
    assert destination_dealer_id(STORES[1][2], 301, shared, ids) == "autosavvy-com-lindon-ut"
    assert destination_dealer_id(STORES[2][2], 302, shared, ids) == ""  # unscanned: skipped, not filed under a sister
    assert destination_dealer_id("https://www.solotoyota.com", 400, shared, ids) == "solotoyota-com"
