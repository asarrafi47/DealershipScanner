"""N6.2: cars touched by late writers (vPIC heal, model_specs, catalog link) are re-synced into the index."""
from __future__ import annotations

import sqlite3

from backend.enrichment import vpic_facts
from backend.scanner.upsert import post_write


def test_heal_rows_resyncs_cars_whose_fuel_and_drive_changed(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE epa_master (id INTEGER PRIMARY KEY, drive TEXT)")
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, vin TEXT, dealer_id TEXT, listing_active INT, "
                 "drivetrain TEXT, fuel_type TEXT, cylinders TEXT, spec_source_json TEXT, epa_master_id INT, "
                 "engine_description TEXT, body_style TEXT)")
    conn.execute("INSERT INTO cars (id, vin, dealer_id, listing_active, drivetrain, fuel_type, engine_description, body_style) "
                 "VALUES (7, '1HGCM82633A004352', 'd1', 1, 'FWD', 'Gasoline', 'x', 'y')")
    conn.execute("INSERT INTO cars (id, vin, dealer_id, listing_active, drivetrain, fuel_type, engine_description, body_style) "
                 "VALUES (8, '1HGCM82633A004353', 'd1', 1, 'AWD', 'Gasoline', 'x', 'y')")
    import backend.enrichment.knowledge_engine as ke
    monkeypatch.setattr(ke, "clear_vpic_lookup_cache", lambda: None)
    monkeypatch.setattr(ke, "prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr(ke, "lookup_vpic_from_cache",
                        lambda vin: {"drivetrain": "AWD", "fuel_type": "Hybrid"} if vin.endswith("352") else {})
    monkeypatch.setattr(vpic_facts, "_vpic_flat_rows", lambda c, v: {})
    monkeypatch.setattr(vpic_facts, "vin_overrides",
                        lambda r, vp: {"drivetrain": (r["drivetrain"], "AWD"), "fuel_type": (r["fuel_type"], "Hybrid")} if vp else {})
    monkeypatch.setattr(vpic_facts, "derived_fills", lambda r, vp, flat: {})
    synced: list[int] = []
    from backend.db import incomplete_listings_db as ild
    monkeypatch.setattr(ild, "sync_incomplete_listing_for_car_id", lambda cid: synced.append(cid))
    stats = vpic_facts.heal_rows(conn)
    assert synced == [7]
    assert stats["resynced"] == 1 and "_touched" not in stats
    assert conn.execute("SELECT drivetrain, fuel_type FROM cars WHERE id=7").fetchone() == ("AWD", "Hybrid")


def test_heal_dry_run_does_not_resync(monkeypatch):
    from backend.db import incomplete_listings_db as ild
    called = []
    monkeypatch.setattr(ild, "sync_incomplete_listing_for_car_id", lambda cid: called.append(cid))
    assert vpic_facts._resync_incomplete_index([]) == 0 and not called


def test_post_write_syncs_after_late_writers(monkeypatch):
    order: list[str] = []
    from backend.db import incomplete_listings_db as ild
    monkeypatch.setattr(post_write, "resolve_car_ids", lambda g, v: [1])
    monkeypatch.setattr(ild, "sync_incomplete_listing_for_car_id", lambda cid: order.append("sync"))
    monkeypatch.setattr(post_write, "sync_incomplete_and_backfill_specs", lambda v, g: order.append("backfill"))
    monkeypatch.setattr(post_write, "link_catalog", lambda v: order.append("link"))
    post_write.run_post_write_enrichment(["V"], get_conn=lambda: None,
                                         apply_model_specs_corrections=lambda **k: order.append("specs"))
    assert order == ["backfill", "specs", "link", "sync"]
