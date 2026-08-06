"""``last_price_change_at`` must only move when the stored price moves.

The upsert deliberately keeps the stored price when the incoming one is missing
(hidden price, "call for price", a feed that dropped the field). The
``last_price_change_at`` CASE compared ``COALESCE(excluded.price, 0)`` though, so
a missing price read as "the price changed to 0" and stamped the row with the
scan timestamp -- on the same statement that had just decided not to touch the
price at all.

Live check on the inventory at the time this was written: of 2,285 active rows
whose ``last_price_change_at`` equalled their last ``scraped_at``, 247 had no
snapshot at that timestamp in their own ``price_provenance_json`` price history,
i.e. the stamp recorded a change that never happened.
``backend/dealer/admin/merchandising.py`` anchors price aging on this column, so
those cars read as permanently just-repriced.
"""

from __future__ import annotations

import sqlite3

from backend.scanner.database import upsert_vehicles

VIN = "1N6ED1EK5TN000001"


def _setup(tmp_path, monkeypatch):
    db_path = tmp_path / "inventory.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    return db_path


def _row(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT price, scraped_at, last_price_change_at FROM cars WHERE vin=?",
            (VIN,),
        ).fetchone()
    finally:
        conn.close()


def _payload(**over):
    base = {
        "vin": VIN,
        "title": "2026 Nissan Frontier SV",
        "year": 2026,
        "make": "Nissan",
        "model": "Frontier",
        "price": 34034,
    }
    base.update(over)
    return base


def test_missing_price_does_not_stamp_a_price_change(tmp_path, monkeypatch):
    db_path = _setup(tmp_path, monkeypatch)
    upsert_vehicles([_payload()])
    _, _, first_stamp = _row(db_path)

    # Rescan with the price missing: the stored price is kept, so nothing changed.
    upsert_vehicles([_payload(price=None)])
    price, scraped_at, stamp = _row(db_path)

    assert price == 34034, "stored price should survive a scan with no price"
    assert scraped_at != first_stamp, "the rescan should have advanced scraped_at"
    assert stamp == first_stamp, (
        "last_price_change_at was stamped for a price that did not change"
    )


def test_real_price_move_still_stamps(tmp_path, monkeypatch):
    db_path = _setup(tmp_path, monkeypatch)
    upsert_vehicles([_payload()])
    _, _, first_stamp = _row(db_path)

    upsert_vehicles([_payload(price=32500)])
    price, scraped_at, stamp = _row(db_path)

    assert price == 32500
    assert stamp == scraped_at != first_stamp, "a real price drop must move the stamp"


def test_first_price_after_a_priceless_listing_stamps(tmp_path, monkeypatch):
    db_path = _setup(tmp_path, monkeypatch)
    upsert_vehicles([_payload(price=None)])
    _, _, first_stamp = _row(db_path)

    upsert_vehicles([_payload(price=34034)])
    price, scraped_at, stamp = _row(db_path)

    assert price == 34034
    assert stamp == scraped_at != first_stamp
