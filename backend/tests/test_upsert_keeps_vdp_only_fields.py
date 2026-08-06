"""An SRP-only rescan must not wipe VDP-only columns.

``upsert_vehicles``' ON CONFLICT clause states the contract that an empty
incoming value never overwrites a stored one, and it honours that for price,
mileage, colors, gallery, packages and the rest. Three columns were assigned
straight from ``excluded.*`` instead: ``carfax_url``, ``history_highlights`` and
``msrp``.

All three are VDP/window-sticker facts that never appear on an SRP card, so a
refresh that only re-reads the search-results feed (``nightly_http_refresh``, a
recipe replay, any run with the VDP queue capped) upserts the same VIN with
carfax_url=NULL, history_highlights='[]' and msrp=NULL -- and used to blank all
three on the stored row. ``carfax_url`` has a second line of defence in
``backend/scanner/vdp/prefetch.py`` (``_DB_MERGE_TEXT_FIELDS``); ``msrp`` and
``history_highlights`` have none, and the merge only runs when the prefetch path
does.
"""

from __future__ import annotations

import json
import sqlite3

from backend.scanner.database import upsert_vehicles

VIN = "1FTFW1ET5DFA00001"


def _seed_db(tmp_path, monkeypatch):
    db_path = tmp_path / "inventory.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    # inv.DB_PATH is resolved at import time; patch the attribute too.
    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    return db_path


def _row(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT price, msrp, carfax_url, history_highlights FROM cars WHERE vin=?",
            (VIN,),
        ).fetchone()
    finally:
        conn.close()


def test_srp_only_rescan_keeps_msrp_carfax_and_history(tmp_path, monkeypatch):
    db_path = _seed_db(tmp_path, monkeypatch)

    # Scan 1: full VDP visit.
    upsert_vehicles(
        [
            {
                "vin": VIN,
                "title": "2024 Ford F-150 XLT",
                "year": 2024,
                "make": "Ford",
                "model": "F-150",
                "price": 55000,
                "msrp": 61000,
                "condition": "Used",
                "carfax_url": f"https://www.carfax.com/VehicleHistory/p/Report.cfx?vin={VIN}",
                "history_highlights": ["No accidents reported", "1 owner"],
            }
        ]
    )
    price, msrp, carfax, highlights = _row(db_path)
    assert (msrp, carfax) == (61000, f"https://www.carfax.com/VehicleHistory/p/Report.cfx?vin={VIN}")
    assert json.loads(highlights) == ["No accidents reported", "1 owner"]

    # Scan 2: SRP card only — price moved, no VDP fields in the payload.
    upsert_vehicles(
        [
            {
                "vin": VIN,
                "title": "2024 Ford F-150 XLT",
                "year": 2024,
                "make": "Ford",
                "model": "F-150",
                "price": 54000,
                "condition": "Used",
            }
        ]
    )
    price, msrp, carfax, highlights = _row(db_path)

    # The fresh fact still wins...
    assert price == 54000
    # ...and the VDP-only facts survive.
    assert msrp == 61000, "msrp wiped by an SRP-only rescan"
    assert carfax and "carfax.com" in carfax, "carfax_url wiped by an SRP-only rescan"
    assert json.loads(highlights or "[]") == [
        "No accidents reported",
        "1 owner",
    ], "history_highlights wiped by an SRP-only rescan"


def test_rescan_still_overwrites_with_a_real_value(tmp_path, monkeypatch):
    """Keep-if-nonempty must not become keep-always."""
    db_path = _seed_db(tmp_path, monkeypatch)
    upsert_vehicles(
        [
            {
                "vin": VIN,
                "title": "2024 Ford F-150 XLT",
                "year": 2024,
                "make": "Ford",
                "model": "F-150",
                "price": 55000,
                "msrp": 61000,
                "carfax_url": "https://example.com/old",
                "history_highlights": ["stale"],
            }
        ]
    )
    upsert_vehicles(
        [
            {
                "vin": VIN,
                "title": "2024 Ford F-150 XLT",
                "year": 2024,
                "make": "Ford",
                "model": "F-150",
                "price": 55000,
                "msrp": 62500,
                "carfax_url": "https://example.com/new",
                "history_highlights": ["fresh"],
            }
        ]
    )
    _, msrp, carfax, highlights = _row(db_path)
    assert msrp == 62500
    assert carfax == "https://example.com/new"
    assert json.loads(highlights) == ["fresh"]
