"""Round-trip test for the dealer_specials store (SQLite backend)."""
from __future__ import annotations

import os

import pytest

from backend.scanner.specials.store import (
    ensure_dealer_specials_table,
    get_specials_for_dealer,
    upsert_specials,
)


@pytest.fixture()
def sqlite_inventory(tmp_path, monkeypatch):
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    from backend.db import inventory_db

    db_path = os.path.join(str(tmp_path), "inv.db")
    monkeypatch.setattr(inventory_db, "DB_PATH", db_path)
    return inventory_db


def _offer(**kw):
    base = {
        "title": "New 2026 Kia K4 LXS", "type": "lease", "vehicle_year": 2026,
        "vehicle_make": "Kia", "vehicle_model": "K4", "vehicle_trim": "LXS",
        "payment": 256.0, "term_months": 36, "due_at_signing": 2719.0,
        "mileage_per_year": 10000, "msrp": 24635.0, "expires": "08/03/2026",
        "fine_print": "Plus tax and license.", "source_url": "https://x.com/specials",
        "raw_html_snippet": "<div>...</div>", "offer_hash": "hash-a",
    }
    base.update(kw)
    return base


def test_store_roundtrip_and_replace(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        ensure_dealer_specials_table(conn)
        n = upsert_specials(conn, "ggkia-com", [_offer(), _offer(offer_hash="hash-b", type="finance", payment=None)])
        assert n == 2
        rows = get_specials_for_dealer(conn, "ggkia-com")
        assert len(rows) == 2
        # lease sorts before finance
        assert rows[0]["type"] == "lease"
        assert rows[0]["payment"] == 256.0
        assert rows[0]["msrp"] == 24635.0

        # Replace with a single fresh offer clears the prior set.
        n2 = upsert_specials(conn, "ggkia-com", [_offer(offer_hash="hash-c", title="New deal")])
        assert n2 == 1
        rows2 = get_specials_for_dealer(conn, "ggkia-com")
        assert len(rows2) == 1
        assert rows2[0]["title"] == "New deal"

        # Other dealers are untouched.
        assert get_specials_for_dealer(conn, "someone-else-com") == []
    finally:
        conn.close()
