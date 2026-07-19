"""Tests for the lease-offer matcher (extraction parsing + inventory matching +
match cache). The LLM call is stubbed so these run offline and deterministically.
"""
from __future__ import annotations

import os

import pytest

from backend.intelligence import lease_matcher as lm


@pytest.fixture()
def sqlite_inventory(tmp_path, monkeypatch):
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    from backend.db import inventory_db

    db_path = os.path.join(str(tmp_path), "inv.db")
    monkeypatch.setattr(inventory_db, "DB_PATH", db_path)
    return inventory_db


def _seed_cars(conn, rows):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin TEXT, stock_number TEXT, year INTEGER, make TEXT, model TEXT,
            trim TEXT, price REAL, msrp REAL, condition TEXT, image_url TEXT,
            dealer_id TEXT, listing_active INTEGER DEFAULT 1
        )
        """
    )
    for r in rows:
        cols = ", ".join(r.keys())
        ph = ", ".join("?" for _ in r)
        cur.execute(f"INSERT INTO cars ({cols}) VALUES ({ph})", tuple(r.values()))
    conn.commit()


# ── Extraction parsing (no live model) ───────────────────────────────────────


def test_extract_parses_json_and_coerces(monkeypatch):
    raw = (
        '{"year": 2026, "make": "Mercedes-Benz", "model": "GLE", '
        '"trim": "GLE 350 4MATIC\\u00ae SUV", "payment": "$399", '
        '"term_months": 24, "due_at_signing": 4999, "mileage_per_year": 7500, '
        '"msrp_or_price": 69965, "credit_tier": null, "expiration": "07/31/2026", '
        '"stock_or_vin_specific": "Applies to stock NL497846"}'
    )
    monkeypatch.setattr(lm, "llm_client", None, raising=False)
    monkeypatch.setattr("backend.utils.llm_client.complete", lambda *a, **k: raw)
    terms = lm.extract_lease_terms({"title": "t", "fine_print": "fp"})
    assert terms["year"] == 2026
    assert terms["make"] == "Mercedes-Benz"
    assert terms["payment"] == 399.0  # "$399" coerced
    assert terms["mileage_per_year"] == 7500
    assert terms["stock_or_vin_specific"] == "Applies to stock NL497846"


def test_extract_recovers_json_from_prose(monkeypatch):
    raw = 'Sure! Here are the terms:\n```json\n{"make":"Kia","model":"K4"}\n```'
    monkeypatch.setattr("backend.utils.llm_client.complete", lambda *a, **k: raw)
    terms = lm.extract_lease_terms({"title": "t", "fine_print": ""})
    assert terms["make"] == "Kia"
    assert terms["model"] == "K4"


def test_extract_returns_none_on_failure(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("model down")

    monkeypatch.setattr("backend.utils.llm_client.complete", boom)
    assert lm.extract_lease_terms({"title": "t", "fine_print": "x"}) is None


# ── Stock/VIN tail matching ──────────────────────────────────────────────────


def test_stock_vin_tail_match():
    # 'NL497846' identifies VIN ...497846
    assert lm._stock_vin_match("NL497846", "4JGFB4FB5TB497846", None)
    assert lm._stock_vin_match("STK12345", None, "STK12345")
    assert not lm._stock_vin_match("NL497846", "4JGFB4FB5TB111111", None)
    assert not lm._stock_vin_match(None, "4JGFB4FB5TB497846", None)


# ── Matching against inventory ───────────────────────────────────────────────


def test_match_narrows_to_stock_specific_vin(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        _seed_cars(
            conn,
            [
                {"vin": "4JGFB4FB5TB497846", "year": 2026, "make": "Mercedes-Benz",
                 "model": "GLE", "trim": "GLE 350 4MATIC® SUV", "price": 69965,
                 "msrp": 69965, "dealer_id": "fj"},
                {"vin": "4JGFB4FB9TB501882", "year": 2026, "make": "Mercedes-Benz",
                 "model": "GLE", "trim": "GLE 350 4MATIC® SUV", "price": 69965,
                 "msrp": 69965, "dealer_id": "fj"},
                {"vin": "WDCXYZ", "year": 2026, "make": "Mercedes-Benz",
                 "model": "GLC", "trim": "GLC 300", "price": 50000, "dealer_id": "fj"},
            ],
        )
        offer = {"vehicle_year": 2026, "vehicle_make": "Mercedes-Benz",
                 "vehicle_model": "GLE", "vehicle_trim": "GLE 350 4MATIC® SUV",
                 "msrp": 69965}
        terms = {"stock_or_vin_specific": "Applies to stock NL497846"}
        matches = lm.match_cars(conn, "fj", terms, offer)
        assert len(matches) == 1
        assert matches[0]["vin"] == "4JGFB4FB5TB497846"
        assert matches[0]["confidence_label"] == "high"
    finally:
        conn.close()


def test_match_trim_fallback_when_no_stock(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        _seed_cars(
            conn,
            [
                {"vin": "A", "year": 2024, "make": "Toyota", "model": "Camry",
                 "trim": "LE", "price": 27000, "dealer_id": "t"},
                {"vin": "B", "year": 2024, "make": "Toyota", "model": "Camry",
                 "trim": "XSE", "price": 34000, "dealer_id": "t"},
            ],
        )
        offer = {"vehicle_year": 2024, "vehicle_make": "Toyota",
                 "vehicle_model": "Camry", "vehicle_trim": "LE"}
        matches = lm.match_cars(conn, "t", None, offer)
        # LE gets high-ish (trim exact), XSE included as low-confidence fallback
        by_vin = {m["vin"]: m for m in matches}
        assert by_vin["A"]["confidence"] > by_vin["B"]["confidence"]
        assert by_vin["A"]["confidence_label"] == "medium"  # trim exact, no msrp
        assert by_vin["B"]["confidence_label"] == "low"
    finally:
        conn.close()


def test_match_make_alias(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        _seed_cars(
            conn,
            [{"vin": "A", "year": 2026, "make": "Mercedes-Benz", "model": "GLE",
              "trim": "GLE 350", "price": 60000, "dealer_id": "fj"}],
        )
        offer = {"vehicle_year": 2026, "vehicle_make": "Mercedes",
                 "vehicle_model": "GLE", "vehicle_trim": "GLE 350"}
        matches = lm.match_cars(conn, "fj", None, offer)
        assert len(matches) == 1
    finally:
        conn.close()


def test_summary_dedupes_model_in_trim():
    offer = {"payment": 399, "term_months": 24, "due_at_signing": 4999,
             "mileage_per_year": 7500, "vehicle_year": 2026,
             "vehicle_make": "Mercedes-Benz", "vehicle_model": "GLE",
             "vehicle_trim": "GLE 350 4MATIC® SUV"}
    s = lm.summarize_deal(offer, None, [{"confidence": 0.9}])
    assert "GLE GLE" not in s
    assert "$399/mo" in s and "24 mo" in s and "7,500 mi/yr" in s


# ── Cache store round-trip ───────────────────────────────────────────────────


def test_match_cache_roundtrip(sqlite_inventory):
    from backend.scanner.specials.lease_matches_store import (
        get_matches_for_dealer,
        get_offer_match,
        upsert_offer_match,
    )

    conn = sqlite_inventory.get_conn()
    try:
        upsert_offer_match(
            conn, "fj", "hash-1", offer_id=5,
            extracted={"make": "Mercedes-Benz"}, summary="$399/mo — 1 qualifying",
            matches=[{"car_id": 1, "confidence": 0.95}], confidence=0.95,
        )
        row = get_offer_match(conn, "fj", "hash-1")
        assert row["summary"] == "$399/mo — 1 qualifying"
        assert row["match_count"] == 1
        assert row["extracted"]["make"] == "Mercedes-Benz"
        assert row["matches"][0]["car_id"] == 1

        # upsert again replaces
        upsert_offer_match(
            conn, "fj", "hash-1", offer_id=5, extracted=None,
            summary="updated", matches=[], confidence=None,
        )
        idx = get_matches_for_dealer(conn, "fj")
        assert idx["hash-1"]["summary"] == "updated"
        assert idx["hash-1"]["match_count"] == 0
    finally:
        conn.close()
