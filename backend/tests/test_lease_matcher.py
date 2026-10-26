"""Tests for the lease-offer matcher (deterministic term parsing + inventory
matching + match cache). No model or network — parsing is pure regex.
"""
from __future__ import annotations

import pytest

from backend.intelligence import lease_matcher as lm


def _seed_cars(pg_db, rows):
    pg_db.add_cars(rows)


# ── Deterministic term parsing (pure regex, no model) ────────────────────────


def test_parse_maps_scraped_offer_fields():
    offer = {
        "title": "New 2026 Mercedes-Benz GLE 350 4MATIC® SUV",
        "vehicle_year": 2026, "vehicle_make": "Mercedes-Benz",
        "vehicle_model": "GLE", "vehicle_trim": "GLE 350 4MATIC® SUV",
        "payment": 399.0, "term_months": 24, "due_at_signing": 4999.0,
        "mileage_per_year": 7500, "msrp": 69965.0, "expires": "07/31/2026",
        "fine_print": "Applies to stock NL497846.",
    }
    terms = lm.parse_lease_terms(offer)
    assert terms["year"] == 2026
    assert terms["make"] == "Mercedes-Benz"
    assert terms["payment"] == 399.0
    assert terms["term_months"] == 24
    assert terms["mileage_per_year"] == 7500
    assert terms["msrp_or_price"] == 69965.0
    assert terms["expiration"] == "07/31/2026"
    # Stock number recovered from the fine print.
    assert terms["stock_or_vin_specific"] == "NL497846"


def test_parse_derives_mileage_from_total_cap_over_term():
    # "over 15,000 miles" on a 24-month lease → 7,500/yr; no scraped mileage.
    offer = {
        "title": "Lease", "vehicle_make": "Toyota", "vehicle_model": "Camry",
        "term_months": 24, "mileage_per_year": None,
        "fine_print": "Lessee responsible for $0.25/mile over 15,000 miles.",
    }
    assert lm.parse_lease_terms(offer)["mileage_per_year"] == 7500


def test_parse_extracts_full_vin_over_stock():
    offer = {
        "title": "Lease", "vehicle_make": "Mercedes-Benz", "vehicle_model": "GLE",
        "fine_print": "Offer applies to VIN 4JGFB4FB5TB497846, stock NL497846.",
    }
    assert lm.parse_lease_terms(offer)["stock_or_vin_specific"] == "4JGFB4FB5TB497846"


def test_parse_no_stock_when_absent():
    offer = {"title": "Lease", "vehicle_make": "Kia", "vehicle_model": "K4",
             "fine_print": "See dealer for details. Expires soon."}
    assert lm.parse_lease_terms(offer)["stock_or_vin_specific"] is None


# ── Stock/VIN tail matching ──────────────────────────────────────────────────


def test_stock_vin_tail_match():
    # 'NL497846' identifies VIN ...497846
    assert lm._stock_vin_match("NL497846", "4JGFB4FB5TB497846", None)
    assert lm._stock_vin_match("STK12345", None, "STK12345")
    assert not lm._stock_vin_match("NL497846", "4JGFB4FB5TB111111", None)
    assert not lm._stock_vin_match(None, "4JGFB4FB5TB497846", None)


# ── Matching against inventory ───────────────────────────────────────────────


@pytest.mark.pg
def test_match_narrows_to_stock_specific_vin(pg_db):
    conn = pg_db.get_conn()
    try:
        _seed_cars(
            pg_db,
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


@pytest.mark.pg
def test_match_trim_fallback_when_no_stock(pg_db):
    conn = pg_db.get_conn()
    try:
        _seed_cars(
            pg_db,
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


@pytest.mark.pg
def test_match_make_alias(pg_db):
    conn = pg_db.get_conn()
    try:
        _seed_cars(
            pg_db,
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


@pytest.mark.pg
def test_match_cache_roundtrip(pg_db):
    from backend.scanner.specials.lease_matches_store import (
        get_matches_for_dealer,
        get_offer_match,
        upsert_offer_match,
    )

    conn = pg_db.get_conn()
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


# ── Cache freshness (B-035) ──────────────────────────────────────────────────


@pytest.mark.pg
def test_refresh_recomputes_when_cached_car_sold_or_ttl_expired(pg_db, monkeypatch):
    from datetime import datetime, timedelta, timezone

    from backend.scanner.specials import store as specials_store

    conn = pg_db.get_conn()
    try:
        _seed_cars(pg_db, [
            {"vin": "V1", "year": 2026, "make": "Mercedes-Benz", "model": "GLE", "dealer_id": "fj"},
        ])
        offer = {"offer_hash": "h1", "id": 1, "type": "lease"}
        monkeypatch.setattr(specials_store, "get_specials_for_dealer", lambda c, d: [offer])
        calls = []
        monkeypatch.setattr(
            lm, "compute_offer_matches",
            lambda c, d, o: calls.append(1) or {
                "extracted": {}, "matches": [{"car_id": 1, "confidence": 0.5}],
                "summary": "s", "confidence": 0.5,
            },
        )
        assert lm.refresh_dealer_lease_matches(conn, "fj")["computed"] == 1
        assert lm.refresh_dealer_lease_matches(conn, "fj")["skipped"] == 1

        # sold car invalidates the cache
        conn.execute("UPDATE cars SET listing_active = 0 WHERE id = 1")
        conn.commit()
        assert lm.refresh_dealer_lease_matches(conn, "fj")["computed"] == 1
        conn.execute("UPDATE cars SET listing_active = 1 WHERE id = 1")
        conn.commit()
        assert lm.refresh_dealer_lease_matches(conn, "fj")["skipped"] == 1

        # an old computed_at expires the cache
        old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        conn.execute("UPDATE lease_offer_matches SET computed_at = ?", (old,))
        conn.commit()
        assert lm.refresh_dealer_lease_matches(conn, "fj")["computed"] == 1
    finally:
        conn.close()
