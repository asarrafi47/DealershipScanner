"""Store round-trip + guard tests for user-submitted dealership reviews.

SQLite isolation mirrors ``test_dealer_specials_store.py`` — never touches prod
Postgres.
"""
from __future__ import annotations

import os

import pytest

from backend.reviews.guards import hash_ip, validate_review
from backend.reviews.store import (
    get_reviews_for_dealer,
    get_user_review,
    increment_report,
    review_summary,
    upsert_review,
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


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

def test_upsert_and_read_roundtrip(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        upsert_review(
            conn, "longotoyota-com", 1,
            rating=5, body="Great experience, no pressure.",
            display_name="Alice",
        )
        upsert_review(
            conn, "longotoyota-com", 2,
            rating=2, body="They tacked on surprise fees at signing.",
            display_name="Bob", is_anonymous=True,
            addon_fee_reported=True, addon_fee_amount=1995.0,
            addon_fee_desc="protection package",
        )
        rows = get_reviews_for_dealer(conn, "longotoyota-com")
        assert len(rows) == 2
        # Newest first (Bob inserted last).
        assert rows[0]["user_id"] == 2
        assert rows[0]["is_anonymous"] == 1
        assert rows[0]["display_name"] == "Bob"  # stored even when anonymous
        assert rows[1]["user_id"] == 1

        # Other dealers untouched.
        assert get_reviews_for_dealer(conn, "someone-else-com") == []
    finally:
        conn.close()


def test_one_review_per_user_edits_in_place(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        upsert_review(conn, "d-com", 7, rating=1, body="First take was awful.")
        upsert_review(conn, "d-com", 7, rating=4, body="Updated: they fixed it well.")
        rows = get_reviews_for_dealer(conn, "d-com")
        assert len(rows) == 1
        assert rows[0]["rating"] == 4
        assert rows[0]["body"].startswith("Updated")

        mine = get_user_review(conn, "d-com", 7)
        assert mine is not None and mine["rating"] == 4
    finally:
        conn.close()


def test_review_summary_aggregation(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        upsert_review(conn, "d-com", 1, rating=5, body="Loved it here.")
        upsert_review(conn, "d-com", 2, rating=3, body="It was fine overall.")
        upsert_review(
            conn, "d-com", 3, rating=1, body="Hidden add-on fees galore.",
            addon_fee_reported=True, addon_fee_amount=899.0, addon_fee_desc="nitrogen",
        )
        s = review_summary(conn, "d-com")
        assert s["count"] == 3
        assert s["avg_rating"] == 3.0
        assert s["addon_fee_count"] == 1
        assert s["addon_fees"] == [{"amount": 899.0, "desc": "nitrogen"}]

        # Empty dealer.
        e = review_summary(conn, "empty-com")
        assert e == {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}
    finally:
        conn.close()


def test_report_increment_and_auto_flag(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        upsert_review(conn, "d-com", 1, rating=5, body="Suspiciously glowing review.")
        rid = get_reviews_for_dealer(conn, "d-com")[0]["id"]

        r1 = increment_report(conn, rid)
        assert r1["report_count"] == 1 and r1["status"] == "published"
        r2 = increment_report(conn, rid)
        assert r2["report_count"] == 2 and r2["status"] == "published"
        r3 = increment_report(conn, rid)
        assert r3["report_count"] == 3 and r3["status"] == "flagged"

        # Flagged reviews drop out of the public list + summary.
        assert get_reviews_for_dealer(conn, "d-com") == []
        assert review_summary(conn, "d-com")["count"] == 0

        # Reporting a nonexistent review is a no-op.
        assert increment_report(conn, 999999) is None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def test_guard_accepts_clean_review():
    ok, err = validate_review({"rating": 4, "body": "Solid service and fair pricing overall."})
    assert ok and err is None


def test_guard_rejects_url_in_body():
    for body in [
        "Check my deal at http://spam.example.com now for details.",
        "Visit www.spammydeals.com to see the offer here.",
        "Best prices at cheapcars.net all week long here.",
    ]:
        ok, err = validate_review({"rating": 5, "body": body})
        assert not ok
        assert "link" in err.lower()


def test_guard_rejects_bad_length():
    ok, err = validate_review({"rating": 5, "body": "too short"})
    assert not ok and "short" in err.lower()
    ok, err = validate_review({"rating": 5, "body": "x" * 4001})
    assert not ok and "long" in err.lower()


def test_guard_rejects_bad_rating():
    ok, err = validate_review({"rating": 0, "body": "A perfectly valid length body here."})
    assert not ok
    ok, err = validate_review({"rating": 9, "body": "A perfectly valid length body here."})
    assert not ok
    ok, err = validate_review({"rating": "abc", "body": "A perfectly valid length body here."})
    assert not ok


def test_guard_requires_addon_desc_when_reported():
    ok, err = validate_review({
        "rating": 2, "body": "They added mystery fees at signing time.",
        "addon_fee_reported": True, "addon_fee_amount": "500",
    })
    assert not ok and "add-on" in err.lower()

    ok, err = validate_review({
        "rating": 2, "body": "They added mystery fees at signing time.",
        "addon_fee_reported": True, "addon_fee_desc": "dealer prep", "addon_fee_amount": "500",
    })
    assert ok and err is None

    # Negative amount rejected.
    ok, err = validate_review({
        "rating": 2, "body": "They added mystery fees at signing time.",
        "addon_fee_reported": True, "addon_fee_desc": "dealer prep", "addon_fee_amount": "-5",
    })
    assert not ok


def test_hash_ip_is_stable_and_opaque():
    assert hash_ip(None) is None
    h1 = hash_ip("1.2.3.4")
    h2 = hash_ip("1.2.3.4")
    assert h1 == h2 and h1 != "1.2.3.4" and len(h1) == 40
