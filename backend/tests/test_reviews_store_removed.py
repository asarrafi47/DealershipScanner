"""SEC-A-007: a resubmit must not resurrect an admin-removed review."""
from __future__ import annotations

from backend.reviews.store import (
    get_reviews_for_dealer,
    get_user_review,
    increment_report,
    set_review_status,
    upsert_review,
)


def test_resubmit_keeps_removed_review_removed(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        upsert_review(conn, "d-com", 7, rating=1, body="Terrible, avoid this place.")
        rid = get_user_review(conn, "d-com", 7)["id"]
        assert set_review_status(conn, rid, "removed")
        upsert_review(conn, "d-com", 7, rating=5, body="Edited and now totally fine.")
        assert get_user_review(conn, "d-com", 7)["status"] == "removed"
        assert get_reviews_for_dealer(conn, "d-com") == []
    finally:
        conn.close()


def test_edit_clears_reporter_dedup_so_reporters_can_reflag(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        upsert_review(conn, "d-com", 8, rating=2, body="Pushy sales staff overall.")
        rid = get_user_review(conn, "d-com", 8)["id"]
        for h in ("a", "b", "c"):
            increment_report(conn, rid, reporter_hash=h)
        assert get_user_review(conn, "d-com", 8)["status"] == "flagged"
        upsert_review(conn, "d-com", 8, rating=2, body="Pushy sales staff, edited.")
        mine = get_user_review(conn, "d-com", 8)
        assert mine["status"] == "published" and mine["report_count"] == 0
        state = increment_report(conn, rid, reporter_hash="a")
        assert state["report_count"] == 1
    finally:
        conn.close()
