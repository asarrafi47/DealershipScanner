"""Public stats and compare page smoke tests."""

from __future__ import annotations


def test_public_listings_count_non_negative():
    from backend.db.inventory_db import public_listings_count

    assert public_listings_count() >= 0


def test_compare_page_requires_login():
    from backend import main

    client = main.app.test_client()
    r = client.get("/compare")
    assert r.status_code == 302
    assert "/login" in (r.headers.get("Location") or "")


def test_compare_page_with_ids_requires_login():
    from backend import main

    client = main.app.test_client()
    r = client.get("/compare?ids=1,2,abc,999999999")
    assert r.status_code == 302
    assert "/login" in (r.headers.get("Location") or "")


def test_record_compare_session():
    from backend.db.user_history_db import (
        get_recent_compared_car_ids,
        record_compare_session,
    )

    uid = 999001
    record_compare_session(uid, [101, 102, 101])
    ids = get_recent_compared_car_ids(uid, limit=10)
    assert 101 in ids
    assert 102 in ids
    assert ids.index(101) < ids.index(102) or ids.index(102) < ids.index(101)
