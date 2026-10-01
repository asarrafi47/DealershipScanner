"""Public stats and compare page smoke tests."""

from __future__ import annotations


def test_public_listings_count_non_negative():
    from backend.db.inventory_db import public_listings_count

    assert public_listings_count() >= 0


def test_compare_page_public_when_anonymous():
    from backend import main

    client = main.app.test_client()
    r = client.get("/compare")
    assert r.status_code == 200


def test_compare_page_with_ids_public_when_anonymous():
    from backend import main

    client = main.app.test_client()
    r = client.get("/compare?ids=1,2,abc,999999999")
    assert r.status_code == 200


def test_record_compare_session(tmp_path, monkeypatch):
    # Own users DB: this test used to write uid 999001 into the developer's real
    # backend/users.db (164 compare_sessions rows by 2026-10-01).
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    from backend.db.user_history_db import (
        ensure_car_history_table,
        get_recent_compared_car_ids,
        record_compare_session,
    )

    ensure_car_history_table()
    uid = 999001
    record_compare_session(uid, [101, 102, 101])
    ids = get_recent_compared_car_ids(uid, limit=10)
    assert 101 in ids
    assert 102 in ids
    assert ids.index(101) < ids.index(102) or ids.index(102) < ids.index(101)
