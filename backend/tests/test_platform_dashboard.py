"""Tests for search analytics and platform dashboard stats."""

from __future__ import annotations

import json

import pytest

from backend.db.search_analytics_db import (
    record_search_event,
    usage_summary,
)
from backend.dealer.admin.platform_stats import inventory_scale, users_scale


@pytest.fixture()
def users_db(tmp_path, monkeypatch):
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    from backend.db.users_db import init_users_db

    init_users_db()
    return db_path


@pytest.fixture()
def analytics_db(tmp_path, monkeypatch):
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    from backend.db import users_db

    users_db.init_users_db()
    from backend.db.search_analytics_db import ensure_search_analytics_table

    ensure_search_analytics_table()
    return db_path


def test_record_and_summarize_search(analytics_db) -> None:
    record_search_event(
        source="smart_search",
        query_text="BMW X5",
        filters={"make": "BMW", "zip_code": "28202"},
        result_count=12,
        user_id=1,
        session_key="sess-a",
        geo_zip="28202",
    )
    record_search_event(
        source="listings",
        query_text="honda accord",
        filters={"make": "Honda"},
        result_count=0,
        session_key="sess-b",
    )
    all_usage = usage_summary(days=30, audience="all")
    assert all_usage["total_searches"] >= 2
    assert all_usage["zero_result_searches"] >= 1
    assert all_usage["logged_in_searches"] >= 1
    assert all_usage["anonymous_searches"] >= 1
    assert "smart_search" in all_usage["by_source"]

    auth_only = usage_summary(days=30, audience="authenticated")
    assert auth_only["total_searches"] >= 1


def test_users_scale(users_db) -> None:
    from backend.db.users_db import save_user

    save_user("u1", "u1@example.com", "password123", role="general_user")
    stats = users_scale()
    assert stats["total"] >= 1
    assert stats["active"] >= 1


def test_inventory_scale(tmp_path, monkeypatch) -> None:
    import backend.db.inventory_db as inv

    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inv_plat.db"))
    # inv.DB_PATH is resolved once at import time, so the env var above has no effect
    # on it post-import — patch the module attribute directly.
    monkeypatch.setattr(inv, "DB_PATH", str(tmp_path / "inv_plat.db"))
    inv.init_inventory_db()
    conn = inv.get_conn()
    conn.execute("DELETE FROM cars")
    conn.commit()
    conn.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, dealer_id, scraped_at, listing_active, price
        ) VALUES (
            '1HGBH41JXMN109186', 'T', 2020, 'Honda', 'Accord', 'dealer-a',
            datetime('now'), 1, 20000
        )
        """
    )
    conn.commit()
    conn.close()
    stats = inventory_scale()
    assert stats["active_cars"] >= 1
    assert stats["dealers_in_inventory"] >= 1
