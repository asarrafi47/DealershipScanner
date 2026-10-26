"""Deleting a dealer user retires their mirrored public cars rows (audit B-008)."""
from __future__ import annotations

from unittest import mock

from backend.dealer import portal_sync


def test_retire_portal_listings_for_user_updates_by_dealer_id():
    conn = mock.MagicMock()
    ctx = mock.MagicMock()
    ctx.__enter__.return_value = conn
    with mock.patch("backend.db.repositories.base_repo.db_conn", return_value=ctx):
        portal_sync.retire_portal_listings_for_user(7)
    sql, params = conn.cursor.return_value.execute.call_args[0]
    assert "listing_active = 0" in sql and "dealer_id = ?" in sql
    assert params[1] == "dealer-portal-7"
    conn.commit.assert_called_once()


def test_delete_user_calls_retire(monkeypatch):
    from backend.db.users_db import accounts

    class Cur:
        def execute(self, sql, *a):
            self.sql = sql
        def fetchall(self):
            return [(0, "org_id")]
        def fetchone(self):
            return (5, None)
        rowcount = 1

    class Conn:
        def cursor(self):
            return Cur()
        def execute(self, *a): pass
        def commit(self): pass
        def close(self): pass

    monkeypatch.setattr(accounts, "get_conn", lambda: Conn())
    import backend.db.dealer_portal_db as ddb
    monkeypatch.setattr(ddb, "delete_vehicles_for_user", lambda uid: None)
    seen = []
    monkeypatch.setattr(portal_sync, "retire_portal_listings_for_user", lambda uid: seen.append(uid))
    assert accounts.delete_user_by_email("a@b.com") is True
    assert seen == [5]
