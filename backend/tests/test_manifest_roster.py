"""
load_manifest roster routing: DEALERS_FROM_ACTIVE_INVENTORY=1 must roster from
live `cars` inventory (the correct set for a delta refresh), taking precedence
over the DEALERS_FROM_DB registry and over a manifest file. Regression guard for
the nightly delta being pinned to a single regional manifest (stale dealers).
"""

from __future__ import annotations

import backend.scanner.manifest as m


def test_active_inventory_flag_routes_to_inventory_roster(monkeypatch):
    monkeypatch.setattr(m, "_load_active_inventory_dealers", lambda: [{"dealer_id": "a-com", "url": "https://a.com", "name": "A"}])
    monkeypatch.setattr(m, "_load_dealers_from_db", lambda: [{"dealer_id": "REGISTRY", "url": "x", "name": "x"}])
    monkeypatch.setenv("DEALERS_FROM_ACTIVE_INVENTORY", "1")
    monkeypatch.setenv("DEALERS_FROM_DB", "1")  # active-inventory must win over registry
    out = m.load_manifest()
    assert [d["dealer_id"] for d in out] == ["a-com"]


def test_registry_flag_still_works_when_active_flag_off(monkeypatch):
    monkeypatch.setattr(m, "_load_dealers_from_db", lambda: [{"dealer_id": "REGISTRY", "url": "x", "name": "x"}])
    monkeypatch.setenv("DEALERS_FROM_DB", "1")
    monkeypatch.delenv("DEALERS_FROM_ACTIVE_INVENTORY", raising=False)
    out = m.load_manifest()
    assert out[0]["dealer_id"] == "REGISTRY"


def test_active_inventory_roster_shape(monkeypatch):
    """The roster function yields {dealer_id, url, name}; base URL is the host,
    derived from a stored listing (VDP) URL."""
    class _FakeCur:
        def execute(self, *_a, **_k):
            pass
        def fetchall(self):
            return [
                ("capomazda-com", "Mazda Capistrano", "https://www.capomazda.com/viewdetails/used/VIN/x"),
                ("nourl-com", "No URL Dealer", None),
            ]
    class _FakeConn:
        def cursor(self):
            return _FakeCur()
        def close(self):
            pass

    import backend.db.inventory_db as inv
    monkeypatch.setattr(inv, "get_conn", lambda: _FakeConn())
    out = m._load_active_inventory_dealers()
    by_id = {d["dealer_id"]: d for d in out}
    assert by_id["capomazda-com"]["url"] == "https://www.capomazda.com"
    assert by_id["capomazda-com"]["name"] == "Mazda Capistrano"
    # No derivable base URL -> empty string, but the dealer is still listed.
    assert by_id["nourl-com"]["url"] == ""
