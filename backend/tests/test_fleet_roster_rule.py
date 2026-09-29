"""Fleet / DEALERS_FROM_SCANNABLE roster rule (2026-09-29 Railway incident).

The first full Railway fleet run rostered active inventory ∪ stored recipes; 136
recipe-only dealers (emptied on purpose, their recipes replay group feeds)
reassigned 6,961 siblings' VINs. A whole-roster scan now takes dealers WITH
active inventory, minus do_not_scan, plus revived recipe-only dealers. An
explicit dealer list is never filtered.
"""
from __future__ import annotations

import backend.scanner.manifest as m


def _d(did, active=True):
    return {"dealer_id": did, "url": f"https://www.{did}", "name": did, "_has_active_inventory": active}


ROSTER = [
    _d("owner-com"),
    _d("sibling-com"),
    _d("groupfeed-com", active=False),      # recipe-only: the incident shape
    _d("revived-com", active=False),        # recipe-only but allowlisted
    _d("normreeves-com"),                   # do-not-scan
    _d("carmax-com"),
]


def test_rule_keeps_active_drops_recipe_only_and_do_not_scan():
    kept, rep = m.apply_scannable_roster_rule(ROSTER, do_not_scan={"normreeves-com"}, revived={"revived-com"})
    assert [d["dealer_id"] for d in kept] == ["owner-com", "sibling-com", "revived-com", "carmax-com"]
    assert rep["excluded_recipe_only"] == ["groupfeed-com"]
    assert rep["excluded_do_not_scan"] == ["normreeves-com"]
    assert rep["revived_recipe_only"] == ["revived-com"]
    assert rep["kept"] == 4


def test_do_not_scan_beats_revived():
    kept, rep = m.apply_scannable_roster_rule([_d("x-com", active=False)], do_not_scan={"x-com"}, revived={"x-com"})
    assert kept == [] and rep["excluded_do_not_scan"] == ["x-com"]


def test_id_lists_read_both_files_with_comments(tmp_path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("# header\nfoo-com  # junk entity\n\n")
    b.write_text("bar-com\nfoo-com\n")
    assert m.load_do_not_scan_ids((a, b, tmp_path / "missing.txt")) == {"foo-com", "bar-com"}


def test_tracked_files_exist_and_parse():
    """deploy/railway/* ship in the image (workspace/ does not)."""
    assert m.DO_NOT_SCAN_PATHS[1].is_file()
    assert "normreeves-com" in m._read_id_list(m.DO_NOT_SCAN_PATHS[1])
    assert m.REVIVED_DEALERS_PATH.is_file()
    assert m.load_revived_dealer_ids() == set()  # starts empty; header comment only


def test_fleet_roster_applies_rule(monkeypatch):
    from backend.scripts import fleet_scan

    monkeypatch.delenv("SCAN_DEALERS", raising=False)
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(m, "_load_scannable_dealers", lambda: list(ROSTER))
    monkeypatch.setattr(m, "load_do_not_scan_ids", lambda paths=None: {"normreeves-com"})
    monkeypatch.setattr(m, "load_revived_dealer_ids", lambda path=None: {"revived-com"})
    # the kill switch for load_manifest must not bypass the fleet rule
    monkeypatch.setenv("SCANNABLE_ROSTER_RULE", "0")
    assert fleet_scan.load_roster() == ["owner-com", "revived-com", "sibling-com"]
    assert fleet_scan.ROSTER_REPORT["excluded_recipe_only"] == ["groupfeed-com"]


def test_fleet_explicit_list_is_never_filtered(monkeypatch):
    from backend.scripts import fleet_scan

    monkeypatch.setenv("SCAN_DEALERS", "groupfeed-com,normreeves-com")
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(m, "_load_scannable_dealers", lambda: (_ for _ in ()).throw(AssertionError("not called")))
    assert fleet_scan.load_roster() == ["groupfeed-com", "normreeves-com"]


def _manifest_env(monkeypatch):
    monkeypatch.setenv("DEALERS_FROM_SCANNABLE", "1")
    monkeypatch.setattr(m, "_load_scannable_dealers", lambda: list(ROSTER))
    monkeypatch.setattr(m, "load_do_not_scan_ids", lambda paths=None: {"normreeves-com"})
    monkeypatch.setattr(m, "load_revived_dealer_ids", lambda path=None: set())


def test_load_manifest_scannable_applies_rule(monkeypatch):
    _manifest_env(monkeypatch)
    monkeypatch.delenv("SCANNABLE_ROSTER_RULE", raising=False)
    ids = [d["dealer_id"] for d in m.load_manifest()]
    assert "groupfeed-com" not in ids and "revived-com" not in ids and "normreeves-com" not in ids
    assert "owner-com" in ids


def test_load_manifest_explicit_ids_are_not_filtered(monkeypatch):
    """dealer_pipeline passes --dealer-id with DEALERS_FROM_SCANNABLE=1: a freshly
    synthesized recipe-only dealer must still resolve."""
    _manifest_env(monkeypatch)
    ids = [d["dealer_id"] for d in m.load_manifest(explicit_dealer_ids=True)]
    assert "groupfeed-com" in ids and "normreeves-com" in ids


def test_load_manifest_rule_kill_switch(monkeypatch):
    _manifest_env(monkeypatch)
    monkeypatch.setenv("SCANNABLE_ROSTER_RULE", "0")
    assert len(m.load_manifest()) == len(ROSTER)


def test_scannable_query_carries_active_flag(monkeypatch):
    class _Cur:
        def execute(self, sql, *_a):
            assert "listing_active" in sql
        def fetchall(self):
            return [("a-com", "A", None, None, None, 1), ("b-com", "", None, "B", "dealeron", 0)]
    class _Conn:
        def cursor(self):
            return _Cur()
        def close(self):
            pass

    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "get_conn", lambda: _Conn())
    by = {d["dealer_id"]: d for d in m._load_scannable_dealers()}
    assert by["a-com"]["_has_active_inventory"] is True
    assert by["b-com"]["_has_active_inventory"] is False
