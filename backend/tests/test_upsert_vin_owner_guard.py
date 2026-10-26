"""VIN ownership guard in upsert_vehicles (2026-09-29 Railway fleet incident).

``cars`` is one row per VIN (ON CONFLICT(vin)), so the last store to write a VIN
owned it: the first full fleet run moved 6,961 VINs to the wrong dealer
(mtnviewnissan-com (CA) -> cleveland-nissan-com (TN) 876, ...). A write must not
move a VIN whose stored row is active and scraped within the guard window under
a different dealer_id.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles

VIN = "1N4BL4DV0PN300001"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    from backend.db import inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    return str(db_path)


def _car(dealer_id: str, price: int = 30000, vin: str = VIN) -> dict:
    return {"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan", "model": "Altima",
            "price": price, "dealer_id": dealer_id, "dealer_name": dealer_id, "dealer_url": f"https://{dealer_id}"}


def _row(db_path: str, vin: str = VIN):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT dealer_id, price, listing_active FROM cars WHERE vin=?", (vin,)).fetchone()
    finally:
        conn.close()


def _set(db_path: str, sql: str, params=()):
    conn = sqlite3.connect(db_path)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _conflicts(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT vin, owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts").fetchall()
    finally:
        conn.close()


def test_fresh_active_row_of_another_dealer_is_not_reassigned(db):
    upsert_vehicles([_car("mtnviewnissan-com", 30000)])
    stats: dict = {}
    n = upsert_vehicles([_car("cleveland-nissan-com", 99999), _car("cleveland-nissan-com", 25000, vin="JN8AT3BB0PW000002")], stats)
    assert n == 1  # only the claimant's own new VIN was written
    assert _row(db) == ("mtnviewnissan-com", 30000, 1)  # owner's row untouched (price too)
    assert _row(db, "JN8AT3BB0PW000002")[0] == "cleveland-nissan-com"
    assert stats["vin_owner_conflicts"] == 1
    assert stats["vin_owner_conflict_vins"] == [VIN]
    assert stats["vin_owner_conflict_owners"] == {"mtnviewnissan-com": 1}
    assert _conflicts(db) == [(VIN, "mtnviewnissan-com", "cleveland-nissan-com")]
    # a second sighting updates seen_at, it does not add a row
    upsert_vehicles([_car("cleveland-nissan-com")])
    assert len(_conflicts(db)) == 1


def test_same_dealer_rewrites_normally(db):
    upsert_vehicles([_car("a-com", 30000)])
    stats: dict = {}
    assert upsert_vehicles([_car("a-com", 28000)], stats) == 1
    assert _row(db) == ("a-com", 28000, 1)
    assert stats["vin_owner_conflicts"] == 0


def test_stale_owner_row_can_be_taken(db):
    upsert_vehicles([_car("a-com")])
    old = (datetime.utcnow() - timedelta(hours=49)).isoformat() + "Z"
    _set(db, "UPDATE cars SET scraped_at=? WHERE vin=?", (old, VIN))
    assert upsert_vehicles([_car("b-com")]) == 1
    assert _row(db)[0] == "b-com"


def test_inactive_owner_row_can_be_taken(db):
    upsert_vehicles([_car("a-com")])
    _set(db, "UPDATE cars SET listing_active=0 WHERE vin=?", (VIN,))
    assert upsert_vehicles([_car("b-com")]) == 1
    assert _row(db) == ("b-com", 30000, 1)


def test_window_is_configurable_and_zero_disables(db, monkeypatch):
    upsert_vehicles([_car("a-com")])
    old = (datetime.utcnow() - timedelta(hours=5)).isoformat() + "Z"
    _set(db, "UPDATE cars SET scraped_at=? WHERE vin=?", (old, VIN))
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "4")
    assert upsert_vehicles([_car("b-com")]) == 1  # 5 h old > 4 h window
    assert _row(db)[0] == "b-com"
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    assert upsert_vehicles([_car("c-com")]) == 1  # guard off: last writer wins
    assert _row(db)[0] == "c-com"


def test_sql_backstop_catches_a_claim_the_prefetch_missed(db, monkeypatch):
    """A concurrent shard may claim the VIN between the prefetch and the write;
    the ON CONFLICT ... WHERE clause still refuses the move."""
    upsert_vehicles([_car("a-com", 30000)])
    monkeypatch.setattr(sdb, "_vin_owned_elsewhere", lambda *a, **k: False)
    stats: dict = {}
    assert upsert_vehicles([_car("b-com", 1)], stats) == 0
    assert _row(db) == ("a-com", 30000, 1)
    assert stats["vin_owner_conflicts"] == 1
    assert stats["vin_owner_conflict_owners"] == {"a-com": 1}


def test_guard_hours_env_parsing(monkeypatch):
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    assert sdb.vin_owner_guard_hours() == 48
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "junk")
    assert sdb.vin_owner_guard_hours() == 48
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    assert sdb.vin_owner_guard_hours() == 0


def test_scan_summary_and_triage_reason_carry_the_count():
    """scan_runs.summary_json gets vin_owner_conflicts; the pipeline triage names
    it in the reason when > 10."""
    import json

    from backend.scripts import dealer_pipeline as dp

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY, dealer_id TEXT, vin TEXT, condition TEXT, dealer_name TEXT, zip_code TEXT, scraped_at TEXT, listing_active INTEGER)")
    conn.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, dealer_id TEXT, provider TEXT, finished_at TEXT, duration_seconds REAL, inventory_rows INTEGER, upserted INTEGER, error TEXT, summary_json TEXT)")
    summary = {"capture_coverage": {"n": 0}, "vin_owner_conflicts": 876,
               "vin_owner_conflict_owners": {"mtnviewnissan-com": 876}}
    conn.execute("INSERT INTO scan_runs (dealer_id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json) VALUES (?,?,?,?,?,?,?,?)",
                 ("cleveland-nissan-com", "dealer_dot_com", "2026-09-29T10:00:00+00:00", 60.0, 0, 0, None, json.dumps(summary)))
    out = dp.assess(conn, "cleveland-nissan-com", "2026-09-29T00:00:00+00:00", known_before=0, recipe_info={"had_recipes": 1})
    assert out["vin_owner_conflicts"] == 876
    assert "VIN owner guard: 876" in out["reason"] and "mtnviewnissan-com 876" in out["reason"]

    conn.execute("UPDATE scan_runs SET summary_json=?", (json.dumps({"capture_coverage": {"n": 0}, "vin_owner_conflicts": 10}),))
    out = dp.assess(conn, "cleveland-nissan-com", "2026-09-29T00:00:00+00:00", known_before=0, recipe_info={"had_recipes": 1})
    assert "VIN owner guard" not in (out.get("reason") or "")


def test_record_scan_outcomes_writes_conflict_count(tmp_path, monkeypatch):
    import json

    from backend.db import inventory_db as inv
    from backend.db.repositories import dealers_repo

    db_path = tmp_path / "t.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inv, "DB_PATH", str(db_path))
    dealers_repo.record_scan_outcomes(
        [{"dealer_id": "b-com", "vin_owner_conflicts": 12, "vin_owner_conflict_owners": {"a-com": 12}}],
        finished_at="2026-09-29T00:00:00Z",
    )
    conn = sqlite3.connect(str(db_path))
    s = json.loads(conn.execute("SELECT summary_json FROM scan_runs").fetchone()[0])
    conn.close()
    assert s["vin_owner_conflicts"] == 12 and s["vin_owner_conflict_owners"] == {"a-com": 12}


# ── N8.6: evidence-aware ownership (persisted rooftop tier, pools, aliases, manual moves) ──

from backend.scanner.upsert import ownership as own  # noqa: E402


def _ev(dealer_id: str, tier: str, **kw) -> dict:
    row = _car(dealer_id, **kw)
    row["_rooftop_evidence_tier"] = tier
    return row


def _tier(db_path: str, vin: str = VIN):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT rooftop_tier FROM cars WHERE vin=?", (vin,)).fetchone()[0]
    finally:
        conn.close()


def _age(db_path: str, hours: int = 100):
    old = (datetime.utcnow() - timedelta(hours=hours)).isoformat() + "Z"
    _set(db_path, "UPDATE cars SET scraped_at=? WHERE vin=?", (old, VIN))


@pytest.fixture()
def pools(tmp_path, monkeypatch):
    """A one-pool, one-alias group_pools.json (primary-co owns pool contests)."""
    path = tmp_path / "group_pools.json"
    path.write_text(json.dumps({
        "pools": [{"primary": "primary-co", "members": ["primary-co", "sister-co"]}],
        "aliases": [["alias-a", "alias-b"]],
    }))
    monkeypatch.setenv("GROUP_POOLS_PATH", str(path))
    return path


def test_legacy_rows_and_unstamped_writes_are_tier_none(db):
    upsert_vehicles([_car("a-com")])
    assert _tier(db) == "none"


def test_strong_claimant_beats_a_fresh_none_owner(db):
    upsert_vehicles([_car("a-com")])
    assert upsert_vehicles([_ev("b-com", "strong")]) == 1
    assert _row(db)[0] == "b-com" and _tier(db) == "strong"


def test_strong_claimant_beats_a_weak_owner(db):
    upsert_vehicles([_ev("a-com", "weak")])
    assert upsert_vehicles([_ev("b-com", "strong")]) == 1
    assert _row(db)[0] == "b-com"


def test_weak_or_unstamped_claimant_never_beats_a_strong_owner_even_when_stale(db):
    upsert_vehicles([_ev("a-com", "strong")])
    _age(db)  # far past the 48 h window: the window must not decide
    stats: dict = {}
    assert upsert_vehicles([_ev("b-com", "weak", price=1)], stats) == 0
    assert upsert_vehicles([_car("c-com", price=1)]) == 0
    assert _row(db) == ("a-com", 30000, 1)
    assert stats["vin_owner_conflict_reasons"] == {"owner_strong": 1}


def test_two_strong_claims_leave_the_stored_owner(db):
    upsert_vehicles([_ev("a-com", "strong")])
    assert upsert_vehicles([_ev("b-com", "strong", price=1)]) == 0
    assert _row(db)[0] == "a-com"


@pytest.mark.parametrize("order", [("x-com", "y-com"), ("y-com", "x-com")])
def test_two_scan_orders_give_one_owner(db, order):
    """x-com's feed stamps the car, y-com's names no store: x-com owns it either way."""
    feeds = {"x-com": _ev("x-com", "strong"), "y-com": _car("y-com")}
    for did in order:
        upsert_vehicles([feeds[did]])
    assert _row(db)[0] == "x-com"


def test_stale_owner_is_decided_by_rules_not_scan_order(db):
    upsert_vehicles([_ev("a-com", "strong")])
    _age(db)
    assert upsert_vehicles([_car("b-com")]) == 0  # rule 2
    _set(db, "UPDATE cars SET rooftop_tier='none' WHERE vin=?", (VIN,))
    assert upsert_vehicles([_ev("c-com", "strong")]) == 1  # rule 1, whatever the window
    assert _row(db)[0] == "c-com"


def test_pool_primary_takes_an_evidence_less_contest_and_the_other_member_does_not(db, pools):
    upsert_vehicles([_car("sister-co")])
    assert upsert_vehicles([_car("primary-co")]) == 1  # primary beats a fresh sister
    assert _row(db)[0] == "primary-co"
    assert upsert_vehicles([_car("sister-co", price=1)]) == 0  # sister never takes it back
    _age(db)
    assert upsert_vehicles([_car("sister-co", price=2)]) == 0  # not even past the window
    assert _row(db)[0] == "primary-co"


def test_pool_primary_does_not_take_from_a_strong_sister(db, pools):
    upsert_vehicles([_ev("sister-co", "strong")])
    assert upsert_vehicles([_car("primary-co", price=1)]) == 0
    assert _row(db)[0] == "sister-co"


def test_alias_ids_of_one_store_never_conflict(db, pools):
    upsert_vehicles([_ev("alias-a", "strong")])
    stats: dict = {}
    assert upsert_vehicles([_car("alias-b", price=1)], stats) == 1
    assert _row(db)[0] == "alias-b" and stats["vin_owner_conflicts"] == 0


def test_uncovered_pair_keeps_the_owner_and_is_reported_group_feed_unstamped(db, pools):
    upsert_vehicles([_car("a-com")])
    stats: dict = {}
    assert upsert_vehicles([_car("b-com", price=1)], stats) == 0
    assert stats["group_feed_unstamped"] == {"a-com>b-com": 1}
    _age(db)
    assert upsert_vehicles([_car("b-com", price=2)]) == 1  # window governs only this case


def test_same_dealer_rescan_keeps_the_stronger_stored_tier(db):
    upsert_vehicles([_ev("a-com", "strong")])
    upsert_vehicles([_car("a-com", price=31000)])
    assert _tier(db) == "strong"
    upsert_vehicles([_ev("a-com", "weak")])
    assert _tier(db) == "strong"


def test_sql_backstop_applies_the_tier_rules_when_the_prefetch_misses(db, monkeypatch):
    """The prefetch is blinded; the ON CONFLICT WHERE alone must give the same answers."""
    from backend.scanner.upsert import guard

    real = guard.prefetch_existing

    def blind(cursor, by_vin, window, owned, **kw):
        spec, _ = real(cursor, by_vin, window, owned, **kw)
        return spec, {}

    monkeypatch.setattr(sdb, "prefetch_existing", blind)
    upsert_vehicles([_ev("a-com", "strong")])
    _age(db)
    assert upsert_vehicles([_car("b-com", price=1)]) == 0  # stale but strong owner
    assert upsert_vehicles([_ev("c-com", "strong", price=2)]) == 0
    _set(db, "UPDATE cars SET rooftop_tier='weak' WHERE vin=?", (VIN,))
    assert upsert_vehicles([_ev("c-com", "strong", price=2)]) == 1
    assert _row(db)[0] == "c-com"
    _set(db, "UPDATE cars SET rooftop_tier='none' WHERE vin=?", (VIN,))
    assert upsert_vehicles([_car("d-com", price=3)]) == 0  # fresh, evidence-less, uncovered


def _moves(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT from_dealer_id, to_dealer_id, evidence, reverted_at FROM car_move_log ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


def test_every_allowed_move_writes_a_car_move_log_row_and_refusals_write_none(db):
    stats: dict = {}
    upsert_vehicles([_car("a-com")])
    assert _moves(db) == []  # a first write is not a move
    upsert_vehicles([_car("b-com", price=1)])  # refused
    assert _moves(db) == []
    upsert_vehicles([_ev("c-com", "strong")], stats)  # allowed
    rows = _moves(db)
    assert len(rows) == 1 == stats["car_moves_logged"]
    assert rows[0][:2] == ("a-com", "c-com") and rows[0][2].startswith(own.SCANNER_MOVE_PREFIX + "strong_claimant")


def test_manual_move_is_honoured_even_against_a_strong_claimant_until_reverted(db):
    upsert_vehicles([_car("a-com")])
    # a person moved the car to m-com (apply_attribution_moves.py shape) and refreshed scraped_at
    _set(db, "UPDATE cars SET dealer_id='m-com' WHERE vin=?", (VIN,))
    _set(db, "INSERT INTO car_move_log (car_id, from_dealer_id, to_dealer_id, evidence) "
             "SELECT id, 'a-com', 'm-com', 'photo evidence' FROM cars WHERE vin=?", (VIN,))
    _age(db)
    stats: dict = {}
    assert upsert_vehicles([_ev("b-com", "strong")], stats) == 0
    assert stats["vin_owner_conflict_reasons"] == {"manual_move": 1}
    _set(db, "UPDATE car_move_log SET reverted_at=datetime('now')")
    assert upsert_vehicles([_ev("b-com", "strong")]) == 1


def test_a_scanner_made_move_is_not_a_manual_hold(db):
    upsert_vehicles([_car("a-com")])
    upsert_vehicles([_ev("b-com", "strong")])  # logged as scanner_guard:...
    assert upsert_vehicles([_ev("c-com", "strong", price=1)]) == 0  # two strong claims
    _set(db, "UPDATE cars SET rooftop_tier='none' WHERE vin=?", (VIN,))
    assert upsert_vehicles([_ev("c-com", "strong", price=1)]) == 1  # not held by its own log row


# decide() and the SQL lists agree on every case (the backstop is the same policy)

def test_decide_table():
    P = own.Pools((("p", ("p", "s")),), (frozenset({"a1", "a2"}),))
    d = lambda o, ot, c, ct, fresh=True, active=True: own.decide(o, ot, active, fresh, c, ct, P)  # noqa: E731
    assert d("a1", "strong", "a2", "none").reason == "alias"
    assert d("x", "none", "y", "strong").allow
    assert not d("x", "strong", "y", "strong").allow
    assert not d("x", "strong", "y", "none", fresh=False).allow
    assert d("s", "none", "p", "none").allow and not d("p", "none", "s", "none", fresh=False).allow
    assert d("x", "none", "y", "none", fresh=False).reason == "window_lapsed"
    assert d("x", "none", "y", "none").reason == "group_feed_unstamped"
    assert d("x", "strong", "y", "none", active=False).allow
    assert own.backstop_lists(P, "p") == ("", ",s,", ",s,")
    assert own.backstop_lists(P, "s") == ("", "", ",p,")
    assert own.backstop_lists(P, "a1") == (",a2,", "", "")


@pytest.mark.parametrize("scorer", ["1", "0"])
def test_gate_stamps_the_evidence_tier_on_kept_rows(monkeypatch, scorer):
    """Both gate paths: a stamped, name-matched row is strong; an unstamped row kept
    for want of evidence is none; the stamp is a short JSON record."""
    from backend.attribution.rooftop import resolve_rooftop_attribution

    monkeypatch.setenv("SCANNER_ROOFTOP_SCORER", scorer)
    rt = {"key": "Alpha Ford", "name": "Alpha Ford"}
    stamped = {"vin": "V1", "_rooftop": dict(rt), "_feed_source": "S1"}
    sister = {"vin": "V2", "_rooftop": {"key": "Beta Ford", "name": "Beta Ford"}}
    kept, rejected = resolve_rooftop_attribution([stamped, sister], dealer_id="alpha-ford", dealer_name="Alpha Ford",
                                                 dealer_url="https://alpha.example")
    assert kept == [stamped] and rejected == [sister]
    assert stamped["_rooftop_evidence_tier"] == "strong"
    ev = json.loads(stamped["_rooftop_evidence"])
    assert ev["rooftop"] == "Alpha Ford" and ev["source"] == "S1"

    plain = {"vin": "V3"}
    kept, _ = resolve_rooftop_attribution([plain], dealer_id="alpha-ford", dealer_name="Alpha Ford",
                                          dealer_url="https://alpha.example")
    assert kept == [plain] and plain["_rooftop_evidence_tier"] == "none" and plain["_rooftop_evidence"] is None
