"""Listing retirement on a real Postgres: W1, W2, W3 and the portal path (P14A.8, ``pg`` tier).

Every retirement test elsewhere runs on SQLite. These drive the four writers that
set ``listing_active = 0`` against a migration-built clone (``pg_db``) and assert
the row counts they report equal the rows that changed:

* W1  ``inventory_reconcile.reconcile_dealer_inventory_after_scan`` (the scanner's
      reconcile, ``UPDATE ... WHERE id IN``);
* W2  ``pipeline.reconcile.reconcile_dealer``, REPORT ONLY since P5A.7 (whole-lot and
      per-bucket counts; the ``scraped_at < since`` comparison is TEXT on Postgres,
      ``'...Z'`` stored against ``'+00:00'`` passed; it writes nothing to ``cars``);
* W3  ``attribution.disown.disown_foreign_rooftop_vins``;
* W4  ``dealer.portal_sync.remove_portal_vehicle_from_public_listings``.

The tests pin today's behaviour (N9 changes W1/W2 to the single writer with
provenance, two-strike and dry mode; the cases here are the baseline it must keep
or consciously change). Retired rows keep their id and gain ``listing_removed_at``;
other dealers are never touched. P5B.3 / P5B.4 add the ``listing_retirements`` (V032) and ``listing_miss_strikes`` (V033)
cases at the end of this file.
"""

from __future__ import annotations

import pytest

from backend.attribution.disown import disown_foreign_rooftop_vins
from backend.dealer.portal_sync import dealer_portal_dealer_id, remove_portal_vehicle_from_public_listings
from backend.scanner import database as sdb
from backend.scanner.inventory_reconcile import reconcile_dealer_inventory_after_scan
from backend.scanner.pipeline.reconcile import reconcile_dealer

pytestmark = pytest.mark.pg

OLD = "2026-10-01T10:00:00Z"  # how the scanner stamps scraped_at (text)
SINCE = "2026-10-14T00:00:00+00:00"  # how the pipeline passes its start (text)


@pytest.fixture()
def env(pg_db, monkeypatch):
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    for name in ("SCANNER_RECONCILE", "SCANNER_RECONCILE_MIN_COVERAGE", "SCANNER_RECONCILE_MIN_ROWS"):
        monkeypatch.delenv(name, raising=False)
    # the baseline cases below pin the policy's ids; the two-strike cases at the end set 2
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "1")
    monkeypatch.setenv("SCANNER_RECONCILE_MIN_ROWS", "8")  # these lots are 10-20 cars; the default is 20
    return pg_db


def _vin(tag: str, i: int) -> str:
    return f"1RC{tag}{i:013d}"  # 17 characters


def _seed(env, dealer: str, tag: str, n: int, *, condition: str = "Used", scraped_at: str = OLD, start: int = 0) -> list[str]:
    vins = [_vin(tag, start + i) for i in range(n)]
    env.add_cars([
        {"vin": v, "dealer_id": dealer, "condition": condition, "scraped_at": scraped_at, "title": "2024 Test Car"}
        for v in vins
    ])
    return vins


def _state(env, dealer: str) -> dict[str, tuple[int, int, str | None]]:
    """``{vin: (id, listing_active, listing_removed_at)}`` for one dealer."""
    with env.connect() as conn:
        rows = conn.execute(
            "SELECT vin, id, listing_active, listing_removed_at FROM cars WHERE dealer_id = %s", (dealer,)
        ).fetchall()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def _inactive(env, dealer: str) -> set[str]:
    return {v for v, (_, active, _) in _state(env, dealer).items() if not active}


def _ids(env, dealer: str) -> dict[str, int]:
    return {v: s[0] for v, s in _state(env, dealer).items()}


# ── W1: the scanner's reconcile ───────────────────────────────────────────────

def test_w1_retires_only_the_unseen_rows_of_its_dealer(env):
    vins = _seed(env, "dealer-a", "A", 10)
    other = _seed(env, "dealer-b", "B", 10)
    ids_before = _ids(env, "dealer-a")
    seen = set(vins[:9])

    out = reconcile_dealer_inventory_after_scan(
        "dealer-a", "", seen, {"deduped_rows": 9}, scraped_conditions={"used"})

    assert out["ran"] is True and out["skipped_reason"] == "ok", out
    assert out["marked_inactive"] == 1 == len(_inactive(env, "dealer-a"))
    assert _inactive(env, "dealer-a") == {vins[9]}
    state = _state(env, "dealer-a")
    assert {v: s[0] for v, s in state.items()} == ids_before  # retired, never deleted
    assert state[vins[9]][2]  # listing_removed_at stamped
    assert all(state[v][2] is None for v in seen)
    assert _inactive(env, "dealer-b") == set() and set(_state(env, "dealer-b")) == set(other)


def test_w1_floor_and_coverage_gates_retire_nothing(env):
    vins = _seed(env, "dealer-a", "A", 10)

    below = reconcile_dealer_inventory_after_scan("dealer-a", "", set(vins[:7]), {"deduped_rows": 7})
    assert below["ran"] is False and below["skipped_reason"].startswith("below_min_rows")

    low = reconcile_dealer_inventory_after_scan("dealer-a", "", set(vins[:4]), {"deduped_rows": 9})
    assert low["ran"] is False and low["skipped_reason"] == "no_retirable_bucket"
    assert low["marked_inactive"] == 0

    assert _inactive(env, "dealer-a") == set()


def test_w1_keeps_a_condition_the_run_never_returned(env):
    used = _seed(env, "dealer-a", "U", 10, condition="Used")
    new = _seed(env, "dealer-a", "N", 10, condition="New")

    out = reconcile_dealer_inventory_after_scan(
        "dealer-a", "", set(used[:9]) | set(new), {"deduped_rows": 19}, scraped_conditions={"used", "new"})
    assert out["marked_inactive"] == 1 and _inactive(env, "dealer-a") == {used[9]}

    # a one-condition run: the new rows it never returned stay active
    env.add_cars([{"vin": _vin("M", i), "dealer_id": "dealer-c", "condition": "New", "scraped_at": OLD} for i in range(2)])
    used_c = _seed(env, "dealer-c", "V", 10, condition="Used")
    only_used = reconcile_dealer_inventory_after_scan(
        "dealer-c", "", set(used_c[:9]), {"deduped_rows": 9}, scraped_conditions={"used"})
    assert only_used["kept_missing_condition"] == 2 and only_used["marked_inactive"] == 1
    assert _inactive(env, "dealer-c") == {used_c[9]}


def test_w1_ignores_rows_without_a_valid_vin(env):
    vins = _seed(env, "dealer-a", "A", 9)
    env.add_cars([
        {"vin": "UNKNOWN-0001", "dealer_id": "dealer-a", "condition": "Used", "scraped_at": OLD},
        {"vin": "SHORT123", "dealer_id": "dealer-a", "condition": "Used", "scraped_at": OLD},
    ])
    out = reconcile_dealer_inventory_after_scan("dealer-a", "", set(vins[:8]), {"deduped_rows": 8},
                                                scraped_conditions={"used"})
    # P5A.1 rule 8: rows without a valid VIN retire only through a retirable bucket, and this one is
    assert out["marked_inactive"] == 3
    assert _inactive(env, "dealer-a") == {vins[8], "UNKNOWN-0001", "SHORT123"}


# ── W2: the pipeline's reconcile ──────────────────────────────────────────────

def test_w2_whole_lot_reports_rows_scraped_before_since_and_writes_nothing(env):
    stale = _seed(env, "dealer-a", "S", 4, scraped_at="2026-10-13T23:59:59Z")
    fresh = _seed(env, "dealer-a", "F", 24, scraped_at="2026-10-14T06:00:00Z")
    other = _seed(env, "dealer-b", "B", 4, scraped_at="2026-10-13T23:59:59Z")
    ids_before = _ids(env, "dealer-a")

    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        out = reconcile_dealer(conn, "dealer-a", SINCE, baseline=28, rows_this_run=24, verdict="ok")
    finally:
        conn.close()

    assert out["eligible"] is True and out["stale"] == 4 == out["retirable"] and out["retired"] == 0
    assert _inactive(env, "dealer-a") == set()
    state = _state(env, "dealer-a")
    assert {v: s[0] for v, s in state.items()} == ids_before
    assert all(state[v][2] is None for v in list(stale) + list(fresh))
    assert _retirements(env, "pipeline_w2") == []
    assert _inactive(env, "dealer-b") == set() and set(_state(env, "dealer-b")) == set(other)


def test_w2_dry_run_counts_and_writes_nothing(env):
    _seed(env, "dealer-a", "S", 4, scraped_at="2026-10-13T23:59:59Z")
    _seed(env, "dealer-a", "F", 24, scraped_at="2026-10-14T06:00:00Z")
    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        whole = reconcile_dealer(conn, "dealer-a", SINCE, 28, 24, "ok", dry_run=True)
        buckets = reconcile_dealer(conn, "dealer-a", SINCE, 28, 24, "ok", dry_run=True,
                                   baseline_buckets={"new": 0, "used": 28, "unknown": 0},
                                   rows_buckets={"new": 0, "used": 24, "unknown": 0})
    finally:
        conn.close()
    assert whole["stale"] == 4 and whole["retired"] == 0
    assert buckets["retirable"] == 4 and buckets["retired"] == 0
    assert _inactive(env, "dealer-a") == set()


def test_w2_text_comparison_of_z_against_plus_zero(env):
    """``scraped_at`` is TEXT: ``'...Z'`` and ``'...+00:00'`` order by character, not by instant.

    A row stamped at exactly the pipeline's start second compares AFTER it ('Z' sorts
    above '+'), so it is not stale; one second earlier is. Both Postgres and SQLite
    compare bytes here, so the boundary is the same; pinned because N9's run_started_iso
    work must keep one text shape on both sides."""
    at_start = _seed(env, "dealer-a", "E", 1, scraped_at="2026-10-14T00:00:00Z")
    before = _seed(env, "dealer-a", "B", 1, scraped_at="2026-10-13T23:59:59Z")
    _seed(env, "dealer-a", "F", 24, scraped_at="2026-10-14T06:00:00Z")
    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        out = reconcile_dealer(conn, "dealer-a", SINCE, baseline=26, rows_this_run=24, verdict="ok")
    finally:
        conn.close()
    assert out["stale"] == 1 == out["retirable"] and out["retired"] == 0
    assert _inactive(env, "dealer-a") == set()


def test_w2_floor_share_and_verdict_gates(env):
    _seed(env, "dealer-a", "S", 20, scraped_at="2026-10-13T23:59:59Z")
    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        bad_verdict = reconcile_dealer(conn, "dealer-a", SINCE, 20, 20, "no_rows")
        floor = reconcile_dealer(conn, "dealer-a", SINCE, 20, 3, "ok")
        share = reconcile_dealer(conn, "dealer-a", SINCE, 100, 20, "ok")
    finally:
        conn.close()
    assert bad_verdict["eligible"] is False and bad_verdict["reason"] == "verdict no_rows"
    assert floor["eligible"] is False and "absolute floor" in floor["reason"]
    assert share["eligible"] is False and "of baseline 100" in share["reason"]
    assert all(o["retired"] == 0 for o in (bad_verdict, floor, share))
    assert _inactive(env, "dealer-a") == set()


def test_w2_baseline_falls_back_to_the_prior_rows(env):
    """A zero 30-day baseline (the retry's known=0) falls back to the first pass's rows."""
    _seed(env, "dealer-a", "S", 16, scraped_at="2026-10-13T20:00:00Z")
    _seed(env, "dealer-a", "F", 24, scraped_at="2026-10-14T06:00:00Z")
    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        out = reconcile_dealer(conn, "dealer-a", SINCE, baseline=0, rows_this_run=24, verdict="ok")
    finally:
        conn.close()
    assert out["baseline_fallback"] == 16
    assert out["retirable"] == 16 and out["retired"] == 0 and _inactive(env, "dealer-a") == set()


def test_w2_per_bucket_guard_keeps_the_condition_the_run_missed(env):
    used = _seed(env, "dealer-a", "U", 6, condition="Used", scraped_at="2026-10-13T23:00:00Z")
    new = _seed(env, "dealer-a", "N", 6, condition="New", scraped_at="2026-10-13T23:00:00Z")
    _seed(env, "dealer-a", "R", 24, condition="New", scraped_at="2026-10-14T06:00:00Z")
    _seed(env, "dealer-b", "B", 3, condition="Used", scraped_at="2026-10-13T23:00:00Z")
    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        out = reconcile_dealer(
            conn, "dealer-a", SINCE, baseline=12, rows_this_run=24, verdict="ok",
            baseline_buckets={"new": 6, "used": 6, "unknown": 0},
            rows_buckets={"new": 24, "used": 0, "unknown": 0})
    finally:
        conn.close()
    assert out["kept_missing_condition"] == 6 and out["retirable"] == 6 and out["retired"] == 0
    assert _inactive(env, "dealer-a") == set()
    assert _inactive(env, "dealer-b") == set()


# ── W3: rooftop disown ────────────────────────────────────────────────────────

def test_w3_disown_returns_the_rowcount_and_scopes_to_the_dealer(env):
    mine = _seed(env, "dealer-a", "A", 5)
    theirs = _seed(env, "dealer-b", "B", 2)
    env.add_cars([{"vin": _vin("I", 0), "dealer_id": "dealer-a", "listing_active": 0, "scraped_at": OLD}])
    ids_before = _ids(env, "dealer-a")

    asked = {mine[0], mine[1], mine[2], theirs[0], _vin("I", 0), _vin("Z", 9)}
    assert disown_foreign_rooftop_vins("dealer-a", asked) == 3

    assert _inactive(env, "dealer-a") == set(mine[:3]) | {_vin("I", 0)}
    state = _state(env, "dealer-a")
    assert {v: s[0] for v, s in state.items()} == ids_before
    assert all(state[v][2] for v in mine[:3])
    assert state[_vin("I", 0)][2] is None  # already inactive: not counted, not restamped
    assert _inactive(env, "dealer-b") == set()
    assert disown_foreign_rooftop_vins("dealer-a", set(mine[:3])) == 0  # idempotent


def test_w3_disown_chunks_past_five_hundred_vins(env):
    vins = _seed(env, "dealer-a", "A", 620)
    assert disown_foreign_rooftop_vins("dealer-a", set(vins)) == 620
    assert len(_inactive(env, "dealer-a")) == 620


# ── W4: dealer-portal delete ──────────────────────────────────────────────────

def test_portal_delete_retires_only_the_callers_own_row(env):
    mine, other_user = dealer_portal_dealer_id(7), dealer_portal_dealer_id(8)
    env.add_cars([
        {"vin": "1PORTAL0000000007", "dealer_id": mine, "scraped_at": OLD},
        {"vin": "1PORTAL0000000008", "dealer_id": other_user, "scraped_at": OLD},
        {"vin": "1SCANNED000000009", "dealer_id": "dealer-a", "scraped_at": OLD},
    ])
    ids_before = _ids(env, mine)

    remove_portal_vehicle_from_public_listings(" 1portal0000000007 ", 7)
    remove_portal_vehicle_from_public_listings("1SCANNED000000009", 7)  # not user 7's row
    remove_portal_vehicle_from_public_listings("1PORTAL0000000008", 7)  # user 8's row
    remove_portal_vehicle_from_public_listings("", 7)

    assert _inactive(env, mine) == {"1PORTAL0000000007"}
    assert _ids(env, mine) == ids_before
    assert _state(env, mine)["1PORTAL0000000007"][2]
    assert _inactive(env, other_user) == set() and _inactive(env, "dealer-a") == set()


# ── P5B.3 provenance (V032) and P5B.4 two-strike (V033) ──────────────────────

def _retirements(env, writer: str | None = None) -> list[tuple]:
    q = "SELECT car_id, vin, dealer_id, writer, strikes FROM listing_retirements"
    args: tuple = ()
    if writer:
        q += " WHERE writer = %s"
        args = (writer,)
    with env.connect() as conn:
        return sorted(conn.execute(q + " ORDER BY car_id", args).fetchall())


def _strikes(env, dealer: str) -> dict[int, int]:
    with env.connect() as conn:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT car_id, strikes FROM listing_miss_strikes WHERE dealer_id = %s", (dealer,)).fetchall()}


def test_every_writer_leaves_one_provenance_row_per_retired_car(env):
    # W1
    a = _seed(env, "dealer-a", "A", 10)
    reconcile_dealer_inventory_after_scan("dealer-a", "", set(a[:9]), {"deduped_rows": 9}, scraped_conditions={"used"})
    w1 = _retirements(env, "scanner_reconcile")
    assert [(r[1], r[2], r[4]) for r in w1] == [(a[9], "dealer-a", 1)]
    assert w1[0][0] == _ids(env, "dealer-a")[a[9]]
    # W2 (report only since P5A.7: no row, no UPDATE)
    _seed(env, "dealer-b", "S", 4, scraped_at="2026-10-13T23:59:59Z")
    _seed(env, "dealer-b", "F", 24, scraped_at="2026-10-14T06:00:00Z")
    from backend.db.inventory_db import get_conn
    conn = get_conn()
    try:
        out = reconcile_dealer(conn, "dealer-b", SINCE, baseline=28, rows_this_run=24, verdict="ok")
    finally:
        conn.close()
    assert out["retired"] == 0 and out["retirable"] == 4 and _retirements(env, "pipeline_w2") == []  # P5A.7: report only
    # W3
    c = _seed(env, "dealer-c", "C", 5)
    assert disown_foreign_rooftop_vins("dealer-c", set(c[:3])) == 3
    assert [r[1] for r in _retirements(env, "disown")] == sorted(c[:3])
    # W4
    mine = dealer_portal_dealer_id(7)
    env.add_cars([{"vin": "1PORTAL0000000007", "dealer_id": mine, "scraped_at": OLD}])
    remove_portal_vehicle_from_public_listings("1PORTAL0000000007", 7)
    assert [(r[1], r[2]) for r in _retirements(env, "portal_sync")] == [("1PORTAL0000000007", mine)]


def test_admin_delete_paths_write_admin_delete_rows(env):
    from backend.dealer.admin.incomplete_listings_api import delete_incomplete_car
    from backend.db.repositories.cars_repo import delete_cars_with_dummy_placeholder_vins

    vins = _seed(env, "dealer-a", "A", 2)
    cid = _ids(env, "dealer-a")[vins[0]]
    from flask import Flask
    with Flask(__name__).app_context():
        delete_incomplete_car(cid)
    assert [(r[0], r[1], r[2]) for r in _retirements(env, "admin_delete")] == [(cid, vins[0], "dealer-a")]
    env.add_cars([{"vin": "VIN001", "dealer_id": "dealer-a", "scraped_at": OLD}])
    out = delete_cars_with_dummy_placeholder_vins()
    assert out["deleted"] >= 1
    assert len(_retirements(env, "admin_delete")) == 1 + out["deleted"]


def test_first_miss_strikes_second_miss_retires(env, monkeypatch):
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "2")
    vins = _seed(env, "dealer-a", "A", 10)
    gone = _ids(env, "dealer-a")[vins[9]]
    args = ("dealer-a", "", set(vins[:9]), {"deduped_rows": 9})

    first = reconcile_dealer_inventory_after_scan(*args, scraped_conditions={"used"})
    assert first["retired"] == 0 and first["struck"] == 1 and _inactive(env, "dealer-a") == set()
    assert _strikes(env, "dealer-a") == {gone: 1} and _retirements(env) == []

    again = reconcile_dealer_inventory_after_scan(*args, scraped_conditions={"used"})  # same day: waits
    assert again["retired"] == 0 and again["strike_waiting"] == 1 and _inactive(env, "dealer-a") == set()

    with env.connect() as conn:  # age the first miss by 21 h
        conn.execute("UPDATE listing_miss_strikes SET first_missed_at = %s", ("2020-01-01T00:00:00+00:00",))
        conn.commit()
    second = reconcile_dealer_inventory_after_scan(*args, scraped_conditions={"used"})
    assert second["retired"] == 1 and _inactive(env, "dealer-a") == {vins[9]}
    assert _strikes(env, "dealer-a") == {}
    assert [(r[0], r[4]) for r in _retirements(env, "scanner_reconcile")] == [(gone, 2)]


def test_dry_mode_writes_no_strikes_and_no_retirements(env, monkeypatch):
    monkeypatch.setenv("SCANNER_RETIRE_STRIKES", "2")
    monkeypatch.setenv("SCANNER_RECONCILE", "dry")
    vins = _seed(env, "dealer-a", "A", 10)
    out = reconcile_dealer_inventory_after_scan(
        "dealer-a", "", set(vins[:9]), {"deduped_rows": 9}, scraped_conditions={"used"})
    assert out["dry_run"] and out["would_strike"] == 1 and out["would_retire"] == 0
    assert _strikes(env, "dealer-a") == {} and _retirements(env) == [] and _inactive(env, "dealer-a") == set()
