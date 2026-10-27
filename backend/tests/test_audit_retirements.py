"""audit_retirements: classify (P5B.5) and verify, apply, rollback (P5B.6).

The database tests run on the Postgres harness (``pg_db``, ``pg`` tier); the verdict
logic (VDP judge, host match, replay verdicts) is pure and runs everywhere.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.scripts import audit_retirements as ar

TS = "2026-09-27T14:03:22+00:00"
OLD = "2026-09-20T10:00:00Z"
NEW = "2026-09-27T14:00:00Z"


def _vin(tag: str, i: int) -> str:
    return f"1AR{tag}{i:013d}"


class CountingConn:
    """Counts commits on a real connection, to prove the classify pass never commits."""

    def __init__(self, conn):
        self.conn, self.commits = conn, 0

    def cursor(self, *a, **k):
        return self.conn.cursor(*a, **k)

    def commit(self):
        self.commits += 1
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


# --------------------------------------------------------------------------
# pure logic
# --------------------------------------------------------------------------

def test_exclude_file_adds_to_the_seeded_lexus_entry(tmp_path):
    f = tmp_path / "ex.txt"
    f.write_text("# comment\nfoo-com,2026-09-25,hand check\n\n", encoding="utf-8")
    ex = ar.load_excludes(f)
    assert ("lexusofknoxville-com", "2026-09-27", ex[0][2]) == ex[0]
    assert ar._excluded(ex, "foo-com", "2026-09-25T01:00:00+00:00") == "hand check"
    assert ar._excluded(ex, "foo-com", "2026-09-26T01:00:00+00:00") is None


@pytest.mark.parametrize("status,final,body,verdict", [
    (200, "u", "<h1>VIN 1ARX0000000000001</h1>", "live"),
    (200, "u", "VIN 1ARX0000000000001 This vehicle has been sold", "gone"),
    (404, "u", "", "gone"),
    (410, "u", "", "gone"),
    (200, "https://d.com/used-inventory/", "grid", "gone"),
    (403, "u", "", "cannot_assess"),
    (429, "u", "", "cannot_assess"),
    (None, "u", "", "cannot_assess"),
    (200, "u", "Just a moment... 1ARX0000000000001", "cannot_assess"),
    (200, "u", "no vin on this page", "cannot_assess"),
])
def test_judge_vdp(status, final, body, verdict):
    got, _why = ar.judge_vdp("1ARX0000000000001", "u", status, final, body)
    assert got == verdict


def test_vdp_host_mismatch_is_cannot_assess_and_never_fetched():
    calls = []
    cands = [
        {"car_id": 1, "vin": "1ARX0000000000001", "dealer_id": "a-com", "source_url": "https://sister.com/v/1"},
        {"car_id": 2, "vin": "1ARX0000000000002", "dealer_id": "a-com", "source_url": "https://www.a.com/v/2"},
    ]

    def fetch(url):
        calls.append(url)
        return 200, url, "1ARX0000000000002"

    out = ar.verify_vdp(cands, {"a-com": "a.com"}, fetch=fetch, sleep=lambda s: None)
    assert out["1"] == {"verdict": "cannot_assess", "why": "host_mismatch", "method": "vdp"}
    assert out["2"]["verdict"] == "live"
    assert calls == ["https://www.a.com/v/2"]


def test_vdp_redirect_off_host_is_cannot_assess():
    cands = [{"car_id": 1, "vin": "1ARX0000000000001", "dealer_id": "a-com", "source_url": "https://a.com/v/1"}]
    out = ar.verify_vdp(cands, {"a-com": "a.com"}, fetch=lambda u: (200, "https://other.com/x", "1ARX0000000000001"),
                        sleep=lambda s: None)
    assert out["1"]["why"] == "redirected_off_host"


def test_replay_verdicts_need_a_complete_replay_to_call_gone():
    cands = [{"car_id": i, "vin": f"1ARX000000000000{i}", "dealer_id": "a-com"} for i in (1, 2)]
    full = ar.verify_replay(cands, {"a-com": "a.com"}, replay=lambda d, u: ({"1ARX0000000000001"}, True))
    assert (full["1"]["verdict"], full["2"]["verdict"]) == ("live", "gone")
    partial = ar.verify_replay(cands, {"a-com": "a.com"}, replay=lambda d, u: ({"1ARX0000000000001"}, False))
    assert (partial["1"]["verdict"], partial["2"]["verdict"]) == ("live", "cannot_assess")
    boom = ar.verify_replay(cands, {"a-com": "a.com"}, replay=lambda d, u: 1 / 0)
    assert {v["verdict"] for v in boom.values()} == {"cannot_assess"}
    nohost = ar.verify_replay(cands, {}, replay=lambda d, u: (set(), True))
    assert {v["verdict"] for v in nohost.values()} == {"cannot_assess"}


def test_scanner_log_parse(tmp_path):
    log = tmp_path / "scanner.log"
    log.write_text(
        "2026-09-27 14:03:20 Rooftop [Foo Toyota]: 12 car(s) belonged to sibling rooftops in this group\n"
        "Recipe [Foo Toyota] https://x/y — HTTP 403 after 7 page(s); keeping 90 VIN(s)\n"
        "Recipe walk [Bar] stopped at page_cap from 40 page(s)\n", encoding="utf-8")
    p = ar.parse_scanner_log(log)
    assert p["disown"] == [{"name": "Foo Toyota", "n": 12, "day": "2026-09-27"}]
    assert [t["name"] for t in p["trunc"]] == ["Foo Toyota", "Bar"]


# --------------------------------------------------------------------------
# classify on Postgres
# --------------------------------------------------------------------------

def _seed_retired(env, dealer, tag, n, *, condition="New", removed_at=TS, writer="scanner_reconcile",
                  source_url_host=None):
    vins = [_vin(tag, i) for i in range(n)]
    env.add_cars([
        {"vin": v, "dealer_id": dealer, "condition": condition, "scraped_at": OLD, "listing_active": 0,
         "listing_removed_at": removed_at, "title": "2024 Test",
         "source_url": f"https://{source_url_host or 'd1.com'}/vdp/{v}"}
        for v in vins
    ])
    if writer:
        with env.connect() as conn:
            for car_id, vin in conn.execute(
                "SELECT id, vin FROM cars WHERE vin = ANY(%s)", (vins,)
            ).fetchall():
                conn.execute(
                    "INSERT INTO listing_retirements (car_id, vin, dealer_id, writer, created_at) VALUES (%s,%s,%s,%s,%s)",
                    (car_id, vin, dealer, writer, TS),
                )
            conn.commit()
    return vins


def _seed_active(env, dealer, tag, n, *, condition="Used"):
    env.add_cars([
        {"vin": _vin(tag, i), "dealer_id": dealer, "condition": condition, "scraped_at": NEW, "title": "2024 Test"}
        for i in range(n)
    ])


def _seed_run(env, dealer, *, buckets=None, replay=None, name="D1 Toyota"):
    summary = {}
    if buckets is not None:
        summary["reconcile"] = {"buckets": buckets}
    if replay is not None:
        summary["recipe_replay"] = replay
    env._insert("scan_runs", [{
        "dealer_id": dealer, "dealer_name": name, "finished_at": "2026-09-27T14:05:00+00:00",
        "duration_seconds": 300.0, "summary_json": json.dumps(summary),
    }], lambda i, r: {})


def _classify(env, **kw):
    conn = CountingConn(env.get_conn())
    try:
        res = ar.classify(conn, since="2026-09-23", **kw)
        assert conn.commits == 0
        return res
    finally:
        conn.close()


def _by_dealer(res):
    return {b["dealer_id"]: b for b in res["batches"]}


@pytest.mark.pg
def test_one_condition_batch_is_flagged(pg_db):
    _seed_retired(pg_db, "d1-com", "A", 30, condition="New")
    _seed_active(pg_db, "d1-com", "B", 25, condition="Used")
    _seed_run(pg_db, "d1-com", buckets={"new": {"matched": 0}, "used": {"matched": 25}})
    res = _classify(pg_db)
    b = _by_dealer(res)["d1-com"]
    assert b["status"] == "audited"
    sigs = b["signatures"].split("|")
    assert "one_condition" in sigs and "bucket_now_empty" in sigs and "large_share" in sigs
    assert len(res["candidates"]) == 30 and {c["dealer_id"] for c in res["candidates"]} == {"d1-com"}


@pytest.mark.pg
def test_one_condition_falls_back_to_scraped_at_when_the_plan_is_missing(pg_db):
    _seed_retired(pg_db, "d1-com", "A", 30, condition="New")
    _seed_active(pg_db, "d1-com", "B", 25, condition="Used")  # scraped_at inside the run window
    _seed_run(pg_db, "d1-com")
    b = _by_dealer(_classify(pg_db))["d1-com"]
    assert "one_condition" in b["signatures"] and "cars_scraped_at" in b["note"]


@pytest.mark.pg
def test_small_healthy_batch_is_not_flagged(pg_db):
    _seed_retired(pg_db, "d1-com", "A", 3, condition="Used")
    _seed_active(pg_db, "d1-com", "B", 100, condition="Used")
    _seed_run(pg_db, "d1-com", buckets={"used": {"matched": 100}})
    res = _classify(pg_db)
    assert _by_dealer(res)["d1-com"]["signatures"] == "" and res["candidates"] == []


@pytest.mark.pg
def test_excluded_batch_is_not_a_candidate(pg_db):
    _seed_retired(pg_db, "lexusofknoxville-com", "L", 40, condition="Used", removed_at="2026-09-27T09:00:00+00:00")
    res = _classify(pg_db)
    b = _by_dealer(res)["lexusofknoxville-com"]
    assert b["status"] == "excluded:exclude_list" and res["candidates"] == []


@pytest.mark.pg
def test_disown_batch_is_not_a_candidate_by_provenance_or_by_log(pg_db, tmp_path):
    _seed_retired(pg_db, "d1-com", "A", 30, writer="disown")
    _seed_retired(pg_db, "d2-com", "B", 12, writer=None, removed_at="2026-09-27T15:00:00+00:00")
    log = tmp_path / "scanner.log"
    log.write_text("2026-09-27 15:00:01 Rooftop [d2-com]: 12 car(s) belonged to sibling rooftops\n", encoding="utf-8")
    res = _classify(pg_db, scanner_log=log)
    got = _by_dealer(res)
    assert got["d1-com"]["status"] == "excluded:disown"
    assert got["d2-com"]["status"] == "excluded:disown"
    assert res["candidates"] == []


@pytest.mark.pg
def test_same_second_disown_and_reconcile_without_provenance_is_held_for_review(pg_db, tmp_path):
    _seed_retired(pg_db, "d2-com", "B", 50, writer=None)
    log = tmp_path / "scanner.log"
    log.write_text("2026-09-27 14:03:22 Delta [d2-com]: 12 car(s) belonged to sibling rooftops\n", encoding="utf-8")
    res = _classify(pg_db, scanner_log=log)
    assert _by_dealer(res)["d2-com"]["status"] == "excluded:mixed_disown" and res["candidates"] == []


@pytest.mark.pg
def test_truncated_replay_is_flagged_from_the_summary_and_from_the_log(pg_db, tmp_path):
    _seed_retired(pg_db, "d1-com", "A", 5, condition="Used")
    _seed_active(pg_db, "d1-com", "B", 200, condition="Used")
    _seed_run(pg_db, "d1-com", buckets={"used": {"matched": 100}}, replay={"walks": [{"end": "page_cap", "complete": False}]})
    assert "truncation_suspect" in _by_dealer(_classify(pg_db))["d1-com"]["signatures"]

    _seed_retired(pg_db, "d3-com", "C", 5, condition="Used")
    _seed_active(pg_db, "d3-com", "D", 200, condition="Used")
    _seed_run(pg_db, "d3-com", buckets={"used": {"matched": 100}}, name="D3 Honda")
    log = tmp_path / "scanner.log"
    log.write_text("Recipe walk [D3 Honda] stopped at page_cap from 40 page(s)\n", encoding="utf-8")
    assert "truncation_suspect" in _by_dealer(_classify(pg_db, scanner_log=log))["d3-com"]["signatures"]


@pytest.mark.pg
def test_default_run_writes_nothing_and_commit_is_refused(pg_db, tmp_path):
    _seed_retired(pg_db, "d1-com", "A", 30, condition="New")
    _seed_active(pg_db, "d1-com", "B", 25, condition="Used")
    before = _snapshot(pg_db)
    guarded = ar.ReadOnlyConnection(pg_db.get_conn())
    try:
        res = ar.classify(guarded, since="2026-09-23")
        with pytest.raises(RuntimeError, match="read-only"):
            guarded.commit()
    finally:
        guarded.close()
    out = tmp_path / "audit"
    ar.write_report(out, res, since="2026-09-23")
    assert {p.name for p in out.iterdir()} == {"batches.csv", "candidates.csv", "report.md"}
    assert len(ar.read_candidates(out)) == 30
    assert _snapshot(pg_db) == before


def _snapshot(env):
    with env.connect() as conn:
        cars = conn.execute("SELECT id, listing_active, listing_removed_at FROM cars ORDER BY id").fetchall()
        logs = conn.execute("SELECT count(*) FROM listing_retirements").fetchone()[0]
    return cars, logs


# --------------------------------------------------------------------------
# apply and rollback on Postgres
# --------------------------------------------------------------------------

@pytest.fixture()
def audit(pg_db, tmp_path, monkeypatch):
    """30 retired new rows for d1-com audited, with an audit directory and tmp log roots."""
    from backend.scanner.pipeline import dealer_logs, lifecycle

    monkeypatch.setattr(dealer_logs, "LOG_ROOT", tmp_path / "logs")
    monkeypatch.setattr(lifecycle, "LOG_ROOT", tmp_path / "logs")
    vins = _seed_retired(pg_db, "d1-com", "A", 30, condition="New")
    _seed_active(pg_db, "d1-com", "B", 25, condition="Used")
    _seed_run(pg_db, "d1-com", buckets={"new": {"matched": 0}, "used": {"matched": 25}})
    res = _classify(pg_db)
    d = tmp_path / "audit"
    ar.write_report(d, res, since="2026-09-23")
    with pg_db.connect() as conn:
        ids = {v: i for v, i in conn.execute("SELECT vin, id FROM cars WHERE dealer_id = 'd1-com'").fetchall()}
    return pg_db, d, vins, ids, tmp_path


def _verdicts(audit_dir, ids, vins, live, gone=()):
    v = {str(ids[x]): {"verdict": "live", "method": "replay"} for x in live}
    v.update({str(ids[x]): {"verdict": "gone", "method": "replay"} for x in gone})
    (audit_dir / "verify.json").write_text(json.dumps(v), encoding="utf-8")


@pytest.mark.pg
def test_apply_requires_a_verify_file(audit):
    env, d, *_ = audit
    conn = env.get_conn()
    try:
        with pytest.raises(SystemExit, match="verify"):
            ar.apply_restore(conn, d)
    finally:
        conn.close()


@pytest.mark.pg
def test_apply_touches_only_verified_live_ids_and_skips_rows_changed_since(audit):
    env, d, vins, ids, tmp = audit
    _verdicts(d, ids, vins, live=vins[:10], gone=vins[10:12])
    with env.connect() as conn:  # row 0 re-retired later; row 1 moved to another dealer
        conn.execute("UPDATE cars SET listing_removed_at = '2026-09-28T00:00:00+00:00' WHERE id = %s", (ids[vins[0]],))
        conn.execute("UPDATE cars SET dealer_id = 'other-com' WHERE id = %s", (ids[vins[1]],))
        conn.commit()
    conn = env.get_conn()
    try:
        res = ar.apply_restore(conn, d, backup_dir=tmp / "bk", stamp="T1")
    finally:
        conn.close()
    assert (res["live"], res["restored"], res["skipped"]) == (10, 8, 2)
    with env.connect() as conn:
        active = {r[0] for r in conn.execute(
            "SELECT vin FROM cars WHERE dealer_id = 'd1-com' AND listing_active = 1 AND vin LIKE '1ARA%'").fetchall()}
        removed = {r[0] for r in conn.execute(
            "SELECT listing_removed_at FROM cars WHERE vin = ANY(%s) AND listing_active = 1", (vins,)).fetchall()}
        recs = conn.execute("SELECT count(*) FROM listing_retirements WHERE writer = 'restore'").fetchone()[0]
        untouched = conn.execute(
            "SELECT count(*) FROM cars WHERE vin = ANY(%s) AND listing_active = 0", (vins,)).fetchone()[0]
    assert active == set(vins[2:10]) and removed == {None} and recs == 8
    assert untouched == 22  # gone, unverified, skipped
    backup = json.loads(Path(res["backup"]).read_text(encoding="utf-8"))
    assert {r["id"] for r in backup["rows"]} == {ids[v] for v in vins[2:10]}
    summary = (tmp / "logs" / "d1-com" / "summary.md").read_text(encoding="utf-8")
    assert "8 row(s) verified live" in summary and (tmp / "logs" / "d1-com" / "scan_runs.md").exists()
    assert "wrongly_retired_restored:8" in (tmp / "logs" / "_learning" / "errors_index.md").read_text(encoding="utf-8")


@pytest.mark.pg
def test_rollback_round_trips_to_the_pre_apply_state(audit):
    env, d, vins, ids, tmp = audit
    _verdicts(d, ids, vins, live=vins[:10])
    before = _cars_state(env)
    conn = env.get_conn()
    try:
        res = ar.apply_restore(conn, d, backup_dir=tmp / "bk", stamp="T2")
    finally:
        conn.close()
    assert _cars_state(env) != before
    conn = env.get_conn()
    try:
        out = ar.rollback(conn, Path(res["backup"]))
    finally:
        conn.close()
    assert out == {"rows": 10, "re_retired": 10, "skipped": 0}
    assert _cars_state(env) == before  # diff = 0
    conn = env.get_conn()
    try:  # a second rollback finds nothing in the restored state
        assert ar.rollback(conn, Path(res["backup"]))["re_retired"] == 0
    finally:
        conn.close()


def _cars_state(env):
    with env.connect() as conn:
        return conn.execute("SELECT id, listing_active, listing_removed_at FROM cars ORDER BY id").fetchall()
