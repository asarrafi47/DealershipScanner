"""P5A.7 on a real Postgres (``pg`` tier): valid-VIN baselines, the report-only reconcile, the
``replay_truncated:`` verdict (downeyhyundai) and the gap-aware retirement alarm (D-N21)."""
from __future__ import annotations

import json

import pytest

from backend.db.retirement_log import ensure_retirement_tables, record_retirements
from backend.scanner.pipeline import reconcile as rec
from backend.scanner.pipeline.assess import assess

pytestmark = pytest.mark.pg

SINCE = "2026-10-20T12:00:00+00:00"
D = "d-p5a7"


@pytest.fixture()
def env(pg_db):
    return pg_db


def _vin(tag: str, i: int) -> str:
    return f"1P7{tag}{i:013d}"  # 17 characters


def _add(env, n, *, tag="V", cond="New", at="2026-10-01T00:00:00Z", vins=None):
    vins = vins or [_vin(tag, i) for i in range(n)]
    env.add_cars([{"vin": v, "dealer_id": D, "condition": cond, "scraped_at": at, "title": "2024 Test Car"} for v in vins])
    return vins


def _sql(env, sql, params=()):
    with env.connect() as conn:
        cur = conn.execute(sql, params)
        out = cur.fetchall() if cur.description else None
        conn.commit()
    return out


def _active(env) -> int:
    return _sql(env, "SELECT COUNT(*) FROM cars WHERE dealer_id = %s AND listing_active = 1", (D,))[0][0]


def _call(env, fn, *a, **kw):
    conn = env.get_conn()
    try:
        return fn(conn, *a, **kw)
    finally:
        conn.close()


def test_baselines_count_valid_vins_only(env):
    _add(env, 30)
    _add(env, 0, vins=[f"SHORT{i}" for i in range(5)])                 # not 17 characters long
    _add(env, 0, vins=[f"UNKNOWN-{i:09d}" for i in range(4)])          # placeholder, 17 characters
    assert _call(env, rec.valid_vin_count, D) == 30
    assert _call(env, rec.bucket_counts, D)["new"] == 30
    out = _call(env, rec.reconcile_dealer, D, SINCE, 30, 30, "ok",
                baseline_buckets={"new": 30, "used": 0, "unknown": 0}, rows_buckets={"new": 30, "used": 0, "unknown": 0})
    assert out["eligible"] and out["retirable"] == 30 and out["invalid_vin_stale"] == 9 and out["retired"] == 0
    assert _active(env) == 39


def test_reconcile_never_updates_even_when_not_dry(env):
    _add(env, 20)
    _add(env, 30, tag="N", at="2026-10-20T13:00:00Z")
    out = _call(env, rec.reconcile_dealer, D, SINCE, 50, 30, "ok", dry_run=False)
    assert out["eligible"] and out["retired"] == 0 and out["retirable"] == 20 and out["report_only"]
    assert _active(env) == 50


def _scan_run(env, truncated: bool):
    _add(env, 400, at="2026-10-20T12:30:00Z")
    summary = {"capture_coverage": {"n": 400, "price": 1.0, "trim": 1.0, "exterior_color": 1.0},
               "recipe_replay": {"truncated": truncated, "recipes": [
                   {"key": "GET /inv", "end": "http_403", "pages": 9, "raw_vins": 400, "total_count": 530,
                    "complete": not truncated, "stale_retry": False}]}}
    _sql(env, "INSERT INTO scan_runs (dealer_id, provider, finished_at, duration_seconds, inventory_rows, upserted, summary_json) "
              "VALUES (%s, 'dealer_dot_com', '2026-10-20T12:40:00+00:00', 600, 400, 400, %s)", (D, json.dumps(summary)))


def test_downeyhyundai_mid_walk_403_is_not_ok(env):
    _scan_run(env, truncated=True)
    out = _call(env, assess, D, SINCE, 450, {"had_recipes": 1})
    assert out["verdict"] == "thin" and out["verdict"] != "ok"
    assert out["reason"].startswith("replay_truncated:") and "400 of 530" in out["reason"]
    assert out["replay_truncated"] is True


def test_complete_replay_is_not_flagged(env):
    _scan_run(env, truncated=False)
    out = _call(env, assess, D, SINCE, 450, {"had_recipes": 1})
    assert out["replay_truncated"] is False and not str(out.get("reason")).startswith("replay_truncated")


def _retire(env, vins, writer="scanner_reconcile"):
    ids = {r[0]: r[1] for r in _sql(env, "SELECT vin, id FROM cars WHERE dealer_id = %s", (D,))}
    conn = env.get_conn()  # the product connection: record_retirements writes qmark SQL
    try:
        ensure_retirement_tables(conn)
        record_retirements(conn, [(ids[v], v, D) for v in vins], writer=writer, reason="not_in_run", strikes=2)
        conn.commit()
    finally:
        conn.close()
    _sql(env, "UPDATE listing_retirements SET created_at = %s WHERE dealer_id = %s", ("2026-10-20T13:00:00+00:00", D))
    _sql(env, "UPDATE cars SET listing_active = 0 WHERE vin = ANY(%s)", (list(vins),))


def test_nightly_alarm_fires_above_ten_percent(env):
    vins = _add(env, 100, at="2026-10-19T00:00:00Z")
    _retire(env, vins[:12])                                            # 12 of 100 prior active
    sr = _call(env, rec.scanner_retirements, D, SINCE, gap_aware=False)
    assert sr["retired"] == 12 and sr["prior_active"] == 100 and sr["alarm"] and sr["threshold"] == 0.10
    assert sr["by_writer"] == {"scanner_reconcile": 12}


def test_gap_aware_alarm_for_rows_last_scraped_35_days_ago(env):
    """Struck in run 1, retired in run 2: the threshold is min(60%, 62.5%) = 60%."""
    vins = _add(env, 100, at="2026-09-15T12:00:00Z")                   # 35 days before the run
    _retire(env, vins[:55])                                            # 55%: under 60%, over the nightly 10%
    nightly = _call(env, rec.scanner_retirements, D, SINCE, gap_aware=False)
    gap = _call(env, rec.scanner_retirements, D, SINCE, gap_aware=True)
    assert nightly["alarm"] and not gap["alarm"]
    assert gap["days"] == 35.0 and gap["threshold"] == 0.60
    _retire(env, vins[55:70])                                          # 70% now
    assert _call(env, rec.scanner_retirements, D, SINCE, gap_aware=True)["alarm"]


def test_alarm_ignores_other_writers(env):
    vins = _add(env, 50, at="2026-10-19T00:00:00Z")
    _retire(env, vins[:29], writer="admin_delete")
    assert _call(env, rec.scanner_retirements, D, SINCE, gap_aware=False)["retired"] == 0
