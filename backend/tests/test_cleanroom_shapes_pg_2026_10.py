"""Clean-room failure shapes on Postgres, pinned as strict xfail tests (N7.9).

The same eight shapes as ``test_cleanroom_shapes_2026_10.py`` (N1.4), run through
the same scanner steps against a migration-built Postgres clone (``pg_db``), plus
one test of the VIN-owner SQL backstop under two concurrent connections. Every
guard and retirement test elsewhere is SQLite-only; the backstop compares
``cars.scraped_at`` as text and the reconcile UPDATEs return their own rowcounts,
so a fix that passes on SQLite can still fail here.

The fixtures, reasons and scan driver are the SQLite file's (imported, so the two
tiers cannot drift). Each test asserts the CORRECT behaviour and is
``xfail(strict=True)`` with the defect and its fixing N-unit: the unit that lands
the fix sees XPASS on both tiers and removes both markers (N9's exit gate runs
``-k "retire or retry or identity"``; N8.6 flips the scan-order, unstamped-feed
and concurrent-backstop shapes). The xfail reasons are the SQLite file's, so a
test here is never xfailed for a Postgres-only reason: a Postgres-specific
failure would show as a plain FAIL, not hide behind the marker.
"""
from __future__ import annotations

import asyncio
import json
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from backend.db import incomplete_listings_db
from backend.db.repositories import data_quality_repo
from backend.scanner import database as sdb
from backend.scanner import inventory_write as iw
from backend.tests.pipeline_patch import patch_pipeline
from backend.tests.test_cleanroom_shapes_2026_10 import (
    _CHAP,
    _COND_CODE,
    _FIVE,
    _IDENT,
    _IDENTITY_REASON,
    _LIFECYCLE_REASON,
    _RET,
    _STRIKE_REASON,
    _TERRY_REASON,
    _chapman_scans,
    _dealer,
    _first_pass_rows,
    _hours_between,
    _jazel_srp,
    _retry_rows,
    _scan,
    _shift_iso,
)

pytestmark = pytest.mark.pg

STEP = 10  # seconds: upper bound on any single wait in the concurrent test

_ISO_PG = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:"


# -- shared helpers ----------------------------------------------------------

@pytest.fixture()
def inv(pg_db, tmp_path, monkeypatch):
    """The SQLite file's ``inv`` env (the scanner env the clean room ran with) on a Postgres clone."""
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.setattr(incomplete_listings_db, "DB_PATH", str(tmp_path / "incomplete_listings.db"))
    data_quality_repo.clear_incomplete_snapshot_cache()
    monkeypatch.setenv("SCANNER_RECONCILE", "1")
    monkeypatch.setenv("SCANNER_AUTO_HEAL", "0")
    for name in ("SCANNER_RECONCILE_MIN_COVERAGE", "SCANNER_RECONCILE_MIN_ROWS", "SCANNER_VIN_OWNER_GUARD_HOURS"):
        monkeypatch.setenv(name, "")
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(tmp_path / "dealer_logs"))
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        lambda *a, **k: {},
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    yield pg_db
    data_quality_repo.clear_incomplete_snapshot_cache()


@pytest.fixture()
def log_root(tmp_path, monkeypatch):
    root = tmp_path / "dealer_logs"
    patch_pipeline(monkeypatch, "LOG_ROOT", root)
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(root))
    return root


def _q(env, sql: str, params: tuple = ()) -> list[tuple]:
    with env.connect() as conn:
        return conn.execute(sql, params).fetchall()


def _vins(env, dealer_id: str, *, active: bool) -> set[str]:
    flag = 1 if active else 0
    return {r[0] for r in _q(
        env, "SELECT vin FROM cars WHERE dealer_id = %s AND COALESCE(listing_active, 1) = %s", (dealer_id, flag))}


def _owner(env, vin: str) -> str | None:
    rows = _q(env, "SELECT dealer_id FROM cars WHERE vin = %s AND COALESCE(listing_active, 1) = 1", (vin,))
    return rows[0][0] if rows else None


def _age_db(env, hours: float) -> None:
    """Pretend everything stored so far happened ``hours`` earlier, on Postgres: every
    ISO-8601 text value moves back in its own text shape (``cars.scraped_at`` is TEXT), every
    timestamp column by an interval. The same job as the SQLite file's ``_age_db``."""
    delta = timedelta(hours=hours)
    with env.connect() as conn:
        cols = conn.execute(
            "SELECT c.table_name, c.column_name, c.data_type FROM information_schema.columns c "
            "JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
            "WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE' AND c.is_generated = 'NEVER' "
            "AND c.data_type IN ('text', 'character varying', 'timestamp with time zone', 'timestamp without time zone')"
        ).fetchall()
        for table, col, kind in cols:
            if kind.startswith("timestamp"):
                conn.execute(f'UPDATE "{table}" SET "{col}" = "{col}" - %s::interval WHERE "{col}" IS NOT NULL',
                             (f"{hours} hours",))
                continue
            values = [r[0] for r in conn.execute(
                f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" ~ %s', (_ISO_PG,)).fetchall()]
            for old in values:
                conn.execute(f'UPDATE "{table}" SET "{col}" = %s WHERE "{col}" = %s', (_shift_iso(old, delta), old))
        conn.commit()


def _table_counts(env) -> dict[str, int]:
    with env.connect() as conn:
        names = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ).fetchall()]
        return {n: conn.execute(f'SELECT COUNT(*) FROM "{n}"').fetchone()[0] for n in names}


def _reset_to(env, baseline: dict[str, int]) -> None:
    """Empty every table a scan wrote to (it was empty in *baseline*); a table that held seeded
    rows and changed would be a harness change, so it fails loudly instead of being cleared."""
    now = _table_counts(env)
    with env.connect() as conn:
        for name, n in now.items():
            if n == baseline.get(name, 0):
                continue
            assert baseline.get(name, 0) == 0, f"{name} held {baseline[name]} seeded row(s) and changed"
            conn.execute(f'TRUNCATE "{name}" RESTART IDENTITY CASCADE')
        conn.commit()
    sdb._vin_owner_table_ready = False


# -- 1-4. in-run retirements by the scanner reconcile on a lifecycle retry ------

@pytest.mark.parametrize(
    "dealer_id",
    [
        pytest.param("terrylabontechevy-com", marks=pytest.mark.xfail(strict=True, reason=_TERRY_REASON)),
        pytest.param("coastlinecdjr-com", marks=pytest.mark.xfail(strict=True, reason=_STRIKE_REASON)),
        pytest.param("napletonhondamortongrove-com", marks=pytest.mark.xfail(strict=True, reason=_STRIKE_REASON)),
        pytest.param("southcountylexus-com", marks=pytest.mark.xfail(strict=True, reason=_STRIKE_REASON)),
    ],
)
def test_retry_retirement_shape(inv, dealer_id):
    """Postgres twin of ``test_retry_retirement_shape``: the retry retires nothing in-run, and a
    second pass at least 20 h later retires exactly the unseen first-pass VINs (none for Terry)."""
    d = _RET["dealers"][dealer_id]
    first = d["first_pass"]
    unseen = {r["vin"] for r in first if not r["re_seen_by_retry"]}
    retry = _retry_rows(dealer_id)
    dealer = _dealer(dealer_id)

    _scan(dealer, _first_pass_rows(dealer_id))
    assert _vins(inv, dealer_id, active=True) == {r["vin"] for r in first}

    _age_db(inv, _hours_between(d["first_pass_at"], d["retry_at"]))
    _scan(dealer, retry)
    retired_in_run = _vins(inv, dealer_id, active=False)
    assert retired_in_run == set(), (
        f"{dealer_id}: inventory_reconcile retired {len(retired_in_run)} first-pass row(s) inside the run, on the "
        f"lifecycle retry scan (prior rows re-seen {len(first) - len(unseen)}/{len(first)}): {sorted(retired_in_run)}"
    )
    assert len(_vins(inv, dealer_id, active=True)) == len(retry) + len(unseen)

    _age_db(inv, 21)
    _scan(dealer, retry)
    expected = set() if dealer_id == "terrylabontechevy-com" else unseen
    assert _vins(inv, dealer_id, active=False) == expected, (
        f"{dealer_id}: second miss >= 20 h later should retire exactly {sorted(expected)}"
    )


# -- 5. the pipeline's lifecycle retry reconcile with a known=0 baseline --------

@pytest.mark.xfail(strict=True, reason=_LIFECYCLE_REASON)
def test_lifecycle_retry_with_known_zero_baseline_retires_nothing(inv, log_root, tmp_path, monkeypatch):
    """Postgres twin: the pipeline's lifecycle retry reconcile writes no retirement (D-RC1 A)."""
    from backend.db.repositories.dealers_repo import record_scan_outcomes
    from backend.scripts import dealer_pipeline as dp

    did = "terrylabontechevy-com"
    d = _RET["dealers"][did]
    first = {r["vin"] for r in d["first_pass"]}
    sdb.upsert_vehicles(_first_pass_rows(did))
    _age_db(inv, _hours_between(d["first_pass_at"], d["retry_at"]))

    def _retry_scan(dealer_ids, **kw):
        rows = _retry_rows(did)
        stats: dict[str, Any] = {}
        n = sdb.upsert_vehicles(rows, stats)
        cc = {"n": len(rows), "price": 1.0, "trim": 1.0, "exterior_color": 1.0}
        finished = (datetime.now(timezone.utc) + timedelta(seconds=1)).replace(microsecond=0).isoformat()
        record_scan_outcomes([{"dealer_id": did, "dealer_name": d["name"], "provider": d["provider"], "upserted": n,
                               "inventory_rows": len(rows), "deduped_rows": len(rows), "seconds": 60.0,
                               "capture_coverage": cc, "vin_owner_conflicts": stats.get("vin_owner_conflicts", 0),
                               "vin_owner_conflict_owners": stats.get("vin_owner_conflict_owners") or {}}],
                             finished_at=finished)
        return {"rc": [0], "chromium_leaks": []}

    patch_pipeline(monkeypatch, "_scan_hints", lambda dealer_id: {})
    patch_pipeline(monkeypatch, "run_lifecycle", lambda dealer, r, **k: {
        "lifecycle": "resynth_ok", "steps": [], "recipe": {"had_recipes": 1, "synth": "resynth_store_scoped"}})
    patch_pipeline(monkeypatch, "scan_retry_batch", _retry_scan)
    patch_pipeline(monkeypatch, "record_timing", lambda dealer_id, run: {"minutes": 1.0, "flags": []})
    patch_pipeline(monkeypatch, "verify_accuracy", lambda conn, dealer_id, since: {
        "rows": 585, "incomplete_rows": 0, "missing": {}, "hard": {}, "hard_rows": 0})

    results = [{"dealer_id": did, "verdict": "no_rows", "rows": 16, "recipe": {"had_recipes": 1},
                "reason": "16 rows stamped to this store of 323 in the feed"}]
    dealers = {did: _dealer(did)}
    summary = dp.run_lifecycle_pass(results, dealers, {did: 0}, out_dir=tmp_path / "out", stamp="2026-10-14 13:16 UTC",
                                    no_vpic=True, known_buckets={did: {"new": 0, "used": 0, "unknown": 0}})
    assert did in summary["retried"], summary
    assert results[0].get("verdict") in ("ok", "thin", "inaccurate"), results[0].get("reason")

    retired = _vins(inv, did, active=False)
    assert not retired, (
        f"the lifecycle retry reconcile retired {len(retired)} row(s) with known_before=0 "
        f"(reconcile={json.dumps(results[0].get('reconcile'), default=str)[:300]})"
    )
    assert first <= _vins(inv, did, active=True)


# -- 6. VIN-owner guard: the owner depends on which store wrote first -----------

def test_guard_scan_order_gives_the_same_owner(inv):
    """Postgres twin: both scan orders file every Chapman Dodge car under chapmandodge-com (D-N3)."""
    scans = _chapman_scans()
    vins = [c["vin"] for c in _CHAP["cars"]]
    baseline = _table_counts(inv)
    owners: dict[str, dict[str, str | None]] = {}
    for label, order in (("ford_first", ("chapmanfordaz-com", "chapmandodge-com")),
                         ("dodge_first", ("chapmandodge-com", "chapmanfordaz-com"))):
        _reset_to(inv, baseline)
        for did in order:
            dealer, rows, place = scans[did]
            _scan(dealer, rows, place=place)
        owners[label] = {v: _owner(inv, v) for v in vins}
    differ = {v: (owners["ford_first"][v], owners["dodge_first"][v]) for v in vins
              if owners["ford_first"][v] != owners["dodge_first"][v]}
    assert not differ, f"owner depends on scan order (ford_first, dodge_first): {differ}"
    assert set(owners["ford_first"].values()) == {"chapmandodge-com"}, owners["ford_first"]


# -- 7. an unstamped group feed files sister and off-roster cars under the store -

def test_unstamped_group_feed_files_no_sister_or_offroster_cars_under_the_store(inv):
    """Postgres twin: only the 810 North Richland Hills cars are filed under 5starford-com, Dallas's own
    land under fivestarforddallas-com, and off-roster rooftops' cars are held (D-N4)."""
    cards = _FIVE["cards"]
    by_city: dict[str, set[str]] = {}
    for c in cards:
        by_city.setdefault(c["vdp_city"], set()).add(c["vin"])
    off_roster = by_city["Carrollton"] | by_city["Lewisville"] | by_city["Grapevine"]

    for did, feed in (("5starford-com", cards), ("fivestarforddallas-com", [c for c in cards if c["in_dallas_feed"]])):
        ro = _FIVE["roster"][did]
        dealer = {"dealer_id": did, "name": ro["name"], "url": ro["url"], "provider": "jazel"}
        place = {"dealer_city": ro["city"], "dealer_state": ro["state"], "dealer_zip": ro["zip"]}
        _scan(dealer, place=place, intercepts=[(f"{ro['url']}/inventory/all-vehicles/", _jazel_srp(feed))])
        if did == "5starford-com":
            filed = _vins(inv, did, active=True)
            misfiled = filed - by_city["North_Richland_Hills"]
            city_of = {c["vin"]: c["vdp_city"] for c in cards}
            assert not misfiled, (
                f"{len(misfiled)} car(s) of other Sam Pack rooftops filed under 5starford-com: "
                f"{dict(Counter(city_of[v] for v in misfiled).most_common())}"
            )
            assert filed == by_city["North_Richland_Hills"]

    assert by_city["Dallas"] <= _vins(inv, "fivestarforddallas-com", active=True)
    held = {v: _owner(inv, v) for v in off_roster if _owner(inv, v) in ("5starford-com", "fivestarforddallas-com")}
    assert not held, f"off-roster rooftop cars filed under a sister store: {len(held)}"


# -- 8. triage accounting: the identity replayed from the 10-13 triage ----------

def _seed_identity(env) -> dict[str, int]:
    """cars and scan_runs as the clean room left them, per dealer (the SQLite file's seed, set-based
    on Postgres: one INSERT ... generate_series per dealer, condition and state)."""
    from backend.db.repositories.dealers_repo import record_scan_outcomes

    cols = _IDENT["columns"]
    active: dict[str, int] = {}
    with env.connect() as conn:
        for i, rec in enumerate(_IDENT["dealers"]):
            row = dict(zip(cols, rec))
            did, cars = row["dealer_id"], row["cars"] or {"active": {}, "inactive": {}}
            active[did] = sum(cars["active"].values())
            for state, flag in (("active", 1), ("inactive", 0)):
                ts = cars.get(f"{state}_scraped_at")
                for cond, n in sorted(cars[state].items()):
                    if not n:
                        continue
                    prefix = f"{i:04d}{flag}{_COND_CODE.get(cond, 'X')}"
                    conn.execute(
                        "INSERT INTO cars (dealer_id, vin, condition, scraped_at, first_seen_at, listing_active, "
                        "listing_removed_at, dealer_name) "
                        "SELECT %s, %s || lpad(j::text, 11, '0'), %s, %s, %s, %s, %s, %s FROM generate_series(0, %s) AS j",
                        (did, prefix, cond or None, ts, ts, flag, None if flag else ts, did, n - 1))
        conn.commit()
    for rec in _IDENT["dealers"]:
        row = dict(zip(cols, rec))
        sr = row["scan_run"]
        if not sr:
            continue
        s = sr["summary"]
        record_scan_outcomes([{
            "dealer_id": row["dealer_id"], "dealer_name": row["dealer_id"], "provider": sr["provider"],
            "seconds": sr["duration_seconds"], "upserted": sr["upserted"], "inventory_rows": sr["inventory_rows"],
            "deduped_rows": sr["deduped_rows"], "error": sr["error"], "capture_coverage": s["capture_coverage"],
            "vin_owner_conflicts": s["vin_owner_conflicts"], "vin_owner_conflict_owners": s["vin_owner_conflict_owners"],
            "reconcile": {"marked_inactive": s["reconcile_marked_inactive"]},
        }], finished_at=sr["finished_at"])
    return active


@pytest.mark.xfail(strict=True, reason=_IDENTITY_REASON)
def test_triage_identity_replay_rows_written_equals_active(inv, monkeypatch):
    """Postgres twin: per dealer, rows_written equals its active rows; fleet captured - refused = written."""
    from backend.scanner.pipeline.assess import assess
    from backend.scanner.pipeline.db import _assess_conn

    exp = _IDENT["expected"]
    active = _seed_identity(inv)
    n_active = _q(inv, "SELECT COUNT(*) FROM cars WHERE COALESCE(listing_active, 1) = 1")[0][0]
    n_inactive = _q(inv, "SELECT COUNT(*) FROM cars WHERE COALESCE(listing_active, 1) = 0")[0][0]
    assert (n_active, n_inactive) == (exp["active"], exp["inactive"])

    patch_pipeline(monkeypatch, "record_timing", lambda dealer_id, run: {"minutes": 0.0, "flags": []})
    patch_pipeline(monkeypatch, "verify_accuracy", lambda conn, dealer_id, since: {"rows": 0})
    since = _IDENT["fleet_started"].replace("Z", "+00:00")
    cols = _IDENT["columns"]
    conn = _assess_conn()
    try:
        tri = [assess(conn, r["dealer_id"], since, int(r["known_before"] or 0), {"had_recipes": 1})
               for r in (dict(zip(cols, rec)) for rec in _IDENT["dealers"])]
    finally:
        conn.close()

    with_run = {r["dealer_id"] for r in (dict(zip(cols, rec)) for rec in _IDENT["dealers"]) if r["scan_run"]}
    wrong = {t["dealer_id"]: (t.get("rows_written"), active[t["dealer_id"]]) for t in tri
             if t.get("rows_written") != active[t["dealer_id"]]}
    rows_today = sum(int(t.get("rows") or 0) for t in tri)
    no_rows = {t["dealer_id"]: active[t["dealer_id"]] for t in tri if t.get("rows") is None and active[t["dealer_id"]]}
    assert not wrong, (
        f"rows_written != active rows for {len(wrong)} of {len(tri)} dealers; triage 'rows' sums to {rows_today:,} "
        f"(the captured feed, before the VIN-owner guard) against {n_active:,} active; dealers written without a "
        f"scan_runs row report rows=None: {no_rows}"
    )
    assert sum(int(t["rows_captured"]) for t in tri if t["dealer_id"] in with_run) == exp["rows_captured"]
    assert sum(int(t["rows_refused_vin_owner"]) for t in tri if t["dealer_id"] in with_run) == exp["rows_refused_vin_owner"]
    assert sum(int(t["rows_written"]) for t in tri if t["dealer_id"] in with_run) == exp["rows_written_with_scan_run"]
    assert sum(int(t["rows_written"]) for t in tri if t["dealer_id"] not in with_run) == exp["rows_written_without_scan_run"]
    assert sum(int(t["rows_written"]) for t in tri) == n_active == exp["active"]


# -- 9. the SQL backstop under two concurrent connections ------------------------

_VIN = "1N4BL4DV0PN300001"


def _claim(dealer_id: str, **fields) -> dict[str, Any]:
    row = {"vin": _VIN, "title": "2023 Nissan Altima SV", "year": 2023, "make": "Nissan", "model": "Altima",
           "trim": "SV", "price": 30000, "mileage": 12000, "condition": "Used",
           "image_url": "https://img.example.test/altima.jpg", "dealer_id": dealer_id, "dealer_name": dealer_id,
           "dealer_url": f"https://{dealer_id}.example.test"}
    row.update(fields)
    return row


def test_backstop_evidence_claimant_beats_unstamped_owner_on_concurrent_connections(inv, monkeypatch):
    """Two writers read 'no owner' before either writes (both prefetches meet at a barrier), so the SQL
    backstop alone decides. The unstamped store writes and commits first; the store whose own feed names
    it (``_rooftop``) writes second, on its own connection. Correct (D-N3): the evidence-backed claimant
    owns the VIN, whatever the order. Deterministic: the second writer waits for the first to finish."""
    monkeypatch.setenv("SCANNER_PARALLEL_UPSERT", "1")
    barrier = threading.Barrier(2, timeout=STEP)
    first_done = threading.Event()
    real_prefetch = sdb.prefetch_existing

    def prefetch_then_meet(*args, **kwargs):
        result = real_prefetch(*args, **kwargs)
        barrier.wait()  # both prefetches done (and empty) before any INSERT
        if threading.current_thread().name.startswith("stamped"):
            assert first_done.wait(STEP), "the unstamped writer never finished"
        return result

    monkeypatch.setattr(sdb, "prefetch_existing", prefetch_then_meet)
    # The attribution gate stamps the tier on rows it keeps (N8.6b); the upsert only reads it.
    stamped = _claim("stamped-store", _rooftop={"key": "acct-1", "name": "Stamped Store", "site": "https://stamped-store.example.test"},
                     _rooftop_evidence_tier="strong", _rooftop_evidence={"feed": "stamped-store", "key": "acct-1"})

    async def run():
        co = iw.InventoryWriteCoordinator()
        assert co.parallel

        async def unstamped():
            n = await co.upsert_vehicles([_claim("unstamped-store")], {})
            first_done.set()
            return n

        loop = asyncio.get_running_loop()
        # the stamped writer runs on a named thread so the barrier hook can tell the two apart
        named = loop.run_in_executor(
            _named_pool(), lambda: sdb.upsert_vehicles([stamped], {}))
        return await asyncio.gather(unstamped(), named)

    asyncio.run(asyncio.wait_for(run(), timeout=STEP * 3))
    assert _owner(inv, _VIN) == "stamped-store", (
        f"owner {_owner(inv, _VIN)}; conflicts "
        f"{_q(inv, 'SELECT owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts')}"
    )


def _named_pool():
    from concurrent.futures import ThreadPoolExecutor

    return ThreadPoolExecutor(max_workers=1, thread_name_prefix="stamped")
