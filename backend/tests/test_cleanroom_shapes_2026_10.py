"""Clean-room failure shapes (fleet 2026-10-13/14), pinned as strict xfail tests (N1.4).

Every test drives the real code path on SQLite with a fixture cut from the
frozen ``cars_cleanroom`` database (read-only extraction, 2026-10-16; see each
fixture's ``source`` block) and asserts the CORRECT behaviour. Each one is
``xfail(strict=True)`` with the defect and the N-unit that fixes it in its
reason, so the unit that lands the fix sees it XPASS and removes the marker
(docs/NEXT_PHASES_PLAN_2026_10.md, N1.4; N9's exit gate runs
``-k "retire or retry or identity"``, N8.6 flips the scan-order and
unstamped-feed shapes).

Facts come from the N1 gather reports (workspace/phase_runs/N1/gather/):
retirements_rca.txt (the 21 in-run retirements, first-pass rows re-seen by the
retry: Terry 0/16, Coastline 8/11, Napleton 16/17, South County Lexus 14/15),
retire_attrib_code_map.txt (W1/W2 writers, the guard, the gate) and
conflicts_rca.md (first-writer-wins, the Chapman lot codes, the Sam Pack SRP).

Owner decisions these tests encode (docs/remediation/OWNER_DECISIONS_LOG.md,
answered 2026-10-16): D-RC1 A (the scanner is the one retirement writer; the
pipeline and lifecycle reconcile only report), D-RC2 (two-strike, the second
miss at least 20 h after the first), D-N3 (a strong stamped tier wins the VIN),
D-N4 (cars of a rooftop missing from the roster are held, never filed under a
sister store).
"""
from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from backend.scanner import database as sdb
from backend.tests.pipeline_patch import patch_pipeline

FIX = Path(__file__).resolve().parent / "fixtures" / "cleanroom_shapes"


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


# ── shared helpers ───────────────────────────────────────────────────────────

@pytest.fixture()
def inv(sqlite_inventory, tmp_path, monkeypatch) -> Path:
    """An isolated SQLite inventory plus the scanner env the clean room ran with
    (workspace/cleanroom_2026_10/cleanroom.env: SCANNER_AUTO_HEAL=0 and no
    reconcile, coverage or guard-window override, so reconcile is on with
    min coverage 0.5, min rows 8 and the 48 h guard window)."""
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.setenv("SCANNER_RECONCILE", "1")
    monkeypatch.setenv("SCANNER_AUTO_HEAL", "0")
    for name in ("SCANNER_RECONCILE_MIN_COVERAGE", "SCANNER_RECONCILE_MIN_ROWS", "SCANNER_VIN_OWNER_GUARD_HOURS"):
        monkeypatch.setenv(name, "")
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(tmp_path / "dealer_logs"))
    return Path(sqlite_inventory.path)


_ISO = re.compile(r"^(\d{4}-\d{2}-\d{2})([T ])(\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$")


def _shift_iso(value: Any, delta: timedelta) -> Any:
    """``value`` moved back by ``delta`` in the same text shape (separator,
    fraction digits, zone suffix), so string comparisons keep working."""
    if not isinstance(value, str):
        return value
    m = _ISO.match(value.strip())
    if not m:
        return value
    day, sep, clock, frac, zone = m.groups()
    dt = datetime.fromisoformat(f"{day}T{clock}{frac or ''}") - delta
    out = dt.strftime("%Y-%m-%d") + sep + dt.strftime("%H:%M:%S")
    if frac:
        out += "." + f"{dt.microsecond:06d}"[: len(frac) - 1].ljust(len(frac) - 1, "0")
    return out + (zone or "")


def _age_db(db: Path, hours: float) -> None:
    """Pretend everything stored so far happened ``hours`` earlier: every
    ISO-8601 text timestamp in every table moves back (cars.scraped_at,
    first_seen_at, listing_removed_at, scan_runs, conflict sightings and any
    strike or retirement table a later phase adds). This is how the tests put a
    real gap between two scans of one store; N9.14 step 3 does the same by hand
    on the strike rows of a scratch DB."""
    delta = timedelta(hours=hours)
    conn = sqlite3.connect(str(db))
    try:
        conn.create_function("cr_age", 1, lambda v: _shift_iso(v, delta), deterministic=True)
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        for t in tables:
            for col in [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')]:
                conn.execute(
                    f'UPDATE "{t}" SET "{col}" = cr_age("{col}") '
                    f"WHERE typeof(\"{col}\") = 'text' AND \"{col}\" GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][T ][0-9][0-9]:*'"
                )
        conn.commit()
    finally:
        conn.close()


def _hours_between(a: str, b: str) -> float:
    ta = datetime.fromisoformat(a.replace("Z", "+00:00"))
    tb = datetime.fromisoformat(b.replace("Z", "+00:00"))
    return (tb - ta).total_seconds() / 3600.0


def _q(db: Path, sql: str, params: tuple = ()) -> list[tuple]:
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _vins(db: Path, dealer_id: str, *, active: bool) -> set[str]:
    flag = 1 if active else 0
    return {r[0] for r in _q(db, "SELECT vin FROM cars WHERE dealer_id = ? AND COALESCE(listing_active, 1) = ?", (dealer_id, flag))}


def _owner(db: Path, vin: str) -> str | None:
    rows = _q(db, "SELECT dealer_id FROM cars WHERE vin = ? AND COALESCE(listing_active, 1) = 1", (vin,))
    return rows[0][0] if rows else None


def _scan(dealer: dict[str, Any], rows: list[dict[str, Any]] | None = None, *, place: dict[str, str] | None = None,
          intercepts: list[tuple[str, Any]] | None = None):
    """One store's scan through the scanner's own steps after the capture
    (phases/dealer_run.py:94-120): the feed's payloads through the per-page gate
    (``inventory_parser``), the all-rows attribution pass (``settle_attribution``),
    the write with the VIN-owner guard (``persist``) and ``after_write``
    (coverage, rooftop disown, inventory_reconcile). ``rows`` are rows as a
    parser hands them over; ``intercepts`` are raw payloads parsed by
    ``dealer['provider']``. The capture, recovery and VDP steps are skipped (no
    network). A fix that makes these steps read new run evidence (a run start
    time, a walk report) sets it here the way ``run_dealer`` does."""
    from backend.attribution import DealerCtx
    from backend.attribution.place import gate_place
    from backend.scanner.inventory_write import InventoryWriteCoordinator
    from backend.scanner.phases.dealer_run_steps import after_write as aw
    from backend.scanner.phases.dealer_run_steps import attribution as at
    from backend.scanner.phases.dealer_run_steps.persist import persist
    from backend.scanner.phases.dealer_run_steps.state import start_run

    async def _go():
        run = start_run(dict(dealer), InventoryWriteCoordinator())
        run.attr_ctx = DealerCtx(run.dealer_id, run.name, run.url, gate_place(place or {}))
        run.roster_place = run.attr_ctx.place
        feed = [dict(r) for r in (rows or [])]
        if intercepts:
            run.intercept_records = list(intercepts)
            feed += at.parse_intercepts(run, at.inventory_parser(run))
        at.settle_attribution(run, feed + list(run.rooftop_refused))
        run.result["deduped_rows"] = len({(v.get("vin") or "").strip().upper() for v in run.all_vehicles})
        await persist(run)
        await aw.after_write(run)
        return run

    return asyncio.run(_go())


# ── 1-4. in-run retirements by the scanner reconcile on a lifecycle retry ────

_RET = _load("inrun_retirements_20261014.json")
_MAKE = {"terrylabontechevy-com": ("Chevrolet", "TL"), "coastlinecdjr-com": ("Ram", "CL"),
         "napletonhondamortongrove-com": ("Honda", "NH"), "southcountylexus-com": ("Lexus", "SC")}
_COND_CODE = {"New": "N", "Used": "U", "Certified": "C", "": "X"}


def _row(dealer_id: str, vin: str, condition: str) -> dict[str, Any]:
    d = _RET["dealers"][dealer_id]
    make = _MAKE[dealer_id][0]
    return {"vin": vin, "condition": condition, "year": 2024, "make": make, "model": "Fixture",
            "title": f"2024 {make} Fixture", "price": 30000.0, "dealer_id": dealer_id, "dealer_name": d["name"],
            "dealer_url": d["url"]}


def _first_pass_rows(dealer_id: str) -> list[dict[str, Any]]:
    return [_row(dealer_id, r["vin"], r["condition"]) for r in _RET["dealers"][dealer_id]["first_pass"]]


def _retry_rows(dealer_id: str) -> list[dict[str, Any]]:
    """What the store-scoped retry recipe returned: the first-pass rows it
    re-saw plus the rows it found first (VINs synthesized per condition,
    counts from cars_cleanroom)."""
    d = _RET["dealers"][dealer_id]
    out = [_row(dealer_id, r["vin"], r["condition"]) for r in d["first_pass"] if r["re_seen_by_retry"]]
    code = _MAKE[dealer_id][1]
    for cond, n in sorted(d["retry_new_rows"].items()):
        out += [_row(dealer_id, f"1RT{code}{_COND_CODE[cond]}{i:011d}", cond) for i in range(n)]
    return out


def _dealer(dealer_id: str) -> dict[str, Any]:
    d = _RET["dealers"][dealer_id]
    return {"dealer_id": dealer_id, "name": d["name"], "url": d["url"], "provider": d["provider"]}


_TERRY_REASON = (
    "defect: inventory_reconcile.py:242 measures coverage AFTER the retry's upsert, so a store-scoped lifecycle "
    "retry counts its own 585 rows (used 134/150, lot 585/601) and retires the 16 first-pass rows it never "
    "re-saw (0/16 prior rows matched; 11 of them still listed in Hendrick's pool at Terry's address); fixed by "
    "N9 P5A.1/P5A.4/P5A.5 (prior-window coverage by VIN with matched > 0, run_started_iso) and P5B.4 two-strike"
)
_STRIKE_REASON = (
    "defect: inventory_reconcile.py:323-330 retires on the first miss, inside the run that wrote the rows "
    "(no two-strike, D-RC2); fixed by N9 P5B.4 (listing_miss_strikes, second miss >= 20 h later) on P5A.1/P5A.5"
)


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
    """First pass (10-13) wrote rows from the old recipe; the lifecycle retry
    (10-14, about 22 h later) replayed a re-synthesized recipe of another scope.

    Correct (N9.14 acceptance): the retry retires nothing in-run (Terry: no
    first-pass row was re-seen, so nothing is even struck; Coastline 8/11,
    Napleton 16/17, South County Lexus 14/15 strike 3/1/1); a second pass with
    the same recipe at least 20 h later retires exactly those 3/1/1 unseen
    first-pass VINs, and still none of Terry's."""
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

    # the next pass, at least 20 h later, with the same recipe
    _age_db(inv, 21)
    _scan(dealer, retry)
    expected = set() if dealer_id == "terrylabontechevy-com" else unseen
    assert _vins(inv, dealer_id, active=False) == expected, (
        f"{dealer_id}: second miss >= 20 h later should retire exactly {sorted(expected)}"
    )


# ── 5. the pipeline's lifecycle retry reconcile with a known=0 baseline ──────

_LIFECYCLE_REASON = (
    "defect: the lifecycle retry reconcile (pipeline/lifecycle.py:331-338 -> pipeline/reconcile.py:73-78, "
    ":152-155) gets known_before=0 for a dealer that was empty before the run, falls back to the first pass's "
    "rows as its baseline (used 16; 134 >= 0.6 x 16) and retires them: a second writer on the scanner's "
    "decision; fixed by N9 P5A.7 (pipeline and lifecycle reconcile report only, D-RC1 A)"
)


@pytest.fixture()
def log_root(tmp_path, monkeypatch) -> Path:
    root = tmp_path / "dealer_logs"
    patch_pipeline(monkeypatch, "LOG_ROOT", root)
    monkeypatch.setenv("DEALER_LOGS_ROOT", str(root))
    return root


@pytest.mark.xfail(strict=True, reason=_LIFECYCLE_REASON)
def test_lifecycle_retry_with_known_zero_baseline_retires_nothing(inv, log_root, tmp_path, monkeypatch):
    """Terry's run, pipeline side: the dealer had no rows when the pipeline
    started (known 0, empty buckets), the first pass wrote 16 used rows and
    got verdict no_rows, the lifecycle re-synthesized and the retry wrote 585
    rows (451 new, 134 used). In the clean room the scanner had already retired
    the 16, so this writer saw stale=0; had it not, this writer retires the same
    16 (retirements_rca.txt section 3). Correct: the pipeline writes no
    retirement (D-RC1 A); the scanner is the one writer."""
    from backend.db.repositories.dealers_repo import record_scan_outcomes
    from backend.scripts import dealer_pipeline as dp

    did = "terrylabontechevy-com"
    d = _RET["dealers"][did]
    first = {r["vin"] for r in d["first_pass"]}
    sdb.upsert_vehicles(_first_pass_rows(did))
    _age_db(inv, _hours_between(d["first_pass_at"], d["retry_at"]))

    def _retry_scan(dealer_ids, **kw):
        # the retry scan's write (the real upsert) and its scan_runs row (the real
        # writer); the scanner's own reconcile is not part of this shape
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


# ── 6. VIN-owner guard: the owner depends on which store wrote first ─────────

_CHAP = _load("chapman_scan_order_20261013.json")
_GUARD_REASON = (
    "defect: first-writer-wins VIN-owner guard (scanner/database.py:83-101, upsert/guard.py:36-67, backstop "
    "upsert/sql.py:150-155) compares no evidence, so the store scanned first keeps a car whose own store's "
    "feed names it (clean room: chapmanfordaz-com wrote first and owns 175 CAE Chapman Dodge cars); fixed by "
    "N8.6 (V029 persisted rooftop tier, evidence-aware guard and backstop, D-N3)"
)


def _wipe(db: Path) -> None:
    conn = sqlite3.connect(str(db))
    try:
        for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall():
            conn.execute(f'DELETE FROM "{t}"')
        conn.commit()
    finally:
        conn.close()


def _chapman_scans() -> dict[str, tuple[dict[str, Any], list[dict[str, Any]], dict[str, str]]]:
    out = {}
    for did, ro in _CHAP["roster"].items():
        rows = []
        for car in _CHAP["cars"]:
            row = {"vin": car["vin"], "condition": car["condition"], "year": car["year"], "make": car["make"],
                   "model": car["model"], "title": f"{car['year']} {car['make']} {car['model']}", "price": car["price"],
                   "stock_number": car["stock_number"], "image_url": car["image"], "gallery": [car["image"]],
                   "dealer_id": did, "dealer_name": ro["name"], "dealer_url": ro["url"]}
            if did == "chapmanfordaz-com":
                row["source_url"] = car["owner_vdp"]  # the Chapman feed: no store field on the card
            else:
                row["_rooftop"] = {"key": "chapmandodge-account", "name": ro["name"], "site": ro["url"],
                                   "address": ro["street"], "city": ro["city"], "state": ro["state"], "zip": ro["zip"]}
            rows.append(row)
        place = {"dealer_address": ro["street"], "dealer_city": ro["city"], "dealer_state": ro["state"],
                 "dealer_zip": ro["zip"], "dealer_address_source": "site_jsonld"}
        dealer = {"dealer_id": did, "name": ro["name"], "url": ro["url"], "provider": ro["provider"]}
        out[did] = (dealer, rows, place)
    return out


@pytest.mark.xfail(strict=True, reason=_GUARD_REASON)
def test_guard_scan_order_gives_the_same_owner(inv):
    """Two stores' feeds carry the same Chapman Dodge cars (lot code CAE in the
    photo path): Chapman Ford's feed names no store, Chapman Dodge's own feed
    stamps itself. Correct (D-N3; N8.6 acceptance "two scan orders give the
    same owner"): both orders file every car under chapmandodge-com."""
    scans = _chapman_scans()
    vins = [c["vin"] for c in _CHAP["cars"]]
    owners: dict[str, dict[str, str | None]] = {}
    for label, order in (("ford_first", ("chapmanfordaz-com", "chapmandodge-com")),
                         ("dodge_first", ("chapmandodge-com", "chapmanfordaz-com"))):
        _wipe(inv)
        for did in order:
            dealer, rows, place = scans[did]
            _scan(dealer, rows, place=place)
        owners[label] = {v: _owner(inv, v) for v in vins}
    differ = {v: (owners["ford_first"][v], owners["dodge_first"][v]) for v in vins
              if owners["ford_first"][v] != owners["dodge_first"][v]}
    assert not differ, f"owner depends on scan order (ford_first, dodge_first): {differ}"
    assert set(owners["ford_first"].values()) == {"chapmandodge-com"}, owners["ford_first"]


# ── 7. an unstamped group feed files sister and off-roster cars under the store

_FIVE = _load("fivestar_jazel_group_srp_20261013.json")
_FEED_REASON = (
    "defect: the Jazel parser drops each card's accountId / accountName / accountAddress (parsers/jazel.py:156-194 "
    "_map; :183 even prefers the manifest name), so no row carries a rooftop and the gate keeps the whole Sam Pack "
    "group SRP (match.py:338-342 no_rooftop_evidence): 124 cars of other rooftops filed under 5starford-com, and "
    "the guard then refuses Dallas's own cars to fivestarforddallas-com; fixed by N8.3 (Jazel account -> _rooftop) "
    "with N8.6 (D-N4: off-roster rooftops held; the plan records the flip under N8.6)"
)


def _jazel_srp(cards: list[dict[str, Any]]) -> str:
    """A Jazel SSR SRP page: one ``jzlSetVehicleInfoContext`` call per card, the
    shape parsers/jazel.py reads, with the card's account (accountId /
    accountName / accountAddress) where the fixture knows it."""
    parts = ["<html><body>"]
    for c in cards:
        doc: dict[str, Any] = {
            "vin": c["vin"], "year": c["year"], "make": c["make"], "model": c["model"], "trim": c["trim"],
            "displayPrice": int(c["price"] or 0), "mileage": c["mileage"] or 0,
            "newOrUsed": "new" if c["condition"] == "New" else "used", "isCertified": c["condition"] == "Certified",
            "isUsed": c["condition"] != "New", "stockNumber": c["stock_number"], "vdpLink": c["vdp"],
        }
        if c["accountId"]:
            acct = _FIVE["accounts"][c["accountId"]]
            doc.update(accountId=c["accountId"], accountName=acct["accountName"], accountAddress=acct["accountAddress"])
        parts.append(f"<script>window.jzlSetVehicleInfoContext('{c['vin']}', {json.dumps(doc)});</script>")
    parts.append("</body></html>")
    return "\n".join(parts)


@pytest.mark.xfail(strict=True, reason=_FEED_REASON)
def test_unstamped_group_feed_files_no_sister_or_offroster_cars_under_the_store(inv):
    """5starford-com (Five Star Ford North Richland Hills) replays the group-wide
    /inventory/all-vehicles/ SRP: 934 cards, 810 at North Richland Hills, 59
    Carrollton, 31 Dallas, 21 Lewisville, 13 Grapevine (VDP city slugs in
    cars_cleanroom). Every card names its store (Jazel accountId 7 North
    Richland Hills, 8 Dallas, 6 Ford Carrollton, 108 Chevrolet Carrollton, 10
    Lewisville, 190 Subaru Grapevine: N1.2's jazel field map). Dallas is a
    rostered sister; accounts 6, 10, 108 and 190 are rooftops missing from the
    roster. Correct (D-N4): only the 810 are filed under 5starford-com,
    Dallas's own cars land under fivestarforddallas-com when it scans, and the
    off-roster rooftops' cars are held, never filed under either sister.
    ``test_single_store_feed_is_untouched`` (no rooftop evidence keeps every
    row) stays as it is: the fix adds evidence, it does not flip that default."""
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


# ── 8. triage accounting: the identity replayed from the 10-13 triage ────────

_IDENT = _load("triage_identity_20261013.json")
_IDENTITY_REASON = (
    "defect: assess.py:104-108 reports rows = capture_coverage.n, taken at persist.py:18 before the VIN-owner "
    "guard drops refused VINs (persist.py:25-35), there is no rows_written, and a dealer written without a "
    "scan_runs row (pause-killed batch) reports nothing: triage 'rows' sums to 208,233 against 199,457 active; "
    "fixed by N9.12 (rows_captured / rows_refused_vin_owner / rows_refused_rooftop / rows_written, error "
    "dealers counted, not null)"
)


def _seed_identity(db: Path) -> dict[str, int]:
    """cars and scan_runs as the clean room left them, per dealer: active and
    inactive rows by condition (synthetic VINs, the dealer's last scrape time)
    and the triage row's scan_runs row through the real writer."""
    from backend.db.repositories.dealers_repo import record_scan_outcomes

    cols = _IDENT["columns"]
    conn = sqlite3.connect(str(db))
    active: dict[str, int] = {}
    try:
        have = {r[1] for r in conn.execute("PRAGMA table_info(cars)")}
        fields = [f for f in ("dealer_id", "vin", "condition", "scraped_at", "first_seen_at", "listing_active",
                              "listing_removed_at", "dealer_name") if f in have]
        sql = f"INSERT INTO cars ({', '.join(fields)}) VALUES ({', '.join('?' * len(fields))})"
        for i, rec in enumerate(_IDENT["dealers"]):
            row = dict(zip(cols, rec))
            did, cars = row["dealer_id"], row["cars"] or {"active": {}, "inactive": {}}
            active[did] = sum(cars["active"].values())
            batch = []
            for state, flag in (("active", 1), ("inactive", 0)):
                ts = cars.get(f"{state}_scraped_at")
                for cond, n in sorted(cars[state].items()):
                    for j in range(n):
                        vals = {"dealer_id": did, "vin": f"{i:04d}{flag}{_COND_CODE.get(cond, 'X')}{j:011d}",
                                "condition": cond or None, "scraped_at": ts, "first_seen_at": ts, "listing_active": flag,
                                "listing_removed_at": None if flag else ts, "dealer_name": did}
                        batch.append(tuple(vals[f] for f in fields))
            conn.executemany(sql, batch)
        conn.execute("CREATE INDEX IF NOT EXISTS cr_shapes_cars_dealer ON cars(dealer_id)")
        conn.commit()
    finally:
        conn.close()
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
    """Replay of the 458-dealer 10-13 triage on the clean room's end state.
    The DB started empty, so every active row was written by its dealer's
    latest run: per dealer, rows_written must equal its active rows (righthonda
    553 and selmanchevy 281 were written by batches the pause killed before
    their scan_runs row). Fleet: captured 208,233 - refused 9,610 = written
    198,623 with a scan_run, + 834 without = 199,457 active (+ 21 inactive)."""
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
