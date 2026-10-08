"""P1C.3: backfill ``saved_at`` on ``dealer_recipes`` entries stamped 0 or null.

Before P1B.3, synthesized sets were saved with ``saved_at=0``, so any older cache file
with a non-zero stamp beat the DB set in ``load_recipes`` and pushed its old set back
up (normreeves-com: a July file over the September DB set). The backfill stamps those
entries with the row's max ``last_ok_at`` (fallback ``updated_at``), writes through a
guarded UPDATE, skips a dealer whose cache file holds a newer success
(scottclarkhonda-com), exports before it writes, and can restore.

Every test runs on a per-test SQLite store from ``recipe_store_harness`` with host
caches under ``tmp_path``; nothing touches the session DB or the real workspace.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from contextlib import closing
from dataclasses import asdict
from datetime import datetime

import pytest

import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner.recipes import PAGINATION_CARSCOMMERCE, EndpointRecipe
from backend.scripts import backfill_recipe_saved_at as bf
from backend.tests.recipe_store_harness import TwoHostRecipeStore

SEPT = 1_790_396_620.14146      # 2026-09-28, the synthesized sets' validation
SEPT_29 = 1_790_685_861.884228  # 2026-09-29, scottclarkhonda's cache success
AUG = 1_785_947_113.131532      # 2026-08-05
JULY = 1_783_272_811.0402281    # 2026-07
UPD_SEPT = "2026-09-28T21:25:26.854579+00:00"
UPD_AUG6 = "2026-08-06T01:03:41.312590+00:00"
SECRET = "SECRET-AUTH-TOKEN-0123456789"

CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/6036528/search"


@pytest.fixture
def store(tmp_path, monkeypatch):
    s = TwoHostRecipeStore(tmp_path, monkeypatch)
    s.dirs["mbp"].mkdir(parents=True, exist_ok=True)
    return s


def _entry(dealer_id: str, url: str, *, saved_at=0.0, last_ok_at=0.0, method="GET",
           post_template=None, **kw) -> dict:
    row = asdict(EndpointRecipe(
        dealer_id=dealer_id, url=url, method=method, content_type="application/json",
        post_template=post_template, provider_hint=kw.pop("provider_hint", "team_velocity"),
        vehicle_rows=40, **kw,
    ))
    row["saved_at"] = saved_at
    row["last_ok_at"] = last_ok_at
    return row


def _template(length: int, fill: str) -> str:
    """A carscommerce POST body (valid JSON) exactly ``length`` chars long."""
    empty = json.dumps({"page": 1, "perPage": 20, "q": ""})
    return json.dumps({"page": 1, "perPage": 20, "q": fill * (length - len(empty))})


def _ensure_table(store: TwoHostRecipeStore) -> None:
    conn = recipe_store._conn()
    try:
        recipe_store._ensure_table(conn)
    finally:
        conn.close()


def put_row(store: TwoHostRecipeStore, dealer_id: str, entries, *, updated_at=UPD_SEPT,
            scan_hints: dict | None = None) -> None:
    """Write a dealer_recipes row verbatim (columns derived as recipe_store does)."""
    _ensure_table(store)
    if isinstance(entries, list):
        payload = json.dumps(entries)
        meta = recipe_store._derive_meta(entries)
    else:  # an unreadable payload, stored as given
        payload = entries
        meta = {"recipe_count": 1, "provider_hint": None, "max_saved_at": 0.0,
                "last_ok_at": 0.0, "stale_count": 0}
    with closing(sqlite3.connect(store.db_path)) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO dealer_recipes (dealer_id, recipes_json, recipe_count, provider_hint, "
            "max_saved_at, last_ok_at, stale_count, updated_at, scan_hints) VALUES (?,?,?,?,?,?,?,?,?)",
            (dealer_id, payload, meta["recipe_count"], meta["provider_hint"], meta["max_saved_at"],
             meta["last_ok_at"], meta["stale_count"], updated_at,
             json.dumps(scan_hints) if scan_hints is not None else None),
        )
        conn.commit()


def snapshot(store: TwoHostRecipeStore) -> dict[str, tuple]:
    """Every row, every column; recipes_json as its md5."""
    with closing(sqlite3.connect(store.db_path)) as conn:
        rows = conn.execute(
            "SELECT dealer_id, recipes_json, recipe_count, provider_hint, max_saved_at, last_ok_at, "
            "stale_count, updated_at, scan_hints FROM dealer_recipes ORDER BY dealer_id"
        ).fetchall()
    return {r[0]: (hashlib.md5(r[1].encode()).hexdigest(),) + tuple(r[2:]) for r in rows}


def entries_of(store: TwoHostRecipeStore, dealer_id: str) -> list[dict]:
    return store.db_row(dealer_id)["rows"]


def run(capsys, *argv: str) -> tuple[int, str]:
    rc = bf.main(list(argv))
    out = capsys.readouterr().out
    return rc, out


def _epoch(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


def seed_fleet(store: TwoHostRecipeStore) -> None:
    """A synthesized set, a stamped set, a hint-only row and the scottclarkhonda shape."""
    put_row(store, "synth-dealer-com", [
        _entry("synth-dealer-com", "https://www.synth-dealer.com/inventory-new.json", last_ok_at=SEPT,
               auth_headers={"x-api-key": SECRET}),
        _entry("synth-dealer-com", "https://www.synth-dealer.com/inventory-used.json", last_ok_at=SEPT - 120),
    ], scan_hints={"notes": "keep me"})
    put_row(store, "stamped-dealer-com", [
        _entry("stamped-dealer-com", "https://www.stamped-dealer.com/inv.json", saved_at=AUG, last_ok_at=AUG),
    ])
    put_row(store, "hints-only-com", [], scan_hints={"requires_browser": True})
    scott_db = [
        _entry("scott-shape-com", "https://www.scott-shape.com/inventory-used.json"),
        _entry("scott-shape-com", "https://www.scott-shape.com/inventory-new.json"),
    ]
    put_row(store, "scott-shape-com", scott_db, updated_at=UPD_AUG6)
    scott_cache = [dict(e) for e in scott_db]
    scott_cache[0]["last_ok_at"] = SEPT_29
    scott_cache[1]["last_ok_at"] = SEPT_29 + 2
    store.write_file("mbp", "scott-shape-com", scott_cache)


# ── dry run ────────────────────────────────────────────────────────────────────


def test_dry_run_writes_nothing(store, capsys, tmp_path):
    seed_fleet(store)
    before = snapshot(store)
    files_before = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*") if p.is_file())
    mtimes = {p: p.stat().st_mtime_ns for p in tmp_path.rglob("*") if p.is_file() and p.suffix == ".json"}

    rc, out = run(capsys, "--cache-dir", str(store.dirs["mbp"]))

    assert rc == 0
    assert "DRY RUN (nothing written)" in out
    assert "rows with saved_at 0/null entries: 2 (plus 0 unreadable row(s), listed below)" in out
    assert "to backfill: 1 row(s), 2 entries; stamp from last_ok_at: 1, from updated_at: 0" in out
    assert "ids: synth-dealer-com" in out
    assert "cache_newer_ok (cache has a newer success for a key; left to P1C.4): 1: scott-shape-com" in out
    assert snapshot(store) == before  # md5 of every recipes_json, every column
    # recipes_json md5 explicitly, as the plan states it
    assert before["synth-dealer-com"][0] == hashlib.md5(
        store.db_row("synth-dealer-com")["recipes_json"].encode()).hexdigest()
    files_after = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*") if p.is_file())
    assert files_after == files_before
    assert {p: p.stat().st_mtime_ns for p in mtimes} == mtimes


def test_dry_run_is_the_default_and_explicit_flag_matches(store, capsys):
    seed_fleet(store)
    _, out_default = run(capsys, "--cache-dir", str(store.dirs["mbp"]))
    _, out_flag = run(capsys, "--dry-run", "--cache-dir", str(store.dirs["mbp"]))
    assert out_default == out_flag


def test_apply_requires_backup_dir(store, capsys):
    seed_fleet(store)
    before = snapshot(store)
    with pytest.raises(SystemExit) as exc:
        bf.main(["--apply", "--cache-dir", str(store.dirs["mbp"])])
    assert exc.value.code == 2
    assert "--apply needs --backup-dir" in capsys.readouterr().err
    assert snapshot(store) == before


# ── apply ──────────────────────────────────────────────────────────────────────


def test_apply_sets_saved_at_from_last_ok_at(store, capsys, tmp_path):
    seed_fleet(store)
    mixed = [
        _entry("mixed-dealer-com", "https://www.mixed-dealer.com/a.json", saved_at=JULY, last_ok_at=JULY),
        _entry("mixed-dealer-com", "https://www.mixed-dealer.com/b.json", last_ok_at=AUG),
    ]
    put_row(store, "mixed-dealer-com", mixed)
    # No success anywhere: the stamp falls back to updated_at (scottclarkstoyota-com's shape).
    put_row(store, "nook-dealer-com", [
        _entry("nook-dealer-com", "https://www.nook-dealer.com/inventory-new.json"),
    ], updated_at=UPD_AUG6)
    before = snapshot(store)
    synth_before = entries_of(store, "synth-dealer-com")
    backup = tmp_path / "backups" / "saved_at_backfill"

    rc, out = run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))

    assert rc == 0, out
    assert "applied: 3" in out
    synth = entries_of(store, "synth-dealer-com")
    assert [e["saved_at"] for e in synth] == [SEPT, SEPT]  # one stamp per row: the row's max last_ok_at
    for old, new in zip(synth_before, synth):  # nothing but saved_at changed in the entries
        assert {k: v for k, v in old.items() if k != "saved_at"} == {k: v for k, v in new.items() if k != "saved_at"}
    row = store.db_row("synth-dealer-com")
    assert row["max_saved_at"] == SEPT
    assert row["scan_hints"] == json.dumps({"notes": "keep me"})  # never touched
    old = before["synth-dealer-com"]
    assert (row["recipe_count"], row["provider_hint"], row["last_ok_at"], row["stale_count"]) == (
        old[1], old[2], old[4], old[5])
    assert row["updated_at"] != UPD_SEPT  # bumped, like every dealer_recipes write

    mixed_after = entries_of(store, "mixed-dealer-com")
    assert [e["saved_at"] for e in mixed_after] == [JULY, AUG]  # only the 0 entry is stamped
    assert store.db_row("mixed-dealer-com")["max_saved_at"] == AUG

    nook = store.db_row("nook-dealer-com")
    assert nook["rows"][0]["saved_at"] == pytest.approx(_epoch(UPD_AUG6))
    assert nook["max_saved_at"] == pytest.approx(_epoch(UPD_AUG6))

    after = snapshot(store)
    for untouched in ("stamped-dealer-com", "hints-only-com", "scott-shape-com"):
        assert after[untouched] == before[untouched]

    exports = list(backup.glob("dealer_recipes_saved_at_backfill_*.json"))
    assert len(exports) == 1
    assert stat.S_IMODE(exports[0].stat().st_mode) == 0o600
    export = json.loads(exports[0].read_text())
    assert export["tool"] == "backfill_recipe_saved_at"
    assert {r["dealer_id"] for r in export["rows"]} == {"synth-dealer-com", "mixed-dealer-com", "nook-dealer-com"}
    by_id = {r["dealer_id"]: r for r in export["rows"]}
    assert hashlib.md5(by_id["synth-dealer-com"]["before"]["recipes_json"].encode()).hexdigest() == old[0]
    assert by_id["synth-dealer-com"]["before"]["updated_at"] == UPD_SEPT
    assert by_id["nook-dealer-com"]["stamp_source"] == "updated_at"
    assert by_id["synth-dealer-com"]["effective_change"] is False  # no cache file on this host

    # max_saved_at=0 with recipes: only the listed skip is left.
    with closing(sqlite3.connect(store.db_path)) as conn:
        left = conn.execute(
            "SELECT dealer_id FROM dealer_recipes WHERE max_saved_at=0 AND recipe_count>0").fetchall()
    assert left == [("scott-shape-com",)]
    assert "rows with max_saved_at=0 and recipes after the run: 1" in out

    # Idempotent: a second run finds nothing to stamp and makes no export.
    again = snapshot(store)
    rc, out = run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0
    assert "to backfill: 0 row(s)" in out
    assert "nothing to write; no export made" in out
    assert snapshot(store) == again
    assert len(list(backup.glob("*.json"))) == 1


@pytest.mark.parametrize("writer", ["scan_hints", "recipe_save"])
def test_changed_row_is_skipped(store, capsys, tmp_path, writer):
    seed_fleet(store)
    conn = recipe_store._conn()
    try:
        plan = bf.plan_backfill(conn, cache_dir=store.dirs["mbp"])
    finally:
        conn.close()
    assert [c.dealer_id for c in plan.candidates] == ["synth-dealer-com"]

    # Another writer lands between the read and the write.
    if writer == "scan_hints":
        assert recipe_store.set_scan_hints("synth-dealer-com", {"price_source": "vdp"})
        expected_rows = entries_of(store, "synth-dealer-com")  # unchanged set, hints moved
    else:
        expected_rows = [_entry("synth-dealer-com", "https://www.synth-dealer.com/fresh.json",
                                saved_at=SEPT + 999, last_ok_at=SEPT + 999)]
        assert recipe_store.db_save_recipes("synth-dealer-com", expected_rows)
    mid = snapshot(store)

    conn = recipe_store._conn()
    try:
        res = bf.apply_plan(conn, plan, backup_dir=tmp_path / "bk", store="sqlite:test")
    finally:
        conn.close()

    assert res.applied == []
    assert res.changed == ["synth-dealer-com"]
    assert snapshot(store) == mid
    assert entries_of(store, "synth-dealer-com") == expected_rows
    assert res.export_path.exists()  # the export still came first


# ── the cache check ────────────────────────────────────────────────────────────


def test_scottclarkhonda_shape_is_skipped(store, capsys, tmp_path):
    seed_fleet(store)
    before = snapshot(store)
    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0
    assert "cache_newer_ok" in out and "scott-shape-com" in out
    assert snapshot(store)["scott-shape-com"] == before["scott-shape-com"]
    assert [e["saved_at"] for e in entries_of(store, "scott-shape-com")] == [0.0, 0.0]
    export = json.loads(next((tmp_path / "bk").glob("*.json")).read_text())
    assert "scott-shape-com" not in {r["dealer_id"] for r in export["rows"]}


def test_cache_only_key_counts_only_when_newer_than_the_db_success(store, capsys):
    db = [_entry("resynth-com", "https://www.resynth-com.com/new-endpoint.json", last_ok_at=SEPT)]
    put_row(store, "resynth-com", db)
    # An endpoint the re-synthesized set replaced: its last success predates the DB's.
    store.write_file("mbp", "resynth-com", [
        _entry("resynth-com", "https://www.resynth-com.com/old-endpoint.json", saved_at=JULY, last_ok_at=AUG),
    ])
    put_row(store, "fresh-key-com", [
        _entry("fresh-key-com", "https://www.fresh-key.com/a.json", last_ok_at=AUG),
    ])
    # A key the DB set lacks, with a success newer than anything the DB set has.
    store.write_file("mbp", "fresh-key-com", [
        _entry("fresh-key-com", "https://www.fresh-key.com/b.json", saved_at=JULY, last_ok_at=SEPT),
    ])
    conn = recipe_store._conn()
    try:
        plan = bf.plan_backfill(conn, cache_dir=store.dirs["mbp"])
    finally:
        conn.close()
    assert [c.dealer_id for c in plan.candidates] == ["resynth-com"]
    assert plan.candidates[0].effective_change is True
    assert plan.skipped == {bf.SKIP_CACHE_NEWER: ["fresh-key-com"]}


def test_unreadable_cache_file_is_skipped(store, capsys):
    seed_fleet(store)
    (store.dirs["mbp"] / "synth-dealer-com.json").write_text("{broken", encoding="utf-8")
    _, out = run(capsys, "--cache-dir", str(store.dirs["mbp"]))
    assert "cache_unreadable (cache file or its last_ok_at unreadable; left to P1C.4): 1: synth-dealer-com" in out


def test_non_numeric_cache_last_ok_at_is_cache_unreadable(store, capsys):
    """A cache success that cannot be read cannot be shown older than the DB's: the
    dealer is skipped (fail closed), never stamped."""
    seed_fleet(store)
    cache = entries_of(store, "synth-dealer-com")
    cache[0]["last_ok_at"] = "yesterday"
    store.write_file("mbp", "synth-dealer-com", cache)
    conn = recipe_store._conn()
    try:
        plan = bf.plan_backfill(conn, cache_dir=store.dirs["mbp"])
    finally:
        conn.close()
    assert plan.candidates == []
    assert plan.skipped == {bf.SKIP_CACHE_NEWER: ["scott-shape-com"],
                            bf.SKIP_CACHE_UNREADABLE: ["synth-dealer-com"]}


def test_no_cache_check_backfills_the_scott_shape(store, capsys, tmp_path):
    seed_fleet(store)
    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"), "--no-cache-check")
    assert rc == 0
    assert "cache check: off (--no-cache-check)" in out
    scott = store.db_row("scott-shape-com")
    assert [e["saved_at"] for e in scott["rows"]] == [pytest.approx(_epoch(UPD_AUG6))] * 2


def test_normreeves_shape_host_adopts_the_september_db_set(store, capsys, tmp_path):
    """The September synthesized set (1,141-char template, saved_at 0) against a July
    cache file (827 chars): after the backfill the host adopts the DB set and the July
    file is not pushed up."""
    sept_template, july_template = _template(1141, "s"), _template(827, "j")
    assert (len(sept_template), len(july_template)) == (1141, 827)
    common = dict(method="POST", provider_hint="dealer_dot_com", pagination=PAGINATION_CARSCOMMERCE)
    db_set = [_entry("norm-shape-com", CC_URL, post_template=sept_template, last_ok_at=SEPT, **common)]
    put_row(store, "norm-shape-com", db_set)
    store.write_file("mbp", "norm-shape-com", [
        _entry("norm-shape-com", CC_URL, post_template=july_template, saved_at=JULY, last_ok_at=AUG, **common),
    ])

    _, out = run(capsys, "--cache-dir", str(store.dirs["mbp"]))
    assert "effective set changes on this host" in out and "norm-shape-com" in out

    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0 and "applied: 1" in out

    loaded = rec.load_recipes("norm-shape-com")  # the mbp host, as the scanner loads it
    assert [len(r.post_template or "") for r in loaded] == [1141]
    db_rows = entries_of(store, "norm-shape-com")
    assert [len(r["post_template"]) for r in db_rows] == [1141]  # the July file was not pushed up
    assert db_rows[0]["saved_at"] == SEPT
    assert [len(r["post_template"]) for r in store.file_rows("mbp", "norm-shape-com")] == [1141]


# ── unreadable rows ────────────────────────────────────────────────────────────


def test_unreadable_and_unstampable_rows_are_listed_and_left(store, capsys, tmp_path):
    put_row(store, "garbled-com", "not json at all")
    put_row(store, "nostamp-com", [_entry("nostamp-com", "https://www.nostamp.com/a.json")], updated_at=None)
    put_row(store, "ok-com", [_entry("ok-com", "https://www.ok.com/a.json", last_ok_at=SEPT)])
    before = snapshot(store)
    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"), "--no-cache-check")
    assert rc == 0
    assert "unreadable (recipes_json unreadable; left as they are): 1: garbled-com" in out
    assert "rows with saved_at 0/null entries: 2 (plus 1 unreadable row(s), listed below)" in out
    assert "no_stamp (no last_ok_at and no parseable updated_at): 1: nostamp-com" in out
    after = snapshot(store)
    assert after["garbled-com"] == before["garbled-com"]
    assert after["nostamp-com"] == before["nostamp-com"]
    assert entries_of(store, "ok-com")[0]["saved_at"] == SEPT


# ── restore ────────────────────────────────────────────────────────────────────


def test_restore_round_trips(store, capsys, tmp_path):
    seed_fleet(store)
    put_row(store, "second-synth-com", [
        _entry("second-synth-com", "https://www.second-synth.com/a.json", last_ok_at=AUG),
    ])
    original = snapshot(store)
    backup = tmp_path / "bk"
    rc, _ = run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0
    applied = snapshot(store)
    assert applied != original
    export = next(backup.glob("*.json"))

    rc, out = run(capsys, "--restore", str(export))  # a dry run first
    assert rc == 0
    assert "DRY RUN" in out and "restorable (unchanged since the backfill): 2" in out
    assert snapshot(store) == applied

    rc, out = run(capsys, "--restore", str(export), "--apply")
    assert rc == 0
    assert "restored: 2" in out
    assert snapshot(store) == original  # every column, recipes_json md5 included


def test_restore_leaves_rows_written_after_the_backfill(store, capsys, tmp_path):
    seed_fleet(store)
    put_row(store, "second-synth-com", [
        _entry("second-synth-com", "https://www.second-synth.com/a.json", last_ok_at=AUG),
    ])
    original = snapshot(store)
    backup = tmp_path / "bk"
    run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))
    assert recipe_store.set_scan_hints("second-synth-com", {"notes": "written after"})
    later = snapshot(store)["second-synth-com"]

    rc, out = run(capsys, "--restore", str(next(backup.glob("*.json"))), "--apply")
    assert rc == 0
    assert "restored: 1" in out
    assert "changed since the backfill (left as they are): 1: second-synth-com" in out
    final = snapshot(store)
    assert final["synth-dealer-com"] == original["synth-dealer-com"]
    assert final["second-synth-com"] == later


def test_restore_refuses_an_export_from_another_store(store, capsys, tmp_path, monkeypatch):
    seed_fleet(store)
    backup = tmp_path / "bk"
    run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))
    applied = snapshot(store)
    monkeypatch.setattr(rec, "store_identity", lambda: "postgres:prod.example:5432/railway")
    rc, out = run(capsys, "--restore", str(next(backup.glob("*.json"))), "--apply")
    assert rc == 2
    assert "restore refused" in out
    assert snapshot(store) == applied


# ── output hygiene ─────────────────────────────────────────────────────────────


def test_output_never_contains_auth_headers(store, capsys, tmp_path):
    seed_fleet(store)
    backup = tmp_path / "bk"
    outs = [run(capsys, "--cache-dir", str(store.dirs["mbp"]))[1],
            run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))[1]]
    export = next(backup.glob("*.json"))
    outs.append(run(capsys, "--restore", str(export))[1])
    outs.append(run(capsys, "--restore", str(export), "--apply")[1])
    for out in outs:
        assert SECRET not in out
        assert "x-api-key" not in out
    # The export is a backup: it holds the secret, so it is private.
    assert SECRET in export.read_text()
    assert stat.S_IMODE(export.stat().st_mode) == 0o600


def test_refused_when_the_store_is_disabled(store, capsys, monkeypatch):
    seed_fleet(store)
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    rc, out = run(capsys, "--no-cache-check")
    assert rc == 2 and "RECIPES_DB_DISABLED" in out


def test_missing_cache_dir_is_an_error(store, tmp_path):
    with pytest.raises(SystemExit) as exc:
        bf.main(["--cache-dir", str(tmp_path / "nope")])
    assert exc.value.code == 2


# ── the cache check fails closed ───────────────────────────────────────────────


def _no_default_cache(monkeypatch, path) -> None:
    """The scanner's default cache dir is ``path`` (a worktree has no workspace/)."""
    monkeypatch.setattr(rec, "resolve_recipes_dir", lambda *a, **k: path)


def test_apply_refused_when_the_default_cache_dir_is_missing(store, capsys, tmp_path, monkeypatch):
    seed_fleet(store)
    _no_default_cache(monkeypatch, tmp_path / "worktree" / "workspace" / "recipes")
    before = snapshot(store)
    backup = tmp_path / "bk"

    rc, out = run(capsys, "--apply", "--backup-dir", str(backup))

    assert rc == 2, out
    assert "APPLY refused (nothing written, no export made)" in out
    assert "refused: the default cache dir" in out and "does not exist" in out
    assert not backup.exists() or list(backup.iterdir()) == []  # no export
    assert snapshot(store) == before
    assert [e["saved_at"] for e in entries_of(store, "scott-shape-com")] == [0.0, 0.0]  # still unstamped
    assert [e["saved_at"] for e in entries_of(store, "synth-dealer-com")] == [0.0, 0.0]

    # Naming the cache dir runs the check: the scott shape is skipped, the rest is stamped.
    rc, out = run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0, out
    assert "cache_newer_ok" in out and "scott-shape-com" in out
    assert [e["saved_at"] for e in entries_of(store, "scott-shape-com")] == [0.0, 0.0]
    assert [e["saved_at"] for e in entries_of(store, "synth-dealer-com")] == [SEPT, SEPT]


def test_dry_run_warns_when_the_default_cache_dir_is_missing(store, capsys, tmp_path, monkeypatch):
    seed_fleet(store)
    _no_default_cache(monkeypatch, tmp_path / "worktree" / "workspace" / "recipes")
    before = snapshot(store)

    rc, out = run(capsys)

    assert rc == 0, out
    assert "cache check: NOT RUN: no cache dir at" in out
    assert "WARNING: the default cache dir" in out and "did not run" in out
    assert "WARNING: --apply would be refused (1 reason(s) above)" in out
    assert "nothing to check" not in out
    assert snapshot(store) == before


def test_apply_refused_when_the_default_cache_dir_is_empty(store, capsys, tmp_path, monkeypatch):
    seed_fleet(store)
    empty = tmp_path / "worktree" / "workspace" / "recipes"
    empty.mkdir(parents=True)
    _no_default_cache(monkeypatch, empty)
    before = snapshot(store)
    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"))
    assert rc == 2, out
    assert "holds no dealer files" in out
    assert not (tmp_path / "bk").exists()
    assert snapshot(store) == before


def test_apply_refused_on_a_cache_tagged_for_another_store(store, capsys, tmp_path):
    seed_fleet(store)
    rec.write_store_tag("postgres:other-host:5432/other", source="test", cache_dir=store.dirs["mbp"])
    before = snapshot(store)

    rc, out = run(capsys, "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0
    assert "store tag verdict: mismatch" in out
    assert "WARNING: the cache dir" in out and "tagged for another store" in out
    assert "WARNING: --apply would be refused" in out

    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 2, out
    assert "refused: the cache dir" in out
    assert not (tmp_path / "bk").exists()
    assert snapshot(store) == before


def test_apply_and_restore_refused_when_the_store_identity_is_unknown(store, capsys, tmp_path, monkeypatch):
    seed_fleet(store)
    backup = tmp_path / "bk"
    rc, _ = run(capsys, "--apply", "--backup-dir", str(backup), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 0
    export = next(backup.glob("*.json"))
    applied = snapshot(store)
    put_row(store, "late-synth-com", [_entry("late-synth-com", "https://www.late-synth.com/a.json", last_ok_at=AUG)])
    with_late = snapshot(store)

    monkeypatch.setattr(rec, "store_identity", lambda: None)
    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk2"), "--cache-dir", str(store.dirs["mbp"]))
    assert rc == 2, out
    assert "store identity could not be worked out" in out
    assert not (tmp_path / "bk2").exists()
    rc, out = run(capsys, "--restore", str(export), "--apply")
    assert rc == 2 and "restore refused" in out
    assert snapshot(store) == with_late
    assert {k: v for k, v in with_late.items() if k != "late-synth-com"} == applied


def test_apply_lists_every_changed_row(store, capsys, tmp_path, monkeypatch):
    """More than 20 rows changed between the read and the write: every id is printed
    (the export lists every candidate, so this list is what tells them apart)."""
    ids = [f"busy-{i:02d}-com" for i in range(bf.MAX_IDS + 5)]
    for did in ids:
        put_row(store, did, [_entry(did, f"https://www.{did}.com/a.json", last_ok_at=SEPT)])
    real_plan = bf.plan_backfill

    def plan_then_hint_writes(conn, **kw):
        plan = real_plan(conn, **kw)
        for c in plan.candidates:  # a hint write lands on every row after the read
            assert recipe_store.set_scan_hints(c.dealer_id, {"notes": "moved"})
        return plan

    monkeypatch.setattr(bf, "plan_backfill", plan_then_hint_writes)
    rc, out = run(capsys, "--apply", "--backup-dir", str(tmp_path / "bk"), "--no-cache-check")
    assert rc == 0, out
    assert "applied: 0" in out
    line = next(ln for ln in out.splitlines() if ln.strip().startswith("changed ("))
    assert line.endswith(": " + ", ".join(ids))
    assert "more)" not in line
