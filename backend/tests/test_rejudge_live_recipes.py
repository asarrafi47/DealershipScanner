"""P12B.7a: rejudge_live_recipes.py re-judges live recipes with the save paths' own walk
and gate, writes nothing in judge mode, and stales only owner-approved ids after a backup.

Fake fetches only (no dealer network). The DB tests use the Postgres harness (``pg_db``).
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from backend.scanner import recipe_validation as rv
from backend.scanner.recipes import EndpointRecipe
from backend.scripts import rejudge_live_recipes as rj
from backend.tests.test_recipe_validation import (  # noqa: F401 (autouse fixture)
    DEALER, NAME, ORIGIN, PLACE, _Feed, _cc_recipe, _lot, _no_registry_or_db,
)


@pytest.fixture(autouse=True)
def _tmp_logs(tmp_path, monkeypatch):
    monkeypatch.setattr(rv, "LOG_ROOT", tmp_path / "dealer_logs")
    monkeypatch.setattr(rv, "one_condition_ok_ids", lambda path=None: set())
    monkeypatch.setattr(rv, "shared_with_other_dealers", lambda *a, **k: {})


def _entry(recipe=None):
    return {"recipes": [recipe or _cc_recipe()], "base_url": ORIGIN, "dealer_name": NAME, "place": PLACE}


def _j(did, verdict, code="", **kw):
    return {"dealer_id": did, "verdict": verdict, "reason_code": code, "reasons": [code] if code else [],
            "vins_total": kw.get("vins", 0), "site_total": kw.get("site"), "coverage": None,
            "per_condition": kw.get("cond", {}), "flags": kw.get("flags", {}), "recipes": 1, "urls": []}


# ── judging ────────────────────────────────────────────────────────────────────

def test_rejudge_dealer_uses_the_gate_and_writes_nothing(tmp_path, monkeypatch):
    from backend.scanner import recipe_store

    def boom(*a, **k):
        raise AssertionError("judge mode must not touch scan_hints")
    monkeypatch.setattr(recipe_store, "set_scan_hints", boom)
    j = rj.rejudge_dealer(DEALER, _entry(), fetch=_Feed(_lot(60, 40), per_page=100))
    assert j["verdict"] in ("ok", "uncertain") and j["vins_total"] == 100
    assert j["urls"] and "?" not in j["urls"][0]
    assert not (tmp_path / "dealer_logs").exists()  # no discovery.md, no errors_index.md


def test_empty_feed_is_a_reject_candidate():
    j = rj.rejudge_dealer(DEALER, _entry(), fetch=_Feed([], per_page=100))
    assert j["verdict"] == "reject" and j["reason_code"] == "zero_rows"


def test_a_dealer_whose_walk_raises_is_an_error_not_a_stale_candidate():
    def fetch(*a, **k):
        raise RuntimeError("socket gone")
    # check_all swallows nothing about a raising fetch; either a reject or an error is
    # fine as long as the sweep survives and apply refuses an error.
    j = rj.rejudge_dealer(DEALER, _entry(), fetch=fetch)
    assert j["verdict"] in ("error", "reject")
    todo, refused = rj.plan_apply([dict(j, verdict="error")], [DEALER])
    assert todo == [] and refused == [DEALER]


def test_rejudge_all_judges_every_dealer_in_order():
    live = {"b-com": _entry(), "a-com": _entry()}
    seen = []
    out = rj.rejudge_all(live, fetch=_Feed(_lot(30, 30), per_page=100), progress=lambda i, n, j: seen.append((i, n)))
    assert [j["dealer_id"] for j in out] == ["a-com", "b-com"] and seen == [(1, 2), (2, 2)]


# ── the report ─────────────────────────────────────────────────────────────────

def test_report_names_one_condition_and_named_dealers():
    js = [_j("carvision-com", "reject", "short_page", vins=40, site=520),
          _j("northparklexus-com", "uncertain", "one_condition", flags={"one_condition": True}),
          _j("x-com", "ok", vins=100)]
    md = rj.build_report(js, stamp="S")
    assert "## One-condition dealers (1)" in md and "at least 14" in md
    assert "- carvision:" in md and "| carvision-com | reject |" in md
    assert "- northparklexus:" in md
    assert "- hendricklexuscharlotte: no live recipe in this run" in md
    assert "Candidates to stale (2)" in md and "| x-com |" not in md.split("## One-condition")[0]


def test_subset_run_does_not_claim_the_14():
    md = rj.build_report([_j("x-com", "ok")], partial=True)
    assert "at least 14" not in md and "(a subset" in md


# ── loading the live set ───────────────────────────────────────────────────────

class _Cur:
    def __init__(self, sets):
        self.sets, self.cur = list(sets), []

    def execute(self, sql, params=None):
        self.cur = self.sets.pop(0)

    def fetchall(self):
        return self.cur


class _Conn:
    def __init__(self, sets):
        self._c = _Cur(sets)

    def cursor(self):
        return self._c


def test_load_live_keeps_live_recipes_only():
    live, dead = _cc_recipe().__dict__, dict(_cc_recipe().__dict__, stale=True)
    conn = _Conn([[("a-com", json.dumps([live, dead])), ("allstale-com", json.dumps([dead])), ("bad-com", "{")],
                  [("a-com", "A Motors", "https://a.com/")]])
    out = rj.load_live(conn)
    assert list(out) == ["a-com"] and len(out["a-com"]["recipes"]) == 1
    assert out["a-com"]["base_url"] == "https://a.com" and out["a-com"]["dealer_name"] == "A Motors"


# ── apply planning and the CLI ─────────────────────────────────────────────────

def test_plan_apply_takes_only_approved_reject_or_uncertain():
    js = [_j("a", "reject"), _j("b", "uncertain"), _j("c", "ok"), _j("d", "reject")]
    todo, refused = rj.plan_apply(js, ["a", "b", "c", "zz"])
    assert [j["dealer_id"] for j in todo] == ["a", "b"] and refused == ["c", "zz"]  # d is not approved


def test_cli_apply_plan_is_a_dry_run_by_default(tmp_path, monkeypatch, capsys):
    called = []
    monkeypatch.setattr(rj, "backup_rows", lambda *a, **k: called.append("backup"))
    monkeypatch.setattr(rj, "apply_stale", lambda *a, **k: called.append("stale"))
    (tmp_path / "j.json").write_text(json.dumps([_j("a-com", "reject")]))
    (tmp_path / "ok.txt").write_text("a-com  # owner ok\n")
    assert rj.main(["--json", str(tmp_path / "j.json"), "--approved", str(tmp_path / "ok.txt")]) == 0
    assert called == [] and "would stale a-com" in capsys.readouterr().out
    assert rj.main(["--json", str(tmp_path / "j.json"), "--approved", str(tmp_path / "ok.txt"), "--apply"]) == 2
    assert called == []  # --apply without --backup-dir stops before any write


def test_cli_refuses_a_tagged_host(monkeypatch):
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert rj.main(["--all"]) == 2


def test_cli_apply_runs_backup_before_stale(tmp_path, monkeypatch):
    order = []
    monkeypatch.setattr(rj, "backup_rows", lambda ids, d, **k: order.append(("backup", ids)) or {
        "rows": 1, "files": {rj.BACKUP_NAME: {"sha256": "0" * 64}}})
    monkeypatch.setattr(rj, "apply_stale", lambda todo, **k: order.append(("stale", len(todo))) or 1)
    (tmp_path / "j.json").write_text(json.dumps([_j("a-com", "reject")]))
    (tmp_path / "ok.txt").write_text("a-com\n")
    assert rj.main(["--json", str(tmp_path / "j.json"), "--approved", str(tmp_path / "ok.txt"),
                    "--backup-dir", str(tmp_path / "b"), "--apply"]) == 0
    assert order == [("backup", ["a-com"]), ("stale", 1)]


def test_a_failed_backup_stops_the_apply(tmp_path, monkeypatch):
    stale = []
    monkeypatch.setattr(rj, "backup_rows", lambda *a, **k: (_ for _ in ()).throw(rj.Refused("no copy")))
    monkeypatch.setattr(rj, "apply_stale", lambda *a, **k: stale.append(1))
    (tmp_path / "j.json").write_text(json.dumps([_j("a-com", "reject")]))
    (tmp_path / "ok.txt").write_text("a-com\n")
    assert rj.main(["--json", str(tmp_path / "j.json"), "--approved", str(tmp_path / "ok.txt"),
                    "--backup-dir", str(tmp_path / "b"), "--apply"]) == 2
    assert stale == []


# ── Postgres: backup and stale ─────────────────────────────────────────────────

def _seed(pg_db, dealer_id, recipes):
    conn = pg_db.connect()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO dealer_recipes (dealer_id, recipes_json, recipe_count) VALUES (%s, %s, %s)",
                    (dealer_id, json.dumps([r.__dict__ for r in recipes]), len(recipes)))
    conn.commit()
    conn.close()


def _csv_copy(pg_db):
    def copy(dsn, ids, dest):
        conn = pg_db.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT dealer_id, recipes_json FROM dealer_recipes WHERE dealer_id = ANY(%s) ORDER BY 1", (ids,))
            rows = cur.fetchall()
        conn.close()
        with open(dest, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["dealer_id", "recipes_json"])
            w.writerows(rows)
    return copy


@pytest.mark.pg
def test_backup_writes_csv_and_sha256_manifest(pg_db, tmp_path):
    import hashlib

    _seed(pg_db, "a-com", [_cc_recipe()])
    _seed(pg_db, "b-com", [_cc_recipe()])
    m = rj.backup_rows(["a-com", "b-com"], tmp_path / "bk", dsn=pg_db.dsn, copy=_csv_copy(pg_db))
    f = tmp_path / "bk" / rj.BACKUP_NAME
    assert m["rows"] == 2 and m["files"][rj.BACKUP_NAME]["sha256"] == hashlib.sha256(f.read_bytes()).hexdigest()
    assert json.loads((tmp_path / "bk" / rj.MANIFEST_NAME).read_text())["ids"] == ["a-com", "b-com"]


@pytest.mark.pg
def test_backup_row_count_mismatch_refuses(pg_db, tmp_path):
    _seed(pg_db, "a-com", [_cc_recipe()])
    short = lambda dsn, ids, dest: Path(dest).write_text("dealer_id,recipes_json\n")  # noqa: E731
    with pytest.raises(rj.Refused):
        rj.backup_rows(["a-com"], tmp_path / "bk", dsn=pg_db.dsn, copy=short)
    with pytest.raises(rj.Refused):  # an id with no row: nothing to back up
        rj.backup_rows(["nobody-com"], tmp_path / "bk2", dsn=pg_db.dsn, copy=_csv_copy(pg_db))


@pytest.mark.pg
def test_apply_stales_only_the_approved_dealer_and_logs_it(pg_db, tmp_path, monkeypatch):
    import backend.scanner.recipes as rec

    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    for did in ("a-com", "b-com"):
        rec.save_recipes(did, [_cc_recipe()])
    j = _j("a-com", "reject", "zero_rows")
    assert rj.apply_stale([j], log_root=tmp_path / "logs") == 1
    a, b = rec.load_recipes("a-com")[0], rec.load_recipes("b-com")[0]
    assert a.stale and a.stale_reason == "rejudge:reject" and not b.stale
    log = (tmp_path / "logs" / "a-com" / "discovery.md").read_text()
    assert "rejudge:reject" in log and "zero_rows" in log
    assert not (tmp_path / "logs" / "b-com").exists()
