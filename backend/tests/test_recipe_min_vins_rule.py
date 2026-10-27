"""P12B.5 (D-OD1): one minimum-VIN rule at save and at scan, and CF-3's duplicate refusal.

Save paths keep a candidate with any store VIN and need RECIPE_MIN_VINS across the kept
set. The scan's union admits a sub-floor recipe only as a condition-pinned, paginated,
gate-ok side; a first-hit replay is judged against the recipe's own saved size."""
from __future__ import annotations

import asyncio
import importlib
import json
import re
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.attribution as attribution
import backend.scanner.recipe_totals as rt
import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner import recipe_validation as rv
from backend.scanner.recipes import PAGINATION_CARSCOMMERCE, PAGINATION_NONE, EndpointRecipe

D = "mclaren-com"
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "recipes")
    monkeypatch.setattr(rv, "LOG_ROOT", tmp_path / "logs")
    monkeypatch.delenv("SCANNER_RECIPE_MIN_VEHICLES", raising=False)
    monkeypatch.setattr(recipe_store, "db_other_dealers_with_url", lambda dealer_id, url: [])


def _vin(i):
    return f"1HGBH41JXMN{i:06d}"


def _recipe(url="https://feed.example/inv", *, pagination=PAGINATION_CARSCOMMERCE, cond=None, rows=0, verdict="", did=D):
    body = {"page": 1, "filters": {"type": [cond]} if cond else {}}
    return EndpointRecipe(dealer_id=did, url=url, method="POST", content_type="application/json",
                          post_template=json.dumps(body), pagination=pagination, provider_hint="carscommerce",
                          vehicle_rows=rows, gate_verdict=verdict)


# ---- the constant -----------------------------------------------------------

def test_one_constant_and_no_literal_floors_elsewhere():
    assert rt.RECIPE_MIN_VINS == 5
    for rel in ("backend/scanner/pipeline/recipes.py", "backend/scripts/synthesize_recipes.py",
                "backend/scripts/cascade_recipes.py", "backend/scanner/recipe_cascade.py"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert not re.search(r"(>=|>|<)\s*(3|5|10)\b|default\s*=\s*(3|5|10)\b|min_vins\s*[:=]\s*(int\s*=\s*)?(3|5|10)\b", text), rel
    assert rec.recipe_min_vehicles() == 10 and rt.union_floor(10) == 5 and rt.union_floor(3) == 3
    assert rt.first_hit_floor(10, 6) == 3 and rt.first_hit_floor(10, 40) == 10 and rt.first_hit_floor(10, 0) == 10
    assert rt.first_hit_floor(10, 2) == 3


# ---- the scan ---------------------------------------------------------------

def _install(monkeypatch, recipes, lots):
    """lots: recipe url+cond -> number of VINs served in one page; each recipe gets disjoint VINs."""
    monkeypatch.setattr(rec, "load_recipes", lambda dealer_id: list(recipes))
    monkeypatch.setattr(rec, "_persist_replay_success", lambda *a, **k: None)
    offsets = {id(r): 1000 * i for i, r in enumerate(recipes)}
    by_body = {r.post_template: r for r in recipes}

    def fake_req(r, body, base_url, url=None):
        if (body or {}).get("page", 1) > 1:
            return 200, {"rows": []}
        n = lots[id(by_body[r.post_template])]
        return 200, {"rows": [offsets[id(r)] + i for i in range(n)]}

    monkeypatch.setattr(rec, "_replay_request", fake_req)
    monkeypatch.setattr(attribution, "parse_page", lambda prov, parsed, ctx, rej, **kw: [{"vin": _vin(i)} for i in parsed["rows"]])


def _scan(union):
    return asyncio.run(rec.try_fetch_via_recipes(D, "carscommerce", "https://x.example", "X", union=union))


def test_mclaren_shape_union_is_36(monkeypatch):
    used, new = _recipe(cond="Used", rows=28, verdict="ok"), _recipe(cond="New", rows=8, verdict="ok")
    _install(monkeypatch, [used, new], {id(used): 28, id(new): 8})
    hit = _scan(True)
    assert hit is not None and hit[1] == 36


def test_sub_floor_side_refused_without_pagination_pin_or_gate(monkeypatch):
    used = _recipe(cond="Used", rows=28, verdict="ok")
    for bad in (_recipe(cond="New", rows=8, verdict="ok", pagination=PAGINATION_NONE),
                _recipe(cond=None, rows=8, verdict="ok"),
                _recipe(cond="New", rows=8, verdict="")):
        _install(monkeypatch, [used, bad], {id(used): 28, id(bad): 8})
        assert _scan(True)[1] == 28, bad


def test_dealer_whose_only_recipe_yields_four_is_none_at_scan(monkeypatch):
    only = _recipe(cond="New", rows=4, verdict="ok")
    _install(monkeypatch, [only], {id(only): 4})
    assert _scan(True) is None


def test_first_hit_uses_the_saved_size(monkeypatch):
    small = _recipe(rows=6)
    _install(monkeypatch, [small], {id(small): 6})
    assert _scan(False)[1] == 6
    big = _recipe(rows=40)
    _install(monkeypatch, [big], {id(big): 1})
    assert _scan(False) is None


# ---- the save paths ---------------------------------------------------------

def _fake_walk(counts):
    def walk(dealer_id, cands, **kw):
        w = SimpleNamespace(walks=[(c, SimpleNamespace(vins=n), set(), {}, None) for c, n in zip(cands, counts)])
        w.keep = lambda recipes: w
        return w
    return walk


def _ensure(monkeypatch, counts):
    from backend.scanner import dealer_place, recipe_synth
    from backend.scanner.pipeline.recipes import ensure_recipe

    cands = [_recipe(f"https://feed.example/{i}", cond=c) for i, c in enumerate(("Used", "New"))][: len(counts)]
    saved: list = []
    monkeypatch.setattr(rec, "load_recipes", lambda did: [])
    monkeypatch.setattr(rec, "replace_recipes", lambda did, recs, reason="": saved.extend(recs))
    monkeypatch.setattr(recipe_synth, "fetch_dealer_html", lambda url: "<html/>")
    monkeypatch.setattr(recipe_synth, "fingerprint_platform", lambda html, url: "carscommerce")
    monkeypatch.setattr(recipe_synth, "synthesize_recipes", lambda *a: list(cands))
    monkeypatch.setattr(rv, "walk_candidates", _fake_walk(counts))
    monkeypatch.setattr(dealer_place, "learn_place", lambda *a, **k: {})
    report = SimpleNamespace(status="ok", summary=lambda: "ok")
    monkeypatch.setattr(rv, "gate_recipes", lambda did, kept, **k: (kept, report))
    return ensure_recipe({"dealer_id": D, "url": "https://x.example", "name": "X"}), saved


def test_ensure_recipe_keeps_three_plus_three(monkeypatch):
    info, saved = _ensure(monkeypatch, [3, 3])
    assert info["synth"] == "saved_2" and [r.vehicle_rows for r in saved] == [3, 3]


def test_ensure_recipe_four_vins_is_validated_zero(monkeypatch):
    info, saved = _ensure(monkeypatch, [4])
    assert info["synth"] == "validated_zero" and not saved


def test_ensure_recipe_one_vin_recommendations_stays_out(monkeypatch):
    info, saved = _ensure(monkeypatch, [1])
    assert info["synth"] == "validated_zero" and not saved


@pytest.fixture
def sr(monkeypatch):
    monkeypatch.chdir(Path.cwd())
    previous = signal.getsignal(signal.SIGALRM)
    module = importlib.import_module("backend.scripts.synthesize_recipes")
    yield module
    signal.alarm(0)
    signal.signal(signal.SIGALRM, previous)


def test_cli_keeps_three_plus_three_and_defaults_to_the_constant(monkeypatch, sr):
    cands = [_recipe("https://feed.example/a", cond="Used"), _recipe("https://feed.example/b", cond="New")]
    monkeypatch.setattr(sr, "classify_dealer", lambda *a, **k: {})
    monkeypatch.setattr(sr, "_gather_html", lambda url: ("<html/>", "carscommerce"))
    monkeypatch.setattr(sr, "is_synthesizable", lambda p: True)
    monkeypatch.setattr(sr, "_try_html_harvest", lambda *a, **k: False)
    monkeypatch.setattr(sr, "synthesize_recipes", lambda *a: list(cands))
    monkeypatch.setattr(rv, "walk_candidates", _fake_walk([3, 3]))
    report = SimpleNamespace(status="ok", summary=lambda: "ok", verdict="ok", reasons=[])
    monkeypatch.setattr(rv, "gate_recipes", lambda did, kept, **k: (kept, report))
    row = sr._process_dealer(D, "X", "https://x.example", min_vins=rt.RECIPE_MIN_VINS, dry_run=True, force=False)
    assert row["vins"] == 6 and row["strategy"] == "synthesize"
    assert sr._process_dealer(D, "X", "https://x.example", min_vins=7, dry_run=True, force=False)["strategy"] == ""
    assert "RECIPE_MIN_VINS" in (ROOT / "backend/scripts/synthesize_recipes.py").read_text()


# ---- gate provenance and CF-3 -----------------------------------------------

def _report(verdict):
    return rv.RecipeValidationReport(dealer_id=D, verdict=verdict, vins_total=30)


def test_gate_stamps_provenance_on_ok_and_uncertain_only(monkeypatch):
    for verdict, stamped in (("ok", "ok"), ("uncertain", "uncertain"), ("reject", "")):
        monkeypatch.setattr(rv, "validate_recipe_set", lambda *a, _v=verdict, **k: _report(_v))
        r = _recipe()
        rv.gate_recipes(D, [r], record=False)
        assert r.gate_verdict == stamped and (r.gate_at > 0) == bool(stamped)


def test_duplicate_endpoint_of_another_dealer_is_refused(monkeypatch):
    monkeypatch.setattr(rv, "validate_recipe_set", lambda *a, **k: _report("ok"))
    mine = _recipe(did=D)
    theirs = _recipe(did="other-com")
    monkeypatch.setattr(recipe_store, "db_other_dealers_with_url",
                        lambda dealer_id, url: [("other-com", [json.loads(json.dumps(theirs.__dict__))])])
    kept, rep = rv.gate_recipes(D, [mine], record=False)
    assert kept == [] and rep.status == "rejected:duplicate_endpoint" and rep.flags["duplicate_endpoint"]
    assert "other-com" in rep.reasons[0]
    kept, rep = rv.gate_recipes(D, [mine], record=False, allow_shared=True)
    assert kept == [mine] and rep.verdict == "ok"
    # a different body at the same URL is a different request
    other_body = dict(theirs.__dict__, post_template=json.dumps({"page": 1, "filters": {"type": ["Used"]}}))
    monkeypatch.setattr(recipe_store, "db_other_dealers_with_url", lambda dealer_id, url: [("other-com", [other_body])])
    assert rv.gate_recipes(D, [mine], record=False)[0] == [mine]
    # a stale copy is not a live duplicate
    monkeypatch.setattr(recipe_store, "db_other_dealers_with_url",
                        lambda dealer_id, url: [("other-com", [dict(theirs.__dict__, stale=True)])])
    assert rv.gate_recipes(D, [mine], record=False)[0] == [mine]


@pytest.mark.pg
def test_duplicate_lookup_reads_the_store(pg_db, monkeypatch):
    monkeypatch.undo()  # the autouse stub off: this test reads the real store
    monkeypatch.setattr(rec, "RECIPES_DIR", Path("/nonexistent-recipes-dir-for-test"))
    from dataclasses import asdict

    theirs = _recipe(did="other-com")
    assert recipe_store.db_save_recipes("other-com", [asdict(theirs)], file_written=False)
    assert [d for d, _ in recipe_store.db_other_dealers_with_url(D, theirs.url)] == ["other-com"]
    assert recipe_store.db_other_dealers_with_url("other-com", theirs.url) == []
    assert set(rv.shared_with_other_dealers(D, [_recipe(did=D)])) == {"other-com"}
