"""P12B.6 (D-OD5): the cascade goes through the set gate, a rejected shape falls through
to the next, a dry run writes nothing, and a success lands in discovery.md as ``cascade``."""
from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

import backend.scanner.recipe_cascade as rc
import backend.scanner.recipe_synth as synth
import backend.scanner.recipe_validation as rv
import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner.recipes import EndpointRecipe

TARGET = "https://www.targetchev.com"


def _recipe(url, *, did="donor1", body=None, rows=100, headers=None):
    return EndpointRecipe(dealer_id=did, url=url, method="POST", content_type="application/json",
                          post_template=body, vehicle_rows=rows, auth_headers=headers or {})


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(rv, "LOG_ROOT", tmp_path / "logs")
    monkeypatch.setattr(recipe_store, "db_other_dealers_with_url", lambda dealer_id, url: [])


# ---- adapt_recipe / guards --------------------------------------------------

def test_adapt_substitutes_host_in_url_body_and_headers():
    donor = _recipe("https://www.donorford.com/api/inv", body=json.dumps({"site": "donorford.com", "k": "donorford"}),
                    headers={"Referer": "https://www.donorford.com/new"})
    out = rc.adapt_recipe(donor, "target", TARGET + "/")
    assert out.url == "https://www.targetchev.com/api/inv"
    assert json.loads(out.post_template) == {"site": "targetchev.com", "k": "targetchev"}
    assert out.auth_headers["Referer"] == "https://www.targetchev.com/new"
    assert out.dealer_id == "target" and out.vehicle_rows == 0 and out.provider_hint.endswith("+cascade")
    assert donor.url == "https://www.donorford.com/api/inv"  # donor untouched


def test_adapt_refuses_same_host_and_tenant_id_paths():
    assert rc.adapt_recipe(_recipe("https://www.targetchev.com/api"), "t", TARGET) is None
    assert rc.adapt_recipe(_recipe("https://www.donorford.com/api/v1/listings/196006/search"), "t", TARGET) is None
    assert rc.path_has_tenant_id("/a/0123456789abcdef0123/b") and not rc.path_has_tenant_id("/api/inventory/v2")


def test_donor_inventory_guard():
    ok = _recipe("https://www.targetchev.com/api", body='{"site":"targetchev.com"}')
    assert not rc._looks_like_donor_inventory(ok, TARGET)
    leaked = _recipe("https://www.targetchev.com/api", body='{"site":"donorford.com"}')
    assert rc._looks_like_donor_inventory(leaked, TARGET)
    assert rc._looks_like_donor_inventory(_recipe("https://other.example/api"), TARGET)


# ---- cascade_for_dealer fall-through ----------------------------------------

def _shapes():
    return [_recipe("https://www.donora.com/api/a", did="a"), _recipe("https://www.donorb.com/api/b", did="b")]


def _count(monkeypatch, vins=20):
    seen = []

    def fake(recipe, url, dealer_id, name):
        seen.append(recipe.url)
        return vins

    monkeypatch.setattr(synth, "validate_recipe", fake)
    return seen


def test_without_gate_first_shape_wins(monkeypatch):
    seen = _count(monkeypatch)
    r, vins, tried = rc.cascade_for_dealer("t", TARGET, "T", _shapes())
    assert r.url.endswith("/api/a") and vins == 20 and tried == 1 and len(seen) == 1


def test_gate_reject_falls_through_to_next_shape(monkeypatch):
    _count(monkeypatch)
    asked = []

    def accept(recipe, vins):
        asked.append(recipe.url)
        return recipe.url.endswith("/api/b")

    r, vins, tried = rc.cascade_for_dealer("t", TARGET, "T", _shapes(), accept=accept)
    assert r.url.endswith("/api/b") and tried == 2 and len(asked) == 2


def test_all_rejected_returns_none_and_gate_error_is_a_refusal(monkeypatch):
    _count(monkeypatch)

    def boom(recipe, vins):
        raise RuntimeError("x")

    r, vins, tried = rc.cascade_for_dealer("t", TARGET, "T", _shapes(), accept=boom)
    assert r is None and vins == 0 and tried == 2


def test_below_floor_never_reaches_gate(monkeypatch):
    _count(monkeypatch, vins=1)
    called = []
    r, *_ = rc.cascade_for_dealer("t", TARGET, "T", _shapes(), accept=lambda *a: called.append(a) or True)
    assert r is None and not called


# ---- the script -------------------------------------------------------------

class _Cur:
    def __init__(self):
        self.sql = ""

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        if "FROM cars" in self.sql:
            return [("donorx",)]
        return [("targetchev", "Target Chevrolet", TARGET)]


def _run_script(monkeypatch, tmp_path, verdicts, argv):
    from backend.scripts import cascade_recipes as cr

    monkeypatch.setattr(cr, "db_connect", lambda autocommit=True: SimpleNamespace(cursor=lambda: _Cur()))
    monkeypatch.setattr(rc, "collect_shapes", lambda ids: _shapes())
    _count(monkeypatch)
    saved, statuses = [], []
    monkeypatch.setattr(rec, "merge_recipes", lambda key, rs: saved.append((key, rs)))
    monkeypatch.setattr(rv, "record_recipe_status", lambda *a, **k: statuses.append(a) or True)
    queue = list(verdicts)

    def fake_set(dealer_id, recipes, fetch=None, **kw):
        return rv.RecipeValidationReport(dealer_id, verdict=queue.pop(0), reasons=["r"], vins_total=20)

    monkeypatch.setattr(rv, "validate_recipe_set", fake_set)
    monkeypatch.setattr(sys, "argv", ["cascade_recipes", *argv])
    assert cr.main() == 0
    return saved, statuses


def test_reject_saves_nothing_and_is_logged_then_next_shape_saved(monkeypatch, tmp_path):
    saved, _ = _run_script(monkeypatch, tmp_path, ["reject", "ok"], [])
    assert [k for k, _ in saved] == ["targetchev"] and saved[0][1][0].url.endswith("/api/b")
    log = (tmp_path / "logs" / "targetchev" / "discovery.md").read_text()
    assert log.count("cascade") >= 2 and "reject" in log.lower()


def test_dry_run_writes_nothing(monkeypatch, tmp_path):
    saved, statuses = _run_script(monkeypatch, tmp_path, ["ok"], ["--dry-run"])
    assert saved == [] and statuses == []
    assert not (tmp_path / "logs").exists()


def test_success_writes_discovery_with_cascade_context(monkeypatch, tmp_path):
    saved, statuses = _run_script(monkeypatch, tmp_path, ["ok"], [])
    assert len(saved) == 1 and len(statuses) == 1
    assert "cascade" in (tmp_path / "logs" / "targetchev" / "discovery.md").read_text()
    assert saved[0][1][0].gate_verdict == "ok"
