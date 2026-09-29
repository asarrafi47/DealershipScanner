"""A tagged (datacenter) scan host never stales or rejects shared recipes for a 401/403.

2026-09-29: the first Railway fleet runs got Cloudflare 403s on 25+ dealers whose
recipes replay fine from a home IP. The replay marked those recipes stale in the
shared store and the pipeline then skipped the dealers on every scanner.
"""
from __future__ import annotations

from backend.scanner import recipes as rc
from backend.scanner import recipe_validation as rv
from backend.scripts import dealer_pipeline as dp


def test_egress_tag_sanitized(monkeypatch):
    monkeypatch.setenv("SCANNER_EGRESS_TAG", " Rail way!")
    assert rc.egress_tag() == "railway"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert rc.egress_tag() == ""


def test_stale_status_becomes_blocked_on_tagged_host(monkeypatch):
    writes = []
    import backend.scanner.recipe_store as rs

    monkeypatch.setattr(rs, "set_scan_hints", lambda did, h: writes.append((did, h)) or True)
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    v = rc.record_stale_status("d1", 403)
    assert v.startswith("blocked:railway:403:")
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    v = rc.record_stale_status("d1", 403)
    assert v.startswith("stale:403:")


def _report(verdict, flags):
    r = rv.RecipeValidationReport.__new__(rv.RecipeValidationReport)
    r.verdict = verdict
    r.flags = flags
    r.reasons = ["auth_needed: HTTP 403"]
    return r


def test_auth_reject_recorded_as_blocked_on_tagged_host(monkeypatch):
    writes = []
    import backend.scanner.recipe_store as rs

    monkeypatch.setattr(rs, "set_scan_hints", lambda did, h: writes.append(h) or True)
    monkeypatch.setattr(rv.RecipeValidationReport, "status", property(lambda self: "rejected:auth_needed"))
    monkeypatch.setattr(rv.RecipeValidationReport, "summary", lambda self: {})
    monkeypatch.setattr(rv.RecipeValidationReport, "stamp", "t", raising=False)
    rep = _report("reject", {"auth_needed": True, "zero_rows": True})

    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    rv.record_recipe_status("d1", rep)
    assert writes[-1]["recipe_status"].startswith("blocked:railway:auth_needed:")

    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    rv.record_recipe_status("d1", rep)
    assert writes[-1]["recipe_status"] == "rejected:auth_needed"

    # a non-auth reject on a tagged host is a real verdict
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    rv.record_recipe_status("d1", _report("reject", {"one_condition": True}))
    assert writes[-1]["recipe_status"] == "rejected:auth_needed"


def test_router_skips_lifecycle_only_on_the_blocked_host(monkeypatch):
    dealer = {"dealer_id": "d1", "url": "https://d1.example"}
    result = {"verdict": "no_rows"}
    st = "blocked:railway:403:2026-09-29T00:00:00+00:00"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert dp.route_verdict(dealer, result, st)["action"] == "none"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert dp.route_verdict(dealer, result, st)["action"] == "lifecycle"


def test_success_clears_blocked(monkeypatch):
    import backend.scanner.recipe_store as rs

    state = {"recipe_status": "blocked:railway:403:x"}
    monkeypatch.setattr(rs, "get_scan_hints", lambda did: dict(state))
    monkeypatch.setattr(rs, "set_scan_hints", lambda did, h: state.update(h) or True)
    assert rc.clear_stale_status("d1") is True
    assert state["recipe_status"] == "ok"
