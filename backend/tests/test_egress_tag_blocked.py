"""A tagged (datacenter) scan host never stales or rejects shared recipes for a 401/403.

2026-09-29: the first Railway fleet runs got Cloudflare 403s on 25+ dealers whose
recipes replay fine from a home IP. The replay marked those recipes stale in the
shared store and the pipeline then skipped the dealers on every scanner.
"""
from __future__ import annotations

import json

from backend.scanner import recipes as rc
from backend.scanner import recipe_validation as rv
from backend.scripts import dealer_pipeline as dp


class _Hints:
    """In-memory scan_hints store behind the recipe_store hint API."""

    def __init__(self, monkeypatch, initial=None):
        import backend.scanner.recipe_store as rs

        self.h = dict(initial or {})
        monkeypatch.setattr(rs, "get_scan_hints", lambda did: dict(self.h))
        monkeypatch.setattr(rs, "mutate_scan_hints", self.mutate)
        monkeypatch.setattr(rs, "set_scan_hints", lambda did, upd, merge=True: self.mutate(
            did, lambda cur: {**cur, **upd}))

    def mutate(self, did, fn):
        new = fn(json.loads(json.dumps(self.h)))
        if new is not None:
            self.h = new
        return True


def test_egress_tag_sanitized(monkeypatch):
    monkeypatch.setenv("SCANNER_EGRESS_TAG", " Rail way!")
    assert rc.egress_tag() == "railway"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert rc.egress_tag() == ""


def test_stale_status_becomes_blocked_on_tagged_host(monkeypatch):
    st = _Hints(monkeypatch)
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    v = rc.record_stale_status("d1", 403)
    assert v.startswith("blocked:railway:403:")
    assert st.h["recipe_blocked"]["railway"] == v
    assert "recipe_status" not in st.h
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    v = rc.record_stale_status("d1", 403)
    assert v.startswith("stale:403:")
    assert st.h["recipe_status"] == v and "railway" in st.h["recipe_blocked"]


def _report(verdict, flags):
    r = rv.RecipeValidationReport.__new__(rv.RecipeValidationReport)
    r.verdict = verdict
    r.flags = flags
    r.reasons = ["auth_needed: HTTP 403"]
    return r


def test_auth_reject_recorded_as_blocked_on_tagged_host(monkeypatch):
    st = _Hints(monkeypatch, {"recipe_status": "ok"})
    monkeypatch.setattr(rv.RecipeValidationReport, "status", property(lambda self: "rejected:auth_needed"))
    monkeypatch.setattr(rv.RecipeValidationReport, "summary", lambda self: {})
    monkeypatch.setattr(rv.RecipeValidationReport, "stamp", "t", raising=False)
    rep = _report("reject", {"auth_needed": True, "zero_rows": True})

    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    rv.record_recipe_status("d1", rep)
    assert st.h["recipe_blocked"]["railway"].startswith("blocked:railway:auth_needed:")
    assert st.h["recipe_status"] == "ok"  # a Railway block never overwrites a home status

    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    rv.record_recipe_status("d1", rep)
    assert st.h["recipe_status"] == "rejected:auth_needed"

    # a non-auth reject on a tagged host is a real verdict
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    st.h["recipe_status"] = "ok"
    rv.record_recipe_status("d1", _report("reject", {"one_condition": True}))
    assert st.h["recipe_status"] == "rejected:auth_needed"


def test_railway_block_does_not_overwrite_home_rejected(monkeypatch):
    st = _Hints(monkeypatch, {"recipe_status": "rejected:one_condition"})
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    rc.record_stale_status("d1", 403)
    assert st.h["recipe_status"] == "rejected:one_condition"


def test_router_skips_lifecycle_only_on_the_blocked_host(monkeypatch):
    dealer = {"dealer_id": "d1", "url": "https://d1.example"}
    result = {"verdict": "no_rows"}
    st = "blocked:railway:403:2026-09-29T00:00:00+00:00"  # legacy value is still honoured
    _Hints(monkeypatch)
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert dp.route_verdict(dealer, result, st)["action"] == "none"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert dp.route_verdict(dealer, result, st)["action"] == "lifecycle"


def test_router_reads_per_host_recipe_blocked(monkeypatch):
    dealer = {"dealer_id": "d1", "url": "https://d1.example"}
    result = {"verdict": "no_rows"}
    _Hints(monkeypatch, {"recipe_status": "ok", "recipe_blocked": {"railway": "blocked:railway:403:x"}})
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert dp.route_verdict(dealer, result)["action"] == "none"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert dp.route_verdict(dealer, result)["action"] == "lifecycle"
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "other")
    assert dp.route_verdict(dealer, result)["action"] == "lifecycle"


def test_home_success_keeps_railway_block(monkeypatch):
    st = _Hints(monkeypatch, {"recipe_status": "stale:403:x", "recipe_blocked": {"railway": "blocked:railway:403:x"}})
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert rc.clear_stale_status("d1") is True
    assert st.h["recipe_status"] == "ok"
    assert st.h["recipe_blocked"] == {"railway": "blocked:railway:403:x"}


def test_tagged_success_clears_only_its_own_entry(monkeypatch):
    st = _Hints(monkeypatch, {"recipe_status": "ok", "recipe_blocked": {"railway": "b1", "fly": "b2"}})
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert rc.clear_stale_status("d1") is True
    assert st.h["recipe_blocked"] == {"fly": "b2"}
    assert rc.clear_stale_status("d1") is False


def test_success_clears_legacy_blocked(monkeypatch):
    st = _Hints(monkeypatch, {"recipe_status": "blocked:railway:403:x"})
    assert rc.clear_stale_status("d1") is True
    assert st.h["recipe_status"] == "ok"


def test_lifecycle_attempt_is_keyed_by_host_class(monkeypatch):
    from datetime import datetime, timezone
    from backend.scanner.pipeline import lifecycle as lc

    now = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
    hints = {"lifecycle_last_attempt@railway": "2026-10-01T01:00:00+00:00"}
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "railway")
    assert lc.lifecycle_attempted_today("d1", hints, now)
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    assert not lc.lifecycle_attempted_today("d1", hints, now)  # home is not blocked by Railway's attempt
