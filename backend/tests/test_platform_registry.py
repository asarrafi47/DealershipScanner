"""Tests for the platform-learning registry.

Network-free: DNS (``cname_chain``) and the HTTP probe are monkeypatched, and the
JSON overrides / unclassified log are pointed at tmp files, so nothing here
touches a live dealer or the committed registry.
"""
from __future__ import annotations

import json

import pytest

from backend.scanner import platform_registry as pr


# ── load_registry: seed + JSON overrides ──────────────────────────────────────


def test_seed_has_expected_platforms():
    names = {e.name for e in pr.SEED_PLATFORMS}
    assert {"carscommerce", "dealer_dot_com", "dealer_on_cosmos", "typesense",
            "team_velocity", "sister_tv", "jazel"} <= names


def test_jazel_seed_is_synthesize_cloudflare():
    # Jazel is now browser-free via an SSR SRP page-walk (jzlSetVehicleInfoContext),
    # so it synthesizes a recipe rather than needing html_harvest.
    jazel = next(e for e in pr.SEED_PLATFORMS if e.name == "jazel")
    assert jazel.strategy == pr.STRATEGY_SYNTHESIZE
    assert jazel.cloudflare is True
    assert jazel.synthesizable is True
    assert any("jazelc.com" in p for p in jazel.cname_patterns)


def test_json_override_adds_new_platform(tmp_path):
    p = tmp_path / "reg.json"
    p.write_text(json.dumps({"platforms": [{
        "name": "acme_platform",
        "cname_patterns": ["acmedealers.net"],
        "html_markers": ["acme-inventory-widget"],
        "strategy": "html_harvest",
        "cloudflare": True,
    }]}))
    reg = pr.load_registry(p)
    by_name = {e.name: e for e in reg}
    assert "acme_platform" in by_name
    assert by_name["acme_platform"].strategy == "html_harvest"
    assert by_name["acme_platform"].synthesizable is False
    # seed entries still present
    assert "jazel" in by_name


def test_json_override_replaces_seed_entry(tmp_path):
    p = tmp_path / "reg.json"
    p.write_text(json.dumps({"platforms": [{
        "name": "jazel",
        "cname_patterns": ["jazelc.com", "newjazelhost.net"],
        "strategy": "html_harvest",
        "notes": "updated",
    }]}))
    by_name = {e.name: e for e in pr.load_registry(p)}
    assert "newjazelhost.net" in by_name["jazel"].cname_patterns
    assert by_name["jazel"].notes == "updated"


def test_bad_strategy_defaults_to_browser(tmp_path):
    p = tmp_path / "reg.json"
    p.write_text(json.dumps({"platforms": [{"name": "weird", "strategy": "nonsense"}]}))
    by_name = {e.name: e for e in pr.load_registry(p)}
    assert by_name["weird"].strategy == pr.STRATEGY_BROWSER


def test_missing_json_returns_seed(tmp_path):
    assert len(pr.load_registry(tmp_path / "does_not_exist.json")) == len(pr.SEED_PLATFORMS)


# ── PlatformEntry matching ────────────────────────────────────────────────────


def test_cname_match_substring():
    jazel = next(e for e in pr.SEED_PLATFORMS if e.name == "jazel")
    assert jazel.matches_cname(["client-sandersonford.jazelc.com", "jazel-cdn.com"]) is True
    assert jazel.matches_cname(["something.else.com"]) is False


def test_typesense_has_no_cname_pattern_but_html_markers():
    ts = next(e for e in pr.SEED_PLATFORMS if e.name == "typesense")
    assert ts.cname_patterns == []
    assert ts.matches_cname(["anything.typesense.net"]) is False  # DNS can't ID it
    assert ts.matches_html('var __tshost = "x.a1.typesense.net";') is True


# ── classify_dealer: DNS path ─────────────────────────────────────────────────


def test_classify_via_dns(monkeypatch):
    monkeypatch.setattr(pr, "cname_chain",
                        lambda host: ["client-x.jazelc.com", "jazel-cdn.com"])
    res = pr.classify_dealer("https://www.sandersonford.com", do_http=False)
    assert res["platform"] == "jazel"
    assert res["source"] == "dns"
    assert res["strategy"] == pr.STRATEGY_SYNTHESIZE
    assert res["cloudflare"] is True


def test_classify_dns_dealer_com(monkeypatch):
    monkeypatch.setattr(pr, "cname_chain",
                        lambda host: ["le0016.secure.dealer.com.edgekey.net"])
    res = pr.classify_dealer("https://www.camelbacktoyota.com", do_http=False)
    assert res["platform"] == "dealer_dot_com"
    assert res["source"] == "dns"
    assert res["synthesizable"] is True


# ── classify_dealer: HTML fallback ────────────────────────────────────────────


def _probe(html=None, *, status=200, size=None, challenge=False, api_hosts=None):
    return pr.HttpProbe(
        status=status,
        size=size if size is not None else (len(html) if html else 0),
        html=html,
        challenge=challenge,
        api_hosts=api_hosts or [],
    )


def test_classify_html_fallback_when_dns_blank(monkeypatch):
    monkeypatch.setattr(pr, "cname_chain", lambda host: [])
    html = "<html>" + "x" * 3000 + 'var __tsHost="h.a1.typesense.net"; typesense.net</html>'
    monkeypatch.setattr(pr, "_http_probe", lambda url, **k: _probe(html))
    res = pr.classify_dealer("https://www.freewayhonda.com")
    assert res["platform"] == "typesense"
    assert res["source"] == "html"


def test_classify_html_uses_recipe_synth_fingerprint(monkeypatch):
    # A generic marker "carscommerce" is in html; recipe_synth's richer
    # fingerprint should still resolve the registry entry by name.
    monkeypatch.setattr(pr, "cname_chain", lambda host: [])
    html = "<html>" + "y" * 3000 + " carscommerce websites-search.api.carscommerce.inc</html>"
    monkeypatch.setattr(pr, "_http_probe", lambda url, **k: _probe(html))
    monkeypatch.setattr(pr, "_recipe_synth_fingerprint", lambda h, u: "carscommerce")
    res = pr.classify_dealer("https://x.com")
    assert res["platform"] == "carscommerce"
    assert res["source"] == "html"


# ── classify_dealer: unknown -> logged ────────────────────────────────────────


def test_unknown_dealer_is_logged(monkeypatch, tmp_path):
    log = tmp_path / "unclassified.json"
    monkeypatch.setattr(pr, "cname_chain", lambda host: ["ext-sq.squarespace.com"])
    monkeypatch.setattr(pr, "_http_probe",
                        lambda url, **k: _probe("<html>" + "z" * 3000 + "</html>",
                                                api_hosts=["search-api.space-auto.com"]))
    res = pr.classify_dealer("https://www.newplatform.com", unclassified_path=log)
    assert res["platform"] is None
    assert res["source"] == "unknown"
    assert res["logged_new"] is True

    logged = json.loads(log.read_text())
    assert "www.newplatform.com" in logged
    sig = logged["www.newplatform.com"]["signals"]
    assert sig["cname_chain"] == ["ext-sq.squarespace.com"]
    assert "search-api.space-auto.com" in sig["api_hosts"]
    assert sig["http_status"] == 200


def test_unknown_log_dedupes_by_host(monkeypatch, tmp_path):
    log = tmp_path / "unclassified.json"
    monkeypatch.setattr(pr, "cname_chain", lambda host: ["unknown.host.net"])
    monkeypatch.setattr(pr, "_http_probe", lambda url, **k: _probe("<html>" + "z" * 3000 + "</html>"))
    first = pr.classify_dealer("https://www.dupe.com", unclassified_path=log)
    second = pr.classify_dealer("https://www.dupe.com", unclassified_path=log)
    assert first["logged_new"] is True
    assert second["logged_new"] is False
    logged = json.loads(log.read_text())
    assert len(logged) == 1  # deduped


def test_learning_loop_json_override_resolves_unknown(monkeypatch, tmp_path):
    """The end-to-end learning loop: an unknown dealer, then ONE JSON entry that
    classifies it (and every future dealer on that platform)."""
    reg_json = tmp_path / "reg.json"
    reg_json.write_text(json.dumps({"platforms": []}))
    monkeypatch.setattr(pr, "cname_chain", lambda host: ["cdn.brandnewvendor.io"])
    monkeypatch.setattr(pr, "_http_probe", lambda url, **k: _probe("<html>" + "q" * 3000 + "</html>"))

    before = pr.classify_dealer("https://www.newco.com",
                                registry=pr.load_registry(reg_json),
                                unclassified_path=tmp_path / "u.json")
    assert before["platform"] is None

    # Human adds one line naming the platform by its CNAME.
    reg_json.write_text(json.dumps({"platforms": [{
        "name": "brandnew", "cname_patterns": ["brandnewvendor.io"], "strategy": "browser",
    }]}))
    after = pr.classify_dealer("https://www.newco.com",
                               registry=pr.load_registry(reg_json), do_http=False)
    assert after["platform"] == "brandnew"
    assert after["source"] == "dns"


# ── cname_chain: dig -> nslookup fallback (subprocess mocked) ──────────────────


def test_cname_chain_parses_dig(monkeypatch):
    class _R:
        stdout = "client-x.jazelc.com.\njazel-cdn.com.\n104.18.17.117\n"

    monkeypatch.setattr(pr.subprocess, "run", lambda *a, **k: _R())
    assert pr.cname_chain("www.sandersonford.com") == ["client-x.jazelc.com", "jazel-cdn.com"]


def test_cname_chain_falls_back_to_nslookup(monkeypatch):
    calls = {"n": 0}

    class _Empty:
        stdout = ""

    class _NsOut:
        stdout = "www.x.com\tcanonical name = client-x.jazelc.com.\n"

    def fake_run(cmd, **k):
        calls["n"] += 1
        return _Empty() if cmd[0] == "dig" else _NsOut()

    monkeypatch.setattr(pr.subprocess, "run", fake_run)
    assert pr.cname_chain("www.x.com") == ["client-x.jazelc.com"]
    assert calls["n"] == 2  # dig (empty) then nslookup


def test_cname_chain_handles_missing_tools(monkeypatch):
    def boom(*a, **k):
        raise OSError("no such tool")

    monkeypatch.setattr(pr.subprocess, "run", boom)
    assert pr.cname_chain("www.x.com") == []  # never raises
