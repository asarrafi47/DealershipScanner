"""Speculation rules (instant navigation, 2026-11-20).

Pins what frontend/static/speculation-rules.json may prefetch or prerender, that it is
delivered by a Speculation-Rules header (no inline script, so the CSP keeps its nonce
and no 'inline-speculation-rules'), served with its required MIME type, and that
nav_perf.js's fallback list matches it. A speculative request must never hit an API,
a static file, logout or any URL with server-side effects, and a car page is never
prerendered (its activation would not reach the server to record the view).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "frontend" / "static" / "speculation-rules.json"
NAV_PERF = ROOT / "frontend" / "static" / "nav_perf.js"

NOSPEC_GUARD = ":root[data-ds-nospec] *"
# Every side-effect-free document a rule may target, as (pathname, search) patterns.
ALLOWED = {
    ("/", ""),
    ("/listings", ""),
    ("/car/:id(\\d+)", ""),
    ("/car/:id(\\d+)", "zip_code=:zip([^&]*)"),
    ("/find-dealers", ""),
}


def _rules() -> dict:
    return json.loads(RULES_PATH.read_text())


def _walk(cond, key):
    """Every value of ``key`` anywhere inside a where-condition."""
    if isinstance(cond, dict):
        for k, v in cond.items():
            if k == key:
                yield v
            else:
                yield from _walk(v, key)
    elif isinstance(cond, list):
        for item in cond:
            yield from _walk(item, key)


def _href_patterns(rule) -> list[dict]:
    out = []
    for v in _walk(rule["where"], "href_matches"):
        out.extend(v if isinstance(v, list) else [v])
    return out


def _all_rules():
    r = _rules()
    for action in ("prefetch", "prerender"):
        for rule in r.get(action, []):
            yield action, rule


def test_only_known_actions_and_document_rules():
    r = _rules()
    assert set(r) <= {"prefetch", "prerender"}
    for _action, rule in _all_rules():
        assert rule["source"] == "document"
        assert rule["eagerness"] in ("moderate", "immediate", "conservative")


def test_every_target_is_an_allowed_side_effect_free_document():
    for action, rule in _all_rules():
        pats = _href_patterns(rule)
        assert pats, (action, rule)
        for p in pats:
            # Object form with an explicit search: a bare string pattern would let
            # /listings match /listings?q=... (search history, analytics, session).
            assert isinstance(p, dict), p
            assert set(p) == {"pathname", "search"}, p
            assert (p["pathname"], p["search"]) in ALLOWED, p
            path = p["pathname"]
            assert not path.startswith(("/api", "/static")), p
            assert "logout" not in path and "logout" not in p["search"], p


def test_prerender_is_only_bare_listings_and_never_a_car_page():
    r = _rules()
    for rule in r["prerender"]:
        pats = _href_patterns(rule)
        assert pats == [{"pathname": "/listings", "search": ""}]
        assert "a[data-ds-prerender]" in list(_walk(rule["where"], "selector_matches"))
        assert "/car" not in json.dumps(rule)


def test_every_rule_is_switched_off_by_the_nospec_guard_and_honours_link_opt_outs():
    for _action, rule in _all_rules():
        negated = [c["not"]["selector_matches"] for c in rule["where"]["and"] if "not" in c]
        assert negated, rule
        sel = negated[0]
        for part in (NOSPEC_GUARD, "[download]", "[data-no-prefetch]", "[rel~=nofollow]"):
            assert part in sel


def test_immediate_rule_only_takes_cars_nav_perf_marks():
    for action, rule in _all_rules():
        if rule["eagerness"] != "immediate":
            continue
        assert action == "prefetch"
        assert "a[data-ds-warm]" in list(_walk(rule["where"], "selector_matches"))
        assert all(p["pathname"].startswith("/car/") for p in _href_patterns(rule))


def test_prerender_markers_only_on_bare_listings_links():
    for tpl in (ROOT / "frontend" / "templates").rglob("*.html"):
        for m in re.finditer(r"<a\b[^>]*\bdata-ds-prerender\b[^>]*>", tpl.read_text(), re.S):
            tag = m.group(0)
            assert "href=\"{{ url_for('listings') }}\"" in tag or 'href="/listings"' in tag, (tpl, tag)


def test_nav_perf_fallback_list_matches_the_rules():
    js = NAV_PERF.read_text()
    m = re.search(r"var ALLOWED_DOC = /(.+)/;", js)
    assert m
    rx = re.compile(m.group(1).replace("\\/", "/"))
    for ok in ("/", "/listings", "/find-dealers", "/car/150", "/car/150?zip_code=28202"):
        assert rx.match(ok), ok
    for bad in ("/listings?q=bmw", "/car/150?zip_code=1&x=2", "/api/cars/1", "/static/x.css",
                "/logout", "/compare?ids=1,2", "/premium", "/home", "/dashboard", "/car/abc"):
        assert not rx.match(bad), bad
    # Where speculation rules exist, nav_perf.js never fetch()es a document itself.
    assert 'HTMLScriptElement.supports("speculationrules")' in js
    assert "if (SPEC_RULES) return;" in js


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    from backend.main import app

    return app.test_client()


def test_html_documents_carry_the_header_and_nothing_else_does(client):
    r = client.get("/login")
    assert r.status_code == 200
    val = r.headers.get("Speculation-Rules", "")
    assert re.fullmatch(r'"/static/speculation-rules\.json\?v=\w+"', val), val
    # No inline rule set: nothing that would need a nonce or 'inline-speculation-rules'.
    assert 'type="speculationrules"' not in r.data.decode()
    csp = r.headers.get("Content-Security-Policy", "")
    assert "inline-speculation-rules" not in csp

    assert "Speculation-Rules" not in client.head("/login").headers
    assert "Speculation-Rules" not in client.get("/health").headers
    assert "Speculation-Rules" not in client.get("/static/nav_perf.js?v=1").headers


def test_rule_file_is_served_as_speculationrules_json(client):
    r = client.get("/static/speculation-rules.json?v=1")
    assert r.status_code == 200
    assert r.mimetype == "application/speculationrules+json"
    assert "immutable" in r.headers["Cache-Control"]
    assert json.loads(r.data) == _rules()
