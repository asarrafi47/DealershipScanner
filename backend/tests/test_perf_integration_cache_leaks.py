"""Cache-leak guarantees across the four perf PRs together (integration of #33 to #36).

Each PR tests its own layer; these pin the seams between them on the real app:

* #33's Server-Timing middleware (outermost WSGI layer) never stamps a response that #34
  marks shared-cacheable (``/``, ``/find-dealers``, the anonymous facets payload of #35),
  so an edge cache never replays one request's timings, while private pages still get it.
* #34's anonymous pages stay cookieless and public with #36's Speculation-Rules header
  on them, and two different anonymous clients get byte-identical copies.
* Two different sessions never get a shareable response, and #36's Speculation-Rules
  header never rides on the signed-in redirect off ``/``.
* Signed-in pages are never marked shareable: ``/home`` sends no public/s-maxage (on dev it
  sends no Cache-Control at all, and the Cloudflare rule only matches ``/`` and
  ``/find-dealers`` without app cookies), and its ``?part=reco`` stays ``private, no-store``.
"""

# ruff: noqa: F811  (fixture import reused as test parameters, see edge_app below)

from __future__ import annotations

import pytest

from backend.tests.test_edge_cache import (
    _assert_nothing_per_user,
    _assert_private,
    _assert_public,
    _login,
)

# test_edge_cache.py's ``app`` fixture (SQLite inventory with one car, users alice_edge and
# bob_edge), reused here as the fixture ``edge_app``. Pytest injects a fixture through a
# parameter of the same name, which ruff reads as a redefinition (F811) of this import.
from backend.tests.test_edge_cache import app as edge_app  # noqa: F401

CACHEABLE = ("/", "/find-dealers")


@pytest.fixture(autouse=True)
def _fresh_facets_payload(monkeypatch):
    """Server-Timing is on by default; the facets body is process-cached, so reset it."""
    monkeypatch.delenv("SERVER_TIMING", raising=False)
    from backend.listings.facets_payload import reset_facets_payload

    reset_facets_payload()
    yield
    reset_facets_payload()


@pytest.mark.parametrize("path", CACHEABLE)
def test_anonymous_edge_pages_are_identical_cookieless_and_untimed(edge_app, path):
    one = edge_app.test_client().get(path)
    two = edge_app.test_client().get(path)
    for r in (one, two):
        assert r.status_code == 200
        _assert_public(r)
        assert "Server-Timing" not in r.headers, "a shared copy must not carry one request's timings"
        assert r.headers.get("Speculation-Rules", "").startswith('"/static/speculation-rules.json')
        _assert_nothing_per_user(r.get_data(as_text=True), "alice_edge", "bob_edge")
    assert one.get_data() == two.get_data()
    assert one.headers["ETag"] == two.headers["ETag"]


def test_two_sessions_never_get_a_shareable_copy(edge_app):
    anon = edge_app.test_client().get("/find-dealers")
    bodies = {}
    for name in ("alice_edge", "bob_edge"):
        c = _login(edge_app, name)
        r = c.get("/find-dealers")
        assert r.status_code == 200
        _assert_private(r)
        # Private responses may carry timings; this proves the middleware is live here.
        assert "Server-Timing" in r.headers
        bodies[name] = r.get_data()
    assert bodies["alice_edge"] != bodies["bob_edge"]
    assert anon.get_data() not in bodies.values()


def test_signed_in_redirect_off_the_landing_carries_no_speculation_rules(edge_app):
    c = _login(edge_app, "alice_edge")
    r = c.get("/")
    assert r.status_code == 302 and r.headers["Location"].endswith("/home")
    _assert_private(r)
    assert "Speculation-Rules" not in r.headers


def test_signed_in_home_is_never_shareable(edge_app):
    c = _login(edge_app, "bob_edge")
    for url in ("/home", "/home?part=reco"):
        r = c.get(url)
        assert r.status_code in (200, 204), (url, r.status_code)
        cc = r.headers.get("Cache-Control", "")
        assert "public" not in cc and "s-maxage" not in cc, (url, cc)
        if "part=reco" in url:
            assert "private" in cc and "no-store" in cc, cc


def test_anonymous_facets_payload_is_cookieless_and_untimed(edge_app):
    r = edge_app.test_client().get("/api/listings/facets")
    assert r.status_code == 200
    assert "Set-Cookie" not in r.headers
    assert r.headers["Cache-Control"].startswith("public")
    assert "Server-Timing" not in r.headers
    # A signed-in caller gets the same bytes and is never told "public" (its session
    # refresh may set a cookie, which is fine on a private response).
    c = _login(edge_app, "alice_edge")
    s = c.get("/api/listings/facets")
    assert s.status_code == 200
    assert s.get_data() == r.get_data()
    cc = s.headers["Cache-Control"]
    assert cc.startswith("private") and "public" not in cc and "s-maxage" not in cc, cc
