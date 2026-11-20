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

from __future__ import annotations

import pytest

from backend.tests.test_edge_cache import (
    PASSWORD,
    _assert_nothing_per_user,
    _assert_private,
    _assert_public,
    _login,
)
from backend.tests.test_home_dashboard_routes import _insert_test_car

CACHEABLE = ("/", "/find-dealers")


@pytest.fixture
def app(app_factory, monkeypatch, tmp_path):
    """Same app as test_edge_cache.py: SQLite inventory with one car, two shoppers."""
    inv = str(tmp_path / "inventory.db")
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    from backend.db import inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db, save_user

    monkeypatch.setattr(inventory_db, "DB_PATH", inv)
    monkeypatch.setenv("SERVER_TIMING", "1")
    a = app_factory(
        INVENTORY_DB_PATH=inv, BILLING_STRIPE_ENABLED="0", CSP_ENFORCE="1", CSP_REPORT_ONLY=""
    ).app
    init_users_db()
    init_inventory_db()
    a.config["TESTING"] = True
    _insert_test_car()
    from backend.db.repositories import listings_repo
    from backend.listings.facets_payload import reset_facets_payload

    listings_repo.clear_inventory_listings_cache()
    reset_facets_payload()
    save_user("alice_edge", "alice@example.com", PASSWORD, role="general", org_id=None)
    save_user("bob_edge", "bob@example.com", PASSWORD, role="general", org_id=None)
    yield a
    reset_facets_payload()


@pytest.fixture(autouse=True)
def _timing_on(monkeypatch):
    monkeypatch.setenv("SERVER_TIMING", "1")


@pytest.mark.parametrize("path", CACHEABLE)
def test_anonymous_edge_pages_are_identical_cookieless_and_untimed(app, path):
    one = app.test_client().get(path)
    two = app.test_client().get(path)
    for r in (one, two):
        assert r.status_code == 200
        _assert_public(r)
        assert "Server-Timing" not in r.headers, "a shared copy must not carry one request's timings"
        assert r.headers.get("Speculation-Rules", "").startswith('"/static/speculation-rules.json')
        _assert_nothing_per_user(r.get_data(as_text=True), "alice_edge", "bob_edge")
    assert one.get_data() == two.get_data()
    assert one.headers["ETag"] == two.headers["ETag"]


def test_two_sessions_never_get_a_shareable_copy(app):
    anon = app.test_client().get("/find-dealers")
    bodies = {}
    for name in ("alice_edge", "bob_edge"):
        c = _login(app, name)
        r = c.get("/find-dealers")
        assert r.status_code == 200
        _assert_private(r)
        # Private responses may carry timings; this proves the middleware is live here.
        assert "Server-Timing" in r.headers
        bodies[name] = r.get_data()
    assert bodies["alice_edge"] != bodies["bob_edge"]
    assert anon.get_data() not in bodies.values()


def test_signed_in_redirect_off_the_landing_carries_no_speculation_rules(app):
    c = _login(app, "alice_edge")
    r = c.get("/")
    assert r.status_code == 302 and r.headers["Location"].endswith("/home")
    _assert_private(r)
    assert "Speculation-Rules" not in r.headers


def test_signed_in_home_is_never_shareable(app):
    c = _login(app, "bob_edge")
    for url in ("/home", "/home?part=reco"):
        r = c.get(url)
        assert r.status_code in (200, 204), (url, r.status_code)
        cc = r.headers.get("Cache-Control", "")
        assert "public" not in cc and "s-maxage" not in cc, (url, cc)
        if "part=reco" in url:
            assert "private" in cc and "no-store" in cc, cc


def test_anonymous_facets_payload_is_cookieless_and_untimed(app):
    r = app.test_client().get("/api/listings/facets")
    assert r.status_code == 200
    assert "Set-Cookie" not in r.headers
    assert r.headers["Cache-Control"].startswith("public")
    assert "Server-Timing" not in r.headers
    # A signed-in caller gets the same bytes and is never told "public" (its session
    # refresh may set a cookie, which is fine on a private response).
    c = _login(app, "alice_edge")
    s = c.get("/api/listings/facets")
    assert s.status_code == 200
    assert s.get_data() == r.get_data()
    cc = s.headers["Cache-Control"]
    assert cc.startswith("private") and "public" not in cc and "s-maxage" not in cc, cc
