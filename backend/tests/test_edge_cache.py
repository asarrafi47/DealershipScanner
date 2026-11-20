"""Edge-cacheable anonymous pages (perf 2026-11-20, backend/web/edge_cache.py).

Cloudflare ignores ``Vary: Cookie``, so these pin the origin's own guarantees:

* ``/`` and ``/find-dealers`` for a request with no app cookie: no Set-Cookie, nothing per
  user in the body (no CSRF token, no nonce value, no username, no inline script), a
  nonce-free CSP, ``Cache-Control: public ... s-maxage``, a weak ETag and 304s.
* The same pages for anyone carrying an app cookie (anonymous with a session, user A,
  user B): ``private, no-store``, no ETag; a signed-in visitor to ``/`` gets the private
  302 to ``/home``.
* The CSRF token is lazy: pages that render it (login, register, compare) still mint it,
  set the session cookie and accept their form posts; pages that do not, never write the
  session.
"""

from __future__ import annotations

import re

import pytest

from backend.tests.test_home_dashboard_routes import _insert_test_car

CACHEABLE = ("/", "/find-dealers")
PASSWORD = "long-enough-password"
_TOKEN_RE = re.compile(r'name="csrf_token"[^>]*value="([^"]+)"')


@pytest.fixture
def app(app_factory, monkeypatch, tmp_path):
    inv = str(tmp_path / "inventory.db")
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    from backend.db import inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db, save_user

    monkeypatch.setattr(inventory_db, "DB_PATH", inv)
    a = app_factory(
        INVENTORY_DB_PATH=inv, BILLING_STRIPE_ENABLED="0", CSP_ENFORCE="1", CSP_REPORT_ONLY=""
    ).app
    init_users_db()
    init_inventory_db()
    a.config["TESTING"] = True
    _insert_test_car()
    from backend.db.repositories import listings_repo

    listings_repo.clear_inventory_listings_cache()
    save_user("alice_edge", "alice@example.com", PASSWORD, role="general", org_id=None)
    save_user("bob_edge", "bob@example.com", PASSWORD, role="general", org_id=None)
    return a


def _set_cookies(resp) -> list[str]:
    return resp.headers.getlist("Set-Cookie")


def _login(app, username: str):
    """Sign in through the real /login form (GET mints the token, POST uses it)."""
    c = app.test_client()
    page = c.get("/login")
    assert page.status_code == 200
    assert any(h.startswith("session=") for h in _set_cookies(page)), "login must set the CSRF cookie"
    token = _TOKEN_RE.search(page.get_data(as_text=True)).group(1)
    r = c.post("/login", data={"csrf_token": token, "login": username, "password": PASSWORD})
    assert r.status_code == 302, r.get_data(as_text=True)[:300]
    assert r.headers["Location"].endswith("/home")
    return c


def _assert_public(resp):
    cc = resp.headers.get("Cache-Control", "")
    assert cc.startswith("public"), cc
    assert "s-maxage=" in cc and "max-age=0" in cc and "stale-while-revalidate=" in cc
    assert "private" not in cc and "no-store" not in cc
    assert resp.headers.get("ETag", "").startswith('W/"')
    vary = {v.strip().lower() for v in resp.headers.get("Vary", "").split(",")}
    assert {"cookie", "accept-encoding"} <= vary
    assert not _set_cookies(resp)


def _assert_private(resp):
    cc = resp.headers.get("Cache-Control", "")
    assert "private" in cc and "no-store" in cc, cc
    assert "public" not in cc and "s-maxage" not in cc
    assert "ETag" not in resp.headers
    assert "cookie" in resp.headers.get("Vary", "").lower()


def _assert_nothing_per_user(body: str, *usernames: str):
    assert 'name="csrf-token"' not in body
    assert 'name="csrf_token"' not in body
    assert not re.search(r'nonce="[^"]+"', body), "a cached page must not carry a nonce value"
    # Every <script> is external (src=); an inline script would need a nonce.
    for tag in re.findall(r"<script\b[^>]*>", body):
        assert "src=" in tag or 'type="application/json"' in tag, tag
    for name in usernames:
        assert name not in body


@pytest.mark.parametrize("path", CACHEABLE)
def test_anonymous_response_is_public_and_carries_nothing_per_user(app, path):
    c = app.test_client()
    r = c.get(path, headers={"Accept-Encoding": "gzip"})
    assert r.status_code == 200
    _assert_public(r)
    csp = r.headers["Content-Security-Policy"]
    assert "nonce-" not in csp
    assert "script-src 'self';" in csp
    assert "object-src 'none'" in csp and "frame-ancestors 'none'" in csp
    body = c.get(path).get_data(as_text=True)
    _assert_nothing_per_user(body, "alice_edge", "bob_edge")
    # The test client keeps cookies: nothing was set, so nothing is stored.
    assert c.get_cookie("session") is None


def test_landing_loads_its_script_from_self(app):
    body = app.test_client().get("/").get_data(as_text=True)
    assert re.search(r'<script src="/static/landing\.js\?v=[^"]+" defer>', body)


@pytest.mark.parametrize("path", CACHEABLE)
def test_etag_revalidates_with_304(app, path):
    c = app.test_client()
    first = c.get(path)
    etag = first.headers["ETag"]
    again = c.get(path)
    assert again.headers["ETag"] == etag  # same anonymous body every time
    r = c.get(path, headers={"If-None-Match": etag})
    assert r.status_code == 304
    assert r.data == b""
    assert r.headers["ETag"] == etag
    assert r.headers["Cache-Control"].startswith("public")
    assert not _set_cookies(r)
    r2 = c.get(path, headers={"If-None-Match": 'W/"something-else"'})
    assert r2.status_code == 200


@pytest.mark.parametrize("path", CACHEABLE)
def test_any_app_cookie_makes_the_page_private(app, path):
    c = app.test_client()
    c.get("/login")  # anonymous session holding only the CSRF token
    assert c.get_cookie("session") is not None
    r = c.get(path)
    assert r.status_code == 200
    _assert_private(r)
    assert "nonce-" not in r.headers["Content-Security-Policy"]
    # An untouched, non-permanent session is not re-sent (SESSION_REFRESH_EACH_REQUEST
    # refreshes permanent sessions only).
    assert not _set_cookies(r)

    d = app.test_client()
    d.set_cookie("sc_dev", "x")
    _assert_private(d.get(path))


def test_foreign_cookie_alone_stays_cacheable(app):
    c = app.test_client()
    c.set_cookie("__cf_bm", "cloudflare-bot-cookie")
    _assert_public(c.get("/"))


def test_signed_in_users_get_a_private_redirect_off_the_landing(app):
    anon = app.test_client().get("/")
    a = _login(app, "alice_edge")
    b = _login(app, "bob_edge")
    for client in (a, b):
        r = client.get("/")
        assert r.status_code == 302 and r.headers["Location"].endswith("/home")
        _assert_private(r)
        assert r.headers.get("ETag") != anon.headers["ETag"]


def test_signed_in_find_dealers_is_private_and_anonymous_copy_has_no_names(app):
    anon = app.test_client().get("/find-dealers")
    _assert_public(anon)
    seen = {}
    for name in ("alice_edge", "bob_edge"):
        client = _login(app, name)
        r = client.get("/find-dealers")
        assert r.status_code == 200
        _assert_private(r)
        seen[name] = r.get_data(as_text=True)
    assert "alice_edge" in seen["alice_edge"] and "bob_edge" in seen["bob_edge"]
    _assert_nothing_per_user(anon.get_data(as_text=True), "alice_edge", "bob_edge")


def test_head_request_matches_get(app):
    r = app.test_client().head("/")
    assert r.status_code == 200
    _assert_public(r)


# ---------------------------------------------------------------- lazy CSRF


@pytest.mark.parametrize("path", ["/login", "/register", "/compare"])
def test_pages_that_render_the_token_still_mint_it(app, path):
    c = app.test_client()
    r = c.get(path)
    assert r.status_code == 200
    assert any(h.startswith("session=") for h in _set_cookies(r))
    body = r.get_data(as_text=True)
    token = (_TOKEN_RE.search(body) or re.search(r'name="csrf-token" content="([^"]+)"', body)).group(1)
    with c.session_transaction() as sess:
        assert sess["_csrf_token"] == token
    assert "public" not in r.headers.get("Cache-Control", "")


def test_register_form_still_posts(app):
    c = app.test_client()
    token = _TOKEN_RE.search(c.get("/register").get_data(as_text=True)).group(1)
    r = c.post(
        "/register",
        data={"csrf_token": token, "username": "carol_edge", "email": "carol@example.com",
              "password": PASSWORD, "plan": "free"},
    )
    assert r.status_code == 302
    bad = app.test_client().post(
        "/login", data={"csrf_token": "nope", "login": "alice_edge", "password": PASSWORD}
    )
    assert bad.status_code == 302 and "session_expired" in bad.headers["Location"]


def test_lazy_token_mints_once_and_only_when_rendered(app):
    from flask import render_template_string, session

    with app.test_request_context("/"):
        render_template_string("<p>no token here</p>")
        assert "_csrf_token" not in session
        assert not session.modified
    with app.test_request_context("/"):
        out = render_template_string("{{ csrf_token }}|{{ csrf_token }}")
        first, second = out.split("|")
        assert first == second == session["_csrf_token"]
        assert len(first) >= 32


def test_session_guard_downgrades_a_public_response_that_set_a_cookie(app):
    from flask import make_response, session

    from backend.web.edge_cache import PUBLIC_CACHE_CONTROL

    @app.route("/_edge_guard_probe")
    def _edge_guard_probe():
        session["oops"] = 1
        resp = make_response("x")
        resp.headers["Cache-Control"] = PUBLIC_CACHE_CONTROL
        resp.set_etag("abc", weak=True)
        return resp

    r = app.test_client().get("/_edge_guard_probe")
    assert _set_cookies(r)
    assert r.headers["Cache-Control"] == "private, no-store"
    assert "ETag" not in r.headers


# ---------------------------------------------------------------- landing data


def test_public_count_reads_the_fingerprint_once_per_floor(monkeypatch):
    from backend.db.repositories import listings_repo

    reads = {"n": 0}
    now = {"t": 1_000_000.0}

    def fp():
        reads["n"] += 1
        return (("cars", 1),)

    monkeypatch.setattr(listings_repo, "_pg_grid_write_fingerprint", fp)
    monkeypatch.setattr(listings_repo, "_count_public_listings_uncached", lambda: 4242)
    monkeypatch.setattr(listings_repo, "_public_count_cache", None)
    monkeypatch.setattr(listings_repo.time, "time", lambda: now["t"])
    assert listings_repo.public_listings_count() == 4242
    assert reads["n"] == 1
    for _ in range(5):
        assert listings_repo.public_listings_count() == 4242
    assert reads["n"] == 1  # within the floor: no stats read
    now["t"] += listings_repo._PUBLIC_COUNT_MIN_RECOUNT_S
    listings_repo.public_listings_count()
    assert reads["n"] == 2
    listings_repo.public_listings_count()
    assert reads["n"] == 2
