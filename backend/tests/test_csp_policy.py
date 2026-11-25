"""CSP sources: esm.sh is gone from both policies (P3.4, security-2, 2026-10-18).

Nothing in ``frontend/`` loads from https://esm.sh, so allowing it in
``script-src`` and ``connect-src`` only widened what an injected script could
pull in. These tests pin the exact script/connect sources of the enforced and
report-only policies, check the headers a real response carries, and check that
no template or static script needs a cross-origin script source.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"


def _directives(policy: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for part in policy.split(";"):
        tokens = part.split()
        if tokens:
            out[tokens[0]] = tokens[1:]
    return out


def test_policy_strings_have_no_esm_sh():
    from backend.web import security

    for value in (security._csp_header_value_enforced("n0nce"), security._CSP_REPORT_ONLY):
        assert "esm.sh" not in value


def test_enforced_policy_script_and_connect_sources_are_exact():
    from backend.web import security

    d = _directives(security._csp_header_value_enforced("n0nce"))
    assert d["script-src"] == ["'self'", "'nonce-n0nce'"]
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    assert d["default-src"] == ["'self'"]


def test_report_only_policy_script_and_connect_sources_are_exact():
    from backend.web import security

    d = _directives(security._CSP_REPORT_ONLY)
    assert d["script-src"] == ["'self'"]
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    assert d["default-src"] == ["'self'"]


def test_enforced_header_on_a_response_has_no_esm_sh(app_factory):
    app = app_factory(CSP_ENFORCE="1", CSP_REPORT_ONLY="").app
    with app.test_client() as c:
        rv = c.get("/login")
    assert rv.status_code == 200
    csp = rv.headers.get("Content-Security-Policy", "")
    assert csp, "CSP_ENFORCE=1 must send the enforced header"
    assert "esm.sh" not in csp
    d = _directives(csp)
    assert len(d["script-src"]) == 2 and d["script-src"][0] == "'self'"
    assert d["script-src"][1].startswith("'nonce-")
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    # The page's script tags carry the same nonce, so 'self' + nonce is enough.
    nonce = d["script-src"][1][len("'nonce-") : -1]
    body = rv.get_data(as_text=True)
    assert f'nonce="{nonce}"' in body


def test_report_only_header_on_a_response_has_no_esm_sh(app_factory):
    app = app_factory(CSP_ENFORCE="0", CSP_REPORT_ONLY="1").app
    with app.test_client() as c:
        rv = c.get("/login")
    assert rv.status_code == 200
    assert "Content-Security-Policy" not in rv.headers
    ro = rv.headers.get("Content-Security-Policy-Report-Only", "")
    assert ro, "CSP_REPORT_ONLY=1 must send the report-only header"
    assert "esm.sh" not in ro
    assert _directives(ro)["script-src"] == ["'self'"]


def test_no_frontend_file_references_esm_sh():
    hits = [
        str(p.relative_to(FRONTEND))
        for p in FRONTEND.rglob("*")
        if p.is_file()
        and p.suffix in {".html", ".js", ".mjs", ".css", ".json"}
        and "esm.sh" in p.read_text(encoding="utf-8", errors="ignore")
    ]
    assert hits == []


_CROSS_ORIGIN_SCRIPT_TAG = re.compile(r"<script[^>]*\ssrc=[\"']?(?:https?:)?//", re.IGNORECASE)
_CROSS_ORIGIN_IMPORT = re.compile(
    r"""(?:\bimport\s*\(\s*|\bfrom\s+|\bimport\s+|importScripts\s*\(\s*)["'](?:https?:)?//""",
)


def test_turnstile_origin_only_on_its_pages():
    from backend.web import security

    assert security.TURNSTILE_PAGE_ENDPOINTS == frozenset({"dealer_portal.dealer_register"})
    d = _directives(security._csp_header_value_enforced("n0nce", turnstile=True))
    assert d["script-src"] == ["'self'", "'nonce-n0nce'", "https://challenges.cloudflare.com"]
    assert d["frame-src"] == ["'self'", "https://challenges.cloudflare.com"]
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"]
    assert "challenges.cloudflare.com" not in security._csp_header_value_enforced("n0nce")


def test_no_template_or_static_script_loads_a_cross_origin_script():
    """script-src is 'self' (+ nonce): a cross-origin script would be blocked."""
    # Card S19: the one allowed cross-origin script is Cloudflare Turnstile on the dealer sign-up
    # page; security.TURNSTILE_PAGE_ENDPOINTS adds its origin to that page's policy only.
    turnstile_tag = '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js"'
    # Card S8: the two phone sign-in pages carry the same widget; they send the origin only while
    # AUTH_PHONE_LOGIN_ENABLED is on (see test_turnstile_origin_on_phone_pages_only_with_the_flag).
    allowed = {
        "dealer_register.html": turnstile_tag,
        "login_phone.html": turnstile_tag,
        "login_phone_code.html": turnstile_tag,
    }
    tag_hits = []
    for p in (FRONTEND / "templates").rglob("*.html"):
        text = p.read_text(encoding="utf-8", errors="ignore")
        rel = str(p.relative_to(FRONTEND / "templates"))
        if rel in allowed:
            text = text.replace(allowed[rel], "")
        if _CROSS_ORIGIN_SCRIPT_TAG.search(text):
            tag_hits.append(rel)
    import_hits = [
        str(p.relative_to(FRONTEND))
        for p in (FRONTEND / "static").rglob("*")
        if p.is_file()
        and p.suffix in {".js", ".mjs"}
        and _CROSS_ORIGIN_IMPORT.search(p.read_text(encoding="utf-8", errors="ignore"))
    ]
    assert tag_hits == []
    assert import_hits == []


def test_turnstile_origin_on_phone_pages_only_with_the_flag(app_factory):
    """Card S8: flag on, the phone pages (and /login, which is the phone page then) allow the
    Turnstile origin; flag off, /login and /staff/login keep the plain policy."""
    on = app_factory(
        CSP_ENFORCE="1",
        CSP_REPORT_ONLY="",
        AUTH_PHONE_LOGIN_ENABLED="1",
        TURNSTILE_SITE_KEY="site-key-1",
    ).app
    with on.test_client() as c:
        for path in ("/login", "/login/phone"):
            rv = c.get(path)
            assert rv.status_code == 200, path
            d = _directives(rv.headers["Content-Security-Policy"])
            assert d["script-src"][-1] == "https://challenges.cloudflare.com", path
            assert d["frame-src"] == ["'self'", "https://challenges.cloudflare.com"], path
            assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"], path
        # the staff password page is a normal page: no Turnstile origin
        rv = c.get("/staff/login")
        assert rv.status_code == 200
        assert "challenges.cloudflare.com" not in rv.headers["Content-Security-Policy"]


def test_turnstile_origin_is_not_on_login_while_the_phone_flag_is_off(app_factory):
    off = app_factory(
        CSP_ENFORCE="1", CSP_REPORT_ONLY="", AUTH_PHONE_LOGIN_ENABLED="", TURNSTILE_SITE_KEY="site-key-1"
    ).app
    with off.test_client() as c:
        rv = c.get("/login")
        assert rv.status_code == 200
        assert "challenges.cloudflare.com" not in rv.headers["Content-Security-Policy"]
        assert c.get("/login/phone").status_code == 404
        assert c.get("/staff/login").status_code == 404


# ---------------------------------------------------------------- every phone page, not just the first one

TURNSTILE_ORIGIN = "https://challenges.cloudflare.com"
_TOKEN = "t" * 40


class _PhoneStub:
    """Just enough of ``AuthPhoneService`` to reach each phone page: a start that opens a challenge
    (or refuses an unticked box), a check that is always a wrong code and a resend that is too soon."""

    def start(self, raw_phone, *, consent, session_nonce=None, **_kw):
        from backend.auth.phone import service as ps

        if consent is not True:
            return ps.StartOutcome(False, ps.R_CONSENT, ps.CONSENT_REQUIRED)
        return ps.StartOutcome(
            True, ps.R_SENT, ps.SENT_TEMPLATE.format(masked="ending 23"), public_id="cid-1", session_nonce=session_nonce
        )

    def check(self, *_a, **_kw):
        from backend.auth.phone import service as ps

        return ps.CheckOutcome(False, ps.R_WRONG, ps.CODE_FAILED)

    def resend(self, *_a, **_kw):
        from backend.auth.phone import service as ps

        return ps.StartOutcome(False, ps.R_TOO_SOON, ps.RESEND_TOO_SOON)


@pytest.fixture
def phone_stub():
    from backend.auth.phone import registry, routes
    from backend.utils.ip_rate_limit import clear_rate_limit_state

    clear_rate_limit_state()  # the per-address phone bucket must not carry over from another test
    routes.set_service(_PhoneStub())
    yield
    routes.reset_service()
    registry.reset()
    clear_rate_limit_state()


def _post_form(client, url, data=None):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _TOKEN
    return client.post(url, data={"csrf_token": _TOKEN, **(data or {})})


def _assert_turnstile_allowed(rv, what: str) -> None:
    d = _directives(rv.headers["Content-Security-Policy"])
    assert d["script-src"][-1] == TURNSTILE_ORIGIN, what
    assert d["frame-src"] == ["'self'", TURNSTILE_ORIGIN], what
    assert d["connect-src"] == ["'self'", "https://tile.openstreetmap.org"], what


def test_turnstile_origin_is_on_every_phone_page_that_shows_the_widget(app_factory, phone_stub):
    """The widget sits on the number page and on the code page, and each page can be rendered in
    more than one way (first visit, refused start, wrong code, resend refused). Every one of them
    must send the origin: a page without it loads the widget as a blocked script and a blank frame,
    and the person can never solve the check."""
    app = app_factory(
        CSP_ENFORCE="1",
        CSP_REPORT_ONLY="",
        AUTH_PHONE_LOGIN_ENABLED="1",
        TURNSTILE_SITE_KEY="site-key-1",
    ).app
    with app.test_client() as c:
        # the number page, refused start (box not ticked): _render_phone_page after a POST
        rv = _post_form(c, "/login/phone", {"phone": "(202) 555-0123", "cf-turnstile-response": "pass"})
        assert rv.status_code == 200 and 'id="ph-start-form"' in rv.get_data(as_text=True)
        _assert_turnstile_allowed(rv, "number page after a refused start")

        # a started challenge: the code page, first view (the open challenge is in the session)
        rv = _post_form(
            c, "/login/phone", {"phone": "(202) 555-0123", "consent": "1", "cf-turnstile-response": "pass"}
        )
        assert rv.status_code == 303 and rv.headers["Location"].endswith("/login/phone/code")
        rv = c.get("/login/phone/code")
        assert rv.status_code == 200 and 'id="ph-code-form"' in rv.get_data(as_text=True)
        _assert_turnstile_allowed(rv, "code page with an open challenge")

        # the failed-code page: wrong code, the code page again with the service's sentence
        rv = _post_form(c, "/login/phone/code", {"code": "111111"})
        html = rv.get_data(as_text=True)
        assert rv.status_code == 200 and 'id="ph-code-form"' in html and "That code did not work" in html
        _assert_turnstile_allowed(rv, "code page after a wrong code")

        # the resend-failure page: too soon, the code page again (HTTP 429)
        rv = _post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
        html = rv.get_data(as_text=True)
        assert rv.status_code == 429 and 'id="ph-code-form"' in html and "wait a minute" in html
        _assert_turnstile_allowed(rv, "code page after a refused resend")

        # the staff password page stays a normal page
        rv = c.get("/staff/login")
        assert rv.status_code == 200
        assert TURNSTILE_ORIGIN not in rv.headers["Content-Security-Policy"]


def test_no_phone_page_sends_the_turnstile_origin_while_the_flag_is_off(app_factory, phone_stub):
    off = app_factory(
        CSP_ENFORCE="1", CSP_REPORT_ONLY="", AUTH_PHONE_LOGIN_ENABLED="", TURNSTILE_SITE_KEY="site-key-1"
    ).app
    with off.test_client() as c:
        for path in ("/login", "/login/phone", "/login/phone/code", "/staff/login"):
            rv = c.get(path)
            assert TURNSTILE_ORIGIN not in rv.headers.get("Content-Security-Policy", ""), path
            assert TURNSTILE_ORIGIN not in rv.get_data(as_text=True), path
        assert [c.get(p).status_code for p in ("/login/phone", "/login/phone/code", "/staff/login")] == [404] * 3
        # a form post is refused with the same 404 and no origin either
        rv = _post_form(c, "/login/phone", {"phone": "(202) 555-0123", "consent": "1"})
        assert rv.status_code == 404
        assert TURNSTILE_ORIGIN not in rv.headers.get("Content-Security-Policy", "")


@pytest.mark.parametrize("page", ["/login/phone", "/login/phone/code"])
def test_phone_pages_with_a_site_key_tell_a_script_less_browser_the_check_needs_javascript(
    app_factory, phone_stub, page
):
    """Without JavaScript Cloudflare's widget cannot appear. The <noscript> note says so, instead of
    leaving a form that can never pass the person check (the "did not load" notice needs the script)."""
    app = app_factory(AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY="site-key-1").app
    with app.test_client() as c:
        _post_form(c, "/login/phone", {"phone": "(202) 555-0123", "consent": "1", "cf-turnstile-response": "pass"})
        html = c.get(page).get_data(as_text=True)
    assert re.search(r"<noscript>[^<]*<p[^>]*>[^<]*needs JavaScript[^<]*</p></noscript>", html), page
    # the note does not read the same on the page that has no widget at all
    no_key = app_factory(AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY=None).app
    with no_key.test_client() as c:
        assert "needs JavaScript" not in c.get("/login/phone").get_data(as_text=True)
