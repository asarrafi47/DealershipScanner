"""Phone sign-in pages and JSON twins (card S8) and the minimal switch of /login (card S9).

Offline tier: the routes are driven through a stub service (``routes.set_service``) that returns
the service's own outcome objects, so these tests cover the routes, the templates, the session and
the account handling without Postgres. The real service end to end (fake provider, fake Turnstile,
a new number signing up and signing in again) is the Postgres-tier test ``test_auth_phone_login_e2e.py``.

ELIGIBILITY: who may sign in by phone is decided by ``is_phone_eligible`` on the row that
``get_user_profile`` returns. A stored NULL role is refused too, because ``get_user_profile`` maps a
missing role to ``dealer_staff``, which is not a shopper role; the rows in ``ELIGIBILITY_ROWS`` are
therefore the whole refusal list (a NULL role is the ``dealer_staff`` row).

Test numbers are the fictional +1 202 555 01xx range. No test sends anything anywhere.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import time
import types
from html import unescape
from pathlib import Path

import pytest
from werkzeug.exceptions import NotFound

from backend.auth import pages as _pages
from backend.auth import session as _auth_session
from backend.auth.phone import accounts as _phone_accounts
from backend.auth.phone import consent, registry, routes
from backend.auth.phone import service as ps
from backend.auth.phone.accounts import PhoneAccounts
from backend.auth.phone.config import CONSENT_TEXT_VERSION
from backend.auth.phone.endpoints import PHONE_FLAG_ENDPOINTS
from backend.db import users_db as _users_db

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"
NUMBER = "(202) 555-0123"
E164 = "+12025550123"
TOKEN = "t" * 40
PASSWORD = "long-enough-password"

# the phone routes: (method, path)
FORM_POSTS = ("/login/phone", "/login/phone/code", "/login/phone/resend", "/login/phone/change", "/staff/login")
JSON_POSTS = ("/api/auth/phone/start", "/api/auth/phone/verify", "/api/auth/phone/resend")
GETS = ("/login/phone", "/login/phone/code", "/staff/login")

# The eight paths that do not exist while the flag is off, and every method a client may try on them.
FLAG_PATHS = (
    "/login/phone", "/login/phone/code", "/login/phone/resend", "/login/phone/change", "/staff/login",
    "/api/auth/phone/start", "/api/auth/phone/verify", "/api/auth/phone/resend",
)
ALL_METHODS = ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")

# What a typed number looks like in every place it must never reach.
TYPED_PIECES = (NUMBER, "555-0123", "(202)")
DIGITS = "2025550123"

BOX_WORDS = ("card", "box", "pill", "badge", "chip", "panel", "tag", "banner", "callout", "alert")


# ---------------------------------------------------------------- stub service


class StubService:
    """Stands in for ``AuthPhoneService``: returns what a test configures and records every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.start_outcome = ps.StartOutcome(True, ps.R_SENT, ps.SENT_TEMPLATE.format(masked="ending 23"), public_id="cid-1")
        self.resend_outcome = ps.StartOutcome(True, ps.R_SENT, ps.SENT_TEMPLATE.format(masked="ending 23"), public_id="cid-1")
        self.check_outcome = ps.CheckOutcome(False, ps.R_WRONG, ps.CODE_FAILED)
        self.sign_ins: list[ps.SignInOutcome] = []
        self.raise_on: str | None = None

    def _note(self, name: str, **kw) -> None:
        self.calls.append((name, kw))
        if self.raise_on == name:
            # The text carries a number: it must never reach a log or a response.
            raise RuntimeError(f"driver exploded for {E164}")

    def names(self) -> list[str]:
        return [n for n, _ in self.calls]

    def kwargs(self, name: str) -> dict:
        return [kw for n, kw in self.calls if n == name][-1]

    def start(self, raw_phone, *, consent, turnstile_token, ip, device_hash, session_nonce=None, purpose="login", user_id=None):
        self._note("start", raw=raw_phone, consent=consent, token=turnstile_token, ip=ip, device=device_hash,
                   nonce=session_nonce, purpose=purpose)
        if consent is not True:
            return ps.StartOutcome(False, ps.R_CONSENT, ps.CONSENT_REQUIRED)
        out = self.start_outcome
        return dataclasses.replace(out, session_nonce=session_nonce) if out.ok else out

    def resend(self, public_id, session_nonce, *, turnstile_token, ip, device_hash):
        self._note("resend", cid=public_id, nonce=session_nonce, token=turnstile_token, ip=ip, device=device_hash)
        out = self.resend_outcome
        return dataclasses.replace(out, public_id=public_id, session_nonce=session_nonce) if out.ok else out

    def check(self, public_id, session_nonce, code, *, device_hash, ip=None):
        self._note("check", cid=public_id, nonce=session_nonce, code=code, device=device_hash, ip=ip)
        return self.check_outcome

    def cancel(self, public_id, session_nonce):
        self._note("cancel", cid=public_id, nonce=session_nonce)
        return ps.StartOutcome(False, ps.R_CANCELLED, ps.CANCELLED)

    def sign_in(self, approved, accounts):
        self._note("sign_in", accounts=accounts)
        return self.sign_ins.pop(0) if self.sign_ins else ps.SignInOutcome(False, ps.R_RETRY)


def approved(purpose="login") -> ps.CheckOutcome:
    return ps.CheckOutcome(True, ps.R_APPROVED, "", phone_e164=E164, purpose=purpose)


# ---------------------------------------------------------------- fixtures and helpers


@pytest.fixture(autouse=True)
def _clean_service(monkeypatch):
    routes.reset_service()
    registry.reset()
    monkeypatch.setattr(routes, "_sleep", lambda s: None)
    monkeypatch.setattr(routes, "_pad_sleep", lambda s: None)  # the R7 floor, without the wait
    yield
    routes.reset_service()
    registry.reset()


@pytest.fixture
def stub():
    s = StubService()
    routes.set_service(s)
    return s


@pytest.fixture
def on(app_factory, stub):
    """The app with phone sign-in on, a Turnstile site key and the stub service."""
    return app_factory(AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY="site-key-1", SUPPORT_CONTACT="help@example.com").app


@pytest.fixture
def off(app_factory):
    return app_factory(AUTH_PHONE_LOGIN_ENABLED=None, TURNSTILE_SITE_KEY="site-key-1").app


def post_form(client, url, data=None, *, token=TOKEN, **kw):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = token
    return client.post(url, data={"csrf_token": token, **(data or {})}, **kw)


def post_json(client, url, body=None, *, token=TOKEN, raw=None, **kw):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = token
    headers = {"X-CSRF-Token": token}
    if raw is not None:
        return client.post(url, data=raw, headers=headers, content_type="application/json", **kw)
    return client.post(url, json=body if body is not None else {}, headers=headers, **kw)


START_FORM = {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"}
START_JSON = {"phone": NUMBER, "consent": True, "turnstile_token": "pass"}


def open_challenge(client, cid="cid-1") -> None:
    """An open challenge in the session without a start: no request, so no device cookie and no rate-limit event."""
    with client.session_transaction() as sess:
        sess["phone_cid"] = cid
        sess["phone_nonce"] = "n" * 18
        sess["phone_sent_msg"] = "We sent a 6-digit code to the number ending 23."
        sess["phone_sent_at"] = time.time() - 500


def is_number_page(html: str) -> bool:
    return 'id="ph-start-form"' in html and 'name="phone"' in html and "Text me a code" in html and 'href="/staff/login"' in html


def is_code_page(html: str) -> bool:
    return 'id="ph-code-form"' in html and "Enter your code" in html


def assert_number_page(html: str) -> None:
    """The page that asks for a number, and not the one that asks for a code."""
    assert is_number_page(html), "the number page (form, phone field, button, staff link) is missing"
    assert 'id="ph-code-form"' not in html and "Enter your code" not in html


def assert_code_page(html: str) -> None:
    assert is_code_page(html), "the code page is missing"
    assert 'id="ph-start-form"' not in html and "Text me a code" not in html


def assert_number_absent(rv, client, caplog=None) -> None:
    """The number the person typed is in no log line, response body, header or session cookie.

    A log line is also checked for the other things the feature never logs: a run of 7 or more digits
    and a 64-hex string (a device or number hash).
    """
    places = {
        "body": rv.get_data(as_text=True),
        "Location": rv.headers.get("Location", ""),
        "Set-Cookie": " ".join(rv.headers.getlist("Set-Cookie")),
        "session": json.dumps(session_dump(client), default=str),
    }
    for where, text in places.items():
        for piece in (*TYPED_PIECES, DIGITS, E164):
            assert piece not in text, (where, piece)
    if caplog is None:
        return
    for rec in caplog.records:
        msg = rec.getMessage()
        for piece in (*TYPED_PIECES, E164):
            assert piece not in msg, (rec.name, piece, msg)
        assert DIGITS not in re.sub(r"\D", "", msg), (rec.name, msg)
        assert not re.search(r"\d{7,}", msg), (rec.name, msg)
        assert not re.search(r"\b[0-9a-f]{64}\b", msg), (rec.name, msg)


def norm(html: str) -> str:
    html = re.sub(r'name="csrf_token" value="[^"]+"', 'name="csrf_token" value="X"', html)
    html = re.sub(r'nonce="[^"]+"', 'nonce="N"', html)
    return re.sub(r"\?v=[0-9A-Za-z._-]+", "?v=V", html)


def start_challenge(client, stub):
    """GET the page (gets the sc_dev cookie), then start: the session now holds a challenge."""
    client.get("/login/phone")
    rv = post_form(client, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"})
    assert rv.status_code == 303 and rv.headers["Location"].endswith("/login/phone/code")
    return rv


def make_user(username="phoneuser", email="p-1-abcd1234@phone.invalid", role="general_user", **kw) -> int:
    from backend.db.users_db import save_user

    return int(save_user(username, email, PASSWORD, role=role, **kw))


def session_dump(client) -> dict:
    with client.session_transaction() as sess:
        return dict(sess)


# ---------------------------------------------------------------- flag off: nothing exists


@pytest.mark.parametrize("path", GETS)
def test_get_routes_are_404_with_the_flag_off(off, stub, path):
    with off.test_client() as c:
        assert c.get(path).status_code == 404
    assert stub.calls == []


@pytest.mark.parametrize("path", FORM_POSTS)
@pytest.mark.parametrize("with_token", [False, True])
def test_form_posts_are_404_with_the_flag_off_token_or_not(off, stub, path, with_token):
    """The flag is decided before the CSRF check: no redirect and no 403 reveals the route."""
    with off.test_client() as c:
        rv = post_form(c, path, {"phone": NUMBER, "consent": "1", "code": "000000"}) if with_token else c.post(path, data={"phone": NUMBER})
    assert rv.status_code == 404
    assert stub.calls == []


@pytest.mark.parametrize("path", JSON_POSTS)
@pytest.mark.parametrize("with_token", [False, True])
def test_json_twins_are_404_with_the_flag_off(off, stub, path, with_token):
    with off.test_client() as c:
        rv = post_json(c, path, {"phone": NUMBER, "consent": True}) if with_token else c.post(path, json={"phone": NUMBER})
    assert rv.status_code == 404
    assert (rv.get_json() or {}).get("error") == "not_found"
    assert stub.calls == []


def _phone_rule_paths(app) -> dict[str, set[str]]:
    """Every URL path a phone endpoint is registered on, with the methods it accepts (HEAD and OPTIONS are implicit)."""
    paths: dict[str, set[str]] = {}
    for rule in app.url_map.iter_rules():
        if rule.endpoint in PHONE_FLAG_ENDPOINTS:
            paths.setdefault(rule.rule, set()).update((rule.methods or set()) - {"HEAD", "OPTIONS"})
    return paths


@pytest.mark.parametrize("flag", [None, "0"], ids=["unset", "zero"])
def test_every_method_on_every_phone_path_is_404_with_the_flag_off(app_factory, stub, flag):
    """Not only GET and POST: a wrong method (Werkzeug's 405) or OPTIONS (its automatic 200) would show the route exists."""
    app = app_factory(AUTH_PHONE_LOGIN_ENABLED=flag, TURNSTILE_SITE_KEY="site-key-1").app
    registered = _phone_rule_paths(app)
    assert set(registered) == set(FLAG_PATHS), "the sweep must cover every path a phone endpoint is registered on"
    answers: dict[tuple[str, str], int] = {}
    with app.test_client() as c:
        for path in sorted(registered):
            for method in ALL_METHODS:
                answers[(method, path)] = c.open(path, method=method).status_code
    assert len(answers) == len(FLAG_PATHS) * len(ALL_METHODS)
    assert {key: code for key, code in answers.items() if code != 404} == {}
    assert stub.calls == []


def test_a_wrong_method_on_a_post_only_phone_path_is_405_with_the_flag_on(on, stub):
    """The control of the sweep above: with the flag on the same requests are not 404, so the sweep is not vacuous."""
    post_only = sorted(path for path, methods in _phone_rule_paths(on).items() if methods == {"POST"})
    assert "/api/auth/phone/start" in post_only and "/login/phone/resend" in post_only
    answers: dict[tuple[str, str], int] = {}
    with on.test_client() as c:
        for path in post_only:
            for method in ("GET", "HEAD", "PUT", "PATCH", "DELETE"):
                answers[(method, path)] = c.open(path, method=method).status_code
    assert {key: code for key, code in answers.items() if code != 405} == {}
    assert stub.calls == []


_GUARDED_VIEWS = (
    ("login_phone_start", "POST", "/login/phone"),
    ("login_phone_code_verify", "POST", "/login/phone/code"),
    ("login_phone_resend", "POST", "/login/phone/resend"),
    ("login_phone_change", "POST", "/login/phone/change"),
    ("api_auth_phone_start", "POST", "/api/auth/phone/start"),
    ("api_auth_phone_verify", "POST", "/api/auth/phone/verify"),
    ("api_auth_phone_resend", "POST", "/api/auth/phone/resend"),
    ("login_phone_page", "GET", "/login/phone"),
    ("login_phone_code_page", "GET", "/login/phone/code"),
)


def test_each_phone_view_checks_the_flag_itself(off, stub):
    """The security hook already answers 404; the view's own guard is the second lock. Calling the view inside a
    bare request context skips the hook, so deleting the guard from one view puts exactly that view in the list."""
    assert {endpoint for endpoint, _m, _p in _GUARDED_VIEWS} <= set(off.view_functions)
    unguarded = []
    for endpoint, method, path in _GUARDED_VIEWS:
        kw = {"json": {"phone": NUMBER}} if method == "POST" else {}
        with off.test_request_context(path, method=method, **kw):
            try:
                off.view_functions[endpoint]()
            except NotFound:
                continue
        unguarded.append(endpoint)
    assert unguarded == []
    assert stub.calls == []


def test_login_is_the_password_form_with_the_flag_off(off):
    with off.test_client() as c:
        rv = c.get("/login")
    html = rv.get_data(as_text=True)
    assert rv.status_code == 200
    assert 'action="/login"' in html and "Username or Email" in html
    assert "Text me a code" not in html and "staff" not in html.lower() and "phone number instead" not in html
    # the tail of the page is exactly what it was before the staff link existed
    assert 'Create one</a></p>\n            </div>' in html


def test_password_login_still_works_at_login_with_the_flag_off(off):
    make_user("pwuser", "pw@example.com")
    with off.test_client() as c:
        rv = post_form(c, "/login", {"login": "pwuser", "password": PASSWORD})
        assert rv.status_code == 302 and rv.headers["Location"].endswith("/home")
        assert session_dump(c).get("auth_method") == "password"


# ---------------------------------------------------------------- the switch of /login (flag on)


def test_login_is_the_phone_page_with_the_flag_on(on):
    with on.test_client() as c:
        rv = c.get("/login")
    html = rv.get_data(as_text=True)
    assert rv.status_code == 200
    assert "Text me a code" in html and "Mobile phone number" in html
    assert 'name="password"' not in html
    assert 'href="/staff/login"' in html and "Use email and password instead" in html


def test_login_phone_page_is_the_same_page_at_its_own_address(on):
    with on.test_client() as c:
        a = norm(c.get("/login").get_data(as_text=True))
        b = norm(c.get("/login/phone").get_data(as_text=True))
    # Only the canonical / og:url head tags legitimately name the request path.
    a, b = (re.sub(r'(href|content)="http[^"]*/login(/phone)?"', r'\1="U"', x) for x in (a, b))
    assert a == b


def test_post_login_with_the_flag_on_never_reads_a_password(on):
    make_user("pwuser", "pw@example.com")
    with on.test_client() as c:
        rv = post_form(c, "/login", {"login": "pwuser", "password": PASSWORD})
        assert rv.status_code == 303 and rv.headers["Location"].endswith("/login")
        assert "user_id" not in session_dump(c)


def test_session_expired_message_shows_on_the_phone_page(on):
    with on.test_client() as c:
        rv = c.get("/login?_error=session_expired")
    assert "Your session expired — please log in again." in rv.get_data(as_text=True)


def test_a_google_error_waiting_in_the_session_shows_once_on_the_phone_page(on):
    with on.test_client() as c:
        with c.session_transaction() as sess:
            sess["oauth_error"] = "Google sign-in was cancelled."
        assert "Google sign-in was cancelled." in c.get("/login").get_data(as_text=True)
        assert "Google sign-in was cancelled." not in c.get("/login").get_data(as_text=True)


def test_staff_login_is_the_password_form_and_signs_in_any_role(on):
    make_user("pwuser", "pw@example.com")
    with on.test_client() as c:
        html = c.get("/staff/login").get_data(as_text=True)
        assert 'action="/staff/login"' in html and 'name="password"' in html
        assert 'href="/login"' in html and "Sign in with your phone number instead" in html
        rv = post_form(c, "/staff/login", {"login": "pwuser", "password": PASSWORD})
        assert rv.status_code == 302 and rv.headers["Location"].endswith("/home")
        assert session_dump(c).get("user_id")


def test_staff_login_bad_password_gets_the_same_sentence_as_before(on):
    make_user("pwuser", "pw@example.com")
    with on.test_client() as c:
        rv = post_form(c, "/staff/login", {"login": "pwuser", "password": "wrong-password-123"})
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert "Invalid username/email or password." in html and 'action="/staff/login"' in html


def _staff_empty(c, monkeypatch):
    return post_form(c, "/staff/login", {"login": "", "password": ""}), 200, "Enter username/email and password."


def _staff_bad_password(c, monkeypatch):
    make_user("pwuser", "pw@example.com")
    return post_form(c, "/staff/login", {"login": "pwuser", "password": "wrong-password-123"}), 200, "Invalid username/email or password."


def _staff_ip_limited(c, monkeypatch):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 1)
    assert post_form(c, "/staff/login", {"login": "", "password": ""}).status_code == 200
    return post_form(c, "/staff/login", {"login": "", "password": ""}), 429, "Too many login attempts. Try again in a minute."


def _staff_account_locked(c, monkeypatch):
    make_user("pwuser", "pw@example.com")
    monkeypatch.setattr(_pages, "_ACCT_FAIL_MAX", 1)
    bad = {"login": "pwuser", "password": "wrong-password-123"}
    assert post_form(c, "/staff/login", bad).status_code == 200
    return post_form(c, "/staff/login", bad), 429, "Too many failed attempts. Try again in 15 minutes."


def _staff_session_expired(c, monkeypatch):
    return c.get("/staff/login?_error=session_expired"), 200, "Your session expired"


def _staff_oauth_error(c, monkeypatch):
    with c.session_transaction() as sess:
        sess["oauth_error"] = "Google sign-in was cancelled."
    return c.get("/staff/login"), 200, "Google sign-in was cancelled."


def _staff_finalize_failed(c, monkeypatch):
    make_user("pwuser", "pw@example.com")
    monkeypatch.setattr(_pages, "_finalize_app_session", lambda *a, **k: False)
    return post_form(c, "/staff/login", {"login": "pwuser", "password": PASSWORD}), 200, "Login failed. Try again."


@pytest.mark.parametrize(
    "case",
    [_staff_empty, _staff_bad_password, _staff_ip_limited, _staff_account_locked, _staff_session_expired, _staff_oauth_error,
     _staff_finalize_failed],
    ids=lambda f: f.__name__.removeprefix("_staff_"),
)
def test_every_staff_login_rerender_keeps_the_staff_form_and_the_phone_link(on, monkeypatch, case):
    """An error page of /staff/login posts back to /staff/login (not /login, which is the phone page now) and
    still offers the way back to the phone page."""
    with on.test_client() as c:
        rv, status, sentence = case(c, monkeypatch)
        assert "user_id" not in session_dump(c), "none of these answers is a sign-in"
    html = rv.get_data(as_text=True)
    assert rv.status_code == status and sentence in html
    assert 'action="/staff/login"' in html and 'action="/login"' not in html
    assert 'href="/login">Sign in with your phone number instead</a>' in html


def test_logout_returns_to_the_phone_page(on):
    make_user("pwuser", "pw@example.com")
    with on.test_client() as c:
        post_form(c, "/staff/login", {"login": "pwuser", "password": PASSWORD})
        rv = post_form(c, "/logout")
        assert rv.status_code == 302 and rv.headers["Location"].endswith("/login")
        assert "Text me a code" in c.get(rv.headers["Location"]).get_data(as_text=True)


@pytest.mark.parametrize("path", ["/login", "/login/phone", "/login/phone/code", "/staff/login"])
def test_phone_and_staff_paths_get_the_device_cookie(on, path):
    with on.test_client() as c:
        rv = c.get(path)
    assert "sc_dev=" in (rv.headers.get("Set-Cookie") or "") or any("sc_dev=" in h for h in rv.headers.getlist("Set-Cookie"))


# ---------------------------------------------------------------- the pages: accessibility and UI rules


def _attr(html: str, element_id: str) -> str:
    m = re.search(r'<(?:input|button|p|span|div|label)[^>]*\bid="%s"[^>]*>' % re.escape(element_id), html)
    assert m, element_id
    return m.group(0)


def test_number_page_accessibility(on):
    with on.test_client() as c:
        html = c.get("/login/phone").get_data(as_text=True)
    phone = _attr(html, "ph-phone")
    for needle in ('type="tel"', 'autocomplete="tel"', 'inputmode="tel"', 'aria-describedby="ph-status"'):
        assert needle in phone, needle
    assert re.search(r'<label[^>]*for="ph-phone"[^>]*>Mobile phone number</label>', html)
    status = _attr(html, "ph-status")
    assert 'aria-live="polite"' in status
    box = _attr(html, "ph-consent")
    assert 'type="checkbox"' in box and "checked" not in box and "required" not in box
    send = _attr(html, "ph-send")
    assert 'aria-disabled="true"' in send and not re.search(r"\sdisabled[\s>=]", send)
    assert "Tick the box to continue" in html and 'id="ph-consent-hint"' in html
    assert "Text me a code" in html


def test_number_page_consent_text_and_links(on):
    with on.test_client() as c:
        html = c.get("/login/phone").get_data(as_text=True)
    label = re.search(r'<label for="ph-consent">(.*?)</label>', html, re.S).group(1)
    assert 'href="/terms"' in label and 'href="/privacy"' in label
    plain = re.sub(r"<[^>]+>", "", label)
    assert "For help, contact help@example.com." in plain
    plain = plain.replace(" For help, contact help@example.com.", "")
    assert plain == consent.CONSENT_TEXT
    for needle in ("Sarrafi Cars", "Carriers are not liable for delayed or undelivered messages", "13 or older to use Sarrafi Cars",
                   "Reply STOP to opt out or HELP for help", "Message and data rates may apply"):
        assert needle in plain


def test_consent_text_is_pinned_to_its_version():
    """Any change to the words must bump CONSENT_TEXT_VERSION (and the owner approves the new version)."""
    pinned = {
        "1": "f5782ed66e31382549c1d763ddafd9973b9e7ea1d9e8b793ebceac37d65f1a58",
        # Same words; the linked Terms and Privacy Policy changed.
        "2": "f5782ed66e31382549c1d763ddafd9973b9e7ea1d9e8b793ebceac37d65f1a58",
    }
    digest = hashlib.sha256(consent.CONSENT_TEXT.encode("utf-8")).hexdigest()
    assert pinned.get(CONSENT_TEXT_VERSION) == digest, (
        "the consent wording changed: bump backend/auth/phone/config.py CONSENT_TEXT_VERSION and add the new hash here"
    )


def test_consent_without_a_support_contact_shows_no_made_up_address(app_factory, stub):
    app = app_factory(AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY="site-key-1", SUPPORT_CONTACT=None).app
    with app.test_client() as c:
        html = c.get("/login/phone").get_data(as_text=True)
    assert "For help, contact" not in html and "Reply STOP to opt out or HELP for help" in html


def test_number_page_has_the_turnstile_widget_and_script(on):
    with on.test_client() as c:
        html = c.get("/login/phone").get_data(as_text=True)
    assert '<div class="cf-turnstile" data-sitekey="site-key-1" data-size="compact"></div>' in html
    assert re.search(r'<script src="https://challenges\.cloudflare\.com/turnstile/v0/api\.js" async defer nonce="[^"]+"></script>', html)
    assert 'id="ph-check-missing"' in html and "did not load" in html
    assert re.search(r'name="csrf_token" value="[^"]{20,}"', html)


def test_number_page_without_a_site_key_says_so(app_factory, stub):
    app = app_factory(AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY=None, SUPPORT_CONTACT="help@example.com").app
    with app.test_client() as c:
        html = c.get("/login/phone").get_data(as_text=True)
    assert "cf-turnstile" not in html and "challenges.cloudflare.com" not in html
    assert "not available right now" in html and "help@example.com" in html


def test_code_page_accessibility(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        html = c.get("/login/phone/code").get_data(as_text=True)
    code = _attr(html, "ph-code")
    for needle in ('autocomplete="one-time-code"', 'inputmode="numeric"', 'pattern="[0-9]*"', 'maxlength="6"', "autofocus",
                   'aria-describedby="ph-status"'):
        assert needle in code, needle
    assert re.search(r'<label[^>]*for="ph-code"[^>]*>6-digit code</label>', html)
    assert 'aria-live="polite"' in _attr(html, "ph-status")
    assert "We sent a 6-digit code to the number ending 23." in html
    assert 'action="/login/phone/code"' in html and 'action="/login/phone/resend"' in html and 'action="/login/phone/change"' in html
    assert "Send again" in html and "Use a different number" in html and ">Sign in</button>" in html
    # the resend countdown: a polite live region for the two announcements, the visible text is silent
    assert 'id="ph-wait-live"' in html and 'aria-live="off"' in _attr(html, "ph-wait")
    resend = _attr(html, "ph-resend")
    assert 'aria-disabled="true"' in resend and not re.search(r"\sdisabled[\s>=]", resend)
    assert re.search(r'data-wait="(\d+)"', resend) and 0 < int(re.search(r'data-wait="(\d+)"', resend).group(1)) <= 60
    # its own Turnstile widget (a token works once, so Send again needs a fresh one)
    assert 'class="cf-turnstile"' in html


def test_the_resend_countdown_is_computed_from_the_send_time(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        with c.session_transaction() as sess:
            sess["phone_sent_at"] = time.time() - 100
        html = c.get("/login/phone/code").get_data(as_text=True)
    resend = _attr(html, "ph-resend")
    assert 'data-wait="0"' in resend and "aria-disabled" not in resend
    assert "You can ask for another code now." in html


def test_the_code_page_without_a_challenge_goes_back_to_the_number_page(on):
    with on.test_client() as c:
        rv = c.get("/login/phone/code")
    assert rv.status_code == 303 and rv.headers["Location"].endswith("/login/phone")


@pytest.mark.parametrize("name", ["login_phone.html", "login_phone_code.html"])
def test_templates_follow_the_ui_rules(name):
    text = (FRONTEND / "templates" / name).read_text(encoding="utf-8")
    assert "<style" not in text
    for tag in re.findall(r"<script\b[^>]*>", text):
        assert 'nonce="{{ csp_nonce }}"' in tag, tag
    classes = [w for attr in re.findall(r'class="([^"]*)"', text) for w in attr.split()]
    assert classes
    for word in classes:
        assert not any(b in word for b in BOX_WORDS), word
    assert "{{ csrf_token }}" in text
    assert 'from "_password_field.html"' not in text and "auth-card" not in text and "error-box" not in text


def test_the_phone_stylesheet_rules_have_no_box_words():
    css = (FRONTEND / "static" / "css" / "13-landing-auth.css").read_text(encoding="utf-8")
    names = re.findall(r"\.(ph-[a-z0-9-]+)", css)
    assert names
    for word in set(names):
        assert not any(b in word for b in BOX_WORDS), word


def test_the_script_is_external_and_cache_busted():
    text = (FRONTEND / "templates" / "login_phone.html").read_text(encoding="utf-8")
    assert "url_for('static', filename='phone_login.js')" in text and "static_cache_ver" in text
    assert (FRONTEND / "static" / "phone_login.js").is_file()


# ---------------------------------------------------------------- start


def test_start_sends_the_form_to_the_service_and_goes_to_the_code_page(on, stub, caplog):
    caplog.set_level("DEBUG")
    with on.test_client() as c:
        c.get("/login/phone")
        rv = post_form(c, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"})
        assert rv.status_code == 303 and rv.headers["Location"].endswith("/login/phone/code")
        kw = stub.kwargs("start")
        assert kw["raw"] == NUMBER and kw["consent"] is True and kw["token"] == "pass" and kw["purpose"] == "login"
        assert re.fullmatch(r"[0-9a-f]{64}", kw["device"] or ""), "a valid sc_dev cookie was presented"
        assert kw["ip"] and kw["nonce"] and len(kw["nonce"]) >= 8
        sess = session_dump(c)
        assert sess["phone_cid"] == "cid-1" and sess["phone_nonce"] == kw["nonce"]
        assert sess["phone_sent_msg"] == "We sent a 6-digit code to the number ending 23."
        assert isinstance(sess["phone_sent_at"], float)
        assert not [k for k in sess if "device" in k.lower()]
        blob = json.dumps(sess, default=str) + " ".join(rv.headers.getlist("Set-Cookie")) + rv.get_data(as_text=True)
        assert "2025550123" not in blob and "555-0123" not in blob and E164 not in blob
    assert "2025550123" not in caplog.text and not re.search(r"\d{7,}", caplog.text)


@pytest.mark.parametrize(
    ("step", "path", "data"),
    [
        ("start", "/login/phone", START_FORM),
        ("resend", "/login/phone/resend", {"cf-turnstile-response": "pass-2"}),
        ("check", "/login/phone/code", {"code": "000000"}),
    ],
)
def test_without_a_device_cookie_every_step_passes_no_device_hash(on, stub, step, path, data):
    """A cookie minted by this very response is not a presented cookie: the service counts it as none."""
    with on.test_client() as c:
        open_challenge(c)  # a challenge in the session, but this browser never fetched a page: no sc_dev
        assert c.get_cookie("sc_dev") is None
        post_form(c, path, data)
    assert stub.kwargs(step)["device"] is None


def test_start_resend_and_check_share_the_device_hash_the_nonce_and_the_address(on, stub):
    """The three steps are derived the same way: one device hash, the nonce that start minted, the client address."""
    with on.test_client() as c:
        c.environ_base["REMOTE_ADDR"] = "198.51.100.9"
        start_challenge(c, stub)
        post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
        post_form(c, "/login/phone/code", {"code": "000000"})
        minted = session_dump(c)["phone_nonce"]
    start, resend, check = stub.kwargs("start"), stub.kwargs("resend"), stub.kwargs("check")
    assert re.fullmatch(r"[0-9a-f]{64}", start["device"] or ""), "a valid sc_dev cookie was presented"
    assert resend["device"] == start["device"] and check["device"] == start["device"]
    assert start["nonce"] == minted and resend["nonce"] == minted and check["nonce"] == minted
    assert start["ip"] == resend["ip"] == check["ip"] == "198.51.100.9"
    assert resend["cid"] == check["cid"] == "cid-1"


def test_the_nonce_is_minted_once_and_reused_by_a_second_start(on, stub):
    with on.test_client() as c:
        c.get("/login/phone")
        post_form(c, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"})
        first = session_dump(c)["phone_nonce"]
        post_form(c, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass-2"})
    nonces = [kw["nonce"] for n, kw in stub.calls if n == "start"]
    assert nonces == [first, first]


def test_start_without_the_ticked_box_is_refused_with_the_services_sentence(on, stub):
    with on.test_client() as c:
        rv = post_form(c, "/login/phone", {"phone": NUMBER, "cf-turnstile-response": "pass"})
        assert rv.status_code == 200
        html = rv.get_data(as_text=True)
        assert re.search(r'<p id="ph-status"[^>]*>Tick the box to continue\.</p>', html)
        assert stub.kwargs("start")["consent"] is False
        assert "phone_cid" not in session_dump(c)
        # the number is not echoed back
        assert "555-0123" not in html and "2025550123" not in html


@pytest.mark.parametrize("value", ["", "0", "yes", "true", "off"])
def test_only_1_or_on_counts_as_consent(on, stub, value):
    with on.test_client() as c:
        post_form(c, "/login/phone", {"phone": NUMBER, "consent": value, "cf-turnstile-response": "pass"})
    assert stub.kwargs("start")["consent"] is False


def test_the_on_value_of_a_checkbox_counts_as_consent(on, stub):
    with on.test_client() as c:
        post_form(c, "/login/phone", {"phone": NUMBER, "consent": "on", "cf-turnstile-response": "pass"})
    assert stub.kwargs("start")["consent"] is True


def _sent(stub, monkeypatch):
    return dict(START_FORM)


def _refused(reason, sentence):
    def setup(stub, monkeypatch):
        stub.start_outcome = ps.StartOutcome(False, reason, sentence)
        return dict(START_FORM)

    return setup


def _consent_unticked(stub, monkeypatch):
    return {k: v for k, v in START_FORM.items() if k != "consent"}


def _service_raises(stub, monkeypatch):
    stub.raise_on = "start"
    return dict(START_FORM)


def _address_throttled(stub, monkeypatch):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 0)
    return dict(START_FORM)


# (id, how the case is set up and what form it posts, HTML status, the sentence the page shows)
START_CASES = [
    ("sent", _sent, 303, None),
    ("invalid_number", _refused(ps.R_INVALID, ps.COULD_NOT_SEND), 200, ps.COULD_NOT_SEND),
    ("consent_unticked", _consent_unticked, 200, ps.CONSENT_REQUIRED),
    ("not_a_person", _refused(ps.R_TURNSTILE, ps.NOT_A_PERSON), 200, ps.NOT_A_PERSON),
    ("reload", _refused(ps.R_LIMIT, ps.RELOAD), 200, ps.RELOAD),
    ("busy", _refused(ps.R_BUSY, ps.BUSY), 429, ps.BUSY),
    ("unavailable", _refused(ps.R_UNAVAILABLE, ps.UNAVAILABLE), 503, ps.UNAVAILABLE),
    ("opted_out", _refused(ps.R_OPTED_OUT, ps.OPTED_OUT), 503, ps.OPTED_OUT),
    ("service_raises", _service_raises, 503, ps.UNAVAILABLE),
    ("address_throttled", _address_throttled, 429, "Please try again in a minute."),
]
START_ALL = [pytest.param(*case[1:], id=case[0]) for case in START_CASES]
START_REFUSALS = [pytest.param(*case[1:], id=case[0]) for case in START_CASES if case[0] != "sent"]


@pytest.mark.parametrize(("setup", "status", "sentence"), START_REFUSALS)
def test_a_refused_start_shows_the_number_page_again_with_the_sentence_and_its_status(on, stub, monkeypatch, setup, status, sentence):
    """No challenge exists after a refused start, so the person is never sent to a code page that has nothing to check."""
    form = setup(stub, monkeypatch)
    with on.test_client() as c:
        rv = post_form(c, "/login/phone", form)
        assert "phone_cid" not in session_dump(c)
    html = rv.get_data(as_text=True)
    assert rv.status_code == status
    assert_number_page(html)
    assert re.search(r'<p id="ph-status"[^>]*>%s</p>' % re.escape(sentence), unescape(html))


@pytest.mark.parametrize(("setup", "status", "sentence"), START_ALL)
def test_no_start_outcome_puts_the_typed_number_anywhere(on, stub, monkeypatch, caplog, setup, status, sentence):
    """Sent, refused, failed or throttled: the number is in no log line, body, header or session cookie."""
    caplog.set_level("DEBUG")
    form = setup(stub, monkeypatch)
    with on.test_client() as c:
        c.get("/login/phone")
        rv = post_form(c, "/login/phone", form)
        assert rv.status_code == status
        assert_number_absent(rv, c, caplog)
        if rv.status_code == 303:
            assert_number_absent(c.get(rv.headers["Location"]), c, caplog)


def test_long_input_is_cut_before_it_reaches_the_service(on, stub):
    with on.test_client() as c:
        post_form(c, "/login/phone", {"phone": "9" * 500, "consent": "1", "cf-turnstile-response": "p" * 9000})
    kw = stub.kwargs("start")
    assert len(kw["raw"]) == 64 and len(kw["token"]) == 4096


# ---------------------------------------------------------------- the per-address request limit

# step -> (form path, form data, the service call it makes, JSON path, JSON body)
THROTTLE_STEPS = {
    "start": ("/login/phone", START_FORM, "start", "/api/auth/phone/start", START_JSON),
    "resend": ("/login/phone/resend", {"cf-turnstile-response": "pass-2"}, "resend", "/api/auth/phone/resend", {"turnstile_token": "pass-2"}),
    "verify": ("/login/phone/code", {"code": "111111"}, "check", "/api/auth/phone/verify", {"code": "111111"}),
}
OTHER_ADDRESS = {"REMOTE_ADDR": "198.51.100.7"}


@pytest.mark.parametrize("step", sorted(THROTTLE_STEPS))
def test_the_fourth_form_request_in_a_minute_from_one_address_never_reaches_the_service(on, stub, monkeypatch, step):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 3)
    path, data, call, _json_path, _json_body = THROTTLE_STEPS[step]
    with on.test_client() as c:
        open_challenge(c)
        for _ in range(3):
            assert post_form(c, path, data).status_code != 429
        assert stub.names().count(call) == 3
        rv = post_form(c, path, data)
        html = rv.get_data(as_text=True)
        assert rv.status_code == 429 and "Please try again in a minute." in html
        # start has no challenge to show, so it asks for a number again; the other two stay on the code page
        (assert_number_page if step == "start" else assert_code_page)(html)
        assert stub.names().count(call) == 3, "a throttled request is answered before the service (and Turnstile) is asked"
        assert session_dump(c)["phone_cid"] == "cid-1", "being throttled does not end the person's open challenge"
        # the limit is per address
        assert post_form(c, path, data, environ_overrides=OTHER_ADDRESS).status_code != 429
    assert stub.names().count(call) == 4


@pytest.mark.parametrize("step", sorted(THROTTLE_STEPS))
def test_the_fourth_json_request_in_a_minute_from_one_address_is_try_again(on, stub, monkeypatch, step):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 3)
    _path, _data, call, json_path, json_body = THROTTLE_STEPS[step]
    with on.test_client() as c:
        open_challenge(c)
        for _ in range(3):
            assert post_json(c, json_path, json_body).status_code != 429
        rv = post_json(c, json_path, json_body)
        assert rv.status_code == 429
        assert rv.get_json() == {"ok": False, "error": "try_again", "message": "Please try again in a minute."}
        assert rv.headers["Cache-Control"] == "no-store"
        assert stub.names().count(call) == 3
        assert session_dump(c)["phone_cid"] == "cid-1"
        assert post_json(c, json_path, json_body, environ_overrides=OTHER_ADDRESS).status_code != 429
    assert stub.names().count(call) == 4


def test_start_resend_and_check_draw_on_one_bucket_per_address(on, stub, monkeypatch):
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 3)
    with on.test_client() as c:
        assert post_form(c, "/login/phone", START_FORM).status_code == 303
        assert post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"}).status_code == 303
        assert post_form(c, "/login/phone/code", {"code": "111111"}).status_code == 200
        assert post_form(c, "/login/phone/code", {"code": "111111"}).status_code == 429
    assert stub.names() == ["start", "resend", "check"]


# ---------------------------------------------------------------- verify: the session and the refusals


def test_a_good_code_signs_in_as_a_phone_session(on, stub, caplog):
    caplog.set_level("DEBUG")
    uid = PhoneAccounts().adopt_or_create(email="p-7-abcd1234@phone.invalid", username="user-7-abcd")
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_up", user_id=uid, identity_id=7)]
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert rv.status_code == 302 and rv.headers["Location"].endswith("/home")
        sess = session_dump(c)
        assert sess["user_id"] == uid and sess["auth_method"] == "phone" and sess["auth_role"] == "shopper"
        assert sess["user_role"] == "general_user" and sess["user_email"] == ""
        assert sess["mfa_ok"] is True and isinstance(sess["auth_at"], float)
        assert not [k for k in sess if k.startswith("phone_")], "the challenge is gone after sign-in"
        kw = stub.kwargs("check")
        assert kw["code"] == "482913" and kw["cid"] == "cid-1" and kw["ip"]
    assert "482913" not in caplog.text and not re.search(r"\d{7,}", caplog.text)


def test_the_check_uses_the_same_device_hash_as_the_start(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        post_form(c, "/login/phone/code", {"code": "000000"})
    start_device = stub.kwargs("start")["device"]
    assert re.fullmatch(r"[0-9a-f]{64}", start_device or ""), "the start was made with the sc_dev cookie the page set"
    assert stub.kwargs("check")["device"] == start_device


def test_a_wrong_code_keeps_the_challenge_and_shows_the_services_sentence(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "111111"})
        assert rv.status_code == 200
        assert ps.CODE_FAILED in rv.get_data(as_text=True) and 'id="ph-code"' in rv.get_data(as_text=True)
        assert session_dump(c)["phone_cid"] == "cid-1"
        assert "user_id" not in session_dump(c)


@pytest.mark.parametrize("reason", [ps.R_NO_CHALLENGE, ps.R_EXPIRED, ps.R_TOO_MANY])
def test_a_finished_challenge_returns_to_the_number_page(on, stub, reason):
    stub.check_outcome = ps.CheckOutcome(False, reason, ps.START_AGAIN)
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "111111"})
        html = rv.get_data(as_text=True)
        assert ps.START_AGAIN in html
        assert_number_page(html)
        assert "phone_cid" not in session_dump(c)


def test_a_code_typed_in_another_browser_gets_the_start_again_sentence_without_asking_the_service(on, stub):
    with on.test_client() as c:  # no challenge in this browser's session
        rv = post_form(c, "/login/phone/code", {"code": "482913"})
    assert ps.START_AGAIN in rv.get_data(as_text=True)
    assert "check" not in stub.names()


def test_a_locked_number_shows_the_lock_sentence_with_a_429(on, stub):
    stub.check_outcome = ps.CheckOutcome(False, ps.R_LOCKED, ps.CODE_LOCKED)
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "111111"})
        assert rv.status_code == 429 and ps.CODE_LOCKED in rv.get_data(as_text=True)


def test_an_approval_for_another_purpose_never_signs_anyone_in(on, stub):
    stub.check_outcome = approved(purpose="change")
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert ps.START_AGAIN in rv.get_data(as_text=True)
        assert "user_id" not in session_dump(c)
    assert "sign_in" not in stub.names()


# Rows ``get_user_profile`` can return for an account that must NOT get a phone session. A stored NULL role is
# one of them: ``get_user_profile`` reads a missing role as ``dealer_staff`` (the "dealer_staff" row below).
ELIGIBILITY_ROWS = {
    "admin": {"role": "admin"},
    "dealership_owner": {"role": "dealership_owner"},
    "dealership_member": {"role": "dealership_member"},
    "dealer_staff": {"role": "dealer_staff"},
    "general_with_dealer_id": {"role": "general_user", "dealer_id": "d-1"},
    "general_with_registry_id": {"role": "general_user", "dealership_registry_id": 5},
    "general_with_org": {"role": "general_user", "org_id": 3},
    "env_admin_email": {"role": "general_user", "email": "boss@example.com"},
    "inactive": {"role": "general_user", "is_active": 0},
}


def _row(**kw) -> dict:
    base = {"id": 41, "username": "user-41-abcd", "email": "p-41-abcd1234@phone.invalid", "role": "general_user",
            "dealer_id": None, "dealership_registry_id": None, "org_id": None, "is_active": 1, "is_premium": 0,
            "subscription_plan_id": None}
    base.update(kw)
    return base


@pytest.mark.parametrize("case", sorted(ELIGIBILITY_ROWS))
def test_an_ineligible_account_gets_the_wrong_code_answer_and_no_session(on, stub, monkeypatch, caplog, case):
    # The code page prints the resend countdown (data-wait, "in N seconds"). Freeze the routes' clock so the two
    # renderings below cannot differ by a second boundary on a slow machine.
    monkeypatch.setattr(routes, "time", types.SimpleNamespace(time=lambda: 1_700_000_000.0))
    monkeypatch.setenv("APP_ADMIN_EMAILS", "boss@example.com")
    monkeypatch.setattr(_users_db, "get_user_profile", lambda uid: _row(**ELIGIBILITY_ROWS[case]))
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=41, identity_id=9)]
    with on.test_client() as c:
        start_challenge(c, stub)
        # what a wrong code looks like on this very page
        stub.check_outcome = ps.CheckOutcome(False, ps.R_WRONG, ps.CODE_FAILED)
        wrong = post_form(c, "/login/phone/code", {"code": "111111"})
        wrong_html = norm(wrong.get_data(as_text=True))
        stub.check_outcome = approved()
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
        html = norm(rv.get_data(as_text=True))
        assert rv.status_code == wrong.status_code == 200
        assert html == wrong_html, "a refused account must look exactly like a wrong code"
        sess = session_dump(c)
        assert "user_id" not in sess and "auth_method" not in sess
        # the challenge itself is dropped; the browser nonce stays so a later start reuses it
        assert not [k for k in sess if k.startswith("phone_") and k != "phone_nonce"], "the spent challenge is dropped"
    refused = [r.getMessage() for r in caplog.records if "login_refused" in r.getMessage()]
    assert len(refused) == 1
    reason = "inactive" if case == "inactive" else "ineligible"
    assert f"event=login_refused reason={reason} purpose=login" in refused[0]
    assert "41" not in refused[0] and "phone.invalid" not in refused[0]


def _orphan_setup(stub, monkeypatch, *, dead, fresh=None):
    """The number's identity points at ``dead``, an account that no longer exists; a second sign_in answers with
    ``fresh`` (a real account id) or, when None, with another id that does not exist either. Returns the list
    of ids the purge seam was called with."""
    real = _users_db.get_user_profile
    monkeypatch.setattr(_users_db, "get_user_profile", lambda uid: real(uid) if fresh and int(uid) == fresh else None)
    purged: list[int] = []
    monkeypatch.setattr(_phone_accounts, "purge_phone_identity", purged.append)
    stub.check_outcome = approved()
    stub.sign_ins = [
        ps.SignInOutcome(True, "signed_in", user_id=dead, identity_id=9),
        ps.SignInOutcome(True, "signed_up", user_id=fresh or dead + 1, identity_id=10),
    ]
    return purged


def test_an_orphan_identity_is_released_and_the_person_signs_in_to_a_fresh_account(on, stub, monkeypatch, caplog):
    caplog.set_level("DEBUG")
    fresh = make_user("fresh", "p-8-fresh1234@phone.invalid")
    purged = _orphan_setup(stub, monkeypatch, dead=4242, fresh=fresh)
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert rv.status_code == 302 and rv.headers["Location"].endswith("/home")
        sess = session_dump(c)
        assert sess["user_id"] == fresh and sess["auth_method"] == "phone"
        assert not [k for k in sess if k.startswith("phone_")], "the challenge is gone after sign-in"
    assert purged == [4242], "the dead account's identity is purged exactly once, by its dead id"
    assert stub.names().count("sign_in") == 2
    messages = [r.getMessage() for r in caplog.records if r.name == "auth_phone"]
    assert messages.count("event=identity_orphan_healed") == 1
    assert "event=login_refused" not in " ".join(messages)
    assert "482913" not in caplog.text and not re.search(r"\d{7,}", caplog.text) and "4242" not in caplog.text
    assert_number_absent(rv, c, caplog)


def test_an_orphan_that_is_still_gone_after_the_heal_gets_the_wrong_code_answer(on, stub, monkeypatch, caplog):
    purged = _orphan_setup(stub, monkeypatch, dead=99)
    with on.test_client() as c:
        start_challenge(c, stub)
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
        html = rv.get_data(as_text=True)
        assert rv.status_code == 200 and ps.CODE_FAILED in html and 'id="ph-code"' in html
        sess = session_dump(c)
        assert "user_id" not in sess and "phone_cid" not in sess, "no session, and the spent challenge is dropped"
    assert purged == [99], "only the first dead id is purged; a second miss gives up instead of looping"
    assert stub.names().count("sign_in") == 2
    assert "event=identity_orphan_healed" in caplog.text
    assert "event=login_refused reason=finalize_failed purpose=login" in caplog.text


def test_a_live_account_triggers_no_heal_and_no_purge(on, stub, monkeypatch, caplog):
    fresh = make_user("alive", "p-3-alive1234@phone.invalid")
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=fresh, identity_id=3)]
    purged: list[int] = []
    monkeypatch.setattr(_phone_accounts, "purge_phone_identity", purged.append)
    with on.test_client() as c:
        start_challenge(c, stub)
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert rv.status_code == 302 and session_dump(c)["user_id"] == fresh
    assert purged == [] and stub.names().count("sign_in") == 1
    assert "identity_orphan_healed" not in caplog.text


def test_sign_in_is_tried_sign_in_tries_times_then_asks_for_a_new_code(on, stub, caplog, monkeypatch):
    pauses: list[float] = []
    monkeypatch.setattr(routes, "_sleep", pauses.append)
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(False, ps.R_RETRY)] * routes._SIGN_IN_TRIES
    with on.test_client() as c:
        start_challenge(c, stub)
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert ps.TRY_AGAIN in rv.get_data(as_text=True)
        assert "user_id" not in session_dump(c) and "phone_cid" not in session_dump(c)
    assert stub.names().count("sign_in") == routes._SIGN_IN_TRIES and len(pauses) == routes._SIGN_IN_TRIES - 1
    assert "event=login_refused reason=retry_exhausted purpose=login" in caplog.text


def test_sign_in_that_wins_on_the_second_try_signs_in(on, stub):
    uid = make_user("late", "p-5-aaaabbbb@phone.invalid")
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(False, ps.R_RETRY), ps.SignInOutcome(True, "signed_in", user_id=uid, identity_id=5)]
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert rv.status_code == 302 and session_dump(c)["user_id"] == uid
    assert stub.names().count("sign_in") == 2


def test_the_service_gets_the_real_accounts_adapter(on, stub):
    uid = make_user("late", "p-5-aaaabbbb@phone.invalid")
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=uid, identity_id=5)]
    with on.test_client() as c:
        start_challenge(c, stub)
        post_form(c, "/login/phone/code", {"code": "482913"})
    assert isinstance(stub.kwargs("sign_in")["accounts"], PhoneAccounts)


# ---------------------------------------------------------------- failures of the service itself


@pytest.mark.parametrize("name", ["start", "check", "resend"])
def test_a_service_exception_is_a_503_sentence_never_a_500(on, stub, caplog, name):
    with on.test_client() as c:
        start_challenge(c, stub)
        stub.raise_on = name
        path = {"start": "/login/phone", "check": "/login/phone/code", "resend": "/login/phone/resend"}[name]
        data = {"phone": NUMBER, "consent": "1", "code": "482913", "cf-turnstile-response": "pass"}
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, path, data)
        html = rv.get_data(as_text=True)
        assert rv.status_code == 503 and ps.UNAVAILABLE in html
        # a start that failed has no challenge: the number page; resend and check keep the person on the code page
        (assert_number_page if name == "start" else assert_code_page)(html)
    assert "event=route_error reason=RuntimeError" in caplog.text
    assert "2025550123" not in caplog.text and "exploded" not in caplog.text


def test_a_provider_that_is_not_configured_is_a_503_sentence(app_factory, caplog, monkeypatch):
    monkeypatch.delenv("AUTH_PHONE_PROVIDER", raising=False)
    app = app_factory(AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY="k", TWILIO_ACCOUNT_SID=None, TWILIO_API_KEY_SID=None,
                      TWILIO_API_KEY_SECRET=None, TWILIO_AUTH_TOKEN=None, TWILIO_VERIFY_SERVICE_SID=None).app
    registry.reset()
    routes.reset_service()
    with app.test_client() as c:
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"})
    assert rv.status_code == 503 and ps.UNAVAILABLE in rv.get_data(as_text=True)
    assert_number_page(rv.get_data(as_text=True))
    assert "event=route_error reason=ProviderUnavailable" in caplog.text
    assert routes._svc is None, "a failure to build the service is not cached"


def test_a_service_failure_after_the_code_was_spent_drops_the_challenge(on, stub):
    stub.check_outcome = approved()
    stub.raise_on = "sign_in"
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert rv.status_code == 503 and ps.UNAVAILABLE in rv.get_data(as_text=True)
        assert "phone_cid" not in session_dump(c) and "user_id" not in session_dump(c)


# ---------------------------------------------------------------- resend and change number


def test_resend_asks_the_service_with_its_own_turnstile_token(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        with c.session_transaction() as sess:
            sess["phone_sent_at"] = time.time() - 500
        rv = post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
        assert rv.status_code == 303 and rv.headers["Location"].endswith("/login/phone/code")
        kw = stub.kwargs("resend")
        assert kw["cid"] == "cid-1" and kw["token"] == "pass-2" and kw["nonce"] == session_dump(c)["phone_nonce"]
        assert time.time() - session_dump(c)["phone_sent_at"] < 30, "the countdown starts again"


def test_resend_too_soon_shows_the_sentence_on_the_code_page(on, stub):
    stub.resend_outcome = ps.StartOutcome(False, ps.R_TOO_SOON, ps.RESEND_TOO_SOON)
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
        assert rv.status_code == 429
        html = rv.get_data(as_text=True)
        assert ps.RESEND_TOO_SOON in html and 'id="ph-code"' in html
        assert session_dump(c)["phone_cid"] == "cid-1"


def test_resend_without_a_challenge_returns_to_the_number_page(on, stub):
    with on.test_client() as c:
        rv = post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
    html = rv.get_data(as_text=True)
    assert ps.START_AGAIN in html and 'id="ph-start-form"' in html
    assert "resend" not in stub.names()


def test_resend_of_a_vanished_challenge_returns_to_the_number_page(on, stub):
    stub.resend_outcome = ps.StartOutcome(False, ps.R_NO_CHALLENGE, ps.START_AGAIN)
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
        assert 'id="ph-start-form"' in rv.get_data(as_text=True) and "phone_cid" not in session_dump(c)


def test_use_a_different_number_cancels_and_forgets_the_challenge(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_form(c, "/login/phone/change")
        assert rv.status_code == 303 and rv.headers["Location"].endswith("/login/phone")
        sess = session_dump(c)
        assert "phone_cid" not in sess and "phone_sent_msg" not in sess
        assert sess["phone_nonce"], "the browser keeps its nonce (a lock keyed by it survives)"
    assert stub.kwargs("cancel")["cid"] == "cid-1"
    with on.test_client() as c2:
        assert c2.get("/login/phone/code").status_code == 303


# ---------------------------------------------------------------- CSRF


@pytest.mark.parametrize("path", ["/login/phone", "/login/phone/code", "/login/phone/resend", "/login/phone/change", "/staff/login"])
def test_a_form_post_without_a_token_goes_back_to_login_with_the_expired_message(on, stub, path):
    with on.test_client() as c:
        rv = c.post(path, data={"phone": NUMBER})
        assert rv.status_code == 302 and "/login?_error=session_expired" in rv.headers["Location"]
        assert "Your session expired" in c.get(rv.headers["Location"]).get_data(as_text=True)
    assert stub.calls == []


@pytest.mark.parametrize("path", JSON_POSTS)
def test_a_json_post_without_the_header_is_403(on, stub, path):
    from backend.utils.csrf import _SESSION_KEY

    with on.test_client() as c:
        # the two rejected posts first: neither may reach the service
        assert c.post(path, json={"phone": NUMBER}).status_code == 403
        with c.session_transaction() as sess:
            sess[_SESSION_KEY] = TOKEN
        assert c.post(path, json={}, headers={"X-CSRF-Token": "x" * 40}).status_code == 403
        assert stub.calls == []
        # the right header gets through to the view (only start has anything to ask the service without a challenge)
        assert post_json(c, path, token=TOKEN).status_code != 403
    assert stub.names() == (["start"] if path.endswith("/start") else [])


# ---------------------------------------------------------------- JSON twins


def test_json_start_success_shape(on, stub):
    with on.test_client() as c:
        c.get("/login/phone")
        rv = post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": True, "turnstile_token": "pass"})
    assert rv.status_code == 200
    assert rv.get_json() == {"ok": True, "message": "We sent a 6-digit code to the number ending 23.", "resend_after_s": 60}
    assert rv.headers["Cache-Control"] == "no-store"
    kw = stub.kwargs("start")
    assert kw["consent"] is True and kw["token"] == "pass" and kw["purpose"] == "login" and kw["raw"] == NUMBER


@pytest.mark.parametrize("consent_value", ["true", 1, "1", None, False])
def test_json_consent_must_be_the_boolean_true(on, stub, consent_value):
    with on.test_client() as c:
        rv = post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": consent_value, "turnstile_token": "pass"})
    assert rv.status_code == 400
    assert rv.get_json() == {"ok": False, "error": "consent_required", "message": ps.CONSENT_REQUIRED}


def test_json_body_must_be_an_object(on, stub):
    with on.test_client() as c:
        for raw in ("[1, 2]", '"x"', "not json", "null"):
            rv = post_json(c, "/api/auth/phone/start", raw=raw)
            assert rv.status_code == 400 and rv.get_json() == {"ok": False, "error": "json_object"}
    assert stub.calls == []


@pytest.mark.parametrize(
    ("sentence", "code", "status"),
    [
        (ps.COULD_NOT_SEND, "could_not_send", 400),
        (ps.NOT_A_PERSON, "turnstile_failed", 400),
        (ps.RELOAD, "reload", 400),
        (ps.BUSY, "busy", 429),
        (ps.UNAVAILABLE, "unavailable", 503),
        (ps.OPTED_OUT, "opted_out", 503),
    ],
)
def test_json_start_failure_words_and_statuses(on, stub, sentence, code, status):
    stub.start_outcome = ps.StartOutcome(False, ps.R_LIMIT, sentence)
    with on.test_client() as c:
        rv = post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": True, "turnstile_token": "pass"})
    assert rv.status_code == status
    assert rv.get_json() == {"ok": False, "error": code, "message": sentence}


def test_reasons_that_share_a_sentence_answer_with_the_same_body_and_status(on, stub):
    """Known and unknown numbers, a VoIP line, a bad number, a limit: one sentence, one answer."""
    answers = set()
    for reason in (ps.R_LIMIT, ps.R_LINE, ps.R_INVALID, ps.R_COUNTRY, ps.R_NOT_ALLOWED, ps.R_PROVIDER, ps.R_LOCKED):
        stub.start_outcome = ps.StartOutcome(False, reason, ps.COULD_NOT_SEND, retry_after_s=30 if reason == ps.R_PROVIDER else None)
        with on.test_client() as c:
            rv = post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": True, "turnstile_token": "pass"})
        answers.add((rv.status_code, json.dumps(rv.get_json(), sort_keys=True)))
    assert len(answers) == 1


def test_json_verify_signs_in_and_hides_the_placeholder_email(on, stub):
    uid = make_user("late", "p-5-aaaabbbb@phone.invalid")
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=uid, identity_id=5)]
    with on.test_client() as c:
        c.get("/login/phone")
        post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": True, "turnstile_token": "pass"})
        rv = post_json(c, "/api/auth/phone/verify", {"code": "482913"})
        assert rv.status_code == 200
        body = rv.get_json()
        assert body["ok"] is True and body["user"]["id"] == uid and body["user"]["email"] == ""
        assert body["user"]["role"] == "general_user" and set(body["user"]) == {"id", "username", "email", "role", "is_premium", "has_paid_access"}
        assert "phone.invalid" not in rv.get_data(as_text=True)
        assert session_dump(c)["auth_method"] == "phone"
        # the session was cleared, so the client fetches a new CSRF token
        me = c.get("/api/auth/me").get_json()
        assert me["user"]["email"] == ""
        assert c.get("/api/auth/csrf").get_json()["csrf_token"]


@pytest.mark.parametrize(
    ("outcome", "code", "status"),
    [
        (ps.CheckOutcome(False, ps.R_WRONG, ps.CODE_FAILED), "code_failed", 400),
        (ps.CheckOutcome(False, ps.R_EXPIRED, ps.START_AGAIN), "start_again", 400),
        (ps.CheckOutcome(False, ps.R_LOCKED, ps.CODE_LOCKED), "code_locked", 429),
        (ps.CheckOutcome(False, ps.R_BUSY, ps.TRY_AGAIN), "try_again", 429),
        (ps.CheckOutcome(False, ps.R_RETRY, ps.CODE_RETRY), "try_again", 503),
    ],
)
def test_json_verify_failures(on, stub, outcome, code, status):
    stub.check_outcome = outcome
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_json(c, "/api/auth/phone/verify", {"code": "111111"})
    assert rv.status_code == status and rv.get_json()["error"] == code and rv.get_json()["ok"] is False


def test_json_verify_without_a_challenge_says_start_again(on, stub):
    with on.test_client() as c:
        rv = post_json(c, "/api/auth/phone/verify", {"code": "111111"})
    assert rv.status_code == 400 and rv.get_json()["error"] == "start_again"
    assert stub.calls == []


def _verify_ineligible(stub, monkeypatch):
    monkeypatch.setattr(_users_db, "get_user_profile", lambda uid: _row(role="admin"))
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=41, identity_id=9)]
    return 400, "code_failed", ps.CODE_FAILED, 200


def _verify_ended(stub, monkeypatch):
    stub.check_outcome = ps.CheckOutcome(False, ps.R_EXPIRED, ps.START_AGAIN)
    return 400, "start_again", ps.START_AGAIN, 200


def _verify_profile_raises(stub, monkeypatch):
    def boom(uid):
        raise RuntimeError(f"users database exploded for {E164}")

    monkeypatch.setattr(_users_db, "get_user_profile", boom)
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=41, identity_id=9)]
    return 503, "unavailable", ps.UNAVAILABLE, 503


def _verify_finalize_fails(stub, monkeypatch):
    monkeypatch.setattr(_users_db, "get_user_profile", lambda uid: _row())
    monkeypatch.setattr(_auth_session, "finalize_app_session", lambda *a, **k: False)
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=41, identity_id=9)]
    return 400, "code_failed", ps.CODE_FAILED, 200


# (channel, how the verify fails -> JSON status, JSON error word, sentence, HTML status)
VERIFY_ENDINGS = [
    pytest.param("json", _verify_ineligible, id="json-ineligible_account"),
    pytest.param("json", _verify_ended, id="json-ended_challenge"),
    pytest.param("json", _verify_profile_raises, id="json-profile_lookup_raises"),
    pytest.param("json", _verify_finalize_fails, id="json-finalize_fails"),
    pytest.param("html", _verify_profile_raises, id="html-profile_lookup_raises"),
    pytest.param("html", _verify_finalize_fails, id="html-finalize_fails"),
]


@pytest.mark.parametrize(("channel", "setup"), VERIFY_ENDINGS)
def test_a_verify_that_cannot_sign_in_ends_the_challenge_and_leaves_no_session(on, stub, monkeypatch, caplog, channel, setup):
    json_status, json_error, sentence, html_status = setup(stub, monkeypatch)
    caplog.set_level("DEBUG")
    with on.test_client() as c:
        start_challenge(c, stub)
        if channel == "json":
            rv = post_json(c, "/api/auth/phone/verify", {"code": "482913"})
            assert rv.status_code == json_status
            assert rv.get_json() == {"ok": False, "error": json_error, "message": sentence}
        else:
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
            assert rv.status_code == html_status and sentence in unescape(rv.get_data(as_text=True))
        sess = session_dump(c)
        assert "phone_cid" not in sess, "the spent challenge is dropped"
        assert "user_id" not in sess and "auth_method" not in sess
        assert not [k for k in sess if k in ("phone_sent_msg", "phone_sent_at")]
    assert "482913" not in caplog.text and "exploded" not in caplog.text and E164 not in caplog.text
    assert not re.search(r"\d{7,}", caplog.text)


def test_a_profile_lookup_that_raises_is_logged_by_class_only(on, stub, monkeypatch, caplog):
    _verify_profile_raises(stub, monkeypatch)
    with on.test_client() as c:
        start_challenge(c, stub)
        with caplog.at_level("WARNING", logger="auth_phone"):
            post_json(c, "/api/auth/phone/verify", {"code": "482913"})
    assert "event=route_error reason=RuntimeError" in caplog.text


def test_a_failed_finalize_is_logged_as_a_refusal_and_no_session_is_started(on, stub, monkeypatch, caplog):
    _verify_finalize_fails(stub, monkeypatch)
    with on.test_client() as c:
        start_challenge(c, stub)
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
    assert rv.status_code == 200
    assert "event=login_refused reason=finalize_failed purpose=login" in caplog.text


def test_json_resend_success_and_failure(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_json(c, "/api/auth/phone/resend", {"turnstile_token": "pass-2"})
        assert rv.status_code == 200 and rv.get_json()["ok"] is True and rv.get_json()["resend_after_s"] == 60
        assert stub.kwargs("resend")["token"] == "pass-2"
        stub.resend_outcome = ps.StartOutcome(False, ps.R_TOO_SOON, ps.RESEND_TOO_SOON)
        rv = post_json(c, "/api/auth/phone/resend", {"turnstile_token": "pass-3"})
        assert rv.status_code == 429 and rv.get_json()["error"] == "too_soon"


def test_json_exception_is_a_503(on, stub, caplog):
    caplog.set_level("DEBUG")
    stub.raise_on = "start"
    with on.test_client() as c:
        rv = post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": True, "turnstile_token": "pass"})
        assert_number_absent(rv, c, caplog)
    assert rv.status_code == 503 and rv.get_json()["error"] == "unavailable"
    assert "exploded" not in rv.get_data(as_text=True) and "exploded" not in caplog.text
    assert "event=route_error reason=RuntimeError" in caplog.text


# ---------------------------------------------------------------- headers and cookies


def test_the_nine_phone_answers_carry_no_server_timing_while_other_pages_do(on, stub, monkeypatch):
    """The phone routes send no Server-Timing header. An HTML page and a JSON
    page outside the feature are the controls: they carry the header in this very app, so the test cannot pass
    just because the header is off. (The staff form is itself an auth route and carries no header either.)"""
    monkeypatch.setattr(routes.Config, "RATE_LIMIT_LOGIN_PER_MIN", 1000)
    answers = {}
    with on.test_client() as c:
        controls = {"GET /terms": c.get("/terms"), "GET /api/auth/csrf": c.get("/api/auth/csrf")}
        answers["GET /staff/login"] = c.get("/staff/login")
        answers["GET /login/phone"] = c.get("/login/phone")
        answers["POST /login/phone"] = post_form(c, "/login/phone", START_FORM)
        answers["GET /login/phone/code"] = c.get("/login/phone/code")
        answers["POST /login/phone/code"] = post_form(c, "/login/phone/code", {"code": "111111"})
        answers["POST /login/phone/resend"] = post_form(c, "/login/phone/resend", {"cf-turnstile-response": "pass-2"})
        answers["POST /login/phone/change"] = post_form(c, "/login/phone/change")
        answers["POST /api/auth/phone/start"] = post_json(c, "/api/auth/phone/start", START_JSON)
        answers["POST /api/auth/phone/verify"] = post_json(c, "/api/auth/phone/verify", {"code": "111111"})
        answers["POST /api/auth/phone/resend"] = post_json(c, "/api/auth/phone/resend", {"turnstile_token": "pass-2"})
    for label, rv in controls.items():
        assert rv.status_code == 200 and "Server-Timing" in rv.headers, f"control {label}: timing is off in this app"
    assert len(answers) == 10
    assert [label for label, rv in answers.items() if rv.status_code >= 500] == []
    assert [label for label, rv in answers.items() if "Server-Timing" in rv.headers] == []


def test_the_phone_pages_and_the_failed_code_answer_are_never_cached(on, stub):
    pages = {}
    with on.test_client() as c:
        pages["GET /login"] = c.get("/login")
        pages["GET /login/phone"] = c.get("/login/phone")
        pages["POST /login/phone (sent)"] = post_form(c, "/login/phone", START_FORM)
        pages["GET /login/phone/code"] = c.get("/login/phone/code")
        pages["POST /login/phone/code (wrong code)"] = post_form(c, "/login/phone/code", {"code": "111111"})
        pages["POST /login/phone (refused)"] = post_form(c, "/login/phone", {"phone": NUMBER})
    assert pages["GET /login/phone/code"].status_code == 200 and pages["POST /login/phone/code (wrong code)"].status_code == 200
    assert {label: rv.headers.get("Cache-Control") for label, rv in pages.items()} == {label: "no-store" for label in pages}


def test_a_started_challenge_keeps_its_session_cookie_for_a_while(on, stub):
    """The person leaves the browser to read the text: the session cookie of a start carries an expiry
    (``session.permanent``). A refused start, which keeps no challenge, sets only a browser-session cookie."""

    def session_cookie(rv) -> str:
        cookies = [h for h in rv.headers.getlist("Set-Cookie") if h.startswith("session=")]
        assert len(cookies) == 1, rv.headers.getlist("Set-Cookie")
        return cookies[0]

    with on.test_client() as c:
        sent = session_cookie(post_form(c, "/login/phone", START_FORM))
    assert re.search(r"expires=", sent, re.I) or re.search(r"max-age=", sent, re.I)
    stub.start_outcome = ps.StartOutcome(False, ps.R_LIMIT, ps.COULD_NOT_SEND)
    with on.test_client() as c:
        refused = session_cookie(post_form(c, "/login/phone", START_FORM))
    assert not re.search(r"expires=|max-age=", refused, re.I), "the control: only a stored challenge makes the session last"


# ---------------------------------------------------------------- the surface other tests pin


def test_the_endpoints_exist_with_the_names_the_csrf_lists_use(on):
    names = {r.endpoint for r in on.url_map.iter_rules()}
    assert PHONE_FLAG_ENDPOINTS <= names
    rules = {(r.rule, m) for r in on.url_map.iter_rules() if r.endpoint in PHONE_FLAG_ENDPOINTS for m in r.methods - {"HEAD", "OPTIONS"}}
    assert rules == {
        ("/login/phone", "GET"), ("/login/phone", "POST"), ("/login/phone/code", "GET"), ("/login/phone/code", "POST"),
        ("/login/phone/resend", "POST"), ("/login/phone/change", "POST"), ("/staff/login", "GET"), ("/staff/login", "POST"),
        ("/api/auth/phone/start", "POST"), ("/api/auth/phone/verify", "POST"), ("/api/auth/phone/resend", "POST"),
    }
    # the form routes keep "login" in their names: a bad token redirects to /login, not a bare 403
    form_posts = {r.endpoint for r in on.url_map.iter_rules() if "POST" in (r.methods or ()) and r.rule in FORM_POSTS}
    assert len(form_posts) == len(FORM_POSTS), "every form POST path has an endpoint"
    assert [ep for ep in sorted(form_posts) if "login" not in ep] == []


def test_module_state_does_not_trip_the_pg_harness_sweep():
    from backend.tests import pg_harness as pgh

    assert [k for k in pgh.sweep_cache_flags() if k.startswith("backend.auth.phone.routes.")] == []


# ---------------------------------------------------------------- review R7: one timing for every start


class _FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0
        self.waits: list[float] = []

    def monotonic(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.waits.append(s)
        self.t += s


@pytest.mark.parametrize(
    ("outcome", "work_s"),
    [
        (ps.StartOutcome(True, ps.R_SENT, ps.SENT_TEMPLATE.format(masked="ending 23"), public_id="cid-1"), 0.2),
        (ps.StartOutcome(True, ps.R_SENT, ps.SENT_TEMPLATE.format(masked="ending 23"), public_id="cid-1"), 1.9),
        (ps.StartOutcome(False, ps.R_PROVIDER, ps.COULD_NOT_SEND), 0.05),
        (ps.StartOutcome(False, ps.R_BUSY, ps.BUSY), 0.01),
    ],
)
def test_every_start_answer_takes_the_same_floor_however_fast_the_service_was(on, stub, monkeypatch, outcome, work_s):
    # A number with an account skips the line-type lookup, so its start is faster: the floor hides it.
    clock = _FakeClock()
    monkeypatch.setattr(routes, "_monotonic", clock.monotonic)
    monkeypatch.setattr(routes, "_pad_sleep", clock.sleep)
    stub.start_outcome = outcome
    real_start = stub.start

    def slow_start(*a, **k):
        clock.t += work_s
        return real_start(*a, **k)

    monkeypatch.setattr(stub, "start", slow_start)
    with on.test_client() as c:
        t0 = clock.t
        post_form(c, "/login/phone", START_FORM)
        assert clock.t - t0 == pytest.approx(routes.START_MIN_SECONDS)
        t1 = clock.t
        post_json(c, "/api/auth/phone/start", START_JSON)
        assert clock.t - t1 == pytest.approx(routes.START_MIN_SECONDS)


def test_a_slow_start_is_not_padded_further(on, stub, monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(routes, "_monotonic", clock.monotonic)
    monkeypatch.setattr(routes, "_pad_sleep", clock.sleep)
    real_start = stub.start

    def slow_start(*a, **k):
        clock.t += routes.START_MIN_SECONDS + 1
        return real_start(*a, **k)

    monkeypatch.setattr(stub, "start", slow_start)
    with on.test_client() as c:
        post_form(c, "/login/phone", START_FORM)
    assert clock.waits == []


# ---------------------------------------------------------------- stage 1: the allow-list keeps /login


def test_while_the_allow_list_is_set_login_stays_the_password_form_with_a_phone_link(app_factory, stub):
    app = app_factory(
        AUTH_PHONE_LOGIN_ENABLED="1", TURNSTILE_SITE_KEY="site-key-1", SUPPORT_CONTACT="help@example.com",
        AUTH_PHONE_ALLOWED_NUMBERS="+12025550142",
    ).app
    with app.test_client() as c:
        html = c.get("/login").get_data(as_text=True)
        assert 'name="password"' in html and 'action="/login"' in html
        assert 'href="/login/phone"' in html
        assert c.get("/login/phone").status_code == 200  # the phone page itself is there
        # a password POST on /login is still read (not redirected to the phone page)
        rv = post_form(c, "/login", {"username": "nobody@example.com", "password": "wrong-password-1"})
        assert rv.status_code in (200, 401, 429) and 'name="password"' in rv.get_data(as_text=True)


def test_without_the_allow_list_login_is_the_phone_page(on, stub):
    with on.test_client() as c:
        html = c.get("/login").get_data(as_text=True)
        assert 'name="password"' not in html
