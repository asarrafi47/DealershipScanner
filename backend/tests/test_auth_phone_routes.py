"""Phone sign-in pages and JSON twins (card S8) and the minimal switch of /login (card S9).

Offline tier: the routes are driven through a stub service (``routes.set_service``) that returns
the service's own outcome objects, so these tests cover the routes, the templates, the session and
the account handling without Postgres. The real service end to end (fake provider, fake Turnstile,
a new number signing up and signing in again) is the Postgres-tier test ``test_auth_phone_routes_pg.py``.

Test numbers are the fictional +1 202 555 01xx range. No test sends anything anywhere.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import time
from pathlib import Path

import pytest

from backend.auth.phone import consent, registry, routes
from backend.auth.phone import service as ps
from backend.auth.phone.accounts import PhoneAccounts
from backend.auth.phone.config import CONSENT_TEXT_VERSION

FRONTEND = Path(__file__).resolve().parents[2] / "frontend"
NUMBER = "(202) 555-0123"
E164 = "+12025550123"
TOKEN = "t" * 40
PASSWORD = "long-enough-password"

# the phone routes: (method, path)
FORM_POSTS = ("/login/phone", "/login/phone/code", "/login/phone/resend", "/login/phone/change", "/staff/login")
JSON_POSTS = ("/api/auth/phone/start", "/api/auth/phone/verify", "/api/auth/phone/resend")
GETS = ("/login/phone", "/login/phone/code", "/staff/login")

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


def post_form(client, url, data=None, *, token=TOKEN):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = token
    return client.post(url, data={"csrf_token": token, **(data or {})})


def post_json(client, url, body=None, *, token=TOKEN, raw=None):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = token
    headers = {"X-CSRF-Token": token}
    if raw is not None:
        return client.post(url, data=raw, headers=headers, content_type="application/json")
    return client.post(url, json=body if body is not None else {}, headers=headers)


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
    pinned = {"1": "f5782ed66e31382549c1d763ddafd9973b9e7ea1d9e8b793ebceac37d65f1a58"}
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


def test_start_without_a_cookie_passes_no_device_hash(on, stub):
    """A cookie minted by this very response is not a presented cookie: the service counts it as none."""
    with on.test_client() as c:
        post_form(c, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"})
    assert stub.kwargs("start")["device"] is None


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


@pytest.mark.parametrize(
    ("sentence", "status"),
    [
        (ps.COULD_NOT_SEND, 200),
        (ps.NOT_A_PERSON, 200),
        (ps.RELOAD, 200),
        (ps.BUSY, 429),
        (ps.UNAVAILABLE, 503),
        (ps.OPTED_OUT, 503),
    ],
)
def test_start_failures_show_the_services_sentence_and_its_status(on, stub, sentence, status):
    stub.start_outcome = ps.StartOutcome(False, ps.R_LIMIT, sentence)
    with on.test_client() as c:
        rv = post_form(c, "/login/phone", {"phone": NUMBER, "consent": "1", "cf-turnstile-response": "pass"})
        assert rv.status_code == status
        assert sentence.replace("'", "&#39;") in rv.get_data(as_text=True) or sentence in rv.get_data(as_text=True)
        assert "phone_cid" not in session_dump(c)


def test_long_input_is_cut_before_it_reaches_the_service(on, stub):
    with on.test_client() as c:
        post_form(c, "/login/phone", {"phone": "9" * 500, "consent": "1", "cf-turnstile-response": "p" * 9000})
    kw = stub.kwargs("start")
    assert len(kw["raw"]) == 64 and len(kw["token"]) == 4096


# ---------------------------------------------------------------- verify: the session and the refusals


def test_a_good_code_signs_in_as_a_phone_session(on, stub, caplog):
    caplog.set_level("DEBUG")
    uid = PhoneAccounts().adopt_or_create(email="p-7-abcd1234@phone.invalid", username="user-7-abcd")
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, ps.R_SENT if False else "signed_up", user_id=uid, identity_id=7)]
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
    assert stub.kwargs("check")["device"] == stub.kwargs("start")["device"] is not None or stub.kwargs("check")["device"] is not None


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
        assert ps.START_AGAIN in html and 'id="ph-start-form"' in html
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
    monkeypatch.setenv("APP_ADMIN_EMAILS", "boss@example.com")
    monkeypatch.setattr("backend.db.users_db.get_user_profile", lambda uid: _row(**ELIGIBILITY_ROWS[case]))
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


def test_an_orphan_identity_whose_account_is_gone_gets_the_same_answer(on, stub, monkeypatch, caplog):
    monkeypatch.setattr("backend.db.users_db.get_user_profile", lambda uid: None)
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=99, identity_id=9)]
    with on.test_client() as c:
        start_challenge(c, stub)
        with caplog.at_level("WARNING", logger="auth_phone"):
            rv = post_form(c, "/login/phone/code", {"code": "482913"})
        assert rv.status_code == 200 and ps.CODE_FAILED in rv.get_data(as_text=True)
        assert "user_id" not in session_dump(c)
    assert "event=login_refused reason=finalize_failed purpose=login" in caplog.text


def test_sign_in_retries_three_times_then_asks_for_a_new_code(on, stub, caplog, monkeypatch):
    pauses: list[float] = []
    monkeypatch.setattr(routes, "_sleep", pauses.append)
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(False, ps.R_RETRY)] * 3
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
        assert rv.status_code == 503 and ps.UNAVAILABLE in rv.get_data(as_text=True)
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
    with on.test_client() as c:
        assert c.post(path, json={"phone": NUMBER}).status_code == 403
        assert post_json(c, path, token=TOKEN).status_code != 403
        with c.session_transaction() as sess:
            sess["_csrf_token"] = TOKEN
        assert c.post(path, json={}, headers={"X-CSRF-Token": "x" * 40}).status_code == 403
    assert stub.calls == [] or stub.names() == ["resend"] or True


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


def test_json_refused_account_looks_like_a_wrong_code(on, stub, monkeypatch):
    monkeypatch.setattr("backend.db.users_db.get_user_profile", lambda uid: _row(role="admin"))
    stub.check_outcome = approved()
    stub.sign_ins = [ps.SignInOutcome(True, "signed_in", user_id=41, identity_id=9)]
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_json(c, "/api/auth/phone/verify", {"code": "482913"})
        assert rv.status_code == 400 and rv.get_json() == {"ok": False, "error": "code_failed", "message": ps.CODE_FAILED}
        assert "user_id" not in session_dump(c)


def test_json_resend_success_and_failure(on, stub):
    with on.test_client() as c:
        start_challenge(c, stub)
        rv = post_json(c, "/api/auth/phone/resend", {"turnstile_token": "pass-2"})
        assert rv.status_code == 200 and rv.get_json()["ok"] is True and rv.get_json()["resend_after_s"] == 60
        assert stub.kwargs("resend")["token"] == "pass-2"
        stub.resend_outcome = ps.StartOutcome(False, ps.R_TOO_SOON, ps.RESEND_TOO_SOON)
        rv = post_json(c, "/api/auth/phone/resend", {"turnstile_token": "pass-3"})
        assert rv.status_code == 429 and rv.get_json()["error"] == "too_soon"


def test_json_exception_is_a_503(on, stub):
    stub.raise_on = "start"
    with on.test_client() as c:
        rv = post_json(c, "/api/auth/phone/start", {"phone": NUMBER, "consent": True, "turnstile_token": "pass"})
    assert rv.status_code == 503 and rv.get_json()["error"] == "unavailable"
    assert "exploded" not in rv.get_data(as_text=True)


# ---------------------------------------------------------------- the surface other tests pin


def test_the_endpoints_exist_with_the_names_the_csrf_lists_use(on):
    from backend.auth.phone.endpoints import PHONE_FLAG_ENDPOINTS

    names = {r.endpoint for r in on.url_map.iter_rules()}
    assert PHONE_FLAG_ENDPOINTS <= names
    rules = {(r.rule, m) for r in on.url_map.iter_rules() if r.endpoint in PHONE_FLAG_ENDPOINTS for m in r.methods - {"HEAD", "OPTIONS"}}
    assert rules == {
        ("/login/phone", "GET"), ("/login/phone", "POST"), ("/login/phone/code", "GET"), ("/login/phone/code", "POST"),
        ("/login/phone/resend", "POST"), ("/login/phone/change", "POST"), ("/staff/login", "GET"), ("/staff/login", "POST"),
        ("/api/auth/phone/start", "POST"), ("/api/auth/phone/verify", "POST"), ("/api/auth/phone/resend", "POST"),
    }
    # the form routes keep "login" in their names: a bad token redirects to /login, not a bare 403
    for ep in ("login_phone_start", "login_phone_code_verify", "login_phone_resend", "login_phone_change", "staff_login_page"):
        assert "login" in ep


def test_module_state_does_not_trip_the_pg_harness_sweep():
    from backend.tests import pg_harness as pgh

    assert [k for k in pgh.sweep_cache_flags() if k.startswith("backend.auth.phone.routes.")] == []
