"""Account and mail updates.

1. Email change confirmation: the current password (or, for a Google/Apple account, a fresh
   Google or Apple sign-in); the change clears a pending reset link and tells the old address.
2. Verification resends are capped per account; a capped request shows the usual "sent" page.
3. Reset and verification mail are sent in the background (backend/utils/background_mail.py).
"""

from __future__ import annotations

import re
import threading
import time

import pytest

from backend.auth import session as auth_session

CSRF = "c" * 32
PASSWORD = "long-enough-password-1"


@pytest.fixture
def mails(monkeypatch):
    """Every transactional mail, whichever module sends it."""
    sent: list[dict] = []

    def fake(*, to_email, subject, text, log_surface="app"):
        sent.append({"to": to_email, "subject": subject, "text": text, "surface": log_surface})

    monkeypatch.setattr("backend.utils.mfa_delivery.send_transactional_email", fake)
    monkeypatch.setattr("backend.auth.password_reset.send_transactional_email", fake)
    monkeypatch.setattr("backend.auth.email_verification.send_transactional_email", fake)
    return sent


@pytest.fixture
def main(app_factory):
    return app_factory(
        PASSWORD_RESET_ENABLED="1",
        PASSWORD_RESET_PEPPER="test-reset-pepper",
        EMAIL_VERIFICATION_ENABLED="1",
        EMAIL_VERIFY_PEPPER="test-pepper",
        BILLING_STRIPE_ENABLED="0",
    )


def _sign_in(client, uid, *, method="password", auth_at=None):
    with client.session_transaction() as s:
        s.permanent = True
        s["user_id"] = uid
        s["user_role"] = "general_user"
        s["_csrf_token"] = CSRF
        s[auth_session.AUTH_AT] = time.time() - 60 if auth_at is None else auth_at
        s[auth_session.AUTH_SEEN_AT] = s[auth_session.AUTH_AT]
        s[auth_session.AUTH_METHOD] = method


def _profile(client, **form):
    data = {"csrf_token": CSRF, "form_action": "profile", **form}
    return client.post("/account/profile", data=data)


def _user(name="mover", email="old@example.com"):
    from backend.db.users_db import save_user

    return int(save_user(name, email, PASSWORD, role="general_user"))


def _email(uid):
    from backend.db.users_db import get_user_profile

    return get_user_profile(uid)["email"]


# ---------------------------------------------------------------- 1. email change


def test_email_change_without_the_password_is_refused(main, mails):
    uid = _user()
    c = main.app.test_client()
    _sign_in(c, uid)
    r = _profile(c, username="mover", email="new@example.com")
    assert b"Enter your current password to change your email" in r.data
    r = _profile(c, username="mover", email="new@example.com", current_password="wrong-password")
    assert b"Current password is incorrect" in r.data
    assert _email(uid) == "old@example.com"
    assert mails == []


def test_email_change_with_the_password_clears_the_reset_link_and_tells_the_old_address(main, mails):
    from backend.auth.password_reset import hash_reset_token
    from backend.db.users_db import get_user_id_by_password_reset_token_hash, set_user_password_reset_token

    uid = _user()
    set_user_password_reset_token(uid, hash_reset_token("pending-token"), int(time.time()) + 3600)
    c = main.app.test_client()
    _sign_in(c, uid)
    r = _profile(c, username="mover", email="new@example.com", current_password=PASSWORD)
    assert b"Profile updated" in r.data
    assert _email(uid) == "new@example.com"
    assert get_user_id_by_password_reset_token_hash(hash_reset_token("pending-token")) is None
    assert [m["to"] for m in mails] == ["old@example.com"]
    assert "changed" in mails[0]["text"] and "new@example.com" not in mails[0]["text"]


def test_username_only_edit_needs_no_password(main, mails):
    uid = _user()
    c = main.app.test_client()
    _sign_in(c, uid)
    r = _profile(c, username="renamed", email="OLD@example.com")
    assert b"Profile updated" in r.data
    assert mails == []


def test_a_failing_notice_never_fails_the_change(main, monkeypatch):
    def boom(**_kw):
        raise RuntimeError("provider down")

    monkeypatch.setattr("backend.utils.mfa_delivery.send_transactional_email", boom)
    uid = _user()
    c = main.app.test_client()
    _sign_in(c, uid)
    r = _profile(c, username="mover", email="new@example.com", current_password=PASSWORD)
    assert r.status_code == 200 and b"Profile updated" in r.data
    assert _email(uid) == "new@example.com"


def test_no_notice_to_an_empty_or_placeholder_address(main, mails):
    from backend.routes.account import _notify_old_email

    for old in ("", "p-123@phone.invalid"):
        _notify_old_email(old)
    assert mails == []


def _oauth_user():
    from backend.db.users_db import save_oauth_user

    return int(save_oauth_user("gmover", "g@example.com", "google-sub-1", role="general_user"))


def test_google_account_needs_a_fresh_google_sign_in(main, mails):
    uid = _oauth_user()
    stale = main.app.test_client()
    _sign_in(stale, uid, method="google", auth_at=time.time() - 3600)
    r = _profile(stale, username="gmover", email="g2@example.com")
    assert b"sign in again with Google or Apple" in r.data
    assert _email(uid) == "g@example.com"
    fresh = main.app.test_client()
    _sign_in(fresh, uid, method="google", auth_at=time.time() - 30)
    r = _profile(fresh, username="gmover", email="g2@example.com")
    assert b"Profile updated" in r.data
    assert _email(uid) == "g2@example.com"


def test_sign_out_everywhere_does_not_make_an_old_google_sign_in_fresh(main, mails):
    uid = _oauth_user()
    c = main.app.test_client()
    _sign_in(c, uid, method="google", auth_at=time.time() - 30)
    with c.session_transaction() as s:
        s[auth_session.AUTH_ORIGIN_AT] = time.time() - 3600
    r = _profile(c, username="gmover", email="g2@example.com")
    assert b"sign in again with Google or Apple" in r.data


def test_profile_form_has_a_current_password_field(main):
    uid = _user()
    c = main.app.test_client()
    _sign_in(c, uid)
    html = c.get("/account/profile").get_data(as_text=True)
    assert 'id="profile-current-password"' in html


# ---------------------------------------------------------------- 2. verification resend cap


def _page(resp) -> str:
    return re.sub(r'nonce="[^"]*"', 'nonce=""', resp.get_data(as_text=True))


def test_verification_resend_is_capped_per_account_and_shows_the_sent_page(main, mails):
    from backend.auth.email_verification import hash_verify_token
    from backend.db.users_db import set_user_email_verify_token

    uid = _user("verifier", "verifier@example.com")
    set_user_email_verify_token(uid, hash_verify_token("first"), int(time.time()) + 3600)
    responses = []
    for i in range(4):  # four addresses, so only the per-account cap can stop the fourth
        c = main.app.test_client()
        _sign_in(c, uid)
        responses.append(
            c.post(
                "/resend-verification", data={"csrf_token": CSRF}, environ_overrides={"REMOTE_ADDR": f"10.6.0.{i + 1}"}
            )
        )
    assert [r.status_code for r in responses] == [200] * 4
    assert len(mails) == 3
    assert _page(responses[3]) == _page(responses[2])


# ---------------------------------------------------------------- 3. mail is sent in the background


def _blocking_send(monkeypatch, target):
    release = threading.Event()
    sent: list[str] = []

    def held_send(*, to_email, subject, text, log_surface="app"):
        release.wait(5.0)
        sent.append(to_email)

    monkeypatch.setattr(target, held_send)
    return release, sent


def test_forgot_password_answers_before_the_mail_is_sent(main, monkeypatch):
    from backend.utils import background_mail

    monkeypatch.setenv("AUTH_MAIL_INLINE", "0")
    release, sent = _blocking_send(monkeypatch, "backend.auth.password_reset.send_transactional_email")
    _user("forgetful", "forgetful@example.com")
    c = main.app.test_client()
    with c.session_transaction() as s:
        s["_csrf_token"] = CSRF
    started = time.monotonic()
    r = c.post("/forgot-password", data={"csrf_token": CSRF, "login": "forgetful@example.com"})
    took = time.monotonic() - started
    assert r.status_code == 200
    assert sent == [] and took < 4.0  # the answer did not wait for the provider
    release.set()
    assert background_mail.drain(10.0)
    assert sent == ["forgetful@example.com"]


def test_verification_resend_answers_before_the_mail_is_sent(main, monkeypatch):
    from backend.auth.email_verification import hash_verify_token
    from backend.db.users_db import set_user_email_verify_token
    from backend.utils import background_mail

    monkeypatch.setenv("AUTH_MAIL_INLINE", "0")
    release, sent = _blocking_send(monkeypatch, "backend.auth.email_verification.send_transactional_email")
    uid = _user("slowverify", "slowverify@example.com")
    set_user_email_verify_token(uid, hash_verify_token("first"), int(time.time()) + 3600)
    c = main.app.test_client()
    _sign_in(c, uid)
    r = c.post("/resend-verification", data={"csrf_token": CSRF})
    assert r.status_code == 200 and b"Verification email sent" in r.data
    assert sent == []
    release.set()
    assert background_mail.drain(10.0)
    assert sent == ["slowverify@example.com"]


def test_background_pool_drops_past_its_bound(monkeypatch):
    from backend.utils import background_mail

    monkeypatch.setenv("AUTH_MAIL_INLINE", "0")
    release = threading.Event()
    monkeypatch.setattr(background_mail, "_slots", threading.BoundedSemaphore(1))
    assert background_mail.submit(release.wait, 5.0) is True
    assert background_mail.submit(release.wait, 5.0) is False
    release.set()
    assert background_mail.drain(10.0)


# ---------------------------------------------------------------- more mail cases


def test_forgot_password_sends_mail_in_the_background(main, monkeypatch):
    from backend.auth import password_reset
    from backend.utils import background_mail

    monkeypatch.setenv("AUTH_MAIL_INLINE", "0")
    threads: list[int] = []
    real = password_reset.get_user_by_login
    monkeypatch.setattr(
        password_reset, "get_user_by_login", lambda li: threads.append(threading.get_ident()) or real(li)
    )
    monkeypatch.setattr(password_reset, "send_transactional_email", lambda **_kw: None)
    _user("lookup", "lookup@example.com")
    c = main.app.test_client()
    with c.session_transaction() as s:
        s["_csrf_token"] = CSRF
    pages = [
        _page(c.post("/forgot-password", data={"csrf_token": CSRF, "login": login}))
        for login in ("lookup@example.com", "nobody@example.com")
    ]
    assert background_mail.drain(10.0)
    assert len(threads) == 2 and threading.get_ident() not in threads
    assert pages[0] == pages[1]


def test_email_change_notice_does_not_use_the_background_pool(main, mails, monkeypatch):
    from backend.utils import background_mail

    monkeypatch.setattr(background_mail, "submit", lambda *a, **k: False)  # a full pool drops everything
    uid = _user()
    c = main.app.test_client()
    _sign_in(c, uid)
    r = _profile(c, username="mover", email="new@example.com", current_password=PASSWORD)
    assert b"Profile updated" in r.data
    assert [m["to"] for m in mails] == ["old@example.com"]


def test_a_failed_background_verification_send_is_logged(main, monkeypatch, caplog):
    import logging

    from backend.auth.email_verification import hash_verify_token
    from backend.db.users_db import set_user_email_verify_token
    from backend.utils import background_mail

    def boom(**_kw):
        raise RuntimeError("provider down")

    monkeypatch.setenv("AUTH_MAIL_INLINE", "0")
    monkeypatch.setattr("backend.auth.email_verification.send_transactional_email", boom)
    uid = _user("failsend", "failsend@example.com")
    set_user_email_verify_token(uid, hash_verify_token("first"), int(time.time()) + 3600)
    c = main.app.test_client()
    _sign_in(c, uid)
    with caplog.at_level(logging.WARNING):
        r = c.post("/resend-verification", data={"csrf_token": CSRF})
        assert background_mail.drain(10.0)
    assert r.status_code == 200 and b"Verification email sent" in r.data
    assert any("email verification send failed" in rec.getMessage() for rec in caplog.records)
