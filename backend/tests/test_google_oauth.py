"""Google OAuth sign-in (optional, env-gated)."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def _verified(user_id: int) -> int:
    """Mark an account's email verified: OAuth auto-links only to verified emails (P3.1)."""
    from backend.db.users_db import mark_user_email_verified

    assert mark_user_email_verified(int(user_id))
    return int(user_id)


def _make_app(app_factory, *, show_button: bool = False, google: bool = False):
    env = {
        "BILLING_STRIPE_ENABLED": "0",
        "ALLOW_DEFAULT_APP_USER": "0",
        "GOOGLE_OAUTH_SHOW_BUTTON": "1" if show_button else None,
        "GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN": None,
    }
    if google:
        env["GOOGLE_OAUTH_CLIENT_ID"] = "test-client-id"
        env["GOOGLE_OAUTH_CLIENT_SECRET"] = "test-client-secret"
        env["GOOGLE_OAUTH_REDIRECT_URI"] = "http://localhost/auth/google/callback"
    else:
        env["GOOGLE_OAUTH_CLIENT_ID"] = ""
        env["GOOGLE_OAUTH_CLIENT_SECRET"] = ""
        env["GOOGLE_OAUTH_REDIRECT_URI"] = ""
    return app_factory(**env).app


def test_google_start_redirects_to_login_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory)
    client = app.test_client()
    r = client.get("/auth/google", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "/login" in r.headers.get("Location", "")


def test_google_start_redirects_to_google_when_configured(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True)
    client = app.test_client()
    with client.session_transaction() as sess:
        sess.clear()
    r = client.get("/auth/google", follow_redirects=False)
    assert r.status_code in (302, 303)
    loc = r.headers.get("Location", "")
    assert loc.startswith("https://accounts.google.com/o/oauth2/v2/auth")
    assert "client_id=test-client-id" in loc
    assert "state=" in loc


def test_google_callback_creates_user_and_logs_in(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True)
    client = app.test_client()

    with client.session_transaction() as sess:
        sess["google_oauth_state"] = "expected-state"

    token_payload = {"access_token": "access-token"}
    userinfo = {
        "sub": "google-sub-123",
        "email": "newuser@example.com",
        "email_verified": True,
    }

    with (
        patch("backend.auth.google_oauth._exchange_code", return_value=token_payload),
        patch("backend.auth.google_oauth._fetch_userinfo", return_value=userinfo),
    ):
        r = client.get(
            "/auth/google/callback?code=abc&state=expected-state",
            follow_redirects=False,
        )

    assert r.status_code in (302, 303)
    assert "/premium" in r.headers.get("Location", "")
    with client.session_transaction() as sess:
        assert sess.get("user_id")
        assert sess.get("user_email") == "newuser@example.com"

    from backend.db.users_db import get_user_by_google_sub

    u = get_user_by_google_sub("google-sub-123")
    assert u is not None
    assert u["email"] == "newuser@example.com"

    r2 = client.get("/premium")
    assert b"signed in with Google" in r2.data
    # 2026-09-30 /premium rebuild: the welcome is one line above the plan table; no
    # separate Upgrade / Continue buttons (Upgrade was dead while billing is off).
    assert b"What each one does" in r2.data


def test_google_callback_returning_user_skips_premium_offer_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    _verified(save_user("existing", "existing@example.com", "long-enough-password", role=ROLE_GENERAL))

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["google_oauth_state"] = "expected-state"

    with (
        patch("backend.auth.google_oauth._exchange_code", return_value={"access_token": "tok"}),
        patch(
            "backend.auth.google_oauth._fetch_userinfo",
            return_value={
                "sub": "google-sub-link",
                "email": "existing@example.com",
                "email_verified": True,
            },
        ),
    ):
        r = client.get(
            "/auth/google/callback?code=abc&state=expected-state",
            follow_redirects=False,
        )

    assert r.status_code in (302, 303)
    loc = r.headers.get("Location", "")
    assert "/premium" not in loc
    u = get_user_by_google_sub("google-sub-link")
    assert u is not None


def test_google_start_premium_intent_offers_premium_for_returning_user(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    _verified(save_user("existing", "existing@example.com", "long-enough-password", role=ROLE_GENERAL))

    client = app.test_client()
    client.get("/auth/google?intent=premium")
    with client.session_transaction() as sess:
        sess["google_oauth_state"] = "expected-state"

    with (
        patch("backend.auth.google_oauth._exchange_code", return_value={"access_token": "tok"}),
        patch(
            "backend.auth.google_oauth._fetch_userinfo",
            return_value={
                "sub": "google-sub-intent",
                "email": "existing@example.com",
                "email_verified": True,
            },
        ),
    ):
        r = client.get(
            "/auth/google/callback?code=abc&state=expected-state",
            follow_redirects=False,
        )

    assert r.status_code in (302, 303)
    assert "/premium" in r.headers.get("Location", "")


def test_google_callback_links_existing_email_user(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    _verified(save_user("existing", "existing@example.com", "long-enough-password", role=ROLE_GENERAL))

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["google_oauth_state"] = "expected-state"

    with (
        patch("backend.auth.google_oauth._exchange_code", return_value={"access_token": "tok"}),
        patch(
            "backend.auth.google_oauth._fetch_userinfo",
            return_value={
                "sub": "google-sub-link",
                "email": "existing@example.com",
                "email_verified": True,
            },
        ),
    ):
        r = client.get(
            "/auth/google/callback?code=abc&state=expected-state",
            follow_redirects=False,
        )

    assert r.status_code in (302, 303)
    u = get_user_by_google_sub("google-sub-link")
    assert u is not None
    assert u["username"] == "existing"


def test_google_callback_rejects_state_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True)
    client = app.test_client()

    with client.session_transaction() as sess:
        sess["google_oauth_state"] = "expected-state"

    r = client.get(
        "/auth/google/callback?code=abc&state=wrong-state",
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)
    assert "/login" in r.headers.get("Location", "")

    r2 = client.get("/login")
    assert b"expired" in r2.data.lower()


def test_login_template_hides_google_button_until_show_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    app = _make_app(app_factory, google=True, show_button=False)
    client = app.test_client()
    r = client.get("/login")
    assert b"Sign in with Google" in r.data
    assert b"hidden" in r.data

    app_visible = _make_app(app_factory, google=True, show_button=True)
    r2 = app_visible.test_client().get("/login")
    assert b"Sign in with Google" in r2.data
    assert b'class="auth-oauth" hidden' not in r2.data


# --- P3.1: pre-registration takeover (D-SEC4 (a)) -----------------------------

_CSRF = "c" * 32
_VICTIM = "victim@example.com"
_SQUATTER_PW = "squatter-password-123"


def _google_callback(client, userinfo: dict):
    with client.session_transaction() as sess:
        sess["google_oauth_state"] = "expected-state"
    with (
        patch("backend.auth.google_oauth._exchange_code", return_value={"access_token": "tok"}),
        patch("backend.auth.google_oauth._fetch_userinfo", return_value=userinfo),
    ):
        return client.get(
            "/auth/google/callback?code=abc&state=expected-state",
            follow_redirects=False,
        )


def _password_login(client, login: str, password: str):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _CSRF
    return client.post(
        "/login",
        data={"csrf_token": _CSRF, "login": login, "password": password},
        follow_redirects=False,
    )


def _victim_info(sub: str = "victim-google-sub", **extra) -> dict:
    info = {"sub": sub, "email": _VICTIM, "email_verified": True}
    info.update(extra)
    return info


def _verification_state(user_id: int) -> dict:
    from backend.db.users_db import get_user_email_verification_state

    return get_user_email_verification_state(int(user_id)) or {}


def test_google_does_not_link_unverified_password_account(app_factory) -> None:
    """A squatter's password account with the victim's email is not linked or entered."""
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import find_users_by_email, get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    squatter_id = save_user("squatter", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    r = _google_callback(client, _victim_info())

    assert r.status_code in (302, 303)
    assert "/login" in r.headers.get("Location", "")
    assert get_user_by_google_sub("victim-google-sub") is None
    assert not (_verification_state(squatter_id).get("google_sub") or "")
    assert not _verification_state(squatter_id).get("email_verified_at")
    assert len(find_users_by_email(_VICTIM)) == 1
    with client.session_transaction() as sess:
        assert not sess.get("user_id")
        pending = sess.get("pending_oauth_link")
        assert pending and pending["provider"] == "google"
        assert pending["user_id"] == squatter_id and pending["email"] == _VICTIM

    page = client.get("/login")
    assert b"already exists" in page.data
    assert b"oauth-link-notice" in page.data


def test_google_username_equal_to_email_does_not_match(app_factory) -> None:
    """A username shaped like the victim's email is not an email match (strict lookup)."""
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    # save_user skips the form rules, standing in for a legacy row.
    squatter_id = _verified(save_user(_VICTIM, "squatter@example.com", _SQUATTER_PW, role=ROLE_GENERAL))
    client = app.test_client()
    r = _google_callback(client, _victim_info())

    assert r.status_code in (302, 303)
    assert "/login" not in r.headers.get("Location", "")
    created = get_user_by_google_sub("victim-google-sub")
    assert created is not None
    assert int(created["id"]) != squatter_id
    assert created["email"] == _VICTIM
    assert not (_verification_state(squatter_id).get("google_sub") or "")
    with client.session_transaction() as sess:
        assert sess.get("user_id") == int(created["id"])


@pytest.mark.parametrize("claim", [None, "true", 1])
def test_google_rejects_missing_or_non_boolean_email_verified(app_factory, claim) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import find_users_by_email, get_user_by_google_sub

    info = {"sub": "unverified-sub", "email": "someone@example.com"}
    if claim is not None:
        info["email_verified"] = claim
    client = app.test_client()
    r = _google_callback(client, info)

    assert r.status_code in (302, 303)
    assert "/login" in r.headers.get("Location", "")
    assert get_user_by_google_sub("unverified-sub") is None
    assert find_users_by_email("someone@example.com") == []
    assert b"not verified" in client.get("/login").data


def test_google_link_completes_after_password_login(app_factory) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    owner_id = save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    _google_callback(client, _victim_info())
    assert get_user_by_google_sub("victim-google-sub") is None

    r = _password_login(client, "owner", _SQUATTER_PW)
    assert r.status_code in (302, 303)
    linked = get_user_by_google_sub("victim-google-sub")
    assert linked is not None and int(linked["id"]) == owner_id
    assert _verification_state(owner_id).get("email_verified_at")
    with client.session_transaction() as sess:
        assert sess.get("user_id") == owner_id
        assert "pending_oauth_link" not in sess

    # The next Google sign-in is a direct sub match.
    other = app.test_client()
    r2 = _google_callback(other, _victim_info())
    assert r2.status_code in (302, 303)
    assert "/login" not in r2.headers.get("Location", "")
    with other.session_transaction() as sess:
        assert sess.get("user_id") == owner_id


def test_google_link_completes_after_api_login(app_factory) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    owner_id = save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    _google_callback(client, _victim_info())
    token = client.get("/api/auth/csrf").get_json()["csrf_token"]
    r = client.post(
        "/api/auth/login",
        json={"login": _VICTIM, "password": _SQUATTER_PW},
        headers={"X-CSRF-Token": token},
    )
    assert r.status_code == 200
    linked = get_user_by_google_sub("victim-google-sub")
    assert linked is not None and int(linked["id"]) == owner_id


def test_google_pending_link_not_applied_to_another_account(app_factory) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    target_id = save_user("target", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    other_id = save_user("other", "other@example.com", "other-password-123", role=ROLE_GENERAL)
    client = app.test_client()
    _google_callback(client, _victim_info())

    r = _password_login(client, "other", "other-password-123")
    assert r.status_code in (302, 303)
    assert get_user_by_google_sub("victim-google-sub") is None
    assert not (_verification_state(target_id).get("google_sub") or "")
    assert not (_verification_state(other_id).get("google_sub") or "")
    with client.session_transaction() as sess:
        assert "pending_oauth_link" not in sess


def test_google_pending_link_expires(app_factory) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    owner_id = save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    _google_callback(client, _victim_info())
    with client.session_transaction() as sess:
        pending = dict(sess["pending_oauth_link"])
        pending["exp"] = 1
        sess["pending_oauth_link"] = pending

    assert b"oauth-link-notice" not in client.get("/login").data
    _password_login(client, "owner", _SQUATTER_PW)
    assert get_user_by_google_sub("victim-google-sub") is None
    assert not _verification_state(owner_id).get("email_verified_at")


def test_google_pending_link_refused_when_email_changed(app_factory) -> None:
    """The account's email must still be the provider's email when the link completes."""
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user, update_user_profile
    from backend.utils.roles import ROLE_GENERAL

    owner_id = save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    _google_callback(client, _victim_info())
    assert update_user_profile(owner_id, "owner", "moved@example.com") is None

    _password_login(client, "owner", _SQUATTER_PW)
    assert get_user_by_google_sub("victim-google-sub") is None


def test_google_does_not_link_account_whose_email_changed_after_verification(app_factory) -> None:
    """A verified account that changes its email to the victim's loses its verified flag."""
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user, update_user_profile
    from backend.utils.roles import ROLE_GENERAL

    squatter_id = _verified(save_user("squatter", "squatter@example.com", _SQUATTER_PW, role=ROLE_GENERAL))
    assert update_user_profile(squatter_id, "squatter", _VICTIM) is None
    client = app.test_client()
    r = _google_callback(client, _victim_info())

    assert "/login" in r.headers.get("Location", "")
    assert get_user_by_google_sub("victim-google-sub") is None
    with client.session_transaction() as sess:
        assert not sess.get("user_id")


def test_google_different_sub_on_linked_account_is_refused(app_factory) -> None:
    app = _make_app(app_factory, google=True)
    from backend.db.users_db import link_user_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    uid = _verified(save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL))
    assert link_user_google_sub(uid, "first-sub")
    client = app.test_client()
    r = _google_callback(client, _victim_info(sub="second-sub"))
    assert "/login" in r.headers.get("Location", "")
    assert b"different Google account" in client.get("/login").data
