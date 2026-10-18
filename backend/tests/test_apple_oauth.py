"""Tests for Apple OAuth scaffold (configuration gates only)."""

from __future__ import annotations

import pytest

from backend.auth import apple_oauth


@pytest.fixture
def client(app_factory):
    main = app_factory()
    main.app.config["TESTING"] = True
    return main.app.test_client()


def test_apple_not_configured_by_default(monkeypatch) -> None:
    for key in (
        "APPLE_OAUTH_CLIENT_ID",
        "APPLE_OAUTH_TEAM_ID",
        "APPLE_OAUTH_KEY_ID",
        "APPLE_OAUTH_PRIVATE_KEY",
        "APPLE_OAUTH_SHOW_BUTTON",
    ):
        monkeypatch.delenv(key, raising=False)
    assert apple_oauth.apple_oauth_configured() is False
    assert apple_oauth.apple_signin_visible() is False


def test_apple_visible_when_fully_configured(monkeypatch) -> None:
    monkeypatch.setenv("APPLE_OAUTH_CLIENT_ID", "com.example.web")
    monkeypatch.setenv("APPLE_OAUTH_TEAM_ID", "TEAM123")
    monkeypatch.setenv("APPLE_OAUTH_KEY_ID", "KEY123")
    monkeypatch.setenv("APPLE_OAUTH_PRIVATE_KEY", "-----BEGIN PRIVATE KEY-----\nMIGT\n-----END PRIVATE KEY-----")
    monkeypatch.setenv("APPLE_OAUTH_SHOW_BUTTON", "1")
    assert apple_oauth.apple_oauth_configured() is True
    assert apple_oauth.apple_signin_visible() is True


def test_apple_start_redirects_to_login_when_unconfigured(client, monkeypatch) -> None:
    monkeypatch.delenv("APPLE_OAUTH_CLIENT_ID", raising=False)
    rv = client.get("/auth/apple", follow_redirects=False)
    assert rv.status_code == 302
    assert "/login" in (rv.headers.get("Location") or "")


# --- P3.1: pre-registration takeover (D-SEC4 (a)) -----------------------------

from unittest.mock import patch  # noqa: E402

_CSRF = "c" * 32
_VICTIM = "victim@example.com"
_SQUATTER_PW = "squatter-password-123"


def _apple_app(app_factory):
    return app_factory(
        BILLING_STRIPE_ENABLED="0",
        ALLOW_DEFAULT_APP_USER="0",
        APPLE_OAUTH_CLIENT_ID="com.example.web",
        APPLE_OAUTH_TEAM_ID="TEAM123",
        APPLE_OAUTH_KEY_ID="KEY123",
        APPLE_OAUTH_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMIGT\n-----END PRIVATE KEY-----",
        APPLE_OAUTH_REDIRECT_URI="http://localhost/auth/apple/callback",
    ).app


def _apple_callback(client, claims: dict):
    with client.session_transaction() as sess:
        sess["apple_oauth_state"] = "expected-state"
    with (
        patch("backend.auth.apple_oauth._exchange_code", return_value={"id_token": "header.payload.sig"}),
        patch("backend.auth.apple_oauth._decode_id_token", return_value=claims),
    ):
        return client.get(
            "/auth/apple/callback?code=abc&state=expected-state",
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


def _victim_claims(sub: str = "001234.victim.0001", **extra) -> dict:
    claims = {"sub": sub, "email": _VICTIM, "email_verified": True}
    claims.update(extra)
    return claims


def _state(user_id: int) -> dict:
    from backend.db.users_db import get_user_email_verification_state

    return get_user_email_verification_state(int(user_id)) or {}


def test_apple_does_not_link_unverified_password_account(app_factory) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import find_users_by_email, get_user_by_apple_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    squatter_id = save_user("squatter", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    r = _apple_callback(client, _victim_claims())

    assert r.status_code in (302, 303)
    assert "/login" in r.headers.get("Location", "")
    assert get_user_by_apple_sub("001234.victim.0001") is None
    assert not (_state(squatter_id).get("apple_sub") or "")
    assert not _state(squatter_id).get("email_verified_at")
    assert len(find_users_by_email(_VICTIM)) == 1
    with client.session_transaction() as sess:
        assert not sess.get("user_id")
        pending = sess.get("pending_oauth_link")
        assert pending and pending["provider"] == "apple" and pending["user_id"] == squatter_id
    page = client.get("/login")
    assert b"already exists" in page.data and b"Apple" in page.data


def test_apple_username_equal_to_email_does_not_match(app_factory) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import get_user_by_apple_sub, mark_user_email_verified, save_user
    from backend.utils.roles import ROLE_GENERAL

    squatter_id = save_user(_VICTIM, "squatter@example.com", _SQUATTER_PW, role=ROLE_GENERAL)
    mark_user_email_verified(squatter_id)
    client = app.test_client()
    r = _apple_callback(client, _victim_claims())

    assert r.status_code in (302, 303)
    assert "/login" not in r.headers.get("Location", "")
    created = get_user_by_apple_sub("001234.victim.0001")
    assert created is not None and int(created["id"]) != squatter_id
    assert created["email"] == _VICTIM
    assert not (_state(squatter_id).get("apple_sub") or "")


@pytest.mark.parametrize("claim", [None, False, "false", "yes"])
def test_apple_rejects_email_without_true_email_verified(app_factory, claim) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import find_users_by_email, get_user_by_apple_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    claims = {"sub": "001234.unverified.0001", "email": "someone@example.com"}
    if claim is not None:
        claims["email_verified"] = claim
    client = app.test_client()
    r = _apple_callback(client, claims)
    assert "/login" in r.headers.get("Location", "")
    assert get_user_by_apple_sub("001234.unverified.0001") is None
    assert find_users_by_email("someone@example.com") == []
    assert b"not verified" in client.get("/login").data

    # Same for an email that matches an existing account: no link, no pending entry.
    claims["email"] = _VICTIM
    r2 = _apple_callback(client, claims)
    assert "/login" in r2.headers.get("Location", "")
    with client.session_transaction() as sess:
        assert "pending_oauth_link" not in sess


def test_apple_accepts_string_true_email_verified(app_factory) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import get_user_by_apple_sub

    client = app.test_client()
    r = _apple_callback(client, _victim_claims(sub="001234.string.0001", email_verified="true"))
    assert "/login" not in r.headers.get("Location", "")
    assert get_user_by_apple_sub("001234.string.0001") is not None


def test_apple_link_completes_after_password_login(app_factory) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import get_user_by_apple_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    owner_id = save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    client = app.test_client()
    _apple_callback(client, _victim_claims())
    assert get_user_by_apple_sub("001234.victim.0001") is None

    r = _password_login(client, _VICTIM, _SQUATTER_PW)
    assert r.status_code in (302, 303)
    linked = get_user_by_apple_sub("001234.victim.0001")
    assert linked is not None and int(linked["id"]) == owner_id
    assert _state(owner_id).get("email_verified_at")
    with client.session_transaction() as sess:
        assert sess.get("user_id") == owner_id
        assert "pending_oauth_link" not in sess


def test_apple_links_verified_account_and_returning_sub(app_factory) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import get_user_by_apple_sub, mark_user_email_verified, save_user
    from backend.utils.roles import ROLE_GENERAL

    owner_id = save_user("owner", _VICTIM, _SQUATTER_PW, role=ROLE_GENERAL)
    mark_user_email_verified(owner_id)
    client = app.test_client()
    r = _apple_callback(client, _victim_claims())
    assert "/login" not in r.headers.get("Location", "")
    linked = get_user_by_apple_sub("001234.victim.0001")
    assert linked is not None and int(linked["id"]) == owner_id

    # A returning sub without email (Apple omits it later) signs in by sub.
    again = app.test_client()
    r2 = _apple_callback(again, {"sub": "001234.victim.0001"})
    assert "/login" not in r2.headers.get("Location", "")
    with again.session_transaction() as sess:
        assert sess.get("user_id") == owner_id


def test_apple_new_user_without_email_gets_relay_address(app_factory) -> None:
    app = _apple_app(app_factory)
    from backend.db.users_db import get_user_by_apple_sub

    client = app.test_client()
    r = _apple_callback(client, {"sub": "001234.noemail.0001"})
    assert "/login" not in r.headers.get("Location", "")
    created = get_user_by_apple_sub("001234.noemail.0001")
    assert created is not None
    assert created["email"].endswith("@privaterelay.appleid.com")
