"""Google OAuth sign-in (optional, env-gated)."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest


def _fresh_app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    *,
    show_button: bool = False,
    google: bool = False,
):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_test.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_test.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_OAUTH_SHOW_BUTTON", raising=False)
    monkeypatch.delenv("GOOGLE_OAUTH_OFFER_PREMIUM_ON_LOGIN", raising=False)
    if google:
        monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_ID", "test-client-id")
        monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_SECRET", "test-client-secret")
        monkeypatch.setenv(
            "GOOGLE_OAUTH_REDIRECT_URI",
            "http://localhost/auth/google/callback",
        )
    else:
        # setenv("") not delenv: reload(main) re-runs load_project_dotenv(override=False),
        # which would re-populate deleted vars from .env; empty values survive.
        monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_ID", "")
        monkeypatch.setenv("GOOGLE_OAUTH_CLIENT_SECRET", "")
        monkeypatch.setenv("GOOGLE_OAUTH_REDIRECT_URI", "")
    if show_button:
        monkeypatch.setenv("GOOGLE_OAUTH_SHOW_BUTTON", "1")
    import backend.main as main

    importlib.reload(main)
    return main.app


def test_google_start_redirects_to_login_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path)
    client = app.test_client()
    r = client.get("/auth/google", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "/login" in r.headers.get("Location", "")


def test_google_start_redirects_to_google_when_configured(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True)
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
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True)
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
    assert b"Upgrade to Premium" in r2.data
    assert b"Continue with free" in r2.data


def test_google_callback_returning_user_skips_premium_offer_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("existing", "existing@example.com", "long-enough-password", role=ROLE_GENERAL)

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
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True)
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("existing", "existing@example.com", "long-enough-password", role=ROLE_GENERAL)

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
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True)
    from backend.db.users_db import get_user_by_google_sub, save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("existing", "existing@example.com", "long-enough-password", role=ROLE_GENERAL)

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
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True)
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
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    app = _fresh_app(monkeypatch, tmp_path, google=True, show_button=False)
    client = app.test_client()
    r = client.get("/login")
    assert b"Sign in with Google" in r.data
    assert b"hidden" in r.data

    app_visible = _fresh_app(monkeypatch, tmp_path, google=True, show_button=True)
    r2 = app_visible.test_client().get("/login")
    assert b"Sign in with Google" in r2.data
    assert b'class="auth-oauth" hidden' not in r2.data
